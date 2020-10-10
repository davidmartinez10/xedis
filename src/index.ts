import os from "os";
import { sep } from "path";
import { existsSync, mkdirSync, readFileSync } from "fs";
import { writeFile } from "fs/promises";

const xedis_dir = os.homedir() + sep + "xedis";

if (!existsSync(xedis_dir)) {
  mkdirSync(xedis_dir);
}

async function nextTick() {
  return new Promise(resolve => process.nextTick(resolve));
}

export function create_store(name: string, maxWriteRate = 3e4, backupInterval = 9e4): {
  set(key: string, value: any, expirationInSeconds?: number): Promise<void>;
  get(key: string): Promise<void>;
  del(key: string): Promise<void>;
} {
  const store: { [key: string]: string } = {};
  const store_path = xedis_dir + sep + name + ".json";
  const backup_path = xedis_dir + sep + name + ".backup.json";

  if (existsSync(store_path)) {
    try {
      const json = readFileSync(store_path, "utf-8");
      Object.assign(store, JSON.parse(json));
    } catch {
      console.log("Persistent file corrupted, loading last functioning backup.");
      try {
        if (!existsSync(backup_path)) {
          throw Error();
        }
        const backup = readFileSync(backup_path, "utf-8");
        Object.assign(store, JSON.parse(backup));
      } catch {
        console.log("Could not recover store from disk. Continuing with an empty object.");
      }
    }
  }

  let write_timeout: NodeJS.Timeout & { _destroyed?: boolean };
  let blocked = false;

  async function debounce_write() {
    if (!blocked && (!write_timeout || write_timeout._destroyed)) {
      write_timeout = setTimeout(
        async function write() {
          blocked = true;
          await writeFile(store_path, JSON.stringify(store));
          blocked = false;
        },
        Date.now() + maxWriteRate
      );
    }
  }

  async function backup() {
    if (!blocked && (!write_timeout || write_timeout._destroyed)) {
      await writeFile(backup_path, JSON.stringify(store));
    } else {
      setTimeout(backup, 500);
    }
  }

  setInterval(backup, backupInterval);

  async function set(key: string, value: any, expirationInSeconds?: number) {
    const obj: { value: any; expiration?: Date } = { value };
    if (typeof expirationInSeconds === "number") {
      obj.expiration = new Date(Date.now() + (expirationInSeconds * 1e3));
      setTimeout(function remove() { del(key); }, expirationInSeconds * 1e3);
    }
    store[key] = JSON.stringify(obj);
    debounce_write();
  }

  async function get(key: string) {
    try {
      const { value } = JSON.parse(store[key]);
      await nextTick();
      return JSON.parse(value);
    } catch {
      throw Error("The key [" + key + "] does not exist.");
    }
  }

  async function del(key: string) {
    try {
      delete store[key];
      debounce_write();
    } catch {
      throw Error("The key [" + key + "] does not exist.");
    }
  }

  return {
    get,
    set,
    del
  };
}
