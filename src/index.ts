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

export function createStore(name: string) {
  const store: { [key: string]: string } = {};
  const store_path = xedis_dir + sep + name + ".json";

  if (existsSync(store_path)) {
    try {
      const json = readFileSync(store_path, "utf-8");
      Object.assign(store, JSON.parse(json.toString()));
    } catch {
      console.error("Could not recover store from disk. Continuing with an empty object.");
    }
  }

  let write_timeout: NodeJS.Timeout & { _destroyed?: boolean };
  let blocked = false;
  async function debounce_write() {
    if (blocked) {
      return;
    }
    if (!write_timeout || write_timeout._destroyed) {
      write_timeout = setTimeout(
        async function write() {
          blocked = true;
          await writeFile(store_path, JSON.stringify(store));
          blocked = false;
        },
        Date.now() + 3e3
      );
    }
  }

  async function set(key: string, value: any, expiration?: Date) {
    store[key] = JSON.stringify({ value, expiration });
    debounce_write();
  }
  async function get(key: string) {
    try {
      const { value } = JSON.parse(store[key]);
      await nextTick();
      return JSON.parse(value);
    } catch {
      throw Error(`The key [${key}] does not exist.`);
    }
  }

  async function del(key: string) {
    try {
      delete store[key];
      debounce_write();
    } catch {
      throw Error(`The key [${key}] does not exist.`);
    }
  }

  return {
    get,
    set,
    del
  };
}
