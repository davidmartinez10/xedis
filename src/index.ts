import os from "os";
import { sep } from "path";
import { existsSync, mkdirSync, readFileSync, promises } from "fs";

const { writeFile, appendFile } = promises;

const xedis_dir = os.homedir() + sep + "xedis";

if (!existsSync(xedis_dir)) {
  mkdirSync(xedis_dir);
}

async function nextTick() {
  return new Promise(resolve => process.nextTick(resolve));
}

interface Xedis {
  set(key: string, value: any, expirationInSeconds?: number): Promise<void>;
  get(key: string): Promise<any>;
  del(key: string): Promise<void>;
  exists(key: string): Promise<boolean>;
  expire(key: string, expirationInSeconds: number): Promise<void>;
  persist(key: string): Promise<void>;
}

export function create_store(name: string, backupInterval = 9e4): Xedis {
  const store: {
    [key: string]: {
      value: any, expiration?: Date
    }
  } = {};
  const expiration_table: { [key: string]: NodeJS.Timeout } = {};
  const store_path = xedis_dir + sep + name + ".xson";
  const backup_path = xedis_dir + sep + name + ".backup.json";

  if (existsSync(store_path)) {
    try {
      const xson = readFileSync(store_path, "utf-8");
      const json = `{${xson.substr(0, xson.length - 1)}}`;
      Object.assign(store, JSON.parse(json));
    } catch {
      console.error("Persistent file corrupted, attempting to load JSON backup.");
      try {
        if (!existsSync(backup_path)) {
          throw Error();
        }
        const backup = readFileSync(backup_path, "utf-8");
        Object.assign(store, JSON.parse(backup));
      } catch {
        console.error("Could not recover store from disk. Continuing with an empty object.");
      }
    }
  }

  Object.entries(store).forEach(function ([key, v]) {
    const obj = typeof v === "object" ? v : JSON.parse(v);
    if (obj?.expiration) {
      const expiration = new Date(obj.expiration).getTime() - Date.now();
      expiration_table[key] = setTimeout(function remove() { del(key); }, expiration);
    }
  });

  async function writeToDisk(key: string, value: string | null) {
    if (!key) {
      return;
    }
    const fragment = `"${key}":${value},`;
    await appendFile(store_path, fragment);
  }

  async function backup() {
    await writeFile(backup_path, JSON.stringify(store));
  }
  setInterval(backup, backupInterval);

  async function set(key: string, value: any, expirationInSeconds?: number) {
    const obj: { value: any; expiration?: Date } = { value };
    if (typeof expirationInSeconds === "number") {
      obj.expiration = new Date(Date.now() + (expirationInSeconds * 1e3));
      expiration_table[key] = setTimeout(function remove() { del(key); }, expirationInSeconds * 1e3);
    }
    store[key] = obj;
    await writeToDisk(key, JSON.stringify(obj));
  }

  async function get(key: string) {
    try {
      await nextTick();
      return JSON.parse(store[key].value);
    } catch (err) {
      throw Error("The key [" + key + "] does not exist.");
    }
  }

  async function del(key: string) {
    try {
      delete store[key];
      writeToDisk(key, null);
    } catch {
      throw Error("The key [" + key + "] does not exist.");
    }
  }

  async function exists(key: string) {
    // eslint-disable-next-line no-prototype-builtins
    return store.hasOwnProperty(key);
  }

  async function expire(key: string, expirationInSeconds: number) {
    const value = await get(key);
    await set(key, value, expirationInSeconds);
  }

  async function persist(key: string) {
    clearTimeout(expiration_table[key]);
  }

  return {
    get,
    set,
    del,
    exists,
    expire,
    persist
  };
}
