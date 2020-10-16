import { sep } from "path";
import { existsSync, mkdirSync, readFileSync, promises, writeFileSync } from "fs";

const { writeFile, appendFile } = promises;

interface Xedis {
    append(key: string, value: string): Promise<number>;
    bgrewriteaof(key: string): Promise<void>;
    bgsave(): Promise<string>;
    decr(key: string): Promise<string>;
    del(key: string): Promise<void>;
    dump(key: string): Promise<void>;
    exists(key: string): Promise<boolean>;
    expire(key: string, expirationInSeconds: number): Promise<void>;
    get(key: string): Promise<string>;
    getrange(key: string, start: number, end: number): Promise<string>;
    getset(key: string, value: string): Promise<string>;
    incr(key: string, step?: number): Promise<string>;
    mget(...args: string[]): Promise<(string | null)[]>;
    persist(key: string): Promise<void>;
    pttl(key: string): Promise<number>;
    randomkey(): Promise<string | null>;
    save(): string;
    set(key: string, value: string, expirationInSeconds?: number | undefined): Promise<void>;
    setrange(key: string, start: number, value: string): Promise<number>;
    strlen(key: string): Promise<number>;
    ttl(key: string): Promise<number>;
}

interface Options {
  appendfsync?: "always" | "everysec";
  backupInterval?: number;
  customPersistencePath?: string;
}

export function create_store(name: string, options?: Options): Xedis {
  const default_options: NonNullable<Options> =
  {
    appendfsync: "everysec",
    backupInterval: 9e4
  };
  const { appendfsync, backupInterval, customPersistencePath } = Object.assign(default_options, options);

  const xedis_dir = customPersistencePath || process.cwd() + sep + ".xedis";

  if (!customPersistencePath) {
    const gitignore = process.cwd() + sep + ".gitignore";
    if(existsSync(gitignore)) {
      appendFile(gitignore, "\r\n# Xedis persistence directory\r\n.xedis\r\n");
    }
  }
  if (!existsSync(xedis_dir)) {
    mkdirSync(xedis_dir);
  }

  const store: {
    [key: string]: {
      value: string, expiration?: Date
    }
  } = {};
  const expiration_table: { [key: string]: { timeout: NodeJS.Timeout, datetime: Date } } = {};

  const store_path = xedis_dir + sep + name;

  if (!existsSync(store_path)) {
    mkdirSync(store_path);
  }

  const persistent_path = store_path + sep + "persistent.aof";
  const backup_path = store_path + sep + "dump.json";

  if (existsSync(persistent_path)) {
    try {
      const aof = readFileSync(persistent_path, "utf-8");
      const json = `{${aof.substr(0, aof.length - 1)}}`;
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
      expiration_table[key] = {
        timeout: setTimeout(function remove() { del(key); }, expiration),
        datetime: new Date(obj.expiration)
      };
    }
  });

  async function writeToDisk(key: string, value: string | null = null) {
    if (!key) {
      return;
    }
    const fragment = `"${key}":${value},`;
    await appendFile(persistent_path, fragment);
  }

  function save() {
    writeFileSync(backup_path, JSON.stringify(store));
    return "OK";

  }

  async function bgsave() {
    await writeFile(backup_path, JSON.stringify(store));
    return "OK";
  }

  if (typeof backupInterval === "number") {
    setInterval(bgsave, backupInterval);
  }

  async function bgrewriteaof() {
    const json = JSON.stringify(store);
    await writeFile(persistent_path, json.substr(1, json.length - 1) + ",");
  }

  async function set(key: string, value: string, expirationInSeconds?: number) {
    const obj: { value: string; expiration?: Date } = { value };
    if (typeof expirationInSeconds === "number") {
      obj.expiration = new Date(Date.now() + (expirationInSeconds * 1e3));
      expiration_table[key] = {
        timeout: setTimeout(function remove() { del(key); }, expirationInSeconds * 1e3),
        datetime: obj.expiration
      };
    }
    store[key] = obj;
    await writeToDisk(key, JSON.stringify(obj));
  }

  async function get(key: string): Promise<string> {
    try {
      return store[key].value;
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
    clearTimeout(expiration_table[key].timeout);
    delete expiration_table[key];
  }

  async function randomkey(): Promise<string | null> {
    const keys = Object.keys(store);
    if (keys.length === 0) {
      return null;
    }
    return keys[Math.round(Math.random() * keys.length)];
  }

  async function append(key: string, value: string): Promise<number> {
    const newValue = await exists(key) ? await get(key) + value : value;
    const expiration = await ttl(key);
    await set(key, newValue, expiration >= 0 ? expiration : undefined);
    return newValue.length;
  }


  async function incr(key: string, step = 1) {
    if (await exists(key)) {
      const value = await get(key);
      if (Number.isNaN(Number(value))) {
        throw Error("You can only call INCR and DECR on integer or null values.");
      }
      const newValue = String(value + step);
      await set(key, newValue);
      return newValue;
    } else {
      await set(key, String(step));
      return String(step);
    }
  }

  async function decr(key: string) {
    return incr(key, -1);
  }

  async function pttl(key: string) {
    if (await exists(key)) {
      if (!(expiration_table[key]?.datetime instanceof Date)) {
        return -1;
      }
      return (expiration_table[key].datetime.getTime() - Date.now());
    }
    return -2;

  }

  async function ttl(key: string) {
    const t = await pttl(key);
    if (t >= 0) {
      return t / 1e3;
    }
    return t;
  }

  async function mget(...args: string[]) {
    return Promise.all(args.map(async function (k) {
      try {
        const value = await get(k);
        return value;
      } catch {
        return null;
      }
    }));
  }

  async function getset(key: string, value: string) {
    const prev = await get(key);
    await set(key, value);
    return prev;
  }

  async function dump(key: string) {
    // RESP
    return JSON.parse(await get(key));
  }

  async function strlen(key: string): Promise<number> {
    return (await exists(key)) ? (await get(key)).length : 0;
  }

  async function getrange(key: string, start: number, end: number): Promise<string> {
    const string = await get(key);
    if (start < 0) {
      start += string.length;
    }
    if (end < 0) {
      end += string.length;
    }
    return string.substr(start, end);
  }

  async function setrange(key: string, start: number, value: string) {
    const length = await strlen(key);
    let string = "";
    if (length < start) {
      string += (await getrange(key, 0, length)).padStart(start, "\0");
    } else {
      string += await getrange(key, 0, start);
    }
    string += value;
    if (string.length < length) {
      const b = await getrange(key, start + value.length, length);
      string += b;
    }
    await set(key, string);
    return string.length;
  }

  return {
    append,
    bgrewriteaof,
    bgsave,
    decr,
    del,
    dump,
    exists,
    expire,
    get,
    getrange,
    getset,
    incr,
    mget,
    persist,
    pttl,
    randomkey,
    save,
    set,
    setrange,
    strlen,
    ttl
  };
}
