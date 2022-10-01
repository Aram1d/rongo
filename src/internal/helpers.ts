import { Binary, ObjectId } from "mongodb";
import { isString } from "lodash";
import { extname } from "path";
import { readFileSync } from "fs";
import YAML from "yaml";
import { CollectionConfig, Stack } from "../.";

// Some reexports for practicality

export { ObjectId, Binary };

// This function creates a default collection config

export function createDefaultConfig(): CollectionConfig {
  return {
    key: "_id",
    foreignKeys: Object.create(null),
    references: Object.create(null)
  };
}

// This function transforms a stack into an exploitable key

export function stackToKey(stack: Stack) {
  return stack.filter(key => isString(key) && !key.startsWith("$")).join(".");
}

// This function loads a schema from a file

export function loadSchema(fileName: string): unknown {
  const extension = extname(fileName);
  const content = readFileSync(fileName).toString();
  switch (extension.toLowerCase()) {
    case ".json":
      return JSON.parse(content);
    case ".yaml":
    case ".yml":
      return YAML.parse(content);
    default:
      throw new Error(`Unknown file extension <${extension}> for Rongo schema`);
  }
}

// This function is an async version of Array.prototype.filter

export function asyncFilter<T>(
  array: Array<T>,
  predicate: (
    item: T,
    index: number,
    array: Array<T>
  ) => boolean | Promise<boolean>
) {
  return array.reduce<Promise<Array<T>>>(
    async (acc, item, index) =>
      (await predicate(item, index, array)) ? [...(await acc), item] : acc,
    Promise.resolve([])
  );
}
