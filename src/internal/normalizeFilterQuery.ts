import { Filter as FilterBase } from "mongodb";
import { isArray, isPlainObject, last } from "lodash";
import { Collection, mapDeep, stackToKey } from "../.";
import { Document, Filter } from "mongodb";

// This function transforms an augmented FilterQuery into a traditional FilterQuery

export async function normalizeFilterQuery<T extends Document>(
  collection: Collection<T>,
  query: Filter<T>,
  options?: { baseQuery?: boolean }
): Promise<FilterBase<T>> {
  if (options?.baseQuery) return query as FilterBase<T>;
  return mapDeep(query, function customizer(value, stack) {
    switch (last(stack)) {
      // If there's an $expr, it has to be ignored by the normalizing process :
      case "$expr":
        return value;
      // If there's an $in or a $nin, process it :
      case "$in":
      case "$nin":
        // Check the current key :
        const key = stackToKey(stack);
        // Get the foreign key config if one exists :
        const foreignKeyConfig = collection.foreignKeys[key];
        // If we're not at a foreign key location, quit early :
        if (!foreignKeyConfig) return;
        // Otherwise, get the foreign collection :
        const foreignCol = collection.rongo.collection(
          foreignKeyConfig.collection
        );

        const primaryKeys = (query: Filter<any>) =>
          foreignCol.find(query).select(foreignCol.key);

        // If we have a foreign filter query :
        if (isPlainObject(value)) return primaryKeys(value);
        // If we have an array of keys and/or foreign filter queries :
        if (isArray(value))
          return value.reduce<Promise<Array<any>>>(
            async (acc, item) =>
              isPlainObject(item)
                ? [...(await acc), ...(await primaryKeys(item))]
                : [...(await acc), item],
            Promise.resolve([])
          );
        // Otherwise, it's a misshaped query :
        throw new Error(
          `Invalid query selector for foreign key <${key}> in collection <${collection.name}> : <$in> and <$nin> selectors must be arrays or foreign filter queries`
        );
    }
  });
}
