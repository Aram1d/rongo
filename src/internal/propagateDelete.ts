import { DeleteOptions, Filter as FilterBase, Document } from "mongodb";
import { differenceWith, entries, isEmpty } from "lodash";
import {
  Collection,
  DeletedKeys,
  DeletePolicy,
  Filter,
  RemoveScheduler
} from "../.";

// This function propagates delete instruction to foreign collections

export async function propagateDelete<T extends Document>(
  collection: Collection<T>,
  query: Filter<T>,
  single: boolean,
  options: (DeleteOptions & { propagate?: boolean }) | undefined,
  scheduler: RemoveScheduler,
  deletedKeys: DeletedKeys
) {
  // Get the primary keys that still need to be deleted :
  const keys = await getKeys(collection, query, single, deletedKeys);
  // If there are keys to delete, apply the appropriate delete policies to cross-collection references :
  if ((options?.propagate ?? true) && !isEmpty(keys))
    for (const [colName, foreignKeys] of entries(collection.references)) {
      // Get the foreign collection :
      const refCol = collection.rongo.collection(colName);
      // For each foreign key which might reference the current keys :
      for (const [foreignKey, foreignKeyConfig] of entries(foreignKeys)) {
        // Define a filter query to the concerned foreign documents :
        const refQuery: Filter<any> = { [foreignKey]: { $in: keys } };

        // If there's no reference to the current keys in the foreign collection, ignore that step :
        if (!(await refCol.has(refQuery, { baseQuery: true }))) continue;
        // Otherwise implement the appropriate delete policy :
        switch (foreignKeyConfig.onDelete) {
          case DeletePolicy.Reject:
            // Simply reject :
            throw new Error(
              `Remove operation in collection <${collection.name}> got rejected : Some foreign keys <${foreignKey}> in collection <${colName}> point to targeted documents`
            );

          case DeletePolicy.Delete:
            // Recursively propagate removal in reference collection :
            scheduler.push(
              await propagateDelete(
                refCol,
                refQuery,
                false,
                options,
                scheduler,
                deletedKeys
              )
            );
            break;

          case DeletePolicy.Unset:
            // Target relevant documents and unset where necessary :
            scheduler.push(() => {
              const [target, filter] = foreignKeyConfig.updater!;
              return refCol.update(
                refQuery,
                { $unset: { [target]: 1 } },
                {
                  multi: true,
                  baseQuery: true,
                  ...(filter && { arrayFilters: [{ [filter]: { $in: keys } }] })
                }
              );
            });
            break;

          case DeletePolicy.Nullify:
            // Target relevant documents and nullify where necessary :
            scheduler.push(() => {
              const [target, filter] = foreignKeyConfig.updater!;
              return refCol.update(
                refQuery,
                { $set: { [target]: null } },
                {
                  multi: true,
                  baseQuery: true,
                  ...(filter && { arrayFilters: [{ [filter]: { $in: keys } }] })
                }
              );
            });
            break;

          case DeletePolicy.Pull:
            // Target relevant documents and pull where necessary :
            scheduler.push(() => {
              const [target, filter] = foreignKeyConfig.updater!;
              return refCol.update(
                refQuery,
                {
                  $pull: {
                    [target]: filter
                      ? { [filter]: { $in: keys } }
                      : { $in: keys }
                  }
                },
                { multi: true, baseQuery: true }
              );
            });
            break;
        }
      }
    }
  // Finally, after clean-up, remove the documents that were still unmarked :
  return async () => {
    const col = await collection.handle;
    return options
      ? col.deleteMany(
          { [collection.key]: { $in: keys } } as FilterBase<T>,
          options
        )
      : col.deleteMany({ [collection.key]: { $in: keys } } as FilterBase<T>);
  };
}

// This function gets the primary keys associated with a filter query and returns them after filtering
// out the ones already marked as deleted in deletedKeys

async function getKeys<T extends Document>(
  collection: Collection<T>,
  query: Filter<T>,
  single: boolean,
  deletedKeys: DeletedKeys
) {
  if (!(collection.name in deletedKeys)) deletedKeys[collection.name] = [];
  const markedKeys = deletedKeys[collection.name];
  // Get the primary keys associated with the filter query :
  const keys: Array<any> = await collection
    .find(query, {
      ...(single && { limit: 1 }),
      projection: { [collection.key]: 1 },
      baseQuery: true
    })
    .select(collection.key);
  // Subtract the keys already marked as deleted :
  const keysToDelete = differenceWith(
    keys,
    markedKeys,
    (k1, k2) => k1.toString() === k2.toString()
  );
  // Add the unmarked keys to the marked ones :
  markedKeys.push(...keysToDelete);
  // Return the unmarked keys :
  return keysToDelete;
}
