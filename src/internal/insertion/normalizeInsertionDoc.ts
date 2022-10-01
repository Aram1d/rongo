import { BulkWriteOptions, Document, OptionalId, WithoutId } from "mongodb";
import { isPlainObject } from "lodash";
import { Collection, DependencyCollector, InsertionDoc, insertSafely, mapDeep, stackToKey } from "../../.";

type InsertionUnion<T extends Document> = InsertionDoc<T> | Array<InsertionDoc<T>>
type Update<T extends Document> = WithoutId<T>

// This function transforms an augmented insertion document into a simple insertion document

export async function normalizeInsertionDoc<T extends Document>(
  collection: Collection<T>,
  doc: Update<T>,
  dependencies: DependencyCollector,
  options?: BulkWriteOptions & { baseDocument?: boolean }
):  Promise<WithoutId<T>>

export async function normalizeInsertionDoc<T extends Document>(
  collection: Collection<T>,
  doc: InsertionUnion<T>,
  dependencies: DependencyCollector,
  options?: BulkWriteOptions & { baseDocument?: boolean }
):  Promise<OptionalId<T> | Array<OptionalId<T>>>

export async function normalizeInsertionDoc<T extends Document>(
  collection: Collection<T>,
  doc: InsertionUnion<T> | Update<T>,
  dependencies: DependencyCollector,
  options?: BulkWriteOptions & { baseDocument?: boolean }
): Promise<OptionalId<T> | Array<OptionalId<T>> | WithoutId<T>> {
  if (options?.baseDocument) return doc as OptionalId<T> | Array<OptionalId<T>>;
  return mapDeep(doc, async (value, stack) => {
    if (!isPlainObject(value)) return;
    // Get the foreign key config :
    const foreignKeyConfig = collection.foreignKeys[stackToKey(stack)];
    // If we're not visiting a foreign key location, finish there :
    if (!foreignKeyConfig) return;
    // Get the foreign collection :
    const foreignCol = collection.rongo.collection(foreignKeyConfig.collection)
    // Insert the nested document :
    const nestedDoc = await insertSafely(
      foreignCol,
      value,
      dependencies,
      options
    );
    // And return its primary key
    return foreignCol.from(nestedDoc).select(foreignCol.key);
  });
}
