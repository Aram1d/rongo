import {
  BulkWriteOptions,
  Document,
  InsertManyResult,
  InsertOneResult,
  OptionalUnlessRequiredId,
  WithId
} from "mongodb";
import { castArray, entries, isArray } from "lodash";
import {
  Collection,
  InsertionDoc,
  Rongo,
  verifyInsertionDoc,
  normalizeInsertionDoc
} from "../../.";

// This function is used to perform nested inserts in a safe way :

export async function insertSafely<T extends Document>(
  collection: Collection<T>,
  doc: InsertionDoc<T> | Array<InsertionDoc<T>>,
  dependencies: DependencyCollector,
  options?: BulkWriteOptions & { baseDocument?: boolean }
) {
  const col = await collection.handle;
  const normalized = await normalizeInsertionDoc(
    collection,
    doc,
    dependencies,
    options
  );
  await verifyInsertionDoc(collection, normalized);
  let result: InsertManyResult<T> | InsertOneResult<T>;
  let documents: WithId<T> | Array<WithId<T>>;
  if (!isArray(normalized)) {
    result = (options
      ? await col.insertOne(normalized as OptionalUnlessRequiredId<T>, options)
      : await col.insertOne(
          normalized as OptionalUnlessRequiredId<T>
        )) as InsertOneResult<T>;
    documents = (await col.findOne({ _id: result.insertedId as any })) as any;
  } else {
    result = options
      ? await col.insertMany(
          normalized as OptionalUnlessRequiredId<T>[],
          options
        )
      : ((await col.insertMany(
          normalized as OptionalUnlessRequiredId<T>[]
        )) as InsertManyResult<T>);
    documents = await col
      .find({ _id: { $in: result.insertedIds } } as any)
      .toArray();
  }
  dependencies.add(
    collection,
    await collection.from(documents).select(collection.key)
  );
  if (!result.acknowledged)
    throw new Error(
      `Something went wrong in the MongoDB driver during insert in collection <${collection.name}>`
    );
  return documents;
}

// This class is used to collect document references across the database for nested insert clean-ups

export class DependencyCollector {
  private readonly rongo: Rongo;
  private dependencies: {
    [collection: string]: Array<any>;
  };

  constructor(rongo: Rongo) {
    this.rongo = rongo;
    this.dependencies = Object.create(null);
  }

  add(collection: Collection<any>, keys: any | Array<any>) {
    if (!(collection.name in this.dependencies))
      this.dependencies[collection.name] = [];
    this.dependencies[collection.name].push(...castArray(keys));
  }

  async delete() {
    for (const [colName, keys] of entries(this.dependencies)) {
      const collection = this.rongo.collection(colName);
      const col = await collection.handle;
      await col.deleteMany({ [collection.key]: { $in: keys } });
    }
    this.dependencies = Object.create(null);
  }
}
