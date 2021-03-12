import {
  Collection as Col,
  CollectionAggregationOptions,
  CollectionInsertManyOptions,
  CollectionInsertOneOptions,
  DbCollectionOptions,
  FindOneOptions,
  MongoCountPreferences,
  WithId
} from "mongodb";
import { get, isArray, map } from "lodash";
import {
  createDefaultConfig,
  Database,
  FilterQuery,
  InsertionDoc,
  normalizeFilterQuery,
  normalizeInsertionDoc
} from ".";

export type Document = {
  [key: string]: any;
};

export class Collection<T extends Document> {
  database: Database;
  name: string;
  handle: Promise<Col<T>>;

  constructor(
    database: Database,
    name: string,
    options: DbCollectionOptions = {}
  ) {
    this.database = database;
    this.name = name;
    this.handle = database.handle.then(db => db.collection<T>(name, options));
    if (!(name in database.graph)) database.graph[name] = createDefaultConfig();
  }

  // Query methods :

  async aggregate<U = T>(
    pipeline?: object[],
    options?: CollectionAggregationOptions
  ) {
    const col = await this.handle;
    return col.aggregate<U>(pipeline, options).toArray();
  }

  async count(query: FilterQuery<T> = {}, options?: MongoCountPreferences) {
    const col = await this.handle;
    const normalized = await normalizeFilterQuery(this, query);
    return col.countDocuments(normalized, options);
  }

  async find<U = T>(
    query: FilterQuery<T> = {},
    options?: FindOneOptions<U extends T ? T : U>
  ) {
    const col = await this.handle;
    const normalized = await normalizeFilterQuery(this, query);
    return col.find(normalized, options ?? {}).toArray();
  }

  async findOne<U = T>(
    query: FilterQuery<T> = {},
    options?: FindOneOptions<U extends T ? T : U>
  ) {
    const col = await this.handle;
    const normalized = await normalizeFilterQuery(this, query);
    return col.findOne(normalized, options);
  }

  async findMap<U = T, K extends keyof U = keyof U>(
    query: FilterQuery<T>,
    key: K,
    options?: FindOneOptions<U extends T ? T : U>
  ) {
    const array = await this.find(query, {
      ...options,
      fields: { [key]: 1 } as any
    });
    return map(array, key);
  }

  async findOneGet<U = T, K extends keyof U = keyof U>(
    query: FilterQuery<T>,
    key: K,
    options?: FindOneOptions<U extends T ? T : U>
  ) {
    const document = await this.findOne(query, {
      ...options,
      fields: { [key]: 1 } as any
    });
    if (document === null)
      throw new Error(
        `Could not find document in collection <${this.name}>.findOneMap`
      );
    return get(document, key) as U[K];
  }

  // Insert methods :

  async insert(
    doc: InsertionDoc<T>,
    options?: CollectionInsertOneOptions
  ): Promise<WithId<T>>;

  async insert(
    docs: Array<InsertionDoc<T>>,
    options?: CollectionInsertManyOptions
  ): Promise<Array<WithId<T>>>;

  async insert(
    doc: InsertionDoc<T> | Array<InsertionDoc<T>>,
    options?: CollectionInsertOneOptions | CollectionInsertManyOptions
  ) {
    const col = await this.handle;
    if (!isArray(doc)) {
      const normalized = await normalizeInsertionDoc(this, doc);
      const result = await col.insertOne(normalized, options);
      return result.ops[0];
    } else {
      const normalized = await Promise.all(
        doc.map(doc => normalizeInsertionDoc(this, doc))
      );
      const result = await col.insertMany(normalized, options);
      return result.ops;
    }
  }
}
