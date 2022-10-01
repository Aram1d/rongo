import {
  AggregateOptions,
  BulkWriteOptions,
  ChangeStreamOptions,
  ClientSession,
  Collection as Col,
  CollectionOptions,
  CollStatsOptions,
  CountDocumentsOptions,
  CreateIndexesOptions,
  DeleteOptions,
  DistinctOptions,
  DropCollectionOptions,
  DropIndexesOptions,
  Document,
  FindOneAndReplaceOptions,
  FindOneAndUpdateOptions,
  FindOptions,
  IndexDescription,
  IndexInformationOptions,
  ListIndexesOptions,
  OperationOptions,
  ReplaceOptions,
  UpdateFilter,
  UpdateOptions,
  WithId
} from "mongodb";
import {
  createDefaultConfig,
  DeletedKeys,
  DependencyCollector,
  enrichPromise,
  Filter,
  findReferences,
  FindReferencesOptions,
  InsertionDoc,
  insertSafely,
  normalizeFilterQuery,
  normalizeInsertionDoc,
  propagateDelete,
  RemoveScheduler,
  RichPromise,
  Rongo,
  Selectable,
  UpdateDoc
} from ".";

// The Collection class

export class Collection<T extends Document = Document> {
  readonly name: string;
  readonly rongo: Rongo;
  readonly handle: Promise<Col<T>>;

  constructor(rongo: Rongo, name: string, options: CollectionOptions = {}) {
    this.name = name;
    this.rongo = rongo;
    this.handle = rongo.handle.then(db => db.collection<T>(name, options));
    if (!(name in rongo.graph)) rongo.graph[name] = createDefaultConfig();
  }

  // Meta data :

  get key() {
    return this.rongo.graph[this.name].key;
  }

  get foreignKeys() {
    return this.rongo.graph[this.name].foreignKeys;
  }

  get references() {
    return this.rongo.graph[this.name].references;
  }

  // Document inspection method :

  from<S extends Selectable<T>>(selectable: S | Promise<S>) {
    return enrichPromise(this, async () => selectable);
  }

  // Query methods :

  async aggregate<U extends Document>(
    pipeline: Array<any> = [],
    options?: AggregateOptions & { baseQuery?: boolean }
  ) {
    const col = await this.handle;
    const [first, ...stages] = pipeline;
    if (first?.$match && !options?.baseQuery)
      pipeline = [
        { $match: await normalizeFilterQuery(this, first.$match) },
        ...stages
      ];
    return col.aggregate<U>(pipeline, options).toArray();
  }

  async count(
    query: Filter<T> = {},
    options?: CountDocumentsOptions & { baseQuery?: boolean }
  ) {
    const col = await this.handle;
    const normalized = await normalizeFilterQuery(this, query, options);
    return options
      ? col.countDocuments(normalized, options)
      : col.countDocuments(normalized);
  }

  countByKeys(keys: Array<any>, options?: CountDocumentsOptions) {
    return this.count({ [this.key]: { $in: keys } } as Filter<T>, {
      ...options,
      baseQuery: true
    });
  }

  async distinct(
    key: string,
    query: Filter<T> = {},
    options?: DistinctOptions & { baseQuery?: boolean }
  ) {
    const col = await this.handle;
    const normalized = await normalizeFilterQuery(this, query, options);
    return options
      ? col.distinct(key, normalized, options)
      : col.distinct(key, normalized);
  }

  find(
    query: Filter<T> = {},
    options?: FindOptions<T> & { baseQuery?: boolean }
  ) {
    return enrichPromise(this, async () => {
      const col = await this.handle;
      const normalized = await normalizeFilterQuery(this, query, options);
      return col.find(normalized, options as any).toArray();
    });
  }

  findOne(
    query: Filter<T> = {},
    options?: FindOptions<T> & { baseQuery?: boolean }
  ) {
    return enrichPromise(this, async () => {
      const col = await this.handle;
      const normalized = await normalizeFilterQuery(this, query, options);
      return col.findOne(normalized, options as object);
    });
  }

  findByKey(key: any, options?: FindOptions<T>) {
    return this.findOne({ [this.key]: key } as Filter<T>, {
      ...options,
      baseQuery: true
    });
  }

  findByKeys(keys: Array<any>, options?: FindOptions<T>) {
    return this.find({ [this.key]: { $in: keys } } as Filter<T>, {
      ...options,
      baseQuery: true
    });
  }

  findReferences(key: any | Array<any>, options?: FindReferencesOptions) {
    return findReferences(this, key, options);
  }

  async has(query: Filter<T> = {}, options?: { baseQuery?: boolean }) {
    return Boolean(await this.count(query, { ...options, limit: 1 }));
  }

  async hasKey(key: any) {
    return this.has({ [this.key]: key } as Filter<T>, { baseQuery: true });
  }

  async hasKeys(keys: Array<any>, options?: { some?: boolean }) {
    const count = await this.count({ [this.key]: { $in: keys } } as Filter<T>, {
      baseQuery: true,
      ...(options?.some && { limit: 1 })
    });
    return count === (options?.some ? 1 : keys.length);
  }

  async isCapped(options?: OperationOptions) {
    const col = await this.handle;
    return options ? col.isCapped(options) : col.isCapped();
  }

  async stats(options?: CollStatsOptions) {
    const col = await this.handle;
    return options ? col.stats(options) : col.stats();
  }

  async watch<U extends Document>(
    pipeline?: object[],
    options?: ChangeStreamOptions & { session?: ClientSession }
  ) {
    const col = await this.handle;
    return col.watch<U>(pipeline, options);
  }

  // Insert method :

  insert(
    doc: InsertionDoc<T>,
    options?: BulkWriteOptions & { baseDocument?: boolean }
  ): RichPromise<WithId<T>>;

  insert(
    docs: Array<InsertionDoc<T>>,
    options?: BulkWriteOptions & { baseDocument?: boolean }
  ): RichPromise<Array<WithId<T>>>;

  insert(
    doc: InsertionDoc<T> | Array<InsertionDoc<T>>,
    options?: BulkWriteOptions & { baseDocument?: boolean }
  ) {
    return enrichPromise(this, async () => {
      const dependencies = new DependencyCollector(this.rongo);
      try {
        return insertSafely(this, doc, dependencies, options);
      } catch (e) {
        await dependencies.delete();
        throw e;
      }
    });
  }

  // Replace methods :

  async replaceOne(
    query: Filter<T>,
    doc: UpdateDoc<T>,
    options?: ReplaceOptions & {
      baseQuery?: boolean;
      baseDocument?: boolean;
    }
  ) {
    const col = await this.handle;
    const normalizedQuery = await normalizeFilterQuery(this, query, options);
    const dependencies = new DependencyCollector(this.rongo);
    try {
      const normalizedDoc = await normalizeInsertionDoc(
        this,
        doc,
        dependencies,
        options
      );
      return options
        ? col.replaceOne(normalizedQuery, normalizedDoc, options)
        : col.replaceOne(normalizedQuery, normalizedDoc);
    } catch (e) {
      await dependencies.delete();
      throw e;
    }
  }

  replaceByKey(
    key: any,
    doc: UpdateDoc<T>,
    options?: ReplaceOptions & { baseDocument?: boolean }
  ) {
    return this.replaceOne({ [this.key]: key } as Filter<T>, doc, {
      ...options,
      baseQuery: true
    });
  }

  findOneAndReplace(
    query: Filter<T>,
    doc: UpdateDoc<T>,
    options?: FindOneAndReplaceOptions & {
      baseQuery?: boolean;
      baseDocument?: boolean;
    }
  ) {
    return enrichPromise(this, async () => {
      const col = await this.handle;
      const normalizedQuery = await normalizeFilterQuery(this, query, options);
      const dependencies = new DependencyCollector(this.rongo);
      try {
        const normalizedDoc = await normalizeInsertionDoc(
          this,
          doc,
          dependencies,
          options
        );

        const result = options
          ? await col.findOneAndReplace(normalizedQuery, normalizedDoc, options)
          : await col.findOneAndReplace(normalizedQuery, normalizedDoc);
        return result.value === undefined ? null : result.value;
      } catch (e) {
        await dependencies.delete();
        throw e;
      }
    });
  }

  findByKeyAndReplace(
    key: any,
    doc: UpdateDoc<T>,
    options?: FindOneAndReplaceOptions & { baseDocument?: boolean }
  ) {
    return this.findOneAndReplace({ [this.key]: key } as Filter<T>, doc, {
      ...options,
      baseQuery: true
    });
  }

  // Update methods :

  async update(
    query: Filter<T>,
    update: UpdateFilter<T> | Partial<T>,
    options?: UpdateOptions & { multi?: boolean; baseQuery?: boolean }
  ) {
    const col = await this.handle;
    const normalized = await normalizeFilterQuery(this, query, options);

    return options
      ? col[options?.multi ? "updateMany" : "updateOne"](
          normalized,
          update,
          options
        )
      : col.updateOne(normalized, update);
  }

  updateByKey(
    key: any,
    update: UpdateFilter<T> | Partial<T>,
    options?: UpdateOptions
  ) {
    return this.update({ [this.key]: key } as Filter<T>, update, {
      ...options,
      baseQuery: true
    });
  }

  updateByKeys(
    keys: Array<any>,
    update: UpdateFilter<T> | Partial<T>,
    options?: UpdateOptions
  ) {
    return this.update({ [this.key]: { $in: keys } } as Filter<T>, update, {
      ...options,
      multi: true,
      baseQuery: true
    });
  }

  findOneAndUpdate(
    query: Filter<T>,
    update: UpdateFilter<T> | T,
    options?: FindOneAndUpdateOptions & { baseQuery?: boolean }
  ) {
    return enrichPromise(this, async () => {
      const col = await this.handle;
      const normalized = await normalizeFilterQuery(this, query, options);
      const result = options
        ? await col.findOneAndUpdate(normalized, update, options)
        : await col.findOneAndUpdate(normalized, update);
      return result.value === undefined ? null : result.value;
    });
  }

  findByKeyAndUpdate(
    key: any,
    update: Filter<T> | T,
    options?: FindOneAndUpdateOptions
  ) {
    return this.findOneAndUpdate({ [this.key]: key } as Filter<T>, update, {
      ...options,
      baseQuery: true
    });
  }

  findByKeysAndUpdate(
    keys: Array<any>,
    update: Filter<T> | T,
    options?: UpdateOptions
  ) {
    return enrichPromise(this, async () => {
      const docs = await this.findByKeys(keys, options);
      await this.updateByKeys(keys, update, options);
      return docs;
    });
  }

  // Delete methods :

  async delete(
    query: Filter<T> = {},
    options?: DeleteOptions & {
      single?: boolean;
      propagate?: boolean;
      baseQuery?: boolean;
    }
  ) {
    const normalized = await normalizeFilterQuery(this, query, options);
    const scheduler: RemoveScheduler = [];
    const deletedKeys: DeletedKeys = Object.create(null);
    const remover = await propagateDelete(
      this,
      normalized,
      options?.single ?? false,
      options,
      scheduler,
      deletedKeys
    );
    for (const task of scheduler) await task();
    return remover();
  }

  deleteByKey(key: any, options?: DeleteOptions & { propagate?: boolean }) {
    return this.delete({ [this.key]: key } as Filter<T>, {
      ...options,
      single: true,
      baseQuery: true
    });
  }

  deleteByKeys(
    keys: Array<any>,
    options?: DeleteOptions & { propagate?: boolean }
  ) {
    return this.delete({ [this.key]: { $in: keys } } as Filter<T>, {
      ...options,
      baseQuery: true
    });
  }

  async drop(options?: DropCollectionOptions) {
    const col = await this.handle;
    await this.delete({}, { ...options, baseQuery: true });
    return options ? col.drop(options) : col.drop();
  }

  findOneAndDelete(
    query: Filter<T>,
    options?: DeleteOptions & { propagate?: boolean; baseQuery?: boolean }
  ) {
    return enrichPromise(this, async () => {
      const col = await this.handle;
      const normalized = await normalizeFilterQuery(this, query, options);
      const result = await col.findOne(normalized, options as object);
      await this.delete(normalized, {
        ...options,
        single: true,
        baseQuery: true
      });
      return result;
    });
  }

  findByKeyAndDelete(
    key: any,
    options?: DeleteOptions & { propagate?: boolean }
  ) {
    return this.findOneAndDelete({ [this.key]: key } as Filter<T>, {
      ...options,
      baseQuery: true
    });
  }

  findByKeysAndDelete(
    keys: Array<any>,
    options?: DeleteOptions & { propagate?: boolean }
  ) {
    return enrichPromise(this, async () => {
      const docs = await this.findByKeys(keys, options);
      await this.deleteByKeys(keys, options);
      return docs;
    });
  }

  // Index methods :

  async createIndex(fieldOrSpec: string | any, options?: CreateIndexesOptions) {
    const col = await this.handle;
    return options
      ? col.createIndex(fieldOrSpec, options)
      : col.createIndexes(fieldOrSpec);
  }

  async createIndexes(
    indexSpecs: IndexDescription[],
    options?: CreateIndexesOptions
  ) {
    const col = await this.handle;
    return options
      ? col.createIndexes(indexSpecs, options)
      : col.createIndexes(indexSpecs);
  }

  async dropIndex(indexName: string, options?: DropIndexesOptions) {
    const col = await this.handle;
    return options
      ? col.dropIndex(indexName, options)
      : col.dropIndex(indexName);
  }

  async dropIndexes(options?: DropIndexesOptions) {
    const col = await this.handle;
    return options ? col.dropIndexes(options) : col.dropIndexes();
  }

  async indexes(options?: IndexInformationOptions) {
    const col = await this.handle;
    return options ? col.indexes(options) : col.indexes();
  }

  async indexExists(
    indexes: string | string[],
    options?: IndexInformationOptions
  ) {
    const col = await this.handle;
    return options
      ? col.indexExists(indexes, options)
      : col.indexExists(indexes);
  }

  async indexInformation(options?: IndexInformationOptions) {
    const col = await this.handle;
    return options ? col.indexInformation(options) : col.indexInformation();
  }

  async listIndexes(options?: ListIndexesOptions) {
    const col = await this.handle;
    return col.listIndexes(options).toArray();
  }
}
