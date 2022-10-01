import {
  AddUserOptions,
  ChangeStreamOptions,
  ClientSession, CollectionOptions,
  CreateCollectionOptions,
  Db,
  DbStatsOptions, ListCollectionsOptions,
  MongoClient,
  MongoClientOptions, RemoveUserOptions,
  RunCommandOptions,
  Document
} from "mongodb";
import {
  buildGraph,
  Collection,
  findDanglingKeys,
  Graph,
  ObjectId,
  Schema
} from ".";

// The Rongo class

export class Rongo {
  readonly client: Promise<MongoClient>;
  readonly handle: Promise<Db>;
  graph: Graph;

  constructor(
    uri: string | Promise<string>,
    {
      schema,
      ...options
    }: MongoClientOptions & { schema?: Schema | string } = {}
  ) {
    this.client = Promise.resolve(uri).then(uri =>
      MongoClient.connect(uri, {
        ...options
      })
    );
    this.handle = this.client.then(client => {
      const dbName = (client as any).s.options.dbName;
      if (!dbName)
        throw new Error("The connection uri must contain a database name");
      return client.db(dbName);
    });
    this.graph = Object.create(null);
    if (schema) this.schema(schema);
  }

  // Client methods :

  active() {
    return this.client;
  }

  async close() {
    const client = await this.client;
    await client.close();
  }

  // Database methods :

  async addUser(
    username: string,
    password: string,
    options?: AddUserOptions
  ) {
    const db = await this.handle;
    return options ? db.addUser(username, password, options) : db.addUser(username, password);
  }

  findDanglingKeys(options?: { batchSize?: number; limit?: number }) {
    return findDanglingKeys(this, options);
  }

  collection<T extends Document = Document>(name: string, options?: CollectionOptions) {
    return new Collection<T>(this, name, options);
  }

  async command(
    command: Document,
    options?: RunCommandOptions,
  ) {
    const db = await this.handle;
    return options ? db.command(command, options): db.command(command);
  }

  async createCollection<T extends Document>(
    name: string,
    options?: CreateCollectionOptions
  ) {
    const db = await this.handle;
    await db.createCollection(name, options);
    return this.collection<T>(name, options);
  }

  async drop() {
    const db = await this.handle;
    return db.dropDatabase();
  }

/*  async executeDbAdminCommand(
    command: object,
    options?: { readPreference?: ReadPreferenceOrMode; session?: ClientSession }
  ) {
    const db = await this.handle;
    return db.executeDbAdminCommand(command, options);
  }*/

  async listCollections(
    filter?: object,
    options?: Exclude<ListCollectionsOptions, 'nameOnly'> & {
      nameOnly: true;
    }
  ) {
    const db = await this.handle;
    return db.listCollections(filter, options).toArray();
  }

  async removeUser(username: string, options?: RemoveUserOptions) {
    const db = await this.handle;
    return options ? db.removeUser(username, options) : db.removeUser(username);
  }

  schema(schema: Schema | string) {
    this.graph = buildGraph(schema);
  }

  async stats(options?: DbStatsOptions) {
    const db = await this.handle;
    return options ?  db.stats(options) : db.stats();
  }

  async watch<T extends object = { _id: ObjectId }>(
    pipeline?: object[],
    options?: ChangeStreamOptions & { session?: ClientSession }
  ) {
    const db = await this.handle;
    return db.watch<T>(pipeline, options);
  }
}
