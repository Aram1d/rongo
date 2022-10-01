import { WithId, Document } from "mongodb";

// Used to store the collection dependencies in an optimally exploitable manner

export type Graph = {
  [collection: string]: CollectionConfig;
};

export type CollectionConfig = {
  key: string;
  foreignKeys: {
    [foreignKey: string]: ForeignKeyConfig;
  };
  references: {
    [collection: string]: {
      [foreignKey: string]: ForeignKeyConfig;
    };
  };
};

export type ForeignKeyConfig = {
  path: Path;
  updater: [string, string | null] | null;
  collection: string;
  onInsert: InsertPolicy;
  onDelete: DeletePolicy;
};

// Used to express the collection dependencies in a user-friendly way

export type Schema = {
  [collection: string]: {
    key?: string;
    foreignKeys?: {
      [foreignKeyPath: string]: {
        collection?: string;
        onInsert?: InsertPolicy;
        onDelete?: DeletePolicy;
      };
    };
  };
};

// The actions one can apply when documents are inserted

export enum InsertPolicy {
  Bypass = "BYPASS",
  Verify = "VERIFY"
}

// The actions one can apply when documents are deleted

export enum DeletePolicy {
  Bypass = "BYPASS",
  Reject = "REJECT", // *
  Delete = "DELETE", // *
  Unset = "UNSET", // *
  Nullify = "NULLIFY", // *
  Pull = "PULL" // x.**.$.**
}

// The general type constraint for documents

//export type Document = object;

// The general type constraint for graph-documents

export type GraphDocument = object;

// Used to tell TypeScript about primary keys

export type PrimaryKey<T> = {
  _primaryKey: true;
  type: T;
};

// Used to tell TypeScript about foreign keys

export type ForeignKey<T extends GraphDocument> = {
  _foreignKey: true;
  document: T;
};

// Used to locate a key with precision

export type Path = Array<string>;

// Used to type-check selectable resource

type SelectableDocumentAtom<T extends Document> = T | WithId<T>;

type SelectableDocument<T extends Document> =
  | SelectableDocumentAtom<T>
  | Partial<SelectableDocumentAtom<T>>;

export type Selectable<T extends Document> =
  | null
  | undefined
  | SelectableDocument<T>
  | Array<SelectableDocument<T>>;
