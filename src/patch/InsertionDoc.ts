import { OptionalId, Document, WithoutId } from "mongodb";
import { ObjectId } from "../.";

export type InsertionDoc<T extends Document> = InsertionDocPatch<OptionalId<T>>;
export type UpdateDoc<T extends Document> = InsertionDocPatch<WithoutId<T>>;

export type InsertionDocPatch<T extends Document> = {
  [K in keyof T]: PrimitivePatch<T[K]>;
};

export type PrimitivePatch<T> = T extends object
  ? T extends ObjectId
    ? ObjectId | InsertionDoc<any>
    : InsertionDocPatch<T>
  : T | InsertionDoc<any>;
