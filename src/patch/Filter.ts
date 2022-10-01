import {
  AlternativeType, BitwiseFilter, BSONType, BSONTypeAlias,
  Join,
  NestedPaths, NonObjectIdLikeDocument,
  PropertyType,
  RootFilterOperators,
  WithId
} from "mongodb";
import { BSONRegExp, Document } from "bson";

export declare interface FilterOperators<TValue> extends NonObjectIdLikeDocument {
  $eq?: TValue;
  $gt?: TValue;
  $gte?: TValue;
  $in?: ReadonlyArray<TValue | Filter<any>> | Filter<any>; // PATCHED
  $lt?: TValue;
  $lte?: TValue;
  $ne?: TValue;
  $nin?: ReadonlyArray<TValue | Filter<any>> | Filter<any>; // PATCHED
  $not?: TValue extends string ? FilterOperators<TValue> | RegExp : FilterOperators<TValue>;
  /**
   * When `true`, `$exists` matches the documents that contain the field,
   * including documents where the field value is null.
   */
  $exists?: boolean;
  $type?: BSONType | BSONTypeAlias;
  $expr?: Record<string, any>;
  $jsonSchema?: Record<string, any>;
  $mod?: TValue extends number ? [number, number] : never;
  $regex?: TValue extends string ? RegExp | BSONRegExp | string : never;
  $options?: TValue extends string ? string : never;
  $geoIntersects?: {
    $geometry: Document;
  };
  $geoWithin?: Document;
  $near?: Document;
  $nearSphere?: Document;
  $maxDistance?: number;
  $all?: ReadonlyArray<any>;
  $elemMatch?: Document;
  $size?: TValue extends ReadonlyArray<any> ? number : never;
  $bitsAllClear?: BitwiseFilter;
  $bitsAllSet?: BitwiseFilter;
  $bitsAnyClear?: BitwiseFilter;
  $bitsAnySet?: BitwiseFilter;
  $rand?: Record<string, never>;
}

export declare type Condition<T> = AlternativeType<T> | FilterOperators<AlternativeType<T>>;

export declare type Filter<TSchema> = Partial<TSchema> | ({
  [Property in Join<NestedPaths<WithId<TSchema>>, '.'>]?: Condition<PropertyType<WithId<TSchema>, Property>>;
} & RootFilterOperators<WithId<TSchema>>);