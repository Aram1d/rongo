import { flatten, isArray, isObject, keys } from "lodash";
import {
  Collection,
  FilterQuery,
  LazyDocuments,
  PredicateSelectorCallback,
  Stack,
  stackToKey
} from "../../.";

// This is the grammar of Selectors :
//
// selector:
// | <field> selector                        { FieldSelector(field, arg, selector) }
// | <index> selector                        { IndexSelector(index, selector) }
// | <$> selector                            { MapSelector(selector) }
// | <$$> selector                           { FlatMapSelector(selector) }
// | <arg as object> selector                { FilterSelector(arg, selector) }
// | <arg as function> selector              { PredicateSelector(arg, selector) }
// | <arg as selector>                       { arg }
// | <[> (selector <,>?)* <]>                { TupleSelector(...[selector]) }
// | <{> ((<field>|<*>) selector <,>?)* <}>  { ObjectSelector(...[field, selector]) }
// | <>                                      { IdentitySelector }
//
// spacing:
// | <[.\s]+>

// The Selector base class defines the abstract signature of a selector

export abstract class Selector {
  abstract select(
    value: any,
    collection: Collection<any>,
    stack: Stack
  ): Promise<any>;

  in(collection: Collection<any>) {
    return this.select(new LazyDocuments(collection, []), collection, []);
  }
}

// The IdentitySelector represents the "empty selector", it returns the value it was passed, unchanged ; It is
// mainly used to "end" selection chains with a non-optional "leaf" selector

export class IdentitySelector extends Selector {
  async select(value: any) {
    return value instanceof LazyDocuments ? value.fetch() : value;
  }
}

// The FieldSelector selects a field in the current value ; if the current value is arrayish, it is first mapped

export class FieldSelector extends Selector {
  private readonly field: string;
  private readonly selector: Selector;

  constructor(field: string, selector: Selector) {
    super();
    this.field = field;
    this.selector = selector;
  }

  async select(
    value: any,
    collection: Collection<any>,
    stack: Stack
  ): Promise<any> {
    // If the current value is an array, insert implicit mapping :
    if (isArray(value) || value instanceof LazyDocuments)
      return new MapSelector(this).select(value, collection, stack);
    // Otherwise, it has to be an object :
    if (!isObject(value))
      throw new Error(
        `Can't resolve field <${this.field}> in primitive value <${value}>`
      );

    value = (value as any)[this.field];
    stack = [...stack, this.field];

    // If value represents foreign key(s), then...
    const foreignKeyConfig = collection.foreignKeys[stackToKey(stack)];
    if (foreignKeyConfig) {
      // If foreign key(s) is nullish and if it's legal, shortcut the selection from there with the nullish value :
      if (
        (foreignKeyConfig.optional && value === undefined) ||
        (foreignKeyConfig.nullable && value === null)
      )
        return value;

      // ...switch to foreign collection :
      collection = collection.rongo.collection(foreignKeyConfig.collection);
      // ...select foreign document(s) as current value :
      if (!isArray(value))
        value = await collection.findOne({ [collection.primaryKey]: value });
      else
        value = new LazyDocuments(collection, [
          { [collection.primaryKey]: { $in: value } }
        ]);
      // ...and reinitialize the stack :
      stack = [];
    }

    return this.selector.select(value, collection, stack);
  }
}

// The IndexSelector selects a precise item in the current array and continues subselection from there

export class IndexSelector extends Selector {
  private readonly index: number;
  private readonly selector: Selector;

  constructor(index: number, selector: Selector) {
    super();
    this.index = index;
    this.selector = selector;
  }

  async select(value: any, collection: Collection<any>, stack: Stack) {
    // If the current value if arrayish, simply access to the targeted item :
    if (isArray(value)) value = value[this.index];
    else if (value instanceof LazyDocuments)
      value = await value.fetchOne(this.index);
    else
      throw new Error(`Can't resolve index <${this.index}> in non-array value`);
    return this.selector.select(value, collection, [...stack, this.index]);
  }
}

// The MapSelector maps items in the current array through the subselector

export class MapSelector extends Selector {
  private readonly selector: Selector;

  constructor(selector: Selector) {
    super();
    this.selector = selector;
  }

  async select(value: any, collection: Collection<any>, stack: Stack) {
    // All items are needed, so if it's a lazy array of documents, fetch them :
    if (value instanceof LazyDocuments) value = await value.fetch();
    if (!isArray(value)) throw new Error("Can't map ($) a non-array value");
    // Map the items to subselections :
    return Promise.all(
      value.map((item, index) =>
        this.selector.select(item, collection, [...stack, index])
      )
    );
  }
}

// The MapSelector maps items in the current array through the subselector and flattens the result

export class FlatMapSelector extends Selector {
  private readonly selector: Selector;

  constructor(selector: Selector) {
    super();
    this.selector = selector;
  }

  async select(value: any, collection: Collection<any>, stack: Stack) {
    // All items are needed, so if it's a lazy array of documents, fetch them :
    if (value instanceof LazyDocuments) value = await value.fetch();
    if (!isArray(value))
      throw new Error("Can't flat-map ($$) a non-array value");
    // Flat-map the items to subselections :
    return flatten(
      await Promise.all(
        value.map((item, index) =>
          this.selector.select(item, collection, [...stack, index])
        )
      )
    );
  }
}

// The FilterSelector adds a filter query to the current lazy array of documents

export class FilterSelector extends Selector {
  private readonly query: FilterQuery<any>;
  private readonly selector: Selector;

  constructor(query: FilterQuery<any>, selector: Selector) {
    super();
    this.query = query;
    this.selector = selector;
  }

  async select(value: any, collection: Collection<any>, stack: Stack) {
    // Simply add a filter query to the current lazy array of documents and keep selecting from there :
    if (value instanceof LazyDocuments)
      return this.selector.select(value.extend(this.query), collection, stack);
    throw new Error(
      "Can't apply MongoDB filter query to non-lazy arrays of documents"
    );
  }
}

// The PredicateSelector filters the current array based on a predicate function

export class PredicateSelector extends Selector {
  private readonly predicate: PredicateSelectorCallback;
  private readonly selector: Selector;

  constructor(predicate: PredicateSelectorCallback, selector: Selector) {
    super();
    this.predicate = predicate;
    this.selector = selector;
  }

  async select(value: any, collection: Collection<any>, stack: Stack) {
    // All items are needed, so if it's a lazy array of documents, fetch them :
    if (value instanceof LazyDocuments) value = await value.fetch();
    if (!isArray(value))
      throw new Error("Can't apply a predicate filter to a non-array value");
    // Async version of Array.prototype.filter :
    value = await value.reduce<Promise<Array<any>>>(
      async (acc, item, index) =>
        (await this.predicate(item, index, value))
          ? [...(await acc), item]
          : acc,
      Promise.resolve([])
    );
    // Keep selection going on filtered array :
    return this.selector.select(value, collection, stack);
  }
}

// The TupleSelector creates a tuple by mapping parallel subselections to tuple items

export class TupleSelector extends Selector {
  private readonly selectors: Array<Selector>;

  constructor(selectors: Array<Selector>) {
    super();
    this.selectors = selectors;
  }

  async select(value: any, collection: Collection<any>, stack: Stack) {
    return Promise.all(
      this.selectors.map(selector => selector.select(value, collection, stack))
    );
  }
}

// The ObjectSelector creates an object by mapping the subselections to actual [key, value] pairs

export class ObjectSelector extends Selector {
  private readonly selectors: Array<[string, Selector]>;
  private readonly fields: Set<string>;

  constructor(selectors: Array<[string, Selector]>) {
    super();
    this.selectors = selectors;
    this.fields = new Set(selectors.map(([field]) => field));
  }

  async select(
    value: any,
    collection: Collection<any>,
    stack: Stack
  ): Promise<any> {
    // If the current value is an array, insert implicit mapping :
    if (isArray(value) || value instanceof LazyDocuments)
      return new MapSelector(this).select(value, collection, stack);
    // Otherwise, it has to be an object :
    if (!isObject(value))
      throw new Error(
        `Can't resolve object selector in primitive value <${value}>`
      );
    const result = Object.create(null);
    for (const [field, selector] of this.selectors)
      if (field !== "*") {
        // Each field definition adds a field to the result and a field-subselection from there on :
        result[field] = await new FieldSelector(field, selector).select(
          value,
          collection,
          stack
        );
      } else {
        // In the case of a wildcard field, do the same as above with the remaining keys in "value" :
        for (const field of keys(value))
          if (!this.fields.has(field))
            result[field] = await new FieldSelector(field, selector).select(
              value,
              collection,
              stack
            );
      }
    return result;
  }
}
