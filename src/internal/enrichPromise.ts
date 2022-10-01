import { Document } from "mongodb";
import { assign, isString } from "lodash";
import {
  Collection,
  FindReferencesOptions,
  parseSelector,
  References,
  select,
  Selectable,
  SelectArgument,
  SelectionOption,
  Selector
} from "../.";

// Used as a contract for promises with advanced Rongo-specific methods

export type RichPromise<S extends Selectable<Document>> = Promise<S> & {
  select(selector: string | Selector, options?: SelectionOption): Promise<any>;
  select(
    chunks: TemplateStringsArray,
    ...args: Array<SelectArgument>
  ): Promise<any>;

  findReferences(options?: FindReferencesOptions): Promise<References>;
};

// Used to transform a promise into a rich promise

export function enrichPromise<T extends Document, S extends Selectable<T>>(
  collection: Collection<T>,
  promiseFactory: () => Promise<S>
) {
  const promise = promiseFactory();
  const richPromise: RichPromise<S> = assign(promise, {
    select(
      chunks: TemplateStringsArray | string | Selector,
      arg?: SelectArgument | SelectionOption | undefined,
      ...args: Array<SelectArgument>
    ) {
      let selector: Selector;
      let options: SelectionOption | undefined;
      if (isString(chunks)) {
        selector = parseSelector(chunks);
        options = arg as SelectionOption | undefined;
      } else if (chunks instanceof Selector) {
        selector = chunks;
        options = arg as SelectionOption | undefined;
      } else
        selector =
          arg === undefined ? select(chunks) : select(chunks, arg, ...args);
      return promise.then(selectable =>
        selector.resolve(selectable, collection, [], options)
      );
    },

    async findReferences(options?: FindReferencesOptions) {
      return collection.findReferences(
        await richPromise.select(collection.key),
        options
      );
    }
  });
  return richPromise;
}

// Workspace :

//////////////////////////////////////////////////////////////////////////////////////

type Space = " " | "\n" | ".";
type Stop = Space | ">" | "$" | "[" | "]" | "{" | "}" | "," | "?" | ":" | "*";

type FlatArray<T> = Array<T extends Array<infer U> ? U : T>;
type ParseError<T extends string = string> = { _error: T };

type EatSpace<Input extends string> = Input extends `${Space}${infer Rest}`
  ? EatSpace<Rest>
  : Input;

type EatIdentifier<
  Input extends string,
  Id extends string = ""
> = Input extends `${infer Char}${infer Rest}`
  ? Char extends Stop
    ? Id extends ""
      ? ParseError<`${Input} doesn't contain an identifier`>
      : [Id, Input]
    : EatIdentifier<Rest, `${Id}${Char}`>
  : Id extends ""
  ? ParseError<`Empty string doesn't contain an identifier`>
  : [Id, Input];

type ProcessFieldOrIndex<
  Type,
  Input extends string,
  Id extends string,
  Rest extends string
> = Type extends Array<any>
  ? ParseExpr<Type, `$ ${Input}`>
  : Id extends keyof Type
  ? ParseExpr<Type[Id], Rest>
  : ParseError<`Can't resolve field or index [${Id}]`>;

type ProcessShortcut<Type, Input extends string> = ParseExpr<
  Exclude<Type, null | undefined>,
  Input
> extends infer Result
  ? Result extends ParseError
    ? Result
    : Result extends [infer Value, infer Rest]
    ? [Extract<Type, null | undefined> | Value, Rest]
    : never
  : never;

type ProcessMap<
  Type,
  Rest extends string,
  Hard extends boolean = false
> = Type extends Array<infer U>
  ? ParseExpr<U, Rest> extends infer Result
    ? Result extends ParseError
      ? Result
      : Result extends [infer Value, infer Rest]
      ? [Hard extends true ? Array<Value> : FlatArray<Value>, Rest]
      : never
    : never
  : ParseError<`Can't ${Hard extends true
      ? "hard-"
      : ""}map a non-array value`>;

type ParseTupleSelector<
  Type,
  Input extends string,
  Tuple extends Array<any> = []
> = ParseExpr<Type, Input> extends infer Result
  ? Result extends ParseError
    ? Result
    : Result extends [infer Value, `${infer Rest}`]
    ? EatSpace<Rest> extends `,${infer Rest}`
      ? ParseTupleSelector<Type, Rest, [...Tuple, Value]>
      : EatSpace<Rest> extends `]${infer Rest}`
      ? [[...Tuple, Value], Rest]
      : ParseError<"Missing tuple closing">
    : never
  : never;

type ParseObjectSelector<
  Type,
  Input extends string,
  Obj extends Record<string, any> = {}
> = Type extends Array<any>
  ? ParseExpr<Type, `$ ${Input}`>
  : Type extends object
  ? EatSpace<Input> extends `${infer Input}`
    ? EatIdentifier<Input> extends [`${infer Id}`, `${infer Rest}`]
      ? Id extends keyof Type
        ? ParseExpr<Type[Id], Rest> extends infer Result
          ? Result extends ParseError
            ? Result
            : Result extends [infer Value, `${infer Rest}`]
            ? EatSpace<Rest> extends `,${infer Rest}`
              ? ParseObjectSelector<Type, Rest, Obj & { [k in Id]: Value }>
              : EatSpace<Rest> extends `}${infer Rest}`
              ? [Obj & { [k in Id]: Value }, Rest]
              : ParseError<"Missing object closing">
            : never
          : never
        : ParseError<`Can't resolve field or index [${Id}]`>
      : Input extends `*${infer Rest}`
      ? ParseExpr<Type[keyof Type], Rest> extends infer Result
        ? Result extends ParseError
          ? Result
          : Result extends [infer Value, `${infer Rest}`]
          ? EatSpace<Rest> extends `,${infer Rest}`
            ? ParseObjectSelector<
                Type,
                Rest,
                Obj & { [k in keyof Type]: Value }
              >
            : EatSpace<Rest> extends `}${infer Rest}`
            ? [Obj & { [k in keyof Type]: Value }, Rest]
            : ParseError<"Missing object closing">
          : never
        : never
      : ParseError<"Object subselections must start with a field or wildcard selector">
    : never
  : ParseError<"Can't resolve object selector in primitive value">;

type ParseExpr<
  Type,
  Input extends string
> = EatSpace<Input> extends `${infer Input}`
  ? EatIdentifier<Input> extends [`${infer Id}`, `${infer Rest}`] // Is field or index selector
    ? ProcessFieldOrIndex<Type, Input, Id, Rest>
    : Input extends `>${infer Rest}` // Is shortcut selector
    ? ProcessShortcut<Type, Rest>
    : Input extends `$$${infer Rest}` // Is hardmap selector
    ? ProcessMap<Type, Rest, true>
    : Input extends `$${infer Rest}` // Is map selector
    ? ProcessMap<Type, Rest>
    : Input extends `[${infer Rest}` // Is tuple selector
    ? ParseTupleSelector<Type, Rest>
    : Input extends `{${infer Rest}` // Is object selector
    ? ParseObjectSelector<Type, Rest>
    : [Type, Input] // Is identity selector
  : never;

type ParseSelector<Type, Input extends string> = ParseExpr<
  Type,
  Input
> extends infer Result
  ? Result extends ParseError
    ? Result
    : Result extends [infer Value, `${infer Rest}`]
    ? EatSpace<Rest> extends ""
      ? Value
      : ParseError<"Incomplete selector parsing">
    : never
  : never;

type C = ParseSelector<{ a: string; b: number }, "{*}">;

type D = ParseSelector<
  { a: { b: Array<{ c: boolean }> } | null; d: number },
  "[a> b c ]  "
>;

type E = ParseSelector<{ a: { b: { c: boolean } } | null }, "a > b c">;

type F = ParseSelector<Array<{ a: Array<{ b: boolean }> }>, " a b">;
