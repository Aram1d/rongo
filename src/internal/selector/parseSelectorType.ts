import {
  DocumentOf,
  ForeignKey,
  ObjectId,
  PrimaryKey,
  PrimaryKeyOf
} from "../../.";

// Tests :

type AuthorDb = {
  _id: PrimaryKey<ObjectId>;
  name: string;
  favoriteBooks: Array<ForeignKey<BookDb>>;
};

type BookDb = {
  _id: PrimaryKey<ObjectId>;
  title: string;
  author: ForeignKey<AuthorDb>;
};

type Expect<Test extends Check, Check> = [Check] extends [Test] ? true : false;

type Test = Expect<ParseSelector<BookDb, " .\t title \v ">, string> &
  Expect<ParseSelector<AuthorDb, " .\n ">, DocumentOf<AuthorDb>> &
  Expect<ParseSelector<BookDb, "author">, DocumentOf<AuthorDb>> &
  Expect<ParseSelector<AuthorDb, "favoriteBooks 1 title">, string> &
  Expect<ParseSelector<{ a: AuthorDb }, "a name">, string> &
  Expect<ParseSelector<Array<BookDb>, "12">, DocumentOf<BookDb>> &
  Expect<ParseSelector<BookDb | null, " >.title ">, string | null> &
  Expect<
    ParseSelector<{ a?: Array<BookDb> }, " a > title">,
    undefined | string[]
  > &
  Expect<
    ParseSelector<{ field: Array<ForeignKey<AuthorDb> | null> }, "field">,
    Array<DocumentOf<AuthorDb> | null>
  > &
  Expect<
    ParseSelector<{ a: Array<BookDb | null> }, " a $> title">,
    Array<string | null>
  > &
  Expect<
    ParseSelector<{ a: Array<BookDb | null> }, " a > title">,
    ParseError<`Can't resolve field <title> in primitive value`>
  > &
  Expect<
    ParseSelector<BookDb, string>,
    ParseError<"Only string literal types can be parsed">
  > &
  Expect<
    ParseSelector<BookDb | null, "title">,
    ParseError<"Can't resolve field <title> in primitive value">
  > &
  Expect<
    ParseSelector<Array<AuthorDb | null>, " 0 > favoriteBooks ">,
    Array<DocumentOf<BookDb>> | null
  > &
  Expect<
    ParseSelector<BookDb, " title . x">,
    ParseError<`Can't resolve field <x> in primitive value`>
  > &
  Expect<
    ParseSelector<BookDb, "titlez">,
    ParseError<`Can't resolve field <titlez> in object, no such property`>
  > &
  Expect<
    ParseSelector<{ b: BookDb }, "b.titlez">,
    ParseError<`Can't resolve field <titlez> in object, no such property`>
  > &
  Expect<
    ParseSelector<Array<BookDb> | BookDb, "title">,
    ParseError<"Can't resolve field <title> in object, no such property">
  > &
  Expect<
    ParseSelector<BookDb, "12">,
    ParseError<`Can't resolve index <12> in non-array value`>
  > &
  Expect<
    ParseSelector<{ x: number }, "x ]">,
    ParseError<"Incomplete selector parsing, please refer to the syntax specification in the doc">
  >;

// Main parser types :

export type ParseSelector<Type, Input extends string> = string extends Input
  ? ParseError<"Only string literal types can be parsed">
  : ParseExpr<Type, Input> extends infer Result
  ? Result extends ParseError
    ? Result
    : Result extends [infer OutType, `${infer Input}`]
    ? EatSpace<Input> extends ""
      ? DocumentOf<OutType>
      : ParseError<"Incomplete selector parsing, please refer to the syntax specification in the doc">
    : never
  : never;

type ParseExpr<
  Type,
  Input extends string
> = ResolveType<Type> extends infer Type
  ? EatSpace<Input> extends `${infer Input}`
    ? EatField<Input> extends [`${infer Field}`, `${infer Input}`]
      ? ParseField<Type, Input, Field>
      : EatIndex<Input> extends [`${infer Index}`, `${infer Input}`]
      ? ParseIndex<Type, Input, Index>
      : Input extends `>${infer Input}`
      ? ParseShortcut<Type, Input>
      : Input extends `$$${infer Input}`
      ? ParseMap<Type, Input, true>
      : Input extends `$${infer Input}`
      ? ParseMap<Type, Input>
      : Input extends `[${infer Input}`
      ? ParseTuple<Type, Input>
      : Input extends `{${infer Input}`
      ? ParseObject<Type, Input>
      : [Type, Input]
    : never
  : never;

type ParseField<Type, Input extends string, Field extends string> = [
  Type
] extends [Array<any>]
  ? ParseExpr<Type, `$ ${Field} ${Input}`>
  : [Type] extends [object]
  ? Field extends keyof Type
    ? ParseExpr<Type[Field], Input>
    : ParseError<`Can't resolve field <${Field}> in object, no such property`>
  : ParseError<`Can't resolve field <${Field}> in primitive value`>;

type ParseIndex<Type, Input extends string, Index extends string> = [
  Type
] extends [Array<any>]
  ? ParseExpr<Type[number], Input>
  : ParseError<`Can't resolve index <${Index}> in non-array value`>;

type ParseShortcut<Type, Input extends string> = ParseExpr<
  Exclude<Type, null | undefined>,
  Input
> extends infer Result
  ? Result extends ParseError
    ? Result
    : Result extends [infer OutType, infer Input]
    ? [Extract<Type, null | undefined> | OutType, Input]
    : never
  : never;

type ParseMap<Type, Input extends string, Hard extends boolean = false> = [
  Type
] extends [Array<infer Element>]
  ? ParseExpr<Element, Input> extends infer Result
    ? Result extends ParseError
      ? Result
      : Result extends [infer OutType, infer Input]
      ? [Hard extends true ? Array<OutType> : FlatArray<OutType>, Input]
      : never
    : never
  : ParseError<"Can't map a non-array value">;

type ParseTuple<
  Type,
  Input extends string,
  Tuple extends Array<any> = []
> = ParseExpr<Type, Input> extends infer Result
  ? Result extends ParseError
    ? Result
    : Result extends [infer OutType, `${infer Input}`]
    ? EatSpace<Input> extends `${infer Input}`
      ? Input extends `,${infer Input}`
        ? ParseTuple<Type, Input, [...Tuple, OutType]>
        : Input extends `]${infer Input}`
        ? [[...Tuple, OutType], Input]
        : ParseError<"Unexpected character, expected <,> or <]>">
      : never
    : never
  : never;

type ParseObject<
  Type,
  Input extends string,
  Obj extends Record<string, any> = {}
> = ParseError<"Not implemented yet">;

// "Eating" types :

type EatSpace<Input extends string> = Input extends `${
  | Space
  | "."}${infer Rest}`
  ? EatSpace<Rest>
  : Input;

type EatField<Input extends string> = EatOneOrMore<
  Input,
  Alpha | Dash,
  Alpha | Dash | Digit
>;

type EatIndex<Input extends string> = EatOneOrMore<Input, Digit>;

// Helpers :

type FlatArray<Element> = Array<
  Element extends Array<infer Element> ? Element : Element
>;

type ParseError<Message extends string = string> = {
  _error: Message;
};

type ResolveType<
  Type,
  Resolve extends boolean = true
> = Type extends PrimaryKey<infer Type>
  ? Type
  : Type extends Array<infer Element>
  ? Array<ResolveType<Element>>
  : Type extends ForeignKey<infer Document>
  ? Resolve extends true
    ? Document
    : PrimaryKeyOf<Document>
  : Type;

type EatOneOrMore<
  Input extends string,
  HeadMatcher extends string,
  TailMatcher extends string = HeadMatcher
> = Input extends `${infer Char}${infer Rest}`
  ? Char extends HeadMatcher
    ? EatOneOrMoreRest<Rest, TailMatcher, Char>
    : ParseError
  : ParseError;

type EatOneOrMoreRest<
  Input extends string,
  TailMatcher extends string,
  Acc extends string
> = Input extends `${infer Char}${infer Rest}`
  ? Char extends TailMatcher
    ? EatOneOrMoreRest<Rest, TailMatcher, `${Acc}${Char}`>
    : [Acc, Input]
  : [Acc, Input];

// Some character primitives :

type Dash = "-" | "_";

type Space = " " | "\n" | "\t" | "\v" | "\r" | "\f";

type Digit = "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9";

type Alpha =
  | "a"
  | "b"
  | "c"
  | "d"
  | "e"
  | "f"
  | "g"
  | "h"
  | "i"
  | "j"
  | "k"
  | "l"
  | "m"
  | "n"
  | "o"
  | "p"
  | "q"
  | "r"
  | "s"
  | "t"
  | "u"
  | "v"
  | "w"
  | "x"
  | "y"
  | "z"
  | "A"
  | "B"
  | "C"
  | "D"
  | "E"
  | "F"
  | "G"
  | "H"
  | "I"
  | "J"
  | "K"
  | "L"
  | "M"
  | "N"
  | "O"
  | "P"
  | "Q"
  | "R"
  | "S"
  | "T"
  | "U"
  | "V"
  | "W"
  | "X"
  | "Y"
  | "Z";
