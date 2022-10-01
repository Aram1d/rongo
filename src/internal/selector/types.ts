import { Filter, Selector } from "../../.";

// Used by FilterSelector and SwitchSelector to filter array values and switch branches

export type SelectorPredicateCallback = (
  value: any,
  index: number, // Not present for SwitchSelector
  array: Array<any> // Not present for SwitchSelector
) => boolean | Promise<boolean>;

// The possible template literal arguments to the select builder :

export type SelectArgument =
  | string
  | number
  | Selector
  | Filter<any>
  | SelectorPredicateCallback;

// The possible entries in the symbol table

export type SelectSymbolEntry = Exclude<SelectArgument, string | number>;

// The selector parsing symbol table

export type SymTable = Map<string, SelectSymbolEntry>;

// These are the options you can parametrize selections with

export type SelectionOption = {
  resolveForeignKeys?: boolean;
};
