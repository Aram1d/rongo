import { Db, MongoClient } from "mongodb";
import {
  FieldSelector,
  FilterQuerySelector,
  FilterSelector,
  HardMapSelector,
  IdentitySelector,
  IndexSelector,
  MapSelector,
  ObjectSelector,
  Rongo,
  select,
  Selector,
  ShortcutSelector,
  SwitchSelector,
  TupleSelector
} from "../.";
import {
  Author,
  AuthorDb,
  Book,
  BookDb,
  graph,
  rongo,
  testSchema
} from "./samples";

it("correctly loads and parses YAML and JSON configuration files", async () => {
  const rongoYml = new Rongo("mongodb://localhost:27017/rongo_test");
  rongoYml.schema("./src/test/schema.test.yaml");
  expect(rongo.graph).toEqual(rongoYml.graph);
  expect(rongo.graph).toEqual(graph);
  await rongoYml.close();

  const rongoInl = new Rongo("mongodb://localhost:27017/rongo_test", {
    schema: testSchema
  });
  expect(rongo.graph).toEqual(rongoInl.graph);
  await rongoInl.close();
});

it("correctly connects to the database", async () => {
  expect(await rongo.active()).toBeTruthy();
  expect(await rongo.active()).toBeInstanceOf(MongoClient);
  expect(await rongo.handle).toBeInstanceOf(Db);
  await rongo.drop();
});

it("correctly inserts nested documents", async () => {
  const [teamOfRivals] = await Book.insert([
    {
      title: "Team of Rivals",
      author: {
        name: "Doris Kearns Goodwin",
        favoriteBooks: []
      }
    }
  ]);

  await Book.insert({
    title: "Harry Potter",
    author: {
      name: "J.K. Rowling",
      favoriteBooks: [
        teamOfRivals._id,
        {
          title: "Emma",
          author: {
            name: "Jane Austen",
            favoriteBooks: []
          }
        }
      ]
    }
  });

  const rowling = (await Author.findOne({ name: "J.K. Rowling" })) as AuthorDb;
  const janeAusten = (await Author.findOne({
    name: "Jane Austen"
  })) as AuthorDb;
  const emma = (await Book.findOne({ title: "Emma" })) as BookDb;

  expect(await Book.count()).toBe(3);
  expect(await Author.count()).toBe(3);
  expect(emma.author.toHexString()).toBe(janeAusten._id.toHexString());
  expect(rowling.favoriteBooks.map(id => id.toHexString())).toEqual([
    teamOfRivals._id.toHexString(),
    emma._id.toHexString()
  ]);
});

it("correctly resolves nested filter queries", async () => {
  const books1 = await Book.find({
    author: {
      $in: {
        name: "J.K. Rowling"
      }
    }
  });

  const books2 = await Book.find({
    author: {
      $in: {
        favoriteBooks: {
          $in: {
            title: "Emma"
          }
        }
      }
    }
  });

  const books3 = await Book.find({
    author: {
      $in: {
        favoriteBooks: {
          $in: [
            {
              author: {
                $in: {
                  name: "Jane Austen"
                }
              }
            }
          ]
        }
      }
    }
  });

  const books4 = await Book.find({
    author: {
      $in: {
        favoriteBooks: {
          $in: [
            {
              author: {
                $in: [{ name: "Jane Austen" }, { name: "Jena Austen" }]
              }
            }
          ]
        }
      }
    }
  });

  expect(books1.length).toBe(1);
  expect(books1[0].title).toBe("Harry Potter");
  expect(books2).toEqual(books1);
  expect(books3).toEqual(books1);
  expect(books4).toEqual(books1);
});

it("correctly cascade-deletes documents", async () => {
  let books = await Book.find(),
    authors = await Author.find();

  expect(books.map(book => book.title)).toEqual([
    "Team of Rivals",
    "Emma",
    "Harry Potter"
  ]);
  expect(authors.map(author => author.name)).toEqual([
    "Doris Kearns Goodwin",
    "Jane Austen",
    "J.K. Rowling"
  ]);

  await Author.delete({ name: "Jane Austen" });

  books = await Book.find();
  authors = await Author.find();

  expect(books.map(book => book.title)).toEqual([
    "Team of Rivals",
    "Harry Potter"
  ]);
  expect(authors.map(author => author.name)).toEqual([
    "Doris Kearns Goodwin",
    "J.K. Rowling"
  ]);
  expect(authors[1].favoriteBooks.map(id => id.toHexString())).toEqual([
    books[0]._id.toHexString()
  ]);
});

it("correctly parses selectors", async () => {
  const predicateSample = () => true;
  const s1 = select``;
  const s2 = select`label`;
  const s3 = select`99`;
  const s4 = select` nested  ${"label"} ${6} `;
  const s5 = select`{a}`;
  const s6 = select` {_id,label  422  , ra-nk }  `;
  const s7 = select`{ *, author {*} }`;
  const s8 = select`  {key ${s7}}`;
  const s9 = select`${s8}`;
  const s10 = select`>`;
  const s11 = select`demo>label`;
  const s12 = select`a $ b $$ c`;
  const s13 = select`a.$.b.$$.c`;
  const s14 = select` ${{ name: "Dan Brown" }}.0.name `;
  const s15 = select`a ${predicateSample} b c`;
  const s16 = select`a ${predicateSample} ? b c`;
  const s17 = select`a ${predicateSample} ? b c : d`;
  const s18 = select`${predicateSample}?${predicateSample}?c::d`;
  const s19 = select`[]`;
  const s20 = select`demo [label, { * }, ${s17}  ]`;
  const s21 = select`
    > demo {
      *, 
      items {
        _id, 
        label,
        types 1 name
      }, 
      things $$ _id, 
      tools ${predicateSample} {
        _id ${predicateSample} ? val : value ,
        force
      }, 
      books [
        ${{ pageCount: { $lt: 200 } }},
        ${{ pageCount: { $gte: 200 } }}
      ], 
      todos [1, 4, 7]
    }
  `;

  const id = new IdentitySelector();
  expect(s1).toEqual(id);
  expect(s2).toEqual(new FieldSelector("label", id));
  expect(s3).toEqual(new IndexSelector(99, id));
  expect(s4).toEqual(
    new FieldSelector(
      "nested",
      new FieldSelector("label", new IndexSelector(6, id))
    )
  );
  expect(s5).toEqual(new ObjectSelector(new Map([["a", id]])));
  expect(s6).toEqual(
    new ObjectSelector(
      new Map<string, Selector>([
        ["_id", id],
        ["label", new IndexSelector(422, id)],
        ["ra-nk", id]
      ])
    )
  );
  expect(s7).toEqual(
    new ObjectSelector(
      new Map<string, Selector>([
        ["*", id],
        [
          "author",
          new ObjectSelector(
            new Map<string, Selector>([["*", id]])
          )
        ]
      ])
    )
  );
  expect(s8).toEqual(
    new ObjectSelector(
      new Map<string, Selector>([["key", s7]])
    )
  );
  expect(s9).toEqual(s8);
  expect(s10).toEqual(new ShortcutSelector(id));
  expect(s11).toEqual(
    new FieldSelector(
      "demo",
      new ShortcutSelector(new FieldSelector("label", id))
    )
  );
  const s12Like = new FieldSelector(
    "a",
    new MapSelector(
      new FieldSelector("b", new HardMapSelector(new FieldSelector("c", id)))
    )
  );
  expect(s12).toEqual(s12Like);
  expect(s13).toEqual(s12Like);
  expect(s14).toEqual(
    new FilterQuerySelector(
      { name: "Dan Brown" },
      new IndexSelector(0, new FieldSelector("name", id))
    )
  );
  expect(s15).toEqual(
    new FieldSelector(
      "a",
      new FilterSelector(
        predicateSample,
        new FieldSelector("b", new FieldSelector("c", id))
      )
    )
  );
  expect(s16).toEqual(
    new FieldSelector(
      "a",
      new SwitchSelector(
        predicateSample,
        new FieldSelector("b", new FieldSelector("c", id)),
        id
      )
    )
  );
  expect(s17).toEqual(
    new FieldSelector(
      "a",
      new SwitchSelector(
        predicateSample,
        new FieldSelector("b", new FieldSelector("c", id)),
        new FieldSelector("d", id)
      )
    )
  );
  expect(s18).toEqual(
    new SwitchSelector(
      predicateSample,
      new SwitchSelector(predicateSample, new FieldSelector("c", id), id),
      new FieldSelector("d", id)
    )
  );
  expect(s19).toEqual(new TupleSelector([id]));
  expect(s20).toEqual(
    new FieldSelector(
      "demo",
      new TupleSelector([
        new FieldSelector("label", id),
        new ObjectSelector(
          new Map<string, Selector>([["*", id]])
        ),
        s17
      ])
    )
  );
  expect(s21).toEqual(
    new ShortcutSelector(
      new FieldSelector(
        "demo",
        new ObjectSelector(
          new Map<string, Selector>([
            ["*", id],
            [
              "items",
              new ObjectSelector(
                new Map<string, Selector>([
                  ["_id", id],
                  ["label", id],
                  ["types", new IndexSelector(1, new FieldSelector("name", id))]
                ])
              )
            ],
            ["things", new HardMapSelector(new FieldSelector("_id", id))],
            [
              "tools",
              new FilterSelector(
                predicateSample,
                new ObjectSelector(
                  new Map<string, Selector>([
                    [
                      "_id",
                      new SwitchSelector(
                        predicateSample,
                        new FieldSelector("val", id),
                        new FieldSelector("value", id)
                      )
                    ],
                    ["force", id]
                  ])
                )
              )
            ],
            [
              "books",
              new TupleSelector([
                new FilterQuerySelector({ pageCount: { $lt: 200 } }, id),
                new FilterQuerySelector({ pageCount: { $gte: 200 } }, id)
              ])
            ],
            [
              "todos",
              new TupleSelector([
                new IndexSelector(1, id),
                new IndexSelector(4, id),
                new IndexSelector(7, id)
              ])
            ]
          ])
        )
      )
    )
  );
});

it("correctly disconnects", async () => {
  await rongo.close();
});
