/-
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
Author: David Thrane Christiansen
-/

import SQLite
import SQLiteTest.Framework

namespace SQLite.Test.Deriving

open SQLite

-- Test structure with basic fields
structure Person where
  name : String
  age : Int32
deriving Repr, BEq, SQLite.Row

-- Test structure with nullable field
structure NullablePerson where
  name : String
  nickname : Option String
  age : Int32
deriving Repr, BEq, SQLite.Row

-- Test structure with various types
structure Product where
  id : Int64
  name : String
  price : Float
  inStock : Bool
deriving Repr, BEq, Inhabited, SQLite.Row

-- Test structure with all nullable fields
structure AllOptional where
  a : Option String
  b : Option Int32
  c : Option Float
deriving Repr, BEq, Inhabited, SQLite.Row

-- Test empty structure
structure Empty where
deriving Repr, BEq, SQLite.Row

-- Test single-field structure with ResultColumn and QueryParam deriving
structure UserId where
  id : Int64
deriving Repr, BEq, Inhabited, SQLite.ResultColumn, SQLite.QueryParam

-- Test another trivial wrapper
structure Username where
  name : String
deriving Repr, BEq, Inhabited, SQLite.ResultColumn, SQLite.QueryParam

-- Test non-structure inductive with multiple fields (for Row)
inductive Coordinate where
  | mk (x : Float) (y : Float)
deriving Repr, BEq, Inhabited, SQLite.Row

-- Test non-structure single-field inductive (for ResultColumn and QueryParam)
inductive Email where
  | mk (addr : String)
deriving Repr, BEq, Inhabited, SQLite.ResultColumn, SQLite.QueryParam

-- Test type with proof field (QueryParam should ignore the proof)
structure NonEmptyString where
  val : String
  nonEmpty : val.length > 0
deriving SQLite.QueryParam

open SQLite.Test in
def runDerivingTests (db : SQLite) : TestM Unit :=
  withHeader "=== Testing Deriving Handlers ===" do
    -- Set up test table
    expectSuccess "Created people table" do
      db.exec "CREATE TABLE IF NOT EXISTS people (name TEXT NOT NULL, age INTEGER NOT NULL);"
      db.exec "DELETE FROM people;"

    -- Insert test data
    expectSuccess "Inserted test people" do
      db.exec "INSERT INTO people (name, age) VALUES ('Alice', 30);"
      db.exec "INSERT INTO people (name, age) VALUES ('Bob', 25);"
      db.exec "INSERT INTO people (name, age) VALUES ('Charlie', 35);"

    -- Test basic derived Row instance
    let people ← expectSuccess' (fun ps => s!"Read {ps.size} people") do
      let stmt ← db.prepare "SELECT name, age FROM people ORDER BY age;"
      let result : Array Person ← stmt.results.toArray
      pure result
    let expected : Array Person := #[
      { name := "Bob", age := 25 },
      { name := "Alice", age := 30 },
      { name := "Charlie", age := 35 }
    ]
    expect (people == expected) s!"Expected {repr expected}, got {repr people}"

    -- Test with query! macro
    let query ← db query!"SELECT name, age FROM people WHERE age > {(27 : Int32)}" as Person
    let filtered : Array Person ← query.toArray
    let expectedFiltered : Array Person := #[
      { name := "Alice", age := 30 },
      { name := "Charlie", age := 35 }
    ]
    expect (filtered == expectedFiltered) s!"Filtered: expected {repr expectedFiltered}, got {repr filtered}"
    recordSuccess "query! macro works with derived Row"

    -- Set up nullable test
    expectSuccess "Created nullable_people table" do
      db.exec "CREATE TABLE IF NOT EXISTS nullable_people (name TEXT NOT NULL, nickname TEXT, age INTEGER NOT NULL);"
      db.exec "DELETE FROM nullable_people;"
      db.exec "INSERT INTO nullable_people (name, nickname, age) VALUES ('Alice', 'Ali', 30);"
      db.exec "INSERT INTO nullable_people (name, nickname, age) VALUES ('Bob', NULL, 25);"

    -- Test nullable fields
    let nullablePeople ← expectSuccess' (fun ps => s!"Read {ps.size} nullable people") do
      let stmt ← db.prepare "SELECT name, nickname, age FROM nullable_people ORDER BY name;"
      let result : Array NullablePerson ← stmt.results.toArray
      pure result
    let expectedNullable : Array NullablePerson := #[
      { name := "Alice", nickname := some "Ali", age := 30 },
      { name := "Bob", nickname := none, age := 25 }
    ]
    expect (nullablePeople == expectedNullable) s!"Expected {repr expectedNullable}, got {repr nullablePeople}"

    -- Set up product table for various types
    expectSuccess "Created products table" do
      db.exec "CREATE TABLE IF NOT EXISTS derived_products (id INTEGER NOT NULL, name TEXT NOT NULL, price REAL NOT NULL, in_stock INTEGER NOT NULL);"
      db.exec "DELETE FROM derived_products;"
      db.exec "INSERT INTO derived_products (id, name, price, in_stock) VALUES (1, 'Widget', 19.99, 1);"
      db.exec "INSERT INTO derived_products (id, name, price, in_stock) VALUES (2, 'Gadget', 29.99, 0);"

    -- Test various types
    let products ← expectSuccess' (fun ps => s!"Read {ps.size} products") do
      let stmt ← db.prepare "SELECT id, name, price, in_stock FROM derived_products ORDER BY id;"
      let result : Array Product ← stmt.results.toArray
      pure result

    expect (products.size == 2) s!"Expected 2 products, got {products.size}"
    expect (products[0]!.id == 1) s!"Expected id 1, got {products[0]!.id}"
    expect (products[0]!.name == "Widget") s!"Expected Widget, got {products[0]!.name}"
    expect (products[0]!.inStock == true) s!"Expected inStock true, got {products[0]!.inStock}"
    expect (products[1]!.inStock == false) s!"Expected inStock false, got {products[1]!.inStock}"
    recordSuccess "Various types work with derived Row"

    -- Test all-optional structure
    expectSuccess "Created all_optional table" do
      db.exec "CREATE TABLE IF NOT EXISTS all_optional (a TEXT, b INTEGER, c REAL);"
      db.exec "DELETE FROM all_optional;"
      db.exec "INSERT INTO all_optional (a, b, c) VALUES ('test', 42, 3.14);"
      db.exec "INSERT INTO all_optional (a, b, c) VALUES (NULL, NULL, NULL);"

    let allOpts ← expectSuccess' (fun ps => s!"Read {ps.size} all-optional rows") do
      let stmt ← db.prepare "SELECT a, b, c FROM all_optional;"
      let result : Array AllOptional ← stmt.results.toArray
      pure result
    expect (allOpts.size == 2) s!"Expected 2 rows, got {allOpts.size}"
    expect (allOpts[0]!.a == some "test") s!"Expected some 'test', got {repr allOpts[0]!.a}"
    expect (allOpts[1]!.a == none) s!"Expected none, got {repr allOpts[1]!.a}"
    recordSuccess "All-optional structure works"

    -- Test empty structure (should read zero columns)
    -- Can't really test Empty meaningfully since it has no columns to read
    -- but the instance should at least compile
    recordSuccess "Empty structure compiles (Row instance exists)"

    -- Test ResultColumn deriving for trivial wrappers
    expectSuccess "Created wrapper_users table" do
      db.exec "CREATE TABLE IF NOT EXISTS wrapper_users (id INTEGER NOT NULL, username TEXT NOT NULL);"
      db.exec "DELETE FROM wrapper_users;"
      db.exec "INSERT INTO wrapper_users (id, username) VALUES (1, 'alice');"
      db.exec "INSERT INTO wrapper_users (id, username) VALUES (2, 'bob');"

    -- Test UserId trivial wrapper
    let userIds ← expectSuccess' (fun ids => s!"Read {ids.size} user IDs") do
      let stmt ← db.prepare "SELECT id FROM wrapper_users ORDER BY id;"
      let result : Array UserId ← stmt.results.toArray
      pure result
    expect (userIds.size == 2) s!"Expected 2 user IDs, got {userIds.size}"
    expect (userIds[0]! == ⟨1⟩) s!"Expected UserId 1, got {repr userIds[0]!}"
    expect (userIds[1]! == ⟨2⟩) s!"Expected UserId 2, got {repr userIds[1]!}"
    recordSuccess "UserId trivial wrapper works with derived ResultColumn"

    -- Test Username trivial wrapper
    let usernames ← expectSuccess' (fun names => s!"Read {names.size} usernames") do
      let stmt ← db.prepare "SELECT username FROM wrapper_users ORDER BY id;"
      let result : Array Username ← stmt.results.toArray
      pure result
    expect (usernames.size == 2) s!"Expected 2 usernames, got {usernames.size}"
    expect (usernames[0]! == ⟨"alice"⟩) s!"Expected Username alice, got {repr usernames[0]!}"
    expect (usernames[1]! == ⟨"bob"⟩) s!"Expected Username bob, got {repr usernames[1]!}"
    recordSuccess "Username trivial wrapper works with derived ResultColumn"

    -- Test that trivial wrappers also work with Row (via ResultColumn instance)
    let mixedQuery ← expectSuccess' (fun pairs => s!"Read {pairs.size} id-name pairs") do
      let stmt ← db.prepare "SELECT id, username FROM wrapper_users ORDER BY id;"
      let result : Array (UserId × Username) ← stmt.results.toArray
      pure result
    expect (mixedQuery.size == 2) s!"Expected 2 pairs, got {mixedQuery.size}"
    expect (mixedQuery[0]! == (⟨1⟩, ⟨"alice"⟩)) s!"Expected (1, alice), got {repr mixedQuery[0]!}"
    recordSuccess "Trivial wrappers work in tuple Row instances"

    -- Test QueryParam deriving with trivial wrappers
    let queryById ← expectSuccess' (fun names => s!"Found {names.size} users by UserId") do
      let targetId : UserId := ⟨1⟩
      let stmt ← db sql!"SELECT username FROM wrapper_users WHERE id = {targetId}"
      let result : Array Username ← stmt.results.toArray
      pure result
    expect (queryById.size == 1) s!"Expected 1 user, got {queryById.size}"
    expect (queryById[0]! == ⟨"alice"⟩) s!"Expected alice, got {repr queryById[0]!}"
    recordSuccess "UserId QueryParam works in sql! macro"

    let queryByName ← expectSuccess' (fun ids => s!"Found {ids.size} users by Username") do
      let targetName : Username := ⟨"bob"⟩
      let stmt ← db sql!"SELECT id FROM wrapper_users WHERE username = {targetName}"
      let result : Array UserId ← stmt.results.toArray
      pure result
    expect (queryByName.size == 1) s!"Expected 1 user, got {queryByName.size}"
    expect (queryByName[0]! == ⟨2⟩) s!"Expected UserId 2, got {repr queryByName[0]!}"
    recordSuccess "Username QueryParam works in sql! macro"

    -- Test non-structure inductive types
    expectSuccess "Created coordinates table" do
      db.exec "CREATE TABLE IF NOT EXISTS coordinates (x REAL NOT NULL, y REAL NOT NULL);"
      db.exec "DELETE FROM coordinates;"
      db.exec "INSERT INTO coordinates (x, y) VALUES (1.0, 2.0);"
      db.exec "INSERT INTO coordinates (x, y) VALUES (3.5, 4.5);"

    let coords ← expectSuccess' (fun cs => s!"Read {cs.size} coordinates") do
      let stmt ← db.prepare "SELECT x, y FROM coordinates ORDER BY x;"
      let result : Array Coordinate ← stmt.results.toArray
      pure result
    expect (coords.size == 2) s!"Expected 2 coordinates, got {coords.size}"
    expect (coords[0]! == .mk 1.0 2.0) s!"Expected (1.0, 2.0), got {repr coords[0]!}"
    expect (coords[1]! == .mk 3.5 4.5) s!"Expected (3.5, 4.5), got {repr coords[1]!}"
    recordSuccess "Non-structure inductive works with derived Row"

    expectSuccess "Created emails table" do
      db.exec "CREATE TABLE IF NOT EXISTS emails (addr TEXT NOT NULL);"
      db.exec "DELETE FROM emails;"
      db.exec "INSERT INTO emails (addr) VALUES ('alice@example.com');"
      db.exec "INSERT INTO emails (addr) VALUES ('bob@example.com');"

    let emails ← expectSuccess' (fun es => s!"Read {es.size} emails") do
      let stmt ← db.prepare "SELECT addr FROM emails ORDER BY addr;"
      let result : Array Email ← stmt.results.toArray
      pure result
    expect (emails.size == 2) s!"Expected 2 emails, got {emails.size}"
    expect (emails[0]! == .mk "alice@example.com") s!"Expected alice@example.com, got {repr emails[0]!}"
    recordSuccess "Non-structure inductive works with derived ResultColumn"

    let queryByEmail ← expectSuccess' (fun es => s!"Found {es.size} emails") do
      let targetEmail : Email := .mk "bob@example.com"
      let stmt ← db sql!"SELECT addr FROM emails WHERE addr = {targetEmail}"
      let result : Array Email ← stmt.results.toArray
      pure result
    expect (queryByEmail.size == 1) s!"Expected 1 email, got {queryByEmail.size}"
    expect (queryByEmail[0]! == .mk "bob@example.com") s!"Expected bob@example.com, got {repr queryByEmail[0]!}"
    recordSuccess "Non-structure inductive works with derived QueryParam"

    -- Test type with proof field
    expectSuccess "Created nonempty_strings table" do
      db.exec "CREATE TABLE IF NOT EXISTS nonempty_strings (val TEXT NOT NULL);"
      db.exec "DELETE FROM nonempty_strings;"
      db.exec "INSERT INTO nonempty_strings (val) VALUES ('hello');"
      db.exec "INSERT INTO nonempty_strings (val) VALUES ('world');"

    let queryByNonEmpty ← expectSuccess' (fun vs => s!"Found {vs.size} strings") do
      let target : NonEmptyString := ⟨"hello", by decide⟩
      let stmt ← db sql!"SELECT val FROM nonempty_strings WHERE val = {target}"
      let result : Array String ← stmt.results.toArray
      pure result
    expect (queryByNonEmpty.size == 1) s!"Expected 1 string, got {queryByNonEmpty.size}"
    expect (queryByNonEmpty[0]! == "hello") s!"Expected hello, got {queryByNonEmpty[0]!}"
    recordSuccess "Type with proof field works with derived QueryParam"

end SQLite.Test.Deriving
