/-
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
Author: David Thrane Christiansen
-/

import SQLite
import SQLiteTest.Framework

namespace SQLite.Test.Deriving

open SQLite
open SQLite.Test

/-! ## Person: Basic Row Deriving -/

structure Person where
  name : String
  age : Int32
deriving Repr, BEq, SQLite.Row

/-- Tests `Person` with basic Row deriving. -/
def testPersonRow (db : SQLite) : TestM Unit :=
  withHeader "Row: basic multi-field structure" do
    expectSuccess "Created people table" do
      db.exec "CREATE TABLE IF NOT EXISTS people (name TEXT NOT NULL, age INTEGER NOT NULL);"
      db.exec "DELETE FROM people;"

    expectSuccess "Inserted test people" do
      db.exec "INSERT INTO people (name, age) VALUES ('Alice', 30);"
      db.exec "INSERT INTO people (name, age) VALUES ('Bob', 25);"
      db.exec "INSERT INTO people (name, age) VALUES ('Charlie', 35);"

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

    let query ← db query!"SELECT name, age FROM people WHERE age > {(27 : Int32)}" as Person
    let filtered : Array Person ← query.toArray
    let expectedFiltered : Array Person := #[
      { name := "Alice", age := 30 },
      { name := "Charlie", age := 35 }
    ]
    expect (filtered == expectedFiltered) s!"Filtered: expected {repr expectedFiltered}, got {repr filtered}"
    recordSuccess "query! macro works with derived Row"

/-! ## NullablePerson: Nullable Field Handling -/

structure NullablePerson where
  name : String
  nickname : Option String
  age : Int32
deriving Repr, BEq, SQLite.Row

/-- Tests `NullablePerson` with nullable field handling. -/
def testNullablePersonRow (db : SQLite) : TestM Unit :=
  withHeader "Row: handles NULL via Option" do
    expectSuccess "Created nullable_people table" do
      db.exec "CREATE TABLE IF NOT EXISTS nullable_people (name TEXT NOT NULL, nickname TEXT, age INTEGER NOT NULL);"
      db.exec "DELETE FROM nullable_people;"
      db.exec "INSERT INTO nullable_people (name, nickname, age) VALUES ('Alice', 'Ali', 30);"
      db.exec "INSERT INTO nullable_people (name, nickname, age) VALUES ('Bob', NULL, 25);"

    let nullablePeople ← expectSuccess' (fun ps => s!"Read {ps.size} nullable people") do
      let stmt ← db.prepare "SELECT name, nickname, age FROM nullable_people ORDER BY name;"
      let result : Array NullablePerson ← stmt.results.toArray
      pure result
    let expectedNullable : Array NullablePerson := #[
      { name := "Alice", nickname := some "Ali", age := 30 },
      { name := "Bob", nickname := none, age := 25 }
    ]
    expect (nullablePeople == expectedNullable) s!"Expected {repr expectedNullable}, got {repr nullablePeople}"

/-! ## Product: Various Field Types -/

structure Product where
  id : Int64
  name : String
  price : Float
  inStock : Bool
deriving Repr, BEq, Inhabited, SQLite.Row

/-- Tests `Product` with various field types (Int64, String, Float, Bool). -/
def testProductRow (db : SQLite) : TestM Unit :=
  withHeader "Row: multiple field types" do
    expectSuccess "Created products table" do
      db.exec "CREATE TABLE IF NOT EXISTS derived_products (id INTEGER NOT NULL, name TEXT NOT NULL, price REAL NOT NULL, in_stock INTEGER NOT NULL);"
      db.exec "DELETE FROM derived_products;"
      db.exec "INSERT INTO derived_products (id, name, price, in_stock) VALUES (1, 'Widget', 19.99, 1);"
      db.exec "INSERT INTO derived_products (id, name, price, in_stock) VALUES (2, 'Gadget', 29.99, 0);"

    let products ← expectSuccess' (fun ps => s!"Read {ps.size} products") do
      let stmt ← db.prepare "SELECT id, name, price, in_stock FROM derived_products ORDER BY id;"
      let result : Array Product ← stmt.results.toArray
      pure result

    let expected : Array Product := #[
      { id := 1, name := "Widget", price := 19.99, inStock := true },
      { id := 2, name := "Gadget", price := 29.99, inStock := false }
    ]
    expect (products == expected) s!"Expected {repr expected}, got {repr products}"

/-! ## AllOptional: All Nullable Fields -/

structure AllOptional where
  a : Option String
  b : Option Int32
  c : Option Float
deriving Repr, BEq, Inhabited, SQLite.Row

/-- Tests `AllOptional` with all nullable fields. -/
def testAllOptionalRow (db : SQLite) : TestM Unit :=
  withHeader "Row: all fields are Option" do
    expectSuccess "Created all_optional table" do
      db.exec "CREATE TABLE IF NOT EXISTS all_optional (a TEXT, b INTEGER, c REAL);"
      db.exec "DELETE FROM all_optional;"
      db.exec "INSERT INTO all_optional (a, b, c) VALUES ('test', 42, 3.14);"
      db.exec "INSERT INTO all_optional (a, b, c) VALUES (NULL, NULL, NULL);"

    let allOpts ← expectSuccess' (fun ps => s!"Read {ps.size} all-optional rows") do
      let stmt ← db.prepare "SELECT a, b, c FROM all_optional ORDER BY rowid;"
      let result : Array AllOptional ← stmt.results.toArray
      pure result
    let expected : Array AllOptional := #[
      { a := some "test", b := some 42, c := some 3.14 },
      { a := none, b := none, c := none }
    ]
    expect (allOpts == expected) s!"Expected {repr expected}, got {repr allOpts}"

/-! ## Empty: Zero-Field Structure -/

structure Empty where
deriving Repr, BEq, SQLite.Row

/-- Tests `Empty` structure (zero-field Row). -/
def testEmptyRow (db : SQLite) : TestM Unit :=
  withHeader "Row: zero-field structure" do
    expectSuccess "Created empty_test table" do
      db.exec "CREATE TABLE IF NOT EXISTS empty_test (dummy INTEGER);"
      db.exec "DELETE FROM empty_test;"
      db.exec "INSERT INTO empty_test (dummy) VALUES (1);"
      db.exec "INSERT INTO empty_test (dummy) VALUES (2);"
      db.exec "INSERT INTO empty_test (dummy) VALUES (3);"

    -- Reading rows as Empty consumes no columns; should return one Empty per row
    let empties ← expectSuccess' (fun es => s!"Read {es.size} empty rows") do
      let stmt ← db.prepare "SELECT 1 FROM empty_test;"
      let result : Array Empty ← stmt.results.toArray
      pure result
    let expected : Array Empty := #[{}, {}, {}]
    expect (empties == expected) s!"Expected {repr expected}, got {repr empties}"

/-! ## UserId and Username: Trivial Wrappers with ResultColumn and QueryParam -/

structure UserId where
  id : Int64
deriving Repr, BEq, Inhabited, SQLite.ResultColumn, SQLite.QueryParam

structure Username where
  name : String
deriving Repr, BEq, Inhabited, SQLite.ResultColumn, SQLite.QueryParam

/-- Tests `UserId` trivial wrapper with ResultColumn deriving. -/
def testUserIdResultColumn (db : SQLite) : TestM Unit :=
  withHeader "ResultColumn: trivial Int64 wrapper" do
    expectSuccess "Created wrapper_users table" do
      db.exec "CREATE TABLE IF NOT EXISTS wrapper_users (id INTEGER NOT NULL, username TEXT NOT NULL);"
      db.exec "DELETE FROM wrapper_users;"
      db.exec "INSERT INTO wrapper_users (id, username) VALUES (1, 'alice');"
      db.exec "INSERT INTO wrapper_users (id, username) VALUES (2, 'bob');"

    let userIds ← expectSuccess' (fun ids => s!"Read {ids.size} user IDs") do
      let stmt ← db.prepare "SELECT id FROM wrapper_users ORDER BY id;"
      let result : Array UserId ← stmt.results.toArray
      pure result
    expect (userIds.size == 2) s!"Expected 2 user IDs, got {userIds.size}"
    expect (userIds[0]! == ⟨1⟩) s!"Expected UserId 1, got {repr userIds[0]!}"
    expect (userIds[1]! == ⟨2⟩) s!"Expected UserId 2, got {repr userIds[1]!}"
    recordSuccess "UserId trivial wrapper works with derived ResultColumn"

/-- Tests `UserId` QueryParam deriving with sql! macro. -/
def testUserIdQueryParam (db : SQLite) : TestM Unit :=
  withHeader "QueryParam: trivial Int64 wrapper" do
    let queryById ← expectSuccess' (fun names => s!"Found {names.size} users by UserId") do
      let targetId : UserId := ⟨1⟩
      let stmt ← db sql!"SELECT username FROM wrapper_users WHERE id = {targetId}"
      let result : Array Username ← stmt.results.toArray
      pure result
    expect (queryById.size == 1) s!"Expected 1 user, got {queryById.size}"
    expect (queryById[0]! == ⟨"alice"⟩) s!"Expected alice, got {repr queryById[0]!}"
    recordSuccess "UserId QueryParam works in sql! macro"

/-- Tests `Username` trivial wrapper with ResultColumn deriving. -/
def testUsernameResultColumn (db : SQLite) : TestM Unit :=
  withHeader "ResultColumn: trivial String wrapper" do
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

/-- Tests `Username` QueryParam deriving with sql! macro. -/
def testUsernameQueryParam (db : SQLite) : TestM Unit :=
  withHeader "QueryParam: trivial String wrapper" do
    let queryByName ← expectSuccess' (fun ids => s!"Found {ids.size} users by Username") do
      let targetName : Username := ⟨"bob"⟩
      let stmt ← db sql!"SELECT id FROM wrapper_users WHERE username = {targetName}"
      let result : Array UserId ← stmt.results.toArray
      pure result
    expect (queryByName.size == 1) s!"Expected 1 user, got {queryByName.size}"
    expect (queryByName[0]! == ⟨2⟩) s!"Expected UserId 2, got {repr queryByName[0]!}"
    recordSuccess "Username QueryParam works in sql! macro"

/-! ## Coordinate: Non-Structure Inductive with Row -/

inductive Coordinate where
  | mk (x : Float) (y : Float)
deriving Repr, BEq, Inhabited, SQLite.Row

/-- Tests `Coordinate` non-structure inductive with Row deriving. -/
def testCoordinateRow (db : SQLite) : TestM Unit :=
  withHeader "Row: non-structure inductive type" do
    expectSuccess "Created coordinates table" do
      db.exec "CREATE TABLE IF NOT EXISTS coordinates (x REAL NOT NULL, y REAL NOT NULL);"
      db.exec "DELETE FROM coordinates;"
      db.exec "INSERT INTO coordinates (x, y) VALUES (1.0, 2.0);"
      db.exec "INSERT INTO coordinates (x, y) VALUES (3.5, 4.5);"

    let coords ← expectSuccess' (fun cs => s!"Read {cs.size} coordinates") do
      let stmt ← db.prepare "SELECT x, y FROM coordinates ORDER BY x;"
      let result : Array Coordinate ← stmt.results.toArray
      pure result
    let expected : Array Coordinate := #[.mk 1.0 2.0, .mk 3.5 4.5]
    expect (coords == expected) s!"Expected {repr expected}, got {repr coords}"

/-! ## Email: Non-Structure Inductive with ResultColumn and QueryParam -/

inductive Email where
  | mk (addr : String)
deriving Repr, BEq, Inhabited, SQLite.ResultColumn, SQLite.QueryParam

/-- Tests `Email` non-structure inductive with ResultColumn and QueryParam deriving. -/
def testEmailResultColumnAndQueryParam (db : SQLite) : TestM Unit :=
  withHeader "ResultColumn/QueryParam: non-structure inductive" do
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

/-! ## NonEmptyString: Type with Proof Field -/

structure NonEmptyString where
  val : String
  nonEmpty : val.length > 0
deriving SQLite.QueryParam

/-- Tests `NonEmptyString` type with proof field (QueryParam ignores proof fields). -/
def testNonEmptyStringQueryParam (db : SQLite) : TestM Unit :=
  withHeader "QueryParam: ignores proof fields" do
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

/-! ## Negative Tests -/

/-! ### Row Rejects Multi-Constructor Types

The `Row` handler only supports single-constructor inductives because it needs to know
statically how many columns to read and in what order.
-/

/-- error: None of the deriving handlers for class `Row` applied to `MultiCtorForRow` -/
#guard_msgs in
inductive MultiCtorForRow where
  | a (x : Int32) | b (y : String)
deriving SQLite.Row

/-! ### Row Cannot Handle Proof Fields

Unlike `QueryParam` which can ignore proof fields (since it only sends data to SQLite),
`Row` cannot construct proof fields from database data. When reading from SQLite, we have
no way to synthesize the proof required by the constructor.
-/

/--
error: Application type mismatch: The argument
  ProofFieldForRow.mk
has type
  (val : Int32) → val ≥ 0 → ProofFieldForRow
but is expected to have type
  ?m.16 → ?m.20 → ProofFieldForRow
in the application
  pure ProofFieldForRow.mk
-/
#guard_msgs in
structure ProofFieldForRow where
  val : Int32
  inBounds : val ≥ 0
deriving SQLite.Row

/-! ### ResultColumn Rejects Multi-Constructor Types

The `ResultColumn` handler only supports single-constructor, single-field types (trivial wrappers)
since a result column is exactly one value.
-/

/-- error: None of the deriving handlers for class `ResultColumn` applied to `MultiCtorForResultColumn` -/
#guard_msgs in
inductive MultiCtorForResultColumn where
  | a (x : Int32) | b (y : String)
deriving SQLite.ResultColumn

/-! ###  ResultColumn Rejects Multi-Field Types

The `ResultColumn` handler only supports single-field types because a single column
can only hold one value.
-/

/-- error: None of the deriving handlers for class `ResultColumn` applied to `MultiFieldForResultColumn` -/
#guard_msgs in
structure MultiFieldForResultColumn where
  x : Int32
  y : String
deriving SQLite.ResultColumn

/-! ### ResultColumn Rejects Zero-Field Types

The `ResultColumn` handler requires exactly one field to read from the column.
-/

/-- error: None of the deriving handlers for class `ResultColumn` applied to `ZeroFieldForResultColumn` -/
#guard_msgs in
structure ZeroFieldForResultColumn where
deriving SQLite.ResultColumn

/-! ### ResultColumn Rejects Types Without Field Instances

Even when the structure shape is valid, deriving fails if the field type lacks a
`ResultColumn` instance. Here, `Option Recursive` has no instance because `Recursive`
itself doesn't.
-/

/--
error: failed to synthesize instance of type class
  ResultColumn (Option Recursive)

Hint: Type class instance resolution failures can be inspected with the `set_option trace.Meta.synthInstance true` command.
-/
#guard_msgs in
structure Recursive where
  next : Option Recursive
deriving SQLite.ResultColumn

/-! ### QueryParam Rejects Multi-Constructor Types

The `QueryParam` handler only supports single-constructor types because it needs to
extract exactly one value to bind.
-/

/-- error: None of the deriving handlers for class `QueryParam` applied to `MultiCtorForQueryParam` -/
#guard_msgs in
inductive MultiCtorForQueryParam where
  | inl (n : Nat) | inr (b : Bool)
deriving SQLite.QueryParam

/-! ### QueryParam Rejects Multiple Data Fields

The `QueryParam` handler only supports types with exactly one non-proof field, since
a single parameter binding corresponds to one value.
-/

/-- error: None of the deriving handlers for class `QueryParam` applied to `MultiDataFieldForQueryParam` -/
#guard_msgs in
structure MultiDataFieldForQueryParam where
  x : Int32
  y : String
deriving SQLite.QueryParam

/-! ### QueryParam Rejects Zero-Field Types

The `QueryParam` handler requires at least one data field to bind.
-/

/-- error: None of the deriving handlers for class `QueryParam` applied to `ZeroFieldForQueryParam` -/
#guard_msgs in
structure ZeroFieldForQueryParam where
deriving SQLite.QueryParam

/-! ### QueryParam Rejects Types with Only Proof Fields

The `QueryParam` handler ignores proof fields (fields with `Prop` type), so a type
with only proof fields has zero bindable data fields.
-/

/-- error: None of the deriving handlers for class `QueryParam` applied to `OnlyProofFields` -/
#guard_msgs in
structure OnlyProofFields where
  h : 1 + 1 = 2
deriving SQLite.QueryParam

/-! ## Test Runner -/

/-- Runs all deriving handler tests. -/
def runDerivingTests (db : SQLite) : TestM Unit :=
  withHeader "=== Testing Deriving Handlers ===" do
    testPersonRow db
    testNullablePersonRow db
    testProductRow db
    testAllOptionalRow db
    testEmptyRow db
    testUserIdResultColumn db
    testUserIdQueryParam db
    testUsernameResultColumn db
    testUsernameQueryParam db
    testCoordinateRow db
    testEmailResultColumnAndQueryParam db
    testNonEmptyStringQueryParam db
