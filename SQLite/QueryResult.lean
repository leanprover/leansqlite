/-
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
Author: David Thrane Christiansen
-/
module

public import SQLite.LowLevel
public import SQLite.Blob
import Std.Data.Iterators
import Lean.Elab.Command

set_option doc.verso true
set_option linter.missingDocs true

namespace SQLite

/--
Provides a canonical way to read a non-null value of type {name}`α` from a SQL query.

A deriving handler is available for single-constructor, single-field inductive types (trivial
wrappers). The generated instance delegates to the wrapped type's instance.
-/
public class ResultColumn (α : Type) where
  get (stmt : Stmt) (column : Int32) : IO α

public instance [ResultColumn α] : ResultColumn (Option α) where
  get stmt i := do
    if (← stmt.columnType i) == .null then
      return none
    else some <$> ResultColumn.get stmt i

public instance : ResultColumn String where
  get stmt i := Stmt.columnText stmt i

public instance : ResultColumn ByteArray where
  get := Stmt.columnBlob

public instance : ResultColumn Float where
  get := Stmt.columnDouble

public instance : ResultColumn Int32 where
  get := Stmt.columnInt

public instance : ResultColumn Int64 where
  get stmt i := Stmt.columnInt64 stmt i

public instance : ResultColumn Bool where
  get stmt i := do return (← Stmt.columnInt stmt i) ≠ 0

public instance : ResultColumn Nat where
  get stmt i := (·.toNatClampNeg) <$> Stmt.columnInt64 stmt i

open Blob in
/--
Defines an instance that interprets a column as a binary blob, according to its {name}`FromBinary`
instance.
-/
public def ResultColumn.asBlob [FromBinary α] : ResultColumn α where
  get stmt i := do
    match (← fromBinary <$> Stmt.columnBlob stmt i) with
    | .ok v => pure v
    | .error e => throw <| IO.userError s!"Failed to deserialize blob in column {i}: {e}"

/--
Provides a canonical way to read a potentially-null value of type {name}`α` from a SQL query.
-/
public class NullableResultColumn (α : Type) where
  get (stmt : Stmt) (column : Int32) : IO α

public instance [ResultColumn α] : NullableResultColumn α where
  get := ResultColumn.get

-- Reading a field can trigger type conversion, so this pattern is not generally safe. Null stays
-- null, at least, so this one is OK.
public instance [ResultColumn α] : NullableResultColumn (Option α) where
  get stmt i := do
    if (← stmt.columnType i) == .null then
      return none
    else some <$> ResultColumn.get stmt i

/--
A monad for reading a row of data from a query result.
-/
public abbrev RowReader (α : Type) := ReaderT Stmt (StateRefT Int32 IO) α

namespace RowReader

/--
Reads the next column using the {lean}`ResultColumn α` instance, updating the column counter.
-/
public def field [ResultColumn α] : RowReader α := do
  let v ← ResultColumn.get (← read) (← getThe Int32)
  modify (· + 1)
  return v

/--
Runs a row reader on a prepared statement.

The statement should have just been stepped, and returned {lean}`true`.
-/
public def run (stmt : Stmt) (act : RowReader α) : IO α := do
  -- The initial column index is 0 here. Binding parameters is 1-indexed, reading columns is
  -- 0-indexed in SQLite.
  return (← ReaderT.run act stmt |>.run 0).1

end RowReader

/--
A type that can be deserialized from a complete row in a SQL query.

A deriving handler is available for single-constructor inductive types. Each constructor parameter
is read sequentially using its {name}`ResultColumn` instance.
-/
public class Row (α : Type) where
  read : RowReader α

public instance [ResultColumn α] : Row α where
  read := RowReader.field

public instance [Row α] [Row β] : Row (α × β) where
  read := do return (← Row.read, ← Row.read)

/--
An iterator over query results. Stepping the iterator steps the underlying prepared statement.
-/
public structure QueryIterator (β : Type) where
  [isRow : Row β]
  stmt : Stmt

open Std
open Iterators

public instance : Iterator (QueryIterator β) IO β where
  IsPlausibleStep _it
    | .yield _it' _v => True
    | .done => True
    | .skip .. => False
  step it := do
    match it.internalState with
    | { isRow, stmt } =>
      let hasRows ← stmt.step
      if hasRows then
        return .deflate <| .yield { it with internalState := { isRow, stmt } } (← isRow.read.run stmt) ⟨⟩
      else
        return .deflate <| .done ⟨⟩

public instance [Monad n] : IteratorLoop (QueryIterator β) IO n := IteratorLoop.defaultImplementation

-- Compatibility shim for 4.27.0
open Lean Elab Command in
#eval show CommandElabM Unit from do
  if (← getEnv).contains `Std.Iterators.IteratorCollect then
    elabCommand (← `(command|public instance {n} {β} [Monad n] : IteratorCollect (QueryIterator β) IO n := IteratorCollect.defaultImplementation))


/--
Returns an iterator into all the results of a prepared statement.
-/
public def Stmt.results [Row α] (stmt : Stmt) : @IterM (QueryIterator α) IO α where
  internalState := QueryIterator.mk stmt

/--
Returns an iterator into all the results of a prepared statement, with the type specified explicitly.
-/
public def Stmt.resultsAs (α : Type) [Row α] (stmt : Stmt) : @IterM (QueryIterator α) IO α := stmt.results

end SQLite
