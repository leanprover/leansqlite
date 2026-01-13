/-
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
Author: David Thrane Christiansen
-/
module
public import SQLite.FFI
public import SQLite.LowLevel
import Std.Data.Iterators
import Lean.Elab.Command

set_option doc.verso true
set_option linter.missingDocs true

namespace SQLite

section Interpolation
open Lean

/--
Provides a canonical means of binding a value of type {name}`α` in a SQLite query.
-/
public class ToSQLite (α : Type u) where
  bind : Stmt → Int32 → α → IO Unit

public instance : ToSQLite String where
  bind := Stmt.bindText

public instance : ToSQLite ByteArray where
  bind := Stmt.bindBlob

public instance : ToSQLite Int32 where
  bind := Stmt.bindInt32

public instance : ToSQLite Int64 where
  bind := Stmt.bindInt64

public instance : ToSQLite Float where
  bind := Stmt.bindFloat

public instance : ToSQLite Unit where
  bind stmt index _ := Stmt.bindNull stmt index

public instance : ToSQLite Nat where
  bind stmt i n := Stmt.bindText stmt i (toString n)

end Interpolation

/--
Provides a canonical way to read a value of type {name}`α` from a SQL query.
-/
public class Field (α : Type) where
  get (stmt : Stmt) (column : Int32) : IO α

-- Reading a field can trigger type conversion, so this pattern is not generally safe. Null stays
-- null, at least, so this one is OK.
public instance [Field α] : Field (Option α) where
  get stmt i := do
    if (← stmt.columnType i) == .null then
      return none
    else some <$> Field.get stmt i

public instance : Field String where
  get stmt i := Stmt.columnText stmt i

public instance : Field ByteArray where
  get := Stmt.columnBlob

public instance : Field Float where
  get := Stmt.columnDouble

public instance : Field Int32 where
  get := Stmt.columnInt

public instance : Field Int64 where
  get stmt i := Stmt.columnInt64 stmt i

public instance : Field Nat where
  get stmt i := (·.toNatClampNeg) <$> Stmt.columnInt64 stmt i

/--
A monad for reading a row of data from a query result.
-/
public abbrev RowReader (α : Type) := ReaderT Stmt (StateRefT Int32 IO) α

namespace RowReader

/--
Reads the next column using the {lean}`Field α` instance, updating the column counter.
-/
public def field [Field α] : RowReader α := do
  let v ← Field.get (← read) (← getThe Int32)
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
-/
public class Row (α : Type) where
  read : RowReader α

public instance [Field α] : Row α where
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

-- Compatibility shim for 4.27.0-rc1
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

namespace Interpolation
open Lean

/--
An interpolated query that creates a prepared statement.
-/
syntax term "sql!" interpolatedStr(term) : term

/--
An interpolated query is run via {name}`SQLite.exec`, discarding the results.
-/
syntax term "exec!" interpolatedStr(term) : term

/--
An interpolated query that creates an iterator into the results.
-/
syntax term "query!" interpolatedStr(term) (&" as " term)? : term

macro_rules
  | `($db sql!%$tk $s) => do
    let chunks := s.raw.getArgs
    let mut sqlStx ← `("")
    let mut args := #[]
    let stmtName := mkIdentFrom tk `stmt
    for c in chunks do
      if let some lit := c.isInterpolatedStrLit? then
        sqlStx ← `($sqlStx ++ $(quote lit))
      else
        let index := args.size + 1
        args := args.push (← ``(ToSQLite.bind $stmtName (Nat.toInt32 $(quote index)) $(⟨c⟩)))
        sqlStx ← `($sqlStx ++ $(quote s!"?{index}"))

    let stx ← `(((let db := $db; SQLite.prepare db $sqlStx >>= fun $stmtName =>
        (([$(args),*] : List (IO Unit)).forM id) *>
        return $stmtName) : IO Stmt))

    return stx
  | `($db exec!%$tk $s) => do
    `(($db sql!%$tk $s >>= Stmt.exec : IO Unit))
  | `($db query!%$tk $s) => do
    `($db sql!%$tk $s <&> Stmt.results)
  | `($db query!%$tk $s as $ty) => do
    `($db sql!%$tk $s <&> Stmt.resultsAs $ty)
