/-
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
Author: David Thrane Christiansen
-/
module
-- This module serves as the root of the `Leansqlite` library.
-- Import modules here that should be built as part of the library.
public import SQLite.FFI
public import SQLite.LowLevel
import Std.Data.Iterators

namespace SQLite

section Interpolation
open Lean

public class ToSQLite α where
  bind : Stmt → Int32 → α → IO Unit

public instance : ToSQLite String where
  bind := Stmt.bindText

public instance : ToSQLite ByteArray where
  bind := Stmt.bindBlob

public instance : ToSQLite Int32 where
  bind := Stmt.bindInt

public instance : ToSQLite Int64 where
  bind := Stmt.bindInt64

public instance : ToSQLite Float where
  bind := Stmt.bindDouble

public instance : ToSQLite Unit where
  bind stmt index _ := Stmt.bindNull stmt index

public instance : ToSQLite Nat where
  bind stmt i n := Stmt.bindText stmt i (toString n)

syntax term "sql!" interpolatedStr(term) : term

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
        pure $stmtName) : IO Stmt))

    return stx

end Interpolation


public class Field α where
  get : Stmt → Int32 → IO α

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

public abbrev RowReader α := ReaderT Stmt (StateRefT Int32 IO) α

namespace RowReader

public def field [Field α] : RowReader α := do
  let v ← Field.get (← read) (← getThe Int32)
  modify (· + 1)
  return v

public def run (stmt : Stmt) (act : RowReader α) : IO α := do
  -- The initial column index is 0 here. Binding parameters is 1-indexed, reading columns is
  -- 0-indexed in SQLite.
  return (← ReaderT.run act stmt |>.run 0).1

end RowReader

public class Row α where
  read : RowReader α

public instance [Field α] : Row α where
  read := RowReader.field

public instance [Row α] [Row β] : Row (α × β) where
  read := do return (← Row.read, ← Row.read)

public structure QueryIterator β where
  [isRow : Row β]
  stmt : Stmt

open Std

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

public def Stmt.results [Row α] (stmt : Stmt) : @IterM (QueryIterator α) IO α where
  internalState := QueryIterator.mk stmt

public def Stmt.resultsAs (α : Type) [Row α] (stmt : Stmt) : @IterM (QueryIterator α) IO α := stmt.results
