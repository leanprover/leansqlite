/-
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
Author: David Thrane Christiansen
-/
module
public import SQLite.FFI

public structure SQLite where
  filename : System.FilePath
  connection : SQLite.FFI.Conn
deriving Repr

namespace SQLite

public def «open» (filename : System.FilePath) : IO SQLite := do
  let connection ← FFI.«open» filename.toString
  return { filename, connection }

public structure Stmt where
  db : SQLite
  stmt : FFI.Stmt
deriving Repr

public def prepare (db : SQLite) (sql : String) : IO Stmt := do
  let stmt ← FFI.prepare db.connection sql
  return { db, stmt }

namespace Stmt

public def columnText (stmt : Stmt) (column : Int32) : IO String :=
  FFI.columnText stmt.stmt column

public def columnBlob (stmt : Stmt) (column : Int32) : IO ByteArray :=
  FFI.columnBlob stmt.stmt column

public def columnDouble (stmt : Stmt) (column : Int32) : IO Float :=
  FFI.columnDouble stmt.stmt column

public def columnInt (stmt : Stmt) (column : Int32) : IO Int32 :=
  FFI.columnInt stmt.stmt column

public def columnInt64 (stmt : Stmt) (column : Int32) : IO Int64 :=
  FFI.columnInt64 stmt.stmt column

end Stmt

public def Stmt.columnCount (stmt : Stmt) : UInt32 :=
  FFI.columnCount stmt.stmt

namespace Value

/-- SQLite data types -/
public inductive DataType where
  | integer : DataType
  | float : DataType
  | text : DataType
  | blob : DataType
  | null : DataType
deriving Repr, BEq, Inhabited

instance : ToString DataType where
  toString
    | .integer => "INTEGER"
    | .float => "FLOAT"
    | .text => "TEXT"
    | .blob => "BLOB"
    | .null => "NULL"

/-- Converts SQLite type code to DataType -/
def DataType.fromCode (code : Int32) : DataType :=
  match code with
  | 1 => DataType.integer
  | 2 => DataType.float
  | 3 => DataType.text
  | 4 => DataType.blob
  | 5 => DataType.null
  | _ => DataType.null  -- Unknown types treated as NULL

/-- Returns the type of a value -/
public def type (value : FFI.Value) : DataType :=
  DataType.fromCode (FFI.valueType value)

/-- Extracts text from a value -/
public def text (value : FFI.Value) : IO String :=
  FFI.valueText value

/-- Extracts blob from a value -/
public def blob (value : FFI.Value) : IO ByteArray :=
  FFI.valueBlob value

/-- Extracts double from a value -/
public def double (value : FFI.Value) : IO Float :=
  FFI.valueDouble value

/-- Extracts int64 from a value -/
public def int64 (value : FFI.Value) : IO Int64 :=
  FFI.valueInt64 value

end Value

namespace Stmt

public def bindText (stmt : Stmt) (index : Int32) (text : String) : IO Unit :=
  FFI.bindText stmt.stmt index text

public def bindDouble (stmt : Stmt) (index : Int32) (value : Float) : IO Unit :=
  FFI.bindDouble stmt.stmt index value

public def bindInt (stmt : Stmt) (index : Int32) (value : Int32) : IO Unit :=
  FFI.bindInt stmt.stmt index value

public def bindInt64 (stmt : Stmt) (index : Int32) (value : Int64) : IO Unit :=
  FFI.bindInt64 stmt.stmt index value

public def bindNull (stmt : Stmt) (index : Int32) : IO Unit :=
  FFI.bindNull stmt.stmt index

public def bindBlob (stmt : Stmt) (index : Int32) (blob : ByteArray) : IO Unit :=
  FFI.bindBlob stmt.stmt index blob

/-- Executes a statement. Returns true if a row is available, false if done. -/
public def step (stmt : Stmt) : IO Bool := do
  let result ← FFI.step stmt.stmt
  return result != 0

/-- Resets a statement to be executed again. -/
public def reset (stmt : Stmt) : IO Unit :=
  FFI.reset stmt.stmt

/--
Returns the type of a column in the result set.

The type is only meaningful if no automatic type conversions have occurred for the value in
question. After a type conversion, the result of calling sqlite3_column_type() is undefined, though
harmless.
-/
public def columnType (stmt : Stmt) (column : Int32) : IO Value.DataType := do
  let code ← FFI.columnType stmt.stmt column
  return Value.DataType.fromCode code
