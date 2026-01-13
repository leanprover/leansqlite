/-
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
Author: David Thrane Christiansen
-/
module
public import SQLite.FFI

set_option doc.verso true


public structure SQLite where
  filename : System.FilePath
  connection : SQLite.FFI.Conn
deriving Repr

namespace SQLite

public def «open» (filename : System.FilePath) : IO SQLite := do
  let connection ← FFI.«open» filename.toString
  return { filename, connection }

/--
Sets the busy timeout for this database connection.

When a table is locked, SQLite will retry the operation for up to {lit}`ms` milliseconds before
returning {lit}`SQLITE_BUSY`. Setting this to 0 (the default) means operations fail immediately when
encountering a lock.

This is essential for concurrent access scenarios where multiple connections or processes may access
the same database file. For example, setting a 5-second timeout allows operations to wait for locks
to be released rather than failing immediately.

This sets a busy handler that sleeps progressively longer between retries. For custom busy handler
logic, use {lit}`sqlite3_busy_handler()` in the C API instead (not currently exposed).

The timeout persists for the lifetime of the connection and can be changed at any time.
-/
public def busyTimeout (db : SQLite) (ms : Int32) : IO Unit :=
  FFI.busyTimeout db.connection ms

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

end Stmt

/--
Executes SQL that doesn't return data.

Useful for DDL and transaction control.
-/
public def exec (db : SQLite) (sql : String) : IO Unit :=
  FFI.exec db.connection sql

/--
Returns the rowid of the most recent successful INSERT into a rowid table.

SQLite tracks an internal row ID column for every table (an {lit}`INTEGER PRIMARY KEY` column
becomes an alias for this column).

If no successful {lit}`INSERT`s have occurred on this connection, returns 0. If the most recent
{lit}`INSERT` was into a table without a rowid (e.g., {lit}`WITHOUT ROWID` tables), the return value
is undefined.

The last rowid is specific to each connection and is not affected by other connections to the same
file.
-/
public def lastInsertRowId (db : SQLite) : IO Int64 :=
  FFI.lastInsertRowId db.connection

/--
Returns the number of rows modified, inserted, or deleted by the most recently completed
{lit}`INSERT`, {lit}`UPDATE`, or {lit}`DELETE` statement.

This count **excludes**:
- Rows modified by triggers
- Rows modified by foreign key actions
- Rows modified by {lit}`REPLACE` constraint resolution
- Changes to views intercepted by {lit}`INSTEAD OF` triggers

Executing any other type of SQL statement (including {lit}`SELECT` and DDL) does **not** modify
the value returned by this function—it remains unchanged until the next data modification statement.

Returns 0 if:
- No rows were affected by the most recent data modification statement
- No {lit}`INSERT`, {lit}`UPDATE`, or {lit}`DELETE` has been executed on this connection yet

**Note**: This value is connection-specific. If a separate thread makes changes on the same
database connection while this function runs, the value returned is unpredictable.

Useful for verifying that a statement had the expected effect, such as checking whether an
{lit}`UPDATE` or {lit}`DELETE` with a {lit}`WHERE` clause matched any rows.
-/
public def changes (db : SQLite) : IO Int64 :=
  FFI.changes db.connection

/--
Transaction modes.
-/
public inductive TransactionMode where
  /-- Default: lock acquired on first read/write -/
  | deferred : TransactionMode
  /-- Write lock acquired immediately -/
  | immediate : TransactionMode
  /-- Exclusive lock acquired immediately -/
  | exclusive : TransactionMode
deriving Repr, BEq

/-- Begins a transaction with the specified mode -/
public def beginTransaction (db : SQLite) (mode : TransactionMode := .deferred) : IO Unit :=
  let kw :=
    match mode with
    | .deferred => "DEFERRED"
    | .immediate => "IMMEDIATE"
    | .exclusive => "EXCLUSIVE"
  db.exec s!"BEGIN {kw} TRANSACTION"

/-- Commits the current transaction. -/
public def commit (db : SQLite) : IO Unit :=
  db.exec "COMMIT"

/-- Rolls the current transaction back. -/
public def rollback (db : SQLite) : IO Unit :=
  db.exec "ROLLBACK"

/--
Executes an IO action within a transaction, automatically committing or rolling back.

If the action succeeds, the transaction is committed. If it throws an exception,
the transaction is rolled back before re-throwing the exception.
-/
public def transaction (db : SQLite) (action : IO α) (mode : TransactionMode := .deferred) : IO α := do
  beginTransaction db mode
  try
    let result ← action
    commit db
    return result
  catch e =>
    rollback db
    throw e

end SQLite
