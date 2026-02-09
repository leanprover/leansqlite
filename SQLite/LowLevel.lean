/-
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
Author: David Thrane Christiansen
-/
module
import all SQLite.FFI
public import SQLite.FFI

set_option doc.verso true
set_option linter.missingDocs true

/-- A connection to a SQLite database. -/
public structure SQLite where
  /-- The filename from which the database was opened. -/
  filename : System.FilePath
  /-- The underlying FFI connection object. -/
  connection : SQLite.FFI.Conn
deriving Repr

namespace SQLite

/--
Opens a file as a SQLite connection.
-/
public def «open» (filename : System.FilePath) : IO SQLite := do
  let connection ← FFI.«open» filename.toString
  return { filename, connection }

/--
The read/write mode used to open a SQLite file.
-/
public inductive Mode where
  /-- Open in read-only mode. The database must already exist. -/
  | readonly
  /-- Open in read-write mode. The database must already exist. -/
  | readWrite
  /-- Open in read-write mode and create the database if it doesn't exist. -/
  | readWriteCreate
deriving Repr, BEq, Hashable, Ord, Inhabited

def Mode.toInt : Mode → Int32
  | .readonly => 0x00000001
  | .readWrite => 0x00000002
  | .readWriteCreate => 0x00000002 ||| 0x00000004

/--
The threading model to be used by a SQLite connection.
-/
public inductive Threading where
  /--
  The new database connection will use the "multi-thread" threading mode. This means that separate
  threads are allowed to use SQLite at the same time, as long as each thread is using a different
  database connection.
  -/
  | nomutex
  /--
  The new database connection will use the "serialized" threading mode. This means the multiple
  threads can safely attempt to use the same database connection at the same time. (Mutexes will
  block any actual concurrency, but in this mode there is no harm in trying.)
  -/
  | fullmutex
deriving Repr, BEq, Hashable, Ord, Inhabited

def Threading.toInt : Threading → Int32
  | .nomutex => 0x00008000
  | .fullmutex => 0x00010000

/--
The flags that control the details of opening a SQLite file.
-/
public structure OpenFlags where
  /--
  Whether the file should be opened read-only or read-write, and whether it should be created if it
  doesn't already exist.
  -/
  mode : Mode := .readWriteCreate
  /-- Interpret the filename as a URI. -/
  uri : Bool := false
  /-- Open as an in-memory database. -/
  memory : Bool := false
  /-- Which threading model to use. -/
  threading : Option Threading := none
  -- The cache flag is intentionally omitted, because it only exists in SQLite for historical
  -- compatibility and is discouraged from use.
deriving Repr, BEq, Hashable, Ord, Inhabited

@[inherit_doc Mode.readonly]
public def OpenFlags.readonly : OpenFlags where
  mode := .readonly

@[inherit_doc Mode.readWrite]
public def OpenFlags.readWrite : OpenFlags where
  mode := .readWrite

@[inherit_doc Mode.readWriteCreate]
public def OpenFlags.readWriteCreate : OpenFlags where
  mode := .readWriteCreate

def OpenFlags.toInt (flags : OpenFlags) : Int32 :=
  let { mode, uri, memory, threading } := flags
  mode.toInt |||
  (if uri then 0x00000040 else 0) |||
  (if memory then 0x00000080 else 0) |||
  (threading.map (·.toInt)).getD 0

/--
Opens a database with specific flags and an optional VFS name.

This is a more flexible version of {name}`open` that allows fine-grained control over how the
database is opened.

The {name}`vfs` parameter specifies the name of the VFS module to use. Pass {lean}`none` (the default)
to use the default VFS.
-/
public def openWith (filename : System.FilePath) (flags : OpenFlags) (vfs : Option String := none) : IO SQLite := do
  let flagBits := flags.toInt
  let connection ← FFI.openV2 filename.toString flagBits vfs
  return { filename, connection }

/--
Sets the busy timeout for this database connection.

When a table is locked, SQLite will retry the operation for up to {name}`ms` milliseconds before
returning an error. Setting this to {lean (type := "Int32")}`0` (the default) means operations fail
immediately when encountering a lock.

This sets a busy handler that sleeps progressively longer between retries. For custom busy handler
logic, use {lit}`sqlite3_busy_handler()` in the C API instead (not currently exposed).

The timeout persists for the lifetime of the connection and can be changed at any time.
-/
public def busyTimeout (db : SQLite) (ms : Int32) : IO Unit :=
  FFI.busyTimeout db.connection ms

/--
A prepared statement. Prepared statements are the primary interface to interacting with SQLite data.
-/
public structure Stmt where
  db : SQLite
  private stmt : FFI.Stmt

instance : Repr Stmt where
  reprPrec := private fun
    | { db, stmt }, _ =>
      .group <|
        .indentD ("{" ++ .line ++
          .group ("db :=" ++ .line ++ repr db ++ ",") ++ .line ++
          .group ("stmt :=" ++ .line ++ repr stmt)) ++
        .line ++ "}"

/--
Prepares a statement.

This function accepts only a single SQL statement. If multiple semicolon-separated
statements are provided, an error is returned. For executing multiple statements without
parameters, use `SQLite.exec`.

Statements may contain host parameters that are later provided with data using the following syntax:
 - {lit}`?` for positional parameters
 - {lit}`?NNN` for positional parameters with manually-specified indices
 - {lit}`:AAAA` for a parameter named {lit}`AAAA` (where {lit}`AAAA` is a SQL identifier)
 - {lit}`@AAAA` for a parameter named {lit}`@AAAA` (where {lit}`AAAA` is a SQL identifier)
 - {lit}`$AAAA` for a parameter named {lit}`AAAA` (where {lit}`AAAA` is a TCL identifier)

Parameters are numbered starting at 1.
-/
public def prepare (db : SQLite) (sql : String) : IO Stmt := do
  let stmt ← FFI.prepare db.connection sql
  return { db, stmt }

namespace Stmt

/-- Executes a statement. Returns {lean}`true` if a row is available, {lean}`false` if done. -/
public def step (stmt : Stmt) : IO Bool := do
  let result ← FFI.step stmt.stmt
  return result != 0

/--
Extracts a string from (0-indexed) {name}`column`.

This may perform type conversion of the underlying data, mutating it.
-/
public def columnText (stmt : Stmt) (column : Int32) : IO String :=
  FFI.columnText stmt.stmt column

/--
Extracts bytes from (0-indexed) {name}`column`.

This may perform type conversion of the underlying data, mutating it.
-/
public def columnBlob (stmt : Stmt) (column : Int32) : IO ByteArray :=
  FFI.columnBlob stmt.stmt column

/--
Extracts a float from (0-indexed) {name}`column`.

This may perform type conversion of the underlying data, mutating it.
-/
public def columnDouble (stmt : Stmt) (column : Int32) : IO Float :=
  FFI.columnDouble stmt.stmt column

/--
Extracts a 32-bit integer from (0-indexed) {name}`column`.

This may perform type conversion of the underlying data, mutating it.
-/
public def columnInt (stmt : Stmt) (column : Int32) : IO Int32 :=
  FFI.columnInt stmt.stmt column

/--
Extracts a 64-bit integer from (0-indexed) {name}`column`.

This may perform type conversion of the underlying data, mutating it.
-/
public def columnInt64 (stmt : Stmt) (column : Int32) : IO Int64 :=
  FFI.columnInt64 stmt.stmt column

/-- Returns the name of a column in the result set (0-indexed). -/
public def columnName (stmt : Stmt) (column : Int32) : IO String :=
  FFI.columnName stmt.stmt column

/-- Returns the original SQL text of the prepared statement. -/
public def sql (stmt : Stmt) : IO String :=
  FFI.sql stmt.stmt

/-- Returns the SQL text with bound parameters expanded (values filled in). -/
public def expandedSql (stmt : Stmt) : IO String :=
  FFI.expandedSql stmt.stmt

/--
Checks whether a prepared statement will only read from the database.

Returns {name}`true` if the statement is read-only (e.g., {lit}`SELECT`), {name}`false` if it
might modify the database (e.g., {lit}`INSERT`, {lit}`UPDATE`, {lit}`DELETE`).

{lit}`BEGIN`, {lit}`COMMIT`, and {lit}`ROLLBACK` are considered read-only by this function.
-/
public def isReadonly (stmt : Stmt) : IO Bool :=
  FFI.stmtReadonly stmt.stmt

/--
Checks whether a statement has been stepped at least once and has not been reset.

Returns {name}`true` if {lean}`stmt` has a current row or has been stepped (even if it is done), or
{name}`false` if it hasn't been stepped yet or has been reset.

Useful to determine if you need to call {lit}`reset` before re-executing a statement with different
parameters.
-/
public def isBusy (stmt : Stmt) : IO Bool :=
  FFI.stmtBusy stmt.stmt

/--
Returns the number of columns in the current result row.

Unlike `Stmt.columnCount` which returns the total number of columns the statement _could_ return,
this returns 0 if no row is currently available (before the first {name}`step` or after {name}`step`
returns {name}`false`).

Returns the actual column count if a row is available.
-/
public def dataCount (stmt : Stmt) : IO UInt32 :=
  FFI.dataCount stmt.stmt

/--
Returns the name of the table from which a result column originates.

Returns an empty string if metadata is not available (e.g. if the column is an expression). This
only works for columns that directly reference a table column, not for computed expressions.
-/
public def columnTableName (stmt : Stmt) (column : Int32) : IO String :=
  FFI.columnTableName stmt.stmt column

/--
Returns the original column name from the table definition.

Returns an empty string if metadata is not available (e.g. if the column is an expression).
For aliased columns (e.g., {lit}`SELECT name AS user_name`), this returns the original
column name ({lit}`name`), while {name}`columnName` returns the alias ({lit}`user_name`).
-/
public def columnOriginName (stmt : Stmt) (column : Int32) : IO String :=
  FFI.columnOriginName stmt.stmt column

/--
Returns the name of the database from which a result column originates.
The database name is typically {lit}`"main"` for the primary database, {lit}`"temp"` for
temporary tables, or the name specified in an {lit}`ATTACH DATABASE` statement.

Returns an empty string if metadata is not available (e.g. if the column is an expression).
This only works for columns that directly reference a table column, not for computed expressions.
-/
public def columnDatabaseName (stmt : Stmt) (column : Int32) : IO String :=
  FFI.columnDatabaseName stmt.stmt column

end Stmt

/--
Returns the number of columns in the result set returned by the prepared statement.

If the result is 0, that means the prepared statement returns no data (for example an
{lit}`UPDATE`). However, just because the result is positive does not mean that one or more rows of
data will be returned. A {lit}`SELECT` statement will always have a positive column count; however,
depending on the {lit}`WHERE` clause constraints and the table content, it might return no rows.
-/
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
deriving Repr, BEq, Inhabited, Hashable, Ord

/-- Returns {lean}`true` if the provided type is {name}`DataType.null`. -/
public def DataType.isNull : DataType → Bool
  | .null => true
  | _ => false

instance : ToString DataType where
  toString
    | .integer => "INTEGER"
    | .float => "FLOAT"
    | .text => "TEXT"
    | .blob => "BLOB"
    | .null => "NULL"

/-- Converts SQLite type code to {name}`DataType` -/
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

/-- Returns the number of SQL parameters in the prepared statement. -/
public def bindParameterCount (stmt : Stmt) : IO UInt32 :=
  FFI.bindParameterCount stmt.stmt

/--
Returns the index of a named parameter.

Named parameters can use {lit}`:name`, {lit}`@name`, or {lit}`$name` syntax.
Returns 0 if the name is not found.

Parameter indices are 1-based.
-/
public def bindParameterIndex (stmt : Stmt) (param : String) : Int32 :=
  FFI.bindParameterIndex stmt.stmt param

/--
Returns the name of a parameter at the given index (1-based).

Returns an empty string if the parameter is unnamed (uses {lit}`?` syntax).
-/
public def bindParameterName (stmt : Stmt) (index : Int32) : IO String :=
  FFI.bindParameterName stmt.stmt index

/--
Binds a string to the host parameter with the given {name}`index`. Parameter indices start at 1, and
the index of a named parameter can be found via {name}`bindParameterIndex`.
-/
public def bindText (stmt : Stmt) (index : Int32) (text : String) : IO Unit :=
  FFI.bindText stmt.stmt index text
/--
Binds a floating-point number to the host parameter with the given {name}`index`. Parameter indices
start at 1, and the index of a named parameter can be found via {name}`bindParameterIndex`.
-/
public def bindFloat (stmt : Stmt) (index : Int32) (value : Float) : IO Unit :=
  FFI.bindDouble stmt.stmt index value

/--
Binds a 32-bit integer to the host parameter with the given {name}`index`. Parameter indices start at 1, and
the index of a named parameter can be found via {name}`bindParameterIndex`.
-/
public def bindInt32 (stmt : Stmt) (index : Int32) (value : Int32) : IO Unit :=
  FFI.bindInt stmt.stmt index value

/--
Binds a 64-bit integer to the host parameter with the given {name}`index`. Parameter indices start at 1, and
the index of a named parameter can be found via {name}`bindParameterIndex`.
-/
public def bindInt64 (stmt : Stmt) (index : Int32) (value : Int64) : IO Unit :=
  FFI.bindInt64 stmt.stmt index value

/--
Binds a null value to the host parameter with the given {name}`index`. Parameter indices start at 1, and
the index of a named parameter can be found via {name}`bindParameterIndex`.
-/
public def bindNull (stmt : Stmt) (index : Int32) : IO Unit :=
  FFI.bindNull stmt.stmt index

/--
Binds a byte array as a SQL BLOB to the host parameter with the given {name}`index`. Parameter
indices start at 1, and the index of a named parameter can be found via {name}`bindParameterIndex`.
-/
public def bindBlob (stmt : Stmt) (index : Int32) (blob : ByteArray) : IO Unit :=
  FFI.bindBlob stmt.stmt index blob

/--
Executes a statement, disregarding the availability of results.

Queries should use {name}`step`, which indicates whether a row is available.
-/
public def exec (stmt : Stmt) : IO Unit := discard stmt.step

/-- Resets a statement to be executed again. -/
public def reset (stmt : Stmt) : IO Unit :=
  FFI.reset stmt.stmt

/--
Clears all bindings on a prepared statement, setting them back to {lit}`NULL`.

This is useful after calling {name}`reset` because parameter bindings persist across resets. Prior to
running {name}`reset` on a statement and and re-executing it with different parameters, all
parameters should be re-bound; using {name}`clearBindings` ensures that no old values persist.
-/
public def clearBindings (stmt : Stmt) : IO Unit :=
  FFI.clearBindings stmt.stmt

/--
Returns the type of a column in the result set.

The type is only meaningful if no automatic type conversions have occurred for the value in
question. After a type conversion, the result of calling sqlite3_column_type() is undefined, though
harmless.
-/
public def columnType (stmt : Stmt) (column : Int32) : IO Value.DataType := do
  let code ← FFI.columnType stmt.stmt column
  return Value.DataType.fromCode code

/--
Checks whether a column is {lit}`NULL`.

This check is only meaningful if no automatic type conversions have occurred for the value in
question.
-/
public def columnNull (stmt : Stmt) (column : Int32) : IO Bool := do
  let t ← stmt.columnType column
  return t == .null


end Stmt

/--
Executes SQL that doesn't return data.

This function supports executing multiple semicolon-separated SQL statements in a single call.
If any statement fails, execution stops and an error is returned. To ensure all statements
execute atomically (all succeed or all roll back), wrap the call in a transaction.
For single statements with parameters, use {lean}`prepare` or the {lit}`sql!` macro instead.
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

This count excludes:
- Rows modified by triggers
- Rows modified by foreign key actions
- Rows modified by {lit}`REPLACE` constraint resolution
- Changes to views intercepted by {lit}`INSTEAD OF` triggers

Executing any other type of SQL statement (including {lit}`SELECT` and DDL) does not modify
the value returned by this function. It remains unchanged until the next data modification statement.

Returns 0 if:
- No rows were affected by the most recent data modification statement
- No {lit}`INSERT`, {lit}`UPDATE`, or {lit}`DELETE` has been executed on this connection yet

*Warning*: This value is connection-specific. If a separate thread makes changes on the same
database connection while this function runs, the value returned is unpredictable.
-/
public def changes (db : SQLite) : IO Int64 :=
  FFI.changes db.connection

/--
Returns the total number of rows inserted, modified, or deleted since the database connection was opened.

Unlike {name}`changes`, which returns the count for the most recent statement, this returns the
cumulative total across all {lit}`INSERT`, {lit}`UPDATE`, and {lit}`DELETE` statements executed
on this connection.

This count excludes the same operations as {name}`changes`:
- Rows modified by triggers
- Rows modified by foreign key actions
- Rows modified by {lit}`REPLACE` constraint resolution
- Changes to views intercepted by {lit}`INSTEAD OF` triggers
-/
public def totalChanges (db : SQLite) : IO Int64 :=
  FFI.totalChanges db.connection

/--
Checks whether the database connection is in autocommit mode.

Returns {name}`true` if the connection is *not* in a transaction (autocommit mode is on). Returns
{name}`false` if the connection is currently in a transaction (autocommit mode is off).

This is useful for checking transaction state before performing operations that require or prohibit
being in a transaction.

Autocommit mode is the default state. It is disabled when a transaction begins, and re-enabled on
commit or rollback.
-/
public def inAutocommit (db : SQLite) : IO Bool :=
  FFI.getAutocommit db.connection

/--
Checks whether the database connection is currently in a transaction.

Returns {name}`true` if in a transaction, {name}`false` if not.

This is the inverse of {name}`inAutocommit`.
-/
public def inTransaction (db : SQLite) : IO Bool := do
  return !(← db.inAutocommit)

/--
Returns the filename of a database attached to this connection.

The {name}`dbName` parameter is typically:
- {lit}`"main"` - The primary database file
- {lit}`"temp"` - The temporary database (used for TEMP tables)
- Any name used in an {lit}`ATTACH DATABASE` statement

Returns {lean (type := "Option System.FilePath")}`none` if the database name is not found.

For in-memory databases ({lit}`:memory:`), this returns an empty string.
For temporary databases, the actual filename may be a system-generated path.
-/
public def dbFilename (db : SQLite) (dbName : String := "main") : IO (Option System.FilePath) := do
  let name ← FFI.dbFilename db.connection dbName
  return if name.isEmpty then none else some name

/--
Database access mode returned by {lit}`dbReadonly`.
-/
public inductive AccessMode where
  /-- The database is read-write. -/
  | readWrite : AccessMode
  /-- The database is read-only. -/
  | readOnly : AccessMode
deriving Repr, BEq, Hashable, DecidableEq, Inhabited, Ord

/--
Checks whether a database is opened in read-only mode.

The {name}`dbName` parameter is typically {lit}`"main"`, {lit}`"temp"`, or an attachment name.

-/
public def dbReadonly (db : SQLite) (dbName : String := "main") : IO (Option AccessMode) := do
  return match (← FFI.dbReadonly db.connection dbName) with
    | 1 => some .readOnly
    | 0 => some .readWrite
    | _ => none

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
deriving Repr, BEq, Hashable, Inhabited, Ord

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

/--
Enables SHA3 extension functions on this connection.

After calling this function, the following SQL functions become available:

: {lit}`sha3(X, SIZE)`

  Computes the {lit}`SHA3` hash of {lit}`X`. {lit}`SIZE` defaults to 256 and can be 224, 256, 384, or 512.

: {lit}`sha3_agg(Y, SIZE)`

  An aggregate function that computes a hash over multiple rows.

: {lit}`sha3_query(SQL, SIZE)`

  Executes {lit}`SQL` and hashes all results deterministically.

It is safe to call this function multiple times on the same connection; subsequent calls simply
re-register the functions.
-/
public def enableSha3 (db : SQLite) : IO Unit :=
  FFI.Extensions.shathreeInit db.connection

end SQLite
