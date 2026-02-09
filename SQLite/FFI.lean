/-
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
Author: David Thrane Christiansen
-/
module
namespace SQLite.FFI


@[extern "leansqlite_initialize"]
private opaque init : IO Unit
builtin_initialize init

opaque T : NonemptyType.{0}

instance : Nonempty T.type := T.property

public section

def Conn : Type := T.type
deriving Nonempty

public instance : Repr Conn where
  reprPrec _ _ := "#<sqlite3 *>"


-- The Stmt type is a representation of a C pointer to a SQLite statement. It is private because
-- it's only valid as long as the connection lasts. The wrapper in `LowLevel.lean` ensures that the
-- connection is retained along with it. While the DB is opened in a way that causes it to return
-- SQLITE_BUSY if closed with active statements, we don't have a way to recover from that, so
-- there's a risk of leaking connections.
private def Stmt : Type := T.type
deriving Nonempty

private instance : Repr Stmt where
  reprPrec _ _ := "#<sqlite3_stmt *>"

def Value : Type := T.type
deriving Nonempty

public instance : Repr Value where
  reprPrec _ _ := "#<sqlite3_value *>"


@[extern "leansqlite_open"]
opaque «open» : String → IO Conn

@[extern "leansqlite_open_v2"]
opaque openV2 : String → Int32 → Option String → IO Conn

@[extern "leansqlite_prepare"]
private opaque prepare : @&Conn → String → IO Stmt

@[extern "leansqlite_column_text"]
private opaque columnText : @&Stmt → Int32 → IO String

@[extern "leansqlite_column_blob"]
private opaque columnBlob : @&Stmt → Int32 → IO ByteArray

@[extern "leansqlite_column_double"]
private opaque columnDouble : @&Stmt → Int32 → IO Float

@[extern "leansqlite_column_int"]
private opaque columnInt : @&Stmt → Int32 → IO Int32

@[extern "leansqlite_column_int64"]
private opaque columnInt64 : @&Stmt → Int32 → IO Int64

@[extern "leansqlite_value_type"]
opaque valueType : @&Value → Int32

@[extern "leansqlite_value_text"]
opaque valueText : Value → IO String

@[extern "leansqlite_value_blob"]
opaque valueBlob : Value → IO ByteArray

@[extern "leansqlite_value_double"]
opaque valueDouble : Value → IO Float

@[extern "leansqlite_value_int64"]
opaque valueInt64 : Value → IO Int64

@[extern "leansqlite_sql"]
private opaque sql : @&Stmt → IO String

@[extern "leansqlite_expanded_sql"]
private opaque expandedSql : @&Stmt → IO String

@[extern "leansqlite_column_count"]
private opaque columnCount : @&Stmt → UInt32

@[extern "leansqlite_column_type"]
private opaque columnType : @&Stmt → Int32 → IO Int32

@[extern "leansqlite_bind_parameter_count"]
private opaque bindParameterCount : @&Stmt → IO UInt32

@[extern "leansqlite_bind_parameter_index"]
private opaque bindParameterIndex : @&Stmt → String → Int32

@[extern "leansqlite_bind_parameter_name"]
private opaque bindParameterName : @&Stmt → Int32 → IO String

@[extern "leansqlite_bind_text"]
private opaque bindText : @&Stmt → Int32 → String → IO Unit

@[extern "leansqlite_bind_double"]
private opaque bindDouble : @&Stmt → Int32 → Float → IO Unit

@[extern "leansqlite_bind_int"]
private opaque bindInt : @&Stmt → Int32 → Int32 → IO Unit

@[extern "leansqlite_bind_int64"]
private opaque bindInt64 : @&Stmt → Int32 → Int64 → IO Unit

@[extern "leansqlite_bind_null"]
private opaque bindNull : @&Stmt → Int32 → IO Unit

@[extern "leansqlite_bind_blob"]
private opaque bindBlob : @&Stmt → Int32 → ByteArray → IO Unit

@[extern "leansqlite_step"]
private opaque step : @&Stmt → IO Int32

@[extern "leansqlite_reset"]
private opaque reset : @&Stmt → IO Unit

@[extern "leansqlite_clear_bindings"]
private opaque clearBindings : @&Stmt → IO Unit

@[extern "leansqlite_exec"]
opaque exec : @&Conn → String → IO Unit

@[extern "leansqlite_last_insert_rowid"]
opaque lastInsertRowId : @&Conn → IO Int64

@[extern "leansqlite_changes"]
opaque changes : @&Conn → IO Int64

@[extern "leansqlite_busy_timeout"]
opaque busyTimeout : @&Conn → Int32 → IO Unit

@[extern "leansqlite_get_autocommit"]
opaque getAutocommit : @&Conn → IO Bool

@[extern "leansqlite_db_filename"]
opaque dbFilename : @&Conn → String → IO String

@[extern "leansqlite_total_changes"]
opaque totalChanges : @&Conn → IO Int64

@[extern "leansqlite_stmt_readonly"]
private opaque stmtReadonly : @&Stmt → IO Bool

@[extern "leansqlite_stmt_busy"]
private opaque stmtBusy : @&Stmt → IO Bool

@[extern "leansqlite_data_count"]
private opaque dataCount : @&Stmt → IO UInt32

@[extern "leansqlite_db_readonly"]
opaque dbReadonly : @&Conn → String → IO Int32

@[extern "leansqlite_column_table_name"]
private opaque columnTableName : @&Stmt → Int32 → IO String

@[extern "leansqlite_column_origin_name"]
private opaque columnOriginName : @&Stmt → Int32 → IO String

@[extern "leansqlite_column_database_name"]
private opaque columnDatabaseName : @&Stmt → Int32 → IO String

@[extern "leansqlite_column_name"]
private opaque columnName : @&Stmt → Int32 → IO String

namespace Extensions

@[extern "leansqlite_shathree_init"]
opaque shathreeInit : @&Conn → IO Unit

end Extensions
