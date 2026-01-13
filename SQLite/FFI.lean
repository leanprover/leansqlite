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

def Stmt : Type := T.type
deriving Nonempty

public instance : Repr Stmt where
  reprPrec _ _ := "#<sqlite3_stmt *>"

def Value : Type := T.type
deriving Nonempty

public instance : Repr Value where
  reprPrec _ _ := "#<sqlite3_value *>"


@[extern "leansqlite_open"]
opaque «open» : String → IO Conn

@[extern "leansqlite_prepare"]
opaque prepare : @&Conn → String → IO Stmt

@[extern "leansqlite_column_text"]
opaque columnText : @&Stmt → Int32 → IO String

@[extern "leansqlite_column_blob"]
opaque columnBlob : @&Stmt → Int32 → IO ByteArray

@[extern "leansqlite_column_double"]
opaque columnDouble : @&Stmt → Int32 → IO Float

@[extern "leansqlite_column_int"]
opaque columnInt : @&Stmt → Int32 → IO Int32

@[extern "leansqlite_column_int64"]
opaque columnInt64 : @&Stmt → Int32 → IO Int64

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

@[extern "leansqlite_column_count"]
opaque columnCount : @&Stmt → UInt32

@[extern "leansqlite_column_type"]
opaque columnType : @&Stmt → Int32 → IO Int32

@[extern "leansqlite_bind_text"]
opaque bindText : @&Stmt → Int32 → String → IO Unit

@[extern "leansqlite_bind_double"]
opaque bindDouble : @&Stmt → Int32 → Float → IO Unit

@[extern "leansqlite_bind_int"]
opaque bindInt : @&Stmt → Int32 → Int32 → IO Unit

@[extern "leansqlite_bind_int64"]
opaque bindInt64 : @&Stmt → Int32 → Int64 → IO Unit

@[extern "leansqlite_bind_null"]
opaque bindNull : @&Stmt → Int32 → IO Unit

@[extern "leansqlite_bind_blob"]
opaque bindBlob : @&Stmt → Int32 → ByteArray → IO Unit

@[extern "leansqlite_step"]
opaque step : @&Stmt → IO Int32

@[extern "leansqlite_reset"]
opaque reset : @&Stmt → IO Unit

@[extern "leansqlite_exec"]
opaque exec : @&Conn → String → IO Unit

@[extern "leansqlite_last_insert_rowid"]
opaque lastInsertRowId : @&Conn → IO Int64

@[extern "leansqlite_changes"]
opaque changes : @&Conn → IO Int64

@[extern "leansqlite_busy_timeout"]
opaque busyTimeout : @&Conn → Int32 → IO Unit
