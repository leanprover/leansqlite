/*
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
Author: David Thrane Christiansen
*/

// This library disables large file support to avoid linking issues with Lean's bundled libc.
// On 32-bit systems, this would limit database files to ~2GB. Since we don't have CI to test
// error handling in this situation, compilation fails.
// If emscripten becomes a target of interest, then this conditional can be made more specific.
#if defined(__i386__) || defined(_M_IX86) || defined(__arm__) ||                                   \
    (defined(__SIZEOF_POINTER__) && __SIZEOF_POINTER__ == 4)
#error "32-bit platforms are not supported by the FFI bindings."
#endif

#include <lean/lean.h>
#include <sqlite3.h>
#include <stdio.h>
#include <string.h>

// Export macro for FFI functions.
// On Windows, adding shathree.c (which has __declspec(dllexport) on its entry point)
// switches the linker to explicit export mode, requiring all exported symbols to be marked.
#ifdef _WIN32
#define LEANSQLITE_API __declspec(dllexport)
#else
#define LEANSQLITE_API __attribute__((visibility("default")))
#endif

// Forward declaration for SHA3 extension initialization (defined in shathree.c)
int sqlite3_shathree_init(sqlite3 *db, char **pzErrMsg, const void *pApi);

void leansqlite_connection_finalize(void *connection) {
  // Uses sqlite3_close instead of sqlite3_close_v2 because uses of the connection will have
  // references to it, so it won't be freed/finalized until they are.
  int code = sqlite3_close((sqlite3 *) connection);
  assert(code == SQLITE_OK);
}

// NOLINTNEXTLINE(misc-unused-parameters)
void leansqlite_connection_foreach(void *connection, b_lean_obj_arg arg) {
  return;
}

lean_external_class *leansqlite_connection_class = NULL;

lean_object *leansqlite_connection(sqlite3 *connection) {
  return lean_alloc_external(leansqlite_connection_class, connection);
}

sqlite3 *leansqlite_get_connection(lean_object *connection) {
  return (sqlite3 *) lean_get_external_data(connection);
}


void leansqlite_stmt_finalize(void *stmt) {
  sqlite3_finalize((sqlite3_stmt *) stmt);
  // The return code is not checked here. According to the docs:
  //
  // If the most recent evaluation of the statement encountered no errors or if the statement has
  // never been evaluated, then sqlite3_finalize() returns SQLITE_OK. If the most recent evaluation
  // of statement S failed, then sqlite3_finalize(S) returns the appropriate error code or extended
  // error code.
  //
  // It's not an indication of an API error to finalize a statement that was last used to do
  // something erroneous, so we just finalize it.
}

// NOLINTNEXTLINE(misc-unused-parameters)
void leansqlite_stmt_foreach(void *connection, b_lean_obj_arg arg) {
  return;
}

lean_external_class *leansqlite_stmt_class = NULL;

lean_object *leansqlite_stmt(sqlite3_stmt *stmt) {
  return lean_alloc_external(leansqlite_stmt_class, stmt);
}

sqlite3_stmt *leansqlite_get_stmt(lean_object *stmt_obj) {
  return (sqlite3_stmt *) lean_get_external_data(stmt_obj);
}

// NOLINTNEXTLINE(misc-unused-parameters)
void leansqlite_value_finalize(void *value) {
  // sqlite3_value objects are owned by SQLite and should not be freed
  // They are only valid during certain callback contexts
  return;
}

// NOLINTNEXTLINE(misc-unused-parameters)
void leansqlite_value_foreach(void *value, b_lean_obj_arg arg) {
  return;
}

lean_external_class *leansqlite_value_class = NULL;

lean_object *leansqlite_value(sqlite3_value *value) {
  return lean_alloc_external(leansqlite_value_class, value);
}

sqlite3_value *leansqlite_get_value(lean_object *value_obj) {
  return (sqlite3_value *) lean_get_external_data(value_obj);
}

LEANSQLITE_API
lean_obj_res leansqlite_initialize() {
  leansqlite_connection_class =
      lean_register_external_class(leansqlite_connection_finalize, leansqlite_connection_foreach);
  leansqlite_stmt_class =
      lean_register_external_class(leansqlite_stmt_finalize, leansqlite_stmt_foreach);
  leansqlite_value_class =
      lean_register_external_class(leansqlite_value_finalize, leansqlite_value_foreach);
  return lean_io_result_mk_ok(lean_box(0));
}

LEANSQLITE_API
lean_obj_res leansqlite_open(lean_obj_arg filename) {
  const char *filename_str = lean_string_cstr(filename);
  sqlite3 *db;
  int code = sqlite3_open(filename_str, &db);
  lean_dec(filename);
  if (code != SQLITE_OK) {
    lean_object *msg = lean_mk_string(sqlite3_errmsg(db));
    sqlite3_close(db);
    return lean_io_result_mk_error(lean_mk_io_error_other_error(code, msg));
  } else {
    return lean_io_result_mk_ok(leansqlite_connection(db));
  }
}

// Uses standard return convention: lean_obj_res
// filename is consumed: lean_obj_arg
// flags is a value: int32_t
// vfs is consumed: lean_obj_arg (empty string means NULL)
LEANSQLITE_API
lean_obj_res leansqlite_open_v2(lean_obj_arg filename, int32_t flags, lean_obj_arg vfs) {
  const char *filename_str = lean_string_cstr(filename);

  const char *vfs_str = NULL;
  // checks for none vs some: none is a scalar
  if (!lean_is_scalar(vfs)) {
    lean_object *lean_vfs_str = lean_ctor_get(vfs, 0);
    vfs_str = lean_string_cstr(lean_vfs_str);
  }

  sqlite3 *db;
  int code = sqlite3_open_v2(filename_str, &db, flags, vfs_str);
  lean_dec(filename);
  lean_dec(vfs);
  if (code != SQLITE_OK) {
    lean_object *msg = lean_mk_string(sqlite3_errmsg(db));
    sqlite3_close(db);
    return lean_io_result_mk_error(lean_mk_io_error_other_error(code, msg));
  } else {
    return lean_io_result_mk_ok(leansqlite_connection(db));
  }
}

LEANSQLITE_API
lean_obj_res leansqlite_prepare(b_lean_obj_arg connection, lean_obj_arg sql) {
  sqlite3 *db = leansqlite_get_connection(connection);
  const char *sql_str = lean_string_cstr(sql);
  const char *tail = sql_str;
  // -1 length means to read to the null terminator
  sqlite3_stmt *stmt;
  int code = sqlite3_prepare_v2(db, sql_str, -1, &stmt, &tail);
  if (code != SQLITE_OK) {
    lean_dec(sql);
    lean_object *msg = lean_mk_string(sqlite3_errmsg(db));
    // stmt is NULL on error, no need to finalize
    return lean_io_result_mk_error(lean_mk_io_error_other_error(code, msg));
  } else if (tail[0] != '\0') {
    // tail points into sql's buffer, so we must read it before decrementing sql
    lean_object *msg1 = lean_mk_string("Unprocessed SQL:");
    lean_object *msg2 = lean_mk_string(tail);
    lean_dec(sql);
    lean_object *msg = lean_string_append(msg1, msg2);
    // lean_string_append borrows only its second argument, and consumes its first, so msg1 doesn't
    // get decremented here.
    lean_dec(msg2);
    sqlite3_finalize(stmt);
    return lean_io_result_mk_error(lean_mk_io_error_other_error(code, msg));
  } else {
    lean_dec(sql);
    return lean_io_result_mk_ok(leansqlite_stmt(stmt));
  }
}

LEANSQLITE_API
lean_obj_res leansqlite_column_text(b_lean_obj_arg stmt_obj, int32_t column) {
  sqlite3_stmt *stmt_ptr = leansqlite_get_stmt(stmt_obj);
  const unsigned char *text = sqlite3_column_text(stmt_ptr, column);
  lean_object *result = lean_mk_string(text != NULL ? (const char *) text : "");
  return lean_io_result_mk_ok(result);
}

LEANSQLITE_API
lean_obj_res leansqlite_column_blob(b_lean_obj_arg stmt_obj, int32_t column) {
  sqlite3_stmt *stmt_ptr = leansqlite_get_stmt(stmt_obj);
  // sqlite3_column_blob must be called before sqlite3_column_bytes to avoid an implicit type
  // conversion that could invalidate the blob pointer.
  // See https://www.sqlite.org/c3ref/column_blob.html ("safest policy")
  const void *blob = sqlite3_column_blob(stmt_ptr, column);
  int bytes = sqlite3_column_bytes(stmt_ptr, column);

  // Allocate ByteArray
  lean_object *byte_array = lean_alloc_sarray(1, bytes, bytes);
  if (bytes > 0 && blob != NULL) {
    memcpy(lean_sarray_cptr(byte_array), blob, bytes);
  }

  return lean_io_result_mk_ok(byte_array);
}

LEANSQLITE_API
lean_obj_res leansqlite_column_double(b_lean_obj_arg stmt_obj, int32_t column) {
  sqlite3_stmt *stmt_ptr = leansqlite_get_stmt(stmt_obj);
  double value = sqlite3_column_double(stmt_ptr, column);
  return lean_io_result_mk_ok(lean_box_float(value));
}

LEANSQLITE_API
lean_obj_res leansqlite_column_int(b_lean_obj_arg stmt_obj, int32_t column) {
  sqlite3_stmt *stmt_ptr = leansqlite_get_stmt(stmt_obj);
  int32_t value = (int32_t) sqlite3_column_int(stmt_ptr, column);
  return lean_io_result_mk_ok(lean_box((uint32_t) value));
}

LEANSQLITE_API
lean_obj_res leansqlite_column_int64(b_lean_obj_arg stmt_obj, int32_t column) {
  sqlite3_stmt *stmt_ptr = leansqlite_get_stmt(stmt_obj);
  int64_t value = sqlite3_column_int64(stmt_ptr, column);
  return lean_io_result_mk_ok(lean_box_uint64(value));
}

LEANSQLITE_API
int32_t leansqlite_value_type(b_lean_obj_arg value_obj) {
  sqlite3_value *value = leansqlite_get_value(value_obj);
  return sqlite3_value_type(value);
}

LEANSQLITE_API
lean_obj_res leansqlite_value_text(lean_obj_arg value_obj) {
  sqlite3_value *value = leansqlite_get_value(value_obj);
  const unsigned char *text = sqlite3_value_text(value);
  lean_object *result = lean_mk_string(text != NULL ? (const char *) text : "");
  lean_dec(value_obj);
  return lean_io_result_mk_ok(result);
}

LEANSQLITE_API
lean_obj_res leansqlite_value_blob(lean_obj_arg value_obj) {
  sqlite3_value *value = leansqlite_get_value(value_obj);
  // sqlite3_value_blob must be called before sqlite3_value_bytes to avoid an implicit type
  // conversion that could invalidate the blob pointer.
  // See https://www.sqlite.org/c3ref/column_blob.html ("safest policy")
  const void *blob = sqlite3_value_blob(value);
  int bytes = sqlite3_value_bytes(value);
  lean_dec(value_obj);

  // Allocate ByteArray
  lean_object *byte_array = lean_alloc_sarray(1, bytes, bytes);
  if (bytes > 0 && blob != NULL) {
    memcpy(lean_sarray_cptr(byte_array), blob, bytes);
  }

  return lean_io_result_mk_ok(byte_array);
}

LEANSQLITE_API
lean_obj_res leansqlite_value_double(lean_obj_arg value_obj) {
  sqlite3_value *value = leansqlite_get_value(value_obj);
  double result = sqlite3_value_double(value);
  lean_dec(value_obj);
  return lean_io_result_mk_ok(lean_box_float(result));
}

LEANSQLITE_API
lean_obj_res leansqlite_value_int64(lean_obj_arg value_obj) {
  sqlite3_value *value = leansqlite_get_value(value_obj);
  int64_t result = sqlite3_value_int64(value);
  lean_dec(value_obj);
  return lean_io_result_mk_ok(lean_box_uint64(result));
}

LEANSQLITE_API
uint32_t leansqlite_column_count(b_lean_obj_arg stmt_obj) {
  sqlite3_stmt *stmt_ptr = leansqlite_get_stmt(stmt_obj);
  int count = sqlite3_column_count(stmt_ptr);
  return (uint32_t) count;
}

LEANSQLITE_API
lean_obj_res leansqlite_column_type(b_lean_obj_arg stmt_obj, int32_t column) {
  sqlite3_stmt *stmt_ptr = leansqlite_get_stmt(stmt_obj);
  int32_t type_code = sqlite3_column_type(stmt_ptr, column);
  return lean_io_result_mk_ok(lean_box((uint32_t) type_code));
}

LEANSQLITE_API
lean_obj_res leansqlite_sql(b_lean_obj_arg stmt_obj) {
  sqlite3_stmt *stmt_ptr = leansqlite_get_stmt(stmt_obj);
  const char *sql = sqlite3_sql(stmt_ptr);
  if (sql == NULL) {
    lean_object *msg =
        lean_mk_string("Failed to get SQL from statement. Either there was insufficient memory for "
                       "the string, or it would be longer than SQLITE_LIMIT_LENGTH.");
    return lean_io_result_mk_error(lean_mk_io_error_other_error(1, msg));
  } else {
    return lean_io_result_mk_ok(lean_mk_string(sql));
  }
}

LEANSQLITE_API
lean_obj_res leansqlite_expanded_sql(b_lean_obj_arg stmt_obj) {
  sqlite3_stmt *stmt_ptr = leansqlite_get_stmt(stmt_obj);
  char *sql = sqlite3_expanded_sql(stmt_ptr);
  if (sql == NULL) {
    lean_object *msg =
        lean_mk_string("Failed to get SQL from statement. Either there was insufficient memory for "
                       "the string, or it would be longer than SQLITE_LIMIT_LENGTH.");
    return lean_io_result_mk_error(lean_mk_io_error_other_error(1, msg));
  } else {
    lean_object *sql_str = lean_mk_string(sql);
    sqlite3_free(sql);
    return lean_io_result_mk_ok(sql_str);
  }
}

// TODO test
LEANSQLITE_API
lean_obj_res leansqlite_bind_parameter_count(b_lean_obj_arg stmt_obj) {
  sqlite3_stmt *stmt_ptr = leansqlite_get_stmt(stmt_obj);
  return lean_io_result_mk_ok(lean_box_uint32(sqlite3_bind_parameter_count(stmt_ptr)));
}

// TODO test
LEANSQLITE_API
int32_t leansqlite_bind_parameter_index(b_lean_obj_arg stmt_obj, lean_obj_arg name) {
  sqlite3_stmt *stmt_ptr = leansqlite_get_stmt(stmt_obj);
  const char *name_cstr = lean_string_cstr(name);
  int32_t result = sqlite3_bind_parameter_index(stmt_ptr, name_cstr);
  lean_dec(name);
  return result;
}

LEANSQLITE_API
lean_obj_res leansqlite_bind_parameter_name(b_lean_obj_arg stmt_obj, int32_t i) {
  sqlite3_stmt *stmt_ptr = leansqlite_get_stmt(stmt_obj);
  const char *name = sqlite3_bind_parameter_name(stmt_ptr, i);
  // sqlite3_bind_parameter_name returns NULL for unnamed parameters (?) or invalid indices
  // We return empty string in both cases, which matches SQLite's behavior
  lean_object *result = lean_mk_string(name != NULL ? name : "");
  return lean_io_result_mk_ok(result);
}

// Uses standard return convention: lean_obj_res
// connection is borrowed: b_lean_obj_arg
// Returns false (0) if in a transaction, true (1) if NOT in a transaction
LEANSQLITE_API
lean_obj_res leansqlite_get_autocommit(b_lean_obj_arg connection) {
  sqlite3 *db = leansqlite_get_connection(connection);
  int autocommit = sqlite3_get_autocommit(db);
  return lean_io_result_mk_ok(lean_box(autocommit != 0 ? 1 : 0));
}

// Uses standard return convention: lean_obj_res
// connection is borrowed: b_lean_obj_arg
// dbName is consumed: lean_obj_arg
// Returns the filename of the specified database ("main", "temp", or attachment name)
LEANSQLITE_API
lean_obj_res leansqlite_db_filename(b_lean_obj_arg connection, lean_obj_arg dbName) {
  sqlite3 *db = leansqlite_get_connection(connection);
  const char *dbName_str = lean_string_cstr(dbName);
  const char *filename = sqlite3_db_filename(db, dbName_str);
  lean_dec(dbName);
  // sqlite3_db_filename returns NULL if the database name is not found
  lean_object *result = lean_mk_string(filename != NULL ? filename : "");
  return lean_io_result_mk_ok(result);
}

LEANSQLITE_API
lean_obj_res leansqlite_total_changes(b_lean_obj_arg connection) {
  sqlite3 *db = leansqlite_get_connection(connection);
  sqlite3_int64 total = sqlite3_total_changes64(db);
  return lean_io_result_mk_ok(lean_box_uint64(total));
}

LEANSQLITE_API
lean_obj_res leansqlite_stmt_readonly(b_lean_obj_arg stmt_obj) {
  sqlite3_stmt *stmt_ptr = leansqlite_get_stmt(stmt_obj);
  int readonly = sqlite3_stmt_readonly(stmt_ptr);
  return lean_io_result_mk_ok(lean_box(readonly != 0 ? 1 : 0));
}

LEANSQLITE_API
lean_obj_res leansqlite_stmt_busy(b_lean_obj_arg stmt_obj) {
  sqlite3_stmt *stmt_ptr = leansqlite_get_stmt(stmt_obj);
  int busy = sqlite3_stmt_busy(stmt_ptr);
  return lean_io_result_mk_ok(lean_box(busy != 0 ? 1 : 0));
}

LEANSQLITE_API
lean_obj_res leansqlite_data_count(b_lean_obj_arg stmt_obj) {
  sqlite3_stmt *stmt_ptr = leansqlite_get_stmt(stmt_obj);
  int count = sqlite3_data_count(stmt_ptr);
  return lean_io_result_mk_ok(lean_box_uint32((uint32_t) count));
}

LEANSQLITE_API
lean_obj_res leansqlite_db_readonly(b_lean_obj_arg connection, lean_obj_arg dbName) {
  sqlite3 *db = leansqlite_get_connection(connection);
  const char *dbName_str = lean_string_cstr(dbName);
  int readonly = sqlite3_db_readonly(db, dbName_str);
  lean_dec(dbName);
  return lean_io_result_mk_ok(lean_box((uint32_t) readonly));
}

LEANSQLITE_API
lean_obj_res leansqlite_column_table_name(b_lean_obj_arg stmt_obj, int32_t column) {
  sqlite3_stmt *stmt_ptr = leansqlite_get_stmt(stmt_obj);
  const char *name = sqlite3_column_table_name(stmt_ptr, column);
  lean_object *result = lean_mk_string(name != NULL ? name : "");
  return lean_io_result_mk_ok(result);
}

LEANSQLITE_API
lean_obj_res leansqlite_column_origin_name(b_lean_obj_arg stmt_obj, int32_t column) {
  sqlite3_stmt *stmt_ptr = leansqlite_get_stmt(stmt_obj);
  const char *name = sqlite3_column_origin_name(stmt_ptr, column);
  lean_object *result = lean_mk_string(name != NULL ? name : "");
  return lean_io_result_mk_ok(result);
}

LEANSQLITE_API
lean_obj_res leansqlite_column_database_name(b_lean_obj_arg stmt_obj, int32_t column) {
  sqlite3_stmt *stmt_ptr = leansqlite_get_stmt(stmt_obj);
  const char *name = sqlite3_column_database_name(stmt_ptr, column);
  lean_object *result = lean_mk_string(name != NULL ? name : "");
  return lean_io_result_mk_ok(result);
}

LEANSQLITE_API
lean_obj_res leansqlite_bind_text(b_lean_obj_arg stmt_obj, int32_t index, lean_obj_arg text) {
  sqlite3_stmt *stmt_ptr = leansqlite_get_stmt(stmt_obj);
  const char *text_str = lean_string_cstr(text);
  // Use SQLITE_TRANSIENT so SQLite makes its own copy of the string
  // NOLINTNEXTLINE(performance-no-int-to-ptr)
  int code = sqlite3_bind_text(stmt_ptr, index, text_str, -1, SQLITE_TRANSIENT);
  lean_dec(text);
  if (code != SQLITE_OK) {
    sqlite3 *db = sqlite3_db_handle(stmt_ptr);
    lean_object *msg = lean_mk_string(sqlite3_errmsg(db));
    return lean_io_result_mk_error(lean_mk_io_error_other_error(code, msg));
  } else {
    return lean_io_result_mk_ok(lean_box(0));
  }
}

LEANSQLITE_API
lean_obj_res leansqlite_bind_double(b_lean_obj_arg stmt_obj, int32_t index, double value) {
  sqlite3_stmt *stmt_ptr = leansqlite_get_stmt(stmt_obj);
  int code = sqlite3_bind_double(stmt_ptr, index, value);
  if (code != SQLITE_OK) {
    sqlite3 *db = sqlite3_db_handle(stmt_ptr);
    lean_object *msg = lean_mk_string(sqlite3_errmsg(db));
    return lean_io_result_mk_error(lean_mk_io_error_other_error(code, msg));
  } else {
    return lean_io_result_mk_ok(lean_box(0));
  }
}

LEANSQLITE_API
lean_obj_res leansqlite_bind_int(b_lean_obj_arg stmt_obj, int32_t index, int32_t value) {
  sqlite3_stmt *stmt_ptr = leansqlite_get_stmt(stmt_obj);
  int code = sqlite3_bind_int(stmt_ptr, index, (int) value);
  if (code != SQLITE_OK) {
    sqlite3 *db = sqlite3_db_handle(stmt_ptr);
    lean_object *msg = lean_mk_string(sqlite3_errmsg(db));
    return lean_io_result_mk_error(lean_mk_io_error_other_error(code, msg));
  } else {
    return lean_io_result_mk_ok(lean_box(0));
  }
}

LEANSQLITE_API
lean_obj_res leansqlite_bind_int64(b_lean_obj_arg stmt_obj, int32_t index, int64_t value) {
  sqlite3_stmt *stmt_ptr = leansqlite_get_stmt(stmt_obj);
  int code = sqlite3_bind_int64(stmt_ptr, index, value);
  if (code != SQLITE_OK) {
    sqlite3 *db = sqlite3_db_handle(stmt_ptr);
    lean_object *msg = lean_mk_string(sqlite3_errmsg(db));
    return lean_io_result_mk_error(lean_mk_io_error_other_error(code, msg));
  } else {
    return lean_io_result_mk_ok(lean_box(0));
  }
}

LEANSQLITE_API
lean_obj_res leansqlite_bind_null(b_lean_obj_arg stmt_obj, int32_t index) {
  sqlite3_stmt *stmt_ptr = leansqlite_get_stmt(stmt_obj);
  int code = sqlite3_bind_null(stmt_ptr, index);
  if (code != SQLITE_OK) {
    sqlite3 *db = sqlite3_db_handle(stmt_ptr);
    lean_object *msg = lean_mk_string(sqlite3_errmsg(db));
    return lean_io_result_mk_error(lean_mk_io_error_other_error(code, msg));
  } else {
    return lean_io_result_mk_ok(lean_box(0));
  }
}

LEANSQLITE_API
lean_obj_res leansqlite_bind_blob(b_lean_obj_arg stmt_obj, int32_t index, lean_obj_arg blob) {
  sqlite3_stmt *stmt_ptr = leansqlite_get_stmt(stmt_obj);
  size_t size = lean_sarray_size(blob);
  const void *data = lean_sarray_cptr(blob);
  // Use SQLITE_TRANSIENT so SQLite makes its own copy
  // NOLINTNEXTLINE(bugprone-narrowing-conversions,performance-no-int-to-ptr)
  int code = sqlite3_bind_blob(stmt_ptr, index, data, size, SQLITE_TRANSIENT);
  lean_dec(blob);

  if (code != SQLITE_OK) {
    sqlite3 *db = sqlite3_db_handle(stmt_ptr);
    lean_object *msg = lean_mk_string(sqlite3_errmsg(db));
    return lean_io_result_mk_error(lean_mk_io_error_other_error(code, msg));
  } else {
    return lean_io_result_mk_ok(lean_box(0));
  }
}

LEANSQLITE_API
lean_obj_res leansqlite_step(b_lean_obj_arg stmt_obj) {
  sqlite3_stmt *stmt_ptr = leansqlite_get_stmt(stmt_obj);
  int code = sqlite3_step(stmt_ptr);
  if (code == SQLITE_ROW) {
    // Return 1 to indicate a row is available
    return lean_io_result_mk_ok(lean_box(1));
  } else if (code == SQLITE_DONE) {
    // Return 0 to indicate done
    return lean_io_result_mk_ok(lean_box(0));
  } else {
    sqlite3 *db = sqlite3_db_handle(stmt_ptr);
    lean_object *msg = lean_mk_string(sqlite3_errmsg(db));
    return lean_io_result_mk_error(lean_mk_io_error_other_error(code, msg));
  }
}

LEANSQLITE_API
lean_obj_res leansqlite_reset(b_lean_obj_arg stmt_obj) {
  sqlite3_stmt *stmt_ptr = leansqlite_get_stmt(stmt_obj);
  int code = sqlite3_reset(stmt_ptr);
  if (code != SQLITE_OK) {
    sqlite3 *db = sqlite3_db_handle(stmt_ptr);
    lean_object *msg = lean_mk_string(sqlite3_errmsg(db));
    return lean_io_result_mk_error(lean_mk_io_error_other_error(code, msg));
  } else {
    return lean_io_result_mk_ok(lean_box(0));
  }
}

LEANSQLITE_API
lean_obj_res leansqlite_exec(b_lean_obj_arg connection, lean_obj_arg sql) {
  sqlite3 *db = leansqlite_get_connection(connection);
  const char *sql_str = lean_string_cstr(sql);

  char *err_msg = NULL;
  int code = sqlite3_exec(db, sql_str, NULL, NULL, &err_msg);
  lean_dec(sql);

  if (code != SQLITE_OK) {
    lean_object *msg;
    if (err_msg == NULL) {
      msg = lean_mk_string("Unknown error");
    } else {
      msg = lean_mk_string(err_msg);
      sqlite3_free(err_msg);
    }
    assert(msg != NULL);
    return lean_io_result_mk_error(lean_mk_io_error_other_error(code, msg));
  } else {
    return lean_io_result_mk_ok(lean_box(0));
  }
}

LEANSQLITE_API
lean_obj_res leansqlite_last_insert_rowid(b_lean_obj_arg connection) {
  sqlite3 *db = leansqlite_get_connection(connection);
  sqlite3_int64 rowid = sqlite3_last_insert_rowid(db);
  return lean_io_result_mk_ok(lean_box_uint64(rowid));
}

LEANSQLITE_API
lean_obj_res leansqlite_changes(b_lean_obj_arg connection) {
  sqlite3 *db = leansqlite_get_connection(connection);
  sqlite3_int64 changes = sqlite3_changes64(db);
  return lean_io_result_mk_ok(lean_box_uint64(changes));
}

LEANSQLITE_API
lean_obj_res leansqlite_busy_timeout(b_lean_obj_arg connection, int32_t ms) {
  sqlite3 *db = leansqlite_get_connection(connection);
  int code = sqlite3_busy_timeout(db, ms);
  if (code != SQLITE_OK) {
    lean_object *msg = lean_mk_string(sqlite3_errmsg(db));
    return lean_io_result_mk_error(lean_mk_io_error_other_error(code, msg));
  } else {
    return lean_io_result_mk_ok(lean_box(0));
  }
}

LEANSQLITE_API
lean_obj_res leansqlite_clear_bindings(b_lean_obj_arg stmt_obj) {
  sqlite3_stmt *stmt_ptr = leansqlite_get_stmt(stmt_obj);
  int code = sqlite3_clear_bindings(stmt_ptr);
  if (code != SQLITE_OK) {
    sqlite3 *db = sqlite3_db_handle(stmt_ptr);
    lean_object *msg = lean_mk_string(sqlite3_errmsg(db));
    return lean_io_result_mk_error(lean_mk_io_error_other_error(code, msg));
  } else {
    return lean_io_result_mk_ok(lean_box(0));
  }
}

LEANSQLITE_API
lean_obj_res leansqlite_column_name(b_lean_obj_arg stmt_obj, int32_t column) {
  sqlite3_stmt *stmt_ptr = leansqlite_get_stmt(stmt_obj);
  const char *name = sqlite3_column_name(stmt_ptr, column);
  if (name == NULL) {
    lean_object *msg = lean_mk_string(
        "Failed to get column name (typically due to an allocation failure in SQLite)");
    return lean_io_result_mk_error(lean_mk_io_error_other_error(1, msg));
  } else {
    lean_object *result = lean_mk_string(name);
    return lean_io_result_mk_ok(result);
  }
}

LEANSQLITE_API
lean_obj_res leansqlite_shathree_init(b_lean_obj_arg connection) {
  sqlite3 *db = leansqlite_get_connection(connection);
  int code = sqlite3_shathree_init(db, NULL, NULL);
  if (code != SQLITE_OK) {
    lean_object *msg = lean_mk_string("Failed to initialize SHA3 extension");
    return lean_io_result_mk_error(lean_mk_io_error_other_error(code, msg));
  }
  return lean_io_result_mk_ok(lean_box(0));
}
