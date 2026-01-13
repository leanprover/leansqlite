/*
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
Author: David Thrane Christiansen
*/

// This library disables large file support to avoid linking issues with Lean's bundled libc.
// On 32-bit systems, this would limit database files to ~2GB. Since we don't have CI to test
// error handling in this situation, compilation fails.
// If emscripten becomes a target of interest, then this conditional can be made more specific.
#if defined(__i386__) || defined(_M_IX86) || defined(__arm__) || (defined(__SIZEOF_POINTER__) && __SIZEOF_POINTER__ == 4)
#error "32-bit platforms are not supported by the FFI bindings."
#endif

#include <lean/lean.h>
#include <sqlite3.h>
#include <string.h>

void leansqlite_connection_finalize(void *connection) {
  // Uses sqlite3_close instead of sqlite3_close_v2 because uses of the connection will have
  // references to it, so it won't be freed/finalized until they are.
  int code = sqlite3_close((sqlite3 *) connection);
  assert(code == SQLITE_OK);
}

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

void leansqlite_stmt_foreach(void *connection, b_lean_obj_arg arg) {
  return;
}

lean_external_class *leansqlite_stmt_class = NULL;

lean_object *leansqlite_stmt(sqlite3_stmt *stmt) {
  return lean_alloc_external(leansqlite_stmt_class, stmt);
}

sqlite3_stmt *stmt(lean_object *stmt) {
  return (sqlite3_stmt *) lean_get_external_data(stmt);
}

void leansqlite_value_finalize(void *value) {
  // sqlite3_value objects are owned by SQLite and should not be freed
  // They are only valid during certain callback contexts
  return;
}

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

lean_obj_res leansqlite_initialize() {
  leansqlite_connection_class = lean_register_external_class(
    leansqlite_connection_finalize,
    leansqlite_connection_foreach
  );
  leansqlite_stmt_class = lean_register_external_class(
    leansqlite_stmt_finalize,
    leansqlite_stmt_foreach
  );
  leansqlite_value_class = lean_register_external_class(
    leansqlite_value_finalize,
    leansqlite_value_foreach
  );
  return lean_io_result_mk_ok(lean_box(0));
}

lean_obj_res leansqlite_open(lean_obj_arg filename) {
  const char *filename_str = lean_string_cstr(filename);
  lean_dec(filename);
  sqlite3 *db;
  int code = sqlite3_open(filename_str, &db);
  if (code != SQLITE_OK) {
    lean_object *msg = lean_mk_string(sqlite3_errmsg(db));
    sqlite3_close(db);
    return lean_io_result_mk_error(lean_mk_io_error_other_error(code, msg));
  } else {
    return lean_io_result_mk_ok(leansqlite_connection(db));
  }
}

lean_obj_res leansqlite_prepare(b_lean_obj_arg connection, lean_obj_arg sql) {
  sqlite3 *db = leansqlite_get_connection(connection);
  const char *sql_str = lean_string_cstr(sql);
  lean_dec(sql);
  const char *tail = sql_str;
  // -1 length means to read to the null terminator
  sqlite3_stmt *stmt;
  int code = sqlite3_prepare_v2(db, sql_str, -1, &stmt, &tail);
  if (code != SQLITE_OK) {
    lean_object *msg = lean_mk_string(sqlite3_errmsg(db));
    // stmt is NULL on error, no need to finalize
    return lean_io_result_mk_error(lean_mk_io_error_other_error(code, msg));
  } else if (tail[0] != '\0') {
    lean_object *msg1 = lean_mk_string("Unprocessed SQL:");
    lean_object *msg2 = lean_mk_string(tail);
    lean_object *msg = lean_string_append(msg1, msg2);
    // lean_string_append borrows only its second argument, and consumes its first, so msg1 doesn't
    // get decremented here.
    lean_dec(msg2);
    sqlite3_finalize(stmt);
    return lean_io_result_mk_error(lean_mk_io_error_other_error(code, msg));
  } else {
    return lean_io_result_mk_ok(leansqlite_stmt(stmt));
  }
}

lean_obj_res leansqlite_column_text(b_lean_obj_arg stmt_obj, int32_t column) {
  sqlite3_stmt *stmt_ptr = stmt(stmt_obj);
  const unsigned char *text = sqlite3_column_text(stmt_ptr, column);
  lean_object *result = lean_mk_string(text != NULL ? (const char *)text : "");
  return lean_io_result_mk_ok(result);
}

lean_obj_res leansqlite_column_blob(b_lean_obj_arg stmt_obj, int32_t column) {
  sqlite3_stmt *stmt_ptr = stmt(stmt_obj);
  int bytes = sqlite3_column_bytes(stmt_ptr, column);
  const void *blob = sqlite3_column_blob(stmt_ptr, column);

  // Allocate ByteArray
  lean_object *byte_array = lean_alloc_sarray(1, bytes, bytes);
  if (bytes > 0 && blob != NULL) {
    memcpy(lean_sarray_cptr(byte_array), blob, bytes);
  }

  return lean_io_result_mk_ok(byte_array);
}

lean_obj_res leansqlite_column_double(b_lean_obj_arg stmt_obj, int32_t column) {
  sqlite3_stmt *stmt_ptr = stmt(stmt_obj);
  double value = sqlite3_column_double(stmt_ptr, column);
  return lean_io_result_mk_ok(lean_box_float(value));
}

lean_obj_res leansqlite_column_int(b_lean_obj_arg stmt_obj, int32_t column) {
  sqlite3_stmt *stmt_ptr = stmt(stmt_obj);
  int32_t value = (int32_t)sqlite3_column_int(stmt_ptr, column);
  return lean_io_result_mk_ok(lean_box(value));
}

lean_obj_res leansqlite_column_int64(b_lean_obj_arg stmt_obj, int32_t column) {
  sqlite3_stmt *stmt_ptr = stmt(stmt_obj);
  int64_t value = sqlite3_column_int64(stmt_ptr, column);
  return lean_io_result_mk_ok(lean_box_uint64(value));
}

int32_t leansqlite_value_type(b_lean_obj_arg value_obj) {
  sqlite3_value *value = leansqlite_get_value(value_obj);
  return sqlite3_value_type(value);
}

lean_obj_res leansqlite_value_text(lean_obj_arg value_obj) {
  sqlite3_value *value = leansqlite_get_value(value_obj);
  lean_dec(value_obj);
  const unsigned char *text = sqlite3_value_text(value);
  lean_object *result = lean_mk_string(text != NULL ? (const char *)text : "");
  return lean_io_result_mk_ok(result);
}

lean_obj_res leansqlite_value_blob(lean_obj_arg value_obj) {
  sqlite3_value *value = leansqlite_get_value(value_obj);
  lean_dec(value_obj);
  int bytes = sqlite3_value_bytes(value);
  const void *blob = sqlite3_value_blob(value);

  // Allocate ByteArray
  lean_object *byte_array = lean_alloc_sarray(1, bytes, bytes);
  if (bytes > 0 && blob != NULL) {
    memcpy(lean_sarray_cptr(byte_array), blob, bytes);
  }

  return lean_io_result_mk_ok(byte_array);
}

lean_obj_res leansqlite_value_double(lean_obj_arg value_obj) {
  sqlite3_value *value = leansqlite_get_value(value_obj);
  lean_dec(value_obj);
  double result = sqlite3_value_double(value);
  return lean_io_result_mk_ok(lean_box_float(result));
}

lean_obj_res leansqlite_value_int64(lean_obj_arg value_obj) {
  sqlite3_value *value = leansqlite_get_value(value_obj);
  lean_dec(value_obj);
  int64_t result = sqlite3_value_int64(value);
  return lean_io_result_mk_ok(lean_box_uint64(result));
}

uint32_t leansqlite_column_count(b_lean_obj_arg stmt_obj) {
  sqlite3_stmt *stmt_ptr = stmt(stmt_obj);
  int count = sqlite3_column_count(stmt_ptr);
  return (uint32_t)count;
}

lean_obj_res leansqlite_column_type(b_lean_obj_arg stmt_obj, int32_t column) {
  sqlite3_stmt *stmt_ptr = stmt(stmt_obj);
  int32_t type_code = sqlite3_column_type(stmt_ptr, column);
  return lean_io_result_mk_ok(lean_box(type_code));
}

lean_obj_res leansqlite_bind_text(b_lean_obj_arg stmt_obj, int32_t index, lean_obj_arg text) {
  sqlite3_stmt *stmt_ptr = stmt(stmt_obj);
  const char *text_str = lean_string_cstr(text);
  lean_dec(text);
  // Use SQLITE_TRANSIENT so SQLite makes its own copy of the string
  int code = sqlite3_bind_text(stmt_ptr, index, text_str, -1, SQLITE_TRANSIENT);
  if (code != SQLITE_OK) {
    sqlite3 *db = sqlite3_db_handle(stmt_ptr);
    lean_object *msg = lean_mk_string(sqlite3_errmsg(db));
    return lean_io_result_mk_error(lean_mk_io_error_other_error(code, msg));
  } else {
    return lean_io_result_mk_ok(lean_box(0));
  }
}

lean_obj_res leansqlite_bind_double(b_lean_obj_arg stmt_obj, int32_t index, double value) {
  sqlite3_stmt *stmt_ptr = stmt(stmt_obj);
  int code = sqlite3_bind_double(stmt_ptr, index, value);
  if (code != SQLITE_OK) {
    sqlite3 *db = sqlite3_db_handle(stmt_ptr);
    lean_object *msg = lean_mk_string(sqlite3_errmsg(db));
    return lean_io_result_mk_error(lean_mk_io_error_other_error(code, msg));
  } else {
    return lean_io_result_mk_ok(lean_box(0));
  }
}

lean_obj_res leansqlite_bind_int(b_lean_obj_arg stmt_obj, int32_t index, uint32_t value) {
  sqlite3_stmt *stmt_ptr = stmt(stmt_obj);
  int code = sqlite3_bind_int(stmt_ptr, index, (int)value);
  if (code != SQLITE_OK) {
    sqlite3 *db = sqlite3_db_handle(stmt_ptr);
    lean_object *msg = lean_mk_string(sqlite3_errmsg(db));
    return lean_io_result_mk_error(lean_mk_io_error_other_error(code, msg));
  } else {
    return lean_io_result_mk_ok(lean_box(0));
  }
}

lean_obj_res leansqlite_bind_int64(b_lean_obj_arg stmt_obj, int32_t index, int64_t value) {
  sqlite3_stmt *stmt_ptr = stmt(stmt_obj);
  int code = sqlite3_bind_int64(stmt_ptr, index, value);
  if (code != SQLITE_OK) {
    sqlite3 *db = sqlite3_db_handle(stmt_ptr);
    lean_object *msg = lean_mk_string(sqlite3_errmsg(db));
    return lean_io_result_mk_error(lean_mk_io_error_other_error(code, msg));
  } else {
    return lean_io_result_mk_ok(lean_box(0));
  }
}

lean_obj_res leansqlite_bind_null(b_lean_obj_arg stmt_obj, int32_t index) {
  sqlite3_stmt *stmt_ptr = stmt(stmt_obj);
  int code = sqlite3_bind_null(stmt_ptr, index);
  if (code != SQLITE_OK) {
    sqlite3 *db = sqlite3_db_handle(stmt_ptr);
    lean_object *msg = lean_mk_string(sqlite3_errmsg(db));
    return lean_io_result_mk_error(lean_mk_io_error_other_error(code, msg));
  } else {
    return lean_io_result_mk_ok(lean_box(0));
  }
}

lean_obj_res leansqlite_bind_blob(b_lean_obj_arg stmt_obj, int32_t index, lean_obj_arg blob) {
  sqlite3_stmt *stmt_ptr = stmt(stmt_obj);
  size_t size = lean_sarray_size(blob);
  const void *data = lean_sarray_cptr(blob);
  // Use SQLITE_TRANSIENT so SQLite makes its own copy
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

lean_obj_res leansqlite_step(b_lean_obj_arg stmt_obj) {
  sqlite3_stmt *stmt_ptr = stmt(stmt_obj);
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

lean_obj_res leansqlite_reset(b_lean_obj_arg stmt_obj) {
  sqlite3_stmt *stmt_ptr = stmt(stmt_obj);
  int code = sqlite3_reset(stmt_ptr);
  if (code != SQLITE_OK) {
    sqlite3 *db = sqlite3_db_handle(stmt_ptr);
    lean_object *msg = lean_mk_string(sqlite3_errmsg(db));
    return lean_io_result_mk_error(lean_mk_io_error_other_error(code, msg));
  } else {
    return lean_io_result_mk_ok(lean_box(0));
  }
}

lean_obj_res leansqlite_exec(b_lean_obj_arg connection, lean_obj_arg sql) {
  sqlite3 *db = leansqlite_get_connection(connection);
  const char *sql_str = lean_string_cstr(sql);
  lean_dec(sql);

  char *err_msg = NULL;
  int code = sqlite3_exec(db, sql_str, NULL, NULL, &err_msg);

  if (code != SQLITE_OK) {
    lean_object *msg = NULL;
    if (err_msg == NULL) {
      lean_object *msg = lean_mk_string("Unknown error");
    } else {
      lean_object *msg = lean_mk_string(err_msg);
      sqlite3_free(err_msg);
    }
    assert(msg != NULL);
    return lean_io_result_mk_error(lean_mk_io_error_other_error(code, msg));
  } else {
    return lean_io_result_mk_ok(lean_box(0));
  }
}
