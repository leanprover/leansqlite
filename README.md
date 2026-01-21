# leansqlite

## Overview

This library provides Lean bindings for SQLite. The implementation is
mostly a thin layer around the SQLite C API. The library includes a
few conveniences on top of the raw C bindings to make working with
SQLite more straightforward:

 * Interpolated strings that expand to prepared statements
 * Type classes for serializing binary blobs, converting data to query
   parameters, and reading columns from results
 * Iterators over query result rows

## Key Modules

The library is organized into three layers:

The `SQLite.FFI` module contains the raw foreign function interface
bindings to the SQLite C API, at a very low level of abstraction.

The `SQLite.LowLevel` module wraps the FFI layer with Lean structures
and types that make the API more ergonomic while still staying close
to the underlying C interface. It provides structures for database
connections and statements, enumerations for modes and options, and
helper functionss. This module is suitable for users who want a
straightforward mapping to SQLite concepts without additional
abstractions.

The main `SQLite` module builds on the low-level API to provide
higher-level conveniences. It includes type classes for binding and
reading values, a row reader monad for extracting query results,
iterator support for working with result sets, and SQL interpolation
syntax for embedding values in queries. These higher-level features
are more experimental, and the specifics of the API are likely to
change.

## SQLite Integration

The library bundles the SQLite amalgamation and compiles it directly
alongside the Lean FFI bindings. This means users do not need to have
SQLite installed on their system, and the library is self-contained.

SQLite is compiled with the following flags:
 * `SQLITE_ENABLE_COLUMN_METADATA` enables the functions that provide
   metadata about result columns, such as the original table and
   column names.
 * `SQLITE_DISABLE_LFS` flag disables large file support, due to an
   incompatibility between C library versions on Linux. On 64-bit
   systems this has no practical effect, as file sizes are not
   limited. However, on 32-bit systems it would restrict database
   files to approximately 2GB. Because of this limitation, the library
   currently does not support 32-bit platforms. If there is a
   compelling use case for 32-bit support, the build configuration
   could be adjusted to work around the large file support issue.

## Development

To build the library, use the standard Lake build command from the
repository root. This will compile both the SQLite amalgamation and
the Lean bindings, producing the library and any default targets.

```bash
lake build
```

The library includes a test suite that exercises the FFI bindings and
higher-level API. To run the tests, use the `lake test` command. By
default, tests run against a temporary database file that is
automatically created and cleaned up.

```bash
lake test
```

For verbose output that shows all passing tests in addition to
failures, pass the verbose flag:

```bash
lake test -- --verbose
```

If you need to run tests against a specific database file instead of a
temporary one, provide the path as an argument. This can be useful for
debugging or inspecting the database state after tests complete.

```bash
lake test -- path/to/database.db
```

You can combine the verbose flag with a custom database path as well:

```bash
lake test -- --verbose path/to/database.db
```

## License

This library is released under the Apache 2.0 license. See the LICENSE
file for the complete license text.

The library bundles SQLite, which is in the public domain. SQLite has
been dedicated to the public domain by its authors and is free for use
for any purpose, commercial or private. For more information about
SQLite's public domain dedication, see the official SQLite copyright
page at https://www.sqlite.org/copyright.html.
