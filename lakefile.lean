import Lake

open System Lake DSL

package leansqlite where
  version := v!"0.1.0"
  keywords := #["sqlite", "database", "ffi"]
  license := "Apache-2.0"

target sqlite.o pkg : FilePath := do
  let oFile := pkg.buildDir / "sqlite3.o"
  let srcJob ← inputTextFile <| pkg.dir / "bindings" / "sqlite3.c"
  let hJob ← inputTextFile <| pkg.dir / "bindings" / "sqlite3.h"
  let extJob ← inputTextFile <| pkg.dir / "bindings" / "sqlite3ext.h"
  let srcJob := srcJob |>.add hJob |>.add extJob
  let weakArgs := #["-I", (pkg.dir / "bindings").toString]
  -- LFS support is disabled. This is because it causes linker errors on Linux, where newer glibc
  -- headers use macros to redirect standard functions (fcntl, fopen, etc.) to their 64-bit variants
  -- (fcntl64, fopen64, etc.). SQLite detects and uses these, but Lean's bundled libc only provides
  -- fcntl and not fcntl64. On 32-bit systems this would limit database files to ~2GB, so we
  -- explicitly fail compilation on 32-bit platforms in the FFI wrappers. On 64-bit systems, there
  -- is no limitation.
  buildO oFile srcJob weakArgs (traceArgs := #["-fPIC", "-DSQLITE_DISABLE_LFS"]) (extraDepTrace := getLeanTrace)

target leansqlite.o pkg : FilePath := do
  let sqliteHeaders := pkg.dir / "bindings"
  let oFile := pkg.buildDir / "leansqlite.o"
  let srcJob ← inputTextFile <| pkg.dir / "bindings" / "leansqlite.c"
  let weakArgs := #["-I", (← getLeanIncludeDir).toString, "-I", sqliteHeaders.toString]
  buildO oFile srcJob weakArgs (traceArgs := #["-fPIC"]) (extraDepTrace := getLeanTrace)

extern_lib leansqlite pkg := do
  let sqliteObj ← sqlite.o.fetch
  let leansqliteObj ← leansqlite.o.fetch
  let libFile := "leansqlite"
  buildStaticLib (pkg.staticLibDir / nameToStaticLib libFile) #[sqliteObj, leansqliteObj]

@[default_target]
lean_lib SQLite where
  needs := #[leansqlite]
  precompileModules := true

@[default_target, test_driver]
lean_exe Test
