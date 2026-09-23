/-
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
Author: David Thrane Christiansen
-/
import Lake

open System Lake DSL

package leansqlite where
  version := v!"0.1.0"
  keywords := #["sqlite", "database", "ffi"]
  license := "Apache-2.0"
  leanOptions := #[⟨`experimental.module, true⟩]

/--
Adds headers to a source job, so that editing a header rebuilds the object compiled from a source
that includes it.
-/
private def withHeaders (headers : Array FilePath) (srcJob : Job FilePath) : FetchM (Job FilePath) := do
  let mut job := srcJob
  for header in headers do
    let headerJob ← inputTextFile header
    job := job.zipWith (fun (src : FilePath) (_ : FilePath) => src) headerJob
  return job

target sqlite.o pkg : FilePath := do
  let oFile := pkg.buildDir / "sqlite3.o"
  -- The bundled SQLite contains its own headers
  let srcJob ← inputTextFile <| pkg.dir / "bindings" / "sqlite3.c"
  let weakArgs := #["-I", (pkg.dir / "bindings").toString]
  -- LFS support is disabled. This is because it causes linker errors on Linux, where newer glibc
  -- headers use macros to redirect standard functions (fcntl, fopen, etc.) to their 64-bit variants
  -- (fcntl64, fopen64, etc.). SQLite detects and uses these, but Lean's bundled libc only provides
  -- fcntl and not fcntl64. On 32-bit systems this would limit database files to ~2GB, so we
  -- explicitly fail compilation on 32-bit platforms in the FFI wrappers. On 64-bit systems, there
  -- is no limitation.
  buildO oFile srcJob weakArgs (traceArgs := #["-fPIC", "-DSQLITE_DISABLE_LFS", "-DSQLITE_ENABLE_COLUMN_METADATA", "-Wno-discarded-qualifiers"]) (extraDepTrace := getLeanTrace)

target leansqlite.o pkg : FilePath := do
  let sqliteHeaders := pkg.dir / "bindings"
  let oFile := pkg.buildDir / "leansqlite.o"
  let srcJob ← inputTextFile <| pkg.dir / "bindings" / "leansqlite.c"
  let srcJob ← withHeaders #[sqliteHeaders / "sqlite3.h"] srcJob
  let weakArgs := #["-I", (← getLeanIncludeDir).toString, "-I", sqliteHeaders.toString]
  buildO oFile srcJob weakArgs (traceArgs := #["-fPIC"]) (extraDepTrace := getLeanTrace)

target shathree.o pkg : FilePath := do
  let sqliteHeaders := pkg.dir / "bindings"
  let oFile := pkg.buildDir / "shathree.o"
  let srcJob ← inputTextFile <| pkg.dir / "bindings" / "shathree.c"
  -- `sqlite3ext.h` includes `sqlite3.h`.
  let srcJob ← withHeaders #[sqliteHeaders / "sqlite3ext.h", sqliteHeaders / "sqlite3.h"] srcJob
  let weakArgs := #["-I", sqliteHeaders.toString]
  buildO oFile srcJob weakArgs (traceArgs := #["-fPIC", "-DSQLITE_CORE"]) (extraDepTrace := getLeanTrace)

extern_lib leansqlite pkg := do
  let sqliteObj ← sqlite.o.fetch
  let leansqliteObj ← leansqlite.o.fetch
  let shathreeObj ← shathree.o.fetch
  let libFile := "leansqlite"
  buildStaticLib (pkg.staticLibDir / nameToStaticLib libFile) #[sqliteObj, leansqliteObj, shathreeObj]

@[default_target]
lean_lib SQLite where
  needs := #[leansqlite]
  -- Needed to interpret `@[extern]` symbols defined in `SQLite.FFI`.
  precompileModules := true

-- Tests live in the `tests/` subproject rather than here, so that downstream projects depending on
-- `leansqlite` don't acquire a transitive dependency on test-only tools (e.g. `plausible`).
@[test_driver]
script tests (args) do
  let pkg ← getRootPackage
  let child ← IO.Process.spawn {
    cmd := "lake"
    args := #["test", "--"] ++ args.toArray
    cwd := pkg.dir / "tests"
  }
  child.wait
