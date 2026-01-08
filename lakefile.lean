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
  buildO oFile srcJob weakArgs (traceArgs := #["-fPIC"]) (extraDepTrace := getLeanTrace)

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
  buildStaticLib (pkg.staticLibDir / libFile) #[sqliteObj, leansqliteObj]

@[default_target]
lean_lib SQLite where
  needs := #[leansqlite]

@[test_driver]
lean_exe Test
