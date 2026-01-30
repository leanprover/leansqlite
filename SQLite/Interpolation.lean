/-
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
Author: David Thrane Christiansen
-/
module

public import SQLite.LowLevel
public import SQLite.QueryParam
public import SQLite.QueryResult

set_option doc.verso true
set_option linter.missingDocs true

namespace SQLite
namespace Interpolation

open Lean

/--
An interpolated query that creates a prepared statement.

Accepts only a single SQL statement. Interpolated values are converted to SQLite
parameter bindings. For multiple statements, use {name}`SQLite.exec`.
-/
syntax term "sql!" interpolatedStr(term) : term

/--
An interpolated query that executes a single statement, discarding the results.

For multiple statements without parameters, use {name}`SQLite.exec` directly.
-/
syntax term "exec!" interpolatedStr(term) : term

/--
An interpolated query that creates an iterator into the results.

Accepts only a single SQL statement that returns data.
-/
syntax term "query!" interpolatedStr(term) (&" as " term)? : term

macro_rules
  | `($db sql!%$tk $s) => do
    let chunks := s.raw.getArgs
    let mut sqlStx ← `("")
    let mut args := #[]
    let stmtName := mkIdentFrom tk `stmt
    for c in chunks do
      if let some lit := c.isInterpolatedStrLit? then
        sqlStx ← `($sqlStx ++ $(quote lit))
      else
        let index := args.size + 1
        args := args.push (← ``(NullableQueryParam.bind $stmtName (Nat.toInt32 $(quote index)) $(⟨c⟩)))
        sqlStx ← `($sqlStx ++ $(quote s!"?{index}"))

    let stx ← `(((let db := $db; SQLite.prepare db $sqlStx >>= fun $stmtName =>
        (([$(args),*] : List (IO Unit)).forM id) *>
        return $stmtName) : IO Stmt))

    return stx
  | `($db exec!%$tk $s) => do
    `(($db sql!%$tk $s >>= Stmt.exec : IO Unit))
  | `($db query!%$tk $s) => do
    `($db sql!%$tk $s <&> Stmt.results)
  | `($db query!%$tk $s as $ty) => do
    `($db sql!%$tk $s <&> Stmt.resultsAs $ty)

end Interpolation
end SQLite
