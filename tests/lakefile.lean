/-
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
Author: David Thrane Christiansen
-/
import Lake
open Lake DSL

require leansqlite from ".."
require plausible from git "https://github.com/leanprover-community/plausible"@"main"

package «leansqlite-tests» where
  leanOptions := #[⟨`experimental.module, true⟩]

@[default_target]
lean_lib SQLiteTest where
  precompileModules := true

@[default_target, test_driver]
lean_exe TestMain
