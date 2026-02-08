/-
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
Author: David Thrane Christiansen
-/
module
public import Lean
public import SQLite.Blob.Classes
import SQLite.Blob.Deriving

open Lean
open SQLite Blob

deriving instance ToBinary, FromBinary for Sum
deriving instance ToBinary for Empty
deriving instance ToBinary for PUnit

deriving instance ToBinary, FromBinary for Std.Format.FlattenBehavior
deriving instance ToBinary, FromBinary for Format
deriving instance ToBinary, FromBinary for Name
deriving instance ToBinary, FromBinary for FVarId
deriving instance ToBinary, FromBinary for MVarId
deriving instance ToBinary, FromBinary for LevelMVarId
deriving instance ToBinary, FromBinary for LMVarId
deriving instance ToBinary, FromBinary for Level
deriving instance ToBinary, FromBinary for BinderInfo
deriving instance ToBinary, FromBinary for Literal


/-
No instances are provided for Expr because it relies via MData on Syntax, and clients may or may not
want to serialize the underlying strings of the substrings contained in the sourceinfos.
-/
