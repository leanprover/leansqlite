/-
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
Author: David Thrane Christiansen
-/
module
public import Lean.Expr
public import Lean.DocString.Types
public import Lean.Data.Position
public import Lean.Data.DeclarationRange
public import Lean.Declaration
public import Lean.Message
public import SQLite.Blob.Classes
public import Std.Data.HashMap
public import Std.Data.TreeMap
import SQLite.Blob.Deriving

open Lean
open SQLite Blob

deriving instance ToBinary, FromBinary for Sum
deriving instance ToBinary, FromBinary for Empty
deriving instance ToBinary, FromBinary for Ordering
deriving instance ToBinary, FromBinary for Except
deriving instance ToBinary, FromBinary for Int8
deriving instance ToBinary, FromBinary for Int16
deriving instance ToBinary, FromBinary for Int32
deriving instance ToBinary, FromBinary for Int64

section
open Std
instance [BEq α] [Hashable α] [ToBinary α] [ToBinary β] : ToBinary (HashMap α β) := .via HashMap.toArray
instance [BEq α] [Hashable α] [FromBinary α] [FromBinary β] : FromBinary (HashMap α β) := .via HashMap.ofArray

instance [BEq α] [Hashable α] [ToBinary α] : ToBinary (HashSet α) := .via HashSet.toArray
instance [BEq α] [Hashable α] [FromBinary α] : FromBinary (HashSet α) := .via HashSet.ofArray

instance [Ord α] [ToBinary α] [ToBinary β] : ToBinary (TreeMap α β) := .via TreeMap.toArray
instance [Ord α] [FromBinary α] [FromBinary β] : FromBinary (TreeMap α β) :=
  .via fun (xs : Array (α × β)) => TreeMap.ofArray xs

instance [Ord α] [ToBinary α] : ToBinary (TreeSet α) := .via TreeSet.toArray
instance [Ord α] [FromBinary α] : FromBinary (TreeSet α) :=
  .via fun (xs : Array α) => TreeSet.ofArray xs
end

deriving instance ToBinary, FromBinary for Std.Format.FlattenBehavior
deriving instance ToBinary, FromBinary for Format
deriving instance ToBinary, FromBinary for Position
deriving instance ToBinary, FromBinary for DeclarationRange
deriving instance ToBinary, FromBinary for MessageSeverity
deriving instance ToBinary, FromBinary for Name

instance [ToBinary α] : ToBinary (NameMap α) := .via Std.TreeMap.toArray
instance [FromBinary α] : FromBinary (NameMap α) :=
  .via (Std.TreeMap.ofArray · Name.quickCmp)

deriving instance ToBinary, FromBinary for DefinitionSafety
deriving instance ToBinary, FromBinary for ReducibilityHints
deriving instance ToBinary, FromBinary for QuotKind
deriving instance ToBinary, FromBinary for FVarId
deriving instance ToBinary, FromBinary for MVarId
deriving instance ToBinary, FromBinary for LevelMVarId
deriving instance ToBinary, FromBinary for LMVarId
deriving instance ToBinary, FromBinary for Level
deriving instance ToBinary, FromBinary for BinderInfo
deriving instance ToBinary, FromBinary for Literal
deriving instance ToBinary, FromBinary for Doc.MathMode
deriving instance ToBinary, FromBinary for Doc.Inline
deriving instance ToBinary, FromBinary for Doc.ListItem
deriving instance ToBinary, FromBinary for Doc.DescItem
deriving instance ToBinary, FromBinary for Doc.Block
deriving instance ToBinary, FromBinary for Doc.Part

/-
No instances are provided for Expr because it relies via MData on Syntax, and clients may or may not
want to serialize the underlying strings of the substrings contained in the SourceInfos.
-/
