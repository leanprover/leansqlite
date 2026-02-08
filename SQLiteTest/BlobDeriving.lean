/-
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
Author: David Thrane Christiansen
-/

import Plausible
import Plausible.ArbitraryFueled
import Plausible.Gen

import SQLite.Blob
import SQLite.Blob.Deriving

import SQLiteTest.Blob

namespace SQLite.Test.BlobDeriving
open SQLite.Blob
open SQLite.Test.Blob
open Plausible Gen Arbitrary

/-! ## Positive Compile-Time Tests -/

/-! ### Zero-field structure (ToBinary only) -/
structure ZeroField where
deriving ToBinary

/-! ### Single-ctor, multiple fields -/
structure Pair where
  x : Nat
  y : String
deriving BEq, Repr, ToBinary, FromBinary

/-! ### Multi-ctor enum (no data fields) -/
inductive Color where
  | r | g | b
deriving BEq, Repr, Inhabited, ToBinary, FromBinary

/-! ### Multi-ctor with data fields -/
inductive Shape where
  | circle (radius : Nat)
  | rect (w : Nat) (h : Nat)
deriving BEq, Repr, ToBinary, FromBinary

/-! ### Multi-ctor with Bool field -/
inductive Msg where
  | flagged (b : Bool) (s : String)
  | plain (s : String)
deriving BEq, Repr, ToBinary, FromBinary

/-! ### Multi-ctor with Option field -/
inductive Cmd where
  | exec (retries : Option Nat) (cmd : String)
  | noop
deriving BEq, Repr, ToBinary, FromBinary

/-! ### Parametric type -/
structure Box (α : Type) where
  val : α
deriving BEq, Repr, ToBinary, FromBinary

/-! ### Recursive type -/
inductive Tree where
  | leaf (val : Nat)
  | node (left : Tree) (right : Tree)
deriving BEq, Repr, ToBinary, FromBinary

/-! ## Negative Compile-Time Tests -/

/-! ### FromBinary on zero-ctor type -/
inductive Void
deriving ToBinary

/-- error: None of the deriving handlers for class `FromBinary` applied to `Void` -/
#guard_msgs in
deriving instance FromBinary for Void

/-! ### ToBinary on type with proof fields -/
/-- error: None of the deriving handlers for class `ToBinary` applied to `ProofField` -/
#guard_msgs in
structure ProofField where
  val : Nat
  pos : val > 0
deriving ToBinary

/-! ### FromBinary on type with proof fields -/
/-- error: None of the deriving handlers for class `FromBinary` applied to `ProofField2` -/
#guard_msgs in
structure ProofField2 where
  val : Nat
  pos : val > 0
deriving FromBinary

/-! ## Runtime Roundtrip Tests -/

instance : Arbitrary Pair where
  arbitrary := Pair.mk <$> arbitrary <*> arbitrary

instance : Shrinkable Pair where

instance : Arbitrary Color where
  arbitrary := do
    let n ← chooseNat
    return [Color.r, .g, .b][n % 3]!

instance : Shrinkable Color where

instance : Arbitrary Shape where
  arbitrary := do
    let n ← chooseNat
    if n % 2 == 0 then
      return .circle (← arbitrary)
    else
      return .rect (← arbitrary) (← arbitrary)

instance : Shrinkable Shape where

instance : Arbitrary Msg where
  arbitrary := do
    let n ← chooseNat
    if n % 2 == 0 then
      return .flagged (← arbitrary) (← arbitrary)
    else
      return .plain (← arbitrary)

instance : Shrinkable Msg where

instance : Arbitrary Cmd where
  arbitrary := do
    let n ← chooseNat
    if n % 2 == 0 then
      return .exec (← arbitrary) (← arbitrary)
    else
      return .noop

instance : Shrinkable Cmd where

instance [Arbitrary α] : Arbitrary (Box α) where
  arbitrary := Box.mk <$> arbitrary

instance [Shrinkable α] : Shrinkable (Box α) where
  shrink b := (Shrinkable.shrink b.val).map Box.mk

def runBlobDerivingTests : IO (Nat × Nat) := do
  let mut success := 0
  let mut failure := 0
  for t in ([Pair, Color, Shape, Msg, Cmd, Box Nat, Box String] : List Testable) do
    if (← Testable.test t) then
      success := success + 1
    else
      failure := failure + 1
  pure (success, failure)

end SQLite.Test.BlobDeriving
