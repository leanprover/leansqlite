/-
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
Author: David Thrane Christiansen
-/

import Plausible

import Plausible.ArbitraryFueled
import Plausible.Gen

import SQLite.Blob

namespace SQLite.Test.Blob
open SQLite.Blob
open Plausible Gen Arbitrary
open Lean

def toFromBinary [ToBinary α] [FromBinary α] [BEq α] (pre post : ByteArray) (x : α) : Bool :=
  let data : ByteArray := ToBinary.serializer x pre ++ post
  let cursor := pre.size
  if h : cursor ≤ data.size then
    let s : Deserializer.State := { data, cursor }
    match FromBinary.deserializer (α := α) s with
    | Except.error _ => false
    | Except.ok (v, s') =>
      s'.cursor ≥ s.cursor &&
      s'.data == s.data &&
      s'.data.extract s'.cursor s'.data.size == post &&
      v == x
  else false

open scoped Plausible.Decorations in
def testProp
    (p : Prop) (cfg : Configuration := {})
    (p' : Decorations.DecorationsOf p := by mk_decorations) [Testable p'] :
    IO (TestResult p') :=
  Testable.checkIO p' (cfg := cfg)

instance : Arbitrary Unit where
  arbitrary := pure ()

instance : Shrinkable Unit where

instance : Arbitrary ByteArray where
  arbitrary := do
    let count ← chooseNat
    let mut out : ByteArray := .empty
    for _ in 0...count do
      out := out.push (← arbitrary)
    return out

instance : Shrinkable ByteArray where
  shrink arr :=
    if arr.isEmpty then []
    else
      [.empty, arr.extract 0 (arr.size / 2), arr.extract (arr.size / 2) arr.size]

instance : Arbitrary String where
  arbitrary := do
    let count ← chooseNat
    let mut out : String := ""
    for _ in 0...count do
      out := out.push (← arbitrary)
    return out

local instance : Repr ByteArray where
  reprPrec xs n := Repr.addAppParen (.group <| ".ofArray" ++ .line ++ repr (xs.foldl (init := #[]) (Array.push))) n


def testUnit' := testProp <| ∀ (u : Unit), u == u

def inst : Testable
      (NamedBinder "pre"
        (∀ (pre : ByteArray),
          NamedBinder "post" (∀ (post : ByteArray), NamedBinder "u" (∀ (u : Unit), toFromBinary pre post u = true)))) :=
  .varTestable

structure CanTest where
  α : Type
  [instTo : ToBinary α]
  [instFrom : FromBinary α]
  [instArb : Arbitrary α]
  [instRepr : Repr α]
  [instBEq : BEq α]
  [instShrink : Shrinkable α]

instance [ToBinary α] [FromBinary α] [Arbitrary α] [Repr α] [BEq α] [Shrinkable α] : CoeDep Type α CanTest where
  coe := {α}

instance : Coe CanTest Type where
  coe t := t.α

def Testable.test' (t : CanTest) : let {α, ..} := t; IO (TestResult (∀ pre post (u : α), toFromBinary pre post u = true)) := do
  let {α, ..} := t
  testProp <| ∀ pre post (u : α), toFromBinary pre post u = true


def Testable.test (t : CanTest) : IO Bool := do
  let {α, ..} := t
  match (← testProp <| ∀ pre post (u : α), toFromBinary pre post u = true) with
  | .success _ =>
    pure true
  | other =>
    IO.println other.toString
    return false

instance : Arbitrary Std.Format.FlattenBehavior where
  arbitrary := oneOf #[pure .allOrNone, pure .fill]

instance : Shrinkable Std.Format.FlattenBehavior where

instance : BEq Std.Format.FlattenBehavior where
  beq
    | .allOrNone, .allOrNone => true
    | .fill, .fill => true
    | _, _ => false

instance : Repr Std.Format.FlattenBehavior where
  reprPrec f _ := match f with
    | .allOrNone => ".allOrNone"
    | .fill => ".fill"

instance : Arbitrary Name where
  arbitrary := do
    let depth ← chooseNat
    let mut n : Name := .anonymous
    for _ in 0...depth do
      n := ← oneOf #[.str n <$> arbitrary, .num n <$> arbitrary] (by grind)
    return n

instance : Shrinkable Name where
  shrink
    | .anonymous => []
    | .str pre s => [.anonymous, pre] ++ (Shrinkable.shrink s).map (.str pre ·)
    | .num pre n => [.anonymous, pre] ++ (Shrinkable.shrink n).map (.num pre ·)

instance : Arbitrary Format where
  arbitrary := do go (← chooseNat)
where
  go : Nat → Gen Format
    | 0 => do
      oneOf #[
        pure .nil,
        pure .line,
        .align <$> chooseAny Bool,
        .text <$> arbitrary
      ]
    | n + 1 => do
      let gens := #[
        pure .nil,
        pure .line,
        .align <$> chooseAny Bool,
        .text <$> arbitrary,
        .nest <$> arbitrary <*> go n,
        .append <$> go n <*> go n,
        @Format.group <$> go n <*> arbitrary,
        .tag <$> arbitrary <*> go n
      ]
      have : 0 < gens.size := by grind
      oneOf gens this

instance : Shrinkable Format where
  shrink := go
where
  go
    | .nil | .line => []
    | .align _ => [.nil]
    | .text _ => [.nil]
    | .nest i f => [.nil, f] ++ (Shrinkable.shrink i).map (.nest · f) ++ (go f).map (.nest i ·)
    | .append f g => [.nil, f, g] ++ (go f).zipWith .append (go g)
    | .group f b => [.nil, f] ++ (go f).map (.group · b)
    | .tag i f => [.nil, f] ++ (Shrinkable.shrink i).map (.tag · f) ++ (go f).map (.tag i)

instance : BEq Format where
  beq := go
where
  go : Format → Format → Bool
    | .nil, .nil => true
    | .nil, _ => false
    | .line, .line => true
    | .line, _ => false
    | .align a, .align b => a == b
    | .align _, _ => false
    | .text a, .text b => a == b
    | .text _, _ => false
    | .nest i f, .nest j g => i == j && go f g
    | .nest .., _ => false
    | .append f1 f2, .append g1 g2 => go f1 g1 && go f2 g2
    | .append .., _ => false
    | .group f b, .group g c => go f g && b == c
    | .group .., _ => false
    | .tag i f, .tag j g => i == j && go f g
    | .tag .., _ => false


instance : Repr Format where
  reprPrec f _ := s!"{f}"

instance : Arbitrary FVarId where
  arbitrary := return { name := ← arbitrary }

instance : Shrinkable FVarId where
  shrink x := (Shrinkable.shrink x.name).map ({ name := · })

instance : Arbitrary MVarId where
  arbitrary := return { name := ← arbitrary }

instance : Shrinkable MVarId where
  shrink x := (Shrinkable.shrink x.name).map ({ name := · })

instance : Arbitrary LevelMVarId where
  arbitrary := return { name := ← arbitrary }

instance : Shrinkable LevelMVarId where
  shrink x := (Shrinkable.shrink x.name).map ({ name := · })

instance : Arbitrary Level where
  arbitrary := do go (← chooseNat)
where
  go : Nat → Gen Level
    | 0 => do
      oneOf #[
        pure .zero,
        .param <$> arbitrary,
        .mvar <$> arbitrary
      ]
    | n + 1 => do
      let gens := #[
        pure .zero,
        .succ <$> go n,
        .max <$> go n <*> go n,
        .imax <$> go n <*> go n,
        .param <$> arbitrary,
        .mvar <$> arbitrary
      ]
      have : 0 < gens.size := by grind
      oneOf gens this

instance : Shrinkable Level where
  shrink := go
where
  go
    | .zero => []
    | .succ l => [.zero, l] ++ (go l).map .succ
    | .max l1 l2 => [.zero, l1, l2] ++ (go l1).zipWith .max (go l2)
    | .imax l1 l2 => [.zero, l1, l2] ++ (go l1).zipWith .imax (go l2)
    | .param n => [.zero] ++ (Shrinkable.shrink n).map .param
    | .mvar m => [.zero] ++ (Shrinkable.shrink m).map .mvar

instance : Arbitrary BinderInfo where
  arbitrary := oneOf #[
    pure .default,
    pure .implicit,
    pure .strictImplicit,
    pure .instImplicit
  ]

instance : Shrinkable BinderInfo where

instance : BEq BinderInfo where
  beq
    | .default, .default => true
    | .default, _ => false
    | .implicit, .implicit => true
    | .implicit, _ => false
    | .strictImplicit, .strictImplicit => true
    | .strictImplicit, _ => false
    | .instImplicit, .instImplicit => true
    | .instImplicit, _ => false

instance : Arbitrary Literal where
  arbitrary := oneOf #[
    .natVal <$> arbitrary,
    .strVal <$> arbitrary
  ]

instance : Shrinkable Literal where
  shrink
    | .natVal n => (Shrinkable.shrink n).map .natVal
    | .strVal s => (Shrinkable.shrink s).map .strVal

def typesToTest : List CanTest := [
  Unit, Nat, UInt8, UInt64, ByteArray, Int, Array Nat, Bool, Bool ⊕ Nat,
  List Char, String, (String × Bool × Nat),
  Std.Format.FlattenBehavior, Format, Name,
  FVarId, MVarId, LevelMVarId, Level, BinderInfo, Literal
]

def runBlobTests : IO (Nat × Nat) := do
  let mut success := 0
  let mut failure := 0
  for t in typesToTest do
    if (← Testable.test t) then
      success := success + 1
    else
      failure := failure + 1
  pure (success, failure)
