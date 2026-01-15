
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

structure Testable where
  α : Type
  [instTo : ToBinary α]
  [instFrom : FromBinary α]
  [instArb : Arbitrary α]
  [instRepr : Repr α]
  [instBEq : BEq α]
  [instShrink : Shrinkable α]

instance [ToBinary α] [FromBinary α] [Arbitrary α] [Repr α] [BEq α] [Shrinkable α] : CoeDep Type α Testable where
  coe := {α}

instance : Coe Testable Type where
  coe t := t.α

def Testable.test' (t : Testable) : let {α, ..} := t; IO (TestResult (∀ pre post (u : α), toFromBinary pre post u = true)) := do
  let {α, ..} := t
  testProp <| ∀ pre post (u : α), toFromBinary pre post u = true


def Testable.test (t : Testable) : IO Bool := do
  let {α, ..} := t
  match (← testProp <| ∀ pre post (u : α), toFromBinary pre post u = true) with
  | .success _ =>
    pure true
  | other =>
    IO.println other.toString
    return false

def runBlobTests : IO (Nat × Nat) := do
  let mut success := 0
  let mut failure := 0
  for t in ([Unit, Nat, UInt8, UInt64, ByteArray, Int, Array Nat, Bool, Bool ⊕ Nat, List Char, String, (String × Bool × Nat)] : List Testable) do
    if (← Testable.test t) then
      success := success + 1
    else
      failure := failure + 1
  pure (success, failure)
