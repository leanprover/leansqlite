/-
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
Author: David Thrane Christiansen
-/
module
public import Lean.Data.Json.Basic
public import Lean.Data.Json.FromToJson

set_option linter.missingDocs true
set_option doc.verso true

namespace SQLite.Blob

/--
A serializer appends a representation of data to a byte array.
-/
public abbrev Serializer (α : Type u) := α → ByteArray → ByteArray

namespace Deserializer

local instance : Repr ByteArray where
  reprPrec xs n := Repr.addAppParen (.group <| ".ofArray" ++ .line ++ repr (xs.foldl (init := #[]) (Array.push))) n

/--
A deserializer reads a value from a byte array. Its state consists of an array and a cursor that
indicates the next byte to be read.
-/
public structure State : Type u where
  /-- The serialized data -/
  data : ByteArray
  /-- The next byte to be read. If all bytes have been read, then the cursor is the size of the data. -/
  cursor : Nat
  cursor_le_data_size : cursor ≤ data.size := by grind
deriving Repr

/--
Initializes a new state with a byte array. The cursor points at the first byte.
-/
public def State.ofByteArray (bytes : ByteArray) : State where
  data := bytes
  cursor := 0

end Deserializer

/--
A deserializer either reads a value of type {name}`α`, modifying the cursor position, or throws an
exception. Deserializers should not modify the byte array.
-/
public abbrev Deserializer (α : Type u) := StateT Deserializer.State (Except String) α

namespace Deserializer
/--
Runs a deserializer. Throws an exception if not all the data are consumed.
-/
public def run (d : Deserializer α) (data : ByteArray) : Except String α := do
  let (value, ⟨_, i, _⟩) ← StateT.run d (.ofByteArray data)
  if i ≠ data.size then throw s!"{data.size - i} bytes were not read during deserialization"
  return value

/--
Deserializes a single byte.
-/
public def byte : Deserializer UInt8 := fun
  | { data, cursor, .. } =>
    if h : cursor = data.size then throw "No more data"
    else
      .ok (data[cursor], { data, cursor := cursor + 1 })

/--
Extracts the next byte without advancing the cursor.
-/
public def peekByte : Deserializer UInt8 := fun
  | { data, cursor, .. } =>
    if h : cursor = data.size then throw "No more data"
    else
      .ok (data[cursor], { data, cursor })

/--
Deserializes {name}`n` bytes.
-/
public def nbytes (n : Nat) : Deserializer ByteArray := fun
  | { data, cursor, .. } =>
    if h : cursor + n > data.size then throw "No more data"
    else
      .ok (data.extract cursor (cursor + n), { data, cursor := cursor + n })

end Deserializer

/--
A canonical serializer for values of type {name}`α`
-/
public class ToBinary (α : Type u) where
  serializer : Serializer α

/--
A canonical deserializer for type {name}`α`
-/
public class FromBinary (α : Type u) where
  deserializer : Deserializer α

/--
Uses the {lean}`ToBinary α` instance to serialize a value.
-/
public def toBinary [ToBinary α] (x : α) : ByteArray := ToBinary.serializer x .empty

/--
Uses the {lean}`ToBinary α` instance to serialize a value of the indicated type.
-/
public def toBinaryOf (α : Type u) [ToBinary α] (x : α) : ByteArray := toBinary x

/--
Uses the {lean}`FromBinary α` instance to deserialize a value.
-/
public def fromBinary [FromBinary α] (data : ByteArray) : Except String α :=
  Deserializer.run FromBinary.deserializer data

/--
Uses the {lean}`FromBinary α` instance to deserialize a value of the indicated type.
-/
public def fromBinaryOf (α : Type u) [FromBinary α] (data : ByteArray) : Except String α :=
  fromBinary data

/--
Constructs a {lean}`ToBinary α` instance from a {lean}`ToBinary β` instance that first transforms
the value with {name}`f` and then serializes the result. {name}`f` should be injective. This should
be paired with a {name}`FromBinary` instance built with `FromBinary.via` (if {name}`f` is
surjective) or `FromBinary.viaExcept` (if {name}`f` is not surjective).
-/
public def ToBinary.via [ToBinary β] (f : α → β) : ToBinary α where
  serializer x := ToBinary.serializer (f x)

/--
Constructs a {lean}`FromBinary β` instance from a {lean}`FromBinary α` instance that deserializes a
value of type {name}`α` and then transforms it with {name}`f`. This should be paired with a
{name}`ToBinary` instance built with {name}`ToBinary.via`.
-/
public def FromBinary.via [FromBinary α] (f : α → β) : FromBinary β where
  deserializer := f <$> FromBinary.deserializer (α := α)

/--
Constructs a {lean}`FromBinary β` instance from a {lean}`FromBinary α` instance that deserializes a
value of type {name}`α` and then transforms it with {name}`f`, which may perform further validation.
This should be paired with a {name}`ToBinary` instance built with {name}`ToBinary.via`.
-/
public def FromBinary.viaExcept [FromBinary α] (f : α → Except String β) : FromBinary β where
  deserializer := do
    let val : α ← FromBinary.deserializer
    f val

public instance : ToBinary Bool where
  serializer
    | true, b => b.push 1
    | false, b => b.push 0

public instance : FromBinary Bool where
  deserializer := do
    return (← .byte) ≠ 0

public instance : ToBinary UInt8 where
  serializer i b := b.push i

public instance : FromBinary UInt8 where
  deserializer := .byte

public instance : ToBinary UInt16 where
  serializer n b := b
    |>.push ((n >>> 8).toUInt8)
    |>.push (n.toUInt8)

public instance : FromBinary UInt16 where
  deserializer
  | { data, cursor, .. } =>
    if h : cursor ≥ data.size - 1 then throw "No more data"
    else
      let val : UInt16 :=
        data[cursor].toUInt16 <<< 8 |||
        data[cursor + 1].toUInt16
      .ok (val, { data, cursor := cursor + 2 })

public instance : ToBinary UInt32 where
  serializer n b := b
    |>.push ((n >>> 24).toUInt8)
    |>.push ((n >>> 16).toUInt8)
    |>.push ((n >>> 8).toUInt8)
    |>.push (n.toUInt8)

public instance : FromBinary UInt32 where
  deserializer
  | { data, cursor, .. } =>
    if h : cursor ≥ data.size - 3 then throw "No more data"
    else
      let val : UInt32 :=
        data[cursor].toUInt32 <<< 24 |||
        data[cursor + 1].toUInt32 <<< 16 |||
        data[cursor + 2].toUInt32 <<< 8 |||
        data[cursor + 3].toUInt32
      .ok (val, { data, cursor := cursor + 4 })

public instance : ToBinary UInt64 where
  serializer n b := b
    |>.push ((n >>> 56).toUInt8)
    |>.push ((n >>> 48).toUInt8)
    |>.push ((n >>> 40).toUInt8)
    |>.push ((n >>> 32).toUInt8)
    |>.push ((n >>> 24).toUInt8)
    |>.push ((n >>> 16).toUInt8)
    |>.push ((n >>> 8).toUInt8)
    |>.push (n.toUInt8)

public instance : FromBinary UInt64 where
  deserializer
  | { data, cursor, .. } =>
    if h : cursor ≥ data.size - 7 then throw "No more data"
    else
      let val : UInt64 :=
        data[cursor].toUInt64 <<< 56 |||
        data[cursor + 1].toUInt64 <<< 48 |||
        data[cursor + 2].toUInt64 <<< 40 |||
        data[cursor + 3].toUInt64 <<< 32 |||
        data[cursor + 4].toUInt64 <<< 24 |||
        data[cursor + 5].toUInt64 <<< 16 |||
        data[cursor + 6].toUInt64 <<< 8 |||
        data[cursor + 7].toUInt64
      .ok (val, { data, cursor := cursor + 8 })

public instance : ToBinary Nat where
  serializer := go
where
  go n b :=
    if n < 2 ^ 7 then
      b |> ToBinary.serializer n.toUInt8
    else
      let chunk := (n % 2^7).toUInt8 ||| (2^7).toUInt8
      b |> ToBinary.serializer chunk |> go (n / 2^7)

public instance : FromBinary Nat where
  deserializer := do
    let mut n := 0
    let mut shift := 0
    repeat
      let byte : UInt8 ← .byte
      if byte ≥ 2 ^ 7 then
        let k := (byte &&& 0x7F).toNat
        n := n ||| k <<< shift
      else
        n := n ||| byte.toNat <<< shift
        break
      shift := shift + 7
    return n

public instance : ToBinary Int where
  serializer
    | .ofNat n, b => b.push 0 |> ToBinary.serializer n
    | .negSucc n, b =>  b.push 1 |> ToBinary.serializer n

public instance : FromBinary Int where
  deserializer := do
    match (← .byte) with
    | 0 => .ofNat <$> FromBinary.deserializer
    | 1 => .negSucc <$> FromBinary.deserializer
    | other => throw s!"Expected tag 0 or 1 for `Int`, got {other}"

public instance : ToBinary Char := .via Char.val

public instance : FromBinary Char := .viaExcept fun (i : UInt32) =>
  if h : i.isValidChar then
    return .mk i h
  else throw s!"Invalid char: {i}"

public instance : ToBinary Float := .via Float.toBits

public instance : FromBinary Float := .via Float.ofBits

public instance [ToBinary α] [ToBinary β] : ToBinary (α × β) where
  serializer
    | (x, y), b =>  b |> ToBinary.serializer x |> ToBinary.serializer y

public instance [FromBinary α] [FromBinary β] : FromBinary (α × β) where
  deserializer := do return (← FromBinary.deserializer, ← FromBinary.deserializer)

public instance [ToBinary α] : ToBinary (Option α) where
  serializer
    | none => (·.push 0)
    | some x =>
      fun b => b |>.push 1 |> ToBinary.serializer x

public instance [FromBinary α] : FromBinary (Option α) where
  deserializer := do
    match (← .byte) with
    | 0 => return none
    | 1 => some <$> FromBinary.deserializer
    | other => throw s!"Expected 0 or 1 for `Option`, got {other}"

public instance [ToBinary α] {p : α → Prop} : ToBinary (Subtype p) := .via Subtype.val

public instance {p : α → Prop} [DecidablePred p] [FromBinary α] : FromBinary (Subtype p) := .viaExcept fun x =>
  if h : p x then return ⟨x, h⟩ else throw "Predicate not satisfied for subtype"

public instance {β : α → Type v} [ToBinary α] [{x : α} → ToBinary (β x)] : ToBinary (Sigma β) where
  serializer
    | ⟨x, y⟩, b => b |> ToBinary.serializer x |> ToBinary.serializer y

public instance {β : α → Type v} [FromBinary α] [{x : α} → FromBinary (β x)] : FromBinary (Sigma β) where
  deserializer := do
    let x ← FromBinary.deserializer
    let y ← FromBinary.deserializer
    return ⟨x, y⟩

public instance (priority := high) [Subsingleton α] [Inhabited α] : ToBinary α where
  serializer _ b := b

public instance (priority := high) [Subsingleton α] [Inhabited α] : FromBinary α where
  deserializer := return default

public instance : ToBinary String where
  serializer s b := b
    |> ToBinary.serializer s.utf8ByteSize
    |> (· ++ s.toByteArray)

public instance : FromBinary String where
  deserializer := do
    let size : Nat ← FromBinary.deserializer
    let bytes ← .nbytes size
    if h : bytes.IsValidUTF8 then
      return String.ofByteArray bytes h
    else
      throw "Invalid UTF-8"


public instance : ToBinary (Fin n) := .via Fin.val

public instance : FromBinary (Fin n) := .viaExcept fun i : Nat =>
  if h : i < n then
    return .mk i h
  else throw s!"Out of bounds for `Fin {n}`: {i}"

public instance : ToBinary ByteArray where
  serializer arr b :=
    arr.size.fold (init := ToBinary.serializer arr.size b) fun i h =>
      ToBinary.serializer arr[i]

public instance : FromBinary ByteArray where
  deserializer := do
    let size : Nat ← FromBinary.deserializer
    size.foldM (init := ByteArray.emptyWithCapacity size) fun _ _ arr => arr.push <$> .byte

public instance [ToBinary α] : ToBinary (Array α) where
  serializer arr b :=
    arr.size.fold (init := ToBinary.serializer arr.size b) fun i h =>
      ToBinary.serializer arr[i]

public instance [FromBinary α] : FromBinary (Array α) where
  deserializer := do
    let size : Nat ← FromBinary.deserializer
    size.foldM (init := Array.emptyWithCapacity size) fun _ _ arr => arr.push <$> FromBinary.deserializer

public instance [ToBinary α] : ToBinary (List α) := .via List.toArray

public instance [FromBinary α] : FromBinary (List α) := .via Array.toList


public instance : ToBinary Lean.JsonNumber := .via fun | { mantissa, exponent } => (mantissa, exponent)

public instance : FromBinary Lean.JsonNumber := .via fun (mantissa, exponent) => { mantissa, exponent }

public partial instance : ToBinary Lean.Json where
  serializer := go
where
  go
    | .null, b => b.push 0
    | .bool true, b => b.push 1
    | .bool false, b => b.push 2
    | .num n, b => b.push 3 |> ToBinary.serializer n
    | .str s, b => b.push 4 |> ToBinary.serializer s
    | .arr xs, b =>
      have : ToBinary Lean.Json := ⟨go⟩
      b.push 5 |> ToBinary.serializer xs
    | .obj xs, b =>
      have : ToBinary Lean.Json := ⟨go⟩
      b.push 6 |> ToBinary.serializer xs.toArray

public partial instance : FromBinary Lean.Json where
  deserializer := go
where
  go := do
    match (← .byte) with
    | 0 => return .null
    | 1 => return .bool true
    | 2 => return .bool false
    | 3 => .num <$> FromBinary.deserializer
    | 4 => .str <$> FromBinary.deserializer
    | 5 =>
      have : FromBinary Lean.Json := ⟨go⟩
      .arr <$> FromBinary.deserializer
    | 6 =>
      have : FromBinary Lean.Json := ⟨go⟩
      let contents : Array (String × Lean.Json) ← FromBinary.deserializer
      return .obj <| .ofArray contents
    | other => throw s!"Expected tag 0-6 for `Json`, got {other}"

/--
Creates a {name}`ToBinary` instance that first converts a value to JSON and then serializes the
result. This serialization is not as a JSON string, which can avoid the overhead of string escaping
in certain specialized circumstances.
-/
public def ToBinary.viaJson [Lean.ToJson α] : ToBinary α := via Lean.ToJson.toJson

/--
Creates a {name}`FromBinary` instance that first deserializes a JSON value and then attempts to
convert it to a value of type {name}`α`.
-/
public def FromBinary.viaJson [Lean.FromJson α] : FromBinary α where
  deserializer := do
    let json ← FromBinary.deserializer
    Lean.FromJson.fromJson? json
