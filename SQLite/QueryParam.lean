/-
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
Author: David Thrane Christiansen
-/
module

public import SQLite.LowLevel
public import SQLite.Blob

set_option doc.verso true
set_option linter.missingDocs true

namespace SQLite

/--
Provides a canonical means of binding a non-null value of type {name}`α` in a SQLite query.

A deriving handler is available for single-constructor inductive types with exactly one non-proof
field. Proof fields (fields with {lean}`Prop` type) are ignored, so types like subtypes work. The
generated instance delegates to the wrapped type's instance.
-/
public class QueryParam (α : Type u) where
  bind : Stmt → Int32 → α → IO Unit

public instance : QueryParam String where
  bind := Stmt.bindText

public instance : QueryParam ByteArray where
  bind := Stmt.bindBlob

public instance : QueryParam Int32 where
  bind := Stmt.bindInt32

public instance : QueryParam Int64 where
  bind := Stmt.bindInt64

public instance : QueryParam Float where
  bind := Stmt.bindFloat

public instance : QueryParam Unit where
  bind stmt index _ := Stmt.bindNull stmt index

public instance : QueryParam Bool where
  bind stmt index b := Stmt.bindInt32 stmt index (if b then 1 else 0)

public instance : QueryParam Nat where
  bind stmt i n := Stmt.bindText stmt i (toString n)

open Blob in
/--
Defines an instance that binds a parameter as a binary blob, according to its {name}`ToBinary`
instance.
-/
public def QueryParam.asBlob [ToBinary α] : QueryParam α where
  bind stmt i x := Stmt.bindBlob stmt i (toBinary x)

/--
Provides a canonical means of binding a potentially null value of type {name}`α` in a SQLite query.
-/
public class NullableQueryParam (α : Type u) where
  bind : Stmt → Int32 → α → IO Unit

public instance [QueryParam α] : NullableQueryParam α where
  bind stmt i val := QueryParam.bind stmt i val

public instance [QueryParam α] : NullableQueryParam (Option α) where
  bind stmt i
    | none => stmt.bindNull i
    | some val => QueryParam.bind stmt i val

/--
Binds a query parameter based on its Lean type's {name}`NullableQueryParam` instance.

{name}`index` is the 1-based positional index of the parameter to be bound.
-/
def Stmt.bind [NullableQueryParam α] (stmt : Stmt) (index : Int32) (param : α) : IO Unit := do
  NullableQueryParam.bind stmt index param

end SQLite
