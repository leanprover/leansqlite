/-
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
Author: David Thrane Christiansen
-/
module

import Lean.Elab.Deriving.Basic
import Lean.Elab.Deriving.Util
import SQLite.Blob.Classes

namespace SQLite.Blob

set_option doc.verso true
set_option linter.missingDocs true

open Lean Elab Meta Parser Term Command
open Elab.Deriving

/-! # Helpers -/

/--
Gets the {lean}`InductiveVal` for a name, if it exists.
-/
private def getInductiveVal? (env : Environment) (name : Name) : Option InductiveVal :=
  match env.find? name with
  | some (.inductInfo val) => some val
  | _ => none

/--
Variant of {name}`Lean.Elab.Deriving.mkHeader` that doesn't add an explicit binder for the target
value. We only need implicit type parameters and instance binders.
-/
private def mkHeader (constraintClass : Name) (indVal : InductiveVal) : TermElabM Header := do
  let argNames ← mkInductArgNames indVal
  let binders ← mkImplicitBinders argNames
  let targetType ← mkInductiveApp indVal argNames
  let instBinders ← mkInstImplicitBinders constraintClass indVal argNames
  let binders : Array (TSyntax `Lean.Parser.Term.bracketedBinder) := (binders ++ instBinders).map (⟨·⟩)
  return {
    binders := binders
    argNames := argNames
    targetNames := #[]
    targetType := targetType
  }

/--
Returns the number of explicit fields for a constructor, excluding the inductive type's parameters.
-/
private def getCtorFieldCount (ctorName : Name) : MetaM Nat := do
  let ctorInfo ← getConstInfoCtor ctorName
  return ctorInfo.numFields

/--
Checks whether any constructor field's type depends on a previous field. Returns {name}`true` if
there are no dependencies.
-/
private def hasNoFieldDependencies (ctorName : Name) : MetaM Bool := do
  let ctorInfo ← getConstInfoCtor ctorName
  forallTelescopeReducing ctorInfo.type fun args _ => do
    let fieldArgs := args[ctorInfo.numParams:].toArray
    let mut prevFVars : Std.HashSet FVarId := {}
    for i in [:fieldArgs.size] do
      let argType ← inferType fieldArgs[i]!
      if argType.hasAnyFVar (prevFVars.contains ·) then
        return false
      prevFVars := prevFVars.insert fieldArgs[i]!.fvarId!
    return true

/--
Checks whether all constructor fields are non-proof (data) fields.
Returns {name}`false` if any field has a {lean}`Prop` type.
-/
private def hasNoProofFields (ctorName : Name) : MetaM Bool := do
  let ctorInfo ← getConstInfoCtor ctorName
  forallTelescopeReducing ctorInfo.type fun args _ => do
    let fieldArgs := args[ctorInfo.numParams:].toArray
    for i in [:fieldArgs.size] do
      let argType ← inferType fieldArgs[i]!
      if ← isProp argType then
        return false
    return true

/--
Returns the tag type name: {name}`UInt8` if ≤ 256 constructors, otherwise {name}`Nat`.
-/
private def tagTypeName (numCtors : Nat) : Name :=
  if numCtors ≤ 256 then ``UInt8 else ``Nat

/-! # ToBinary Generation -/

/--
Generates the {name}`ToBinary` body for a zero-constructor type.
-/
private def mkToBinaryZeroCtorBody : TermElabM Term := `(nofun)

/--
Generates the {name}`ToBinary` body for a single-constructor type. No tag is emitted; fields are serialized
sequentially.
-/
private def mkToBinarySingleCtorBody (indVal : InductiveVal) : TermElabM Term := do
  let ctorName := indVal.ctors[0]!
  let numFields ← getCtorFieldCount ctorName
  let fieldNames : Array Name := (Array.range numFields).map fun i => Name.mkSimple s!"f_{i}"
  let patternElems : Array (TSyntax `term) := fieldNames.map fun n => mkIdent n
  let pattern ← `(⟨$patternElems,*⟩)
  let acc : TSyntax `term ← `(acc)
  let mut result : TSyntax `term := acc
  for name in fieldNames do
    result ← `($result |> ToBinary.serializer $(mkIdent name))
  `(fun | $pattern, acc => $result)

/--
Generates the ToBinary body for a multi-constructor type. Each constructor gets a sequential tag
({name}`UInt8` or {name}`Nat`), then fields are serialized.
-/
private def mkToBinaryMultiCtorBody (indVal : InductiveVal) : TermElabM Term := do
  let tagType := tagTypeName indVal.ctors.length
  let mut arms : Array (TSyntax ``matchAlt) := #[]
  for ctorIdx in [:indVal.ctors.length] do
    let ctorName := indVal.ctors[ctorIdx]!
    let numFields ← getCtorFieldCount ctorName
    let fieldNames : Array Name := (Array.range numFields).map fun i => Name.mkSimple s!"f_{i}"
    let patternElems : Array (TSyntax `term) := fieldNames.map fun n => mkIdent n
    let pattern ← `($(mkCIdent ctorName) $patternElems*)
    let tagLit ← `(($(Syntax.mkNumLit (toString ctorIdx)) : $(mkIdent tagType)))
    let acc : TSyntax `term ← `(acc)
    let mut result ← `($acc |> ToBinary.serializer $tagLit)
    for name in fieldNames do
      result ← `($result |> ToBinary.serializer $(mkIdent name))
    let arm ← `(matchAltExpr| | $pattern, acc => $result)
    arms := arms.push arm
  `(fun $arms:matchAlt*)

/--
Generates the auxiliary function definition for {name}`ToBinary`.
-/
private def mkToBinaryAuxFunction (ctx : Deriving.Context) (i : Nat) : TermElabM Command := do
  let auxFunName := ctx.auxFunNames[i]!
  let indVal := ctx.typeInfos[i]!
  let header ← mkHeader ``ToBinary indVal
  let targetType := header.targetType

  let body ← match indVal.ctors.length with
    | 0 => mkToBinaryZeroCtorBody
    | 1 => mkToBinarySingleCtorBody indVal
    | _ => mkToBinaryMultiCtorBody indVal

  if indVal.isRec then
    `(@[no_expose] partial def $(Lean.mkIdent auxFunName) $header.binders:bracketedBinder* : Serializer $targetType :=
        have : ToBinary $targetType := ⟨$(Lean.mkIdent auxFunName)⟩
        $body)
  else
    `(@[no_expose] def $(Lean.mkIdent auxFunName) $header.binders:bracketedBinder* : Serializer $targetType := $body)

/--
Creates instance commands for {name}`ToBinary`.
-/
private def mkToBinaryInstanceCmds (ctx : Deriving.Context) (typeNames : Array Name) : TermElabM (Array Command) := do
  let mut instances := #[]
  for i in [:ctx.typeInfos.size] do
    let indVal := ctx.typeInfos[i]!
    if typeNames.contains indVal.name then
      let auxFunName := ctx.auxFunNames[i]!
      let argNames ← mkInductArgNames indVal
      let binders ← mkImplicitBinders argNames
      let binders := binders ++ (← mkInstImplicitBinders ``ToBinary indVal argNames)
      let binders : TSyntaxArray `Lean.Parser.Term.implicitBinder := binders.map (⟨·⟩)
      let indType ← mkInductiveApp indVal argNames
      let type ← `(ToBinary $indType)
      let val ← `(⟨$(Lean.mkIdent auxFunName)⟩)
      let instCmd ← `(instance $binders:implicitBinder* : $type := $val)
      instances := instances.push instCmd
  return instances

/--
The main deriving handler for {name}`ToBinary`.
-/
def mkToBinaryInstanceHandler (declNames : Array Name) : CommandElabM Bool := do
  let env ← getEnv
  if ← declNames.allM fun name => do
    let some indVal := getInductiveVal? env name | return false
    liftTermElabM do
      for ctorName in indVal.ctors do
        if !(← hasNoProofFields ctorName) then return false
        if !(← hasNoFieldDependencies ctorName) then return false
      return true
  then
    let ctx ← liftTermElabM <| mkContext ``ToBinary "toBinaryAux" declNames[0]!
    let auxFunCmd ← liftTermElabM <| mkToBinaryAuxFunction ctx 0
    elabCommand auxFunCmd
    let instanceCmds ← liftTermElabM <| mkToBinaryInstanceCmds ctx declNames
    instanceCmds.forM elabCommand
    return true
  else
    return false

/--
Wraps a constructor in an explicit lambda to prevent {name}`optParam` defaults from reducing the arity.
Generates `fun f_0 f_1 ... => Ctor f_0 f_1 ...`.
-/
private def mkCtorLambda (ctorName : Name) (numFields : Nat) : TermElabM Term := do
  let fieldNames : Array (TSyntax `ident) := (Array.range numFields).map fun i => mkIdent (Name.mkSimple s!"f_{i}")
  let ctorApp ← `($(mkCIdent ctorName) $fieldNames*)
  `(fun $fieldNames* => $ctorApp)

/-! # FromBinary Generation -/

/--
Generates the {name}`FromBinary` body for a zero-constructor (uninhabited) type. The generated
deserializer immediately throws an error.
-/
private def mkFromBinaryZeroCtorBody (indVal : InductiveVal) : TermElabM Term := do
  let errorMsg := s!"Cannot deserialize uninhabited type `{indVal.name}`"
  let errorMsgLit := Syntax.mkStrLit errorMsg
  `(throw $errorMsgLit)

/--
Generates the {name}`FromBinary` body for a single-constructor type.
-/
private def mkFromBinarySingleCtorBody (indVal : InductiveVal) : TermElabM Term := do
  let ctorName := indVal.ctors[0]!
  let numFields ← getCtorFieldCount ctorName
  if numFields == 0 then
    `(pure ⟨⟩)
  else
    let ctorFn ← mkCtorLambda ctorName numFields
    let mut result ← `($ctorFn <$> FromBinary.deserializer)
    for _ in [1:numFields] do
      result ← `($result <*> FromBinary.deserializer)
    return result

/--
Generates the {name}`FromBinary` body for a multi-constructor type. Reads a tag, then dispatches to
the appropriate constructor.
-/
private def mkFromBinaryMultiCtorBody (indVal : InductiveVal) : TermElabM Term := do
  let numCtors := indVal.ctors.length
  let tagType := tagTypeName numCtors

  let mut matchArms : Array (TSyntax ``matchAlt) := #[]
  for ctorIdx in [:numCtors] do
    let ctorName := indVal.ctors[ctorIdx]!
    let numFields ← getCtorFieldCount ctorName
    let tagLit := Syntax.mkNumLit (toString ctorIdx)
    let armBody ← if numFields == 0 then
        `(pure ($(mkCIdent ctorName) : _))
      else
        let ctorFn ← mkCtorLambda ctorName numFields
        let mut result ← `($ctorFn <$> FromBinary.deserializer)
        for _ in [1:numFields] do
          result ← `($result <*> FromBinary.deserializer)
        pure result
    let matchArm ← `(matchAltExpr| | $tagLit => $armBody)
    matchArms := matchArms.push matchArm

  -- Error arm
  let typeName := indVal.name
  let errorMsg := s!"Expected tag 0-{numCtors - 1} for `{typeName}`, got "
  let errorMsgLit := Syntax.mkStrLit errorMsg
  let errorArm ← `(matchAltExpr| | other => throw ($errorMsgLit ++ toString other))
  matchArms := matchArms.push errorArm

  `(FromBinary.deserializer >>= fun (tag : $(mkIdent tagType)) =>
      match tag with $matchArms:matchAlt*)

/--
Generates the auxiliary function definition for FromBinary.
-/
private def mkFromBinaryAuxFunction (ctx : Deriving.Context) (i : Nat) : TermElabM Command := do
  let auxFunName := ctx.auxFunNames[i]!
  let indVal := ctx.typeInfos[i]!
  let header ← mkHeader ``FromBinary indVal
  let targetType := header.targetType

  let body ← match indVal.ctors.length with
    | 0 => mkFromBinaryZeroCtorBody indVal
    | 1 => mkFromBinarySingleCtorBody indVal
    | _ => mkFromBinaryMultiCtorBody indVal

  if indVal.isRec then
    `(@[no_expose] partial def $(Lean.mkIdent auxFunName) $header.binders:bracketedBinder* : Deserializer $targetType :=
        have : FromBinary $targetType := ⟨$(Lean.mkIdent auxFunName)⟩
        $body)
  else
    `(@[no_expose] def $(Lean.mkIdent auxFunName) $header.binders:bracketedBinder* : Deserializer $targetType := $body)

/--
Creates instance commands for {name}`FromBinary`.
-/
private def mkFromBinaryInstanceCmds (ctx : Deriving.Context) (typeNames : Array Name) : TermElabM (Array Command) := do
  let mut instances := #[]
  for i in [:ctx.typeInfos.size] do
    let indVal := ctx.typeInfos[i]!
    if typeNames.contains indVal.name then
      let auxFunName := ctx.auxFunNames[i]!
      let argNames ← mkInductArgNames indVal
      let binders ← mkImplicitBinders argNames
      let binders := binders ++ (← mkInstImplicitBinders ``FromBinary indVal argNames)
      let binders : TSyntaxArray `Lean.Parser.Term.implicitBinder := binders.map (⟨·⟩)
      let indType ← mkInductiveApp indVal argNames
      let type ← `(FromBinary $indType)
      let val ← `(⟨$(Lean.mkIdent auxFunName)⟩)
      let instCmd ← `(instance $binders:implicitBinder* : $type := $val)
      instances := instances.push instCmd
  return instances

/--
The main deriving handler for {name}`FromBinary`.
-/
def mkFromBinaryInstanceHandler (declNames : Array Name) : CommandElabM Bool := do
  let env ← getEnv
  if ← declNames.allM fun name => do
    let some indVal := getInductiveVal? env name | return false
    liftTermElabM do
      for ctorName in indVal.ctors do
        if !(← hasNoProofFields ctorName) then return false
        if !(← hasNoFieldDependencies ctorName) then return false
      return true
  then
    let ctx ← liftTermElabM <| mkContext ``FromBinary "fromBinaryAux" declNames[0]!
    let auxFunCmd ← liftTermElabM <| mkFromBinaryAuxFunction ctx 0
    elabCommand auxFunCmd
    let instanceCmds ← liftTermElabM <| mkFromBinaryInstanceCmds ctx declNames
    instanceCmds.forM elabCommand
    return true
  else
    return false

/-! # Registration -/

initialize
  registerDerivingHandler ``ToBinary mkToBinaryInstanceHandler

initialize
  registerDerivingHandler ``FromBinary mkFromBinaryInstanceHandler
