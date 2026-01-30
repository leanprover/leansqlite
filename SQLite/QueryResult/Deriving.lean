/-
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
Author: David Thrane Christiansen
-/
module

import Lean.Elab.Deriving.Basic
import Lean.Elab.Deriving.Util
import SQLite.QueryParam
import SQLite.QueryResult

set_option doc.verso true
set_option linter.missingDocs true

/-!
This module provides deriving handlers for {name}`SQLite.Row`, {name}`SQLite.ResultColumn`, and
{name}`SQLite.QueryParam`.
-/

namespace SQLite

open Lean Elab Meta Parser Term Command
open Elab.Deriving

variable [Monad m] [MonadEnv m] [MonadError m]

/--
Returns the number of explicit (non-implicit, non-instance) parameters for a constructor,
excluding the inductive type's parameters.
-/
private def getCtorFieldCount (ctorName : Name) : m Nat := do
  let ctorInfo ← getConstInfoCtor ctorName
  -- numFields is the number of fields excluding the inductive type's parameters
  return ctorInfo.numFields

/--
Information about constructor fields, distinguishing proofs from data.
-/
private structure CtorFieldInfo where
  /-- Total number of fields -/
  totalFields : Nat
  /-- Indices of non-proof fields (0-indexed) -/
  dataFieldIndices : Array Nat
  deriving Inhabited

/--
Analyzes constructor fields to identify which are proofs (have {lean}`Prop` type) and which are data.
Returns the total field count and the indices of non-proof fields.
-/
private def analyzeCtorFields (ctorName : Name) : MetaM CtorFieldInfo := do
  let ctorInfo ← getConstInfoCtor ctorName
  forallTelescopeReducing ctorInfo.type fun args _ => do
    -- Skip the inductive type's parameters
    let fieldArgs := args[ctorInfo.numParams:].toArray
    let mut dataIndices : Array Nat := #[]
    for i in [:fieldArgs.size] do
      let argType ← inferType fieldArgs[i]!
      -- Check if the type is a Prop, and if so, skip it.
      if !(← isProp argType) then
        dataIndices := dataIndices.push i
    return { totalFields := fieldArgs.size, dataFieldIndices := dataIndices }

/--
Returns true if the inductive type has exactly one constructor.
-/
private def isSingleConstructor (indVal : InductiveVal) : Bool :=
  indVal.ctors.length == 1

/--
Gets the InductiveVal for a name, if it exists.
-/
private def getInductiveVal? (env : Environment) (name : Name) : Option InductiveVal :=
  match env.find? name with
  | some (.inductInfo val) => some val
  | _ => none

/--
Variant of {name}`Lean.Elab.Deriving.mkHeader` that doesn't add an explicit binder
for the target value. We only need implicit type parameters and instance binders.
The {name}`constraintClass` parameter specifies which type class to use for instance constraints.
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

/-!
# {name}`SQLite.Row`

Automatically generates {name}`Row` instances for single-constructor inductive types. Each constructor
parameter is read sequentially from the row using its {lean}`ResultColumn` instance.
-/

/--
Creates the body of the {name}`Row.read` function for a single-constructor inductive.
-/
private def mkRowReadBody (indVal : InductiveVal) : TermElabM Term := do
  let ctorName := indVal.ctors[0]!
  let numFields ← getCtorFieldCount ctorName

  -- Start with: pure Ctor
  -- Chain: <*> RowReader.field for each field
  let mut result ← `(pure $(mkCIdent ctorName))
  for _ in [:numFields] do
    result ← `($result <*> RowReader.field)
  return result

/--
Generates a private definition that will be used as the {name}`Row.read` implementation.
-/
private def mkRowAuxFunction (ctx : Deriving.Context) (i : Nat) : TermElabM Command := do
  let auxFunName := ctx.auxFunNames[i]!
  let indVal := ctx.typeInfos[i]!

  -- Use ResultColumn constraints since RowReader.field uses ResultColumn
  let header ← mkHeader ``ResultColumn indVal

  -- Generate the body
  let body ← mkRowReadBody indVal

  -- Create the function definition
  `(private def $(Lean.mkIdent auxFunName) $header.binders:bracketedBinder* : RowReader $(header.targetType) := $body)

/--
Creates instance commands for {name}`Row`. This is a custom version of {name}`Elab.Deriving.mkInstanceCmds`
because {name}`Row` instances need {name}`ResultColumn` constraints (what {name}`RowReader.field` uses) rather
than {name}`Row` constraints.
-/
private def mkRowInstanceCmds (ctx : Deriving.Context) (typeNames : Array Name) : TermElabM (Array Command) := do
  let mut instances := #[]
  for i in [:ctx.typeInfos.size] do
    let indVal := ctx.typeInfos[i]!
    if typeNames.contains indVal.name then
      let auxFunName := ctx.auxFunNames[i]!
      let argNames ← mkInductArgNames indVal
      let binders ← mkImplicitBinders argNames
      -- Use ResultColumn instead of Row for instance binders
      let binders := binders ++ (← mkInstImplicitBinders ``ResultColumn indVal argNames)
      -- Convert Array Syntax to TSyntaxArray for the quotation
      let binders : TSyntaxArray `Lean.Parser.Term.implicitBinder := binders.map (⟨·⟩)
      let indType ← mkInductiveApp indVal argNames
      let type ← `(Row $indType)
      let val ← `(⟨$(Lean.mkIdent auxFunName)⟩)
      let instCmd ← `(instance $binders:implicitBinder* : $type := $val)
      instances := instances.push instCmd
  return instances

/--
The main deriving handler for the {name}`Row` type class.

This handler supports any single-constructor inductive type. Each constructor parameter
is read sequentially using its {name}`ResultColumn` instance.
-/
def mkRowInstanceHandler (declNames : Array Name) : CommandElabM Bool := do
  let env ← getEnv
  -- Only handle single-constructor inductives
  if ← declNames.allM fun name => do
    let some indVal := getInductiveVal? env name | return false
    return isSingleConstructor indVal
  then
    let ctx ← liftTermElabM <| mkContext ``Row "row" declNames[0]!
    let auxFunCmd ← liftTermElabM <| mkRowAuxFunction ctx 0
    elabCommand auxFunCmd
    let instanceCmds ← liftTermElabM <| mkRowInstanceCmds ctx declNames
    instanceCmds.forM elabCommand
    return true
  else
    return false

initialize
  registerDerivingHandler ``Row mkRowInstanceHandler

/-!
# {name}`SQLite.ResultColumn`

Automatically generates {name}`ResultColumn` instances for single-constructor, single-field inductive
types (trivial wrappers).
-/

/--
Generates a private definition that will be used as the {name}`ResultColumn.get` implementation.
-/
private def mkResultColumnAuxFunction (ctx : Deriving.Context) (i : Nat) : TermElabM Command := do
  let auxFunName := ctx.auxFunNames[i]!
  let indVal := ctx.typeInfos[i]!
  let header ← mkHeader ``ResultColumn indVal
  let ctorName := indVal.ctors[0]!

  let body ← `(fun stmt col => $(mkCIdent ctorName) <$> ResultColumn.get stmt col)

  `(private def $(Lean.mkIdent auxFunName) $header.binders:bracketedBinder* : Stmt → Int32 → IO $(header.targetType) := $body)

/--
The main deriving handler for the {name}`ResultColumn` type class.

This handler supports single-constructor, single-field inductive types (trivial wrappers),
since {name}`ResultColumn` reads a single column value from a SQLite query.
-/
def mkResultColumnInstanceHandler (declNames : Array Name) : CommandElabM Bool := do
  let env ← getEnv
  -- Only handle single-constructor, single-field inductives
  let canHandle ←
    declNames.allM fun name => do
      let some indVal := getInductiveVal? env name | return false
      if !isSingleConstructor indVal then return false
      let numFields ← liftTermElabM <| getCtorFieldCount indVal.ctors[0]!
      return numFields == 1
  if canHandle then
    let ctx ← liftTermElabM <| mkContext ``ResultColumn "resultColumn" declNames[0]!
    let auxFunCmd ← liftTermElabM <| mkResultColumnAuxFunction ctx 0
    elabCommand auxFunCmd
    let instanceCmds ← liftTermElabM <| Elab.Deriving.mkInstanceCmds ctx ``ResultColumn declNames
    instanceCmds.forM elabCommand
    return true
  else
    return false

initialize
  registerDerivingHandler ``ResultColumn mkResultColumnInstanceHandler

/-!
# {name}`SQLite.QueryParam`

Automatically generates {name}`QueryParam` instances for single-constructor inductive types with
exactly one non-proof field. Proof fields (fields with {lean}`Prop` type) are ignored, allowing
deriving for types like subtypes.
-/

/--
Generates a private definition that will be used as the {name}`QueryParam.bind` implementation.
-/
private def mkQueryParamAuxFunction (ctx : Deriving.Context) (i : Nat) : TermElabM Command := do
  let auxFunName := ctx.auxFunNames[i]!
  let indVal := ctx.typeInfos[i]!
  let header ← mkHeader ``QueryParam indVal

  -- Analyze fields to find which is the data field and which are proofs
  let fieldInfo ← analyzeCtorFields indVal.ctors[0]!
  let dataFieldIdx := fieldInfo.dataFieldIndices[0]!

  -- Build pattern: use `x` for the data field, `_` for proof fields
  let mut patternElems : Array (TSyntax `term) := #[]
  for idx in [:fieldInfo.totalFields] do
    if idx == dataFieldIdx then
      patternElems := patternElems.push (← `(x))
    else
      patternElems := patternElems.push (← `(_))

  let pattern ← `(⟨$patternElems,*⟩)
  let body ← `(fun stmt idx $pattern => QueryParam.bind stmt idx x)

  `(private def $(Lean.mkIdent auxFunName) $header.binders:bracketedBinder* : Stmt → Int32 → $(header.targetType) → IO Unit := $body)

/--
The main deriving handler for the {name}`QueryParam` type class.

This handler supports single-constructor inductive types with exactly one non-proof field.
Proof fields (fields with {lean}`Prop` type) are ignored. This allows deriving for types like
subtypes that carry proof obligations.
-/
def mkQueryParamInstanceHandler (declNames : Array Name) : CommandElabM Bool := do
  let env ← getEnv
  -- Only handle single-constructor inductives with exactly one non-proof field
  if ← declNames.allM fun name => do
    let some indVal := getInductiveVal? env name | return false
    if !isSingleConstructor indVal then return false
    let fieldInfo ← liftTermElabM <| analyzeCtorFields indVal.ctors[0]!
    return fieldInfo.dataFieldIndices.size == 1
  then
    let ctx ← liftTermElabM <| mkContext ``QueryParam "queryParam" declNames[0]!
    let auxFunCmd ← liftTermElabM <| mkQueryParamAuxFunction ctx 0
    elabCommand auxFunCmd
    let instanceCmds ← liftTermElabM <| Elab.Deriving.mkInstanceCmds ctx ``QueryParam declNames
    instanceCmds.forM elabCommand
    return true
  else
    return false

initialize
  registerDerivingHandler ``QueryParam mkQueryParamInstanceHandler

end SQLite
