/-
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
Author: David Thrane Christiansen
-/

namespace SQLite.Test

structure Config where
  verbose : Bool
  report : String → IO Unit

structure Stats where
  successes : Nat
  failures : Nat

def Stats.total (s : Stats) := s.successes + s.failures

abbrev TestM := ReaderT Config (ReaderT (IO.Ref (Option String)) (StateRefT Stats IO))

def withHeader (header : String) (action : TestM α) : TestM α := do
  let config ← readThe Config
  let headerRef ← readThe (IO.Ref (Option String))
  if config.verbose then
    config.report header
  else
    headerRef.modify fun h => some (h.getD "" ++ header)
  try
    action
  finally
    if !config.verbose then
      headerRef.set none

def report (msg : String) : TestM Unit := do
  (← readThe Config).report msg

def verbose (act : TestM Unit) : TestM Unit := do
  if (← readThe Config).verbose then act

def recordSuccess (message : String) : TestM Unit := do
  verbose do
    report s!"✓ {message}"
  modify fun s => { s with successes := s.successes + 1 }

def recordFailure (message : String) : TestM Unit := do
  let headerRef ← readThe (IO.Ref (Option String))
  unless (← readThe Config).verbose do
    let pending ← headerRef.get
    match pending with
    | some h =>
      report h
      headerRef.set none
    | none => pure ()
  report s!"✗ {message}"
  modify fun s => { s with failures := s.failures + 1 }

def expectSuccess' (onSuccess : α → String) (act : IO α) (onFailure : IO.Error → String := fun e => s!"ERROR:\n\t{e}") : TestM α :=
  try
    let val ← act
    recordSuccess (onSuccess val)
    pure val
  catch e => recordFailure (onFailure e); throw e

def expectSuccess (onSuccess : String) (act : IO α) (onFailure : IO.Error → String := fun e => s!"ERROR: {onSuccess}\n\t{e}") : TestM α :=
  try
    let val ← act
    recordSuccess onSuccess
    pure val
  catch e => recordFailure (onFailure e); throw e

def expectFailure' (onFailure : IO.Error → String) (act : IO α) (onSuccess : α → String := fun _ =>  "Should have failed but didn't") : TestM Unit :=
  try
    let val ← act
    recordFailure (onSuccess val)
  catch e => recordSuccess (onFailure e)

def expectFailure (onFailure : String) (act : IO α) (onSuccess : α → String := fun _ => s!"Should have failed but didn't: {onFailure}") : TestM Unit :=
  try
    let val ← act
    recordFailure (onSuccess val)
  catch _ => recordSuccess onFailure

def expect (cond : Bool) (msg : String) : TestM Unit :=
  if cond then pure () else recordFailure msg
