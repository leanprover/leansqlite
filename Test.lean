/-
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
Author: David Thrane Christiansen
-/
import SQLite

open SQLite

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

def recordSuccess (message : String) : TestM Unit := do
  let config ← readThe Config
  if config.verbose then
    config.report s!"✓ {message}"
  modify fun s => { s with successes := s.successes + 1 }

def recordFailure (message : String) : TestM Unit := do
  let config ← readThe Config
  let headerRef ← readThe (IO.Ref (Option String))
  if !config.verbose then
    let pending ← headerRef.get
    match pending with
    | some h =>
      config.report h
      headerRef.set none
    | none => pure ()
  config.report s!"✗ {message}"
  modify fun s => { s with failures := s.failures + 1 }

def testBasicInserts (db : SQLite) : TestM Unit :=
  withHeader "=== Testing Basic Inserts ===" do
    -- Create table with various data types
    let createStmt ← db.prepare "CREATE TABLE IF NOT EXISTS products (id INTEGER, name TEXT, price REAL, stock INTEGER, description TEXT);"
    let _ ← createStmt.step
    recordSuccess "Created table"

    -- Test bindInt, bindText, bindDouble, bindInt64
    let insertStmt ← db.prepare "INSERT INTO products (id, name, price, stock, description) VALUES (?, ?, ?, ?, ?);"
    insertStmt.bindInt 1 101
    insertStmt.bindText 2 "Laptop"
    insertStmt.bindDouble 3 1299.99
    insertStmt.bindInt64 4 5000000000  -- Large number to test int64
    insertStmt.bindText 5 "High-performance laptop"
    let _ ← insertStmt.step
    recordSuccess "Inserted product 1 (Laptop, bindInt/bindText/bindDouble/bindInt64)"

    -- Test bindNull
    insertStmt.reset
    insertStmt.bindInt 1 102
    insertStmt.bindText 2 "Mystery Item"
    insertStmt.bindNull 3  -- NULL price
    insertStmt.bindInt 4 0
    insertStmt.bindNull 5  -- NULL description
    let _ ← insertStmt.step
    recordSuccess "Inserted product 2 (Mystery Item with NULL values, bindNull)"

    -- Test with another product
    insertStmt.reset
    insertStmt.bindInt 1 103
    insertStmt.bindText 2 "Monitor"
    insertStmt.bindDouble 3 449.50
    insertStmt.bindInt64 4 2500000000
    insertStmt.bindText 5 "4K display"
    let _ ← insertStmt.step
    recordSuccess "Inserted product 3 (Monitor)"

def testQueryResults (db : SQLite) : TestM Unit :=
  withHeader "=== Query Results (Testing columnInt64, columnText, columnDouble) ===" do
    let selectStmt ← db.prepare "SELECT id, name, price, stock, description FROM products;"
    let selectColCount := selectStmt.columnCount
    recordSuccess s!"Column count for SELECT: {selectColCount}"

    let config ← readThe Config
    let mut hasRow ← selectStmt.step
    while hasRow do
      let id ← selectStmt.columnInt64 0
      let name ← selectStmt.columnText 1
      let price ← selectStmt.columnDouble 2
      let stock ← selectStmt.columnInt64 3
      let description ← selectStmt.columnText 4

      if config.verbose then
        config.report s!"  Product {id}: {name}"
        config.report s!"    Price: ${price}, Stock: {stock}"
        config.report s!"    Description: {description}"
      hasRow ← selectStmt.step


def testBlobSupport (db : SQLite) : TestM Unit :=
  withHeader "=== Testing BLOB Support ===" do
    -- Create a table with BLOB column
    let blobCreate ← db.prepare "CREATE TABLE IF NOT EXISTS files (id INTEGER, name TEXT, data BLOB);"
    let _ ← blobCreate.step

    -- Insert BLOB data
    let blobInsert ← db.prepare "INSERT INTO files (id, name, data) VALUES (?, ?, ?);"
    let testData : ByteArray := ByteArray.mk #[72, 101, 108, 108, 111] -- "Hello"
    blobInsert.bindInt 1 1
    blobInsert.bindText 2 "test.txt"
    blobInsert.bindBlob 3 testData
    let _ ← blobInsert.step
    recordSuccess "Inserted BLOB data"

    -- Insert NULL BLOB
    blobInsert.reset
    blobInsert.bindInt 1 2
    blobInsert.bindText 2 "empty.txt"
    blobInsert.bindNull 3
    let _ ← blobInsert.step
    recordSuccess "Inserted NULL BLOB"

    -- Insert empty BLOB
    blobInsert.reset
    blobInsert.bindInt 1 3
    blobInsert.bindText 2 "zero.txt"
    blobInsert.bindBlob 3 ByteArray.empty
    let _ ← blobInsert.step
    recordSuccess "Inserted empty BLOB"

    -- Query and check BLOBs
    let config ← readThe Config
    let blobSelect ← db.prepare "SELECT id, name, data FROM files;"
    let mut hasRow ← blobSelect.step
    while hasRow do
      let id ← blobSelect.columnInt64 0
      let name ← blobSelect.columnText 1
      let blob ← blobSelect.columnBlob 2

      if config.verbose then
        config.report s!"  File {id}: {name}"
        config.report s!"    BLOB size: {blob.size} bytes"

      hasRow ← blobSelect.step

def testUnboundParameters (db : SQLite) : TestM Unit := do
  let missingStmt ← db.prepare "INSERT INTO products (id, name, price) VALUES (?, ?, ?);"
  missingStmt.bindInt 1 999
  -- Intentionally skip binding parameters 2 and 3
  -- SQLite allows this - unbound parameters are NULL
  let _ ← missingStmt.step
  recordSuccess "Step with unbound parameters succeeded (SQLite treats them as NULL)"

def testInvalidParameterIndex (db : SQLite) : TestM Unit := do
  let invalidBindStmt ← db.prepare "INSERT INTO products (id, name) VALUES (?, ?);"
  match ← (invalidBindStmt.bindText 999 "test").toBaseIO with
  | Except.ok _ =>
      recordFailure "Should have failed but didn't"
  | Except.error e =>
      recordSuccess s!"Correctly rejected invalid index: {e}"

def testInvalidSQL (db : SQLite) : TestM Unit := do
  match ← (db.prepare "INVALID SQL SYNTAX;").toBaseIO with
  | Except.ok _ =>
      recordFailure "Should have failed but didn't"
  | Except.error e =>
      recordSuccess s!"Correctly failed to prepare: {e}"

def testOutOfRangeColumn (db : SQLite) : TestM Unit := do
  let rangeStmt ← db.prepare "SELECT id FROM products LIMIT 1;"
  let _ ← rangeStmt.step
  -- SQLite allows reading out-of-range columns but returns NULL/0
  let outOfRange ← rangeStmt.columnInt64 999
  recordSuccess s!"Out-of-range column returned: {outOfRange} (SQLite returns 0 for invalid columns)"

def testMultipleStatements (db : SQLite) : TestM Unit := do
  match ← (db.prepare "SELECT 1; SELECT 2;").toBaseIO with
  | Except.ok _ =>
      recordFailure "Should have failed but didn't"
  | Except.error e =>
      recordSuccess s!"Correctly rejected multiple statements: {e}"

def testStepAfterDone (db : SQLite) : TestM Unit := do
  let doneStmt ← db.prepare "SELECT 1;"
  let _ ← doneStmt.step
  let result2 ← doneStmt.step
  if result2 then
    recordFailure "Step returned true after done"
  else
    recordSuccess "Step correctly returned false when done"

def testTypeFlexibility (db : SQLite) : TestM Unit := do
  let typeStmt ← db.prepare "SELECT ?;"
  typeStmt.bindInt 1 42
  let _ ← typeStmt.step
  let text ← typeStmt.columnText 0
  recordSuccess s!"Integer converted to text: '{text}'"

def testNullInNonOptionTypes (db : SQLite) : TestM Unit := do
  let nullStmt ← db.prepare "SELECT NULL;"
  let _ ← nullStmt.step
  let nullAsInt ← nullStmt.columnInt64 0
  let nullAsDouble ← nullStmt.columnDouble 0
  recordSuccess s!"NULL as int64: {nullAsInt} (returns 0); double: {nullAsDouble} (returns 0.0)"

def testConstraintViolation (db : SQLite) : TestM Unit := do
  let constraintCreate ← db.prepare "CREATE TABLE IF NOT EXISTS unique_test (id INTEGER PRIMARY KEY, value TEXT);"
  let _ ← constraintCreate.step
  -- Clear any existing data
  let clearStmt ← db.prepare "DELETE FROM unique_test WHERE id = 1;"
  let _ ← clearStmt.step
  let insertOnce ← db.prepare "INSERT INTO unique_test (id, value) VALUES (1, 'first');"
  let _ ← insertOnce.step
  match ← (db.prepare "INSERT INTO unique_test (id, value) VALUES (1, 'duplicate');").toBaseIO with
  | Except.ok stmt =>
    match ← stmt.step.toBaseIO with
    | Except.ok _ =>
        recordFailure "Duplicate insert should have failed"
    | Except.error e =>
        recordSuccess s!"Correctly rejected duplicate: {e}"
  | Except.error e =>
      recordSuccess s!"Failed at prepare: {e}"

def testErrorCases (db : SQLite) : TestM Unit :=
  withHeader "=== Testing Error Cases ===" do
    testUnboundParameters db
    testInvalidParameterIndex db
    testInvalidSQL db
    testOutOfRangeColumn db
    testMultipleStatements db
    testStepAfterDone db
    testTypeFlexibility db
    testNullInNonOptionTypes db
    testConstraintViolation db

def testInterpolation (db : SQLite) : TestM Unit :=
  withHeader "=== Testing SQL Interpolation ===" do
    let treeCreate ← db sql!"CREATE TABLE IF NOT EXISTS trees (id INTEGER PRIMARY KEY, name TEXT, height INTEGER)"
    discard <| treeCreate.step
    let treeEmpty ← db sql!"DELETE FROM trees;"
    discard <| treeEmpty.step
    for (name, height) in [("pine", (20 : Int32)), ("poplar", 25)] do
      let treeInsert ← db sql!"INSERT INTO trees (id, name, height) VALUES (null, {name}, {height})"
      discard <| treeInsert.step
    let q1 ← db sql!"SELECT name FROM trees WHERE height < {(22 : Int32)}"
    repeat
      let hasRow ← q1.step
      unless hasRow do break
      let n ← q1.columnText 0
      if n ≠ "pine" then
        throw <| .userError s!"expected pine, got {n}"
      else
        recordSuccess s!"Interpolation works correctly, found: {n}"

def testResultIter (db : SQLite) : TestM Unit :=
  withHeader "=== Testing result iteration ===" do
    -- Create a nature-themed table with four columns
    let createStmt ← db.prepare "CREATE TABLE IF NOT EXISTS rivers (id INTEGER PRIMARY KEY, name TEXT, length_km REAL, country TEXT);"
    let _ ← createStmt.step
    recordSuccess "Created rivers table"

    -- Ensure the table is empty in case it already existed
    let clearStmt ← db.prepare "DELETE FROM rivers;"
    let _ ← clearStmt.step
    recordSuccess "Cleared existing river data"

    -- Insert some sample data
    let insertStmt ← db.prepare "INSERT INTO rivers (name, length_km, country) VALUES (?, ?, ?);"

    insertStmt.bindText 1 "Amazon"
    insertStmt.bindDouble 2 6400.0
    insertStmt.bindText 3 "Brazil"
    let _ ← insertStmt.step
    insertStmt.reset

    insertStmt.bindText 1 "Nile"
    insertStmt.bindDouble 2 6650.0
    insertStmt.bindText 3 "Egypt"
    let _ ← insertStmt.step
    insertStmt.reset

    insertStmt.bindText 1 "Mississippi"
    insertStmt.bindDouble 2 3766.0
    insertStmt.bindText 3 "United States"
    let _ ← insertStmt.step

    recordSuccess "Inserted river data"

    let q ← db sql!"SELECT name, length_km FROM rivers ORDER BY length_km"
    let all : Array (String × Nat) ← q.results.toArray
    let expected := #[("Mississippi", 3766), ("Amazon", 6400), ("Nile", 6650)]
    if all == expected then
      recordSuccess "Records matched"
    else
      recordFailure s!"Duplicate insert should have failed. Expected {repr expected}, got {repr all}"

def testTransactions (db : SQLite) : TestM Unit :=
  withHeader "=== Testing Transactions ===" do
    -- Create a test table
    let createStmt ← db.prepare "CREATE TABLE IF NOT EXISTS accounts (id INTEGER PRIMARY KEY, balance INTEGER);"
    let _ ← createStmt.step
    recordSuccess "Created accounts table"

    -- Clear any existing data
    let clearStmt ← db.prepare "DELETE FROM accounts;"
    let _ ← clearStmt.step

    -- Test basic transaction commit
    db.beginTransaction
    let insertStmt ← db.prepare "INSERT INTO accounts (id, balance) VALUES (?, ?);"
    insertStmt.bindInt 1 1
    insertStmt.bindInt 2 100
    let _ ← insertStmt.step
    db.commit
    recordSuccess "Transaction committed successfully"

    -- Verify data was committed
    let selectStmt ← db.prepare "SELECT balance FROM accounts WHERE id = 1;"
    let hasRow ← selectStmt.step
    if hasRow then
      let balance ← selectStmt.columnInt 0
      if balance == 100 then
        recordSuccess "Committed data verified"
      else
        recordFailure s!"Expected balance 100, got {balance}"
    else
      recordFailure "No row found after commit"

    -- Test transaction rollback
    db.beginTransaction
    let updateStmt ← db.prepare "UPDATE accounts SET balance = 200 WHERE id = 1;"
    let _ ← updateStmt.step
    db.rollback
    recordSuccess "Transaction rolled back successfully"

    -- Verify data was not changed
    let verifyStmt ← db.prepare "SELECT balance FROM accounts WHERE id = 1;"
    let hasRow2 ← verifyStmt.step
    if hasRow2 then
      let balance ← verifyStmt.columnInt 0
      if balance == 100 then
        recordSuccess "Rollback verified, data unchanged"
      else
        recordFailure s!"Expected balance 100 after rollback, got {balance}"
    else
      recordFailure "No row found after rollback"

    -- Test transaction helper with success
    match ← (db.transaction (do
      let insertStmt2 ← db.prepare "INSERT INTO accounts (id, balance) VALUES (?, ?);"
      insertStmt2.bindInt 1 2
      insertStmt2.bindInt 2 250
      let _ ← insertStmt2.step
      pure ())
    ).toBaseIO with
    | Except.ok _ =>
        recordSuccess "Transaction helper committed successfully"
    | Except.error e =>
        recordFailure s!"Transaction helper failed: {e}"

    -- Verify transaction helper committed
    let verifyStmt2 ← db.prepare "SELECT balance FROM accounts WHERE id = 2;"
    let hasRow3 ← verifyStmt2.step
    if hasRow3 then
      let balance ← verifyStmt2.columnInt 0
      if balance == 250 then
        recordSuccess "Transaction helper commit verified"
      else
        recordFailure s!"Expected balance 250, got {balance}"
    else
      recordFailure "No row found after transaction helper"

    -- Test transaction helper with error (rollback)
    match ← (db.transaction (do
      let insertStmt3 ← db.prepare "INSERT INTO accounts (id, balance) VALUES (?, ?);"
      insertStmt3.bindInt 1 3
      insertStmt3.bindInt 2 300
      let _ ← insertStmt3.step
      throw (IO.userError "Simulated error")
      pure ())
    ).toBaseIO with
    | Except.ok _ =>
        recordFailure "Transaction helper should have failed"
    | Except.error _ =>
        recordSuccess "Transaction helper correctly caught error"

    -- Verify transaction helper rolled back
    let verifyStmt3 ← db.prepare "SELECT COUNT(*) FROM accounts WHERE id = 3;"
    let _ ← verifyStmt3.step
    let count ← verifyStmt3.columnInt 0
    if count == 0 then
      recordSuccess "Transaction helper rollback verified"
    else
      recordFailure s!"Expected 0 rows for id=3, got {count}"

def testLastInsertRowId (db : SQLite) : TestM Unit :=
  withHeader "=== Testing Last Insert Row ID ===" do
    -- Create a table with INTEGER PRIMARY KEY (which is a rowid alias)
    let createStmt ← db.prepare "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT);"
    let _ ← createStmt.step
    recordSuccess "Created users table"

    -- Clear any existing data
    let clearStmt ← db.prepare "DELETE FROM users;"
    let _ ← clearStmt.step

    -- Insert first row with explicit NULL for id (auto-generates rowid)
    let insertStmt1 ← db.prepare "INSERT INTO users (id, name) VALUES (NULL, 'Alice');"
    let _ ← insertStmt1.step
    let rowid1 ← db.lastInsertRowId
    if rowid1 > 0 then
      recordSuccess s!"First insert returned rowid: {rowid1}"
    else
      recordFailure s!"Expected positive rowid, got {rowid1}"

    -- Insert second row - should get a different rowid
    let insertStmt2 ← db.prepare "INSERT INTO users (id, name) VALUES (NULL, 'Bob');"
    let _ ← insertStmt2.step
    let rowid2 ← db.lastInsertRowId
    if rowid2 > rowid1 then
      recordSuccess s!"Second insert returned rowid: {rowid2}"
    else
      recordFailure s!"Expected rowid > {rowid1}, got {rowid2}"

    -- Verify we can use the rowid to fetch the data
    let selectStmt ← db.prepare "SELECT name FROM users WHERE id = ?;"
    selectStmt.bindInt64 1 rowid2
    let hasRow ← selectStmt.step
    if hasRow then
      let name ← selectStmt.columnText 0
      if name == "Bob" then
        recordSuccess "Successfully queried using last insert rowid"
      else
        recordFailure s!"Expected 'Bob', got '{name}'"
    else
      recordFailure "No row found with last insert rowid"

    -- Test with explicit rowid
    let insertStmt3 ← db.prepare "INSERT INTO users (id, name) VALUES (?, ?);"
    insertStmt3.bindInt64 1 999
    insertStmt3.bindText 2 "Charlie"
    let _ ← insertStmt3.step
    let rowid3 ← db.lastInsertRowId
    if rowid3 == 999 then
      recordSuccess s!"Explicit rowid insert returned: {rowid3}"
    else
      recordFailure s!"Expected rowid 999, got {rowid3}"


def runTests (dbPath : System.FilePath) (verbose : Bool) (report : String → IO Unit := IO.println) : IO UInt32 := do
  -- Set up monad layers
  let config : Config := { verbose := verbose, report }
  let initialStats : Stats := { successes := 0, failures := 0 }
  let headerRef ← IO.mkRef (none : Option String)

  config.report "=== Testing SQLite Bindings ==="
  if verbose then config.report s!"Database: {dbPath}"

  -- Open database
  let db : SQLite ← SQLite.open dbPath
  if verbose then config.report "✓ Opened database"

  let (_result, finalStats) ← (((do
    testBasicInserts db
    testQueryResults db
    testBlobSupport db
    testErrorCases db
    testInterpolation db
    testResultIter db
    testTransactions db
    testLastInsertRowId db
  ).run config).run headerRef).run initialStats

  -- Print summary
  config.report ""
  config.report "=== Test Summary ==="
  config.report s!"Successes: {finalStats.successes}"
  config.report s!"Failures:  {finalStats.failures}"
  config.report s!"Total:     {finalStats.total}"

  if finalStats.failures == 0 then
    config.report "\n✓ All tests passed!"
    return 0
  else
    config.report s!"\n✗ {finalStats.failures} test(s) failed!"
    return 1

def main (args : List String) : IO UInt32 := do
  -- Parse arguments
  let hasVerbose := args.contains "--verbose"
  let dbPath := args.filter (fun arg => arg != "--verbose") |>.head?

  match dbPath with
  | some path =>
      -- Use the provided database path
      runTests (System.mkFilePath [path]) hasVerbose
  | none =>
      -- Create a temporary database file using Lean's API
      IO.FS.withTempFile fun _handle tempFile =>
        runTests tempFile hasVerbose

/-- info: true -/
#guard_msgs in
#eval do return (← runTests "foo.db" false (report := fun _ => pure ())) == 0
