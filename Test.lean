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

def testBasicInserts (db : SQLite) : TestM Unit :=
  withHeader "=== Testing Basic Inserts ===" do
    -- Create table with various data types
    expectSuccess "Created table" do
      let createStmt ← db.prepare "CREATE TABLE IF NOT EXISTS products (id INTEGER, name TEXT, price REAL, stock INTEGER, description TEXT);"
      let _ ← createStmt.step

    -- Test bindInt, bindText, bindDouble, bindInt64
    let insertStmt ← expectSuccess "Inserted product 1 (Laptop, bindInt/bindText/bindDouble/bindInt64)" do
      let insertStmt ← db.prepare "INSERT INTO products (id, name, price, stock, description) VALUES (?, ?, ?, ?, ?);"
      insertStmt.bindInt 1 101
      insertStmt.bindText 2 "Laptop"
      insertStmt.bindDouble 3 1299.99
      insertStmt.bindInt64 4 5000000000  -- Large number to test int64
      insertStmt.bindText 5 "High-performance laptop"
      discard insertStmt.step
      pure insertStmt


    -- Test bindNull
    expectSuccess  "Inserted product 2 (Mystery Item with NULL values, bindNull)" do
      insertStmt.reset
      insertStmt.bindInt 1 102
      insertStmt.bindText 2 "Mystery Item"
      insertStmt.bindNull 3  -- NULL price
      insertStmt.bindInt 4 0
      insertStmt.bindNull 5  -- NULL description
      discard insertStmt.step


    -- Test with another product
    expectSuccess "Inserted product 3 (Monitor)" do
      insertStmt.reset
      insertStmt.bindInt 1 103
      insertStmt.bindText 2 "Monitor"
      insertStmt.bindDouble 3 449.50
      insertStmt.bindInt64 4 2500000000
      insertStmt.bindText 5 "4K display"
      discard insertStmt.step


def testQueryResults (db : SQLite) : TestM Unit :=
  withHeader "=== Query Results (Testing columnInt64, columnText, columnDouble) ===" do
    let (selectStmt, selectColCount) ← expectSuccess' (fun (_, cols) => s!"Column count for SELECT: {cols}") do
      let selectStmt ← db.prepare "SELECT id, name, price, stock, description FROM products;"
      let selectColCount := selectStmt.columnCount

      pure (selectStmt, selectColCount)

    expect (selectColCount == 5) s!"Expected five columns, got {selectColCount}"

    let mut hasRow ← selectStmt.step
    let mut rowCount := 0
    while hasRow do
      rowCount := rowCount + 1
      let id ← selectStmt.columnInt64 0
      let name ← selectStmt.columnText 1
      let price ← selectStmt.columnDouble 2
      let stock ← selectStmt.columnInt64 3
      let description ← selectStmt.columnText 4

      verbose do
        report s!"  Product {id}: {name}"
        report s!"    Price: ${price}, Stock: {stock}"
        report s!"    Description: {description}"
      hasRow ← selectStmt.step

    expect (rowCount == 3) s!"Expected 3 rows, got {rowCount}"

def testBlobSupport (db : SQLite) : TestM Unit :=
  withHeader "=== Testing BLOB Support ===" do
    -- Create a table with BLOB column
    let blobCreate ← db.prepare "CREATE TABLE IF NOT EXISTS files (id INTEGER, name TEXT, data BLOB);"
    blobCreate.exec

    -- Insert BLOB data
    let blobInsert ← db.prepare "INSERT INTO files (id, name, data) VALUES (?, ?, ?);"
    expectSuccess "Inserted BLOB data" do
      let testData : ByteArray := ByteArray.mk #[72, 101, 108, 108, 111] -- "Hello"
      blobInsert.bindInt 1 1
      blobInsert.bindText 2 "test.txt"
      blobInsert.bindBlob 3 testData
      blobInsert.exec

    -- Insert NULL BLOB
    expectSuccess "Inserted NULL BLOB" do
      blobInsert.reset
      blobInsert.bindInt 1 2
      blobInsert.bindText 2 "empty.txt"
      blobInsert.bindNull 3
      blobInsert.exec

    -- Insert empty BLOB
    expectSuccess "Inserted empty BLOB" do
      blobInsert.reset
      blobInsert.bindInt 1 3
      blobInsert.bindText 2 "zero.txt"
      blobInsert.bindBlob 3 ByteArray.empty
      blobInsert.exec

    -- Query and check BLOBs
    let blobSelect ← db.prepare "SELECT id, name, data FROM files;"
    let mut hasRow ← blobSelect.step
    let mut blobSizes := #[]
    while hasRow do
      let id ← blobSelect.columnInt64 0
      let name ← blobSelect.columnText 1
      let blob ← blobSelect.columnBlob 2

      blobSizes := blobSizes.push (id, name, blob.size)

      verbose do
        report s!"  File {id}: {name}"
        report s!"    BLOB size: {blob.size} bytes"

      hasRow ← blobSelect.step
    let expected := #[(1, "test.txt", 5), (2, "empty.txt", 0), (3, "zero.txt", 0)]
    expect (blobSizes == expected) s!"Expected blob sizes {expected}, got {blobSizes}"

def testUnboundParameters (db : SQLite) : TestM Unit := do
  expectSuccess "Step with unbound parameters succeeded (SQLite treats them as NULL)" do
    let missingStmt ← db.prepare "INSERT INTO products (id, name, price) VALUES (?, ?, ?);"
    missingStmt.bindInt 1 999
    -- Intentionally skip binding parameters 2 and 3
    -- SQLite allows this - unbound parameters are NULL
    missingStmt.exec

  let getVals ← db.prepare "SELECT name, price FROM products WHERE id = 999;"
  let mut hasRow ← getVals.step
  let mut rowCount := 0
  while hasRow do
    expect (← getVals.columnNull 0) "Expected null name"
    expect (← getVals.columnNull 1) "Expected null price"
    rowCount := rowCount + 1
    hasRow ← getVals.step
  expect (rowCount = 1) "Only one row for unbound parameters"

def testInvalidParameterIndex (db : SQLite) : TestM Unit := do
  let invalidBindStmt ← db.prepare "INSERT INTO products (id, name) VALUES (?, ?);"
  expectFailure' (s!"Correctly rejected invalid index: {·}") do
    invalidBindStmt.bindText 999 "test"

def testInvalidSQL (db : SQLite) : TestM Unit := do
  expectFailure' (s!"Correctly failed to prepare: {·}") do
    discard <| db.prepare "INVALID SQL SYNTAX;"

def testOutOfRangeColumn (db : SQLite) : TestM Unit := do
  let outOfRange ← expectSuccess' (s!"Out-of-range column returned: {·} (SQLite returns 0 for invalid columns)") do
    let rangeStmt ← db.prepare "SELECT id FROM products LIMIT 1;"
    let _ ← rangeStmt.step
    -- SQLite allows reading out-of-range columns but returns NULL/0
    rangeStmt.columnInt64 999
  expect (outOfRange == 0) s!"Expected 0 for out-of-range column, got {outOfRange}"

def testMultipleStatements (db : SQLite) : TestM Unit := do
  expectFailure' (s!"Correctly rejected multiple statements: {·}") do
    discard <| db.prepare "SELECT 1; SELECT 2;"

def testStepAfterDone (db : SQLite) : TestM Unit := do
  let doneStmt ← db.prepare "SELECT 1;"
  -- Discard the first row
  doneStmt.exec
  -- Is there a second row?
  if (← doneStmt.step) then
    recordFailure "Step returned true after done"
  else
    recordSuccess "Step correctly returned false when done"

def testTypeFlexibility (db : SQLite) : TestM Unit := do
  let text ← expectSuccess' (s!"Integer converted to text: '{·}'") do
    let typeStmt ← db.prepare "SELECT ?;"
    typeStmt.bindInt 1 42
    typeStmt.exec
    typeStmt.columnText 0
  expect (text == "42") s!"Expected \"42\", got {text.quote}"

def testNullInNonOptionTypes (db : SQLite) : TestM Unit := do
  let nullStmt ← db.prepare "SELECT NULL;"
  nullStmt.exec
  let nullAsInt ← nullStmt.columnInt64 0
  expect (nullAsInt == 0) s!"Expected NULL as 0 at type INT, got {nullAsInt}"
  let nullAsDouble ← nullStmt.columnDouble 0
  expect (nullAsDouble == 0.0) s!"Expected NULL as 0.0 as a double, got {nullAsDouble}"
  recordSuccess s!"NULL as int64: {nullAsInt} (returns 0); double: {nullAsDouble} (returns 0.0)"

def testConstraintViolation (db : SQLite) : TestM Unit := do

  db.exec "CREATE TABLE IF NOT EXISTS unique_test (id INTEGER PRIMARY KEY, value TEXT);"
  db.exec "DELETE FROM unique_test WHERE id = 1;"
  db.exec "INSERT INTO unique_test (id, value) VALUES (1, 'first');"

  try
    let stmt ←
      try
        db.prepare "INSERT INTO unique_test (id, value) VALUES (1, 'duplicate');"
      catch
        | (e : IO.Error) =>
          recordFailure s!"Failed at prepare: {e}"
          return
    stmt.exec
    recordFailure "Duplicate insert should have failed"
  catch
    | (e : IO.Error) => recordSuccess s!"Correctly rejected duplicate: {e}"

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
    treeCreate.exec
    let treeEmpty ← db sql!"DELETE FROM trees;"
    treeEmpty.exec

    for (name, height) in [("pine", (20 : Int32)), ("poplar", 25)] do
      let treeInsert ← db sql!"INSERT INTO trees (id, name, height) VALUES (null, {name}, {height})"
      treeInsert.exec
    let q1 ← db sql!"SELECT name FROM trees WHERE height < {(22 : Int32)}"
    let mut hasRow ← q1.step
    while hasRow do
      let n ← q1.columnText 0
      if n ≠ "pine" then
        throw <| .userError s!"expected pine, got {n}"
      else
        recordSuccess s!"Interpolation works correctly, found: {n}"
      hasRow ← q1.step

def testResultIter (db : SQLite) : TestM Unit :=
  withHeader "=== Testing result iteration ===" do
    expectSuccess "Created rivers table and deleted old data" do
      db.exec "CREATE TABLE IF NOT EXISTS rivers (id INTEGER PRIMARY KEY, name TEXT, length_km REAL, country TEXT);"
      db.exec "DELETE FROM rivers;"

    -- Insert some sample data
    expectSuccess "Inserted river data" do
      let insertStmt ← db.prepare "INSERT INTO rivers (name, length_km, country) VALUES (?, ?, ?);"

      insertStmt.bindText 1 "Amazon"
      insertStmt.bindDouble 2 6400.0
      insertStmt.bindText 3 "Brazil"
      insertStmt.exec
      insertStmt.reset; insertStmt.clearBindings

      insertStmt.bindText 1 "Nile"
      insertStmt.bindDouble 2 6650.0
      insertStmt.bindText 3 "Egypt"
      insertStmt.exec
      insertStmt.reset; insertStmt.clearBindings

      insertStmt.bindText 1 "Mississippi"
      insertStmt.bindDouble 2 3766.0
      insertStmt.bindText 3 "United States"
      insertStmt.exec

    let q ← db sql!"SELECT name, length_km FROM rivers ORDER BY length_km"
    let all : Array (String × Nat) ← q.results.toArray
    let expected := #[("Mississippi", 3766), ("Amazon", 6400), ("Nile", 6650)]
    if all == expected then
      recordSuccess "Records matched"
    else
      recordFailure s!"Duplicate insert should have failed. Expected {repr expected}, got {repr all}"

    let q' ← db sql!"SELECT length_km FROM rivers"
    let mut out := ""
    for (l : String) in q'.results do
      out := out ++ l ++ "\n"
    let expected := "6400.0\n6650.0\n3766.0\n"
    expect (out == expected) s!"Expected {expected.quote} but got {out.quote}"

def testTransactions (db : SQLite) : TestM Unit :=
  withHeader "=== Testing Transactions ===" do
    -- Create a test table
    db.exec "CREATE TABLE IF NOT EXISTS accounts (id INTEGER PRIMARY KEY, balance INTEGER);"
    recordSuccess "Created accounts table"

    -- Clear any existing data
    db.exec "DELETE FROM accounts;"

    -- Test basic transaction commit
    db.beginTransaction
    let insertStmt ← db.prepare "INSERT INTO accounts (id, balance) VALUES (?, ?);"
    insertStmt.bindInt 1 1
    insertStmt.bindInt 2 100
    insertStmt.exec
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
    db.exec "UPDATE accounts SET balance = 200 WHERE id = 1;"
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
    try
      db.transaction do
        let insertStmt2 ← db.prepare "INSERT INTO accounts (id, balance) VALUES (?, ?);"
        insertStmt2.bindInt 1 2
        insertStmt2.bindInt 2 250
        insertStmt2.exec
      recordSuccess "Transaction helper committed successfully"
    catch
      | (e : IO.Error) => recordFailure s!"Transaction helper failed: {e}"

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
    try
      db.transaction do
        let insertStmt3 ← db.prepare "INSERT INTO accounts (id, balance) VALUES (?, ?);"
        insertStmt3.bindInt 1 3
        insertStmt3.bindInt 2 300
        insertStmt3.exec
        throw (IO.userError "Simulated error")
        pure ()
      recordFailure "Transaction helper should have failed"
    catch | (_ : IO.Error) => recordSuccess "Transaction helper correctly caught error"

    -- Verify transaction helper rolled back
    let verifyStmt3 ← db.prepare "SELECT COUNT(*) FROM accounts WHERE id = 3;"
    verifyStmt3.exec
    let count ← verifyStmt3.columnInt 0
    if count == 0 then
      recordSuccess "Transaction helper rollback verified"
    else
      recordFailure s!"Expected 0 rows for id=3, got {count}"

def testLastInsertRowId (db : SQLite) : TestM Unit :=
  withHeader "=== Testing Last Insert Row ID ===" do
    -- Create a table with INTEGER PRIMARY KEY (which is a rowid alias)
    db.exec "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT);"
    recordSuccess "Created users table"

    -- Clear any existing data
    db.exec "DELETE FROM users;"

    -- Insert first row with explicit NULL for id (auto-generates rowid)
    db.exec "INSERT INTO users (id, name) VALUES (NULL, 'Alice');"
    let rowid1 ← db.lastInsertRowId
    if rowid1 > 0 then
      recordSuccess s!"First insert returned rowid: {rowid1}"
    else
      recordFailure s!"Expected positive rowid, got {rowid1}"

    -- Insert second row - should get a different rowid
    db.exec "INSERT INTO users (id, name) VALUES (NULL, 'Bob');"
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
    insertStmt3.exec
    let rowid3 ← db.lastInsertRowId
    if rowid3 == 999 then
      recordSuccess s!"Explicit rowid insert returned: {rowid3}"
    else
      recordFailure s!"Expected rowid 999, got {rowid3}"

def testChanges (db : SQLite) : TestM Unit :=
  withHeader "=== Testing Changes Count ===" do
    -- Create and populate a test table
    db.exec "CREATE TABLE IF NOT EXISTS employees (id INTEGER PRIMARY KEY, name TEXT, salary INTEGER, active INTEGER);"
    recordSuccess "Created employees table"

    -- Clear any existing data
    db.exec "DELETE FROM employees;"

    -- Test INSERT - should affect 1 row
    let insertStmt ← db.prepare "INSERT INTO employees (name, salary, active) VALUES (?, ?, ?);"
    insertStmt.bindText 1 "Alice"
    insertStmt.bindInt 2 50000
    insertStmt.bindInt 3 1
    insertStmt.exec
    let changes1 ← db.changes
    if changes1 == 1 then
      recordSuccess s!"INSERT affected {changes1} row"
    else
      recordFailure s!"Expected 1 row affected, got {changes1}"

    -- Insert more rows for testing
    insertStmt.reset
    insertStmt.clearBindings
    insertStmt.bindText 1 "Bob"
    insertStmt.bindInt 2 60000
    insertStmt.bindInt 3 1
    insertStmt.exec

    insertStmt.reset
    insertStmt.clearBindings
    insertStmt.bindText 1 "Charlie"
    insertStmt.bindInt 2 55000
    insertStmt.bindInt 3 1
    insertStmt.exec

    insertStmt.reset
    insertStmt.clearBindings
    insertStmt.bindText 1 "Diana"
    insertStmt.bindInt 2 70000
    insertStmt.bindInt 3 0
    insertStmt.exec

    -- Test UPDATE matching multiple rows
    db.exec "UPDATE employees SET salary = salary + 5000 WHERE active = 1;"
    let changes2 ← db.changes
    if changes2 == 3 then
      recordSuccess s!"UPDATE affected {changes2} rows"
    else
      recordFailure s!"Expected 3 rows affected, got {changes2}"

    -- Test UPDATE matching single row
    db.exec "UPDATE employees SET name = 'Robert' WHERE name = 'Bob';"
    let changes3 ← db.changes
    if changes3 == 1 then
      recordSuccess s!"Single-row UPDATE affected {changes3} row"
    else
      recordFailure s!"Expected 1 row affected, got {changes3}"

    -- Test UPDATE matching no rows
    db.exec "UPDATE employees SET salary = 100000 WHERE name = 'NonExistent';"
    let changes4 ← db.changes
    if changes4 == 0 then
      recordSuccess s!"No-match UPDATE affected {changes4} rows"
    else
      recordFailure s!"Expected 0 rows affected, got {changes4}"

    -- Test DELETE matching rows
    db.exec "DELETE FROM employees WHERE active = 0;"
    let changes5 ← db.changes
    if changes5 == 1 then
      recordSuccess s!"DELETE affected {changes5} row"
    else
      recordFailure s!"Expected 1 row affected, got {changes5}"

    -- Test DELETE matching no rows
    db.exec "DELETE FROM employees WHERE salary > 1000000;"
    let changes6 ← db.changes
    if changes6 == 0 then
      recordSuccess s!"No-match DELETE affected {changes6} rows"
    else
      recordFailure s!"Expected 0 rows affected, got {changes6}"

    -- Test SELECT returns 0 (not a data modification statement)
    db.exec "SELECT * FROM employees;"
    let changes7 ← db.changes
    if changes7 == 0 then
      recordSuccess s!"SELECT returned changes count of {changes7}"
    else
      recordFailure s!"Expected 0 changes for SELECT, got {changes7}"

def testBusyTimeout (dbPath : System.FilePath) : TestM Unit :=
  withHeader "=== Testing Busy Timeout ===" do
    -- Test that setting busy timeout doesn't error
    let db1 ← SQLite.open dbPath
    expectSuccess "Set busy timeout to 5000ms" do
      db1.busyTimeout 5000

    -- Test setting it to 0 (immediate failure on lock)
    expectSuccess "Set busy timeout to 0ms (immediate failure)" do
      db1.busyTimeout 0


    -- Test with concurrent access scenario
    -- Open two connections to the same database
    let db2 ← SQLite.open dbPath
    db2.busyTimeout 100  -- Short timeout for testing

    -- Create a test table on db1
    db1.exec "CREATE TABLE IF NOT EXISTS timeout_test (id INTEGER PRIMARY KEY, value TEXT);"
    db1.exec "DELETE FROM timeout_test;"

    -- Start an exclusive transaction on db1 (locks the database)
    db1.beginTransaction .exclusive

    -- Insert data on db1 within the transaction
    let insertStmt1 ← db1.prepare "INSERT INTO timeout_test (value) VALUES (?);"
    insertStmt1.bindText 1 "locked"
    insertStmt1.exec

    -- Try to write from db2 - should fail due to lock even with timeout
    -- (because db1 has an exclusive lock)
    try
      let insertStmt2 ← db2.prepare "INSERT INTO timeout_test (value) VALUES (?);"
      insertStmt2.bindText 1 "blocked"
      insertStmt2.exec
      recordFailure "Should have failed due to database lock"
    catch
      | (_ : IO.Error) => recordSuccess "Correctly failed due to database lock"

    -- Commit the transaction on db1
    db1.commit
    recordSuccess "Committed transaction, releasing lock"

    -- Now db2 should be able to write
    try
      let insertStmt3 ← db2.prepare "INSERT INTO timeout_test (value) VALUES (?);"
      insertStmt3.bindText 1 "success"
      insertStmt3.exec
      recordSuccess "Successfully wrote after lock released"
    catch
      | (e : IO.Error) =>
        recordFailure s!"Should have succeeded after lock release: {e}"

def testColumnName (db : SQLite) : TestM Unit :=
  withHeader "=== Testing Column Names ===" do
    -- Create a simple test table
    db.exec "CREATE TABLE IF NOT EXISTS column_test (id INTEGER PRIMARY KEY, username TEXT, age INTEGER);"

    -- Prepare a SELECT statement to test column names
    let selectStmt ← db.prepare "SELECT id, age, username FROM column_test;"

    -- Get column names
    let name0 ← selectStmt.columnName 0
    let name1 ← selectStmt.columnName 1
    let name2 ← selectStmt.columnName 2

    -- Verify column names match expected values
    if name0 == "id" && name1 == "age" && name2 == "username" then
      recordSuccess s!"Column names correct: {name0}, {name1}, {name2}"
    else
      recordFailure s!"Expected 'id', 'age', 'username', got '{name0}', '{name1}', '{name2}'"

    -- Test with SELECT * to verify column order
    let selectAllStmt ← db.prepare "SELECT * FROM column_test;"
    let allName0 ← selectAllStmt.columnName 0
    let allName1 ← selectAllStmt.columnName 1
    let allName2 ← selectAllStmt.columnName 2

    if allName0 == "id" && allName1 == "username" && allName2 == "age" then
      recordSuccess "Column names correct with SELECT *"
    else
      recordFailure s!"SELECT * gave incorrect names: '{allName0}', '{allName1}', '{allName2}'"

    -- Test with column aliases
    let aliasStmt ← db.prepare "SELECT id AS user_id, username AS name FROM column_test;"
    let aliasName0 ← aliasStmt.columnName 0
    let aliasName1 ← aliasStmt.columnName 1

    if aliasName0 == "user_id" && aliasName1 == "name" then
      recordSuccess s!"Column aliases work: {aliasName0}, {aliasName1}"
    else
      recordFailure s!"Expected 'user_id', 'name', got '{aliasName0}', '{aliasName1}'"

def testClearBindings (db : SQLite) : TestM Unit :=
  withHeader "=== Testing Clear Bindings ===" do
    -- Create test table with 3 columns
    db.exec "CREATE TABLE IF NOT EXISTS bindings_test (id INTEGER PRIMARY KEY, value1 INTEGER, value2 TEXT, value3 INTEGER);"
    -- Clear any existing data
    db.exec "DELETE FROM bindings_test;"

    -- Test 1: Demonstrate that bindings persist after reset
    let insertStmt ← db.prepare "INSERT INTO bindings_test (value1, value2, value3) VALUES (?, ?, ?);"
    insertStmt.bindInt 1 100
    insertStmt.bindText 2 "first"
    insertStmt.bindInt 3 300
    insertStmt.exec

    -- Reset without clearing bindings
    insertStmt.reset
    -- Only rebind first two parameters - third one should persist
    insertStmt.bindInt 1 200
    insertStmt.bindText 2 "second"
    -- Don't bind parameter 3 - it should still be 300
    insertStmt.exec

    -- Verify the second row has value3=300 (persisted from first binding)
    let selectStmt1 ← db.prepare "SELECT value3 FROM bindings_test WHERE value1 = 200;"
    let hasRow1 ← selectStmt1.step
    if hasRow1 then
      let value3 ← selectStmt1.columnInt 0
      if value3 == 300 then
        recordSuccess "Bindings persist after reset (value3 = 300)"
      else
        recordFailure s!"Expected persisted value 300, got {value3}"
    else
      recordFailure "No row found for second insert"

    -- Test 2: Use clearBindings to reset all parameters
    insertStmt.reset
    insertStmt.clearBindings
    recordSuccess "Called clearBindings"

    -- Now bind only first two parameters
    insertStmt.bindInt 1 400
    insertStmt.bindText 2 "third"
    -- Don't bind parameter 3 - should be NULL now
    let _ ← insertStmt.step

    -- Verify the third row has NULL for value3
    let selectStmt2 ← db.prepare "SELECT value3 FROM bindings_test WHERE value1 = 400;"
    let hasRow2 ← selectStmt2.step
    if hasRow2 then
      let value3Type ← selectStmt2.columnType 0
      if value3Type.isNull then
        recordSuccess "After clearBindings, unbound parameter is NULL"
      else
        let value3 ← selectStmt2.columnInt 0
        recordFailure s!"Expected NULL, got {value3} with type {value3Type}"
    else
      recordFailure "No row found for third insert"

    -- Test 3: Verify clearBindings can be called multiple times safely
    insertStmt.reset
    insertStmt.clearBindings
    insertStmt.clearBindings  -- Call twice
    recordSuccess "clearBindings can be called multiple times"

    -- Bind and insert to verify statement still works
    insertStmt.bindInt 1 500
    insertStmt.bindText 2 "fourth"
    insertStmt.bindInt 3 600
    insertStmt.exec

    let selectStmt3 ← db.prepare "SELECT value1, value3 FROM bindings_test WHERE value2 = 'fourth';"
    let hasRow3 ← selectStmt3.step
    if hasRow3 then
      let v1 ← selectStmt3.columnInt 0
      let v3 ← selectStmt3.columnInt 1
      if v1 == 500 && v3 == 600 then
        recordSuccess "Statement works correctly after multiple clearBindings"
      else
        recordFailure s!"Expected 500 and 600, got {v1} and {v3}"
    else
      recordFailure "No row found after multiple clearBindings"


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
    testChanges db
    testBusyTimeout dbPath
    testClearBindings db
    testColumnName db
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
#eval IO.FS.withTempDir fun d => do return (← runTests (d / "foo.db") false (report := fun _ => pure ())) == 0
