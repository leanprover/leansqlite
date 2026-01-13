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

def testSqlIntrospection (db : SQLite) : TestM Unit :=
  withHeader "=== Testing SQL Introspection ===" do
    -- Test basic SQL text retrieval
    let selectStmt ← db.prepare "SELECT id, name FROM products WHERE id > ?;"
    let sqlText ← selectStmt.sql
    expect (sqlText == "SELECT id, name FROM products WHERE id > ?;")
      s!"Expected original SQL, got: {sqlText}"
    recordSuccess s!"Original SQL retrieved: {sqlText}"

    -- Test expanded SQL without bindings
    let expandedBefore ← selectStmt.expandedSql
    verbose do report s!"  Expanded SQL before binding: {expandedBefore}"
    recordSuccess "Expanded SQL retrieved before binding"

    -- Bind a parameter and check expanded SQL
    selectStmt.bindInt 1 100
    let expandedAfter ← selectStmt.expandedSql
    verbose do report s!"  Expanded SQL after binding: {expandedAfter}"
    expect (expandedAfter.contains "100") s!"Expected expanded SQL to contain '100', got: {expandedAfter}"
    recordSuccess "Expanded SQL shows bound parameter"

    -- Test with complex SQL
    let complexStmt ← db.prepare "INSERT INTO products (id, name, price) VALUES (?, ?, ?);"
    let complexSql ← complexStmt.sql
    expect (complexSql.contains "INSERT") s!"Expected SQL to contain INSERT"
    recordSuccess "Complex SQL text retrieved correctly"

def testParameterInfo (db : SQLite) : TestM Unit :=
  withHeader "=== Testing Parameter Information ===" do
    -- Test parameter count with positional parameters
    let stmt1 ← db.prepare "SELECT * FROM products WHERE id = ? AND name = ? AND price > ?;"
    let count1 ← stmt1.bindParameterCount
    expect (count1 == 3) s!"Expected 3 parameters, got {count1}"
    recordSuccess s!"Parameter count correct: {count1}"

    -- Test with no parameters
    let stmt2 ← db.prepare "SELECT * FROM products;"
    let count2 ← stmt2.bindParameterCount
    expect (count2 == 0) s!"Expected 0 parameters, got {count2}"
    recordSuccess "Zero parameters detected correctly"

    -- Test named parameters with :name syntax
    let stmt3 ← db.prepare "SELECT * FROM products WHERE id = :product_id AND name = :product_name;"
    let count3 ← stmt3.bindParameterCount
    expect (count3 == 2) s!"Expected 2 named parameters, got {count3}"
    recordSuccess "Named parameters counted correctly"

    -- Test bindParameterIndex
    let idx1 := stmt3.bindParameterIndex ":product_id"
    expect (idx1 == 1) s!"Expected index 1 for :product_id, got {idx1}"
    recordSuccess s!"Parameter :product_id at index {idx1}"

    let idx2 := stmt3.bindParameterIndex ":product_name"
    expect (idx2 == 2) s!"Expected index 2 for :product_name, got {idx2}"
    recordSuccess s!"Parameter :product_name at index {idx2}"

    -- Test with non-existent parameter name
    let idxNone := stmt3.bindParameterIndex ":nonexistent"
    expect (idxNone == 0) s!"Expected index 0 for nonexistent parameter, got {idxNone}"
    recordSuccess "Nonexistent parameter returns index 0"

    -- Test bindParameterName
    let name1 ← stmt3.bindParameterName 1
    expect (name1 == ":product_id") s!"Expected ':product_id', got '{name1}'"
    recordSuccess s!"Parameter name at index 1: {name1}"

    let name2 ← stmt3.bindParameterName 2
    expect (name2 == ":product_name") s!"Expected ':product_name', got '{name2}'"
    recordSuccess s!"Parameter name at index 2: {name2}"

def testParameterVariants (db : SQLite) : TestM Unit :=
  withHeader "=== Testing Parameter Syntax Variants ===" do
    -- Test $name syntax
    let stmt1 ← db.prepare "SELECT * FROM products WHERE id = $id;"
    let count1 ← stmt1.bindParameterCount
    expect (count1 == 1) s!"Expected 1 parameter with $id, got {count1}"
    let name1 ← stmt1.bindParameterName 1
    expect (name1 == "$id") s!"Expected '$id', got '{name1}'"
    let idx1 := stmt1.bindParameterIndex "$id"
    expect (idx1 == 1) s!"Expected index 1 for $id, got {idx1}"
    recordSuccess "$ parameter syntax works correctly"

    -- Test @name syntax
    let stmt2 ← db.prepare "SELECT * FROM products WHERE name = @name;"
    let count2 ← stmt2.bindParameterCount
    expect (count2 == 1) s!"Expected 1 parameter with @name, got {count2}"
    let name2 ← stmt2.bindParameterName 1
    expect (name2 == "@name") s!"Expected '@name', got '{name2}'"
    let idx2 := stmt2.bindParameterIndex "@name"
    expect (idx2 == 1) s!"Expected index 1 for @name, got {idx2}"
    recordSuccess "@ parameter syntax works correctly"

    -- Test mixed parameter types
    let stmt3 ← db.prepare "SELECT * FROM products WHERE id = ? AND name = :name AND price > $price;"
    let count3 ← stmt3.bindParameterCount
    expect (count3 == 3) s!"Expected 3 mixed parameters, got {count3}"

    let name_idx := stmt3.bindParameterIndex ":name"
    expect (name_idx == 2) s!"Expected index 2 for :name in mixed params, got {name_idx}"

    let price_idx := stmt3.bindParameterIndex "$price"
    expect (price_idx == 3) s!"Expected index 3 for $price in mixed params, got {price_idx}"

    recordSuccess "Mixed parameter syntaxes work correctly"

    -- Test unnamed parameters (?)
    let stmt4 ← db.prepare "INSERT INTO products (id, name) VALUES (?, ?);"
    let unnamed1 ← stmt4.bindParameterName 1
    expect (unnamed1 == "") s!"Expected empty string for unnamed param, got '{unnamed1}'"
    let unnamed2 ← stmt4.bindParameterName 2
    expect (unnamed2 == "") s!"Expected empty string for unnamed param, got '{unnamed2}'"
    recordSuccess "Unnamed parameters return empty string"

def testAutocommit (db : SQLite) : TestM Unit :=
  withHeader "=== Testing Autocommit and Transaction State ===" do
    -- Initially should be in autocommit mode (no transaction)
    let autocommit1 ← db.inAutocommit
    expect autocommit1 s!"Expected to be in autocommit mode initially"
    recordSuccess "Initially in autocommit mode"

    let inTxn1 ← db.inTransaction
    expect (!inTxn1) s!"Expected NOT to be in transaction initially"
    recordSuccess "Not in transaction initially"

    -- Begin a transaction - should disable autocommit
    db.beginTransaction
    let autocommit2 ← db.inAutocommit
    expect (!autocommit2) s!"Expected autocommit to be off after BEGIN"
    recordSuccess "Autocommit disabled after BEGIN TRANSACTION"

    let inTxn2 ← db.inTransaction
    expect inTxn2 s!"Expected to be in transaction after BEGIN"
    recordSuccess "In transaction after BEGIN"

    -- Make a change in the transaction
    db.exec "CREATE TABLE IF NOT EXISTS autocommit_test (id INTEGER, value TEXT);"
    db.exec "DELETE FROM autocommit_test;"
    db.exec "INSERT INTO autocommit_test (id, value) VALUES (1, 'test');"

    -- Still in transaction
    let inTxn3 ← db.inTransaction
    expect inTxn3 s!"Expected to still be in transaction"
    recordSuccess "Still in transaction after INSERT"

    -- Commit - should re-enable autocommit
    db.commit
    let autocommit3 ← db.inAutocommit
    expect autocommit3 s!"Expected autocommit to be on after COMMIT"
    recordSuccess "Autocommit re-enabled after COMMIT"

    let inTxn4 ← db.inTransaction
    expect (!inTxn4) s!"Expected NOT to be in transaction after COMMIT"
    recordSuccess "Not in transaction after COMMIT"

    -- Test with rollback
    db.beginTransaction
    let inTxn5 ← db.inTransaction
    expect inTxn5 s!"Expected to be in transaction after second BEGIN"

    db.exec "INSERT INTO autocommit_test (id, value) VALUES (2, 'rollback');"

    db.rollback
    let autocommit4 ← db.inAutocommit
    expect autocommit4 s!"Expected autocommit to be on after ROLLBACK"
    recordSuccess "Autocommit re-enabled after ROLLBACK"

    let inTxn6 ← db.inTransaction
    expect (!inTxn6) s!"Expected NOT to be in transaction after ROLLBACK"
    recordSuccess "Not in transaction after ROLLBACK"

    -- Test with transaction helper
    let beforeHelper ← db.inAutocommit
    expect beforeHelper s!"Expected autocommit before transaction helper"

    -- Can't check state during transaction since db.transaction uses IO, not TestM
    -- But we can insert data and verify it committed
    db.transaction do
      db.exec "INSERT INTO autocommit_test (id, value) VALUES (3, 'helper');"

    let afterHelper ← db.inAutocommit
    expect afterHelper s!"Expected autocommit after transaction helper"

    -- Verify the data was committed
    let verifyStmt ← db.prepare "SELECT COUNT(*) FROM autocommit_test WHERE id = 3;"
    verifyStmt.exec
    let count ← verifyStmt.columnInt 0
    expect (count == 1) s!"Expected 1 row with id=3, got {count}"

    recordSuccess "Autocommit state correct with transaction helper"

def testDbFilename (dbPath : System.FilePath) : TestM Unit :=
  withHeader "=== Testing Database Filename ===" do
    -- Open a file-based database
    let db ← SQLite.open dbPath

    -- Test getting the main database filename
    if let some mainFilename ← db.dbFilename "main" then
      -- The filename should match our opened database path. Because /var is a symlink to /private/var
      -- on MacOS, and that's where temp files as used for this test are saved, we compare only the
      -- filename part here.
      expect (mainFilename.fileName == dbPath.fileName)
        s!"Expected main db filename to be '{dbPath}', got '{mainFilename}'"
      recordSuccess s!"Main database filename: {mainFilename}"
    else
      recordFailure "Expected main database filename to be available"

    -- Test with default parameter (should default to "main")
    if let some defaultFilename ← db.dbFilename then
      expect (defaultFilename.fileName == dbPath.fileName)
        s!"Expected default filename to match main, got '{defaultFilename}'"
      recordSuccess "Default dbName parameter works correctly"
    else
      recordFailure "Expected default database filename to be available"

    -- Test with non-existent database name
    let nonExistent ← db.dbFilename "nonexistent"
    expect nonExistent.isNone
      s!"Expected none for nonexistent database, got '{nonExistent}'"
    recordSuccess "Non-existent database name returns none"

    -- Test with in-memory database
    let memDb ← SQLite.open ":memory:"
    let memFilename ← memDb.dbFilename "main"
    -- In-memory databases don't have an associated file, so this returns none
    expect (memFilename == none)
      s!"Expected none for in-memory database, got '{memFilename}'"
    recordSuccess "In-memory database returns none"

def testTotalChanges (db : SQLite) : TestM Unit :=
  withHeader "=== Testing Total Changes ===" do
    -- Get initial total changes
    let initialTotal ← db.totalChanges
    verbose do report s!"  Initial total changes: {initialTotal}"

    -- Make some changes
    db.exec "CREATE TABLE IF NOT EXISTS total_test (id INTEGER, value TEXT);"
    db.exec "DELETE FROM total_test;"

    db.exec "INSERT INTO total_test (id, value) VALUES (1, 'a');"
    db.exec "INSERT INTO total_test (id, value) VALUES (2, 'b');"
    db.exec "INSERT INTO total_test (id, value) VALUES (3, 'c');"

    let afterInserts ← db.totalChanges
    let insertDiff := afterInserts - initialTotal
    expect (insertDiff == 3) s!"Expected 3 inserts, got {insertDiff}"
    recordSuccess s!"Total changes after 3 inserts: {afterInserts} (delta: {insertDiff})"

    -- Update some rows
    db.exec "UPDATE total_test SET value = 'x' WHERE id <= 2;"
    let afterUpdates ← db.totalChanges
    let updateDiff := afterUpdates - afterInserts
    expect (updateDiff == 2) s!"Expected 2 updates, got {updateDiff}"
    recordSuccess s!"Total changes after 2 updates: {afterUpdates} (delta: {updateDiff})"

    -- Delete a row
    db.exec "DELETE FROM total_test WHERE id = 1;"
    let afterDelete ← db.totalChanges
    let deleteDiff := afterDelete - afterUpdates
    expect (deleteDiff == 1) s!"Expected 1 delete, got {deleteDiff}"
    recordSuccess s!"Total changes after 1 delete: {afterDelete} (delta: {deleteDiff})"

    -- Verify totalChanges accumulates (unlike changes)
    let finalTotal ← db.totalChanges
    let totalDiff := finalTotal - initialTotal
    expect (totalDiff == 6) s!"Expected total of 6 changes (3+2+1), got {totalDiff}"
    recordSuccess s!"Cumulative total changes verified: {totalDiff}"

def testStmtReadonly (db : SQLite) : TestM Unit :=
  withHeader "=== Testing Statement Readonly ===" do
    -- Test SELECT (read-only)
    let selectStmt ← db.prepare "SELECT * FROM products;"
    let selectReadonly ← selectStmt.isReadonly
    expect selectReadonly s!"Expected SELECT to be readonly"
    recordSuccess "SELECT statement is read-only"

    -- Test INSERT (not read-only)
    let insertStmt ← db.prepare "INSERT INTO products (id, name) VALUES (?, ?);"
    let insertReadonly ← insertStmt.isReadonly
    expect (!insertReadonly) s!"Expected INSERT to NOT be readonly"
    recordSuccess "INSERT statement is not read-only"

    -- Test UPDATE (not read-only)
    let updateStmt ← db.prepare "UPDATE products SET name = ? WHERE id = ?;"
    let updateReadonly ← updateStmt.isReadonly
    expect (!updateReadonly) s!"Expected UPDATE to NOT be readonly"
    recordSuccess "UPDATE statement is not read-only"

    -- Test DELETE (not read-only)
    let deleteStmt ← db.prepare "DELETE FROM products WHERE id = ?;"
    let deleteReadonly ← deleteStmt.isReadonly
    expect (!deleteReadonly) s!"Expected DELETE to NOT be readonly"
    recordSuccess "DELETE statement is not read-only"

    -- Test BEGIN (read-only per SQLite definition)
    let beginStmt ← db.prepare "BEGIN TRANSACTION;"
    let beginReadonly ← beginStmt.isReadonly
    expect beginReadonly s!"Expected BEGIN to be readonly"
    recordSuccess "BEGIN TRANSACTION is considered read-only"

    -- Test CREATE (not read-only)
    let createStmt ← db.prepare "CREATE TABLE IF NOT EXISTS readonly_test (id INTEGER);"
    let createReadonly ← createStmt.isReadonly
    expect (!createReadonly) s!"Expected CREATE to NOT be readonly"
    recordSuccess "CREATE TABLE statement is not read-only"

def testStmtBusy (db : SQLite) : TestM Unit :=
  withHeader "=== Testing Statement Busy ===" do
    let stmt ← db.prepare "SELECT id, name FROM products LIMIT 2;"

    -- Before step - should not be busy
    let busyBefore ← stmt.isBusy
    expect (!busyBefore) s!"Expected statement to NOT be busy before step"
    recordSuccess "Statement not busy before first step"

    -- After first step - should be busy
    let hasRow1 ← stmt.step
    expect hasRow1 s!"Expected first row"
    let busyAfterStep ← stmt.isBusy
    expect busyAfterStep s!"Expected statement to be busy after step"
    recordSuccess "Statement busy after first step"

    -- After second step - still busy
    stmt.exec
    let busyAfterStep2 ← stmt.isBusy
    expect busyAfterStep2 s!"Expected statement to still be busy"
    recordSuccess "Statement still busy after second step"

    -- Step one more time to get DONE status (LIMIT 2, so third step returns false)
    let hasRow3 ← stmt.step
    expect (!hasRow3) s!"Expected no more rows (LIMIT 2)"

    -- Statement should NOT be busy after reaching DONE
    -- sqlite3_stmt_busy returns false once SQLITE_DONE is reached
    let busyAfterDone ← stmt.isBusy
    expect (!busyAfterDone) s!"Expected statement to NOT be busy after done"
    recordSuccess "Statement not busy after stepping to DONE"

    -- After reset - should not be busy
    stmt.reset
    let busyAfterReset ← stmt.isBusy
    expect (!busyAfterReset) s!"Expected statement to NOT be busy after reset"
    recordSuccess "Statement not busy after reset"

def testDataCount (db : SQLite) : TestM Unit :=
  withHeader "=== Testing Data Count ===" do
    let stmt ← db.prepare "SELECT id, name, price FROM products LIMIT 1;"

    -- Before step - data count should be 0
    let countBefore ← stmt.dataCount
    expect (countBefore == 0) s!"Expected data count 0 before step, got {countBefore}"
    recordSuccess "Data count is 0 before first step"

    -- columnCount should still return the total columns
    let colCount := stmt.columnCount
    expect (colCount == 3) s!"Expected column count 3, got {colCount}"
    recordSuccess s!"Column count is {colCount} (always available)"

    -- After step with row - data count should match column count
    let hasRow ← stmt.step
    if hasRow then
      let countAfterStep ← stmt.dataCount
      expect (countAfterStep == 3) s!"Expected data count 3 after step, got {countAfterStep}"
      recordSuccess "Data count matches column count when row available"

    -- After stepping through all rows - data count should be 0 again
    let mut hasMore ← stmt.step
    while hasMore do
      hasMore ← stmt.step

    let countAfterDone ← stmt.dataCount
    expect (countAfterDone == 0) s!"Expected data count 0 after done, got {countAfterDone}"
    recordSuccess "Data count is 0 after stepping through all rows"

    -- Column count should still be available
    let colCountAfter := stmt.columnCount
    expect (colCountAfter == 3) s!"Expected column count still 3, got {colCountAfter}"
    recordSuccess "Column count remains available even when no current row"

def testDbReadonly (dbPath : System.FilePath) : TestM Unit :=
  withHeader "=== Testing Database Readonly ===" do
    -- Open a read-write database
    let db ← SQLite.open dbPath

    -- Check main database is read-write
    let mainMode ← db.dbReadonly "main"
    expect (mainMode.isEqSome .readWrite)
      s!"Expected main database to be read-write, got {repr mainMode}"
    recordSuccess "Main database is read-write"

    -- Test default parameter
    let defaultMode ← db.dbReadonly
    expect (defaultMode.isEqSome .readWrite)
      s!"Expected default (main) to be read-write"
    recordSuccess "Default dbName parameter works correctly"

    -- Test non-existent database
    let nonExistent ← db.dbReadonly "nonexistent"
    expect nonExistent.isNone
      s!"Expected nonexistent database mode, got {repr nonExistent}"
    recordSuccess "Non-existent database returns notFound"

def testOpenWith (dbPath : System.FilePath) : TestM Unit :=
  withHeader "=== Testing sqlite3_open_v2 (openWith) ===" do
    -- First create a database with some data
    do
      let db ← SQLite.open dbPath
      db.exec "CREATE TABLE IF NOT EXISTS open_test (id INTEGER, value TEXT);"
      db.exec "INSERT INTO open_test VALUES (1, 'test');"
      recordSuccess "Created test database with data"

    -- Test opening in read-only mode
    let readOnlyDb ← SQLite.openWith dbPath { mode := .readonly }
    let mode ← readOnlyDb.dbReadonly
    expect (mode.isEqSome .readOnly)
      s!"Expected read-only mode, got {repr mode}"
    recordSuccess "Opened database in read-only mode"

    -- Verify we can read from read-only database
    let stmt ← readOnlyDb.prepare "SELECT id, value FROM open_test;"
    let hasRow ← stmt.step
    expect hasRow "Expected to read row from read-only database"
    let id ← stmt.columnInt 0
    let value ← stmt.columnText 1
    expect (id == 1) s!"Expected id 1, got {id}"
    expect (value == "test") s!"Expected value 'test', got '{value}'"
    recordSuccess "Successfully read from read-only database"

    -- Verify we cannot write to read-only database
    try
      readOnlyDb.exec "INSERT INTO open_test VALUES (2, 'fail');"
      recordFailure "Expected error when writing to read-only database"
    catch e =>
      recordSuccess s!"Read-only database correctly prevented write: {e}"

    -- Test opening with readwrite + create flags
    IO.FS.withTempFile fun _handle tempFile => do
      let rwDb ← SQLite.openWith tempFile .readWriteCreate
      let rwMode ← rwDb.dbReadonly
      expect (rwMode.isEqSome .readWrite)
        s!"Expected read-write mode, got {repr rwMode}"
      recordSuccess "Opened new database with readwrite + create flags"

      -- Verify we can write
      rwDb.exec "CREATE TABLE test (x INTEGER);"
      rwDb.exec "INSERT INTO test VALUES (42);"
      let verifyStmt ← rwDb.prepare "SELECT x FROM test;"
      let hasVerifyRow ← verifyStmt.step
      expect hasVerifyRow "Expected to read back written data"
      let x ← verifyStmt.columnInt 0
      expect (x == 42) s!"Expected 42, got {x}"
      recordSuccess "Successfully wrote to database opened with readwrite + create"

    -- Test that readonly fails if database doesn't exist
    let nonExistentPath := dbPath.parent.getD "." / "nonexistent-test-db-readonly.db"
    -- Make sure it doesn't exist
    try
      IO.FS.removeFile nonExistentPath
    catch _ => pure ()

    try
      let _db ← SQLite.openWith nonExistentPath { mode := .readonly }
      recordFailure "Expected error when opening non-existent database in read-only mode"
    catch _e =>
      recordSuccess "Read-only open correctly failed for non-existent database"

    -- Test that readwrite without create fails if database doesn't exist
    let nonExistentPath2 := dbPath.parent.getD "." / "nonexistent-test-db-readwrite.db"
    try
      IO.FS.removeFile nonExistentPath2
    catch _ => pure ()

    try
      let _db ← SQLite.openWith nonExistentPath2 { mode := .readWrite }
      recordFailure "Expected error when opening non-existent database with readwrite (no create)"
    catch _e =>
      recordSuccess "Read-write without create correctly failed for non-existent database"

def testColumnMetadata (db : SQLite) : TestM Unit :=
  withHeader "=== Testing Column Metadata ===" do
    -- Create a test table
    db.exec "CREATE TABLE IF NOT EXISTS metadata_test (user_id INTEGER, user_name TEXT, user_email TEXT);"

    -- Test with simple SELECT
    let stmt1 ← db.prepare "SELECT user_id, user_name, user_email FROM metadata_test;"

    let tableName0 ← stmt1.columnTableName 0
    let tableName1 ← stmt1.columnTableName 1
    let tableName2 ← stmt1.columnTableName 2

    -- Note: These return empty if SQLITE_ENABLE_COLUMN_METADATA is not enabled
    if tableName0.isEmpty then
      recordSuccess "Column metadata not available (SQLITE_ENABLE_COLUMN_METADATA not enabled)"
    else
      expect (tableName0 == "metadata_test") s!"Expected table 'metadata_test', got '{tableName0}'"
      expect (tableName1 == "metadata_test") s!"Expected table 'metadata_test', got '{tableName1}'"
      expect (tableName2 == "metadata_test") s!"Expected table 'metadata_test', got '{tableName2}'"
      recordSuccess s!"Column table names: {tableName0}, {tableName1}, {tableName2}"

    -- Test origin names
    let originName0 ← stmt1.columnOriginName 0
    let originName1 ← stmt1.columnOriginName 1
    let originName2 ← stmt1.columnOriginName 2

    if originName0.isEmpty then
      recordSuccess "Column origin names not available (SQLITE_ENABLE_COLUMN_METADATA not enabled)"
    else
      expect (originName0 == "user_id") s!"Expected origin 'user_id', got '{originName0}'"
      expect (originName1 == "user_name") s!"Expected origin 'user_name', got '{originName1}'"
      expect (originName2 == "user_email") s!"Expected origin 'user_email', got '{originName2}'"
      recordSuccess s!"Column origin names: {originName0}, {originName1}, {originName2}"

    -- Test database names
    let dbName0 ← stmt1.columnDatabaseName 0
    let dbName1 ← stmt1.columnDatabaseName 1
    let dbName2 ← stmt1.columnDatabaseName 2

    if dbName0.isEmpty then
      recordSuccess "Column database names not available (SQLITE_ENABLE_COLUMN_METADATA not enabled)"
    else
      expect (dbName0 == "main") s!"Expected database 'main', got '{dbName0}'"
      expect (dbName1 == "main") s!"Expected database 'main', got '{dbName1}'"
      expect (dbName2 == "main") s!"Expected database 'main', got '{dbName2}'"
      recordSuccess s!"Column database names: {dbName0}, {dbName1}, {dbName2}"

    -- Test with aliases
    let stmt2 ← db.prepare "SELECT user_id AS id, user_name AS name FROM metadata_test;"

    -- columnName should return the alias
    let alias0 ← stmt2.columnName 0
    let alias1 ← stmt2.columnName 1
    expect (alias0 == "id") s!"Expected alias 'id', got '{alias0}'"
    expect (alias1 == "name") s!"Expected alias 'name', got '{alias1}'"
    recordSuccess s!"Column aliases: {alias0}, {alias1}"

    -- columnOriginName should return the original name (if metadata enabled)
    let origin0 ← stmt2.columnOriginName 0
    let origin1 ← stmt2.columnOriginName 1

    if !origin0.isEmpty then
      expect (origin0 == "user_id") s!"Expected original 'user_id', got '{origin0}'"
      expect (origin1 == "user_name") s!"Expected original 'user_name', got '{origin1}'"
      recordSuccess "Origin names differ from aliases (metadata enabled)"

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
    testSqlIntrospection db
    testParameterInfo db
    testParameterVariants db
    testAutocommit db
    testDbFilename dbPath
    testTotalChanges db
    testStmtReadonly db
    testStmtBusy db
    testDataCount db
    testDbReadonly dbPath
    testOpenWith dbPath
    testColumnMetadata db
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

-- Check that this also works in the interpreter in meta code
/-- info: true -/
#guard_msgs in
#eval IO.FS.withTempDir fun d => do return (← runTests (d / "foo.db") false (report := fun _ => pure ())) == 0
