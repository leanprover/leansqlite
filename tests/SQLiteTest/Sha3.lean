/-
Copyright (c) 2026 Lean FRO LLC. All rights reserved.
Released under Apache 2.0 license as described in the file LICENSE.
Author: David Thrane Christiansen
-/

import SQLite
import SQLiteTest.Framework

namespace SQLite.Test.Sha3

open SQLite SQLite.Test

def testEnableSha3 (db : SQLite) : TestM Unit :=
  withHeader "=== Testing SHA3 Extension ===" do
    -- Test enabling SHA3 extension
    expectSuccess "Enabled SHA3 extension" do
      db.enableSha3

    -- Test calling enableSha3 twice (should be safe)
    expectSuccess "Enabled SHA3 extension again (idempotent)" do
      db.enableSha3

def testSha3Function (db : SQLite) : TestM Unit :=
  withHeader "=== Testing sha3() Function ===" do
    -- Test sha3 with text input
    let hash1 ← expectSuccess' (fun h => s!"sha3('hello') = {h.size} bytes") do
      let stmt ← db.prepare "SELECT sha3('hello');"
      discard stmt.step
      stmt.columnBlob 0
    expect (hash1.size == 32) s!"Expected 32-byte hash (SHA3-256), got {hash1.size}"

    -- Test sha3 with different sizes
    let hash224 ← expectSuccess' (fun h => s!"sha3('hello', 224) = {h.size} bytes") do
      let stmt ← db.prepare "SELECT sha3('hello', 224);"
      discard stmt.step
      stmt.columnBlob 0
    expect (hash224.size == 28) s!"Expected 28-byte hash (SHA3-224), got {hash224.size}"

    let hash384 ← expectSuccess' (fun h => s!"sha3('hello', 384) = {h.size} bytes") do
      let stmt ← db.prepare "SELECT sha3('hello', 384);"
      discard stmt.step
      stmt.columnBlob 0
    expect (hash384.size == 48) s!"Expected 48-byte hash (SHA3-384), got {hash384.size}"

    let hash512 ← expectSuccess' (fun h => s!"sha3('hello', 512) = {h.size} bytes") do
      let stmt ← db.prepare "SELECT sha3('hello', 512);"
      discard stmt.step
      stmt.columnBlob 0
    expect (hash512.size == 64) s!"Expected 64-byte hash (SHA3-512), got {hash512.size}"

    -- Test sha3 with NULL returns NULL
    expectSuccess "sha3(NULL) returns NULL" do
      let stmt ← db.prepare "SELECT sha3(NULL);"
      discard stmt.step
      let isNull ← stmt.columnNull 0
      if !isNull then throw <| IO.userError "Expected NULL result"

    -- Test sha3 with blob input
    let hashBlob ← expectSuccess' (fun h => s!"sha3(blob) = {h.size} bytes") do
      let stmt ← db.prepare "SELECT sha3(X'48454C4C4F');"  -- "HELLO" as hex
      discard stmt.step
      stmt.columnBlob 0
    expect (hashBlob.size == 32) s!"Expected 32-byte hash, got {hashBlob.size}"

def testSha3Query (db : SQLite) : TestM Unit :=
  withHeader "=== Testing sha3_query() Function ===" do
    -- Create a test table
    db.exec "CREATE TABLE IF NOT EXISTS sha3_test (id INTEGER PRIMARY KEY, name TEXT, value INTEGER);"
    db.exec "DELETE FROM sha3_test;"
    db.exec "INSERT INTO sha3_test (name, value) VALUES ('alice', 100);"
    db.exec "INSERT INTO sha3_test (name, value) VALUES ('bob', 200);"
    db.exec "INSERT INTO sha3_test (name, value) VALUES ('charlie', 300);"

    -- Test sha3_query produces a hash
    let queryHash ← expectSuccess' (fun h => s!"sha3_query() = {h.size} bytes") do
      let stmt ← db.prepare "SELECT sha3_query('SELECT * FROM sha3_test ORDER BY id');"
      discard stmt.step
      stmt.columnBlob 0
    expect (queryHash.size == 32) s!"Expected 32-byte hash, got {queryHash.size}"

    -- Test that same query produces same hash (deterministic)
    let queryHash2 ← expectSuccess' (fun _ => "sha3_query is deterministic") do
      let stmt ← db.prepare "SELECT sha3_query('SELECT * FROM sha3_test ORDER BY id');"
      discard stmt.step
      stmt.columnBlob 0
    expect (queryHash == queryHash2) "Expected identical hashes for same query"

    -- Test that different ORDER BY produces different hash
    let queryHashDesc ← expectSuccess' (fun _ => "Different ORDER BY produces different hash") do
      let stmt ← db.prepare "SELECT sha3_query('SELECT * FROM sha3_test ORDER BY id DESC');"
      discard stmt.step
      stmt.columnBlob 0
    expect (queryHash != queryHashDesc) "Expected different hashes for different ordering"

    -- Test sha3_query with different hash sizes
    let queryHash512 ← expectSuccess' (fun h => s!"sha3_query(..., 512) = {h.size} bytes") do
      let stmt ← db.prepare "SELECT sha3_query('SELECT * FROM sha3_test', 512);"
      discard stmt.step
      stmt.columnBlob 0
    expect (queryHash512.size == 64) s!"Expected 64-byte hash, got {queryHash512.size}"

def testSha3Agg (db : SQLite) : TestM Unit :=
  withHeader "=== Testing sha3_agg() Function ===" do
    -- Test sha3_agg aggregate function
    let aggHash ← expectSuccess' (fun h => s!"sha3_agg() = {h.size} bytes") do
      let stmt ← db.prepare "SELECT sha3_agg(name) FROM sha3_test;"
      discard stmt.step
      stmt.columnBlob 0
    expect (aggHash.size == 32) s!"Expected 32-byte hash, got {aggHash.size}"

    -- Test sha3_agg with different hash size
    let aggHash384 ← expectSuccess' (fun h => s!"sha3_agg(..., 384) = {h.size} bytes") do
      let stmt ← db.prepare "SELECT sha3_agg(value, 384) FROM sha3_test;"
      discard stmt.step
      stmt.columnBlob 0
    expect (aggHash384.size == 48) s!"Expected 48-byte hash, got {aggHash384.size}"

    -- Test sha3_agg with multiple columns
    let aggHashMulti ← expectSuccess' (fun h => s!"sha3_agg with multiple values = {h.size} bytes") do
      let stmt ← db.prepare "SELECT sha3_agg(name || ':' || value) FROM sha3_test;"
      discard stmt.step
      stmt.columnBlob 0
    expect (aggHashMulti.size == 32) s!"Expected 32-byte hash, got {aggHashMulti.size}"

def testSha3Determinism (db : SQLite) : TestM Unit :=
  withHeader "=== Testing SHA3 Determinism ===" do
    -- Verify that sha3_query produces consistent results across multiple calls
    let hash1 ← expectSuccess' (fun _ => "First hash of sha3_test") do
      let stmt ← db.prepare "SELECT sha3_query('SELECT * FROM sha3_test ORDER BY id');"
      discard stmt.step
      stmt.columnBlob 0

    -- Modify and restore data to potentially change physical storage
    db.exec "INSERT INTO sha3_test (name, value) VALUES ('temp', 999);"
    db.exec "DELETE FROM sha3_test WHERE name = 'temp';"

    let hash2 ← expectSuccess' (fun _ => "Second hash after insert/delete") do
      let stmt ← db.prepare "SELECT sha3_query('SELECT * FROM sha3_test ORDER BY id');"
      discard stmt.step
      stmt.columnBlob 0

    expect (hash1 == hash2) "Expected identical hashes after insert/delete cycle"
    recordSuccess "Verified: sha3_query produces consistent hashes for same logical content"

def runSha3Tests (db : SQLite) : TestM Unit := do
  testEnableSha3 db
  testSha3Function db
  testSha3Query db
  testSha3Agg db
  testSha3Determinism db
