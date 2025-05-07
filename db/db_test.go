package db

import (
	"os"
	"testing"
)

func setupTempDb(t *testing.T) (*DB, func()) {
	t.Helper()
	// create a temp file
	f, err := os.CreateTemp("", "kvstore_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	// remove file on cleanup
	// I defer closing until test ends, because file may be deleted by OS maybe?
	cleanup := func() {
		f.Close()
		os.Remove(f.Name())
	}

	// open our DB instance using the file
	db, err := Open(f.Name())
	if err != nil {
		cleanup()
		t.Fatalf("failed to open DB: %v", err)
	}

	return db, cleanup
}

func TestSetAndGet(t *testing.T) {
	db, cleanup := setupTempDb(t)
	defer cleanup()
	defer db.Close()

	// set a key and retrieve it using new RPC-style signatures
	setArgs := &SetArgs{Key: "foo", Val: "bar"}
	if err := db.Set(setArgs, &struct{}{}); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	var val string
	if err := db.Get(&GetArgs{Key: "foo"}, &val); err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if val != "bar" {
		t.Errorf("expected 'bar', got '%s'", val)
	}
}

func TestOverwrite(t *testing.T) {
	db, cleanup := setupTempDb(t)
	defer cleanup()
	defer db.Close()

	// set a key twice
	db.Set(&SetArgs{Key: "key", Val: "first"}, &struct{}{})
	db.Set(&SetArgs{Key: "key", Val: "second"}, &struct{}{})

	var val string
	if err := db.Get(&GetArgs{Key: "key"}, &val); err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if val != "second" {
		t.Errorf("expected 'second', got '%s'", val)
	}
}

func TestKeyNotFound(t *testing.T) {
	db, cleanup := setupTempDb(t)
	defer cleanup()
	defer db.Close()

	var val string
	err := db.Get(&GetArgs{Key: "missing"}, &val)
	if _, ok := err.(*KeyNotFoundError); !ok {
		t.Errorf("expected KeyNotFoundError, got %v", err)
	}
}
