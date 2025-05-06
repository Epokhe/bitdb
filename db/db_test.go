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

	// set a key and retrieve it
	err := db.Set("foo", "bar")
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	val, err := db.Get("foo")
	if err != nil {
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
	db.Set("key", "first")
	db.Set("key", "second")

	val, err := db.Get("key")
	if err != nil {
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

	_, err := db.Get("missing")
	_, ok := err.(*KeyNotFoundError)
	if !ok {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}
}
