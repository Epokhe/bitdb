package core

import (
	"os"
	"testing"
)

func SetupTempDb(tb testing.TB) (path string, db *DB) {
	tb.Helper()

	// make a temp dir
	path, err := os.MkdirTemp("", "kvdb_test_*")
	if err != nil {
		tb.Fatalf("CreateTemp failed: %v", err)
	}

	// open the db
	db, err = Open(path)
	if err != nil {
		// if Open fails, clean up the file immediately
		os.Remove(path)
		tb.Fatalf("Open(%q) failed: %v", path, err)
	}

	// On cleanup, close DB then delete file
	tb.Cleanup(func() {
		db.Close()
		os.Remove(path)
	})

	return path, db
}
