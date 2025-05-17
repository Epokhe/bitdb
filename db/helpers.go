package db

import (
	"os"
	"testing"
)

func SetupTempDb(tb testing.TB) (path string, db *DB) {
	tb.Helper()

	// make a temp filename
	tmp, err := os.CreateTemp("", "kvdb_*_.db")
	if err != nil {
		tb.Fatalf("CreateTemp failed: %v", err)
	}
	path = tmp.Name()
	tmp.Close()

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
