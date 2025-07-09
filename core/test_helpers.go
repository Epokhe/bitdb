package core

import (
	"os"
	"testing"
)

func SetupTempDB(tb testing.TB, dbOpts ...Option) (path string, db *DB) {
	// make a temp dir
	path, err := os.MkdirTemp("", "kvdb_test_*")
	if err != nil {
		tb.Fatalf("CreateTemp failed: %v", err)
	}

	// open the db
	db, err = Open(path, dbOpts...)
	if err != nil {
		// if Open fails, clean up the file immediately
		_ = os.RemoveAll(path)
		tb.Fatalf("Open(%q) failed: %v", path, err)
	}

	// On cleanup, close DB then delete file
	tb.Cleanup(func() {
		_ = db.Close()
		_ = os.RemoveAll(path)
	})

	return path, db
}
