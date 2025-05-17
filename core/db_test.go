package core

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
)

func TestSetAndGet(t *testing.T) {
	_, db := SetupTempDb(t)

	// set a key and retrieve it
	if err := db.Set("foo", "bar"); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	if val, err := db.Get("foo"); err != nil {
		t.Fatalf("Get returned error: %v", err)
	} else if val != "bar" {
		t.Errorf("expected 'bar', got '%s'", val)
	}
}

func TestOverwrite(t *testing.T) {
	_, db := SetupTempDb(t)

	// set a key twice
	db.Set("key", "first")
	db.Set("key", "second")

	if val, err := db.Get("key"); err != nil {
		t.Fatalf("Get returned error: %v", err)
	} else if val != "second" {
		t.Errorf("expected 'second', got '%s'", val)
	}
}

func TestKeyNotFound(t *testing.T) {
	_, db := SetupTempDb(t)

	if _, err := db.Get("missing"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("expected KeyNotFoundError, got %v", err)
	}
}

func TestPersistence(t *testing.T) {
	path, db := SetupTempDb(t)

	db.Set("a", "1")
	db.Set("b", "2")
	db.Close()

	// Re-open
	db2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer db2.Close()

	if val, err := db2.Get("a"); err != nil || val != "1" {
		t.Errorf("expected a=1 after reopen, got %q, %v", val, err)
	}
	if val, err := db2.Get("b"); err != nil || val != "2" {
		t.Errorf("expected b=2 after reopen, got %q, %v", val, err)
	}
}

func TestLoadIndexOverwrite(t *testing.T) {
	path, db := SetupTempDb(t)

	db.Set("foo", "first")
	db.Set("foo", "second")
	db.Close()

	// Now reopen and Get should return “second”
	db2, _ := Open(path)
	defer db2.Close()
	if val, err := db2.Get("foo"); err != nil || val != "second" {
		t.Errorf("wanted final ‘second’, got %q", val)
	}
}

func TestEmptyDB(t *testing.T) {
	_, db := SetupTempDb(t)

	if _, err := db.Get("nope"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("expected KeyNotFoundError on empty DB, got %v", err)
	}
}

func TestManyKeys(t *testing.T) {
	_, db := SetupTempDb(t)

	for i := 0; i < 1000; i++ {
		k, v := fmt.Sprintf("k%03d", i), fmt.Sprintf("v%03d", i)
		if err := db.Set(k, v); err != nil {
			t.Fatalf("Set %d failed: %v", i, err)
		}
	}
	for i := 0; i < 1000; i++ {
		k, want := fmt.Sprintf("k%03d", i), fmt.Sprintf("v%03d", i)
		if got, err := db.Get(k); err != nil || got != want {
			t.Errorf("Get %q = %q, %v; want %q", k, got, err, want)
		}
	}
}

func TestTruncatedHeader(t *testing.T) {
	// Manually write a valid record + only half of the next header
	f, _ := os.CreateTemp("", "kv_test")
	defer os.Remove(f.Name())
	// header+key+val of (“x”→“y”)
	f.Write([]byte("\x01\x00\x00\x00\x01\x00\x00\x00xy"))
	// now write only 2 of the next 8 header bytes
	f.Write([]byte{0x02, 0x00})
	f.Close()

	// Open should succeed, index should only contain “x”
	db, err := Open(f.Name())
	if err != nil {
		t.Fatalf("Open on truncated header: %v", err)
	}
	defer db.Close()
	if val, err := db.Get("x"); err != nil || val != "y" {
		t.Errorf("expected x→y, got %q, %v", val, err)
	}

	if _, err = db.Get("<garbage>"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("expected missing key, got %v", err)
	}

}

func TestTruncatedKey(t *testing.T) {
	// write header for keyLen=3,valLen=2, then only 1 byte of the key
	f, _ := os.CreateTemp("", "kv")
	defer os.Remove(f.Name())
	// header: keyLen=3,valLen=2
	f.Write([]byte{3, 0, 0, 0, 2, 0, 0, 0})
	// only 1 of the 3 key bytes
	f.Write([]byte("x"))
	f.Close()

	db, err := Open(f.Name())
	if err != nil {
		t.Fatalf("open on partial-key: %v", err)
	}
	defer db.Close()
	if len(db.index) != 0 {
		t.Errorf("expected no entries, got index %v", db.index)
	}
}

func TestTruncatedValue(t *testing.T) {
	// write one good record, then header+full key, but only 1 of 2 value bytes
	f, _ := os.CreateTemp("", "kv")
	defer os.Remove(f.Name())
	// good record: keyLen=1,valLen=1,"k","v"
	f.Write([]byte{1, 0, 0, 0, 1, 0, 0, 0, 'k', 'v'})
	// next header: keyLen=2,valLen=2
	f.Write([]byte{2, 0, 0, 0, 2, 0, 0, 0})
	// write full key "hi"
	f.Write([]byte("hi"))
	// only 1 of 2 value bytes
	f.Write([]byte("X"))
	f.Close()

	db, err := Open(f.Name())
	if err != nil {
		t.Fatalf("open on partial-value: %v", err)
	}
	defer db.Close()

	// only the first good record should be indexed
	if val, err := db.Get("k"); err != nil || val != "v" {
		t.Errorf("expected k→v, got %q, %v", val, err)
	}
	if _, err = db.Get("hi"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("expected hi missing, got %v", err)
	}
}

func TestOverwriteAfterPartialAppend(t *testing.T) {
	path, db := SetupTempDb(t)

	// 1) Write two good records: “a”→“1”, “b”→“2”
	if err := db.Set("a", "1"); err != nil {
		t.Fatalf("Set a=1: %v", err)
	}
	if err := db.Set("b", "2"); err != nil {
		t.Fatalf("Set b=2: %v", err)
	}

	// Capture the offset where “c” would go:
	offC := db.offset

	// 2) Simulate a crash *during* the third Set:
	//    manually open the same file and write only half of the 8-byte header
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open for corrupt: %v", err)
	}
	// Seek to where the next record should start
	if _, err := f.Seek(offC, io.SeekStart); err != nil {
		t.Fatalf("seek to offC: %v", err)
	}
	// Write only 4 of the 8 header bytes (e.g. keyLen=3, valLen=4 → write only keyLen)
	hdrPart := make([]byte, 4)
	binary.LittleEndian.PutUint32(hdrPart, 3)
	if _, err := f.Write(hdrPart); err != nil {
		t.Fatalf("write partial header: %v", err)
	}
	f.Close()

	// 3) Re-open the DB (loadIndex will stop at offC, and db.offset will be set to offC)
	db2, err := Open(f.Name())
	if err != nil {
		t.Fatalf("OpenDB after partial append: %v", err)
	}
	defer db2.Close()

	// 4) Now do the real Set("c","3") — it *must* go at offC, overwriting the garbage.
	if err := db2.Set("c", "3"); err != nil {
		t.Fatalf("Set c=3: %v", err)
	}

	// 5) And now Get("c") should succeed
	if got, err := db2.Get("c"); err != nil {
		t.Fatalf("Get c failed: %v", err)
	} else if got != "3" {
		t.Errorf("expected c→3 after overwrite, got %q", got)
	}
}
