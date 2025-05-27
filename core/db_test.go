package core

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
		db.Set(k, v)
	}

	for i := 0; i < 1000; i++ {
		k, want := fmt.Sprintf("k%03d", i), fmt.Sprintf("v%03d", i)
		if got, err := db.Get(k); err != nil || got != want {
			t.Errorf("Get %q = %q, %v; want %q", k, got, err, want)
		}
	}
}

func TestTruncatedHeader(t *testing.T) {
	dir, db := SetupTempDb(t)

	// Manually write a valid record + only half of the next header
	f, _ := os.Create(filepath.Join(dir, "seg001"))
	// header+key+val of (“x”→“y”)
	f.Write([]byte("\x01\x00\x00\x00\x01\x00\x00\x00xy"))
	// now write only 2 of the next 8 header bytes
	f.Write([]byte{0x02, 0x00})
	f.Close()

	// Open should succeed, index should only contain “x”
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open on truncated header: %v", err)
	}
	if val, err := db.Get("x"); err != nil || val != "y" {
		t.Errorf("expected x→y, got %q, %v", val, err)
	}

	if _, err = db.Get("<garbage>"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("expected missing key, got %v", err)
	}
}

func TestTruncatedKey(t *testing.T) {
	dir, db := SetupTempDb(t)

	// write header for keyLen=3,valLen=2, then only 1 byte of the key
	f, _ := os.Create(filepath.Join(dir, "seg001"))
	// header: keyLen=3,valLen=2
	f.Write([]byte{3, 0, 0, 0, 2, 0, 0, 0})
	// only 1 of the 3 key bytes
	f.Write([]byte("x"))
	f.Close()

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("open on partial-key: %v", err)
	}
	if len(db.LastSegment().index) != 0 {
		t.Errorf("expected no entries, got index %v", db.LastSegment().index)
	}
}

func TestTruncatedValue(t *testing.T) {
	dir, db := SetupTempDb(t)

	// write one good record, then header+full key, but only 1 of 2 value bytes
	f, _ := os.Create(filepath.Join(dir, "seg001"))
	// good record: keyLen=1,valLen=1,"k","v"
	f.Write([]byte{1, 0, 0, 0, 1, 0, 0, 0, 'k', 'v'})
	// next header: keyLen=2,valLen=2
	f.Write([]byte{2, 0, 0, 0, 2, 0, 0, 0})
	// write full key "hi"
	f.Write([]byte("hi"))
	// only 1 of 2 value bytes
	f.Write([]byte("X"))
	f.Close()

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("open on partial-value: %v", err)
	}

	// only the first good record should be indexed
	if val, err := db.Get("k"); err != nil || val != "v" {
		t.Errorf("expected k→v, got %q, %v", val, err)
	}
	if _, err = db.Get("hi"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("expected hi missing, got %v", err)
	}
}

func TestOverwriteAfterPartialAppend(t *testing.T) {
	dir, db := SetupTempDb(t)

	// 1) Write two good records: “a”→“1”, “b”→“2”
	db.Set("a", "1")
	db.Set("b", "2")

	// Capture the offset where “c” would go:
	offC := db.LastSegment().size

	// 2) Simulate a crash *during* the third Set:
	//    manually open the same file and write only half of the 8-byte header
	f, _ := os.OpenFile(db.LastSegment().path, os.O_WRONLY, 0)

	// Seek to where the next record should start
	f.Seek(offC, io.SeekStart)

	// Write only 4 of the 8 header bytes (e.g. keyLen=3, valLen=4 → write only keyLen)
	hdrPart := make([]byte, 4)
	binary.LittleEndian.PutUint32(hdrPart, 3)
	f.Write(hdrPart)
	f.Close()

	// 3) Re-open the DB (loadIndex will stop at offC, and db.offset will be set to offC)
	db2, err := Open(dir)
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

func TestSegmentCount(t *testing.T) {
	const (
		keys         = 10
		rounds       = 5 // overwrite each key this many times
		segSizeMax   = 1 * 32
		keyLen       = 5
		overhead     = 8                          // 4B keyLen prefix + 4B valLen prefix
		valLen       = 3                          // choosing this deliberately to make writeLen a factor of segSizeMax
		writeLen     = overhead + keyLen + valLen // bytes per record
		totalWrites  = keys * rounds
		totalBytes   = writeLen * totalWrites                     // overall bytes touched
		expectedSegs = (totalBytes + segSizeMax - 1) / segSizeMax // `ceil`ed division
	)

	// Open with a tiny segment threshold
	_, db := SetupTempDb(t, WithSegmentSizeMax(int64(segSizeMax)))

	// 1) Drive the writes
	for r := 0; r < rounds; r++ {
		for k := 0; k < keys; k++ {
			key := fmt.Sprintf("k%04d", k)
			db.Set(key, "xxx")
		}
	}

	// 2) Observe on‐disk state
	segs := len(db.segments)
	size, err := db.DiskSize()
	if err != nil {
		t.Fatalf("DiskSize: %v", err)
	}

	t.Logf(
		"expectedSegments=%d, observedSegments=%d; totalBytes=%d, segSizeMax=%d, on-disk size=%d",
		expectedSegs, segs, totalBytes, segSizeMax, size,
	)

	// 3) Checks
	if segs != expectedSegs {
		t.Fatalf("segment count mismatch: expected %d, got %d", expectedSegs, segs)
	}
	if size < totalBytes {
		t.Fatalf("disk size too small: expected ≥%d, got %d", totalBytes, size)
	}
}

func TestGetLatestWinsAcrossSegments(t *testing.T) {
	_, db := SetupTempDb(t, WithSegmentSizeMax(1)) // force a new segment per write

	db.Set("k", "v1")
	db.Set("k", "v2")

	out, _ := db.Get("k")
	if out != "v2" {
		t.Fatalf("want v2, got %q", out)
	}
}

func TestExactBoundaryRollover(t *testing.T) {
	// tiny segment size so we can hit boundary quickly:
	_, db := SetupTempDb(t, WithSegmentSizeMax(50))

	// compute one writeLen for "k00"->"x"
	writeLen := int64(8 + 3 + 1) // overhead(8) + key("k00")=3 + val("x")=1

	// write enough records to get within one record of the edge:
	n := int(50/writeLen) - 1
	for i := 0; i < n; i++ {
		db.Set(fmt.Sprintf("k%02d", i), "x")
	}

	// segment count so far:
	if got := len(db.segments); got != 1 {
		t.Fatalf("want exactly 1 segment, got %d", got)
	}

	// one more write: exactly fills the segment
	db.Set(fmt.Sprintf("k%02d", n), "x")
	if got := len(db.segments); got != 1 {
		t.Errorf("exact fill should NOT roll over, got %d segments", got)
	}

	// one more: this must roll to segment #2
	db.Set(fmt.Sprintf("k%02d", n+1), "x")
	if got := len(db.segments); got != 2 {
		t.Errorf("overflow write should roll over, got %d segments", got)
	}
}

func TestRecoveryAcrossSegmentBoundary(t *testing.T) {
	dir, db := SetupTempDb(t, WithSegmentSizeMax(16))

	// ─── SETUP: roll three segments by overwriting the same key ───
	db.Set("foo", "A")
	db.Set("foo", "B")
	db.Set("foo", "C")

	// ─── CRASH: truncate the last segment before C’s header ───
	last := db.LastSegment()
	off := last.index["foo"] // where C’s header would start
	f, _ := os.OpenFile(last.path, os.O_WRONLY, 0)
	f.Truncate(off)
	f.Close()

	// ─── RECOVER: re-open and check that “C” was dropped, so Get returns “B” ───
	db2, err := Open(dir, WithSegmentSizeMax(16))
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer db2.Close()

	got, err := db2.Get("foo")
	if err != nil {
		t.Fatalf("Get after recovery: %v", err)
	}
	if got != "B" {
		t.Errorf("expected foo→B after recovery, got %q", got)
	}
}
