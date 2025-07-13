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
	db, _, _ := SetupTempDB(t, WithMergeEnabled(false))

	// set a key and immediately retrieve it (read-after-write consistency)
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
	db, _, _ := SetupTempDB(t, WithMergeEnabled(false))

	// set a key twice
	_ = db.Set("key", "first")
	_ = db.Set("key", "second")

	if val, err := db.Get("key"); err != nil {
		t.Fatalf("Get returned error: %v", err)
	} else if val != "second" {
		t.Errorf("expected 'second', got '%s'", val)
	}
}

func TestKeyNotFound(t *testing.T) {
	db, _, _ := SetupTempDB(t, WithMergeEnabled(false))

	if _, err := db.Get("missing"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("expected KeyNotFoundError, got %v", err)
	}
}

func TestPersistence(t *testing.T) {
	db, path, _ := SetupTempDB(t, WithMergeEnabled(false))

	_ = db.Set("a", "1")
	_ = db.Set("b", "2")
	_ = db.Close()

	// Re-open
	db2, err := Open(path, WithMergeEnabled(false))
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer db2.Close() // nolint:errcheck

	if val, err := db2.Get("a"); err != nil || val != "1" {
		t.Errorf("expected a=1 after reopen, got %q, %v", val, err)
	}
	if val, err := db2.Get("b"); err != nil || val != "2" {
		t.Errorf("expected b=2 after reopen, got %q, %v", val, err)
	}
}

func TestLoadIndexOverwrite(t *testing.T) {
	db, path, _ := SetupTempDB(t, WithMergeEnabled(false))

	_ = db.Set("foo", "first")
	_ = db.Set("foo", "second")
	_ = db.Close()

	// Now reopen and Get should return "second"
	db2, _ := Open(path, WithMergeEnabled(false))
	defer db2.Close() // nolint:errcheck
	if val, err := db2.Get("foo"); err != nil || val != "second" {
		t.Errorf("wanted final 'second', got %q", val)
	}
}

func TestEmptyDB(t *testing.T) {
	db, _, _ := SetupTempDB(t, WithMergeEnabled(false))

	if _, err := db.Get("nope"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("expected KeyNotFoundError on empty DB, got %v", err)
	}
}

func TestManyKeys(t *testing.T) {
	db, _, _ := SetupTempDB(t, WithMergeEnabled(false))

	for i := 0; i < 1000; i++ {
		k, v := fmt.Sprintf("k%03d", i), fmt.Sprintf("v%03d", i)
		_ = db.Set(k, v)
	}

	for i := 0; i < 1000; i++ {
		k, want := fmt.Sprintf("k%03d", i), fmt.Sprintf("v%03d", i)
		if got, err := db.Get(k); err != nil || got != want {
			t.Errorf("Get %q = %q, %v; want %q", k, got, err, want)
		}
	}
}

func TestTruncatedHeader(t *testing.T) {
	_, dir, _ := SetupTempDB(t, WithMergeEnabled(false))

	// Manually write a valid record + only half of the next header
	f, _ := os.Create(filepath.Join(dir, "seg001"))
	// header+key+val of ("x"→"y")
	_, _ = f.Write([]byte{1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 'x', 'y'})
	// now write only 2 of the next 10 header bytes
	_, _ = f.Write([]byte{0x02, 0x00})
	_ = f.Close()

	// Open should succeed, index should only contain "x"
	db, err := Open(dir, WithMergeEnabled(false))
	if err != nil {
		t.Fatalf("Open on truncated header: %v", err)
	}

	// good record should be indexed
	if val, err := db.Get("x"); err != nil || val != "y" {
		t.Errorf("expected x→y, got %q, %v", val, err)
	}

	// second key should not be indexed
	if len(db.index) != 1 {
		t.Errorf("expected 1 entry, got index %v", db.index)
	}
}

func TestTruncatedKey(t *testing.T) {
	_, dir, _ := SetupTempDB(t, WithMergeEnabled(false))

	f, _ := os.Create(filepath.Join(dir, "seg001"))

	// write one good record
	_, _ = f.Write([]byte{1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 'k', 'v'})

	// write header for keyLen=3,valLen=2, then only 1 byte of the key
	_, _ = f.Write([]byte{3, 0, 0, 0, 2, 0, 0, 0})
	// only 1 of the 3 key bytes
	_, _ = f.Write([]byte("x"))
	_ = f.Close()

	db, err := Open(dir, WithMergeEnabled(false))
	if err != nil {
		t.Fatalf("open on partial-key: %v", err)
	}

	// good record should be indexed
	if val, err := db.Get("k"); err != nil || val != "v" {
		t.Errorf("expected k→v, got %q, %v", val, err)
	}

	// second key should not be indexed
	if len(db.index) != 1 {
		t.Errorf("expected 1 entry, got index %v", db.index)
	}

}

func TestTruncatedValue(t *testing.T) {
	_, dir, _ := SetupTempDB(t, WithMergeEnabled(false))

	// write one good record, then header+full key, but only 1 of 2 value bytes
	f, _ := os.Create(filepath.Join(dir, "seg001"))
	// good record: keyLen=1, valLen=1, type=1, reserved=0, "k","v"
	_, _ = f.Write([]byte{1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 'k', 'v'})
	// next header: keyLen=2, valLen=2
	_, _ = f.Write([]byte{2, 0, 0, 0, 2, 0, 0, 0})
	// write full key "hi"
	_, _ = f.Write([]byte("hi"))
	// only 1 of 2 value bytes
	_, _ = f.Write([]byte("X"))
	_ = f.Close()

	db, err := Open(dir, WithMergeEnabled(false))
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
	db, dir, _ := SetupTempDB(t, WithMergeEnabled(false))

	// 1) Write two good records: "a"→"1", "b"→"2"
	_ = db.Set("a", "1")
	_ = db.Set("b", "2")

	// Capture the offset where "c" would go:
	active := db.segments[len(db.segments)-1]
	offC := active.size

	// 2) Simulate a crash *during* the third Set:
	//    manually open the same file and write only half of the 10-byte header
	f, _ := os.OpenFile(getSegmentPath(db.dir, active.id), os.O_WRONLY, 0)

	// Seek to where the next record should start
	_, _ = f.Seek(offC, io.SeekStart)

	// Write only 4 of the 10 header bytes (write only keyLen)
	hdrPart := make([]byte, 4)
	binary.LittleEndian.PutUint32(hdrPart, 3)
	_, _ = f.Write(hdrPart)
	_ = f.Close()

	// 3) Re-open the DB (scanSegment will stop at offC, and db.offset will be set to offC)
	db2, err := Open(dir, WithMergeEnabled(false))
	if err != nil {
		t.Fatalf("OpenDB after partial append: %v", err)
	}
	defer db2.Close() // nolint:errcheck

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
		keys              = 10
		rounds            = 5 // overwrite each key this many times
		rolloverThreshold = 1 * 32
		totalWrites       = keys * rounds

		// calculate the size of a single record
		overhead = hdrLen
		keyLen   = 5 // "k%04d"
		valLen   = 2 // "x"
		writeLen = overhead + keyLen + valLen

		// With post-write rollover, a segment can fit n writes where the size
		// after the nth write is the first to be >= the threshold.
		// The number of writes that fit is floor((threshold-1)/writeLen) + 1.
		writesPerSeg = (rolloverThreshold-1)/writeLen + 1

		// The DB starts with 1 segment. A new segment is created after a
		// rollover is triggered. The number of rollovers is totalWrites / writesPerSeg.
		expectedSegs = 1 + (totalWrites / writesPerSeg)
	)

	// open with tiny segment threshold
	db, _, _ := SetupTempDB(t, WithRolloverThreshold(int64(rolloverThreshold)), WithMergeEnabled(false))

	// drive all the writes
	for r := 0; r < rounds; r++ {
		for k := 0; k < keys; k++ {
			key := fmt.Sprintf("k%04d", k)
			_ = db.Set(key, "xx")
		}
	}

	// observe on-disk state
	segs := len(db.segments)
	size, err := db.DiskSize()
	if err != nil {
		t.Fatalf("DiskSize: %v", err)
	}

	t.Logf(
		"writesPerSeg=%d, expectedSegs=%d, observedSegs=%d; segSizeMax=%d, diskSize=%d",
		writesPerSeg, expectedSegs, segs, rolloverThreshold, size,
	)

	if segs != expectedSegs {
		t.Fatalf("segment count mismatch: expected %d, got %d", expectedSegs, segs)
	}

	if size < int64(totalWrites*writeLen) {
		t.Fatalf("disk size too small: expected ≥%d, got %d",
			totalWrites*writeLen, size)
	}
}

func TestGetLatestWinsAcrossSegments(t *testing.T) {
	db, _, _ := SetupTempDB(t, WithRolloverThreshold(1), WithMergeEnabled(false)) // force a new segment per write

	_ = db.Set("k", "v1")
	_ = db.Set("k", "v2")

	out, _ := db.Get("k")
	if out != "v2" {
		t.Fatalf("want v2, got %q", out)
	}
}

func TestRecoveryAcrossSegmentBoundary(t *testing.T) {
	db, dir, _ := SetupTempDB(t, WithRolloverThreshold(16), WithMergeEnabled(false))

	// ─── SETUP: roll three segments by overwriting the same key ───
	_ = db.Set("foo", "A")
	_ = db.Set("foo", "B")
	_ = db.Set("foo", "C")

	// ─── CRASH: truncate the last segment before C's header ───
	active := db.segments[len(db.segments)-1]
	off := db.index["foo"].offset // where C's header would start
	f, _ := os.OpenFile(getSegmentPath(db.dir, active.id), os.O_WRONLY, 0)
	_ = f.Truncate(off)
	_ = f.Close()

	// ─── RECOVER: re-open and check that "C" was dropped, so Get returns "B" ───
	db2, err := Open(dir, WithRolloverThreshold(16), WithMergeEnabled(false))
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer db2.Close() // nolint:errcheck

	got, err := db2.Get("foo")
	if err != nil {
		t.Fatalf("Get after recovery: %v", err)
	}
	if got != "B" {
		t.Errorf("expected foo→B after recovery, got %q", got)
	}
}

// TestManifestOrderingAffectsWinner rewrites the MANIFEST lines so the older
// segment is replayed *after* the newer one and verifies that the DB returns
// the value from the segment that appears last in the file, regardless of its
// numeric id.
func TestManifestOrderingAffectsWinner(t *testing.T) {
	db, dir, _ := SetupTempDB(t, WithRolloverThreshold(1), WithMergeEnabled(false)) // force 1 key per segment

	_ = db.Set("k", "old") // seg001
	_ = db.Set("k", "new") // seg002 (last-writer-wins originally)
	_ = db.Close()

	// Rewrite MANIFEST: list seg002 first, seg001 second
	manPath := filepath.Join(dir, "MANIFEST")
	if err := os.WriteFile(manPath, []byte("2\n1\n"), 0o644); err != nil {
		t.Fatalf("rewrite manifest: %v", err)
	}

	reopened, err := Open(dir, WithRolloverThreshold(db.rolloverThreshold), WithMergeEnabled(false))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close() // nolint:errcheck

	if got, _ := reopened.Get("k"); got != "old" {
		t.Fatalf("want 'old' (manifest order 2→1), got %q", got)
	}
}

// TestEmptyTailSegmentReuse simulates a crash right after MANIFEST was updated
// with a new id but before any bytes were written to that file.  On reopen the
// DB should reuse the zero-byte file as its active writer.
func TestEmptyTailSegmentReuse(t *testing.T) {
	db, dir, _ := SetupTempDB(t, WithMergeEnabled(false))
	_ = db.Set("a", "1") // seg001 with data

	// Force-create an empty seg002 and *do not* write to it.
	err := db.addSegment()
	if err != nil {
		t.Fatalf("addSegment: %v", err)
	}

	newSeg := db.segments[len(db.segments)-1]
	empty := getSegmentPath(db.dir, newSeg.id)
	_ = db.Close()

	db2, err := Open(dir, WithRolloverThreshold(db.rolloverThreshold), WithMergeEnabled(false))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close() // nolint:errcheck

	if err := db2.Set("b", "2"); err != nil {
		t.Fatalf("set after reopen: %v", err)
	}

	info, _ := os.Stat(empty)
	if info.Size() == 0 {
		t.Fatalf("expected %s to be reused and non-empty", empty)
	}
}

// TestNextFileNumberSkipsGaps ensures new segment ids always exceed the max
// id seen in existing segments, even when MANIFEST ids skip numbers.
func TestNextFileNumberSkipsGaps(t *testing.T) {
	dir := t.TempDir()

	// Pre-seed seg005 and seg009, and MANIFEST listing both
	for _, id := range []int{5, 9} {
		name := fmt.Sprintf("seg%03d", id)
		_ = os.WriteFile(filepath.Join(dir, name), nil, 0o644)
	}
	_ = os.WriteFile(filepath.Join(dir, "MANIFEST"), []byte("5\n9\n"), 0o644)

	db, err := Open(dir, WithRolloverThreshold(1), WithMergeEnabled(false))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close() // nolint:errcheck

	// Trigger creation of new segment via Set
	_ = db.Set("k", "v")
	_ = db.Set("k", "v") // second write should roll to new segment

	active := db.segments[len(db.segments)-1]
	if active.id <= 9 {
		t.Fatalf("expected new id >9, got %d", active.id)
	}
}

func TestDeleteBasic(t *testing.T) {
	db, _, _ := SetupTempDB(t, WithMergeEnabled(false))

	// Set then delete
	_ = db.Set("key1", "value1")
	err := db.Delete("key1")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Get should return ErrKeyNotFound
	if _, err := db.Get("key1"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound after delete, got %v", err)
	}
}

func TestDeleteNonExistentKey(t *testing.T) {
	db, _, _ := SetupTempDB(t, WithMergeEnabled(false))

	err := db.Delete("nonexistent")
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound for nonexistent key, got %v", err)
	}
}

func TestDeleteAfterOverwrite(t *testing.T) {
	db, _, _ := SetupTempDB(t, WithMergeEnabled(false))

	_ = db.Set("key", "value1")
	_ = db.Set("key", "value2") // overwrite
	err := db.Delete("key")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if _, err := db.Get("key"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound after delete, got %v", err)
	}
}

func TestDeletePersistence(t *testing.T) {
	db, path, _ := SetupTempDB(t, WithMergeEnabled(false))

	_ = db.Set("key1", "value1")
	_ = db.Set("key2", "value2")
	_ = db.Delete("key1")
	_ = db.Close()

	// Reopen and verify deletion persisted
	db2, err := Open(path, WithMergeEnabled(false))
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer db2.Close() // nolint:errcheck

	// key1 should still be deleted
	if _, err := db2.Get("key1"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("deleted key found after reopen: %v", err)
	}

	// key2 should still exist
	if val, err := db2.Get("key2"); err != nil || val != "value2" {
		t.Errorf("expected key2=value2, got %q, %v", val, err)
	}
}

func TestDeleteTriggersRollover(t *testing.T) {
	db, _, _ := SetupTempDB(t, WithRolloverThreshold(25), WithMergeEnabled(false))

	_ = db.Set("key1", "value1") // 20 bytes (10 header + 4 key + 6 value)

	countBefore := len(db.segments)

	// This delete should trigger rollover (20 + 14 = 34 > 25)
	_ = db.Delete("key1") // 14 bytes (10 header + 4 key + 0 value)

	countAfter := len(db.segments)
	if countAfter != countBefore+1 {
		t.Errorf("expected rollover, segments: %d -> %d", countBefore, countAfter)
	}
}

func TestSetAfterDelete(t *testing.T) {
	db, _, _ := SetupTempDB(t, WithMergeEnabled(false))

	_ = db.Set("key", "original")
	_ = db.Delete("key")
	_ = db.Set("key", "resurrected")

	if val, err := db.Get("key"); err != nil || val != "resurrected" {
		t.Errorf("expected key=resurrected, got %q, %v", val, err)
	}
}

func TestDeleteMultipleKeys(t *testing.T) {
	db, _, _ := SetupTempDB(t, WithMergeEnabled(false))

	// Set multiple keys
	keys := []string{"a", "b", "c", "d", "e"}
	for _, key := range keys {
		_ = db.Set(key, "value_"+key)
	}

	// Delete every other key
	toDelete := []string{"a", "c", "e"}
	for _, key := range toDelete {
		if err := db.Delete(key); err != nil {
			t.Fatalf("Delete %q failed: %v", key, err)
		}
	}

	// Verify deleted keys are gone
	for _, key := range toDelete {
		if _, err := db.Get(key); !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("expected %q to be deleted, got %v", key, err)
		}
	}

	// Verify remaining keys still exist
	remaining := []string{"b", "d"}
	for _, key := range remaining {
		expected := "value_" + key
		if val, err := db.Get(key); err != nil || val != expected {
			t.Errorf("expected %q=%q, got %q, %v", key, expected, val, err)
		}
	}
}
