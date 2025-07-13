//go:build goexperiment.synctest

package core

import (
	"errors"
	"fmt"
	mapset "github.com/deckarep/golang-set/v2"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
)

// TestMergeRunsOnlyWhenThresholdExceeded ensures we do NOT merge prematurely,
// then checks we merge when threshold is crossed.
func TestMergeRunsOnlyWhenThresholdExceeded(t *testing.T) {
	synctest.Run(func() {
		db, _, _ := SetupTempDB(t,
			WithRolloverThreshold(20), // multiple records per segment
			WithMergeThreshold(3),     // start merge after 3 inactive segments
			WithMergeEnabled(true),
		)

		// Each Set operation adds 14 bytes (10 bytes header + 2 bytes key + 2 bytes value).
		// Segment size limit is 20 bytes.
		_ = db.Set("k1", "v1")
		_ = db.Set("k1", "v2") // segment 1 over threshold, rollover
		_ = db.Set("k1", "v3")
		_ = db.Set("k1", "v4") // segment 2 over threshold, rollover

		// Currently there are 2 inactive segments, below merge threshold.

		synctest.Wait()
		if got := len(db.segments); got != 3 {
			t.Fatalf("merge ran too early; segments=%d", got)
		}

		_ = db.Set("k1", "v5")
		_ = db.Set("k1", "v6") // segment 3 over threshold, rollover. triggers merge

		synctest.Wait() // wait until merge goroutine exits

		if got := len(db.segments); got > 3 {
			t.Fatalf("expected ≤3 segments after merge, got %d", got)
		}
	})
}

// TestMergeKeepsLatestValue checks last-writer-wins correctness across merge.
func TestMergeKeepsLatestAndDropsObsolete(t *testing.T) {
	synctest.Run(func() {
		db, _, _ := SetupTempDB(t,
			WithRolloverThreshold(20),
			WithMergeThreshold(2),
			WithMergeEnabled(true),
		)

		// Each Set operation adds 10 bytes header + len(key) + len(value).
		_ = db.Set("k1", "old")
		_ = db.Set("k2", "old") // segment 1 over threshold, rollover
		_ = db.Set("k1", "new")
		_ = db.Set("k2", "new") // segment 2 over threshold, rollover, triggers merge

		synctest.Wait()

		// since `old` values are dropped, we will have 2 segments merged to one,
		// and 1 active segment: 2 segments in total
		if got := len(db.segments); got != 2 {
			t.Fatalf("expected 2 segments after merge, got %d", got)
		}

		if v, err := db.Get("k1"); err != nil {
			t.Fatalf("Get returned error: %v", err)
		} else if v != "new" {
			t.Fatalf("want new, got %q", v)
		}
	})
}

// TestMergeProducesMultipleSegments ensures merge may create more than one
// output segment when the size limit is tiny.
func TestMergeProducesMultipleSegments(t *testing.T) {
	synctest.Run(func() {
		db, _, _ := SetupTempDB(t,
			WithRolloverThreshold(20),
			WithMergeThreshold(3),
			WithMergeEnabled(true),
		)

		// Each Set operation adds 10 bytes header + len(key) + len(value).
		// Segment size limit is 20 bytes.
		for i := 0; i < 6; i++ {
			k := fmt.Sprintf("k%d", i)
			_ = db.Set(k, "v") // Segment rollover every 2 sets. Triggers merge after 2 rollovers.
		}

		// we should have 4 segments with ids 1,2,3,4
		synctest.Wait()

		// now, we should still have 4 segments, but they should have new ids
		if got := len(db.segments); got != 4 {
			t.Fatalf("expected 4 segments after merge, got %d", got)
		}

		// check new ids. note that only first 3 should be updated, so they should be 5,6,7,4
		want := []int{5, 6, 7, 4}

		for i, seg := range db.segments {
			if seg.id != want[i] {
				t.Fatalf("expected seg id %d, got %d", want[i], seg.id)
			}
		}
	})
}

// TestWritesWhileMerging checks two critical concurrency behaviors:
//  1. Writes that occur while a merge is active are correctly preserved.
//  2. Multiple rapid-fire merge triggers result in only a single merge operation,
//     preventing race conditions.
func TestWritesWhileMerging(t *testing.T) {
	synctest.Run(func() {
		var wg sync.WaitGroup
		wg.Add(1)

		var db *DB

		db, _, _ = SetupTempDB(t,
			WithRolloverThreshold(20),
			WithMergeThreshold(2), // Merge after 2 inactive segments.
			WithMergeEnabled(true),
			WithOnMergeStart(func() {
				// Pause the merge as soon as it starts.
				wg.Wait()

				// Trigger another merge, which will be skipped because of semaphore lock
				_ = db.Set("k1", "vx")
				_ = db.Set("k5", "v5") // segment 3 over threshold, rollover, triggers merge(skipped)

				// Trigger another one just in case
				_ = db.Set("k6", "v6")
				_ = db.Set("k7", "v7") // segment 4 over threshold, rollover, triggers merge(skipped)

				// At this point, there are 4 inactive + 1 active = 5 segments.

			}),
		)

		// Create two inactive segments (seg 1, seg 2).
		_ = db.Set("k1", "v1")
		_ = db.Set("k2", "v2") // segment 1 over threshold, rollover
		_ = db.Set("k2", "vy")
		_ = db.Set("k4", "v4") // segment 2 over threshold, rollover, triggers merge

		// The merge starts on segments 1 and 2, and then immediately pauses.
		// Rest of the Set calls happen inside the merge start callback

		// Un-pause the merge, allowing it to complete.
		wg.Done()
		synctest.Wait()

		// k2 is merged, should have its latest value
		if v, _ := db.Get("k2"); v != "vy" {
			t.Fatalf("want k2=vy, got %q", v)
		}
		// k1 got a new value outside the merge
		if v, _ := db.Get("k1"); v != "vx" {
			t.Fatalf("want k1=vx, got %q", v)
		}
		if v, _ := db.Get("k6"); v != "v6" {
			t.Fatalf("want k6=v6, got %q", v)
		}

		// after merge, we should have seg1 and seg2 merge, decreasing segment count
		if got := len(db.segments); got != 4 {
			t.Fatalf("expected 4 segments without merge, got %d", got)
		}

		// Verify that only ONE merge ran by checking the final segment ids.
		want := []int{6, 3, 4, 5}
		for i, seg := range db.segments {
			if seg.id != want[i] {
				t.Fatalf("expected seg id %d, got %d", want[i], seg.id)
			}
		}
	})
}

// TestMergeMultiRecordSegments verifies merging segments that hold multiple
// records each keeps the latest values.
func TestMergeMultiRecordSegments(t *testing.T) {
	synctest.Run(func() {
		db, _, _ := SetupTempDB(t,
			WithRolloverThreshold(20),
			WithMergeThreshold(3),
			WithMergeEnabled(true),
		)

		_ = db.Set("k1", "v1")
		_ = db.Set("k2", "v2") // segment 1 over threshold, rollover
		_ = db.Set("k1", "v3")
		_ = db.Set("k3", "v3") // segment 2 over threshold, rollover
		_ = db.Set("k4", "v4")
		_ = db.Set("k2", "v5") // segment 3 over threshold, rollover, triggers merge

		synctest.Wait()

		if v, _ := db.Get("k1"); v != "v3" {
			t.Fatalf("want k1=v3, got %q", v)
		}
		if v, _ := db.Get("k2"); v != "v5" {
			t.Fatalf("want k2=v2, got %q", v)
		}
		if v, _ := db.Get("k3"); v != "v3" {
			t.Fatalf("want k3=v3, got %q", v)
		}
		if v, _ := db.Get("k4"); v != "v4" {
			t.Fatalf("want k4=v4, got %q", v)
		}
	})
}

// TestMergeDisabled verifies that merges do not run when disabled.
func TestMergeDisabled(t *testing.T) {
	synctest.Run(func() {
		db, _, _ := SetupTempDB(t,
			WithRolloverThreshold(20),
			WithMergeThreshold(2),
			WithMergeEnabled(false),
		)

		// Each Set operation adds 10 bytes header + len(key) + len(value).
		// Segment size limit is 20 bytes.
		for i := 0; i < 6; i++ {
			k := fmt.Sprintf("k%d", i)
			_ = db.Set(k, "v") // Triggers segment rollover after 2 sets.
		}

		synctest.Wait()

		if got := len(db.segments); got != 4 {
			t.Fatalf("expected 4 segments without merge, got %d", got)
		}
		want := []int{1, 2, 3, 4}
		for i, seg := range db.segments {
			if seg.id != want[i] {
				t.Fatalf("expected seg id %d, got %d", want[i], seg.id)
			}
		}
	})
}

// TestMergePersistence verifies state is consistent after closing and reopening following a merge.
func TestMergePersistence(t *testing.T) {
	synctest.Run(func() {
		db, dir, _ := SetupTempDB(t,
			WithRolloverThreshold(20),
			WithMergeThreshold(3),
			WithMergeEnabled(true),
		)

		_ = db.Set("a", "1")
		_ = db.Set("b", "1") // seg1 over threshold, rollover
		_ = db.Set("a", "2")
		_ = db.Set("c", "3") // seg2 over threshold, rollover
		_ = db.Set("d", "4")
		_ = db.Set("b", "2") // seg3 over threshold, rollover, triggers merge

		synctest.Wait()

		// save segment ids before closing the db
		segs := make([]int, len(db.segments))
		for i, seg := range db.segments {
			segs[i] = seg.id
		}

		// save values before closing the db
		vals := map[string]string{}
		for _, k := range []string{"a", "b", "c", "d"} {
			v, err := db.Get(k)
			if err != nil {
				t.Fatalf("get %s: %v", k, err)
			}
			vals[k] = v
		}

		_ = db.Close()

		reopened, err := Open(dir,
			WithRolloverThreshold(20),
			WithMergeThreshold(3),
			WithMergeEnabled(true),
		)
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		defer reopened.Close() // nolint:errcheck

		if len(reopened.segments) != len(segs) {
			t.Fatalf("segment count mismatch after reopen: got %d want %d",
				len(reopened.segments), len(segs))
		}
		for i, seg := range reopened.segments {
			if seg.id != segs[i] {
				t.Fatalf("seg id mismatch at %d: got %d want %d", i, seg.id, segs[i])
			}
		}

		for k, want := range vals {
			got, err := reopened.Get(k)
			if err != nil || got != want {
				t.Fatalf("want %s=%s, got %s err=%v", k, want, got, err)
			}
		}
	})
}

// TestMultipleSequentialMerges triggers several merges and verifies the final
// segment count matches the mathematical expectation.
func TestMultipleSequentialMerges(t *testing.T) {
	synctest.Run(func() {
		const (
			// Using a threshold that is sensitive to write length changes.
			rolloverThreshold = 89
			mergeThreshold    = 2
			overhead          = hdrLen
			keyLen            = 2 // "k1" → 2 chars
			valLen            = 2 // "v0", "v1", ...
			writeLen          = overhead + keyLen + valLen

			// With post-write rollover, a segment can fit n writes where the size
			// after the nth write is the first to be >= the threshold.
			// The number of writes that fit is floor((threshold-1)/writeLen) + 1.
			writesPerSeg = (rolloverThreshold-1)/writeLen + 1
		)

		key := "k1"

		var mergeCount int
		db, _, _ := SetupTempDB(t,
			WithRolloverThreshold(rolloverThreshold),
			WithMergeThreshold(mergeThreshold),
			WithMergeEnabled(true),
			WithOnMergeStart(func() { mergeCount++ }),
		)

		// Create enough writes to trigger multiple merges. We want to end up
		// creating five segments in total which means four rollovers.
		const (
			numSegments = 5
			totalWrites = writesPerSeg * (numSegments - 1)
		)

		for i := 0; i < totalWrites; i++ {
			_ = db.Set(key, fmt.Sprintf("v%d", i))
			// wait after each rollover so merges can complete one by one
			if (i+1)%writesPerSeg == 0 {
				synctest.Wait()
			}
		}
		synctest.Wait()

		expectedMerges := numSegments - mergeThreshold
		if mergeCount != expectedMerges {
			t.Fatalf("expected %d merges, got %d", expectedMerges, mergeCount)
		}
		// same key getting overwritten should keep inactive segments at 1 length
		if got := len(db.segments); got != 2 {
			t.Fatalf("expected 2 segments after merge, got %d", got)
		}
	})
}

// TestMergeAfterTruncatedRecord verifies that the merge process can gracefully
// handle a truncated segment file.
func TestMergeAfterTruncatedRecord(t *testing.T) {
	synctest.Run(func() {
		var db *DB

		db, _, _ = SetupTempDB(t,
			WithRolloverThreshold(20),
			WithMergeThreshold(2), // Merge after 2 inactive segments.
			WithMergeEnabled(true),
			WithOnMergeStart(func() {
				// This callback runs synchronously at the start of a merge.
				// It provides a race-free window to corrupt a segment file
				// before the merge's recordScanner begins reading it.

				// The merge is running on segments 1 and 2. Let's truncate seg 1.
				// seg001 contains k1 and k2. We will truncate it mid-way through k2's record.
				// Truncate the file by removing the last byte of the last record.
				err := db.segments[0].file.Truncate(db.segments[0].size - 1)
				if err != nil {
					t.Fatalf("truncate file in callback: %v", err)
				}
			}),
		)

		// Create two inactive segments (seg 1, seg 2).
		// Each Set adds 12 bytes. Rollover is at 20.
		_ = db.Set("k1", "v1")
		_ = db.Set("k2", "v2") // seg1 rolls over.
		_ = db.Set("k3", "v3")
		_ = db.Set("k4", "v4") // seg2 rolls over, triggers merge.

		// Wait for the merge process (including the hook) to complete.
		synctest.Wait()

		// The merge should complete without propagating an error.
		select {
		case err := <-db.MergeErrors():
			t.Fatalf("unexpected merge error: %v", err)
		default:
		}

		// k1 from the truncated segment should be present.
		if v, err := db.Get("k1"); err != nil || v != "v1" {
			t.Fatalf("expected k1=v1, got %q, %v", v, err)
		}

		// k2, the truncated record, did not get included into the merged segment,
		// but its entry in db.index points to its location in the deleted segment.
		// So we expect a file closed error when trying to call seg.file.Read
		if v, err := db.Get("k2"); !errors.Is(err, fs.ErrClosed) {
			t.Fatalf("expected k2 to lead to file closed error, but got value: %q %v", v, err)
		}
		// k3 and k4 from the other, healthy segment should be present.
		if v, err := db.Get("k3"); err != nil || v != "v3" {
			t.Fatalf("expected k3=v3, got %q, %v", v, err)
		}
		if v, err := db.Get("k4"); err != nil || v != "v4" {
			t.Fatalf("expected k4=v4, got %q, %v", v, err)
		}
	})
}

// TestMergeDeletesOldSegments triggers a merge and verifies that on-disk
// segment files are replaced with the newly merged ones. It also checks that
// the MANIFEST lists only the new segment ids.
func TestMergeDeletesOldSegments(t *testing.T) {
	synctest.Run(func() {
		var captureErr error
		var wg sync.WaitGroup

		filesBefore := mapset.NewSet[string]()
		segsBefore := mapset.NewSet[string]()

		// Pause merge so we can snapshot the directory before it runs.
		wg.Add(1)

		var dir string
		var db *DB
		db, dir, _ = SetupTempDB(t,
			WithRolloverThreshold(20),
			WithMergeThreshold(2),
			WithMergeEnabled(true),
			WithOnMergeStart(func() {
				var entries []fs.DirEntry
				entries, captureErr = os.ReadDir(dir)
				for _, e := range entries {
					filesBefore.Add(e.Name())
				}

				// get the segments that will be merged
				db.rw.RLock()
				for _, seg := range db.segments[:len(db.segments)-1] {
					// seg001, seg002, seg003
					segsBefore.Add(fmt.Sprintf("seg%03d", seg.id))
				}
				db.rw.RUnlock()

				wg.Wait() // hold merge until after snapshot
			}),
		)

		// Create two inactive segments then trigger merge.
		_ = db.Set("k1", "v1")
		_ = db.Set("k2", "v2") // seg001 rollover
		_ = db.Set("k3", "v3")
		_ = db.Set("k4", "v4") // seg002 rollover -> triggers merge

		wg.Done()       // allow merge to continue
		synctest.Wait() // wait for merge completion

		if captureErr != nil {
			t.Fatalf("readdir before merge: %v", captureErr)
		}

		afterEntries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("readdir after merge: %v", err)
		}

		filesAfter := mapset.NewSet[string]()
		for _, e := range afterEntries {
			filesAfter.Add(e.Name())
		}

		// sanity check: to-merge segments must have their files in the directory
		if res := segsBefore.Difference(filesBefore); res.Cardinality() != 0 {
			t.Fatalf("segment files before merge not found: %v", res)
		}

		// files of the merged segments should not exist after the merge
		if res := segsBefore.Intersect(filesAfter); res.Cardinality() != 0 {
			t.Fatalf("old segment files still exist after merge: %v", res)
		}

		// Validate MANIFEST lists the ids of updated db.segments only.
		manPath := filepath.Join(dir, "MANIFEST")
		manBytes, err := os.ReadFile(manPath)
		if err != nil {
			t.Fatalf("read manifest: %v", err)
		}

		// parse manifest ids to a set
		manIds := mapset.NewSet(strings.Fields(string(manBytes))...)

		// get the updated db.segments
		wantIds := mapset.NewSet[string]()
		db.rw.RLock()
		for _, seg := range db.segments {
			wantIds.Add(fmt.Sprintf("%d", seg.id))
		}
		db.rw.RUnlock()

		if !manIds.Equal(wantIds) {
			t.Fatalf("manifest ids %v, want %v", manIds, wantIds)
		}
	})
}

func TestMergeRollbackOnError(t *testing.T) {
	synctest.Run(func() {
		var dir string
		var db *DB
		db, dir, _ = SetupTempDB(t,
			WithRolloverThreshold(20),
			WithMergeThreshold(2),
			WithMergeEnabled(true),
			WithOnMergeStart(func() {
				// Close the first segment file so scanning fails mid-merge
				if err := db.segments[0].file.Close(); err != nil {
					t.Fatalf("close segment: %v", err)
				}
			}),
		)

		// Create two inactive segments then trigger merge.
		_ = db.Set("k1", "v1")
		_ = db.Set("k2", "v2") // seg001 rollover
		_ = db.Set("k3", "v3")
		_ = db.Set("k4", "v4") // seg002 rollover -> triggers merge

		// Record pre-merge segment ids.
		db.rw.RLock()
		wantIDs := make([]int, len(db.segments))
		for i, seg := range db.segments {
			wantIDs[i] = seg.id
		}
		db.rw.RUnlock()

		synctest.Wait() // wait for merge attempt

		var mergeErr error
		select {
		case mergeErr = <-db.MergeErrors():
		default:
			t.Fatalf("expected merge error but none")
		}
		if mergeErr == nil {
			t.Fatalf("merge error nil")
		}

		// Ensure segments unchanged.
		db.rw.RLock()
		if len(db.segments) != len(wantIDs) {
			db.rw.RUnlock()
			t.Fatalf("segment count changed: got %d want %d", len(db.segments), len(wantIDs))
		}
		for i, seg := range db.segments {
			if seg.id != wantIDs[i] {
				db.rw.RUnlock()
				t.Fatalf("segment id %d changed: got %d want %d", i, seg.id, wantIDs[i])
			}
		}
		db.rw.RUnlock()

		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("readdir: %v", err)
		}
		files := mapset.NewSet[string]()
		for _, e := range entries {
			files.Add(e.Name())
		}
		wantFiles := mapset.NewSet[string]("MANIFEST")
		for _, id := range wantIDs {
			wantFiles.Add(fmt.Sprintf("seg%03d", id))
		}
		if !files.Equal(wantFiles) {
			t.Fatalf("unexpected files after failed merge: %v, want %v", files, wantFiles)
		}
	})
}

func TestMergeHandlesDeletedKeys(t *testing.T) {
	synctest.Run(func() {
		db, _, _ := SetupTempDB(t,
			WithRolloverThreshold(20),
			WithMergeThreshold(2),
			WithMergeEnabled(true),
		)

		_ = db.Set("k1", "old")
		_ = db.Set("k2", "val") // rollover to seg002
		_ = db.Delete("k1")
		_ = db.Delete("k2") // rollover to seg003 + merge trigger

		synctest.Wait() // wait for merge to complete

		// Verify both keys are deleted after merge
		if _, err := db.Get("k1"); !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("expected k1 to be deleted after merge, got %v", err)
		}
		if _, err := db.Get("k2"); !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("expected k2 to be deleted after merge, got %v", err)
		}

		// We expect 2 segments total (1 merged + 1 active, both with no data)
		if len(db.segments) != 2 {
			t.Errorf("expected 2 segments after merging all deletions, got %d", len(db.segments))
		}

		// Verify merged segment is empty
		if seg := db.segments[0]; seg.size > 0 {
			t.Errorf("merged segment %d should be empty but has size %d", seg.id, seg.size)
		}

		// Verify no data remains in merge segment by checking total disk size
		totalSize, err := db.DiskSize()
		if err != nil {
			t.Fatalf("DiskSize failed: %v", err)
		}

		if totalSize > 0 {
			t.Errorf("expected no disk usage after deleting everything, got %d bytes", totalSize)
		}

	})
}
