//go:build goexperiment.synctest

package core

import (
	"errors"
	"fmt"
	"io"
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

		// Each Set operation adds 12 bytes (8 bytes header + 2 bytes key + 2 bytes value).
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

		// Each Set operation adds 8 bytes header + len(key) + len(value).
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

		// Each Set operation adds 8 bytes header + len(key) + len(value).
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

		// Verify that only ONE merge ran by checking the final segment IDs.
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

		// Each Set operation adds 8 bytes header + len(key) + len(value).
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
			// Use a threshold that is more sensitive to write length changes.
			// With a threshold of 23, a writeLen of 11 results in 3 writes/seg,
			// while a writeLen of 12 results in 2 writes/seg.
			rolloverThreshold = 23
			mergeThreshold    = 2
			overhead          = 8 // 4B keyLen + 4B valLen
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
				// We use the segment's tracked size instead of calling Stat for efficiency.
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

		// k2, the truncated record, is truncated on disk
		// but its location lives in the index. We expect an EOF here.
		if v, err := db.Get("k2"); !errors.Is(err, io.EOF) {
			t.Fatalf("expected k2 to be lead to EOF, but got value: %q %v", v, err)
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
