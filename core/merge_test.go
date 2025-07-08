//go:build goexperiment.synctest

package core

import (
	"fmt"
	"testing"
	"testing/synctest"
)

// TestMergeRunsOnlyWhenThresholdExceeded ensures we do NOT merge prematurely,
// then checks we merge when threshold is crossed.
func TestMergeRunsOnlyWhenThresholdExceeded(t *testing.T) {
	synctest.Run(func() {
		_, db := SetupTempDB(t,
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
		_, db := SetupTempDB(t,
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
		_, db := SetupTempDB(t,
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
		want := [4]int{5, 6, 7, 4}

		for i, seg := range db.segments {
			if seg.id != want[i] {
				t.Fatalf("expected seg id %d, got %d", want, seg.id)
			}
		}
	})
}

// TestMergeWhileWriting checks that writes performed while a merge is running
// are preserved and that segments created after the merge begins remain unmerged.
// todo this test may fail because we're not making merge wait...
func TestMergeWhileWriting(t *testing.T) {
	synctest.Run(func() {
		_, db := SetupTempDB(t,
			WithRolloverThreshold(20),
			WithMergeThreshold(1),
			WithMergeEnabled(true),
		)

		// Each Set operation adds 8 bytes header + len(key) + len(value).
		_ = db.Set("a", "1")
		_ = db.Set("b", "2") // Segment rollover. triggers merge

		// merge is running, let's do more sets
		_ = db.Set("a", "3")
		_ = db.Set("b", "4") // Segment rollover. can't trigger merge
		_ = db.Set("a", "5")
		_ = db.Set("b", "6") // Segment rollover. can't trigger merge

		// currently 4 segments and merge is running.
		// but merge won't merge shit, so we will have the same segments.

		println(len(db.segments))
		synctest.Wait()

		if v, _ := db.Get("a"); v != "5" {
			t.Fatalf("want a=6, got %q", v)
		}
		if v, _ := db.Get("b"); v != "6" {
			t.Fatalf("want b=2, got %q", v)
		}

		println(len(db.segments))

		if got := len(db.segments); got < 3 {
			t.Fatalf("expected at least 3 segments, got %d", got)
		}
	})
}

// TestMergeMultiRecordSegments verifies merging segments that hold multiple
// records each keeps the latest values.
func TestMergeMultiRecordSegments(t *testing.T) {
	synctest.Run(func() {
		_, db := SetupTempDB(t,
			WithRolloverThreshold(20),
			WithMergeThreshold(1),
			WithMergeEnabled(true),
		)

		_ = db.Set("k1", "v1")
		_ = db.Set("k2", "v2")
		_ = db.Set("k1", "v3")
		_ = db.Set("k3", "v3")
		_ = db.Set("k4", "v4")

		synctest.Wait()

		if v, _ := db.Get("k1"); v != "v3" {
			t.Fatalf("want k1=v3, got %q", v)
		}
		if v, _ := db.Get("k2"); v != "v2" {
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
		_, db := SetupTempDB(t,
			WithRolloverThreshold(20),
			WithMergeThreshold(1),
			WithMergeEnabled(false),
		)

		// Each Set operation adds 8 bytes header + len(key) + len(value).
		// Segment size limit is 20 bytes.
		for i := 0; i < 6; i++ {
			k := fmt.Sprintf("k%d", i)
			_ = db.Set(k, "v") // Segment rollover every 2 sets.
		}

		synctest.Wait()

		if got := len(db.segments); got != 4 {
			t.Fatalf("expected 4 segments without merge, got %d", got)
		}
		want := [4]int{1, 2, 3, 4}
		for i, seg := range db.segments {
			if seg.id != want[i] {
				t.Fatalf("expected seg id %d, got %d", want, seg.id)
			}
		}
	})
}
