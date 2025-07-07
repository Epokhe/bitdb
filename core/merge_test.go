//go:build goexperiment.synctest

package core

import (
	"fmt"
	"testing"
	"testing/synctest"
)

// TestMergeRunsOnlyWhenThresholdExceeded ensures we do NOT merge prematurely,
// then forces a merge by limiting both segment size and threshold
func TestMergeRunsOnlyWhenThresholdExceeded(t *testing.T) {
	synctest.Run(func() {
		_, db := SetupTempDB(t,
			WithSegmentSizeMax(20), // multiple records per segment
			WithMergeAfter(2),      // start merge after 2 inactive segments
			WithMergeEnabled(true),
		)

		_ = db.Set("k1", "v1")
		_ = db.Set("k1", "v2")
		_ = db.Set("k1", "v3")
		_ = db.Set("k1", "v4")
		_ = db.Set("k1", "v5") // now 3 segments but below threshold

		synctest.Wait()
		if got := len(db.segments); got != 3 {
			t.Fatalf("merge ran too early; segments=%d", got)
		}

		_ = db.Set("k1", "v6")
		_ = db.Set("k1", "v7") // rolls to 4th segment -> triggers merge

		synctest.Wait() // wait until merge goroutine exits

		if got := len(db.segments); got > 3 {
			t.Fatalf("expected ≤3 segments after merge, got %d", got)
		}
	})
}

// TestMergeKeepsLatestValue checks last-writer-wins correctness across merge.
func TestMergeKeepsLatestValue(t *testing.T) {
	synctest.Run(func() {
		_, db := SetupTempDB(t,
			WithSegmentSizeMax(20),
			WithMergeAfter(1), // merge after every rollover
			WithMergeEnabled(true),
		)

		_ = db.Set("k", "old")
		_ = db.Set("x", "1")
		_ = db.Set("y", "1") // rolls to new segment
		_ = db.Set("k", "new")
		_ = db.Set("z", "1") // trigger merge

		synctest.Wait()
		if v, _ := db.Get("k"); v != "new" {
			t.Fatalf("want new, got %q", v)
		}
	})
}

// TestMergeDropsObsoleteRecords ensures overwritten keys are removed.
func TestMergeDropsObsoleteRecords(t *testing.T) {
	synctest.Run(func() {
		_, db := SetupTempDB(t,
			WithSegmentSizeMax(20),
			WithMergeAfter(1),
			WithMergeEnabled(true),
		)

		_ = db.Set("a", "1")
		_ = db.Set("b", "2")
		_ = db.Set("c", "3")
		_ = db.Set("d", "4")
		synctest.Wait()

		_ = db.Set("a", "x") // rolls to new seg and overwrites
		synctest.Wait()

		if v, _ := db.Get("a"); v != "x" {
			t.Fatalf("want a=x after merge, got %q", v)
		}
	})
}

// TestMergeWhileWriting checks that writes performed while a merge is running
// are preserved and that segments created after the merge begins remain unmerged.
func TestMergeWhileWriting(t *testing.T) {
	synctest.Run(func() {
		_, db := SetupTempDB(t,
			WithSegmentSizeMax(20),
			WithMergeAfter(1),
			WithMergeEnabled(true),
		)

		_ = db.Set("a", "1")
		_ = db.Set("b", "2")
		_ = db.Set("c", "3")
		_ = db.Set("d", "4")
		_ = db.Set("e", "5") // triggers merge

		_ = db.Set("a", "6") // written while merge is running
		_ = db.Set("f", "7")

		synctest.Wait()

		if v, _ := db.Get("a"); v != "6" {
			t.Fatalf("want a=6, got %q", v)
		}
		if v, _ := db.Get("b"); v != "2" {
			t.Fatalf("want b=2, got %q", v)
		}
		if v, _ := db.Get("c"); v != "3" {
			t.Fatalf("want c=3, got %q", v)
		}
		if v, _ := db.Get("d"); v != "4" {
			t.Fatalf("want d=4, got %q", v)
		}
		if v, _ := db.Get("e"); v != "5" {
			t.Fatalf("want e=5, got %q", v)
		}
		if v, _ := db.Get("f"); v != "7" {
			t.Fatalf("want f=7, got %q", v)
		}

		if got := len(db.segments); got < 3 {
			t.Fatalf("expected at least 3 segments, got %d", got)
		}
	})
}

// TestMergeProducesMultipleSegments ensures merge may create more than one
// output segment when the size limit is tiny.
func TestMergeProducesMultipleSegments(t *testing.T) {
	synctest.Run(func() {
		_, db := SetupTempDB(t,
			WithSegmentSizeMax(20),
			WithMergeAfter(1),
			WithMergeEnabled(true),
		)

		for i := 0; i < 6; i++ {
			k := fmt.Sprintf("k%d", i)
			_ = db.Set(k, "v")
		}

		synctest.Wait()

		if got := len(db.segments); got != 3 {
			t.Fatalf("expected 3 segments after merge, got %d", got)
		}
	})
}

// TestMergeMultiRecordSegments verifies merging segments that hold multiple
// records each keeps the latest values.
func TestMergeMultiRecordSegments(t *testing.T) {
	synctest.Run(func() {
		_, db := SetupTempDB(t,
			WithSegmentSizeMax(20),
			WithMergeAfter(1),
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
			WithSegmentSizeMax(20),
			WithMergeAfter(1),
			WithMergeEnabled(false),
		)

		for i := 0; i < 5; i++ {
			k := fmt.Sprintf("k%d", i)
			_ = db.Set(k, "v")
		}

		synctest.Wait()

		if got := len(db.segments); got != 3 {
			t.Fatalf("expected 3 segments without merge, got %d", got)
		}
		for i, seg := range db.segments {
			want := i + 1
			if seg.id != want {
				t.Fatalf("expected seg id %d, got %d", want, seg.id)
			}
		}
	})
}
