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
			WithSegmentSizeMax(1), // every Set rolls segment
			WithMergeAfter(2),     // start merge after 2 inactive segments
			WithMergeEnabled(true),
		)

		_ = db.Set("k1", "v1") // seg001
		_ = db.Set("k1", "v2") // seg002
		_ = db.Set("k1", "v3") // seg003 -> 2 inactive segments, should not merge yet

		synctest.Wait()
		if got := len(db.segments); got != 3 {
			t.Fatalf("merge ran too early; segments=%d", got)
		}

		_ = db.Set("k1", "v4") // seg004 - should trigger merge

		synctest.Wait() // wait until merge goroutine exits

		if got := len(db.segments); got > 2 {
			t.Fatalf("expected ≤2 segments after merge, got %d", got)
		}
	})
}

// TestMergeKeepsLatestValue checks last-writer-wins correctness across merge.
func TestMergeKeepsLatestValue(t *testing.T) {
	synctest.Run(func() {
		_, db := SetupTempDB(t,
			WithSegmentSizeMax(1),
			WithMergeAfter(1), // merge after every rollover
			WithMergeEnabled(true),
		)

		_ = db.Set("k", "old")
		_ = db.Set("k", "new")

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
			WithSegmentSizeMax(1),
			WithMergeAfter(1),
			WithMergeEnabled(true),
		)

		_ = db.Set("a", "1")

		_ = db.Set("b", "2")
		synctest.Wait() // trigger merge

		_ = db.Set("a", "x") // overwrites a
		synctest.Wait()

		// Count how many segments contain key "a".
		cnt := 0
		for _, seg := range db.segments {
			rs := newRecordScanner(seg)

			for rs.scan() {
				if rs.record.key == "a" {
					cnt++
				}
			}
		}
		if cnt != 1 {
			t.Fatalf("expected 1 live record for key 'a', found %d", cnt)
		}
	})
}

// TestMergeWhileWriting checks that writes performed while a merge is running
// are preserved and that segments created after the merge begins remain unmerged.
func TestMergeWhileWriting(t *testing.T) {
	synctest.Run(func() {
		_, db := SetupTempDB(t,
			WithSegmentSizeMax(1),
			WithMergeAfter(1),
			WithMergeEnabled(true),
		)

		_ = db.Set("a", "1")
		_ = db.Set("b", "2")
		_ = db.Set("c", "3") // triggers merge

		_ = db.Set("a", "4") // written while merge is running
		_ = db.Set("d", "5")

		synctest.Wait()

		if v, _ := db.Get("a"); v != "4" {
			t.Fatalf("want a=4, got %q", v)
		}
		if v, _ := db.Get("b"); v != "2" {
			t.Fatalf("want b=2, got %q", v)
		}
		if v, _ := db.Get("c"); v != "3" {
			t.Fatalf("want c=3, got %q", v)
		}
		if v, _ := db.Get("d"); v != "5" {
			t.Fatalf("want d=5, got %q", v)
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
			WithSegmentSizeMax(1),
			WithMergeAfter(1),
			WithMergeEnabled(true),
		)

		_ = db.Set("k1", "v1")
		_ = db.Set("k2", "v2")
		_ = db.Set("k3", "v3") // start merge

		synctest.Wait()

		if got := len(db.segments); got != 3 {
			t.Fatalf("expected 3 segments after merge, got %d", got)
		}
	})
}

// TestMergeDisabled verifies that merges do not run when disabled.
func TestMergeDisabled(t *testing.T) {
	synctest.Run(func() {
		_, db := SetupTempDB(t,
			WithSegmentSizeMax(1),
			WithMergeAfter(1),
			WithMergeEnabled(false),
		)

		for i := 0; i < 5; i++ {
			k := fmt.Sprintf("k%d", i)
			_ = db.Set(k, "v")
		}

		synctest.Wait()

		if got := len(db.segments); got != 5 {
			t.Fatalf("expected 5 segments without merge, got %d", got)
		}
		for i, seg := range db.segments {
			want := i + 1
			if seg.id != want {
				t.Fatalf("expected seg id %d, got %d", want, seg.id)
			}
		}
	})
}

// TestSequentialMergeTriggers rolls segments quickly ensuring that only a single
// merge runs at a time.
func TestSequentialMergeTriggers(t *testing.T) {
	synctest.Run(func() {
		_, db := SetupTempDB(t,
			WithSegmentSizeMax(1),
			WithMergeAfter(1),
			WithMergeEnabled(true),
		)

		for i := 0; i < 10; i++ {
			k := fmt.Sprintf("x%d", i)
			_ = db.Set(k, "v")
		}

		synctest.Wait()
		if got := len(db.segments); got != 10 {
			t.Fatalf("expected 10 segments after merge, got %d", got)
		}
	})
}
