//go:build goexperiment.synctest
// +build goexperiment.synctest

package core

import (
	"testing"
	"testing/synctest"
)

// TestMergeRunsOnlyWhenThresholdExceeded ensures we do NOT merge prematurely,
// then forces a merge by limiting both segment size and threshold
func TestMergeRunsOnlyWhenThresholdExceeded(t *testing.T) {
	synctest.Run(func() {
		_, db := SetupTempDb(t,
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
		_, db := SetupTempDb(t,
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
		_, db := SetupTempDb(t,
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
