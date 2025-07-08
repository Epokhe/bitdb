//go:build goexperiment.synctest

package core

import (
    "fmt"
    "math"
    "testing"
    "testing/synctest"
)

// TestMultipleSequentialMerges triggers several merges and verifies the final
// segment count matches the mathematical expectation.
func TestMultipleSequentialMerges(t *testing.T) {
    synctest.Run(func() {
        const (
            rollover       = int64(20)
            mergeThreshold = 2
        )

        key := "k1"
        baseVal := "v"

        // recordSize is 8 bytes header plus key and value lengths.
        recordSize := int64(8 + len(key) + len(baseVal))
        writesPerSeg := int(math.Ceil(float64(rollover) / float64(recordSize)))

        var mergeCount int
        _, db := SetupTempDB(t,
            WithRolloverThreshold(rollover),
            WithMergeThreshold(mergeThreshold),
            WithMergeEnabled(true),
            WithOnMergeStart(func() { mergeCount++ }),
        )

        // Create enough writes to trigger multiple merges. We want to end up
        // creating five segments in total which means four rollovers.
        numSegments := 5
        totalWrites := writesPerSeg * (numSegments - 1)
        for i := 0; i < totalWrites; i++ {
            _ = db.Set(key, fmt.Sprintf("%s%d", baseVal, i))
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
        if got := len(db.segments); got != 2 {
            t.Fatalf("expected 2 segments after merge, got %d", got)
        }
    })
}

