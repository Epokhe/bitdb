package core

import (
	"fmt"
	"testing"
)

func Benchmark_Merge(b *testing.B) {
	const (
		rollover        = 1024 // 1KB segments
		mergeThreshold  = 5    // start merge after 5 inactive segments
		recordsPerBatch = 50   // writes per segment to exceed the threshold
	)

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		_, db := SetupTempDB(b,
			WithRolloverThreshold(rollover),
			WithMergeThreshold(mergeThreshold),
			WithMergeEnabled(false),
		)

		for seg := 0; seg < mergeThreshold; seg++ {
			for r := 0; r < recordsPerBatch; r++ {
				key := fmt.Sprintf("key%03d%02d", seg, r)
				val := fmt.Sprintf("val%03d%02d", seg, r)
				if err := db.Set(key, val); err != nil {
					b.Fatalf("set: %v", err)
				}
			}
		}

		b.StartTimer()
		if err := db.merge(); err != nil {
			b.Fatalf("merge: %v", err)
		}
	}
}
