package core

import (
	"fmt"
	"testing"
)

func Benchmark_Get(b *testing.B) {
	db, _, _ := SetupTempDB(b, WithMergeEnabled(false))

	// preload some keys so Get has something to fetch
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("k%04d", i)
		_ = db.Set(key, "v")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// try to fetch every key, because retrieval time will differ
		// depending on which segment they are in
		//key := fmt.Sprintf("k%04d", i%10000)
		key := "k0050"
		if _, err := db.Get(key); err != nil {
			b.Fatalf("db.get: %v", err)
		}
	}
}

func Benchmark_Set(b *testing.B) {
	db, _, _ := SetupTempDB(b, WithMergeEnabled(false))

	// Run the timed loop of b.N Set calls
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("k%04d", i%10000)
		if err := db.Set(key, "value"); err != nil {
			b.Fatalf("db.set: %v", err)
		}
	}
}

func Benchmark_Fsync_Set(b *testing.B) {
	db, _, _ := SetupTempDB(b, WithFsync(true), WithMergeEnabled(false))

	// Run the timed loop of b.N Set calls
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("k%04d", i%10000)
		if err := db.Set(key, "value"); err != nil {
			b.Fatalf("db.set: %v", err)
		}
	}
}

func Benchmark_Merge(b *testing.B) {
	const (
		rollover        = 1024 // 1KB segments
		mergeThreshold  = 5    // start merge after 5 inactive segments
		recordsPerBatch = 50   // writes per segment to exceed the threshold
	)

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db, _, cleanup := SetupTempDB(b,
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

		// cleanup the db at the end of each iteration
		cleanup()
	}
}
