package core

import (
	"fmt"
	"testing"
)

func Benchmark_RPC_Get(b *testing.B) {
	_, db := SetupTempDb(b)

	// preload some keys so Get has something to fetch
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("k%04d", i)
		db.Set(key, "v")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// try to fetch every key, because retrieval time will differ
		// depending on which segment they are in
		key := fmt.Sprintf("k%04d", i%10000)
		if _, err := db.Get(key); err != nil {
			b.Fatalf("db.get: %v", err)
		}
	}
}

func Benchmark_RPC_Set(b *testing.B) {
	_, db := SetupTempDb(b)

	// Run the timed loop of b.N Set calls
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("k%04d", i%10000)
		if err := db.Set(key, "value"); err != nil {
			b.Fatalf("db.set: %v", err)
		}
	}
}
