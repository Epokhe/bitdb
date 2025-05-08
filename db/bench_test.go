package db

import (
	"fmt"
	"net/rpc"
	"testing"
)

func Benchmark_RPC_Get(b *testing.B) {
	_, db := setupTempDb(b)

	// start the RPC server on that file
	addr, cleanup, err := StartRPC(db, ":1234")
	if err != nil {
		b.Fatalf("start server: %v", err)
	}
	defer cleanup()

	// preload some keys so Get has something to fetch
	client, _ := rpc.Dial("tcp", addr)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("k%04d", i)
		client.Call("DB.Set", &SetArgs{Key: key, Val: "v"}, new(struct{}))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		client.Call("DB.Get", &GetArgs{Key: "k0050"}, new(string))
	}
}

func Benchmark_RPC_Set(b *testing.B) {
	_, db := setupTempDb(b)

	// start the RPC server on that file
	addr, cleanup, err := StartRPC(db, ":1234")
	if err != nil {
		b.Fatalf("start server: %v", err)
	}
	defer cleanup()

	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		b.Fatalf("dial: %v", err)
	}

	// Run the timed loop of b.N Set calls
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("k%08d", i)
		// we ignore the reply (empty struct)
		if err := client.Call("DB.Set", &SetArgs{Key: key, Val: "value"}, new(struct{})); err != nil {
			b.Fatalf("Set RPC failed: %v", err)
		}
	}
}
