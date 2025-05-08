# Benchmarks

Results for `38ea150a45a96f39a8583273cd23eb369c9d17c9`
```
Benchmark_RPC_Get-10    	2025/05/08 03:23:44 rpc.Serve: accept:accept tcp [::]:1234: use of closed network connection
   37900	     31189 ns/op	     499 B/op	      18 allocs/op
Benchmark_RPC_Set-10    	2025/05/08 03:23:46 rpc.Serve: accept:accept tcp [::]:1234: use of closed network connection
   35313	     34751 ns/op	     653 B/op	      19 allocs/op
```

Results for `987ec559493fbc62bea7760532ee42733e290483`
```
Benchmark_RPC_Get-10    	2025/05/08 03:39:37 rpc.Serve: accept:accept tcp [::]:1234: use of closed network connection
    4184	    301736 ns/op	  328583 B/op	      24 allocs/op
Benchmark_RPC_Set-10    	2025/05/08 03:39:39 rpc.Serve: accept:accept tcp [::]:1234: use of closed network connection
   36666	     33115 ns/op	     598 B/op	      21 allocs/op
```

