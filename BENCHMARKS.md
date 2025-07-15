# Benchmarks

Tested on a Hetzner CX22 instance with an attached volume.

Results for `ef4269b50c8435802b33d40d543f18de0453af0c`

```
Benchmark_Get-2         	  426068	      3166 ns/op	      72 B/op	       3 allocs/op
Benchmark_Set-2         	  373412	      3103 ns/op	      66 B/op	       3 allocs/op
Benchmark_Fsync_Set-2   	     133	   8287028 ns/op	     154 B/op	       3 allocs/op
Benchmark_Merge-2       	      15	  93315538 ns/op	  131915 B/op	    2132 allocs/op
Benchmark_Open-2        	      88	  13460656 ns/op	 3112296 B/op	   70150 allocs/op
```

Results for `d6ec8217887fabe679058ccd401e832eaad82e44`

```
Benchmark_Get-2         	  381571	      3195 ns/op	       1 B/op	       1 allocs/op
Benchmark_Set-2         	  379314	      3378 ns/op	      58 B/op	       3 allocs/op
Benchmark_Fsync_Set-2   	     100	  10253050 ns/op	     118 B/op	       3 allocs/op
Benchmark_Merge-2       	      15	  80148333 ns/op	   91941 B/op	    2071 allocs/op
```

Results for `e1e0b2a9631ab39c65e085e82037cc0ecdc11895`

```
Benchmark_Get-2         	  341846	      3302 ns/op	       1 B/op	       1 allocs/op
Benchmark_Set-2         	  326890	      3131 ns/op	      42 B/op	       3 allocs/op
Benchmark_Fsync_Set-2   	     100	  11263236 ns/op	     101 B/op	       3 allocs/op
```

- previous ones included the rpc timing, so the 10x drop is probably due to that
- fsync is obviously not usable like this. we gotta write some batching shit for that to be viable

Results for `38ea150a45a96f39a8583273cd23eb369c9d17c9`

```
Benchmark_RPC_Get-10    	39168	     32176 ns/op	     499 B/op	      18 allocs/op
Benchmark_RPC_Set-10    	37246	     36354 ns/op	     647 B/op	      19 allocs/op
```

Results for `987ec559493fbc62bea7760532ee42733e290483`

```
Benchmark_RPC_Get-10    	4184	    301736 ns/op	  328583 B/op	      24 allocs/op
Benchmark_RPC_Set-10    	36666	     33115 ns/op	     598 B/op	      21 allocs/op
```

