# run tests
test *ARGS:
    go test {{ARGS}} ./core/

# run benchmarks
bench *ARGS:
    go test {{ARGS}} ./core/ -run='^$' -bench=Benchmark_RPC_ -benchmem

# run single benchmark
benchsingle NAME *ARGS:
    go test {{ARGS}} ./core/ -run='^$' -bench={{NAME}} -benchmem

