# run tests
test *ARGS:
    go test {{ARGS}} ./core/

# run benchmarks
bench *ARGS:
    go test {{ARGS}} ./cmd/server -run='^$' -bench=Benchmark_RPC_ -benchmem

# run single benchmark
benchsingle NAME *ARGS:
    go test {{ARGS}} ./cmd/server -run='^$' -bench={{NAME}} -benchmem

