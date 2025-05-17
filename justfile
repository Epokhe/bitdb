# run tests
test:
    go test ./core/

# run benchmarks
bench:
    go test ./cmd/server -run='^$' -bench=Benchmark_RPC_ -benchmem
