# run tests
test *ARGS:
    go test {{ARGS}} ./core/

# run a single test
testsingle NAME *ARGS:
    go test {{ARGS}} ./core/ -run '^{{NAME}}$'

# run benchmarks
bench *ARGS:
    go test {{ARGS}} ./core/ -run='^$' -bench=Benchmark_RPC_ -benchmem

# run a single benchmark
benchsingle NAME *ARGS:
    go test {{ARGS}} ./core/ -run='^$' -bench={{NAME}} -benchmem

# Run profiler and serve the result on http
profile NAME *ARGS:
    go test {{ARGS}} ./core/ -run='^$' -bench={{NAME}} -cpuprofile cpu.out
    go tool pprof -http=0.0.0.0:1730 ./core.test cpu.out

# sync to remote
sync:
    rsync -avzh --exclude .git ../lsm-tree overseer:~/
