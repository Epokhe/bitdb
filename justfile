# Note: I added this to Hetzner to run tests on an attached volume
# (because of fsync problem on local disk)
# export LSMTREE_TMPDIR=/mnt/HC_Volume_102592592/tmp

tempdir := env("LSMTREE_TMPDIR", "/tmp")

foo:
    @echo {{tempdir}}

# run tests
test *ARGS:
    TMPDIR={{tempdir}} go test {{ARGS}} ./core/

# run a single test
testsingle NAME *ARGS:
    TMPDIR={{tempdir}} go test {{ARGS}} ./core/ -run '^{{NAME}}$'

# run benchmarks
bench *ARGS:
    TMPDIR={{tempdir}} go test {{ARGS}} ./core/ -run='^$' -bench=Benchmark_ -benchmem

# run a single benchmark
benchsingle NAME *ARGS:
    TMPDIR={{tempdir}} go test {{ARGS}} ./core/ -run='^$' -bench={{NAME}} -benchmem

# Run profiler and serve the result on http
profile NAME *ARGS:
    TMPDIR={{tempdir}} go test {{ARGS}} ./core/ -run='^$' -bench={{NAME}} -cpuprofile cpu.out
    go tool pprof -http=0.0.0.0:1730 ./core.test cpu.out

# sync to remote
sync:
    rsync -avzh --exclude .git ../lsm-tree overseer:~/

# lint with golangci-lint
lint:
    golangci-lint run ./...
