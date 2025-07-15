# Note: I added this to Hetzner to run tests on an attached volume
# (because of fsync problem on local disk)
# export BITDB_TMPDIR=/mnt/HC_Volume_102592592/tmp

tempdir := env("BITDB_TMPDIR", "/tmp")

# Run all tests (or single tests via -run in ARGS)
test *ARGS:
    TMPDIR={{tempdir}} GOEXPERIMENT=synctest go test {{ARGS}} ./core/

# Run with race detector (again, single tests via -run)
testrace *ARGS:
    TMPDIR={{tempdir}} GOEXPERIMENT=synctest go test -race {{ARGS}} ./core/

# Generate coverage report
cover:
    TMPDIR={{tempdir}} GOEXPERIMENT=synctest go test -race -coverprofile=coverage.out --covermode=atomic ./core/

# Open coverage report in browser
cover-report: cover
    go tool cover -html=coverage.out

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
    rsync -avzh --exclude .git ../bitdb overseer:~/

# lint with golangci-lint
lint:
    golangci-lint run ./...
