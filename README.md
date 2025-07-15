# BitDB

[![build](https://github.com/Epokhe/bitdb/actions/workflows/build.yml/badge.svg)](https://github.com/Epokhe/bitdb/actions/workflows/build.yml)
[![codecov](https://codecov.io/github/Epokhe/bitdb/graph/badge.svg?token=S8W8Z1TZAM)](https://codecov.io/github/Epokhe/bitdb)

BitDB is a lightweight key/value store written in Go. It is a toy project that I worked on while learning Go. It follows
a [Bitcask](https://riak.com/assets/bitcask-intro.pdf) style architecture:

* **Append-only segments** – all writes are appended to the active segment file. Older segments become read-only.
* **In-memory index** – keys are mapped to the segment and byte offset of their latest value for fast reads.
* **Background merging** – old segments can be compacted into new ones to drop obsolete values and reclaim space.

## Running

Run the server with `go run` and point it at a data directory:

```bash
go run ./cmd/server -path ./data
```

Then use the client to set and get keys:

```bash
go run ./cmd/client set foo bar
go run ./cmd/client get foo
```

## Testing

To run tests:

```bash
just test
```

Test with race detector enabled:

```bash
just testrace
```

I run tests with race detector by default now.

## Linting

For lint, we use golangci-lint tool. Run with:

```bash
just lint
```

## Benchmarking/profiling

Results are on BENCHMARKS.md

I'm not doing these on my Mac so I don't wear down my ssd.

Run on hetzner or somewhere else(remember to change TMPDIR as written in justfile)

```bash
# Run all benchmarks
just bench
```

Profiling works by running benchmarks and opening a server.
After running the profiler, just go to $HOST:1730

```bash
# Profile Set
just profile Benchmark_Set
```
