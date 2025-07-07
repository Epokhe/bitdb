# BitDB

BitDB is a lightweight key/value store written in Go. It is a toy project that I built while learning Go. It follows a [Bitcask](https://riak.com/riakkv/latest/learn/concepts/bitcask/) style architecture:

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

Use the `justfile` to run tests:

```bash
just test
```

Test with race detector enabled:

```bash
just testrace
```

The `justfile` also contains commands for benchmarks and profiling.

