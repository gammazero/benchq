# benchq - Benchmark Results: cascadeq vs go-dsqueue

Some LLM-generated benchmarks to compare [`cascadeq`](https://github.com/gammazero/cascadeq) and [`go-dsqueue`](https://github.com/ipfs/go-dsqueue) persistent queues.

> Note: the configuration below deliberately forces nearly every item through the backing store, which maximises the contrast but is not how `go-dsqueue` is actually driven by its main consumer (boxo's provide queue). For results using boxo's real configuration, see [Production configuration (how kubo uses dsqueue)](#production-configuration-how-kubo-uses-dsqueue) below.

## Environment

| | |
|---|---|
| Date | 2026-06-01 |
| Machine | Apple M1 Max |
| OS | darwin 25.5.0 |
| Go | 1.26.3 |
| GOMAXPROCS | 10 |
| cascadeq | v0.0.2 |
| go-dsqueue | v0.2.0 |
| go-ds-leveldb | v0.5.2 |

## Configuration

Both queues are configured to force the majority of data to disk.

| Parameter | cascadeq | dsqueue |
|---|---|---|
| Memory cap | `WithMaxMemory(512)` | — |
| Item cap | `WithMaxMemItems(8)` | — |
| Disk flush threshold | ~4 items per half-queue | `WithBufferSize(4)` |
| Backing store | numbered `.dat` files | LevelDB |
| Item size | 128 bytes | 128 bytes |

With these settings, fewer than 8 items remain in cascadeq's memory at any time
and dsqueue's inBuf flushes to LevelDB every 4 items.  For any realistic b.N
well over 99% of items pass through the backing store.

## Results

```
goos: darwin
goarch: arm64
pkg: github.com/gammazero/benchq
cpu: Apple M1 Max
BenchmarkCascadeQPut-10       	  576967	      7605 ns/op	  16.83 MB/s	     168 B/op	       0 allocs/op
BenchmarkCascadeQGet-10       	   59358	     69376 ns/op	   1.85 MB/s	    2379 B/op	       6 allocs/op
BenchmarkCascadeQPutGet-10    	  416949	      8254 ns/op	  15.51 MB/s	     170 B/op	       0 allocs/op
BenchmarkDSQueuePut-10        	     499	   7554341 ns/op	   0.02 MB/s	    3376 B/op	      34 allocs/op
BenchmarkDSQueueGet-10        	     366	  10810252 ns/op	   0.01 MB/s	    2968 B/op	      39 allocs/op
BenchmarkDSQueuePutGet-10     	     440	   7231143 ns/op	   0.02 MB/s	    3393 B/op	      34 allocs/op
```

## Summary

| Benchmark | cascadeq | dsqueue | ratio (cq faster) |
|---|---|---|---|
| Put | 7,605 ns/op (16.83 MB/s) | 7,554,341 ns/op (0.02 MB/s) | ~993× |
| Get | 69,376 ns/op (1.85 MB/s) | 10,810,252 ns/op (0.01 MB/s) | ~156× |
| PutGet | 8,254 ns/op (15.51 MB/s) | 7,231,143 ns/op (0.02 MB/s) | ~876× |

## What each benchmark measures

- **Put** — enqueue throughput: the main goroutine puts b.N items while a
  background goroutine drains the queue. ns/op reflects producer latency.
- **Get** — dequeue throughput from a disk-heavy queue: uses a chunk-based
  fill (untimed) / drain (timed) cycle so Go's benchmark calibration stays
  bounded. Each 500-item chunk has >99% of items on disk before the timed
  drain begins.
- **PutGet** — end-to-end throughput: the main goroutine enqueues and is
  timed until the last item is consumed by a background goroutine.

## Analysis

**cascadeq** batches items into sequentially numbered binary files (one file
per ~4 items with these settings). Each file write is a single `os.Rename`
of a fully-written temp file; each file read loads all items into headQ in
one pass, amortising the per-file syscall cost over multiple dequeues.

**dsqueue** stores each item as a LevelDB key (the item payload is
base64-encoded into the key itself; the value is empty). Dequeuing via
`Out()` requires one LevelDB range query and one delete per item. Under these
settings the `go-ds-leveldb` / `syndtr/goleveldb` write path appears to issue
an `fsync` on each delete, resulting in ~7–10 ms of latency per item on macOS
— consistent with the ~5–15 ms cost of a synchronous `fsync` on Apple SSDs.

## Production configuration (how kubo uses dsqueue)

The configuration above forces almost every item through the backing store with a
4-item dsqueue buffer and no dedup, which maximises the contrast but is not how boxo
drives the queue. boxo's `provider/reprovider.go` constructs it as:

```go
dsqueue.New(ds, "provide", dsqueue.WithDedupCacheSize(2048))
```

That is the default 16K input buffer plus a 2048-entry dedup cache, on the repo's root
datastore. In kubo that root datastore is **LevelDB by default**, or Pebble/Badger when
the matching init profile is used. LevelDB writes are synchronous (`syncWrites=true`, an
`fsync` per `Put`/`Delete`); Pebble uses `pebble.NoSync` (no per-write `fsync`); the kubo
plugins do not override either. The benchmarks in `prod_bench_test.go` use that config,
add Pebble alongside LevelDB, and add the scenario the provide queue actually hits:
draining a large persisted backlog after a restart.

### Environment

| | |
|---|---|
| Machine | 32-core x86_64 Linux, NVMe SSD |
| Go | 1.26.3 |
| cascadeq | v0.0.2 |
| go-dsqueue | v0.2.0 |
| go-ds-leveldb | v0.5.2 |
| go-ds-pebble | v0.5.11 |

### Restart with a persisted backlog

A node restarts (or a burst of adds overflows the buffer), leaving a backlog in the
backing store that is then drained. boxo drains item-by-item via `Out()`. `go-dsqueue`
also ships `GetN(n)`, which issues one ordered query and one batched delete per call.

Per-item cost draining a 20k backlog:

| drain method | LevelDB | Pebble | cascadeq |
|---|---|---|---|
| `Out()` (item-by-item, what boxo does today) | 2.6 ms | 1.26 ms | n/a |
| `GetN(16384)` (batched) | 3.1 µs | 2.2 µs | n/a |
| cascadeq `Out()` | n/a | n/a | 0.9 µs |

The `Out()` path gets slower as the backlog grows, because `go-dsqueue` re-issues a
`Query{Limit:1}` and point-deletes from the front for every item, so each head read
scans past an ever-growing run of not-yet-compacted tombstones. This is not the
`OrderByKey` query (every sorted KV backend serves that from its native iterator); it is
tombstone accumulation under per-item front deletion, and it affects Pebble too:

| backlog | `Out()` Pebble | `GetN(16384)` Pebble | cascadeq |
|---|---|---|---|
| 5k | 199 µs | 2.3 µs | 0.6 µs |
| 10k | 881 µs | 1.5 µs | n/a |
| 20k | 1265 µs | 2.2 µs | 0.9 µs |
| 40k | 1678 µs | 2.6 µs | 0.7 µs |

Draining via `GetN` amortises the tombstone scan over the batch, flattening the cost to a
few microseconds per item regardless of backlog size (~600 to 1000x faster than per-item
`Out()`) and staying within ~3-4x of cascadeq. cascadeq avoids the tombstones entirely by
reading items back in order from its own sequential files.

### Steady state

When the queue stays shallow (it drains about as fast as it fills) items never reach
disk, and all three are within ~1.5x of each other:

| cascadeq | dsqueue LevelDB | dsqueue Pebble |
|---|---|---|
| 2.2 µs | 3.2 µs | 3.0 µs |

### Takeaway

At the forced-to-disk config the gap looks like ~100 to 1000x, but most of that is the
per-item `fsync` on LevelDB plus the 4-item buffer. At boxo's config the only place
`go-dsqueue` is genuinely slow is draining a large persisted backlog item-by-item, and
that is fixable in place by draining via `GetN` instead of `Out()`, with no library change
and the queue staying inside the pluggable datastore. cascadeq remains modestly faster and
allocates less, but at boxo's config the difference is microseconds, not milliseconds.
