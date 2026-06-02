# benchq - Benchmark Results: cascadeq vs go-dsqueue

Some LLM-generated benchmarks to compare [`cascadeq`](https://github.com/gammazero/cascadeq) and [`go-dsqueue`](https://github.com/ipfs/go-dsqueue) persistent queues.

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
