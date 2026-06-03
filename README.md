# benchq - Benchmark Results: cascadeq vs go-dsqueue

Some LLM-generated benchmarks to compare [`cascadeq`](https://github.com/gammazero/cascadeq) and [`go-dsqueue`](https://github.com/ipfs/go-dsqueue) persistent queues.

## Production configuration (how kubo uses dsqueue)

The config above forces almost every item to disk with a 4-item buffer and no dedup. That
is not how `go-dsqueue` is used. Its main consumer is kubo's provide queue, which has two
paths:

- **Default** (`Provide.DHT.SweepEnabled=true`): the `go-libp2p-kad-dht` buffered
  `SweepingProvider`. dsqueue at `/provider/bprov`, drained with `GetN(1024)`
  (`buffered.DefaultBatchSize`), dedup off.
- **Legacy** (`Provide.DHT.SweepEnabled=false`, or HTTP-only routing): boxo's
  `provider/reprovider.go`. dsqueue at `/provider/dsq-provide`, drained item-by-item via
  `Out()`, dedup 2048.

Both keep the default 16K buffer. The root datastore is **LevelDB by default**, or
Pebble/Badger via init profile. LevelDB fsyncs every write (`syncWrites=true`), Pebble
does not (`pebble.NoSync`); the kubo plugins keep these defaults.

**The headline changes once the config is real.** At kubo's default (`GetN(1024)`)
cascadeq is about 5 to 8x faster, not the 100 to 1000x the forced-to-disk config shows.
The huge gap is only the legacy `Out()` path. Per-item cost draining a 20k backlog after a
restart:

| drain path (20k backlog) | per item | cascadeq faster |
|---|---|---|
| cascadeq | 0.9 µs | baseline |
| `GetN(16384)` Pebble | 2.2 µs | ~2x |
| `GetN(16384)` LevelDB | 3.1 µs | ~3x |
| `GetN(1024)` Pebble (default) | 4.7 µs | ~5x |
| `GetN(1024)` LevelDB (default) | 6.8 µs | ~8x |
| `Out()` Pebble (legacy) | 1.26 ms | ~1400x |
| `Out()` LevelDB (legacy) | 2.6 ms | ~2900x |

### Environment

| | |
|---|---|
| Machine | 32-core x86_64 Linux, NVMe SSD |
| Go | 1.26.3 |
| cascadeq | v0.0.2 |
| go-dsqueue | v0.2.0 |
| go-ds-leveldb | v0.5.2 |
| go-ds-pebble | v0.5.11 |

### Why Out() is slow

`Out()` re-runs `Query{Limit:1}` and deletes from the front for every item, so each read
scans past a growing run of tombstones that compaction has not cleared yet. This is not
the `OrderByKey` query (every sorted KV backend serves that from its native iterator); it
is tombstone buildup from per-item deletes, and Pebble hits it too:

| backlog | `Out()` Pebble | `GetN(1024)` Pebble | `GetN(16384)` Pebble | cascadeq |
|---|---|---|---|---|
| 5k | 199 µs | 2.1 µs | 2.3 µs | 0.6 µs |
| 20k | 1265 µs | 4.7 µs | 2.2 µs | 0.9 µs |
| 40k | 1678 µs | 6.2 µs | 2.6 µs | 0.7 µs |

`GetN` spreads the tombstone scan over the batch. The default 1024 keeps the cost in
microseconds but it still creeps up with backlog. A bigger batch (16384) keeps it flat.
cascadeq has no tombstones: it reads items back in order from its own files.

### Related: the keystore already hit this

The same tombstone cost was already fixed for the DHT provider *keystore* (the multihashes
scheduled for reproviding), a separate datastore from the queue. Each reset deleted every
key one by one, leaving tombstones that grew the datastore until compaction
(`ipfs/kubo#11096`). The fix (`libp2p/go-libp2p-kad-dht#1233`, `ipfs/kubo#11198`) gives the
keystore its own datastore per reset and drops the whole directory with `os.RemoveAll`
instead of per-key deletes. Different component, same root cause: mass point-deletes on an
LSM.

### Steady state

When the queue stays shallow (it drains about as fast as it fills) nothing reaches disk,
and all three are within ~1.5x:

| cascadeq | dsqueue LevelDB | dsqueue Pebble |
|---|---|---|
| 2.2 µs | 3.2 µs | 3.0 µs |

## Worst-case (for go-dsqueue) Comparison

The configuration below deliberately forces nearly every item through the backing store, which maximises the contrast but is not how `go-dsqueue` is actually driven by its main consumer (kubo's provide queue). This  example demonstrates that it is possible to configure go-dsqueue in a way that negatively affects its performance in a way that cascadeq is not as suceptible to. This is **NOT A REAL WORKLOAD EXAMPLE*.

### Configuration

Both queues are configured to force the majority of data to disk.

| Parameter | cascadeq | dsqueue |
|---|---|---|
| Memory cap | `WithMaxMemory(512)` | — |
| Item cap | `WithMaxMemItems(8)` | — |
| Disk flush threshold | ~4 items per half-queue | `WithBufferSize(4)` |
| Backing store | numbered `.dat` files | LevelDB |
| Item size | 128 bytes | 128 bytes |

With these settings, fewer than 8 items remain in cascadeq's memory at any time
and dsqueue's inBuf flushes to LevelDB every 4 items.

### Results

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

### Summary

| Benchmark | cascadeq | dsqueue | ratio (cq faster) |
|---|---|---|---|
| Put | 7,605 ns/op (16.83 MB/s) | 7,554,341 ns/op (0.02 MB/s) | ~993× |
| Get | 69,376 ns/op (1.85 MB/s) | 10,810,252 ns/op (0.01 MB/s) | ~156× |
| PutGet | 8,254 ns/op (15.51 MB/s) | 7,231,143 ns/op (0.02 MB/s) | ~876× |

### What each benchmark measures

- **Put** — enqueue throughput: the main goroutine puts b.N items while a
  background goroutine drains the queue. ns/op reflects producer latency.
- **Get** — dequeue throughput from a disk-heavy queue: uses a chunk-based
  fill (untimed) / drain (timed) cycle so Go's benchmark calibration stays
  bounded. Each 500-item chunk has >99% of items on disk before the timed
  drain begins.
- **PutGet** — end-to-end throughput: the main goroutine enqueues and is
  timed until the last item is consumed by a background goroutine.

### Analysis

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

## Takeaway

At kubo's default, the queue drains in batches via `GetN`, so cascadeq is only ~5 to 8x faster and
both sit in the microseconds. The one slow spot left is the legacy boxo provider on a
large backlog (milliseconds per item, growing with size); it is fixable in place by
switching it to `GetN`, no library change and the queue stays in the pluggable datastore.
cascadeq is still a bit faster and allocates less, but at the batch sizes kubo uses the gap
is microseconds, not milliseconds.

 - The ~100 to 1000x for the (unrealiztic) worst-case configuration is the legacy `Out()` path at a worst-case config.
 - For a realistic configuration, the difference between `go-dsqueue` and `cacadeq` is microseconds: not meaningfully significant.
