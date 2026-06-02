package main

import (
	"sync"
	"testing"

	"github.com/gammazero/cascadeq"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/ipfs/go-dsqueue"
)

// ── constants ────────────────────────────────────────────────────────────────

const (
	// benchItemSize is the payload size per item.
	benchItemSize = 128

	// cascadeq memory limits.
	// run() halves maxMemBytes and maxMemItems internally (one half each for
	// headQ and tailQ). With these values each half-queue holds at most ~2
	// items before spilling to a numbered .dat file on disk.
	cqMaxMem   = 512
	cqMaxItems = 8

	// dsqueue flushes inBuf to LevelDB when it reaches this many entries.
	// With b.N >> 4, the vast majority of items are written to LevelDB.
	dsqBufSize = 4

	// getDiskChunk is the number of items enqueued (untimed) then dequeued
	// (timed) per round of the Get benchmarks.  It is large enough relative
	// to the in-memory capacities (cqMaxItems=8, dsqBufSize=4) that >99% of
	// items in each round must pass through the backing store.
	getDiskChunk = 500
)

// benchItem is the shared payload written on every Put call.
var benchItem = func() []byte {
	b := make([]byte, benchItemSize)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}()

// ── helper constructors ───────────────────────────────────────────────────────

func newCascadeQ(b *testing.B) *cascadeq.Queue {
	b.Helper()
	q, err := cascadeq.New("bench", b.TempDir(),
		cascadeq.WithMaxMemory(cqMaxMem),
		cascadeq.WithMaxMemItems(cqMaxItems),
	)
	if err != nil {
		b.Fatal(err)
	}
	return q
}

// newDSQueue returns a dsqueue backed by a fresh LevelDB datastore and a
// cleanup function that closes both.
func newDSQueue(b *testing.B) (*dsqueue.DSQueue, func()) {
	b.Helper()
	ds, err := leveldb.NewDatastore(b.TempDir(), nil)
	if err != nil {
		b.Fatal(err)
	}
	q := dsqueue.New(ds, "bench", dsqueue.WithBufferSize(dsqBufSize))
	return q, func() { q.Close(); ds.Close() }
}

// drainCascadeQ reads exactly n items from q in a background goroutine.
func drainCascadeQ(q *cascadeq.Queue, n int) *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Go(func() {
		for got := 0; got < n; {
			select {
			case <-q.Out():
				got++
			case <-q.Done():
				return
			}
		}
	})
	return &wg
}

// drainDSQueue reads exactly n items from q in a background goroutine.
func drainDSQueue(q *dsqueue.DSQueue, n int) *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Go(func() {
		for got := 0; got < n; {
			if _, ok := <-q.Out(); !ok {
				return
			}
			got++
		}
	})
	return &wg
}

// ── cascadeq benchmarks ──────────────────────────────────────────────────────

// BenchmarkCascadeQPut measures enqueue throughput with forced disk overflow.
// A background goroutine drains the queue so the producer never stalls on
// backpressure.  Memory limits are tiny (cqMaxMem=512 B), so after the first
// ~4 items per half-queue every subsequent item overflows to a numbered .dat
// file on disk.
func BenchmarkCascadeQPut(b *testing.B) {
	q := newCascadeQ(b)
	defer q.Close()
	b.SetBytes(benchItemSize)

	wg := drainCascadeQ(q, b.N)

	b.ResetTimer()
	for range b.N {
		if err := q.Put(benchItem); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	wg.Wait()
}

// BenchmarkCascadeQGet measures dequeue throughput from a disk-heavy queue.
//
// Each round (getDiskChunk items):
//   - Enqueue getDiskChunk items while the timer is stopped — with the tiny
//     memory cap only ~8 items stay in RAM, the rest go to .dat files.
//   - Drain getDiskChunk items while the timer runs — the majority of reads
//     load items from those .dat files.
//
// This chunk pattern keeps the timed portion linear in b.N (avoiding the
// unbounded b.N growth that plagues benchmarks with large untimed setup).
func BenchmarkCascadeQGet(b *testing.B) {
	q := newCascadeQ(b)
	defer q.Close()
	b.SetBytes(benchItemSize)

	b.ResetTimer()
	for got := 0; got < b.N; {
		chunk := min(getDiskChunk, b.N-got)

		b.StopTimer()
		for range chunk {
			if err := q.Put(benchItem); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()

		for i := 0; i < chunk; {
			select {
			case <-q.Out():
				i++
				got++
			case <-q.Done():
				b.Fatalf("queue closed after %d of %d items", got, b.N)
			}
		}
	}
	b.StopTimer()
}

// BenchmarkCascadeQPutGet measures end-to-end throughput: the benchmark
// goroutine enqueues while a background goroutine dequeues, with disk overflow
// throughout.  ns/op reflects the combined producer + consumer latency.
func BenchmarkCascadeQPutGet(b *testing.B) {
	q := newCascadeQ(b)
	defer q.Close()
	b.SetBytes(benchItemSize)

	wg := drainCascadeQ(q, b.N)

	b.ResetTimer()
	for range b.N {
		if err := q.Put(benchItem); err != nil {
			b.Fatal(err)
		}
	}
	wg.Wait()
	b.StopTimer()
}

// ── dsqueue benchmarks ───────────────────────────────────────────────────────

// BenchmarkDSQueuePut measures enqueue throughput with LevelDB-backed storage.
// A background goroutine drains the queue.  With dsqBufSize=4, the inBuf
// flushes to LevelDB after every 4 items, so essentially all items are written
// to the datastore under any realistic b.N.
func BenchmarkDSQueuePut(b *testing.B) {
	q, cleanup := newDSQueue(b)
	defer cleanup()
	b.SetBytes(benchItemSize)

	wg := drainDSQueue(q, b.N)

	b.ResetTimer()
	for range b.N {
		if err := q.Put(benchItem); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	wg.Wait()
}

// BenchmarkDSQueueGet measures dequeue throughput from a LevelDB-backed queue.
// Uses the same chunk-based fill-drain pattern as BenchmarkCascadeQGet.
// With dsqBufSize=4, after the first item is held as next-to-deliver and up
// to 3 items remain in inBuf, all remaining items in each chunk are committed
// to LevelDB before the timed drain phase begins.
func BenchmarkDSQueueGet(b *testing.B) {
	q, cleanup := newDSQueue(b)
	defer cleanup()
	b.SetBytes(benchItemSize)

	b.ResetTimer()
	for got := 0; got < b.N; {
		chunk := min(getDiskChunk, b.N-got)

		b.StopTimer()
		for range chunk {
			if err := q.Put(benchItem); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()

		for i := 0; i < chunk; {
			item, ok := <-q.Out()
			if !ok {
				b.Fatalf("queue closed after %d of %d items", got, b.N)
			}
			_ = item
			i++
			got++
		}
	}
	b.StopTimer()
}

// BenchmarkDSQueuePutGet measures end-to-end throughput with a background
// consumer and LevelDB-backed storage throughout.
func BenchmarkDSQueuePutGet(b *testing.B) {
	q, cleanup := newDSQueue(b)
	defer cleanup()
	b.SetBytes(benchItemSize)

	wg := drainDSQueue(q, b.N)

	b.ResetTimer()
	for range b.N {
		if err := q.Put(benchItem); err != nil {
			b.Fatal(err)
		}
	}
	wg.Wait()
	b.StopTimer()
}
