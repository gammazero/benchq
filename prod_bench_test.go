package main

// Production-configuration benchmarks.
//
// The original bench_test.go runs go-dsqueue with WithBufferSize(4) and no
// dedup against a single LevelDB backend. That is not how boxo uses it. boxo's
// provider/reprovider.go constructs the queue as:
//
//	dsqueue.New(ds, "provide", dsqueue.WithDedupCacheSize(2048))
//
// i.e. the DEFAULT buffer size (16*1024) and a 2048-entry dedup cache, on the
// repo's root datastore. In kubo that root datastore is LevelDB by default, or
// Pebble/Badger when the matching init profile is used. LevelDB writes are
// synchronous (fsync per Put/Delete); Pebble writes use pebble.NoSync (no
// per-write fsync), matching the kubo levelds and pebbleds plugins.
//
// These benchmarks use that production config and add the scenario that
// actually matters for the provide queue: draining a large persisted backlog
// after a restart.

import (
	"encoding/binary"
	"sync"
	"testing"

	"github.com/gammazero/cascadeq"
	"github.com/ipfs/go-datastore"
	leveldb "github.com/ipfs/go-ds-leveldb"
	pebbleds "github.com/ipfs/go-ds-pebble"
	"github.com/ipfs/go-dsqueue"
)

// prodDedupSize matches boxo provider/reprovider.go WithDedupCacheSize(2048).
const prodDedupSize = 2048

// prodItem returns a unique benchItemSize payload for index i. Items must be
// unique or the dedup cache would drop every repeat (real CIDs are unique;
// dedup only catches re-adds), which would also deadlock a fixed-count drain.
func prodItem(i int) []byte {
	b := make([]byte, benchItemSize)
	binary.BigEndian.PutUint64(b, uint64(i))
	return b
}

// newLevelDB opens a LevelDB datastore with kubo's settings (syncWrites=true,
// hardcoded by go-ds-leveldb; the levelds plugin does not override it).
func newLevelDB(b *testing.B, dir string) datastore.Batching {
	b.Helper()
	ds, err := leveldb.NewDatastore(dir, nil)
	if err != nil {
		b.Fatal(err)
	}
	return ds
}

// newPebble opens a Pebble datastore with kubo's settings (default
// writeOptions = pebble.NoSync; the pebbleds plugin does not override it).
func newPebble(b *testing.B, dir string) datastore.Batching {
	b.Helper()
	ds, err := pebbleds.NewDatastore(dir)
	if err != nil {
		b.Fatal(err)
	}
	return ds
}

func newProdDSQueue(ds datastore.Batching) *dsqueue.DSQueue {
	// Default buffer (16*1024) + dedup 2048, exactly as boxo configures it.
	return dsqueue.New(ds, "provide", dsqueue.WithDedupCacheSize(prodDedupSize))
}

// ── Scenario A: restart-with-backlog drain ───────────────────────────────────
//
// The producer enqueues b.N items, the queue is closed to persist the backlog,
// then a fresh queue is opened (simulating a process restart) and the timed
// section drains all b.N items via Out(). ns/op is the per-item cost of
// draining a persisted backlog of size b.N. This is the path a node hits when
// it restarts holding queued provides, or when a burst of adds overflows the
// in-memory buffer to the backing store. Run with a fixed -benchtime=Nx so all
// backends drain the same backlog size.

func benchDSQueueRestartDrain(b *testing.B, open func(*testing.B, string) datastore.Batching) {
	dir := b.TempDir()
	ds := open(b, dir)
	defer ds.Close()

	q := newProdDSQueue(ds)
	for i := range b.N {
		if err := q.Put(prodItem(i)); err != nil {
			b.Fatal(err)
		}
	}
	if err := q.Close(); err != nil { // flush + sync backlog to the datastore
		b.Fatal(err)
	}

	q2 := newProdDSQueue(ds) // reopen: reads the persisted backlog
	b.SetBytes(benchItemSize)
	b.ResetTimer()
	for range b.N {
		<-q2.Out()
	}
	b.StopTimer()
	q2.Close()
}

func BenchmarkRestartDrain_DSQueue_LevelDB(b *testing.B) {
	benchDSQueueRestartDrain(b, newLevelDB)
}

func BenchmarkRestartDrain_DSQueue_Pebble(b *testing.B) {
	benchDSQueueRestartDrain(b, newPebble)
}

func BenchmarkRestartDrain_CascadeQ(b *testing.B) {
	dir := b.TempDir()
	q, err := cascadeq.New("provide", dir) // production defaults (4096 items / 1 MiB)
	if err != nil {
		b.Fatal(err)
	}
	for i := range b.N {
		if err := q.Put(prodItem(i)); err != nil {
			b.Fatal(err)
		}
	}
	if err := q.Close(); err != nil { // persists to .dat files + snapshot
		b.Fatal(err)
	}

	q2, err := cascadeq.New("provide", dir) // reopen
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(benchItemSize)
	b.ResetTimer()
	for range b.N {
		<-q2.Out()
	}
	b.StopTimer()
	q2.Close()
}

// benchDSQueueRestartDrainGetN drains the persisted backlog via GetN(batch)
// instead of item-by-item Out(). GetN issues one ordered Query per batch and
// batch-deletes, so the front-tombstone scan is paid once per batch rather than
// once per item, amortizing the LSM tombstone-accumulation cost by ~batch. This
// is the in-place fix available without changing libraries: boxo would call
// GetN instead of Out().
func benchDSQueueRestartDrainGetN(b *testing.B, open func(*testing.B, string) datastore.Batching, batch int) {
	dir := b.TempDir()
	ds := open(b, dir)
	defer ds.Close()

	q := newProdDSQueue(ds)
	for i := range b.N {
		if err := q.Put(prodItem(i)); err != nil {
			b.Fatal(err)
		}
	}
	if err := q.Close(); err != nil {
		b.Fatal(err)
	}

	q2 := newProdDSQueue(ds)
	b.SetBytes(benchItemSize)
	b.ResetTimer()
	for got := 0; got < b.N; {
		items, err := q2.GetN(min(batch, b.N-got))
		if err != nil {
			b.Fatal(err)
		}
		if len(items) == 0 {
			b.Fatalf("GetN returned 0 with %d of %d drained", got, b.N)
		}
		got += len(items)
	}
	b.StopTimer()
	q2.Close()
}

func BenchmarkRestartDrainGetN_DSQueue_LevelDB(b *testing.B) {
	benchDSQueueRestartDrainGetN(b, newLevelDB, 16*1024)
}

func BenchmarkRestartDrainGetN_DSQueue_Pebble(b *testing.B) {
	benchDSQueueRestartDrainGetN(b, newPebble, 16*1024)
}

// ── Scenario B: steady-state put+get at production config ─────────────────────
//
// Producer enqueues b.N items while a background goroutine drains. At the
// production buffer (16K) and cascadeq's default caps, a moderate workload
// stays mostly in memory and flows straight to Out(), which is the common
// steady-state path when provides drain about as fast as they arrive.

func benchDSQueueSteady(b *testing.B, open func(*testing.B, string) datastore.Batching) {
	dir := b.TempDir()
	ds := open(b, dir)
	defer ds.Close()
	q := newProdDSQueue(ds)
	defer q.Close()
	b.SetBytes(benchItemSize)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for got := 0; got < b.N; {
			if _, ok := <-q.Out(); !ok {
				return
			}
			got++
		}
	}()

	b.ResetTimer()
	for i := range b.N {
		if err := q.Put(prodItem(i)); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	wg.Wait()
}

func BenchmarkSteady_DSQueue_LevelDB(b *testing.B) { benchDSQueueSteady(b, newLevelDB) }
func BenchmarkSteady_DSQueue_Pebble(b *testing.B)  { benchDSQueueSteady(b, newPebble) }

func BenchmarkSteady_CascadeQ(b *testing.B) {
	dir := b.TempDir()
	q, err := cascadeq.New("provide", dir)
	if err != nil {
		b.Fatal(err)
	}
	defer q.Close()
	b.SetBytes(benchItemSize)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for got := 0; got < b.N; {
			select {
			case <-q.Out():
				got++
			case <-q.Done():
				return
			}
		}
	}()

	b.ResetTimer()
	for i := range b.N {
		if err := q.Put(prodItem(i)); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	wg.Wait()
}
