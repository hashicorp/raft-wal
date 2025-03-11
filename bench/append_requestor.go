// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/benmathews/bench"
	histwriter "github.com/benmathews/hdrhistogram-writer"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	wal "github.com/hashicorp/raft-wal"
	"go.etcd.io/bbolt"
)

var (
	_ bench.RequesterFactory = &appendRequesterFactory{}

	randomData []byte
)

func init() {
	randomData = make([]byte, 1024*1024)
	if _, err := rand.Read(randomData); err != nil {
		log.Printf("failed to read random data: %v", err)
	}
}

// appendRequesterFactory implements bench.RequesterFactory
type appendRequesterFactory struct {
	opts   opts
	output io.Writer
}

// GetRequester returns a new Requester, called for each Benchmark
// connection.
func (f *appendRequesterFactory) GetRequester(number uint64) bench.Requester {
	if number > 0 {
		panic("wal only supports a single writer")
	}

	var fn func() (raft.LogStore, error)
	switch f.opts.version {
	case "wal":
		fn = func() (raft.LogStore, error) {
			return wal.Open(f.opts.dir, wal.WithSegmentSize(f.opts.segSize*1024*1024))
		}
	case "bolt":
		fn = func() (raft.LogStore, error) {
			boltOpts := raftboltdb.Options{
				Path: filepath.Join(f.opts.dir, "raft.db"),
				BoltOptions: &bbolt.Options{
					NoFreelistSync: f.opts.noFreelistSync,
				},
			}
			return raftboltdb.New(boltOpts)
		}
	default:
		panic("unknown LogStore version: " + f.opts.version)
	}

	return &appendRequester{
		opts:     f.opts,
		output:   f.output,
		newStore: fn,
	}
}

// appendRequester implements bench.Requester for appending entries to the WAL.
type appendRequester struct {
	closed uint32

	opts opts

	batch        []*raft.Log
	index        uint64
	newStore     func() (raft.LogStore, error)
	store        raft.LogStore
	truncateStop func()
	output       io.Writer

	truncateTiming *hdrhistogram.Histogram
}

// Setup prepares the Requester for benchmarking.
func (r *appendRequester) Setup() error {
	ls, err := r.newStore()
	if err != nil {
		return err
	}
	r.store = ls

	// Prebuild the batch of logs. There is no compression so we don't care that
	// they are all the same data.
	r.batch = make([]*raft.Log, r.opts.batchSize)
	for i := range r.batch {
		r.batch[i] = &raft.Log{
			// We'll vary the indexes each time but save on setting this up the same
			// way every time to!
			Data:       randomData[:r.opts.logSize],
			AppendedAt: time.Now(),
		}
	}
	r.index = 1

	if r.opts.preLoadN > 0 {
		// Write lots of big records and then delete them again. We'll use batches
		// of 1000 1024 byte records for now to speed things up a bit.
		preBatch := make([]*raft.Log, 0, 1000)
		fmt.Fprintf(r.output, "Preloading up to index %d\n", r.opts.preLoadN)
		for r.index <= uint64(r.opts.preLoadN) {
			preBatch = append(preBatch, &raft.Log{Index: r.index, Data: randomData[:1024]})
			r.index++
			if len(preBatch) == 1000 {
				err := r.store.StoreLogs(preBatch)
				if err != nil {
					return err
				}
				preBatch = preBatch[:0]
			}
		}
		if len(preBatch) > 0 {
			err := r.store.StoreLogs(preBatch)
			if err != nil {
				return err
			}
		}

		// Now truncate back to trailingLogs.
		fmt.Fprintf(r.output, "Truncating 1 - %d\n", r.index-uint64(r.opts.truncateTrailingLogs))
		err := r.store.DeleteRange(1, r.index-uint64(r.opts.truncateTrailingLogs))
		if err != nil {
			return err
		}
		r.dumpStats()
	}
	if r.opts.truncatePeriod > 0 {
		r.truncateTiming = hdrhistogram.New(1, 10_000_000, 3)
		fmt.Fprintf(r.output, "Starting Truncator every %s\n", r.opts.truncatePeriod)
		ctx, cancel := context.WithCancel(context.Background())
		r.truncateStop = cancel
		go r.runTruncate(ctx)
	} else {
		fmt.Fprintf(r.output, "Truncation disabled\n")
	}

	return nil
}

func (r *appendRequester) runTruncate(ctx context.Context) {
	ticker := time.NewTicker(r.opts.truncatePeriod)
	for {
		select {
		case <-ticker.C:
			if atomic.LoadUint32(&r.closed) == 1 {
				return
			}
			first, err := r.store.FirstIndex()
			if err != nil {
				panic(err)
			}
			last, err := r.store.LastIndex()
			if err != nil {
				panic(err)
			}

			deleteMax := uint64(0)
			if last > uint64(r.opts.truncateTrailingLogs) {
				deleteMax = last - uint64(r.opts.truncateTrailingLogs)
			}
			if deleteMax >= first {
				st := time.Now()
				err := r.store.DeleteRange(first, deleteMax)
				elapsed := time.Since(st)
				if err := r.truncateTiming.RecordValue(elapsed.Microseconds()); err != nil {
					log.Print(err)
				}
				if err != nil {
					panic(err)
				}
			}

		case <-ctx.Done():
			return
		}
	}
}

// Request performs a synchronous request to the system under test.
func (r *appendRequester) Request() error {
	// Update log indexes
	for i := range r.batch {
		r.batch[i].Index = r.index
		r.index++
	}
	return r.store.StoreLogs(r.batch)
}

type metricer interface {
	Metrics() map[string]uint64
}

func (r *appendRequester) dumpStats() {
	if m, ok := r.store.(metricer); ok {
		fmt.Fprintln(r.output, "\n== METRICS ==========")
		for k, v := range m.Metrics() {
			fmt.Fprintf(r.output, "% 25s: % 15d\n", k, v)
		}
	}
	if r.truncateTiming != nil {
		scaleFactor := 0.001 // Scale us to ms.
		if err := histwriter.WriteDistributionFile(r.truncateTiming, nil, scaleFactor, outFileName(r.opts, "truncate-lat")); err != nil {
			fmt.Fprintf(r.output, "ERROR writing truncate histogram: %s\n", err)
		}
		printHistogram(r.output, "Truncate Latency (ms)", r.truncateTiming, 1000)
	}
}

// Teardown is called upon benchmark completion.
func (r *appendRequester) Teardown() error {
	old := atomic.SwapUint32(&r.closed, 1)
	if old == 0 {
		r.dumpStats()
		if c, ok := r.store.(io.Closer); ok {
			return c.Close()
		}
	}
	return nil
}
