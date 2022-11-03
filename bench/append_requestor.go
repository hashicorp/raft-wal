// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"crypto/rand"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/benmathews/bench"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	wal "github.com/hashicorp/raft-wal"
)

var (
	_ bench.RequesterFactory = &appendRequesterFactory{}

	randomData []byte
)

func init() {
	randomData = make([]byte, 1024*1024)
	rand.Read(randomData)
}

// appendRequesterFactory implements bench.RequesterFactory
type appendRequesterFactory struct {
	Dir       string
	Version   string
	LogSize   int
	BatchSize int
	SegSize   int
	Preload   int
}

// GetRequester returns a new Requester, called for each Benchmark
// connection.
func (f *appendRequesterFactory) GetRequester(number uint64) bench.Requester {
	if number > 0 {
		panic("wal only supports a single writer")
	}

	var fn func() (raft.LogStore, error)
	switch f.Version {
	case "wal":
		fn = func() (raft.LogStore, error) {
			return wal.Open(f.Dir, wal.WithSegmentSize(f.SegSize*1024*1024))
		}
	case "bolt":
		fn = func() (raft.LogStore, error) {
			return raftboltdb.NewBoltStore(filepath.Join(f.Dir, "raft.db"))
		}
	default:
		panic("unknown LogStore version: " + f.Version)
	}

	return &appendRequester{
		dir:       f.Dir,
		logSize:   f.LogSize,
		batchSize: f.BatchSize,
		preload:   f.Preload,
		newStore:  fn,
	}
}

// appendRequester implements bench.Requester for appending entries to the WAL.
type appendRequester struct {
	dir                string
	logSize, batchSize int
	preload            int
	batch              []*raft.Log
	index              uint64
	newStore           func() (raft.LogStore, error)
	store              raft.LogStore
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
	r.batch = make([]*raft.Log, r.batchSize)
	for i := range r.batch {
		r.batch[i] = &raft.Log{
			// We'll vary the indexes each time but save on setting this up the same
			// way every time to!
			Data:       randomData[:r.logSize],
			AppendedAt: time.Now(),
		}
	}
	r.index = 1

	if r.preload > 0 {
		// Write lots of big records and then delete them again. We'll use batches
		// of 1000 1024 byte records for now to speed things up a bit.
		preBatch := make([]*raft.Log, 0, 1000)
		for r.index <= uint64(r.preload) {
			preBatch = append(preBatch, &raft.Log{Index: r.index, Data: randomData[:1024]})
			r.index++
			if len(preBatch) == 1000 {
				fmt.Printf("Preloading up to index %d\n", r.index)
				err := r.store.StoreLogs(preBatch)
				if err != nil {
					return err
				}
				preBatch = preBatch[:0]
			}
		}
		if len(preBatch) > 0 {
			fmt.Printf("Preloading up to index %d\n", r.index)
			err := r.store.StoreLogs(preBatch)
			if err != nil {
				return err
			}
		}
		// Now truncate all, but one of those back out. We leave one to be more
		// realistic since raft always leaves some recent logs. Note r.index is
		// already at the next index after the one we just wrote so the inclusive
		// delete range is not one but two before that to leave the one before
		// intact.
		fmt.Printf("Truncating 1 - %d\n", r.index-2)
		err := r.store.DeleteRange(1, r.index-2)
		if err != nil {
			return err
		}
		r.dumpStats()
	}

	return nil
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
		fmt.Println("== METRICS ==========")
		for k, v := range m.Metrics() {
			fmt.Printf("% 25s: % 15d\n", k, v)
		}
	}
}

// Teardown is called upon benchmark completion.
func (r *appendRequester) Teardown() error {
	r.dumpStats()
	if c, ok := r.store.(io.Closer); ok {
		return c.Close()
	}
	return nil
}
