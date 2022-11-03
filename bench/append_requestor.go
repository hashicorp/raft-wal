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
			// Use small 1MiB segments for now to show effects of rotating more quickly.
			return wal.Open(f.Dir, wal.WithSegmentSize(1024*1024))
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
		newStore:  fn,
	}
}

// appendRequester implements bench.Requester for appending entries to the WAL.
type appendRequester struct {
	dir                string
	logSize, batchSize int
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

// Teardown is called upon benchmark completion.
func (r *appendRequester) Teardown() error {
	if m, ok := r.store.(metricer); ok {
		for k, v := range m.Metrics() {
			fmt.Printf("% 25s: % 15d\n", k, v)
		}
	}
	if c, ok := r.store.(io.Closer); ok {
		return c.Close()
	}
	return nil
}
