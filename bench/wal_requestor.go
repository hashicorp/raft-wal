// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"crypto/rand"
	"fmt"
	"time"

	"github.com/benmathews/bench"
	"github.com/hashicorp/raft"
	wal "github.com/hashicorp/raft-wal"
)

var (
	_ bench.RequesterFactory = &walAppendRequesterFactory{}

	randomData []byte
)

func init() {
	randomData = make([]byte, 1024*1024)
	rand.Read(randomData)
}

// walAppendRequesterFactory implements bench.RequesterFactory
type walAppendRequesterFactory struct {
	Dir       string
	LogSize   int
	BatchSize int
}

// GetRequester returns a new Requester, called for each Benchmark
// connection.
func (f *walAppendRequesterFactory) GetRequester(number uint64) bench.Requester {
	if number > 0 {
		panic("wal only supports a single writer")
	}
	return &walAppendRequester{
		dir:       f.Dir,
		logSize:   f.LogSize,
		batchSize: f.BatchSize,
	}
}

// walRequester implements bench.Requester for appending entries to the WAL.
type walAppendRequester struct {
	dir                string
	logSize, batchSize int
	wal                *wal.WAL
	batch              []*raft.Log
	index              uint64
}

// Setup prepares the Requester for benchmarking.
func (r *walAppendRequester) Setup() error {
	wal, err := wal.Open(r.dir)
	if err != nil {
		return err
	}
	r.wal = wal

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
func (r *walAppendRequester) Request() error {
	// Update log indexes
	for i := range r.batch {
		r.batch[i].Index = r.index
		r.index++
	}
	return r.wal.StoreLogs(r.batch)
}

// Teardown is called upon benchmark completion.
func (r *walAppendRequester) Teardown() error {
	m := r.wal.Metrics()
	for k, v := range m {
		fmt.Printf("% 15s: % 6d\n", k, v)
	}
	return r.wal.Close()
}
