// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package migrate

import (
	"fmt"
	"time"

	"github.com/hashicorp/raft"
)

// CopyLogs takes an src and a dst raft.LogStore implementation and copies all
// entries from src to dst. It assumes dst is empty. Neither LogStore may be in
// use at the time. batchBytes is the target number of bytes of log data to
// group into each append for efficiency. If progress is non-nil it will be
// delivered updates during the copy since it could take a while. Updates will
// be delivered best-effort with a short wait of 1 millisecond. If the channel
// blocks for longer updates may be lost. The caller should sufficiently buffer
// it and ensure it's being drained as fast as needed.
func CopyLogs(dst, src raft.LogStore, batchBytes int, progress chan<- string) error {
	st := time.Now()
	update := func(message string, args ...interface{}) {
		if progress == nil {
			return
		}
		select {
		case progress <- fmt.Sprintf(message, args...):
		case <-time.After(time.Millisecond):
		}
	}

	first, err := src.FirstIndex()
	if err != nil {
		return fmt.Errorf("failed getting first index: %w", err)
	}
	last, err := src.LastIndex()
	if err != nil {
		return fmt.Errorf("failed getting last index: %w", err)
	}

	batch := make([]*raft.Log, 0, 4096)
	batchSize := 0
	n := 0
	batchN := 1
	total := int(last - first + 1)
	totalBytes := 0
	update("starting to copy %d log entries with indexes [%d, %d]", total, first, last)
	for idx := first; idx <= last; idx++ {
		var log raft.Log
		n++
		err := src.GetLog(idx, &log)
		if err != nil {
			return fmt.Errorf("failed copying log %d (%d/%d): %w", idx, n, total, err)
		}
		batch = append(batch, &log)
		// Fudge some overhead for headers and other fields this is about right for
		// our WAL anyway.
		batchSize += len(log.Data) + 32
		if batchSize >= batchBytes {
			// Flush the batch
			batchSummary := fmt.Sprintf("batch %6d: %d entries ending at %d", batchN, len(batch), idx)
			err := dst.StoreLogs(batch)
			if err != nil {
				return fmt.Errorf("failed writing %s: %w", batchSummary, err)
			}
			update("  -> wrote %s (%3.0f%% complete)", batchSummary, (float32(n-1)/float32(total))*100.0)
			batchN++
			batch = batch[:0]
			totalBytes += batchSize
			batchSize = 0
		}
	}
	if len(batch) > 0 {
		// Flush the batch
		batchSummary := fmt.Sprintf("batch %6d: %d entries ending at %d", batchN, len(batch), last)
		err := dst.StoreLogs(batch)
		if err != nil {
			return fmt.Errorf("failed writing %s: %w", batchSummary, err)
		}
		update("  -> wrote %s (%3.0f%% complete)", batchSummary, (float32(n-1)/float32(total))*100.0)
		batchN++
		batch = batch[:0]
		totalBytes += batchSize
		batchSize = 0
	}
	update("DONE: took %s to copy %d entries (%d bytes)", time.Since(st), total, totalBytes)
	return nil
}

// CopyStable copies the known hashicorp/raft library used keys from one stable
// store to another. Since StableStore has no list method there is no general
// way to copy all possibly stored keys, however this is sufficient for standard
// uses of `hashicorp/raft` as of the current release since it only every writes
// these keys to StableStore. If other keys are written by another code path,
// the caller can provide them in extraKeys and/or extraIntKeys depending on which
// interface method they were written with - we don't assume all implementations
// share a key space for Set and SetUint64. Both can be nil for just the
// standard raft keys to be copied.
func CopyStable(dst, src raft.StableStore, extraKeys, extraIntKeys [][]byte, progress chan<- string) error {
	// https://github.com/hashicorp/raft/blob/44124c28758b8cfb675e90c75a204a08a84f8d4f/raft.go#L22-L26
	knownIntKeys := [][]byte{
		[]byte("CurrentTerm"),
		[]byte("LastVoteTerm"),
	}
	knownKeys := [][]byte{
		[]byte("LastVoteCand"),
	}

	update := func(message string, args ...interface{}) {
		if progress == nil {
			return
		}
		select {
		case progress <- fmt.Sprintf(message, args...):
		case <-time.After(time.Millisecond):
		}
	}

	st := time.Now()
	update("copying %d int, %d regular KVs", len(knownIntKeys)+len(extraIntKeys),
		len(knownKeys)+len(extraKeys))
	for _, k := range append(knownIntKeys, extraIntKeys...) {
		v, err := src.GetUint64(k)
		if err != nil {
			return fmt.Errorf("failed to read int key %s: %w", k, err)
		}
		err = dst.SetUint64(k, v)
		if err != nil {
			return fmt.Errorf("failed to set int key %s => %d: %w", k, v, err)
		}
		update("  copied int %s => %d", k, v)
	}
	for _, k := range append(knownKeys, extraKeys...) {
		v, err := src.Get(k)
		if err != nil {
			return fmt.Errorf("failed to read key %s: %w", k, err)
		}
		err = dst.Set(k, v)
		if err != nil {
			return fmt.Errorf("failed to set key %s => %s: %w", k, v, err)
		}
		update("  copied %s => %q", k, v)
	}
	update("DONE: took %s to copy %d KVs", time.Since(st),
		len(knownIntKeys)+len(extraIntKeys)+len(knownKeys)+len(extraKeys))
	return nil
}
