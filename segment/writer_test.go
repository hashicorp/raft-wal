// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package segment

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft-wal/types"
	"github.com/stretchr/testify/require"
)

// TestConcurrentReadersAndWriter is designed to be run with race detector
// enabled to validate the concurrent behavior of the segment.
func TestConcurrentReadersAndWriter(t *testing.T) {
	vfs := newTestVFS()
	f := NewFiler("test", vfs)

	seg1 := testSegment(1)

	// Increase size limit so we keep going for a while. We don't want to make
	// this too large that we time out easily on slower machines though or in CI.
	// 256KiB passes easily on my laptop (~5s) and is big enough to take a
	// while to test concurrent accesses.
	seg1.SizeLimit = 256 * 1024

	wf, err := f.Create(seg1)
	require.NoError(t, err)

	var lastIndexWritten uint64
	var sealedMaxIndex uint64
	var numReads uint64

	writer := func() {
		idx := uint64(1)
		for {
			err := wf.Append([]types.LogEntry{{Index: idx, Data: []byte("test")}})
			if err != nil {
				panic("error during append: " + err.Error())
			}

			sealed, _, err := wf.Sealed()
			if err != nil {
				panic("error during sealed: " + err.Error())
			}
			atomic.StoreUint64(&lastIndexWritten, idx)
			if sealed {
				atomic.StoreUint64(&sealedMaxIndex, idx)
				return
			}
			idx++
		}
	}

	reader := func(doneCh chan<- struct{}) {
		// Follow the tail
		idx := uint64(1)
		for {
			// Complete once writer has stopped and we've read all of it's written
			// entries.
			finalIdx := atomic.LoadUint64(&sealedMaxIndex)
			if finalIdx > 0 && idx > finalIdx {
				doneCh <- struct{}{}
				return
			}
			if idx > wf.LastIndex() {
				time.Sleep(time.Millisecond)
				continue
			}

			log, err := wf.GetLog(idx)
			if err != nil {
				panic("error during GetLog: " + err.Error())
			}
			if string(log) != "test" {
				panic("bad log read: " + string(log))
			}
			atomic.AddUint64(&numReads, 1)
			idx++
		}
	}

	// Start 10 readers and 1 writer in parallel
	done := make(chan struct{}, 10)
	for i := 0; i < cap(done); i++ {
		go reader(done)
	}
	go writer()

	complete := 0
	// Takes about 5 seconds on my laptop. Give it a really generous margin for CI
	// etc. though.
	timeoutCh := time.After(30 * time.Second)
	for complete < cap(done) {
		select {
		case <-timeoutCh:
			t.Fatalf("Took longer than 10 seconds to write and read the whole segment. w=%d, r=%d s=%d",
				atomic.LoadUint64(&lastIndexWritten),
				atomic.LoadUint64(&numReads),
				atomic.LoadUint64(&sealedMaxIndex),
			)
		case <-done:
			complete++
		}
	}

	t.Logf("Written: %d, Read: %d, SealedMax: %d",
		atomic.LoadUint64(&lastIndexWritten),
		atomic.LoadUint64(&numReads),
		atomic.LoadUint64(&sealedMaxIndex),
	)

	// Check we actually did something!
	require.Greater(t, int(atomic.LoadUint64(&lastIndexWritten)), 1000)
	require.Greater(t, int(atomic.LoadUint64(&numReads)), 1000)
	require.Greater(t, int(atomic.LoadUint64(&sealedMaxIndex)), 1000)
}
