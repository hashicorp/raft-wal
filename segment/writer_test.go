// Copyright IBM Corp. 2020, 2025
// SPDX-License-Identifier: MPL-2.0

package segment

import (
	"fmt"
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
			if string(log.Bs) != "test" {
				panic("bad log read: " + string(log.Bs))
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

func TestWriterRecoversFromWriteFailure(t *testing.T) {
	cases := []struct {
		name         string
		setupFailure func(f *testWritableFile, batch []types.LogEntry)
		fixFailure   func(batch []types.LogEntry)
	}{
		{
			name: "fwrite failure",
			setupFailure: func(f *testWritableFile, batch []types.LogEntry) {
				f.failNextWrite()
			},
		},
		{
			name: "fsync failure",
			setupFailure: func(f *testWritableFile, batch []types.LogEntry) {
				f.failNextSync()
			},
		},
		{
			name: "log append failure",
			setupFailure: func(f *testWritableFile, batch []types.LogEntry) {
				// Should cause monotonicity check to fail but only on last log after
				// other logs have been written and internal state updated.
				batch[len(batch)-1].Index = 123456
			},
			fixFailure: func(batch []types.LogEntry) {
				batch[len(batch)-1].Index = batch[len(batch)-2].Index + 1
			},
		},
	}

	for _, tc := range cases {
		tc := tc

		testFn := func(t *testing.T, empty bool) {
			vfs := newTestVFS()

			f := NewFiler("test", vfs)

			seg0 := testSegment(1)

			w, err := f.Create(seg0)
			require.NoError(t, err)
			defer w.Close()

			batch := make([]types.LogEntry, 5)
			for i := range batch {
				batch[i].Index = uint64(i + 1)
				batch[i].Data = []byte(fmt.Sprintf("val-%d", i+1))
			}
			maxIdx := len(batch)
			expectFirstIdx := 0
			expectLastIdx := 0

			if !empty {
				require.NoError(t, w.Append(batch))
				expectFirstIdx = 1
				expectLastIdx = maxIdx
				for i := range batch {
					batch[i].Index = uint64(i + maxIdx + 1)
					batch[i].Data = []byte(fmt.Sprintf("val-%d", i+maxIdx+1))
				}
			}

			tf := testFileFor(t, w)

			tc.setupFailure(tf, batch)

			require.Error(t, w.Append(batch))
			assertExpectedLogs(t, w, expectFirstIdx, expectLastIdx)

			if tc.fixFailure != nil {
				tc.fixFailure(batch)
			}

			// Now retry that write, it should work!
			expectFirstIdx = 1
			expectLastIdx = int(batch[4].Index)
			require.NoError(t, w.Append(batch))
			assertExpectedLogs(t, w, expectFirstIdx, expectLastIdx)

			// Also, re-open the file "from disk" to make sure what has been written
			// is correct and recoverable!
			w2, err := f.RecoverTail(seg0)
			require.NoError(t, err)
			assertExpectedLogs(t, w2, expectFirstIdx, expectLastIdx)
			w2.Close()
		}

		t.Run(tc.name+" empty", func(t *testing.T) {
			testFn(t, true)
		})
		t.Run(tc.name+" non-empty", func(t *testing.T) {
			testFn(t, false)
		})
	}
}

func assertExpectedLogs(t *testing.T, w types.SegmentWriter, first, last int) {
	t.Helper()

	require.Equal(t, uint64(last), w.LastIndex())
	if last == 0 {
		return
	}
	assertExpectedReaderLogs(t, w, first, last)
}

func assertExpectedReaderLogs(t *testing.T, r types.SegmentReader, first, last int) {
	t.Helper()

	for idx := first; idx <= last; idx++ {
		buf, err := r.GetLog(uint64(idx))
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("val-%d", idx), string(buf.Bs))
		buf.Close()
	}
}

func TestWriterForceSeal(t *testing.T) {
	vfs := newTestVFS()

	f := NewFiler("test", vfs)

	seg0 := testSegment(1)

	w, err := f.Create(seg0)
	require.NoError(t, err)
	defer w.Close()

	batch := make([]types.LogEntry, 5)
	for i := range batch {
		batch[i].Index = uint64(i + 1)
		batch[i].Data = []byte(fmt.Sprintf("val-%d", i+1))
	}
	require.NoError(t, w.Append(batch))

	assertExpectedLogs(t, w, 1, 5)

	// Should not have sealed after one append.
	sealed, indexStart, err := w.Sealed()
	require.NoError(t, err)
	require.False(t, sealed)
	require.Equal(t, 0, int(indexStart))

	// Force seal it
	indexStart, err = w.ForceSeal()
	require.NoError(t, err)
	require.Greater(t, int(indexStart), 0)

	// It should be possible to open it with a reader now
	seg0.IndexStart = indexStart
	r, err := f.Open(seg0)
	require.NoError(t, err)

	assertExpectedReaderLogs(t, r, 1, 5)
}
