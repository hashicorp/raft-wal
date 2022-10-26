// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package segment

import (
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/hashicorp/go-wal"
	"github.com/stretchr/testify/require"
)

func TestFileName(t *testing.T) {
	fn := FileName(wal.SegmentInfo{BaseIndex: 0, ID: 1})
	require.Equal(t, "00000000000000000000-0000000000000001.wal", fn)

	fn = FileName(wal.SegmentInfo{BaseIndex: 7394872394732, ID: 0xab1234cd4567ef})
	require.Equal(t, "00000007394872394732-00ab1234cd4567ef.wal", fn)
}

func TestSegmentBasics(t *testing.T) {
	vfs := newTestVFS()

	f := NewFiler("test", vfs)

	seg0 := testSegment(1)

	w, err := f.Create(seg0)
	require.NoError(t, err)
	defer w.Close()

	// Verify the underlying File is not "dirty" (i.e. it had Sync called after
	// being created and written to).
	require.False(t, w.(*Writer).wf.(*testWritableFile).dirty)

	// To really test Create worked, we have to use OpenReader to re-open and
	// validate the header
	r, err := f.Open(seg0)
	require.NoError(t, err)
	r.Close() // Done with this for now.

	// Append to writer
	err = w.Append([]wal.LogEntry{{Index: 1, Data: []byte("one")}})
	require.NoError(t, err)

	// Should have been "fsynced"
	file := testFileFor(t, w)
	require.False(t, file.dirty)

	// Should be able to read that from tail (can't use "open" yet to read it
	// separately since it's not a sealed segment).
	gotBs, err := w.GetLog(1)
	require.NoError(t, err)
	require.Equal(t, []byte("one"), gotBs)

	expectVals := append([]string{}, "one")
	// OK, now write some more.
	batch := make([]wal.LogEntry, 0, 10)
	for idx := uint64(2); idx < 12; idx++ {
		// Customize value  each time just to check we're really reading the right
		// thing...
		val := strings.Repeat(fmt.Sprintf("%03d ", idx), 128)
		batch = append(batch, wal.LogEntry{Index: idx, Data: []byte(val)})
		expectVals = append(expectVals, val)
	}
	require.NoError(t, w.Append(batch))

	// Peek at the whole "file"
	t.Logf("\n" + file.Dump())

	// Now we should be able to read those all back sequentially through the
	// writer, though some are in the tail block and some in complete blocks.
	for idx := uint64(1); idx < 12; idx++ {
		gotBs, err := w.GetLog(idx)
		require.NoError(t, err, "failed reading idx=%d", idx)
		require.Equal(t, expectVals[idx-1], string(gotBs), "bad value for idx=%d", idx)
	}
}

func TestRecovery(t *testing.T) {
	cases := []struct {
		name               string
		numPreviousEntries int
		appendEntrySizes   []int
		corrupt            func(*testWritableFile) error
		wantErr            string
		wantLastIndex      uint64
		wantSealed         bool
		wantIndexStart     uint64
	}{
		{
			name:               "recover empty",
			numPreviousEntries: 0,
			appendEntrySizes:   []int{},
			// no corruption (clean shutdown)
			wantLastIndex: 0,
		},
		{
			name:               "recover first batch",
			numPreviousEntries: 0,
			appendEntrySizes:   []int{10},
			// no corruption (clean shutdown)
			wantLastIndex: 1,
		},
		{
			name:               "recover later batch",
			numPreviousEntries: 10,
			appendEntrySizes:   []int{10},
			// no corruption (clean shutdown)
			wantLastIndex: 11,
		},
		{
			name:               "recover multi-entry batch",
			numPreviousEntries: 10,
			appendEntrySizes:   []int{10, 10, 10, 10},
			// no corruption (clean shutdown)
			wantLastIndex: 14,
		},
		{
			name:               "missing end of commit",
			numPreviousEntries: 10,
			appendEntrySizes:   []int{10, 10, 10, 10},
			corrupt: func(twf *testWritableFile) error {
				// zero out just the very last commit frame
				for i := twf.maxWritten - frameHeaderLen; i < twf.maxWritten; i++ {
					twf.buf[i] = 0
				}
				return nil
			},
			// should recover back to before the append
			wantLastIndex: 10,
		},
		{
			name:               "partial initial commit",
			numPreviousEntries: 0,
			appendEntrySizes:   []int{10, 10, 10, 10},
			corrupt: func(twf *testWritableFile) error {
				// corrupt a byte in the last commit
				twf.buf[fileHeaderLen+frameHeaderLen] = 127
				return nil
			},
			// should recover back to before the append
			wantLastIndex: 0,
		},
		{
			name:               "torn write with some data in middle of commit missing",
			numPreviousEntries: 10,
			appendEntrySizes:   []int{10, 10, 10, 10},
			corrupt: func(twf *testWritableFile) error {
				// zero out one byte from somewhere near the start of the commit (but
				// not in the frameheader)
				twf.buf[twf.lastSyncStart+10] = 0
				return nil
			},
			// should recover back to before the append
			wantLastIndex: 10,
		},
		{
			name:               "torn write with header in commit corrupt",
			numPreviousEntries: 10,
			appendEntrySizes:   []int{10, 10, 10, 10},
			corrupt: func(twf *testWritableFile) error {
				// We rely on knowing the sizes of the entries in this case which were
				// header + 10 byte + 6 bytes padding each. We corrupt not the first but
				// second header. We'll set the typ byte to an invalid value.
				twf.buf[twf.lastSyncStart+encodedFrameSize(10)] = 65
				return nil
			},
			// should recover back to before the append
			wantLastIndex: 10,
		},
		{
			name:               "empty file",
			numPreviousEntries: 0,
			appendEntrySizes:   []int{},
			corrupt: func(twf *testWritableFile) error {
				twf.buf = twf.buf[:0]
				twf.dirty = false
				twf.maxWritten = 0
				return nil
			},
			// should throw an EOF error on recover as there is no file header to verify
			wantErr: io.EOF.Error(),
		},
		{
			name:               "bad segment header",
			numPreviousEntries: 0,
			appendEntrySizes:   []int{},
			corrupt: func(twf *testWritableFile) error {
				twf.buf[0] = 123 // twiddle the magic value
				return nil
			},
			// should throw an EOF error on recover as there is no file header to verify
			wantErr: "corrupt",
		},
		{
			name:               "bad segment header BaseIndex",
			numPreviousEntries: 0,
			appendEntrySizes:   []int{},
			corrupt: func(twf *testWritableFile) error {
				twf.buf[8] = 123 // twiddle the base index
				return nil
			},
			// should throw an EOF error on recover as there is no file header to verify
			wantErr: "segment header BaseIndex 123 doesn't match metadata 1",
		},
		{
			name:               "sealed tail",
			numPreviousEntries: 0,
			// SizeLimit is set to 4KiB write 5 1KiB values to force it to be sealed.
			appendEntrySizes: []int{1024, 1024, 1024, 1024, 1024},
			wantLastIndex:    5,
			wantSealed:       true,
			wantIndexStart:   1234, // Not sure how useful this is..
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			vfs := newTestVFS()

			f := NewFiler("test", vfs)

			seg0 := testSegment(1)

			w, err := f.Create(seg0)
			require.NoError(t, err)
			defer w.Close()

			// Append previous entries. We just pick a fixed size and format that's
			// easy to verify but generally fits in our test block size.
			for i := 0; i < tc.numPreviousEntries; i++ {
				// Append individually, could do commit batches but this is all in
				// memory so no real benefit.
				v := fmt.Sprintf("%05d: blah", i+1) // 11 bytes == 16 + 8 + 4 with overhead
				err := w.Append([]wal.LogEntry{{Index: uint64(i + 1), Data: []byte(v)}})
				require.NoError(t, err)
			}

			// Now create a batch with the entries sized as in the test case
			batch := make([]wal.LogEntry, 0, len(tc.appendEntrySizes))
			for i, len := range tc.appendEntrySizes {
				idx := 1 + tc.numPreviousEntries + i
				if len < 6 {
					panic("we need 6 bytes to encode the index for verification")
				}
				v := fmt.Sprintf("%05d:%s", idx, strings.Repeat("P", len-6))
				batch = append(batch, wal.LogEntry{Index: uint64(idx), Data: []byte(v)})
			}

			err = w.Append(batch)
			require.NoError(t, err)
			w.Close()

			// All written. Optionally corrupt the underlying file data to simulate
			// different crash cases.
			if tc.corrupt != nil {
				file := testFileFor(t, w)
				require.NoError(t, tc.corrupt(file))
			}

			// Now recover file
			w, err = f.RecoverTail(seg0)

			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)

			t.Log("\n" + testFileFor(t, w).Dump())

			require.Equal(t, int(tc.wantLastIndex), int(w.LastIndex()))

			sealed, indexStart, err := w.Sealed()
			require.NoError(t, err)

			require.Equal(t, tc.wantSealed, sealed)
			require.Equal(t, tc.wantIndexStart, indexStart)

			lastIdx := w.LastIndex()
			if tc.wantSealed {
				// Appends should fail
				err := w.Append([]wal.LogEntry{{Index: lastIdx, Data: []byte("bad")}})
				require.ErrorContains(t, err, "foo")
				return
			}

			// Verify we can continue to append and then read back everything.

			for i := 0; i < 10; i++ {
				// Append individually, could do commit batches but this is all in
				// memory so no real benefit.
				lastIdx++
				v := fmt.Sprintf("%05d: Some other bytes of data too.", lastIdx)
				err := w.Append([]wal.LogEntry{{Index: lastIdx, Data: []byte(v)}})
				require.NoError(t, err)
			}

			t.Log("\n" + testFileFor(t, w).Dump())

			// Read the whole log!
			for idx := uint64(1); idx <= lastIdx; idx++ {
				gotBs, err := w.GetLog(idx)
				require.NoError(t, err, "failed reading idx=%d", idx)
				require.True(t, strings.HasPrefix(string(gotBs), fmt.Sprintf("%05d:", idx)), "bad value for idx=%d", idx)
			}
		})
	}
}

var nextSegID uint64

func testSegment(baseIndex uint64) wal.SegmentInfo {
	id := atomic.AddUint64(&nextSegID, 1)
	return wal.SegmentInfo{
		BaseIndex: baseIndex,
		ID:        id,
		Codec:     wal.CodecBinaryV1,
		SizeLimit: 4 * 1024, // Small limit to make it easier to test sealing
		// Other fields don't really matter at segment level for now.
	}
}
