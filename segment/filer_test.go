// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package segment

import (
	"fmt"
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

func TestPreallocatedSize(t *testing.T) {
	s := PreallocatedSize(wal.SegmentInfo{BlockSize: 1024 * 1024, NumBlocks: 64})
	require.Equal(t, 64*1024*1024, int(s))
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
	// OK, now write enough to fill up the block with a few more entries.
	batch := make([]wal.LogEntry, 0, 10)
	for idx := uint64(2); idx < 12; idx++ {
		// Customize value  each time just to check we're really reading the right
		// thing...
		val := strings.Repeat(fmt.Sprintf("%03d ", idx), int(seg0.BlockSize/8)/4)
		batch = append(batch, wal.LogEntry{Index: idx, Data: []byte(val)})
		expectVals = append(expectVals, val)
	}
	require.NoError(t, w.Append(batch))

	// then a 32KiB message which will split over multiple blocks and another
	// short one which should end up sharing a block with the last fragment from
	// before.
	val12 := strings.Repeat(fmt.Sprintf("%03d ", 12), int(seg0.BlockSize*2)/4)
	require.NoError(t, w.Append([]wal.LogEntry{
		{Index: 12, Data: []byte(val12)},
		{Index: 13, Data: []byte("thirteen")},
	}))
	expectVals = append(expectVals, val12, "thirteen")

	// Peek at the whole "file"
	t.Logf(file.Dump())

	// Now we should be able to read those all back sequentially through the
	// writer, though some are in the tail block and some in complete blocks.
	for idx := uint64(1); idx < 14; idx++ {
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
	}{
		// {
		// 	name:               "recover first block",
		// 	numPreviousEntries: 0,
		// 	// Recover a single entry append
		// 	appendEntrySizes: []int{10},
		// 	// no corruption (clean shutdown)
		// 	wantLastIndex: 1,
		// },
		// {
		// 	name: "recover second block",
		// 	// should just fill block 0 and spill to block 1 so the whole append is in
		// 	// block 1.
		// 	numPreviousEntries: 5,
		// 	// Recover a single entry append at start
		// 	appendEntrySizes: []int{10},
		// 	// no corruption (clean shutdown)
		// 	wantLastIndex: 6,
		// },
		// {
		// 	name: "recover across block boundary",
		// 	// Half fill block 0
		// 	numPreviousEntries: 2,
		// 	// Recover an entry that is fragmented into block 1
		// 	appendEntrySizes: []int{128},
		// 	// no corruption (clean shutdown)
		// 	wantLastIndex: 3,
		// },
		{
			name:               "recover multi-block commit",
			numPreviousEntries: 0,
			// Recover an entry that is fragmented across multiple blocks
			appendEntrySizes: []int{300},
			// no corruption (clean shutdown)
			wantLastIndex: 1,
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

			// Verify we can continue to append and then read back everything.
			lastIdx := w.LastIndex()
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
		BlockSize: 128,
		NumBlocks: 64,
		Codec:     wal.CodecBinaryV1,
		// Other fields don't really matter at segment level for now.
	}
}
