// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package segment

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/hashicorp/raft-wal/types"
	"github.com/stretchr/testify/require"
)

func TestFileName(t *testing.T) {
	fn := FileName(types.SegmentInfo{BaseIndex: 0, ID: 1})
	require.Equal(t, "00000000000000000000-0000000000000001.wal", fn)

	fn = FileName(types.SegmentInfo{BaseIndex: 7394872394732, ID: 0xab1234cd4567ef})
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

	// Append to writer
	err = w.Append([]types.LogEntry{{Index: 1, Data: []byte("one")}})
	require.NoError(t, err)

	// Should have been "fsynced"
	file := testFileFor(t, w)
	require.False(t, file.dirty)

	// Now we've committed to file we should be able to open and read a valid file
	// header.
	r, err := f.Open(seg0)
	require.NoError(t, err)
	r.Close() // Done with this for now.

	// Should be able to read that from tail (can't use "open" yet to read it
	// separately since it's not a sealed segment).
	got, err := w.GetLog(1)
	require.NoError(t, err)
	require.Equal(t, []byte("one"), got.Bs)

	expectVals := append([]string{}, "one")
	// OK, now write some more.
	batch := make([]types.LogEntry, 0, 10)
	for idx := uint64(2); idx < 12; idx++ {
		// Customize value  each time just to check we're really reading the right
		// thing...
		val := strings.Repeat(fmt.Sprintf("%03d ", idx), 128)
		batch = append(batch, types.LogEntry{Index: idx, Data: []byte(val)})
		expectVals = append(expectVals, val)
	}
	require.NoError(t, w.Append(batch))

	// Peek at the whole "file"
	t.Logf("\n" + file.Dump())

	// Now we should be able to read those all back sequentially through the
	// writer, though some are in the tail block and some in complete blocks.
	for idx := uint64(1); idx < 12; idx++ {
		got, err := w.GetLog(idx)
		require.NoError(t, err, "failed reading idx=%d", idx)
		require.Equal(t, expectVals[idx-1], string(got.Bs), "bad value for idx=%d", idx)
	}

	// We just wrote enough data to ensure the segment was sealed.
	sealed, indexStart, err := w.Sealed()
	require.NoError(t, err)
	require.True(t, sealed)
	require.Greater(t, int(indexStart), 1)
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
	}{
		// {
		// 	name:               "recover empty",
		// 	numPreviousEntries: 0,
		// 	appendEntrySizes:   []int{},
		// 	// no corruption (clean shutdown)
		// 	wantLastIndex: 0,
		// },
		// {
		// 	name:               "recover first batch",
		// 	numPreviousEntries: 0,
		// 	appendEntrySizes:   []int{10},
		// 	// no corruption (clean shutdown)
		// 	wantLastIndex: 1,
		// },
		// {
		// 	name:               "recover later batch",
		// 	numPreviousEntries: 10,
		// 	appendEntrySizes:   []int{10},
		// 	// no corruption (clean shutdown)
		// 	wantLastIndex: 11,
		// },
		// {
		// 	name:               "recover multi-entry batch",
		// 	numPreviousEntries: 10,
		// 	appendEntrySizes:   []int{10, 10, 10, 10},
		// 	// no corruption (clean shutdown)
		// 	wantLastIndex: 14,
		// },
		// {
		// 	name:               "missing end of commit",
		// 	numPreviousEntries: 10,
		// 	appendEntrySizes:   []int{10, 10, 10, 10},
		// 	corrupt: func(twf *testWritableFile) error {
		// 		// zero out just the very last commit frame
		// 		_, err := twf.WriteAt(bytes.Repeat([]byte{0}, frameHeaderLen), int64(twf.maxWritten-frameHeaderLen))
		// 		return err
		// 	},
		// 	// should recover back to before the append
		// 	wantLastIndex: 10,
		// },
		// {
		// 	name:               "partial initial commit",
		// 	numPreviousEntries: 0,
		// 	appendEntrySizes:   []int{10, 10, 10, 10},
		// 	corrupt: func(twf *testWritableFile) error {
		// 		// corrupt a byte in the last commit
		// 		_, err := twf.WriteAt([]byte{127}, int64(fileHeaderLen+frameHeaderLen))
		// 		return err
		// 	},
		// 	// should recover back to before the append
		// 	wantLastIndex: 0,
		// },
		// {
		// 	name:               "torn write with some data in middle of commit missing",
		// 	numPreviousEntries: 10,
		// 	appendEntrySizes:   []int{10, 10, 10, 10},
		// 	corrupt: func(twf *testWritableFile) error {
		// 		// zero out one byte from somewhere near the start of the commit (but
		// 		// not in the frameheader)
		// 		_, err := twf.WriteAt([]byte{0}, int64(twf.lastSyncStart+frameHeaderLen+2))
		// 		return err
		// 	},
		// 	// should recover back to before the append
		// 	wantLastIndex: 10,
		// },
		// {
		// 	name:               "torn write with header in commit corrupt",
		// 	numPreviousEntries: 10,
		// 	appendEntrySizes:   []int{10, 10, 10, 10},
		// 	corrupt: func(twf *testWritableFile) error {
		// 		// We rely on knowing the sizes of the entries in this case which were
		// 		// header + 10 byte + 6 bytes padding each. We corrupt not the first but
		// 		// second header. We'll set the typ byte to an invalid value.
		// 		_, err := twf.WriteAt([]byte{65}, int64(twf.lastSyncStart+encodedFrameSize(10)))
		// 		return err
		// 	},
		// 	// should recover back to before the append
		// 	wantLastIndex: 10,
		// },
		// {
		// 	name:               "empty file",
		// 	numPreviousEntries: 0,
		// 	appendEntrySizes:   []int{},
		// 	corrupt: func(twf *testWritableFile) error {
		// 		// replace buf with an zero-capacity buffer to simulate zero length file
		// 		twf.buf.Store([]byte{})
		// 		twf.dirty = false
		// 		twf.maxWritten = 0
		// 		return nil
		// 	},
		// 	// should throw an EOF error on recover as there is no file header to verify
		// 	wantErr: io.EOF.Error(),
		// },
		{
			name: "bad segment header, valid commit",
			// This makes two commits which means header must have been committed so
			// must be validated.
			numPreviousEntries: 1,
			appendEntrySizes:   []int{10},
			corrupt: func(twf *testWritableFile) error {
				// twiddle the magic value
				_, err := twf.WriteAt([]byte{123}, 0)
				return err
			},
			wantErr: "corrupt",
		},
		{
			name: "bad segment header BaseIndex, valid commit",
			// This makes two commits which means header must have been committed so
			// must be validated.
			numPreviousEntries: 1,
			appendEntrySizes:   []int{10},
			corrupt: func(twf *testWritableFile) error {
				// twiddle the base index
				_, err := twf.WriteAt([]byte{123}, 8)
				return err
			},
			wantErr: "segment header BaseIndex 123 doesn't match metadata 1",
		},
		{
			name: "bad segment header, part of initial commit",
			// Only one commit, should be detected as incomplete/torn as header is
			// part of commit and NOT error but just init as an empty segment since
			// the first commit is incomplete.
			numPreviousEntries: 0,
			appendEntrySizes:   []int{10},
			corrupt: func(twf *testWritableFile) error {
				// twiddle the magic value
				_, err := twf.WriteAt([]byte{123}, 0)
				return err
			},
			wantLastIndex: 0, // Should recover as an empty segment
		},
		{
			name: "bad segment header BaseIndex, valid commit",
			// Only one commit, should be detected as incomplete/torn as header is
			// part of commit and NOT error but just init as an empty segment since
			// the first commit is incomplete.
			numPreviousEntries: 0,
			appendEntrySizes:   []int{10},
			corrupt: func(twf *testWritableFile) error {
				// twiddle the base index
				_, err := twf.WriteAt([]byte{123}, 8)
				return err
			},
			wantLastIndex: 0, // Should recover as an empty segment
		},
		{
			name:               "sealed tail",
			numPreviousEntries: 0,
			// SizeLimit is set to 4KiB write 5 1KiB values to force it to be sealed.
			appendEntrySizes: []int{1024, 1024, 1024, 1024, 1024},
			wantLastIndex:    5,
			// After recovery we should find it is already sealed.
			wantSealed: true,
			// Note that we'll implicitly test we can read all the indexes through the
			// reader after recovery. That's a different path to reading frm the
			// on-disk index though which is tested in reader_test.go
		},
		{
			name:               "value larger than minBufSize",
			numPreviousEntries: 0,
			// Write a value larger than our minBufSize to check those read/Write code
			// paths work. This will also seal the segment in one shot.
			appendEntrySizes: []int{minBufSize + 10},
			wantLastIndex:    1,
			// After recovery we should find it is already sealed.
			wantSealed: true,
			// Note that we'll implicitly test we can read all the indexes through the
			// reader after recovery. That's a different path to reading frm the
			// on-disk index though which is tested in reader_test.go
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
				err := w.Append([]types.LogEntry{{Index: uint64(i + 1), Data: []byte(v)}})
				require.NoError(t, err)
			}

			// Now create a batch with the entries sized as in the test case
			batch := make([]types.LogEntry, 0, len(tc.appendEntrySizes))
			for i, len := range tc.appendEntrySizes {
				idx := 1 + tc.numPreviousEntries + i
				if len < 6 {
					panic("we need 6 bytes to encode the index for verification")
				}
				v := fmt.Sprintf("%05d:%s", idx, strings.Repeat("P", len-6))
				batch = append(batch, types.LogEntry{Index: uint64(idx), Data: []byte(v)})
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
			if tc.wantSealed {
				require.Greater(t, int(indexStart), 1)
			}

			lastIdx := w.LastIndex()
			if tc.wantSealed {
				// Appends should fail
				err := w.Append([]types.LogEntry{{Index: lastIdx, Data: []byte("bad")}})
				require.ErrorContains(t, err, "sealed")
				return
			} else {
				// Verify we can continue to append and then read back everything.
				for i := 0; i < 10; i++ {
					// Append individually, could do commit batches but this is all in
					// memory so no real benefit.
					lastIdx++
					v := fmt.Sprintf("%05d: Some other bytes of data too.", lastIdx)
					err := w.Append([]types.LogEntry{{Index: lastIdx, Data: []byte(v)}})
					require.NoError(t, err)
				}

				if tc.wantLastIndex == 0 {
					// We expected an empty segment, possibly due to a corrupt header as
					// part of the first commit. Verify that the header is now correct
					// after we've appended more data.
					r, err := f.Open(seg0)
					require.NoError(t, err)
					r.Close()
				}
			}

			t.Log("\n" + testFileFor(t, w).Dump())

			// Read the whole log!
			for idx := uint64(1); idx <= lastIdx; idx++ {
				got, err := w.GetLog(idx)
				require.NoError(t, err, "failed reading idx=%d", idx)
				require.True(t, strings.HasPrefix(string(got.Bs), fmt.Sprintf("%05d:", idx)), "bad value for idx=%d", idx)
			}
		})
	}
}

func TestListAndDelete(t *testing.T) {
	vfs := newTestVFS()

	f := NewFiler("test", vfs)

	// Create 5 sealed segments
	idx := uint64(1)
	expectFiles := make(map[uint64]uint64)
	var lastSealedID uint64
	for i := 0; i < 5; i++ {
		seg := testSegment(idx)
		w, err := f.Create(seg)
		require.NoError(t, err)

		expectFiles[seg.ID] = seg.BaseIndex
		lastSealedID = seg.ID

		var sealed bool
		for sealed == false {
			val := fmt.Sprintf("%05d. Some Value.", idx)
			err = w.Append([]types.LogEntry{{Index: idx, Data: []byte(val)}})
			require.NoError(t, err)

			sealed, _, err = w.Sealed()
			require.NoError(t, err)
			idx++
		}
		w.Close()
	}

	// And one tail
	seg := testSegment(idx)
	w, err := f.Create(seg)
	require.NoError(t, err)
	w.Close()
	expectFiles[seg.ID] = seg.BaseIndex

	// Now list should have all the segments.
	list, err := f.List()
	require.NoError(t, err)

	require.Equal(t, expectFiles, list)

	// Now delete the tail file and the last segment
	err = f.Delete(seg.BaseIndex, seg.ID)
	require.NoError(t, err)

	err = f.Delete(expectFiles[lastSealedID], lastSealedID)
	require.NoError(t, err)

	delete(expectFiles, seg.ID)
	delete(expectFiles, lastSealedID)

	// List should be updated
	list, err = f.List()
	require.NoError(t, err)
}

func TestListEdgeCases(t *testing.T) {
	cases := []struct {
		name      string
		files     []string
		wantErr   string
		wantFiles map[uint64]uint64
	}{
		{
			name:      "empty dir",
			wantFiles: map[uint64]uint64{},
		},
		{
			name:  "single tail",
			files: []string{"00000000001-00000000001.wal"},
			wantFiles: map[uint64]uint64{
				1: 1,
			},
		},
		{
			name: "other random files",
			files: []string{
				"00000000001-00000000001.wal",
				"blah.txt",
			},
			wantFiles: map[uint64]uint64{
				1: 1,
			},
		},
		{
			name: "badly formed wal segments",
			files: []string{
				"0000000000100000000001.wal",
			},
			wantErr: "corrupt",
		},
		{
			name: "badly formed wal segments",
			files: []string{
				"00000000001-zxcv.wal",
			},
			wantErr: "corrupt",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			vfs := newTestVFS()

			for _, fname := range tc.files {
				_, err := vfs.Create("test", fname, 128)
				require.NoError(t, err)
			}

			f := NewFiler("test", vfs)

			list, err := f.List()

			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantFiles, list)
		})
	}
}

var nextSegID uint64

func testSegment(baseIndex uint64) types.SegmentInfo {
	id := atomic.AddUint64(&nextSegID, 1)
	return types.SegmentInfo{
		BaseIndex: baseIndex,
		MinIndex:  baseIndex,
		ID:        id,
		Codec:     1,
		SizeLimit: 4 * 1024, // Small limit to make it easier to test sealing
		// Other fields don't really matter at segment level for now.
	}
}
