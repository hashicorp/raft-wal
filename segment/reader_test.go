// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package segment

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft-wal/types"
	"github.com/stretchr/testify/require"
)

type entryDesc struct {
	len, num int
}

func TestReader(t *testing.T) {
	cases := []struct {
		name          string
		firstIndex    uint64
		entries       []entryDesc
		corrupt       func(twf *testWritableFile) error
		wantLastIndex uint64
		wantOpenErr   string
	}{
		{
			name:       "basic sealed",
			firstIndex: 1,
			entries: []entryDesc{
				// 28 * 128 bytes entries are all that will fit in a 4KiB segment after
				// headers and index size are accounted for.
				{len: 128, num: 28},
			},
			wantLastIndex: 28,
		},
		{
			name:       "value larger than minBufSize",
			firstIndex: 1,
			entries: []entryDesc{
				{len: 128, num: 5},
				{len: minBufSize + 10, num: 1},
			},
			wantLastIndex: 6,
		},
		{
			name:       "sealed file truncated",
			firstIndex: 1,
			entries: []entryDesc{
				{len: 128, num: 28},
			},
			corrupt: func(twf *testWritableFile) error {
				twf.Truncate(0)
				return nil
			},
			wantOpenErr: "corrupt",
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
			t.Cleanup(func() {
				_ = w.Close()
			})

			// Append previous entries. We just pick a fixed size and format that's
			// easy to verify but generally fits in our test block size.
			idx := tc.firstIndex
			wantLength := make(map[uint64]int)
			for _, desc := range tc.entries {
				// Append individually, could do commit batches but this is all in
				// memory so no real benefit.
				padLen := 0
				if desc.len > 6 {
					padLen = desc.len - 6
				}
				padding := strings.Repeat("P", padLen)
				for range desc.num {
					v := fmt.Sprintf("%05d:%s", idx, padding)
					err := w.Append([]types.LogEntry{{Index: idx, Data: []byte(v)}})
					require.NoError(t, err, "error appending entry idx=%d", idx)
					wantLength[idx] = desc.len
					idx++
				}
			}

			// Should have sealed
			sealed, indexStart, err := w.Sealed()
			require.NoError(t, err)
			require.True(t, sealed)

			if tc.corrupt != nil {
				file := testFileFor(t, w)
				require.NoError(t, tc.corrupt(file))
			}

			seg0.IndexStart = indexStart
			seg0.MaxIndex = w.LastIndex()
			seg0.SealTime = time.Now()

			// Now open the "file" with a reader.
			r, err := f.Open(seg0)

			if tc.wantOpenErr != "" {
				require.ErrorContains(t, err, tc.wantOpenErr)
				return
			}
			require.NoError(t, err)

			// Make sure we can read every value
			for idx := tc.firstIndex; idx <= tc.wantLastIndex; idx++ {
				got, err := r.GetLog(idx)
				require.NoError(t, err, "error reading idx=%d", idx)
				require.True(t, strings.HasPrefix(string(got.Bs), fmt.Sprintf("%05d:", idx)), "bad value for idx=%d", idx)
				require.Len(t, string(got.Bs), wantLength[idx])
			}

			// And we should _not_ read one either side
			if tc.firstIndex > 1 {
				_, err := r.GetLog(tc.firstIndex - 1)
				require.ErrorIs(t, err, types.ErrNotFound)
			}
			_, err = r.GetLog(tc.wantLastIndex + 1)
			require.ErrorIs(t, err, types.ErrNotFound)
		})
	}
}
