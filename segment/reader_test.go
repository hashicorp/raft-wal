// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package segment

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/go-wal"
	"github.com/stretchr/testify/require"
)

type entryDesk struct {
	len, num int
}

func TestReader(t *testing.T) {
	cases := []struct {
		name          string
		firstIndex    uint64
		entries       []entryDesk
		wantLastIndex uint64
		wantOpenErr   string
	}{
		{
			name:       "basic sealed",
			firstIndex: 1,
			entries: []entryDesk{
				{len: 128, num: 28}, // 4KiB segment size
			},
			wantLastIndex: 28,
		},
		{
			name:       "value larger than minBufSize",
			firstIndex: 1,
			entries: []entryDesk{
				{len: 128, num: 5},
				{len: minBufSize + 10, num: 1},
			},
			wantLastIndex: 6,
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
			idx := tc.firstIndex
			for _, desc := range tc.entries {
				// Append individually, could do commit batches but this is all in
				// memory so no real benefit.
				padLen := 0
				if desc.len > 6 {
					padLen = desc.len - 6
				}
				padding := strings.Repeat("P", padLen)
				for i := 0; i < desc.num; i++ {
					v := fmt.Sprintf("%05d:%s", idx, padding)
					err := w.Append([]wal.LogEntry{{Index: idx, Data: []byte(v)}})
					require.NoError(t, err, "error appending entry idx=%d", idx)
					idx++
				}
			}

			// Should have sealed
			sealed, indexStart, err := w.Sealed()
			require.NoError(t, err)
			require.True(t, sealed)

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
				gotBs, err := r.GetLog(idx)
				require.NoError(t, err, "error reading idx=%d", idx)
				require.True(t, strings.HasPrefix(string(gotBs), fmt.Sprintf("%05d:", idx)), "bad value for idx=%d", idx)
			}

			// And we should _not_ read one either side
			if tc.firstIndex > 1 {
				_, err := r.GetLog(tc.firstIndex - 1)
				require.ErrorIs(t, err, wal.ErrNotFound)
			}
			_, err = r.GetLog(tc.wantLastIndex + 1)
			require.ErrorIs(t, err, wal.ErrNotFound)
		})
	}
}
