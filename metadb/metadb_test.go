// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package metadb

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/raft-wal/types"
	"github.com/stretchr/testify/require"
)

func TestMetaDB(t *testing.T) {
	cases := []struct {
		name        string
		writeState  *types.PersistentState
		writeStable map[string][]byte
	}{
		{
			name:       "basic storage",
			writeState: makeState(4),
			writeStable: map[string][]byte{
				"CurrentTerm":  []byte{0, 0, 0, 0, 0, 0, 0, 5},
				"LastVoteTerm": []byte{0, 0, 0, 0, 0, 0, 0, 5},
				"LastVoteCand": []byte("server1"),
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			tmpDir, err := os.MkdirTemp("", "raft-wal-meta-test-*")
			require.NoError(t, err)
			defer os.RemoveAll(tmpDir)

			{
				// Should be able to load the DB
				var db BoltMetaDB
				gotState, err := db.Load(tmpDir)
				require.NoError(t, err)
				defer db.Close()

				require.Equal(t, 0, int(gotState.NextSegmentID))
				require.Empty(t, gotState.Segments)

				if tc.writeState != nil {
					require.NoError(t, db.CommitState(*tc.writeState))
				}
				for k, v := range tc.writeStable {
					require.NoError(t, db.SetStable([]byte(k), v))
				}

				// Close DB and re-open a new one to ensure persistence.
				db.Close()
			}

			var db BoltMetaDB
			gotState, err := db.Load(tmpDir)
			require.NoError(t, err)

			require.Equal(t, *tc.writeState, gotState)

			for k, v := range tc.writeStable {
				got, err := db.GetStable([]byte(k))
				require.NoError(t, err)
				require.Equal(t, v, got)
			}
		})
	}
}

func TestMetaDBErrors(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "raft-wal-meta-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	var db BoltMetaDB

	// Calling anything before load is an error
	require.ErrorIs(t, db.CommitState(types.PersistentState{NextSegmentID: 1234}), ErrUnintialized)

	_, err = db.GetStable([]byte("foo"))
	require.ErrorIs(t, err, ErrUnintialized)

	err = db.SetStable([]byte("foo"), []byte("bar"))
	require.ErrorIs(t, err, ErrUnintialized)

	// Loading twice is OK from same dir
	_, err = db.Load(tmpDir)
	require.NoError(t, err)
	_, err = db.Load(tmpDir)
	require.NoError(t, err)

	// But not from a different (valid) one
	tmpDir2, err := ioutil.TempDir("", "wal-fs-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir2)

	_, err = db.Load(tmpDir2)
	require.ErrorContains(t, err, "already open in dir")

	// Loading from a non-existent dir is an error
	var db2 BoltMetaDB
	_, err = db2.Load("fake-dir-that-does-not-exist")
	require.ErrorContains(t, err, "no such file or directory")
}

func makeState(nSegs int) *types.PersistentState {
	startIdx := 1000
	perSegment := 100
	startID := 1234
	// Times are pesky remove as much stuff that doesn't survive serilisation as
	// possible as we don't really care about it!
	startTime := time.Now().UTC().Round(time.Second).Add(time.Duration(-1*nSegs) * time.Minute)

	state := &types.PersistentState{
		NextSegmentID: uint64(startID + nSegs),
	}

	for i := 0; i < (nSegs - 1); i++ {
		si := types.SegmentInfo{
			ID:         uint64(startID + i),
			BaseIndex:  uint64(startIdx + (i * perSegment)),
			MinIndex:   uint64(startIdx + (i * perSegment)),
			MaxIndex:   uint64(startIdx + ((i + 1) * perSegment) - 1),
			Codec:      1,
			IndexStart: 123456,
			CreateTime: startTime.Add(time.Duration(i) * time.Minute),
			SealTime:   startTime.Add(time.Duration(i+1) * time.Minute),
			SizeLimit:  64 * 1024 * 1024,
		}
		state.Segments = append(state.Segments, si)
	}
	if nSegs > 0 {
		// Append an unsealed tail
		i := nSegs - 1
		si := types.SegmentInfo{
			ID:         uint64(startID + i),
			BaseIndex:  uint64(startIdx + (i * perSegment)),
			MinIndex:   uint64(startIdx + (i * perSegment)),
			Codec:      1,
			CreateTime: startTime.Add(time.Duration(i) * time.Minute),
			SizeLimit:  64 * 1024 * 1024,
		}
		state.Segments = append(state.Segments, si)
	}
	return state
}
