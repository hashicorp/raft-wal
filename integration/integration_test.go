package integration

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/hashicorp/raft"
	wal "github.com/hashicorp/raft-wal"
	"github.com/hashicorp/raft-wal/metadb"
	"github.com/stretchr/testify/require"
)

type step func(w *wal.WAL) error

func TestIntegrationScenarios(t *testing.T) {
	cases := []struct {
		name                          string
		steps                         []step
		expectFirstIdx, expectLastIdx int
		expectNumSegments             int
	}{
		{
			name: "basic creation, appends, rotation",
			steps: []step{
				// ~256 bytes plus overhead per log want to write more than 4K segment
				// size. Batches of 4 are ~1k so 5 batches is enough to rotate once.
				appendLogsInBatches(5, 4),
			},
			expectFirstIdx:    1,
			expectLastIdx:     20,
			expectNumSegments: 2,
		},
		{
			name: "starting at high index, appends, rotation",
			steps: []step{
				appendFirstLogAt(1_000_000),
				// ~256 bytes plus overhead per log want to write more than 4K segment
				// size. Batches of 4 are ~1k so 5 batches is enough to rotate once.
				appendLogsInBatches(5, 4),
			},
			expectFirstIdx:    1_000_000,
			expectLastIdx:     1_000_020,
			expectNumSegments: 2,
		},
		{
			name: "head truncation deleting no files",
			steps: []step{
				appendLogsInBatches(11, 4),
				deleteRange(1, 2),
			},
			expectFirstIdx:    3,
			expectLastIdx:     44,
			expectNumSegments: 3,
		},
		{
			name: "head truncation deleting multiple files",
			steps: []step{
				appendLogsInBatches(11, 4),
				deleteRange(1, 20),
			},
			expectFirstIdx:    21,
			expectLastIdx:     44,
			expectNumSegments: 2,
		},
		{
			name: "tail truncation in active segment",
			steps: []step{
				appendLogsInBatches(11, 4),
				deleteRange(44, 44), // Delete the last one log
			},
			expectFirstIdx:    1,
			expectLastIdx:     43,
			expectNumSegments: 4,
		},
		{
			name: "tail truncation in active segment and write more",
			steps: []step{
				appendLogsInBatches(11, 4),
				deleteRange(44, 44), // Delete the last one log
				appendLogsInBatches(1, 4),
			},
			expectFirstIdx:    1,
			expectLastIdx:     47,
			expectNumSegments: 4,
		},
		{
			name: "tail truncation deleting files",
			steps: []step{
				appendLogsInBatches(11, 4),
				deleteRange(20, 44),
			},
			expectFirstIdx: 1,
			expectLastIdx:  19,
			// Only need 2 segments but the truncation will rotate to a new tail
			expectNumSegments: 3,
		},
		{
			name: "tail truncation deleting files and write more",
			steps: []step{
				appendLogsInBatches(11, 4),
				deleteRange(20, 44),
				appendLogsInBatches(1, 4),
			},
			expectFirstIdx: 1,
			expectLastIdx:  23,
			// Only need 2 segments but the truncation will rotate to a new tail
			expectNumSegments: 3,
		},
		{
			name: "write some logs, truncate everything, restart logs from different index",
			steps: []step{
				appendLogsInBatches(11, 4),
				deleteRange(1, 44),
				appendFirstLogAt(1000),
				appendLogsInBatches(1, 4),
			},
			expectFirstIdx:    1000,
			expectLastIdx:     1004,
			expectNumSegments: 1,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tmpDir, err := os.MkdirTemp("", tc.name)
			require.NoError(t, err)
			defer os.RemoveAll(tmpDir)

			// Wrap the BoltDB meta store so we can peek into it's values.
			meta := &PeekingMetaStore{
				meta: &metadb.BoltMetaDB{},
			}

			w, err := wal.Open(tmpDir,
				// 4k segments to test rotation quicker
				wal.WithSegmentSize(4096),
				wal.WithMetaStore(meta),
			)
			require.NoError(t, err)

			// Execute initial operations
			for i, step := range tc.steps {
				require.NoError(t, step(w), "failed on step %d", i)
			}

			// Assert expected properties
			assertLogContents(t, w, tc.expectFirstIdx, tc.expectLastIdx)
			assertNumSegments(t, meta, tmpDir, tc.expectNumSegments)

			// Close WAL and re-open
			require.NoError(t, w.Close())

			meta2 := &PeekingMetaStore{
				meta: &metadb.BoltMetaDB{},
			}

			w2, err := wal.Open(tmpDir,
				wal.WithSegmentSize(4096),
				wal.WithMetaStore(meta2),
			)
			require.NoError(t, err)
			defer w2.Close()

			// Assert expected properties still hold
			assertLogContents(t, w2, tc.expectFirstIdx, tc.expectLastIdx)
			assertNumSegments(t, meta2, tmpDir, tc.expectNumSegments)
		})
	}
}

func appendLogsInBatches(nBatches, nPerBatch int) step {
	return func(w *wal.WAL) error {
		lastIdx, err := w.LastIndex()
		if err != nil {
			return err
		}
		nextIdx := lastIdx + 1

		return appendLogsInBatchesStartingAt(w, nBatches, nPerBatch, int(nextIdx))
	}
}

func appendFirstLogAt(index int) step {
	return func(w *wal.WAL) error {
		return appendLogsInBatchesStartingAt(w, 1, 1, index)
	}
}

func appendLogsInBatchesStartingAt(w *wal.WAL, nBatches, nPerBatch, firstIndex int) error {
	nextIdx := uint64(firstIndex)

	batch := make([]*raft.Log, 0, nPerBatch)
	for b := 0; b < nBatches; b++ {
		for i := 0; i < nPerBatch; i++ {
			log := raft.Log{
				Index: nextIdx,
				Data:  makeValue(nextIdx),
			}
			batch = append(batch, &log)
			nextIdx++
		}
		if err := w.StoreLogs(batch); err != nil {
			return err
		}
		batch = batch[:0]
	}
	return nil
}

func makeValue(n uint64) []byte {
	// Values are 16 repetitions of a 16 byte string based on the index so 256
	// bytes total.
	return bytes.Repeat([]byte(fmt.Sprintf("val-%011d\n", n)), 16)
}

func deleteRange(min, max int) step {
	return func(w *wal.WAL) error {
		return w.DeleteRange(uint64(min), uint64(max))
	}
}

func assertLogContents(t *testing.T, w *wal.WAL, first, last int) {
	t.Helper()

	firstIdx, err := w.FirstIndex()
	require.NoError(t, err)
	lastIdx, err := w.LastIndex()
	require.NoError(t, err)

	require.Equal(t, first, int(firstIdx))
	require.Equal(t, last, int(lastIdx))

	var log raft.Log
	for i := first; i <= last; i++ {
		err := w.GetLog(uint64(i), &log)
		require.NoError(t, err, "log index %d", i)
		require.Equal(t, i, int(log.Index), "log index %d", i)
		require.Equal(t, string(makeValue(log.Index)), string(log.Data), "log index %d", i)
	}
}

func assertNumSegments(t *testing.T, meta *PeekingMetaStore, dir string, numSegments int) {
	t.Helper()

	state := meta.PeekState()
	require.Equal(t, numSegments, len(state.Segments))

	// Check the right number of segment files on disk too
	des, err := os.ReadDir(dir)
	require.NoError(t, err)

	segFiles := make([]string, 0, numSegments)
	for _, de := range des {
		if de.IsDir() {
			continue
		}
		if strings.HasSuffix(de.Name(), ".wal") {
			segFiles = append(segFiles, de.Name())
		}
	}
	require.Equal(t, numSegments, len(segFiles), "expected two segment files, got %v", segFiles)
}
