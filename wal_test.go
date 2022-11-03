// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package wal

import (
	"context"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestWALOpen(t *testing.T) {
	cases := []struct {
		name               string
		tsOpts             []testStorageOpt
		walOpts            []walOpt
		expectErr          string
		expectSegmentBases []uint64
		expectCalls        map[string]int
		ignoreInvalidMeta  bool

		// validate recovery of data
		expectFirstIndex uint64
		expectLastIndex  uint64
	}{
		{
			name: "invalid custom codec",
			walOpts: []walOpt{
				WithCodec(&BinaryCodec{}),
			},
			expectErr: "codec is using a reserved ID",
		},
		{
			name: "empty dir creates new log",
			// Should end up with one segment created with baseIndex 1
			expectSegmentBases: []uint64{1},
			expectCalls: map[string]int{
				"Create":      1,
				"CommitState": 1,
				"List":        1,
				"Load":        1,
			},
			expectFirstIndex: 0,
			expectLastIndex:  0,
		},
		{
			name: "single segment recovery",
			tsOpts: []testStorageOpt{
				segTail(10), // Single tail segment with 10 entries
			},
			expectSegmentBases: []uint64{1},
			expectCalls: map[string]int{
				"List":        1,
				"Load":        1,
				"RecoverTail": 1,
			},
			expectFirstIndex: 1,
			expectLastIndex:  10,
		},
		{
			name: "multiple segment recovery",
			tsOpts: []testStorageOpt{
				segFull(),
				segFull(),
				segTail(10),
			},
			expectSegmentBases: []uint64{1, 101, 201},
			expectCalls: map[string]int{
				"List":        1,
				"Load":        1,
				"RecoverTail": 1,
				"Open":        2,
			},
			expectFirstIndex: 1,
			expectLastIndex:  210,
		},
		{
			name: "metadb load fails",
			tsOpts: []testStorageOpt{
				func(ts *testStorage) {
					ts.loadErr = os.ErrNotExist
				},
			},
			expectErr: "file does not exist",
		},
		{
			// This is kinda far-fetched since meta db is typically in same Dir, but
			// it _might_ not be with a custom MetaStore.
			name: "list fails",
			tsOpts: []testStorageOpt{
				func(ts *testStorage) {
					ts.listErr = os.ErrNotExist
				},
			},
			expectErr: "file does not exist",
		},
		{
			name: "invalid codec",
			tsOpts: []testStorageOpt{
				segFull(),
				segFull(),
				segTail(10),
				func(ts *testStorage) {
					// Invalid codec we don't know how to decode.
					ts.metaState.Segments[0].Codec = 1234
				},
			},
			expectErr: "uses an unknown codec",
		},
		{
			name: "recover tail fails",
			tsOpts: []testStorageOpt{
				segTail(10),
				func(ts *testStorage) {
					ts.recoverErr = ErrCorrupt
				},
			},
			expectErr: "corrupt",
		},
		{
			name: "open fails",
			tsOpts: []testStorageOpt{
				segFull(),
				segTail(10),
				func(ts *testStorage) {
					ts.openErr = os.ErrNotExist
				},
			},
			expectErr: "file does not exist",
		},
		{
			name: "commit fails",
			tsOpts: []testStorageOpt{
				func(ts *testStorage) {
					ts.commitErr = os.ErrPermission
				},
			},
			expectErr: "permission denied",
		},
		{
			name: "commit fails",
			tsOpts: []testStorageOpt{
				func(ts *testStorage) {
					ts.createErr = os.ErrPermission
				},
			},
			expectErr: "permission denied",
		},
		{
			name: "recover tampered meta store",
			tsOpts: []testStorageOpt{
				segFull(),
				segTail(10), // multiple tails is invalid
				segTail(10),
				func(ts *testStorage) {
					ts.createErr = os.ErrPermission
				},
			},
			expectErr:         "unsealed segment is not at tail",
			ignoreInvalidMeta: true, // disable the test's setup checks on validity
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ts, w, err := testOpenWAL(t, tc.tsOpts, tc.walOpts, tc.ignoreInvalidMeta)

			// Error or not we should never commit an invalid set of segments to meta.
			if !tc.ignoreInvalidMeta {
				ts.assertValidMetaState(t)
			}

			if tc.expectErr != "" {
				require.ErrorContains(t, err, tc.expectErr)
				return
			}
			require.NoError(t, err)

			// Check we ended up with the segments we expected
			segmentBases := make([]uint64, 0)
			for _, seg := range ts.segments {
				segmentBases = append(segmentBases, seg.info().BaseIndex)
			}
			sort.Slice(segmentBases, func(i, j int) bool {
				return segmentBases[i] < segmentBases[j]
			})

			committedSegmentBases := make([]uint64, 0)
			for _, seg := range ts.metaState.Segments {
				committedSegmentBases = append(committedSegmentBases, seg.BaseIndex)
			}
			require.Equal(t, tc.expectSegmentBases, segmentBases)
			require.Equal(t, tc.expectSegmentBases, committedSegmentBases)
			if tc.expectCalls != nil {
				require.Equal(t, tc.expectCalls, ts.calls)
			}

			// Validate the data was recovered and is readable
			first, err := w.FirstIndex()
			require.NoError(t, err)
			require.Equal(t, tc.expectFirstIndex, first)
			last, err := w.LastIndex()
			require.NoError(t, err)
			require.Equal(t, tc.expectLastIndex, last)

			// Every index between first and last inclusive should be readable
			var log, log2 raft.Log
			if last > 0 {
				for idx := first; idx <= last; idx++ {
					err := w.GetLog(idx, &log)
					require.NoError(t, err)
					require.Equal(t, idx, log.Index, "WAL returned wrong entry for index")
					validateLogEntry(t, &log)
				}
			}

			// We should _not_ be able to get logs outside of the expected range
			if first > 0 {
				err := w.GetLog(first-1, &log)
				require.ErrorIs(t, err, ErrNotFound)
			}
			err = w.GetLog(last+1, &log)
			require.ErrorIs(t, err, ErrNotFound)

			// Basic test that appending after recovery works
			log.Index = last + 1
			log.Data = []byte("appended")
			err = w.StoreLog(&log)
			require.NoError(t, err)

			err = w.GetLog(last+1, &log2)
			require.NoError(t, err)
			require.Equal(t, last+1, log2.Index)
			require.Equal(t, "appended", string(log2.Data))
		})
	}
}

func TestStoreLogs(t *testing.T) {
	cases := []struct {
		name      string
		tsOpts    []testStorageOpt
		walOpts   []walOpt
		store     []*raft.Log
		store2    []*raft.Log
		expectErr string
		// validate recovery of data
		expectFirstIndex uint64
		expectLastIndex  uint64
	}{
		// {
		// 	name:             "empty log append",
		// 	store:            makeRaftLogs(1, 5),
		// 	expectFirstIndex: 1,
		// 	expectLastIndex:  5,
		// },
		// {
		// 	name:             "empty log append, start from non-zero",
		// 	store:            makeRaftLogs(10000, 5),
		// 	expectFirstIndex: 10000,
		// 	expectLastIndex:  10004,
		// },
		// {
		// 	name: "existing segment log append",
		// 	tsOpts: []testStorageOpt{
		// 		segTail(10),
		// 	},
		// 	store:            makeRaftLogs(11, 5),
		// 	expectFirstIndex: 1,
		// 	expectLastIndex:  15,
		// },
		// {
		// 	name: "existing multi segment log append",
		// 	tsOpts: []testStorageOpt{
		// 		segFull(),
		// 		segFull(),
		// 		segTail(10),
		// 	},
		// 	store:            makeRaftLogs(211, 5),
		// 	expectFirstIndex: 1,
		// 	expectLastIndex:  215,
		// },
		// {
		// 	name: "out of order",
		// 	tsOpts: []testStorageOpt{
		// 		segTail(10),
		// 	},
		// 	// Append logs starting from 100 when tail is currently 10
		// 	store:     makeRaftLogs(100, 5),
		// 	expectErr: "non-monotonic log entries",
		// },
		// {
		// 	name: "out of sequence",
		// 	tsOpts: []testStorageOpt{
		// 		segTail(10),
		// 	},
		// 	// Append logs starting from 10 but not with gaps internally
		// 	store:     makeRaftLogsSparse(10, 11, 14, 15),
		// 	expectErr: "non-monotonic log entries",
		// },
		// {
		// 	name: "rotate when full",
		// 	tsOpts: []testStorageOpt{
		// 		segTail(99),
		// 	},
		// 	store:            makeRaftLogs(100, 5),
		// 	expectFirstIndex: 1,
		// 	expectLastIndex:  104,
		// },
		// {
		// 	name: "rotate and append more",
		// 	tsOpts: []testStorageOpt{
		// 		segTail(99),
		// 	},
		// 	store:            makeRaftLogs(100, 5),
		// 	store2:           makeRaftLogs(105, 5),
		// 	expectFirstIndex: 1,
		// 	expectLastIndex:  109,
		// },
		{
			name: "empty rotate and append more",
			// Start from empty (this had a bug initially since open didn't initialize
			// the reader but that is only noticed after the initial segment is no
			// longer the tail and you attempt to fetch.
			tsOpts:           []testStorageOpt{},
			store:            makeRaftLogs(100, 101),
			store2:           makeRaftLogs(201, 5),
			expectFirstIndex: 100,
			expectLastIndex:  205,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ts, w, err := testOpenWAL(t, tc.tsOpts, tc.walOpts, false)
			require.NoError(t, err)

			err = w.StoreLogs(tc.store)

			if tc.expectErr != "" {
				require.ErrorContains(t, err, tc.expectErr)
				return
			}
			require.NoError(t, err)

			if len(tc.store2) > 0 {
				err = w.StoreLogs(tc.store2)
				require.NoError(t, err)
			}

			t.Log(ts.debugDump())

			// Validate the data was recovered and is readable
			first, err := w.FirstIndex()
			require.NoError(t, err)
			require.Equal(t, int(tc.expectFirstIndex), int(first))
			last, err := w.LastIndex()
			require.NoError(t, err)
			require.Equal(t, int(tc.expectLastIndex), int(last))

			// Check all log entries exist that are meant to
			if tc.expectLastIndex > 0 {
				firstAppended := tc.expectLastIndex - uint64(len(tc.store)+len(tc.store2)) + 1
				// Start one _before_ the appended logs (assuming there was one before)
				// to verify that we didn't accidentally overwrite the previous tail.
				start := firstAppended - 1
				if start < tc.expectFirstIndex {
					start = tc.expectFirstIndex
				}
				var log raft.Log
				for idx := start; idx <= tc.expectLastIndex; idx++ {
					err := w.GetLog(idx, &log)
					require.NoError(t, err, "failed to find log %d", idx)
					require.Equal(t, int(idx), int(log.Index))
					if idx >= firstAppended {
						appendedOffset := int(idx - firstAppended)
						var want *raft.Log
						if appendedOffset < len(tc.store) {
							want = tc.store[appendedOffset]
						} else {
							want = tc.store2[appendedOffset-len(tc.store)]
						}
						require.Equal(t, int(want.Index), int(log.Index))
						require.Equal(t, want.Data, log.Data)
						require.Equal(t, want.AppendedAt.Round(1), log.AppendedAt.Round(1))
					}
				}
			}
		})
	}
}

func TestDeleteRange(t *testing.T) {
	cases := []struct {
		name      string
		tsOpts    []testStorageOpt
		walOpts   []walOpt
		deleteMin uint64
		deleteMax uint64
		expectErr string
		// validate recovery of data
		expectFirstIndex       uint64
		expectLastIndex        uint64
		expectDeleted          []uint64
		expectNHeadTruncations uint64
		expectNTailTruncations uint64
	}{
		{
			name: "no-op empty range",
			tsOpts: []testStorageOpt{
				firstIndex(1000),
				segFull(),
				segTail(10),
			},
			// Delete a range before the log starts
			deleteMin:        1000,
			deleteMax:        999,
			expectFirstIndex: 1000,
			expectLastIndex:  1000 + 100 + 10 - 1,
		},
		{
			name: "no-op truncate from head",
			tsOpts: []testStorageOpt{
				firstIndex(1000),
				segFull(),
				segTail(10),
			},
			// Delete a range before the log starts
			deleteMin:        0,
			deleteMax:        100,
			expectFirstIndex: 1000,
			expectLastIndex:  1000 + 100 + 10 - 1,
		},
		{
			name: "no-op truncate from after tail",
			tsOpts: []testStorageOpt{
				firstIndex(1000),
				segFull(),
				segTail(10),
			},
			// Delete a range before the log starts
			deleteMin:        5000,
			deleteMax:        5100,
			expectFirstIndex: 1000,
			expectLastIndex:  1000 + 100 + 10 - 1,
		},
		{
			name: "error: truncate from middle",
			tsOpts: []testStorageOpt{
				firstIndex(1000),
				segFull(),
				segTail(10),
			},
			// Delete a range before the log starts
			deleteMin: 1010,
			deleteMax: 1020,
			expectErr: "only suffix or prefix ranges may be deleted from log",
		},
		{
			name: "in-segment head truncation",
			tsOpts: []testStorageOpt{
				firstIndex(1000),
				segFull(),
				segTail(10),
			},
			// Delete a range before the log starts
			deleteMin:              0,
			deleteMax:              1010,
			expectFirstIndex:       1011,
			expectLastIndex:        1000 + 100 + 10 - 1,
			expectNHeadTruncations: 11,
		},
		{
			name: "segment deleting head truncation",
			tsOpts: []testStorageOpt{
				firstIndex(1000),
				segFull(),
				segTail(10),
			},
			// Delete a range before the log starts start exactly on firstIndex to
			// test boundary conditions.
			deleteMin:              1000,
			deleteMax:              1101, // Leave some entries in the tail
			expectFirstIndex:       1102,
			expectLastIndex:        1000 + 100 + 10 - 1,
			expectDeleted:          []uint64{1000},
			expectNHeadTruncations: 102,
		},
		{
			name: "head truncation deleting all segments",
			tsOpts: []testStorageOpt{
				firstIndex(1000),
				segFull(),
				segTail(10),
			},
			// Delete a range before the log starts start exactly on firstIndex to
			// test boundary conditions.
			deleteMin:              1000,
			deleteMax:              1000 + 100 + 10 - 1,
			expectFirstIndex:       0,
			expectLastIndex:        0,
			expectDeleted:          []uint64{1000, 1100},
			expectNHeadTruncations: 110,
		},
		{
			name: "non-deleting tail-truncation",
			tsOpts: []testStorageOpt{
				firstIndex(1000),
				segFull(),
				segTail(10),
			},
			// Delete the last two entries from the log
			deleteMin:              1000 + 100 + 10 - 2,
			deleteMax:              1000 + 100 + 10 - 1,
			expectFirstIndex:       1000,
			expectLastIndex:        1107,
			expectDeleted:          []uint64{},
			expectNTailTruncations: 2,
		},
		{
			name: "tail-deleting tail-truncation",
			tsOpts: []testStorageOpt{
				firstIndex(1000),
				segFull(),
				segTail(10),
			},
			deleteMin:              1051,
			deleteMax:              1000 + 100 + 10 - 1,
			expectFirstIndex:       1000,
			expectLastIndex:        1050,
			expectDeleted:          []uint64{1100},
			expectNTailTruncations: 59,
		},
		{
			name: "multi-segment deleting tail-truncation",
			tsOpts: []testStorageOpt{
				firstIndex(1000),
				segFull(),
				segFull(),
				segTail(10),
			},
			deleteMin:              1051,
			deleteMax:              1000 + 100 + 100 + 10 - 1,
			expectFirstIndex:       1000,
			expectLastIndex:        1050,
			expectDeleted:          []uint64{1100, 1200},
			expectNTailTruncations: 159,
		},
		{
			name: "everything deleting truncation",
			tsOpts: []testStorageOpt{
				firstIndex(1000),
				segFull(),
				segFull(),
				segTail(10),
			},
			deleteMin:        1,
			deleteMax:        1000 + 100 + 100 + 10 - 1,
			expectFirstIndex: 0,
			expectLastIndex:  0,
			expectDeleted:    []uint64{1000, 1100, 1200},
			// This is technically neither tail nor head since all entries are being
			// removed but since head deletions are simpler we treat it as a head
			// deletion.
			expectNHeadTruncations: 210,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ts, w, err := testOpenWAL(t, tc.tsOpts, tc.walOpts, false)
			require.NoError(t, err)

			err = w.DeleteRange(tc.deleteMin, tc.deleteMax)

			if tc.expectErr != "" {
				require.ErrorContains(t, err, tc.expectErr)
				return
			}
			require.NoError(t, err)

			t.Log(ts.debugDump())

			// Validate the data was recovered and is readable
			first, err := w.FirstIndex()
			require.NoError(t, err)
			require.Equal(t, int(tc.expectFirstIndex), int(first), "unexpected first index")
			last, err := w.LastIndex()
			require.NoError(t, err)
			require.Equal(t, int(tc.expectLastIndex), int(last), "unexpected last index")

			// Check all log entries exist that are meant to
			var log raft.Log

			if tc.expectLastIndex > 0 {
				for idx := tc.expectFirstIndex; idx <= tc.expectLastIndex; idx++ {
					err := w.GetLog(idx, &log)
					require.NoError(t, err, "failed to find log %d", idx)
					require.Equal(t, int(idx), int(log.Index))
					validateLogEntry(t, &log)
				}
			}

			// We should _not_ be able to get logs outside of the expected range
			if first > 0 {
				err := w.GetLog(first-1, &log)
				require.ErrorIs(t, err, ErrNotFound)
			}
			err = w.GetLog(last+1, &log)
			require.ErrorIs(t, err, ErrNotFound)

			if len(tc.expectDeleted) > 0 {
				ts.assertDeletedAndClosed(t, tc.expectDeleted...)
			}

			// Now validate we can continue to append as expected.
			nextIdx := tc.expectLastIndex + 1
			err = w.StoreLogs(makeRaftLogsSparse(nextIdx))
			require.NoError(t, err)
			last, err = w.LastIndex()
			require.NoError(t, err)
			require.Equal(t, int(nextIdx), int(last))

			// and read it back
			err = w.GetLog(nextIdx, &log)
			require.NoError(t, err, "failed to find appended log %d", nextIdx)
			require.Equal(t, int(nextIdx), int(log.Index))
			validateLogEntry(t, &log)

			// Verify the metrics recorded what we expected!
			metrics := w.Metrics()
			require.Equal(t, int(tc.expectNTailTruncations), int(metrics["tail_truncations"]))
			require.Equal(t, int(tc.expectNHeadTruncations), int(metrics["head_truncations"]))
		})
	}
}

func TestStable(t *testing.T) {
	_, w, err := testOpenWAL(t, nil, nil, false)
	require.NoError(t, err)

	// Should be empty initially.
	got, err := w.Get([]byte("test"))
	require.NoError(t, err)
	require.Nil(t, got)

	gotInt, err := w.GetUint64([]byte("testInt"))
	require.NoError(t, err)
	require.Equal(t, uint64(0), gotInt)

	err = w.Set([]byte("test"), []byte("foo"))
	require.NoError(t, err)
	err = w.SetUint64([]byte("testInt"), 1234)
	require.NoError(t, err)

	// Should return values
	got, err = w.Get([]byte("test"))
	require.NoError(t, err)
	require.Equal(t, "foo", string(got))

	gotInt, err = w.GetUint64([]byte("testInt"))
	require.NoError(t, err)
	require.Equal(t, uint64(1234), gotInt)
}

func TestStableRecovery(t *testing.T) {
	tsOpts := []testStorageOpt{
		stable("test", "foo"),
		stableInt("testInt", 1234),
	}
	_, w, err := testOpenWAL(t, tsOpts, nil, false)
	require.NoError(t, err)

	// Should return "persisted" values
	got, err := w.Get([]byte("test"))
	require.NoError(t, err)
	require.Equal(t, "foo", string(got))

	gotInt, err := w.GetUint64([]byte("testInt"))
	require.NoError(t, err)
	require.Equal(t, uint64(1234), gotInt)
}

// TestConcurrentReadersAndWriter is designed to be run with race detector
// enabled to validate the concurrent behavior of the WAL.
func TestConcurrentReadersAndWriter(t *testing.T) {
	_, w, err := testOpenWAL(t, nil, nil, false)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start writers, we logically only support a single writer since that is
	// typical in raft but validate that even if appending and truncation are
	// triggered in separate threads there is no race.
	appender := func() {
		index := uint64(1000)
		for {
			if ctx.Err() != nil {
				return
			}

			err := w.StoreLogs(makeRaftLogsSparse(index))
			if err != nil {
				panic("error during append: " + err.Error())
			}
			if index%1009 == 0 {
				// Every 1009 (prime so it doesn't always fall on segment boundary)
				// entries, truncate the last one and rewrite it to simulate head
				// truncations too (which are always done synchronously with appends in
				// raft).
				err = w.DeleteRange(index, index) // delete the last one (include min/max)
				if err != nil {
					panic("error during delete range: " + err.Error())
				}
				// Validate that we actually truncated
				last, err := w.LastIndex()
				if err != nil {
					panic("error fetching last: " + err.Error())
				}
				if last != index-1 {
					panic(fmt.Sprintf("didn't truncate tail expected last=%d got last=%d", index-1, last))
				}
				// Write it back again so we don't get stuck on this index forever on the next loop or leave a gap!
				err = w.StoreLogs(makeRaftLogsSparse(index))
				if err != nil {
					panic("error during append after truncate: " + err.Error())
				}
				t.Logf("truncated tail and appended to %d", index)
			}
			index++

			// Tiny sleep to stop CPUs burning
			time.Sleep(250 * time.Microsecond)
		}
	}

	truncator := func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
				// continue below
			}

			last, err := w.LastIndex()
			if err != nil {
				panic("error fetching last: " + err.Error())
			}

			err = w.DeleteRange(0, last-100) // Keep the last 100 entries
			if err != nil {
				panic("error during delete range: " + err.Error())
			}

			first, err := w.FirstIndex()
			if err != nil {
				panic("error fetching first: " + err.Error())
			}
			if first != last-99 {
				panic(fmt.Sprintf("truncate didn't work, expected first=%d got first=%d", last-99, first))
			}
			t.Logf("truncated head to %d", last-99)
		}
	}

	reader := func() {
		var log raft.Log
		var total int
		var good int
		for {
			select {
			case <-ctx.Done():
				// On completion, validate that we had at least _some_ percentage of
				// succesful reads and didn't just get NotFound on every iteration!
				if total > 0 && good == 0 {
					panic("reader only every got ErrNotFound")
				}
				return
			case <-time.After(1 * time.Millisecond):
				// continue below
			}

			// Read first entry, middle entry and last entry to make sure we hit all
			// segment types.
			first, err := w.FirstIndex()
			if err != nil {
				panic("error fetching first: " + err.Error())
			}
			last, err := w.LastIndex()
			if err != nil {
				panic("error fetching last: " + err.Error())
			}

			middle := (last - first) / 2

			get := func(name string, idx uint64) int {
				err = w.GetLog(idx, &log)
				if err == ErrNotFound {
					return 0
				}
				if err != nil {
					panic("error fetching log: " + err.Error())
				}
				if log.Index != idx {
					panic(fmt.Errorf("wrong %s log want=%d got=%d", name, idx, log.Index))
				}
				return 1
			}

			total += 3
			good += get("first", first)
			good += get("middle", middle)
			good += get("last", last)
		}
	}

	// Run writers
	go appender()
	go truncator()

	// Run 10 readers
	for i := 0; i < 1; /*0*/ i++ {
		go reader()
	}

	// Wait for timeout
	<-ctx.Done()
}

func TestClose(t *testing.T) {
	// Multiple "files" to open
	opts := []testStorageOpt{
		segFull(),
		segFull(),
		segFull(),
		segTail(1),
	}

	ts, w, err := testOpenWAL(t, opts, nil, false)
	require.NoError(t, err)

	// ensure files are actually open
	ts.assertAllClosed(t, false)

	// Close and check
	require.NoError(t, w.Close())
	ts.assertAllClosed(t, true)

	// Ensure all public methods now return an error
	_, err = w.FirstIndex()
	require.ErrorIs(t, err, ErrClosed)

	_, err = w.LastIndex()
	require.ErrorIs(t, err, ErrClosed)

	var log raft.Log
	err = w.GetLog(1, &log)
	require.ErrorIs(t, err, ErrClosed)

	err = w.StoreLog(&log)
	require.ErrorIs(t, err, ErrClosed)

	err = w.StoreLogs([]*raft.Log{&log})
	require.ErrorIs(t, err, ErrClosed)

	err = w.DeleteRange(1, 2)
	require.ErrorIs(t, err, ErrClosed)

	_, err = w.Get([]byte("foo"))
	require.ErrorIs(t, err, ErrClosed)

	_, err = w.GetUint64([]byte("foo"))
	require.ErrorIs(t, err, ErrClosed)

	err = w.Set([]byte("foo"), []byte("foo"))
	require.ErrorIs(t, err, ErrClosed)

	err = w.SetUint64([]byte("foo"), 1)
	require.ErrorIs(t, err, ErrClosed)
}
