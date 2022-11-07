// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package migrate

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-wal/types"
	"github.com/stretchr/testify/require"
)

func TestCopyLogs(t *testing.T) {
	cases := []struct {
		name           string
		startIndex     uint64
		numLogs        int
		batchBytes     int
		wantNumBatches int
		nilChan        bool
		cancelCtx      bool
		wantErr        string
	}{
		{
			name:       "basic copy",
			startIndex: 1234,
			numLogs:    1000,
			// Each log is 26 bytes but we assume 32 bytes of overhead for encoding in
			// general. So each log takes up 58 bytes of our batch size. 550 bytes is
			// not quite enough for 10 but we treat it as a soft limit so we'll get 10
			// per batch
			batchBytes:     550,
			wantNumBatches: 100,
		},
		{
			name:           "start from 1",
			startIndex:     1,
			numLogs:        1000,
			batchBytes:     580, // Exact fit for 10 entries
			wantNumBatches: 100,
		},
		{
			name:           "nil progress chan",
			startIndex:     1234,
			numLogs:        1000,
			batchBytes:     580,
			wantNumBatches: 100,
			// A nil progress chan shouldn't block the copy.
			nilChan: true,
		},
		{
			name:           "context cancel",
			startIndex:     1234,
			numLogs:        1000,
			batchBytes:     580,
			wantNumBatches: 0,
			cancelCtx:      true,
			wantErr:        "context canceled",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			src := populateTestLogStore(t, tc.startIndex, tc.numLogs)
			dst := &testLogStore{}
			var progress chan string
			if !tc.nilChan {
				// Buffer it more than enough so we won't have to read concurrently.
				progress = make(chan string, tc.wantNumBatches*3)
			}

			ctx := context.Background()
			if tc.cancelCtx {
				cancelledCtx, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cancelledCtx
			}

			err := CopyLogs(ctx, dst, src, tc.batchBytes, progress)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err, "failed copy")

			if progress != nil {
				close(progress)
				for s := range progress {
					t.Log(s)
				}
			}

			// Verify the copy!
			wantFirst, _ := src.FirstIndex()
			wantLast, _ := src.LastIndex()
			gotFirst, _ := dst.FirstIndex()
			gotLast, _ := dst.LastIndex()
			require.Equal(t, int(wantFirst), int(gotFirst))
			require.Equal(t, int(wantLast), int(gotLast))

			var log raft.Log
			for idx := wantFirst; idx <= wantLast; idx++ {
				err := dst.GetLog(idx, &log)
				require.NoError(t, err)
				require.Equal(t, int(idx), int(log.Index))
				require.Equal(t, string(logPayload(idx)), string(log.Data))
			}

			// Validate that we actually split into chunks as expected.
			require.Equal(t, tc.wantNumBatches, dst.appends)
		})
	}
}

func TestCopyStable(t *testing.T) {
	cases := []struct {
		name         string
		srcVals      map[string]string
		srcIntVals   map[string]uint64
		extraKeys    [][]byte
		extraIntKeys [][]byte
		nilChan      bool
		cancelCtx    bool
		wantErr      string
	}{
		{
			name: "basic raft data",
			srcVals: map[string]string{
				"LastVoteCand": "s1",
			},
			srcIntVals: map[string]uint64{
				"CurrentTerm":  1234,
				"LastVoteTerm": 1000,
			},
		},
		{
			name: "context cancelled",
			srcVals: map[string]string{
				"LastVoteCand": "s1",
			},
			srcIntVals: map[string]uint64{
				"CurrentTerm":  1234,
				"LastVoteTerm": 1000,
			},
			cancelCtx: true,
			wantErr:   "context canceled",
		},
		{
			name: "additional keys",
			srcVals: map[string]string{
				"LastVoteCand": "s1",
				"my_app_key":   "foo",
				"my_other_key": "baz",
				"no_copy":      "bar",
			},
			srcIntVals: map[string]uint64{
				"CurrentTerm":                   1234,
				"LastVoteTerm":                  1000,
				"favorite_term_so_far":          569,
				"least_favorite_number_no_copy": 4321,
			},
			extraKeys:    [][]byte{[]byte("my_app_key"), []byte("my_other_key")},
			extraIntKeys: [][]byte{[]byte("favorite_term_so_far")},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			src := newTestStableStore()
			dst := newTestStableStore()

			// Insert src values:
			for k, v := range tc.srcIntVals {
				err := src.SetUint64([]byte(k), v)
				require.NoError(t, err)
			}
			for k, v := range tc.srcVals {
				err := src.Set([]byte(k), []byte(v))
				require.NoError(t, err)
			}

			var progress chan string
			if !tc.nilChan {
				// Buffer it more than enough so we won't have to read concurrently.
				progress = make(chan string, (len(tc.srcIntVals)+len(tc.srcVals))*3)
			}

			ctx := context.Background()
			if tc.cancelCtx {
				cancelledCtx, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cancelledCtx
			}
			err := CopyStable(ctx, dst, src, tc.extraKeys, tc.extraIntKeys, progress)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err, "failed copy")

			close(progress)
			for s := range progress {
				t.Log(s)
			}

			// Verify the copy!
			for k, v := range tc.srcIntVals {
				if strings.HasSuffix(k, "no_copy") {
					continue
				}
				got, err := dst.GetUint64([]byte(k))
				require.NoError(t, err)
				require.Equal(t, int(v), int(got), "wrong int value copied for key %s", k)
			}
			for k, v := range tc.srcVals {
				if strings.HasSuffix(k, "no_copy") {
					continue
				}
				got, err := dst.Get([]byte(k))
				require.NoError(t, err)
				require.Equal(t, v, string(got), "wrong value copied for key %s", k)
			}
		})
	}
}

func logPayload(idx uint64) string {
	return fmt.Sprintf("Log entry for index %6d", idx)
}

func populateTestLogStore(t *testing.T, startIdx uint64, n int) *testLogStore {
	t.Helper()
	ls := &testLogStore{}
	for idx := startIdx; idx < (startIdx + uint64(n)); idx++ {
		err := ls.StoreLog(&raft.Log{
			Index:      idx,
			Data:       []byte(logPayload(idx)),
			AppendedAt: time.Now(),
		})
		require.NoError(t, err)
	}
	return ls
}

type testLogStore struct {
	appends int
	logs    []*raft.Log
}

// FirstIndex returns the first index written. 0 for no entries.
func (s *testLogStore) FirstIndex() (uint64, error) {
	if len(s.logs) < 1 {
		return 0, nil
	}
	return s.logs[0].Index, nil
}

// LastIndex returns the last index written. 0 for no entries.
func (s *testLogStore) LastIndex() (uint64, error) {
	if len(s.logs) < 1 {
		return 0, nil
	}
	return s.logs[len(s.logs)-1].Index, nil
}

// GetLog gets a log entry at a given index.
func (s *testLogStore) GetLog(index uint64, log *raft.Log) error {
	first, _ := s.FirstIndex()
	last, _ := s.LastIndex()
	if first == 0 || index < first || index > last {
		return types.ErrNotFound
	}
	offset := index - first
	*log = *s.logs[offset]
	return nil
}

// StoreLog stores a log entry.
func (s *testLogStore) StoreLog(log *raft.Log) error {
	return s.StoreLogs([]*raft.Log{log})
}

// StoreLogs stores multiple log entries.
func (s *testLogStore) StoreLogs(logs []*raft.Log) error {
	last, _ := s.LastIndex()
	prev := last
	for _, log := range logs {
		if prev > 0 && (prev+1) != log.Index {
			return fmt.Errorf("logs out of sequence got index=%d expecting index=%d", log.Index, prev+1)
		}
		s.logs = append(s.logs, log)
		prev = log.Index
	}
	s.appends++
	return nil
}

// DeleteRange deletes a range of log entries. The range is inclusive.
func (s *testLogStore) DeleteRange(min uint64, max uint64) error {
	panic("not implemented") // Don't need this in this package.
}

type testStableStore struct {
	d map[string][]byte
}

func newTestStableStore() *testStableStore {
	return &testStableStore{
		d: make(map[string][]byte),
	}
}

func (s *testStableStore) Set(key []byte, val []byte) error {
	s.d[(string(key))] = val
	return nil
}

// Get returns the value for key, or an empty byte slice if key was not found.
func (s *testStableStore) Get(key []byte) ([]byte, error) {
	return s.d[string(key)], nil
}

func (s *testStableStore) SetUint64(key []byte, val uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], val)
	s.d[(string(key))] = buf[:]
	return nil
}

// GetUint64 returns the uint64 value for key, or 0 if key was not found.
func (s *testStableStore) GetUint64(key []byte) (uint64, error) {
	v, ok := s.d[string(key)]
	if !ok {
		return 0, nil
	}
	return binary.BigEndian.Uint64(v), nil
}
