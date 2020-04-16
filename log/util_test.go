package log

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSearchSegmentIndex(t *testing.T) {
	offsets := []uint64{1, 5, 8, 100, 1000, 1002, 1003}

	cases := []struct {
		reqIdx   uint64
		expected uint64
	}{
		{1, 1},
		{2, 1},
		{4, 1},
		{5, 5},
		{6, 5},
		{7, 5},
		{8, 8},
		{99, 8},
		{100, 100},
		{101, 100},
		{999, 100},
		{1001, 1000},
		{1002, 1002},
		{1003, 1003},
		{1004, 1003},
		{99999, 1003},
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("case %v", c.reqIdx), func(t *testing.T) {
			r, err := searchSegmentIndex(offsets, c.reqIdx)
			require.NoError(t, err)
			require.Equal(t, c.expected, r)

		})
	}
}

func TestHeadSegmentsToDelete(t *testing.T) {
	offsets := []uint64{1, 5, 8, 100, 1000, 1002, 1003}

	cases := []struct {
		newFirstIndex uint64
		toDelete      int
	}{
		{1, 0},
		{2, 0},
		{4, 0},
		{5, 1},
		{6, 1},
		{7, 1},
		{8, 2},
		{99, 2},
		{100, 3},
		{101, 3},
		{999, 3},
		{1001, 4},
		{1002, 5},
		{1003, 6},
		{1004, 6},
		{99999, 6},
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("case %v", c.newFirstIndex), func(t *testing.T) {
			toDelete := segmentContainingIndex(offsets, c.newFirstIndex)
			require.Equal(t, c.toDelete, toDelete)
			require.Less(t, toDelete, len(offsets))
		})
	}

	// some odd cases
	require.Equal(t, 0, segmentContainingIndex([]uint64{1}, 1))
	require.Equal(t, 0, segmentContainingIndex([]uint64{1}, 5))

	require.Equal(t, 0, segmentContainingIndex([]uint64{5}, 4))
	require.Equal(t, 0, segmentContainingIndex([]uint64{5}, 5))
	require.Equal(t, 0, segmentContainingIndex([]uint64{5}, 6))
}
