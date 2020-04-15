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
