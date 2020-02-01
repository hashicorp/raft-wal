package log

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSegment_Basic(t *testing.T) {
	dir, err := ioutil.TempDir("", "testsegment")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := newSegment(dir, 1, true, nil)
	require.NoError(t, err)

	logs := [][]byte{
		[]byte("log 1"),
		[]byte("log 2"),
		[]byte("log 3"),
	}

	i := 0
	err = s.StoreLogs(1, func() []byte {
		if i >= len(logs) {
			return nil
		}

		l := logs[i]
		i++
		return l
	})
	require.NoError(t, err)

	out := make([]byte, 32)
	for i, l := range logs {
		n, err := s.GetLog(uint64(i+1), out)
		require.NoError(t, err)
		require.Equal(t, l, out[:n])
	}

}

func TestEncoding(t *testing.T) {
	cases := []struct {
		input   uint32
		padding uint32
	}{
		{1, 7},
		{8, 0},
		{31, 1},
		{65, 7},
		{71, 1},
		{1 << 20, 0},
		{1<<20 - 5, 5},
		{1<<20 + 1<<13 + 1<<3 - 5, 5},
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("case: %v", c.input), func(t *testing.T) {
			l, p := encodeLength(c.input)
			fl, fp := decodeLength(l)

			t.Logf("encoding %x: 0x%x", c.input, l)
			require.Equal(t, c.input, fl)
			require.Equal(t, c.padding, p)
			require.Equal(t, c.padding, fp)
		})
	}
}
