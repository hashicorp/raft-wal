package log

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBasic(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "raftwal")
	require.NoError(t, err)
	defer os.RemoveAll(tmpdir)

	lc := LogConfig{
		FirstIndexUpdatedCallback: func(uint64) error { return nil },
		NoSync:                    false,
		Compression:               LogCompressionNone,
	}

	log, err := NewLog(tmpdir, lc)
	require.NoError(t, err)

	require.Zero(t, log.FirstIndex())
	require.Zero(t, log.LastIndex())

	_, err = log.GetLog(1)
	require.Error(t, err)

	data := []string{}
	for i := 1; i <= 10; i++ {
		data = append(data, fmt.Sprintf("data %v", i))
	}
	err = log.StoreLogs(1, stringsIterator(data))
	require.NoError(t, err)

	require.Equal(t, uint64(1), log.FirstIndex())
	require.Equal(t, uint64(10), log.LastIndex())

	for i := 0; i < len(data); i++ {
		d, err := log.GetLog(uint64(i + 1))
		require.NoError(t, err)
		require.Equal(t, data[i], string(d))
	}
}
