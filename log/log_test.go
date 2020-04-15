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
		UserLogConfig: UserLogConfig{

			NoSync:      false,
			Compression: LogCompressionNone,
		},
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

func Test_CreatesSegments_AtInsertions(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "raftwal")
	require.NoError(t, err)
	defer os.RemoveAll(tmpdir)

	lc := LogConfig{
		FirstIndexUpdatedCallback: func(uint64) error { return nil },
		UserLogConfig: UserLogConfig{
			SegmentChunkSize: 4,
			NoSync:           false,
			Compression:      LogCompressionNone,
		},
	}

	currSegments := func() []uint64 {
		t.Helper()

		ss, err := segmentsIn(tmpdir)
		require.NoError(t, err)
		return ss
	}

	require.Equal(t, []uint64{}, currSegments())

	log, err := NewLog(tmpdir, lc)
	require.NoError(t, err)

	require.Equal(t, []uint64{1}, currSegments())

	splits := []uint64{
		1,
		5,
		9,
		13,
		17,
	}

	for i := uint64(1); i < 20; i++ {
		d := fmt.Sprintf("data %v", i)
		err := log.StoreLogs(i, stringsIterator([]string{d}))
		require.NoError(t, err)

		ss, err := segmentsIn(tmpdir)
		require.NoError(t, err)

		len := int((i-1)/4 + 1)
		require.Len(t, ss, len)
		require.Equal(t, splits[:len], ss)

		require.Equal(t, splits[:len], log.segmentBases)
	}
}

func Test_Log_ReadsFromMultipleSegments(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "raftwal")
	fmt.Println("CREATED IN ", tmpdir)
	require.NoError(t, err)
	defer os.RemoveAll(tmpdir)

	lc := LogConfig{
		FirstIndexUpdatedCallback: func(uint64) error { return nil },
		UserLogConfig: UserLogConfig{
			SegmentChunkSize: 4,
			NoSync:           false,
			Compression:      LogCompressionNone,
		},
	}

	log, err := NewLog(tmpdir, lc)
	require.NoError(t, err)

	require.Len(t, log.segmentBases, 1)

	for i := uint64(1); i < 20; i++ {
		d := fmt.Sprintf("data %v", i)
		err := log.StoreLogs(i, stringsIterator([]string{d}))
		require.NoError(t, err)

	}

	require.Len(t, log.segmentBases, 5)

	for i := uint64(1); i < 20; i++ {
		t.Run(fmt.Sprintf("case %v", i), func(t *testing.T) {
			bytes, err := log.GetLog(i)
			require.NoError(t, err)

			require.Equal(t, fmt.Sprintf("data %v", i), string(bytes))
		})
	}
}
