package log

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestBasic(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "raftwal")
	require.NoError(t, err)
	defer os.RemoveAll(tmpdir)

	lc := LogConfig{
		FirstIndexUpdatedCallback: func(uint64) error { return nil },
		UserLogConfig: UserLogConfig{

			NoSync:      true,
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
			NoSync:           true,
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

func createTestLogWithMultipleSegments(t *testing.T, entries, entriesPerSegment uint64) *log {
	tmpdir, err := ioutil.TempDir("", "raftwal")
	require.NoError(t, err)

	t.Cleanup(func() { os.RemoveAll(tmpdir) })

	lc := LogConfig{
		FirstIndexUpdatedCallback: func(uint64) error { return nil },
		UserLogConfig: UserLogConfig{
			SegmentChunkSize: entriesPerSegment,
			NoSync:           true,
			Compression:      LogCompressionNone,
		},
	}

	log, err := NewLog(tmpdir, lc)
	require.NoError(t, err)

	require.Len(t, log.segmentBases, 1)

	for i := uint64(1); i <= entries; i++ {
		d := fmt.Sprintf("data %v", i)
		err := log.StoreLogs(i, stringsIterator([]string{d}))
		require.NoError(t, err)

	}

	return log
}

func Test_Log_ReadsFromMultipleSegments(t *testing.T) {
	log := createTestLogWithMultipleSegments(t, 20, 4)

	require.Len(t, log.segmentBases, 5)

	for i := uint64(1); i <= 20; i++ {
		t.Run(fmt.Sprintf("case %v", i), func(t *testing.T) {
			bytes, err := log.GetLog(i)
			require.NoError(t, err)

			require.Equal(t, fmt.Sprintf("data %v", i), string(bytes))
		})
	}

	_, err := log.GetLog(21)
	require.EqualError(t, err, raft.ErrLogNotFound.Error())
}

func TestLog_TruncateHead(t *testing.T) {
	entries := uint64(22)
	for i := uint64(1); i < entries; i++ {
		t.Run(fmt.Sprintf("entries:%v truncate_head:%v", entries, i), func(t *testing.T) {

			log := createTestLogWithMultipleSegments(t, entries, 4)
			require.Len(t, log.segmentBases, 6)

			err := log.TruncateHead(i)
			require.NoError(t, err)

			require.Equal(t, i+1, log.FirstIndex())
			require.Equal(t, entries, log.LastIndex())

			// check all head queries error now

			// all head queries should error
			for j := uint64(1); j <= i; j++ {
				_, err := log.GetLog(j)
				require.EqualError(t, err, raft.ErrLogNotFound.Error())
			}
			for j := uint64(i + 1); j <= entries; j++ {
				b, err := log.GetLog(j)
				require.NoError(t, err)
				require.Equal(t, fmt.Sprintf("data %v", j), string(b))
			}

			_, err = log.GetLog(entries + 1)
			require.EqualError(t, err, raft.ErrLogNotFound.Error())

			// insertion after deletion is fine
			err = log.StoreLogs(entries+1, stringsIterator([]string{"after deletion 1", "after deletion 2"}))
			require.NoError(t, err)

			b, err := log.GetLog(entries + 1)
			require.NoError(t, err)
			require.Equal(t, "after deletion 1", string(b))

			b, err = log.GetLog(entries + 2)
			require.NoError(t, err)
			require.Equal(t, "after deletion 2", string(b))
		})
	}
}

func TestLog_TruncateTail(t *testing.T) {
	testFn := func(t *testing.T, entries, i uint64) {
		log := createTestLogWithMultipleSegments(t, entries, 4)
		require.Len(t, log.segmentBases, 6)

		err := log.TruncateTail(i)
		require.NoError(t, err)

		require.Equal(t, uint64(1), log.FirstIndex())
		require.Equal(t, i-1, log.LastIndex())

		// check all head queries error now

		// all head queries should error
		for j := uint64(1); j < i; j++ {
			b, err := log.GetLog(j)
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("data %v", j), string(b))
		}
		for j := i; j <= entries+2; j++ {
			_, err := log.GetLog(j)
			require.EqualError(t, err, raft.ErrLogNotFound.Error())
		}

		// insertion after deletion must use new index
		err = log.StoreLogs(entries+1, stringsIterator([]string{"after deletion 1", "after deletion 2"}))
		require.Error(t, err)

		err = log.StoreLogs(i, stringsIterator([]string{"after deletion 1", "after deletion 2"}))
		require.NoError(t, err)

		b, err := log.GetLog(i)
		require.NoError(t, err)
		require.Equal(t, "after deletion 1", string(b))

		b, err = log.GetLog(i + 1)
		require.NoError(t, err)
		require.Equal(t, "after deletion 2", string(b))
	}

	for _, entries := range []uint64{3, 4, 8, 22} {
		for i := uint64(22); i <= entries; i++ {
			t.Run(fmt.Sprintf("entries:%v truncate_tail:%v", entries, i), func(t *testing.T) {
				testFn(t, entries, i)
			})
		}
	}
}
