package log

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

var testLogConfig = LogConfig{UserLogConfig: UserLogConfig{NoSync: true}}

func stringsIterator(d []string) func() []byte {
	i := 0
	return func() []byte {
		if i >= len(d) {
			return nil
		}

		l := []byte(d[i])
		i++
		return l

	}
}

func TestSegment_Basic(t *testing.T) {
	dir, err := ioutil.TempDir("", "testsegment")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	cases := []struct {
		name   string
		config UserLogConfig
	}{
		{"basic", UserLogConfig{}},
		{"compressed_zlib", UserLogConfig{Compression: LogCompressionZlib}},
		{"compressed_gzip", UserLogConfig{Compression: LogCompressionGZip}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fp := filepath.Join(dir, "testsegment_"+c.name)
			config := LogConfig{UserLogConfig: c.config}

			s, err := openSegment(fp, 1, true, config)
			require.NoError(t, err)
			defer s.Close()

			logs := []string{
				"log 1",
				"log 2",
				"log 3",
			}

			_, err = s.StoreLogs(1, stringsIterator(logs))
			require.NoError(t, err)

			moreLogs := []string{
				"log 4",
				"log 5",
				"log 6",
			}
			_, err = s.StoreLogs(4, stringsIterator(moreLogs))
			require.NoError(t, err)

			logs = append(logs, moreLogs...)
			out := make([]byte, 32)
			for i, l := range logs {
				n, err := s.GetLog(uint64(i+1), out)
				require.NoError(t, err)
				require.Equal(t, []byte(l), out[:n])
			}

			_, err = s.GetLog(uint64(len(logs)+1), out)
			require.EqualError(t, err, raft.ErrLogNotFound.Error())

			_, err = s.GetLog(uint64(len(logs))+100, out)
			require.EqualError(t, err, raft.ErrLogNotFound.Error())

			_, err = s.StoreLogs(1, stringsIterator(moreLogs))
			require.Error(t, err)
			require.Equal(t, errOutOfSequence, err)

			_, err = s.StoreLogs(1000, stringsIterator(moreLogs))
			require.Error(t, err)
			require.Equal(t, errOutOfSequence, err)
		})
	}
}

func TestSegment_OtherBase(t *testing.T) {
	dir, err := ioutil.TempDir("", "testsegment")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	cases := []struct {
		name   string
		config UserLogConfig
	}{
		{"basic", UserLogConfig{}},
		{"compressed_zlib", UserLogConfig{Compression: LogCompressionZlib}},
		{"compressed_gzip", UserLogConfig{Compression: LogCompressionGZip}},
	}

	baseIndex := uint64(51200)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fp := filepath.Join(dir, "testsegment_"+c.name)
			config := LogConfig{UserLogConfig: c.config}
			s, err := openSegment(fp, baseIndex, true, config)
			require.NoError(t, err)
			defer s.Close()

			logs := []string{
				"log 1",
				"log 2",
				"log 3",
			}

			_, err = s.StoreLogs(baseIndex, stringsIterator(logs))
			require.NoError(t, err)

			moreLogs := []string{
				"log 4",
				"log 5",
				"log 6",
			}
			_, err = s.StoreLogs(baseIndex+3, stringsIterator(moreLogs))
			require.NoError(t, err)

			logs = append(logs, moreLogs...)
			out := make([]byte, 32)
			for i, l := range logs {
				n, err := s.GetLog(baseIndex+uint64(i), out)
				require.NoError(t, err)
				require.Equal(t, []byte(l), out[:n])
			}

			_, err = s.GetLog(baseIndex+uint64(len(logs)+1), out)
			require.EqualError(t, err, raft.ErrLogNotFound.Error())

			_, err = s.GetLog(baseIndex+uint64(len(logs))+100, out)
			require.EqualError(t, err, raft.ErrLogNotFound.Error())

			_, err = s.StoreLogs(1, stringsIterator(moreLogs))
			require.Error(t, err)
			require.Equal(t, errOutOfSequence, err)

			_, err = s.StoreLogs(baseIndex, stringsIterator(moreLogs))
			require.Error(t, err)
			require.Equal(t, errOutOfSequence, err)

			_, err = s.StoreLogs(baseIndex+1000, stringsIterator(moreLogs))
			require.Error(t, err)
			require.Equal(t, errOutOfSequence, err)
		})
	}
}

func TestSegment_SealingWorks(t *testing.T) {
	s := testSegment(t, 100, 3)

	err := s.Seal()
	require.NoError(t, err)

	// inspect seal flag
	var sealHeader [5]byte
	_, err = s.f.ReadAt(sealHeader[:], 26)
	require.NoError(t, err)

	require.Equal(t, byte(sealFlag), sealHeader[0])

	indexOffset := binary.BigEndian.Uint32(sealHeader[1:])

	indexData := make([]byte, 512)
	n, err := s.readRecordAt(indexOffset, 0, indexSentinelIndex, indexData)
	require.NoError(t, err)

	offsets, err := parseIndexData(indexData[:n])
	require.NoError(t, err)
	require.Equal(t, s.offsets, offsets)
}

func TestSegment_OpenningFiles_Sealed(t *testing.T) {
	baseIndex := uint64(100)
	s := testSegment(t, baseIndex, 3)
	fp := s.f.Name()

	err := s.Seal()
	require.NoError(t, err)

	// Ensure it's sealed
	var sealHeader [1]byte
	_, err = s.f.ReadAt(sealHeader[:], 26)
	require.NoError(t, err)
	require.Equal(t, byte(sealFlag), sealHeader[0])

	// now open file again
	// fails to open for write again
	_, err = openSegment(fp, baseIndex, true, testLogConfig)
	require.EqualError(t, err, errSealedFile.Error())

	// fails to open for wront index again
	_, err = openSegment(fp, 10, false, testLogConfig)
	require.EqualError(t, err, fmt.Sprintf("mismatch base index: %v != 10", baseIndex))

	s2, err := openSegment(fp, baseIndex, false, testLogConfig)
	require.NoError(t, err)

	require.Equal(t, s.baseIndex, s2.baseIndex)
	require.Equal(t, s.offsets, s2.offsets)
	require.Equal(t, s.nextOffset, s2.nextOffset)
	require.Equal(t, s.nextIndex(), s2.nextIndex())

	// inspect logs
	out := make([]byte, 30)
	for i := baseIndex; i < baseIndex+3; i++ {
		l := fmt.Sprintf("data %v", i)
		n, err := s.GetLog(uint64(i), out)
		require.NoErrorf(t, err, "log %v", i)
		require.Equalf(t, []byte(l), out[:n], "log %v", i)
	}

}

func TestSegment_OpenningFiles_Unsealed(t *testing.T) {
	dir, err := ioutil.TempDir("", "testsegment")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	fp := filepath.Join(dir, "testsegment_otherbase")

	s, err := openSegment(fp, 1, true, testLogConfig)
	require.NoError(t, err)
	defer s.Close()

	logs := []string{
		"log 1",
		"log 2",
		"log 3",
	}

	_, err = s.StoreLogs(1, stringsIterator(logs))
	require.NoError(t, err)

	// Ensure it's not sealed
	var sealHeader [1]byte
	_, err = s.f.ReadAt(sealHeader[:], 26)
	require.NoError(t, err)
	require.Zero(t, sealHeader[0])

	// now open file again
	// fails to open for write again
	s2, err := openSegment(fp, 1, true, testLogConfig)
	require.NoError(t, err)

	require.Equal(t, s.baseIndex, s2.baseIndex)
	require.Equal(t, s.offsets, s2.offsets)
	require.Equal(t, s.nextOffset, s2.nextOffset)
	require.Equal(t, s.nextIndex(), s2.nextIndex())

	// inspect logs
	out := make([]byte, 30)
	for i, l := range logs {
		n, err := s.GetLog(uint64(i+1), out)
		require.NoError(t, err)
		require.Equal(t, []byte(l), out[:n])
	}

}

func TestSegment_TruncateTail(t *testing.T) {
	testFn := func(t *testing.T, initBase, dataLength, dIdx uint64) {
		s := testSegment(t, initBase, dataLength)

		offsets := append([]uint32{}, s.offsets...)
		offsets = append(offsets, s.nextOffset)

		err := s.truncateTail(dIdx)
		require.NoError(t, err)

		diff := dIdx - initBase
		require.Equal(t, offsets[:diff], s.offsets)
		require.Equal(t, offsets[diff], s.nextOffset)
		require.Equal(t, dIdx, s.nextIndex())
	}

	testInvalidFn := func(t *testing.T, initBase, dataLength uint64) {
		s := testSegment(t, initBase, dataLength)

		offsets := append([]uint32{}, s.offsets...)
		nextOffset := s.nextOffset

		// truncating before base is an error
		err := s.truncateTail(initBase - 1)
		require.Error(t, err)
		require.Equal(t, offsets, s.offsets)
		require.Equal(t, nextOffset, s.nextOffset)

		// truncating beyond data is a noop
		err = s.truncateTail(initBase + dataLength + 1)
		require.NoError(t, err)
		require.Equal(t, offsets, s.offsets)
		require.Equal(t, nextOffset, s.nextOffset)

		// truncating way beyond data is noop
		err = s.truncateTail(initBase + dataLength + 100)
		require.NoError(t, err)
		require.Equal(t, offsets, s.offsets)
		require.Equal(t, nextOffset, s.nextOffset)
	}

	for _, initBase := range []uint64{1, 50} {
		for _, dataLength := range []uint64{5} {
			for dIdx := initBase; dIdx < initBase+dataLength; dIdx++ {
				t.Run(fmt.Sprintf("init:%v len:%v dIdx:%v", initBase, dataLength, dIdx), func(t *testing.T) {
					testFn(t, initBase, dataLength, dIdx)
				})
			}

			t.Run(fmt.Sprintf("init:%v len:%v invalid truncations", initBase, dataLength), func(t *testing.T) {
				testInvalidFn(t, initBase, dataLength)
			})
		}

	}
}

func testSegment(t *testing.T, baseIndex, sampleData uint64) *segment {
	dir, err := ioutil.TempDir("", "testsegment")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })

	fp := filepath.Join(dir, "testsegment")
	s, err := openSegment(fp, baseIndex, true, testLogConfig)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	logs := make([]string, 0, sampleData)
	for i := uint64(0); i < sampleData; i++ {
		logs = append(logs, fmt.Sprintf("data %v", i+baseIndex))
	}

	_, err = s.StoreLogs(baseIndex, stringsIterator(logs))
	require.NoError(t, err)

	return s
}

func TestPadding(t *testing.T) {
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
			p := recordPadding(c.input)
			require.Equal(t, c.padding, p)
		})
	}
}
