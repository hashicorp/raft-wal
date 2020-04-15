package log

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

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

			s, err := newSegment(fp, 1, true, config)
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
			s, err := newSegment(fp, baseIndex, true, config)
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
				n, err := s.GetLog(uint64(i)+baseIndex, out)
				require.NoError(t, err)
				require.Equal(t, []byte(l), out[:n])
			}

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
	dir, err := ioutil.TempDir("", "testsegment")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	fp := filepath.Join(dir, "testsegment_otherbase")

	s, err := newSegment(fp, 1, true, testLogConfig)
	require.NoError(t, err)
	defer s.Close()

	logs := []string{
		"log 1",
		"log 2",
		"log 3",
	}

	_, err = s.StoreLogs(1, stringsIterator(logs))
	require.NoError(t, err)

	err = s.Seal()
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
	dir, err := ioutil.TempDir("", "testsegment")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	fp := filepath.Join(dir, "testsegment_otherbase")

	s, err := newSegment(fp, 1, true, testLogConfig)
	require.NoError(t, err)
	defer s.Close()

	logs := []string{
		"log 1",
		"log 2",
		"log 3",
	}

	_, err = s.StoreLogs(1, stringsIterator(logs))
	require.NoError(t, err)

	err = s.Seal()
	require.NoError(t, err)

	// Ensure it's sealed
	var sealHeader [1]byte
	_, err = s.f.ReadAt(sealHeader[:], 26)
	require.NoError(t, err)
	require.Equal(t, byte(sealFlag), sealHeader[0])

	// now open file again
	// fails to open for write again
	_, err = newSegment(fp, 1, true, testLogConfig)
	require.EqualError(t, err, errSealedFile.Error())

	s2, err := newSegment(fp, 1, false, testLogConfig)
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

func TestSegment_OpenningFiles_Unsealed(t *testing.T) {
	dir, err := ioutil.TempDir("", "testsegment")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	fp := filepath.Join(dir, "testsegment_otherbase")

	s, err := newSegment(fp, 1, true, testLogConfig)
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
	s2, err := newSegment(fp, 1, true, testLogConfig)
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
