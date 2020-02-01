package log

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"

	"github.com/coreos/etcd/pkg/fileutil"
)

var (
	crc32Table = crc32.MakeTable(crc32.Castagnoli)
)

type segment struct {
	mu sync.RWMutex

	baseIndex    uint64
	openForWrite bool

	offsets    []uint32
	nextOffset uint32

	f  *os.File
	bw *bufio.Writer
}

func segmentName(baseIndex uint64) string {
	return fmt.Sprintf("wal-%016x.log", baseIndex)
}

func newSegment(dir string, baseIndex uint64, forWrite bool, config *LogConfig) (*segment, error) {
	fp := filepath.Join(dir, segmentName(baseIndex))

	rdf := os.O_RDONLY
	if forWrite {
		rdf = os.O_RDWR
	}

	f, err := os.OpenFile(fp, rdf|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	fileutil.Preallocate(f, 64*1024*1024, true)

	var header [512]byte
	if _, err := f.Write(header[:]); err != nil {
		os.Remove(fp)
		return nil, err
	}

	return &segment{
		baseIndex:    baseIndex,
		openForWrite: forWrite,
		offsets:      make([]uint32, 0, 512),
		nextOffset:   512,
		f:            f,
		bw:           bufio.NewWriterSize(f, 4096),
	}, nil
}

func (s *segment) GetLog(index uint64, out []byte) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if index < s.baseIndex {
		return 0, fmt.Errorf("wrong segment, base index = %v", s.baseIndex)
	} else if index > s.baseIndex+uint64(len(s.offsets)) {
		return 0, fmt.Errorf("wront segment, max index = %v", s.baseIndex+uint64(len(s.offsets)))
	}

	li := index - s.baseIndex

	var rl uint32
	if li == uint64(len(s.offsets)-1) {
		rl = s.nextOffset - s.offsets[li]
	} else {
		rl = s.offsets[li+1] - s.offsets[li]
	}

	bytes := make([]byte, rl)
	_, err := s.f.ReadAt(bytes, int64(s.offsets[li]))
	if err != nil {
		return 0, err
	}
	lIndex := binary.BigEndian.Uint64(bytes[:8])
	if lIndex != index {
		return 0, fmt.Errorf("index mismatch %v != %v", lIndex, index)
	}

	lChecksum := binary.BigEndian.Uint32(bytes[8:12])
	lLength, lPadding := decodeLength(binary.BigEndian.Uint32(bytes[12:16]))

	if 16+lLength+lPadding != rl {
		return 0, fmt.Errorf("mismatch length: %v != %v", 16+lLength+lPadding, rl)
	}

	fChecksum := crc32.Checksum(bytes[16:16+lLength], crc32Table)
	if lChecksum != fChecksum {
		return 0, fmt.Errorf("checksums mismatch")
	}

	n := copy(out, bytes[16:16+lLength])
	if uint32(n) != lLength {
		return n, fmt.Errorf("bytes too small: %v != %v; %v", n, lLength, bytes)
	}
	return n, nil
}

func (s *segment) StoreLogs(index uint64, next func() []byte) error {
	if !s.openForWrite {
		return fmt.Errorf("file is ready only")
	}

	var err error

	s.mu.Lock()
	defer s.mu.Unlock()

	var rh [16]byte
	var padding [8]byte
	var writtenEntries int

	if int(index-s.baseIndex) != len(s.offsets) {
		return fmt.Errorf("out of sequence write: %v != %v", index-s.baseIndex, len(s.offsets))
	}

	s.bw.Reset(s.f)

	nextOffset := s.nextOffset
	offsetLengths := len(s.offsets)

	for data := next(); data != nil; data = next() {
		// prepare header
		checksum := crc32.Checksum(data, crc32Table)

		binary.BigEndian.PutUint64(rh[:8], index)
		binary.BigEndian.PutUint32(rh[8:12], checksum)
		l, padl := encodeLength(uint32(len(data)))
		binary.BigEndian.PutUint32(rh[12:16], l)

		if _, err = s.bw.Write(rh[:]); err != nil {
			goto ROLLBACK
		}
		if _, err = s.bw.Write(data); err != nil {
			goto ROLLBACK
		}
		if _, err = s.bw.Write(padding[:padl]); err != nil {
			goto ROLLBACK
		}

		s.offsets = append(s.offsets, nextOffset)

		recSize := 16 + uint32(len(data)) + padl
		nextOffset += recSize
		writtenEntries++
		index++
	}

	if writtenEntries == 0 {
		return nil
	}

	if err = s.bw.Flush(); err != nil {
		goto ROLLBACK
		return err
	}

	if err = fileutil.Fsync(s.f); err != nil {
		goto ROLLBACK
		return err
	}

	s.nextOffset = nextOffset
	return nil
ROLLBACK:
	// roll back if we can
	s.offsets = s.offsets[:offsetLengths]
	s.f.Truncate(int64(s.nextOffset))
	return err
}

func decodeLength(lenField uint32) (dataBytes, padBytes uint32) {
	data := lenField & 0x0FFFFFFF
	padding := uint32(lenField>>28) & 0x7
	return data, padding
}

func encodeLength(dataLength uint32) (uint32, uint32) {
	l := uint32(dataLength & 0x0FFFFFFF)
	padding := (8 - (dataLength & 0x7)) & 0x7
	l |= uint32((padding & 0x7) << 28)
	return l, uint32(padding)
}
