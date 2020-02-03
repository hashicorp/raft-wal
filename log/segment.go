package log

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sync"

	"github.com/coreos/etcd/pkg/fileutil"
)

var (
	crc32Table          = crc32.MakeTable(crc32.Castagnoli)
	segment_magic_bytes = []byte("__RAFT_WAL__")
)

const (
	segment_header_size   = 62
	segmentVersion        = 1
	segmentDataInitOffset = 512
	indexSentinelIndex    = math.MaxUint64
	sealFlag              = 0xFF
)

var (
	errOutOfSequence = errors.New("out of sequence index")
	errLogNotFound   = errors.New("log entry not found")
	errWrongSegment  = errors.New("log predates this segment")
)

type segment struct {
	baseIndex    uint64
	openForWrite bool
	config       LogConfig

	writeLock sync.Mutex

	offsetLock sync.RWMutex
	offsets    []uint32
	nextOffset uint32

	f  *os.File
	bw *bufio.Writer

	persistTransformers []transformer
	loadTransformers    []transformer
}

func segmentName(baseIndex uint64) string {
	return fmt.Sprintf("wal-%016x.log", baseIndex)
}

func setSegmentHeader(b [segment_header_size]byte, baseIndex uint64, config LogConfig) {
	copy(b[:], segment_magic_bytes)
	b[16] = segmentVersion
	b[17] = byte(config.Compression)
	binary.BigEndian.PutUint64(b[18:26], baseIndex)
}

func parseSegmentHeader(b [segment_header_size]byte) (baseIndex uint64, compression LogCompression, err error) {
	if !bytes.Equal(b[:len(segment_magic_bytes)], segment_magic_bytes) {
		return 0, 0, errors.New("invalid file")
	}
	if b[16] != segmentVersion {
		return 0, 0, errors.New("unsupported version")
	}

	return binary.BigEndian.Uint64(b[18:26]), LogCompression(b[17]), nil
}

func newSegment(fp string, baseIndex uint64, forWrite bool, config LogConfig) (*segment, error) {
	rdf := os.O_RDONLY
	if forWrite {
		rdf = os.O_RDWR
	}

	var f *os.File
	var err error

	if fileutil.Exist(fp) {
		f, err = os.Open(fp)
		if err != nil {
			return nil, err
		}

		var header [segment_header_size]byte
		_, err := f.ReadAt(header[:], 0)
		if err != nil {
			return nil, err
		}
		bi, compression, err := parseSegmentHeader(header)
		if err != nil {
			return nil, err
		}

		if bi != baseIndex {
			return nil, fmt.Errorf("mismatch base index: %v != %v", bi, baseIndex)
		}

		config.Compression = compression

		// FIXME: FIND LATEST VALID OFFSET, and rebuild index
	} else {
		f, err = os.OpenFile(fp, rdf|os.O_CREATE, 0600)
		if err != nil {
			return nil, err
		}

		if err := fileutil.Preallocate(f, 64*1024*1024, true); err != nil {
			return nil, err
		}

		var header [segment_header_size]byte
		setSegmentHeader(header, baseIndex, config)
		if _, err := f.Write(header[:]); err != nil {
			os.Remove(fp)
			return nil, err
		}

		if _, err := f.Seek(segmentDataInitOffset, io.SeekStart); err != nil {
			return nil, err
		}
	}

	return &segment{
		baseIndex:           baseIndex,
		openForWrite:        forWrite,
		config:              config,
		offsets:             make([]uint32, 0, 512),
		nextOffset:          segmentDataInitOffset,
		f:                   f,
		bw:                  bufio.NewWriterSize(f, 4096),
		persistTransformers: persistTransformers(config),
		loadTransformers:    loadTransformers(config),
	}, nil
}

func (s *segment) untransform(data []byte) ([]byte, error) {
	return runTransformers(s.loadTransformers, data)
}

func (s *segment) readRecordAt(offset, rl uint32, index uint64, out []byte) (int, error) {
	if rl == 0 {
		var l [4]byte
		_, err := s.f.ReadAt(l[:], int64(offset+12))
		if err != nil {
			return 0, err
		}

		lf := binary.BigEndian.Uint32(l[:])
		rl = 16 + lf + recordPadding(lf)
	}

	record := make([]byte, rl)
	_, err := s.f.ReadAt(record, int64(offset))
	if err != nil {
		return 0, err
	}
	lIndex := binary.BigEndian.Uint64(record[:8])
	if lIndex != index {
		return 0, fmt.Errorf("index mismatch %v != %v", lIndex, index)
	}

	lChecksum := binary.BigEndian.Uint32(record[8:12])
	lLength := binary.BigEndian.Uint32(record[12:16])
	lPadding := recordPadding(lLength)

	if 16+lLength+lPadding != rl {
		return 0, fmt.Errorf("mismatch length: %v != %v", 16+lLength+lPadding, rl)
	}

	fChecksum := crc32.Checksum(record[16:16+lLength], crc32Table)
	if lChecksum != fChecksum {
		return 0, fmt.Errorf("checksums mismatch")
	}

	data, err := s.untransform(record[16 : 16+lLength])
	if err != nil {
		return 0, err
	}

	n := copy(out, data)
	if n != len(data) {
		return n, fmt.Errorf("record too small: %v != %v; %v", n, lLength, record)
	}
	return n, nil

}
func (s *segment) GetLog(index uint64, out []byte) (int, error) {
	s.offsetLock.RLock()

	if index < s.baseIndex {
		s.offsetLock.RUnlock()
		return 0, errWrongSegment
	} else if index > s.baseIndex+uint64(len(s.offsets)) {
		s.offsetLock.RUnlock()
		return 0, errLogNotFound
	}

	li := index - s.baseIndex

	var rl uint32
	if li == uint64(len(s.offsets)-1) {
		rl = s.nextOffset - s.offsets[li]
	} else {
		rl = s.offsets[li+1] - s.offsets[li]
	}

	offset := s.offsets[li]
	s.offsetLock.RUnlock()

	return s.readRecordAt(offset, rl, index, out)
}

func (s *segment) transform(data []byte) ([]byte, error) {
	return runTransformers(s.persistTransformers, data)
}

var padding [8]byte

func (s *segment) writeRecord(index uint64, data []byte) (uint32, error) {
	var rh [16]byte
	var err error

	data, err = s.transform(data)
	if err != nil {
		return 0, err
	}

	// prepare header
	checksum := crc32.Checksum(data, crc32Table)

	binary.BigEndian.PutUint64(rh[:8], index)
	binary.BigEndian.PutUint32(rh[8:12], checksum)
	l := uint32(len(data))
	padl := recordPadding(l)
	binary.BigEndian.PutUint32(rh[12:16], l)

	if _, err := s.bw.Write(rh[:]); err != nil {
		return 0, err
	}
	if _, err := s.bw.Write(data); err != nil {
		return 0, err
	}
	if _, err := s.bw.Write(padding[:padl]); err != nil {
		return 0, err
	}

	return 16 + uint32(len(data)) + padl, nil
}

func (s *segment) StoreLogs(index uint64, next func() []byte) error {
	if !s.openForWrite {
		return fmt.Errorf("file is ready only")
	}

	var err error

	s.writeLock.Lock()
	defer s.writeLock.Unlock()

	s.offsetLock.RLock()
	if int(index-s.baseIndex) != len(s.offsets) {
		s.offsetLock.RUnlock()
		return errOutOfSequence
	}

	startingOffset := s.nextOffset
	nextOffset := s.nextOffset
	s.offsetLock.RUnlock()

	var writtenEntries int

	s.bw.Reset(s.f)

	newOffsets := make([]uint32, 0, 16)
	for data := next(); data != nil; data = next() {
		recSize, err := s.writeRecord(index, data)
		if err != nil {
			goto ROLLBACK
		}

		newOffsets = append(newOffsets, nextOffset)

		nextOffset += recSize
		writtenEntries++
		index++
	}

	if writtenEntries == 0 {
		return nil
	}

	if err = s.bw.Flush(); err != nil {
		goto ROLLBACK
	}

	if err = fileutil.Fdatasync(s.f); err != nil {
		goto ROLLBACK
	}

	s.offsetLock.Lock()
	s.nextOffset = nextOffset
	s.offsets = append(s.offsets, newOffsets...)
	s.offsetLock.Unlock()

	return nil
ROLLBACK:

	_, err = s.f.Seek(int64(startingOffset), io.SeekStart)
	if err != nil {
		return err
	}
	err = fileutil.ZeroToEnd(s.f)
	if err != nil {
		return err
	}
	return err
}

func (s *segment) Close() error {
	return s.f.Close()
}

func parseIndexData(data []byte) ([]uint32, error) {
	len := binary.BigEndian.Uint32(data[:4])
	out := make([]uint32, len)
	err := binary.Read(bytes.NewReader(data[4:]), binary.BigEndian, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *segment) indexData() ([]byte, error) {
	var buf bytes.Buffer

	var lenBytes [4]byte
	binary.BigEndian.PutUint32(lenBytes[:], uint32(len(s.offsets)))
	if _, err := buf.Write(lenBytes[:]); err != nil {
		return nil, err
	}

	if err := binary.Write(&buf, binary.BigEndian, s.offsets); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (s *segment) writeIndex() (uint32, error) {
	s.offsetLock.RLock()
	defer s.offsetLock.RUnlock()

	offset := s.nextOffset
	data, err := s.indexData()
	if err != nil {
		return 0, err
	}

	_, err = s.writeRecord(indexSentinelIndex, data)
	if err != nil {
		return 0, err
	}
	if err := s.bw.Flush(); err != nil {
		return 0, err
	}

	return offset, nil
}

func (s *segment) Seal() error {
	s.writeLock.Lock()
	defer s.writeLock.Unlock()

	offset, err := s.writeIndex()
	if err != nil {
		return err
	}

	if err != fileutil.Fdatasync(s.f) {
		return err
	}

	var b [5]byte
	b[0] = sealFlag
	binary.BigEndian.PutUint32(b[1:], offset)

	if _, err := s.f.WriteAt(b[:], 26); err != nil {
		return err
	}

	return fileutil.Fdatasync(s.f)
}

func recordPadding(length uint32) uint32 {
	last := uint32(length) & 0x7
	return (8 - last) & 0x7
}
