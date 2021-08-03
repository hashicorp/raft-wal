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
	"github.com/hashicorp/raft"
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

	segmentHeaderStartOffsetVersion     = 16
	segmentHeaderStartOffsetCompression = 17
	segmentHeaderStartOffsetBaseIndex   = 18
	segmentHeaderStartOffsetSeal        = segmentHeaderStartOffsetBaseIndex + 8

	// These offsets are relative to segmentHeaderStartOffsetSeal
	segmentSealStartOffsetSealFlag    = 0
	segmentSealStartOffsetIndexOffset = segmentSealStartOffsetSealFlag + 1
	segmentSealStartOffsetIndexSize   = segmentSealStartOffsetIndexOffset + 4
	segmentHeaderSealSize             = 9
)

var (
	errOutOfSequence = errors.New("out of sequence index")
	errWrongSegment  = errors.New("log predates this segment")
	errSealedFile    = errors.New("file was sealed; cannot be opened for write")
	errReadOnlyFile  = errors.New("file is read only")
)

// segment manages a single file that contains a sequence of logs.
type segment struct {
	// baseIndex is the index of the first record in the file, or, for a newly
	// created segment, the first record that will be written to the file.
	baseIndex    uint64
	openForWrite bool
	config       LogConfig

	// writeLock must be held when making any changes to the segment file.
	writeLock sync.Mutex

	// offsetLock must be held when modifying offsets.
	offsetLock sync.RWMutex
	// offsets are the file offsets of each log record.
	offsets []uint32
	// nextOffset is the file offset where the next log record should be written.
	nextOffset int

	f  *os.File
	fp string
	bw *bufio.Writer

	persistTransformers []transformer
	loadTransformers    []transformer
}

// setSegmentHeader populates b with a segment header based on the other arguments.
func setSegmentHeader(b *[segment_header_size]byte, baseIndex uint64, config LogConfig) {
	copy(b[:], segment_magic_bytes)
	b[segmentHeaderStartOffsetVersion] = segmentVersion
	b[segmentHeaderStartOffsetCompression] = byte(config.Compression)
	putU64(b[:], segmentHeaderStartOffsetBaseIndex, baseIndex)
}

// parseSegmentHeader extracts the header constituents from b.
func parseSegmentHeader(b [segment_header_size]byte) (baseIndex uint64, compression LogCompression, sealed bool, indexOffset int, indexRecSize int, err error) {
	if !bytes.Equal(b[:len(segment_magic_bytes)], segment_magic_bytes) {
		return 0, 0, false, 0, 0, errors.New("invalid file: bad magic")
	}
	if b[segmentHeaderStartOffsetVersion] != segmentVersion {
		return 0, 0, false, 0, 0, errors.New("unsupported version")
	}

	sealed = b[segmentHeaderStartOffsetSeal+segmentSealStartOffsetSealFlag] == sealFlag
	offset := getU32(b[segmentHeaderStartOffsetSeal:], segmentSealStartOffsetIndexOffset)
	recSize := getU32(b[segmentHeaderStartOffsetSeal:], segmentSealStartOffsetIndexSize)
	return getU64(b[:], segmentHeaderStartOffsetBaseIndex), LogCompression(b[17]), sealed, int(offset), int(recSize), nil
}

// openSegment returns a segment for file fp, creating one if it doesn't exist and
// forWrite is true.  The segment can be appended to if forWrite is true and the
// segment hasn't been sealed.
func openSegment(fp string, baseIndex uint64, forWrite bool, config LogConfig) (*segment, error) {
	switch {
	case fileutil.Exist(fp):
		return openExistingSegment(fp, baseIndex, forWrite, config)
	case forWrite:
		return createSegment(fp, baseIndex, config)
	default:
		return nil, fmt.Errorf("segment file %q does not exist", fp)
	}
}

// openExistingSegment opens the file with path fp and interprets it as a segment.
func openExistingSegment(fp string, baseIndex uint64, forWrite bool, config LogConfig) (*segment, error) {
	rdf := os.O_RDONLY
	if forWrite {
		rdf = os.O_RDWR
	}

	f, err := os.OpenFile(fp, rdf, 0o600)
	if err != nil {
		return nil, fmt.Errorf("error opening existing segment file '%q': %w", fp, err)
	}

	var header [segment_header_size]byte
	_, err = f.ReadAt(header[:], 0)
	if err != nil {
		return nil, err
	}
	bi, compression, sealed, indexOffset, indexRecSize, err := parseSegmentHeader(header)
	if err != nil {
		return nil, err
	}

	if bi != baseIndex {
		return nil, fmt.Errorf("mismatch base index: %v != %v", bi, baseIndex)
	}

	config.Compression = compression

	s := &segment{
		baseIndex:           baseIndex,
		openForWrite:        forWrite,
		config:              config,
		offsets:             make([]uint32, 0, 512),
		nextOffset:          segmentDataInitOffset,
		f:                   f,
		fp:                  fp,
		bw:                  bufio.NewWriterSize(f, 4096),
		persistTransformers: persistTransformers(config.UserLogConfig),
		loadTransformers:    loadTransformers(config.UserLogConfig),
	}

	if sealed && forWrite {
		return nil, errSealedFile
	}

	if sealed {
		s.nextOffset = indexOffset

		indexData := make([]byte, indexRecSize)
		n, err := s.readRecordAt(indexOffset, indexRecSize, indexSentinelIndex, indexData)
		if err != nil {
			return nil, fmt.Errorf("failed to parse index data: %v", err)
		}
		s.offsets, err = parseIndexData(indexData[:n])
		if err != nil {
			return nil, fmt.Errorf("failed to parse index data: %v", err)
		}
	} else {
		err := s.loadUnsealedContent()
		if err != nil {
			return nil, fmt.Errorf("failed to load segment: %v", err)
		}
	}

	config.Logger.Trace("openExistingSegment", "baseIndex", baseIndex, "fp", fp)
	return s, nil
}

// loadUnsealedContent reads the log records in an existing segment file to
// build our in-progress offset index and set s.nextOffset.
func (s *segment) loadUnsealedContent() error {
	offsets := make([]uint32, 0, 512)
	nextOffset := segmentDataInitOffset
	nextIndex := s.baseIndex

	var data [recordHeaderSize]byte
	for {
		_, err := s.f.ReadAt(data[:], int64(nextOffset))
		if err != nil {
			return err
		}

		foundIndex := getU64(data[:], recordHeaderStartOffsetIndex)
		dataLength := int(getU32(data[:], recordHeaderStartOffsetLength))

		if foundIndex == 0 || foundIndex == indexSentinelIndex {
			break
		}

		if foundIndex != nextIndex {
			return fmt.Errorf("mismatched index expected %v != %v", nextIndex, foundIndex)
		}

		offsets = append(offsets, uint32(nextOffset))
		nextOffset += recordSize(dataLength)
		nextIndex++
	}
	if _, err := s.f.Seek(int64(nextOffset), io.SeekStart); err != nil {
		return fmt.Errorf("error seeking to next offset: %w", err)
	}

	s.offsets = offsets
	s.nextOffset = nextOffset
	return nil
}

// createSegment creates a segment backed by a file at path fp, overwriting any
// file that might already be there.  The segment header is written and the
// returned segment is positioned ready to receive new logs.
func createSegment(fp string, baseIndex uint64, config LogConfig) (*segment, error) {
	f, err := os.OpenFile(fp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, err
	}

	if err := fileutil.Preallocate(f, 10*1024*1024, true); err != nil {
		return nil, err
	}

	var header [segment_header_size]byte
	setSegmentHeader(&header, baseIndex, config)
	if _, err := f.Write(header[:]); err != nil {
		_ = os.Remove(fp)
		return nil, err
	}

	if _, err := f.Seek(segmentDataInitOffset, io.SeekStart); err != nil {
		_ = os.Remove(fp)
		return nil, err
	}

	config.Logger.Trace("createSegment", "baseIndex", baseIndex, "fp", fp)
	return &segment{
		baseIndex:           baseIndex,
		openForWrite:        true,
		config:              config,
		offsets:             make([]uint32, 0, 512),
		nextOffset:          segmentDataInitOffset,
		f:                   f,
		fp:                  fp,
		bw:                  bufio.NewWriterSize(f, 4096),
		persistTransformers: persistTransformers(config.UserLogConfig),
		loadTransformers:    loadTransformers(config.UserLogConfig),
	}, nil
}

func (s *segment) untransform(data []byte) ([]byte, error) {
	return runTransformers(s.loadTransformers, data)
}

func getU32(src []byte, offset int) uint32 {
	return binary.BigEndian.Uint32(src[offset : offset+4])
}
func getU64(src []byte, offset int) uint64 {
	return binary.BigEndian.Uint64(src[offset : offset+8])
}

// readRecordAt reads a log record of length rl at the given offset and stores it in out.
// rl is optional as the length can also be read from the log at the cost of an extra read.
// Returns the length of data written to out and any error encountered.
func (s *segment) readRecordAt(offset, rl int, index uint64, out []byte) (int, error) {
	if rl == 0 {
		var l [4]byte
		_, err := s.f.ReadAt(l[:], int64(offset+recordHeaderStartOffsetLength))
		if err != nil {
			return 0, err
		}

		lf := int(binary.BigEndian.Uint32(l[:]))
		rl = recordSize(lf)
	}

	record := make([]byte, rl)
	_, err := s.f.ReadAt(record, int64(offset))
	if err != nil {
		return 0, err
	}
	lIndex := getU64(record, recordHeaderStartOffsetIndex)
	if lIndex != index {
		return 0, fmt.Errorf("index mismatch %v != %v", lIndex, index)
	}

	lChecksum := getU32(record, recordHeaderStartOffsetChecksum)
	lLength := int(getU32(record, recordHeaderStartOffsetLength))
	lPadding := recordPadding(lLength)

	if recordHeaderSize+lLength+lPadding != rl {
		return 0, fmt.Errorf("mismatch length: %v != %v", 16+lLength+lPadding, rl)
	}

	fChecksum := crc32.Checksum(record[recordHeaderSize:recordHeaderSize+lLength], crc32Table)
	if lChecksum != fChecksum {
		return 0, fmt.Errorf("checksums mismatch")
	}

	data, err := s.untransform(record[recordHeaderSize : recordHeaderSize+lLength])
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
	defer s.offsetLock.RUnlock()

	if index < s.baseIndex {
		return 0, errWrongSegment
	} else if index >= s.baseIndex+uint64(len(s.offsets)) {
		return 0, fmt.Errorf("index not in segment (%d > %d+%d): %w", index, s.baseIndex, len(s.offsets), raft.ErrLogNotFound)
	}

	li := index - s.baseIndex

	var rl int
	if li == uint64(len(s.offsets)-1) {
		rl = s.nextOffset - int(s.offsets[li])
	} else {
		rl = int(s.offsets[li+1] - s.offsets[li])
	}

	offset := s.offsets[li]
	n, err := s.readRecordAt(int(offset), rl, index, out)
	if err != nil {
		s.config.Logger.Trace("GetLog", "error", err, "s", s)
	}
	return n, err
}

func (s *segment) transform(data []byte) ([]byte, error) {
	return runTransformers(s.persistTransformers, data)
}

const (
	recordHeaderStartOffsetIndex    = 0
	recordHeaderStartOffsetChecksum = 8
	recordHeaderStartOffsetLength   = 12
	recordHeaderSize                = 16
)

var padding [8]byte

func putU32(dest []byte, offset int, value uint32) {
	binary.BigEndian.PutUint32(dest[offset:offset+4], value)
}
func putU64(dest []byte, offset int, value uint64) {
	binary.BigEndian.PutUint64(dest[offset:offset+8], value)
}

// writeRecord passes data through any configured transformers,
// creates a header for the record, then writes the header,
// record, and enough padding bytes to 64-bit align the record.
// Returns the number of bytes written or an error.
func (s *segment) writeRecord(index uint64, data []byte) (int, error) {
	s.config.Logger.Trace("writeRecord", "index", index, "fp", s.fp)

	var rh [16]byte
	var err error

	data, err = s.transform(data)
	if err != nil {
		return 0, err
	}

	// prepare header
	checksum := crc32.Checksum(data, crc32Table)

	putU64(rh[:], recordHeaderStartOffsetIndex, index)
	putU32(rh[:], recordHeaderStartOffsetChecksum, checksum)
	putU32(rh[:], recordHeaderStartOffsetLength, uint32(len(data)))

	if _, err := s.bw.Write(rh[:]); err != nil {
		return 0, err
	}
	if _, err := s.bw.Write(data); err != nil {
		return 0, err
	}
	if _, err := s.bw.Write(padding[:recordPadding(len(data))]); err != nil {
		return 0, err
	}

	return recordSize(len(data)), nil
}

func recordSize(dataLen int) int {
	return recordHeaderSize + dataLen + recordPadding(dataLen)
}

// StoreLogs consumes the iterator next, writing each log/byteslice it emits
// to the segment.  index is the log index of the first value returned by next,
// which must be one greater than the last index written to the segment.
// In the event of a write or fsync error, we attempt to undo any side effects
// or partial writes: we rewind the file pointer to where it was when we started,
// then write zeroes over the end of the file, which may truncate it.
func (s *segment) StoreLogs(index uint64, next func() []byte) (int, error) {
	if !s.openForWrite {
		return 0, errReadOnlyFile
	}

	var err error

	s.writeLock.Lock()
	defer s.writeLock.Unlock()

	s.offsetLock.RLock()
	if int(index-s.baseIndex) != len(s.offsets) {
		s.offsetLock.RUnlock()
		return 0, fmt.Errorf("index=%d baseIndex=%d lenOffsets=%d err=%w", index, s.baseIndex, len(s.offsets), errOutOfSequence)
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

		newOffsets = append(newOffsets, uint32(nextOffset))

		nextOffset += recSize
		writtenEntries++
		index++
	}

	if writtenEntries == 0 {
		return 0, nil
	}

	if err = s.bw.Flush(); err != nil {
		goto ROLLBACK
	}

	if err = s.sync(); err != nil {
		goto ROLLBACK
	}

	s.offsetLock.Lock()
	s.nextOffset = nextOffset
	s.offsets = append(s.offsets, newOffsets...)
	s.offsetLock.Unlock()

	return writtenEntries, nil
ROLLBACK:

	_, err = s.f.Seek(int64(startingOffset), io.SeekStart)
	if err != nil {
		return 0, err
	}
	err = fileutil.ZeroToEnd(s.f)
	if err != nil {
		return 0, err
	}
	return 0, err
}

func (s *segment) Close() error {
	return s.f.Close()
}

// parseIndexData deserializes an offset index.
func parseIndexData(data []byte) ([]uint32, error) {
	len := binary.BigEndian.Uint32(data[:4])
	out := make([]uint32, len)
	err := binary.Read(bytes.NewReader(data[4:]), binary.BigEndian, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// indexData serializes the offset index as a sequence of big-endian 32-bit words.
// The first word is the count of offsets.
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

// writeIndex persists an index of offsets as a special final record.
// This record always has the magic index value of MaxUint64.
// Returns the number of bytes written or error.
func (s *segment) writeIndex() (int, error) {
	s.offsetLock.RLock()
	defer s.offsetLock.RUnlock()

	data, err := s.indexData()
	if err != nil {
		return 0, err
	}

	recSize, err := s.writeRecord(indexSentinelIndex, data)
	if err != nil {
		return 0, err
	}
	if err := s.bw.Flush(); err != nil {
		return 0, err
	}

	return recSize, nil
}

func (s *segment) nextIndex() uint64 {
	s.offsetLock.RLock()
	defer s.offsetLock.RUnlock()

	return s.baseIndex + uint64(len(s.offsets))
}

// Seal seals the segment file, which means appending an index
// and modifying the segment header to store the sealFlag and
// the index offset and size.
func (s *segment) Seal() error {
	s.writeLock.Lock()
	defer s.writeLock.Unlock()

	indexOffset := s.nextOffset
	indexSize, err := s.writeIndex()
	if err != nil {
		return err
	}

	if err := s.sync(); err != nil {
		return err
	}

	var b [9]byte
	b[0] = sealFlag
	putU32(b[:], segmentSealStartOffsetIndexOffset, uint32(indexOffset))
	putU32(b[:], segmentSealStartOffsetIndexSize, uint32(indexSize))

	if _, err := s.f.WriteAt(b[:], segmentHeaderStartOffsetSeal); err != nil {
		return err
	}

	if err := s.sync(); err != nil {
		return err
	}

	return nil
}

// truncateTail makes index the new next index of the file.  Any log entries
// that had an index greater or equal will be deleted.
func (s *segment) truncateTail(index uint64) error {
	if !s.openForWrite {
		return errReadOnlyFile
	}

	if index < s.baseIndex {
		return fmt.Errorf("invalid index, less than base")
	}

	sealed, err := s.Sealed()
	if err != nil {
		return err
	}
	if sealed {
		// TODO we should support truncating sealed files.
		return errSealedFile
	}

	s.writeLock.Lock()
	defer s.writeLock.Unlock()

	diff := int(index - s.baseIndex)

	// TODO should we also be grabbing offsetLock?
	if diff >= len(s.offsets) {
		return nil
	}

	newNextOffset := s.offsets[diff]
	newOffsets := s.offsets[:diff]

	err = s.f.Truncate(int64(newNextOffset))
	if err != nil {
		return err
	}

	if err := s.sync(); err != nil {
		return err
	}

	_, err = s.f.Seek(int64(newNextOffset), io.SeekStart)
	if err != nil {
		return err
	}

	s.offsets = newOffsets
	s.nextOffset = int(newNextOffset)

	return nil
}

func (s *segment) sync() error {
	if s.config.NoSync {
		return nil
	}

	return fileutil.Fdatasync(s.f)
}

// recordPadding returns the number of bytes of padding needed to 64-bit align
// a record.
func recordPadding(length int) int {
	last := length & 0x7
	return (8 - last) & 0x7
}

// lastIndexInFile returns the last true index contained in the file.  In
// sealed segments the last record will be an offset index record, with an
// index value of MaxUInt.  For sealed segments lastIndexInFile returns the
// index of the penultimate record, i.e. that of the last actual log.
func (s *segment) lastIndexInFile() (uint64, error) {
	s.offsetLock.RLock()
	defer s.offsetLock.RUnlock()

	if len(s.offsets) == 0 {
		return 0, nil
	}

	o := len(s.offsets) - 1
	sealed, err := s.Sealed()
	if err != nil {
		return 0, err
	}

	switch {
	case sealed && o == 0:
		return 0, fmt.Errorf("sealed segment has no log records")
	case sealed:
		o--
	}

	return s.indexForRecordAtOffset(int(s.offsets[o]))
}

// indexForRecordOffset returns the raw value of the index field for the record
// at recordOffset bytes from the start of the file.
func (s *segment) indexForRecordAtOffset(recordOffset int) (uint64, error) {
	var data [recordHeaderSize]byte
	_, err := s.f.ReadAt(data[:], int64(recordOffset))
	if err != nil {
		return 0, err
	}

	foundIndex := getU64(data[:], recordHeaderStartOffsetIndex)
	return foundIndex, nil
}

// Sealed returns true if the segment is sealed.
func (s *segment) Sealed() (bool, error) {
	s.offsetLock.RLock()
	defer s.offsetLock.RUnlock()

	if len(s.offsets) == 0 {
		return false, nil
	}
	lastIndex, err := s.indexForRecordAtOffset(int(s.offsets[len(s.offsets)-1]))
	if err != nil {
		return false, err
	}
	return lastIndex == indexSentinelIndex, nil
}
