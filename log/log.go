package log

import (
	"fmt"
	"path/filepath"
	"sync"
)

type LogCompression byte

const (
	LogCompressionNone LogCompression = iota
	LogCompressionZlib
	LogCompressionGZip
)

type Log interface {
	FirstIndex() uint64
	LastIndex() uint64

	GetLog(index uint64) ([]byte, error)
	StoreLogs(nextIndex uint64, next func() []byte) error

	TruncateTail(index uint64) error
	TruncateHead(index uint64) error
}

type log struct {
	mu     sync.RWMutex
	dir    string
	config LogConfig

	firstIndex uint64
	lastIndex  uint64

	segmentBases  []uint64
	activeSegment *segment

	csMu          sync.RWMutex
	cachedSegment *segment

	firstIndexUpdatedCallback func(uint64) error
}

type UserLogConfig struct {
	SegmentChunkSize uint64

	NoSync bool

	Compression LogCompression

	TruncateOnFailure bool
}

type LogConfig struct {
	KnownFirstIndex uint64

	FirstIndexUpdatedCallback func(uint64) error

	UserLogConfig
}

const defaultLogChunkSize = 4096

func NewLog(dir string, c LogConfig) (*log, error) {
	bases, err := segmentsIn(dir)
	if err != nil {
		return nil, err
	}

	if c.SegmentChunkSize == 0 {
		c.SegmentChunkSize = defaultLogChunkSize
	}

	var active *segment
	if len(bases) == 0 {
		bases = append(bases, 1)
		active, err = newSegment(filepath.Join(dir, segmentName(1)), 1, true, c)
		if err != nil {
			return nil, err
		}

	}

	l := &log{
		dir:                       dir,
		firstIndex:                c.KnownFirstIndex,
		firstIndexUpdatedCallback: c.FirstIndexUpdatedCallback,
		segmentBases:              bases,
		activeSegment:             active,
		config:                    c,
	}

	return l, nil
}

func (l *log) FirstIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.firstIndex
}

func (l *log) updateIndexs(firstWriteIndex uint64, count int) error {
	if l.firstIndex == 0 && count != 0 {
		l.firstIndex = firstWriteIndex
		l.lastIndex = uint64(count)
	} else {
		l.lastIndex += uint64(count)
	}

	return nil
}

func (l *log) LastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.lastIndex
}

func (l *log) segmentFor(index uint64) (*segment, error) {
	firstIdx, lastIdx := l.firstIndex, l.lastIndex

	if index < firstIdx || index > lastIdx {
		return nil, fmt.Errorf("out of range: %v not in [%v, %v]", index, firstIdx, lastIdx)
	}

	if index >= l.activeSegment.baseIndex {
		return l.activeSegment, nil
	}

	sBase, err := searchSegmentIndex(l.segmentBases, index)
	if err != nil {
		return nil, err
	}

	l.csMu.Lock()
	defer l.csMu.Unlock()

	if l.cachedSegment != nil && l.cachedSegment.baseIndex == sBase {
		return l.cachedSegment, nil
	}

	seg, err := newSegment(filepath.Join(l.dir, segmentName(sBase)), sBase, false, l.config)
	if err != nil {
		return nil, err
	}

	l.cachedSegment.Close()
	l.cachedSegment = seg
	return seg, nil
}

func (l *log) GetLog(index uint64) ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	s, err := l.segmentFor(index)
	if err != nil {
		return nil, err
	}

	out := make([]byte, 1024*1024)
	n, err := s.GetLog(index, out)
	if err != nil {
		return nil, err
	}

	return out[:n], nil
}

func (l *log) StoreLogs(nextIndex uint64, next func() []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if nextIndex != l.lastIndex+1 {
		return fmt.Errorf("out of order insertion %v != %v", nextIndex, l.lastIndex+1)
	}

	err := l.maybeStartNewSegment()
	if err != nil {
		return err
	}

	entries, err := l.activeSegment.StoreLogs(nextIndex, next)
	if err != nil {
		l.updateIndexs(nextIndex, entries)
		return err
	}

	return l.updateIndexs(nextIndex, entries)
}

func (l *log) maybeStartNewSegment() error {
	s := l.activeSegment

	if s.nextIndex()-s.baseIndex < l.config.SegmentChunkSize {
		return nil
	}

	err := s.Seal()
	if err != nil {
		return err
	}
	s.Close()

	nextBase := s.nextIndex()
	active, err := newSegment(filepath.Join(l.dir, segmentName(nextBase)), nextBase, true, l.config)
	if err != nil {
		return err
	}

	l.segmentBases = append(l.segmentBases, nextBase)
	l.activeSegment = active

	return nil
}

func (l *log) TruncateTail(index uint64) error {
	panic("not implemented")
}

func (l *log) TruncateHead(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	err := l.firstIndexUpdatedCallback(index + 1)
	if err != nil {
		return err
	}
	l.firstIndex = index + 1
	return nil
}
