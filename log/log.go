package log

import (
	"fmt"
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
	mu sync.RWMutex

	firstIndex uint64
	lastIndex  uint64

	segmentChunkSize uint64

	segments      []*segment
	activeSegment *segment

	firstIndexUpdatedCallback func(uint64) error
}

type LogConfig struct {
	KnownFirstIndex  uint64
	SegmentChunkSize uint64

	FirstIndexUpdatedCallback func(uint64) error

	NoSync bool

	Compression LogCompression

	TruncateOnFailure bool
}

func NewLog(dir string, c *LogConfig) (Log, error) {
	l := &log{
		firstIndex:                c.KnownFirstIndex,
		segmentChunkSize:          c.SegmentChunkSize,
		firstIndexUpdatedCallback: c.FirstIndexUpdatedCallback,
	}

	return l, nil
}

func (l *log) FirstIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.firstIndex
}

func (l *log) LastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.lastIndex
}

func (l *log) GetLog(index uint64) ([]byte, error) {
	l.mu.RLock()
	firstIdx, lastIdx := l.firstIndex, l.lastIndex
	l.mu.RUnlock()

	if index < firstIdx || index > lastIdx {
		return nil, fmt.Errorf("out of range: %v not in [%v, %v]", index, firstIdx, lastIdx)
	}

	panic("not implemented")
}

func (l *log) StoreLogs(nextIndex uint64, next func() []byte) error {
	panic("not implemented")
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
