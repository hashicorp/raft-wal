package log

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/coreos/etcd/pkg/fileutil"
)

const defaultLogChunkSize = 4096

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

	Close() error
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

type log struct {
	mu      sync.RWMutex
	dir     string
	dirFile *os.File
	config  LogConfig

	lf *lockFile

	firstIndex uint64
	lastIndex  uint64

	segmentBases  []uint64
	activeSegment *segment

	csMu          sync.RWMutex
	cachedSegment *segment

	firstIndexUpdatedCallback func(uint64) error
}

func NewLog(dir string, c LogConfig) (*log, error) {

	dirFile, err := fileutil.OpenDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to open directory: %v", err)
	}

	lf, err := newLockFile(filepath.Join(dir, "lock"), c.NoSync)
	if err != nil {
		return nil, fmt.Errorf("failed to open lock file: %v", err)
	}

	bases, err := segmentsIn(dir)
	if err != nil {
		return nil, err
	}

	if c.SegmentChunkSize == 0 {
		c.SegmentChunkSize = defaultLogChunkSize
	}
	if len(bases) == 0 && c.KnownFirstIndex != 0 {
		return nil, fmt.Errorf("new WAL log requires 0 index")
	}

	var active *segment
	if len(bases) == 0 {
		bases = append(bases, 1)
		active, err = openSegment(filepath.Join(dir, segmentName(1)), 1, true, c)
		if err != nil {
			return nil, err
		}

	}

	l := &log{
		dir:                       dir,
		dirFile:                   dirFile,
		lf:                        lf,
		firstIndex:                c.KnownFirstIndex,
		firstIndexUpdatedCallback: c.FirstIndexUpdatedCallback,
		segmentBases:              bases,
		activeSegment:             active,
		config:                    c,
	}

	// delete old files if they present from older run
	l.deleteOldLogFiles()
	l.redoPendingTransaction()
	l.syncDir()

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
		return nil, errLogNotFound
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

	seg, err := openSegment(filepath.Join(l.dir, segmentName(sBase)), sBase, false, l.config)
	if err != nil {
		return nil, err
	}

	if l.cachedSegment != nil {
		l.cachedSegment.Close()
	}
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

	return l.startNewSegment(s.nextIndex())
}

func (l *log) startNewSegment(nextBase uint64) error {
	active, err := openSegment(filepath.Join(l.dir, segmentName(nextBase)), nextBase, true, l.config)
	if err != nil {
		return err
	}
	err = l.syncDir()
	if err != nil {
		return err
	}

	l.segmentBases = append(l.segmentBases, nextBase)
	l.activeSegment = active

	return nil

}

func (l *log) TruncateTail(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	err := l.lf.startTransaction(command{Type: cmdTruncatingTail, Index: index})
	if err != nil {
		return err
	}

	err = l.truncateTailImpl(index)
	if err != nil {
		return fmt.Errorf("failed to truncate tail: %v", err)
	}

	return l.lf.commit()
}

func (l *log) truncateTailImpl(index uint64) error {

	// make deletion idempotent, so deleting what's already deleted doesn't
	// fail
	if index > l.lastIndex {
		return nil
	}

	if index <= l.firstIndex {
		return fmt.Errorf("deletion would render log empty")
	}

	if index >= l.activeSegment.baseIndex {
		if err := l.activeSegment.truncateTail(index); err != nil {
			return err
		}
		l.lastIndex = index - 1

		return l.lf.commit()
	}

	l.activeSegment.Close()

	idx := segmentContainingIndex(l.segmentBases, index)
	if l.segmentBases[idx] == index {
		idx--
	}

	toKeep, toDelete := l.segmentBases[:idx+1], l.segmentBases[idx+1:]
	for _, sb := range toDelete {
		fp := filepath.Join(l.dir, segmentName(sb))
		if err := os.Remove(fp); err != nil {
			return err
		}
	}
	l.segmentBases = toKeep

	l.clearCachedSegment()

	err := l.startNewSegment(index)
	if err != nil {
		return err
	}
	err = l.syncDir()
	if err != nil {
		return err
	}

	l.lastIndex = index - 1
	return l.lf.commit()
}

func (l *log) TruncateHead(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// make deletion idempotent, so deleting what's already deleted doesn't
	// fail
	if index < l.firstIndex {
		return nil
	}

	if index >= l.lastIndex {
		return fmt.Errorf("deletion would render log empty")
	}

	err := l.firstIndexUpdatedCallback(index + 1)
	if err != nil {
		return err
	}
	l.firstIndex = index + 1

	l.deleteOldLogFiles()

	return nil
}

func (l *log) deleteOldLogFiles() {
	delIdx := segmentContainingIndex(l.segmentBases, l.firstIndex)

	toDelete, toKeep := l.segmentBases[:delIdx], l.segmentBases[delIdx:]

	for _, sb := range toDelete {
		fp := filepath.Join(l.dir, segmentName(sb))
		os.Remove(fp)
	}

	l.segmentBases = toKeep
}

func (l *log) redoPendingTransaction() error {
	c, err := l.lf.currentTransaction()
	if err != nil {
		return err
	}
	if c == nil {
		return nil
	}

	switch c.Type {
	case cmdTruncatingTail:
		err := l.truncateTailImpl(c.Index)
		if err != nil {
			return err
		}
		return l.lf.commit()
	default:
		return fmt.Errorf("unknown command: %v", c.Type)
	}
}

func (l *log) clearCachedSegment() error {
	l.csMu.Lock()
	defer l.csMu.Unlock()

	c := l.cachedSegment
	l.cachedSegment = nil

	if c != nil {
		return l.cachedSegment.Close()
	}
	return nil
}

func (l *log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	err := l.activeSegment.Close()
	berr := l.clearCachedSegment()
	if err != nil {
		return err
	}
	return berr
}

func (l *log) syncDir() error {
	if l.config.NoSync {
		return nil
	}

	return fileutil.Fsync(l.dirFile)
}
