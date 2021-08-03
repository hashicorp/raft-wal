package log

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/raft"
)

const defaultLogChunkSize = 4096

type LogCompression byte

const (
	LogCompressionNone LogCompression = iota
	LogCompressionZlib
	LogCompressionGZip
)

type SegmentInfo struct {
	Path     string
	LogCount int
}

type Log interface {
	FirstIndex() uint64
	LastIndex() uint64

	GetLog(index uint64) ([]byte, error)
	StoreLogs(nextIndex uint64, next func() []byte) error

	TruncateTail(index uint64) error
	TruncateHead(index uint64) error

	Close() error

	GetSealedLogPath(index uint64) (*SegmentInfo, error)
}

type UserLogConfig struct {
	SegmentChunkSize uint64

	NoSync bool

	Compression LogCompression

	TruncateOnFailure bool

	Logger hclog.Logger
}

type LogConfig struct {
	KnownFirstIndex uint64

	FirstIndexUpdatedCallback func(uint64) error

	UserLogConfig
}

// log manages a collection of segment files on disk.
type log struct {
	mu      sync.RWMutex
	dir     string
	dirFile *os.File
	config  LogConfig

	lf *lockFile

	// firstIndex is the first log index known to Raft
	firstIndex uint64
	// lastIndex is the last log index known; the next StoreLogs call
	// must be for an index of lastIndex+1.
	lastIndex uint64

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
		return nil, fmt.Errorf("failed to read segment directory: %v", err)
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
			return nil, fmt.Errorf("failed to open segment: %v", err)
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

	base := bases[len(bases)-1]
	s, err := l.segmentForUnsafe(base)
	if err != nil {
		return nil, fmt.Errorf("failed getting segment for index %d: %v", base, err)
	}
	idx, err := s.lastIndexInFile()
	if err != nil {
		return nil, fmt.Errorf("failed to get last index: %v", err)
	}
	l.lastIndex = idx

	if l.lastIndex > 0 && l.activeSegment == nil {
		if err := l.startNewSegment(l.lastIndex + 1); err != nil {
			return nil, fmt.Errorf("failed to resume at index %d: %w", l.lastIndex+1, err)
		}
	}

	return l, nil
}

func (l *log) FirstIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.firstIndex
}

func (l *log) updateIndexs(firstWriteIndex uint64, count int) error {
	if count == 0 {
		return nil
	}

	switch {
	case l.firstIndex == 0:
		l.firstIndex = firstWriteIndex
		err := l.firstIndexUpdatedCallback(firstWriteIndex)
		if err != nil {
			return err
		}
		l.lastIndex = l.firstIndex + uint64(count) - 1
	case l.lastIndex == 0:
		l.lastIndex = l.firstIndex + uint64(count) - 1
	default:
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
	if index > l.lastIndex {
		return nil, fmt.Errorf("index too big (%d > %d): %w", index, l.lastIndex, raft.ErrLogNotFound)
	}

	return l.segmentForUnsafe(index)
}

// segmentForUnsafe is a version of segmentFor that doesn't check to ensure if
// the index is newer than lastIndex, e.g. because we're just starting up and
// don't know the lastIndex yet.
func (l *log) segmentForUnsafe(index uint64) (*segment, error) {
	if index < l.firstIndex {
		return nil, fmt.Errorf("index too small (%d < %d): %w", index, l.firstIndex, raft.ErrLogNotFound)
	}

	// TODO remove first clause. Probably instead we should create an activeSegment,
	// e.g. for after a snapshot restore based on another node's logs.
	if l.activeSegment != nil && index >= l.activeSegment.baseIndex {
		l.config.Logger.Trace("segmentForUnsafe returning active segment", "s", l.activeSegment)
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

	if l.cachedSegment != nil && l.cachedSegment != l.activeSegment {
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
		return nil, raft.ErrLogNotFound
	}

	out := make([]byte, 1024*1024)
	n, err := s.GetLog(index, out)
	if err != nil {
		return nil, err
	}

	return out[:n], nil
}

// StoreLogs consumes the iterator next, writing each log/byteslice it emits
// to the log store.  The first value returned by next has log index nextIndex.
// TODO we currently assume all byteslices will fit in the current segment,
// we should instead check for full segment on each write.
func (l *log) StoreLogs(nextIndex uint64, next func() []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// If lastIndex is zero don't enforce this check.  After a total log
	// deletion (e.g. due to a snapshot restore), the raft lib expects LastIndex
	// to return 0.
	if l.lastIndex != 0 && nextIndex != l.lastIndex+1 {
		return fmt.Errorf("out of order insertion nextIndex=%v != lastIndex+1=%v", nextIndex, l.lastIndex+1)
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

// TruncateHead deletes all entries up to and including index. After returning
// a non-nil error, l.firstIndex will be index+1.  Note that while we delete files
// consisting solely of deleted log indexes, the file that contains a mix of
// deleted and kept indexes will not be scrubbed of the deleted records.  We
// rely on the meta page to keep track of what the real "firstIndex" is.
func (l *log) TruncateHead(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// make deletion idempotent, so deleting what's already deleted doesn't
	// fail
	if index < l.firstIndex {
		return nil
	}

	l.firstIndex = index + 1

	err := l.firstIndexUpdatedCallback(l.firstIndex)
	if err != nil {
		return err
	}

	if err := l.deleteOldLogFiles(); err != nil {
		return err
	}

	seg, err := l.segmentFor(index)
	if err != nil && !errors.Is(err, raft.ErrLogNotFound) {
		return err
	}
	if seg == l.activeSegment {
		if err := l.clearCachedSegment(); err != nil {
			return err
		}
	}

	deleteAll := index >= l.lastIndex
	if deleteAll {
		l.lastIndex = 0
		err := l.startNewSegment(index + 1)
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *log) deleteOldLogFiles() error {
	delIdx := segmentContainingIndex(l.segmentBases, l.firstIndex)

	toDelete, toKeep := l.segmentBases[:delIdx], l.segmentBases[delIdx:]

	l.config.Logger.Trace("deleteOldLogs", "toDelete", toDelete, "toKeep", toKeep)

	for _, sb := range toDelete {
		fp := filepath.Join(l.dir, segmentName(sb))
		if err := os.Remove(fp); err != nil {
			return fmt.Errorf("error removing old log segment from disk: %v", err)
		}
	}

	l.segmentBases = toKeep
	return nil
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
		return c.Close()
	}
	return nil
}

func (l *log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.config.Logger.Trace("closing raft-wal")
	var ret *multierror.Error
	if err := l.lf.Close(); err != nil {
		ret = multierror.Append(ret, err)
	}
	if err := l.activeSegment.Close(); err != nil {
		ret = multierror.Append(ret, err)
	}
	if err := l.clearCachedSegment(); err != nil {
		ret = multierror.Append(ret, err)
	}
	l.activeSegment = nil
	return ret.ErrorOrNil()
}

func (l *log) syncDir() error {
	if l.config.NoSync {
		return nil
	}

	return fileutil.Fsync(l.dirFile)
}

func (l *log) GetSealedLogPath(index uint64) (*SegmentInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.activeSegment.baseIndex <= index {
		return nil, nil
	}

	s, err := l.segmentFor(index)
	if err != nil {
		return nil, err
	}

	return &SegmentInfo{
		Path:     s.f.Name(),
		LogCount: len(s.offsets),
	}, nil
}
