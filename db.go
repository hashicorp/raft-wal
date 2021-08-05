package raftwal

import (
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"

	"github.com/hashicorp/go-msgpack/codec"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-wal/log"
)

var (
	msgPackHandle = &codec.MsgpackHandle{}
	crc32Table    = crc32.MakeTable(crc32.Castagnoli)
)

type LogConfig = log.UserLogConfig

func New(dir string) (*wal, error) {
	return NewWAL(dir, LogConfig{})
}

type FileWALLog interface {
	raft.LogStore
	Close() error
	// GetSealedLogPath returns the sealed segment file that contains index
	// or an error if none is found.  A nil SegmentInfo with no error
	// is returned if index is part of the active segment, i.e. the segment
	// file is not yet sealed.
	GetSealedLogPath(index uint64) (*log.SegmentInfo, error)
	GetSealedLogFiles(startIndex uint64) ([]*log.SegmentInfo, error)
	GetMetaIfNewerVersion(version uint64) (*MetaInfo, error)
}

var _ FileWALLog = (*wal)(nil)
var _ raft.LogStore = (*wal)(nil)
var _ raft.StableStore = (*wal)(nil)

var errNotFound = errors.New("not found")

type wal struct {
	mu sync.RWMutex

	metaFile *os.File
	meta     *meta

	log    log.Log
	dir    string
	config LogConfig
}

func NewWAL(dir string, c LogConfig) (*wal, error) {
	wal := &wal{
		dir:    dir,
		config: c,
	}

	err := wal.restoreMetaPage(filepath.Join(dir, "meta"))
	if err != nil {
		return nil, fmt.Errorf("failed to create meta: %v", err)
	}

	// note that log.NewLog will sync directory, including metapage

	wal.log, err = log.NewLog(dir, log.LogConfig{
		KnownFirstIndex:           wal.meta.FirstIndex,
		FirstIndexUpdatedCallback: wal.setFirstIndex,
		UserLogConfig:             c,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create log: %v", err)
	}

	return wal, nil

}

func (w *wal) setFirstIndex(newIndex uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.setFirstIndexLocked(newIndex)
}

// FirstIndex returns the first index written. 0 for no entries.
func (w *wal) FirstIndex() (uint64, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.meta.FirstIndex, nil
}

// LastIndex returns the last index written. 0 for no entries.
func (w *wal) LastIndex() (uint64, error) {
	return w.log.LastIndex(), nil
}

// GetLog gets a log entry at a given index.
func (w *wal) GetLog(index uint64, log *raft.Log) error {
	b, err := w.log.GetLog(index)
	if err != nil {
		return err
	}

	err = codec.NewDecoderBytes(b, msgPackHandle).Decode(log)
	return err
}

// StoreLog stores a log entry.
func (w *wal) StoreLog(log *raft.Log) error {
	return w.StoreLogs([]*raft.Log{log})
}

// StoreLogs stores multiple log entries.
func (w *wal) StoreLogs(logs []*raft.Log) error {
	if len(logs) == 0 {
		return nil
	}

	encoder := codec.NewEncoderBytes(nil, msgPackHandle)

	lastIndex := logs[0].Index - 1
	i := 0

	var berr error
	bytes := func() []byte {
		if i >= len(logs) {
			return nil
		}

		l := logs[i]
		i++

		if l.Index != lastIndex+1 {
			berr = fmt.Errorf("storing non-consecutive logs: %v != %v", l.Index, lastIndex+1)
			return nil
		}
		lastIndex = l.Index

		var b []byte
		encoder.ResetBytes(&b)
		berr = encoder.Encode(l)
		if berr != nil {
			return nil
		}

		return b
	}

	err := w.log.StoreLogs(logs[0].Index, bytes)
	if err != nil {
		return err
	}

	return berr
}

// DeleteRange deletes a range of log entries. The range is inclusive.
func (w *wal) DeleteRange(min, max uint64) error {
	firstIdx, _ := w.FirstIndex()
	lastIdx, _ := w.LastIndex()

	if min <= firstIdx && max > firstIdx && max <= lastIdx {
		return w.log.TruncateHead(max)
	} else if min > firstIdx && max == lastIdx {
		return w.log.TruncateTail(min)
	}

	return fmt.Errorf("deleting mid ranges not supported [%v, %v] is in [%v, %v]",
		min, max, firstIdx, lastIdx)
}

func (w *wal) Close() error {
	return w.log.Close()
}

func (w *wal) GetSealedLogPath(index uint64) (*log.SegmentInfo, error) {
	return w.log.GetSealedLogPath(index)
}

func (w *wal) GetSealedLogFiles(startIndex uint64) ([]*log.SegmentInfo, error) {
	return w.log.GetSealedLogFiles(startIndex)
}