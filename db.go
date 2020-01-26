package raftwal

import (
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/ugorji/go/codec"
)

const (
	logChunkSize = 4096
)

var (
	msgPackHandle = &codec.MsgpackHandle{}
	crc32Table    = crc32.MakeTable(crc32.Castagnoli)
)

func New(dir string) (*wal, error) {
	return newWAL(dir, logChunkSize)
}

var _ raft.LogStore = (*wal)(nil)
var _ raft.StableStore = (*wal)(nil)

var errNotFound = errors.New("not found")

type wal struct {
	mu sync.RWMutex

	metaFile *os.File
	meta     *meta

	dir string
}

func newWAL(dir string, logChunkSize uint32) (*wal, error) {
	return nil, fmt.Errorf("not implemented yet")

}

// FirstIndex returns the first index written. 0 for no entries.
func (w *wal) FirstIndex() (uint64, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.meta.FirstIndex, nil
}

// LastIndex returns the last index written. 0 for no entries.
func (w *wal) LastIndex() (uint64, error) {
	panic("not implemented")
}

// GetLog gets a log entry at a given index.
func (w *wal) GetLog(index uint64, log *raft.Log) error {
	panic("not implemented")
}

// StoreLog stores a log entry.
func (w *wal) StoreLog(log *raft.Log) error {
	panic("not implemented")
}

// StoreLogs stores multiple log entries.
func (w *wal) StoreLogs(logs []*raft.Log) error {
	panic("not implemented")
}

// DeleteRange deletes a range of log entries. The range is inclusive.
func (w *wal) DeleteRange(min, max uint64) error {
	panic("not implemented")
}

func (w *wal) purgeOldLogFiles() error {
	panic("not implemented")

}

func (w *wal) truncateLogs(idx uint64) error {
	panic("not implemented")
}
