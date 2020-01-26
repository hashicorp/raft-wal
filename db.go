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

//var _ raft.LogStore = (*wal)(nil)
var _ raft.StableStore = (*wal)(nil)

var errNotFound = errors.New("not found")

type wal struct {
	metaFile *os.File
	meta     *meta
	metaLock sync.RWMutex

	nextOffset uint64

	dir string

	logChunks uint32
}

func newWAL(dir string, logChunkSize uint32) (*wal, error) {
	return nil, fmt.Errorf("not implemented yet")

}
