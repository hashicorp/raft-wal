package raftwal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"

	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/ugorji/go/codec"
)

const (
	meta_page_size   = 4 * 1024
	meta_header_size = 32
)

var (
	meta_magic_header = []byte("__RAFT_WAL_META_v0__")
)

type meta struct {
	Version uint64

	FirstIndex uint64
	BytesStore map[string][]byte
	UintStore  map[string]uint64
}

func newMeta() *meta {
	return &meta{
		Version:    0,
		BytesStore: map[string][]byte{},
		UintStore:  map[string]uint64{},
	}
}

func (m *meta) toBytes() ([]byte, error) {
	bytes := make([]byte, meta_page_size)

	dataBytes := bytes[:meta_page_size-4]
	enc := codec.NewEncoderBytes(&dataBytes, msgPackHandle)
	err := enc.Encode(m)

	if err != nil {
		return nil, err
	}

	checksum := crc32.Checksum(bytes[:meta_page_size-4], crc32Table)
	binary.BigEndian.PutUint32(bytes[meta_page_size-4:], checksum)

	return bytes, nil
}

func loadMetaFromBytes(bytes []byte) (*meta, error) {
	if len(bytes) != meta_page_size {
		return nil, fmt.Errorf("failed to read meta page: invalid length (%v != %v)", len(bytes), meta_page_size)
	}

	storedChecksum := binary.BigEndian.Uint32(bytes[meta_page_size-4:])

	dataBytes := bytes[:meta_page_size-4]
	foundChecksum := crc32.Checksum(dataBytes, crc32Table)
	if storedChecksum != foundChecksum {
		return nil, fmt.Errorf("failed to read meta page: mismatch checksum")
	}

	var m meta
	err := codec.NewDecoderBytes(dataBytes, msgPackHandle).Decode(&m)
	if err != nil {
		return nil, fmt.Errorf("failed to read meta page: %v", err)
	}

	return &m, nil
}

func (l *wal) Get(key []byte) ([]byte, error) {
	l.metaLock.RLock()
	defer l.metaLock.RUnlock()

	v, ok := l.meta.BytesStore[string(key)]
	if !ok {
		return nil, errNotFound
	}

	r := make([]byte, len(v))
	copy(r, v)
	return r, nil
}

func (l *wal) GetUint64(key []byte) (uint64, error) {
	l.metaLock.RLock()
	defer l.metaLock.RUnlock()

	v, ok := l.meta.UintStore[string(key)]
	if !ok {
		return 0, errNotFound
	}

	return v, nil
}

func (l *wal) Set(key []byte, val []byte) error {
	l.metaLock.Lock()
	defer l.metaLock.Unlock()

	vc := make([]byte, len(val))
	copy(vc, val)

	l.meta.Version++
	l.meta.BytesStore[string(key)] = vc

	return l.writeMetaPage()
}

func (l *wal) SetUint64(key []byte, val uint64) error {
	l.metaLock.Lock()
	defer l.metaLock.Unlock()

	l.meta.Version++
	l.meta.UintStore[string(key)] = val

	return l.writeMetaPage()
}

func (l *wal) setFirstIndex(idx uint64) error {
	l.metaLock.Lock()
	defer l.metaLock.Unlock()

	l.meta.Version++
	l.meta.FirstIndex = idx

	return l.writeMetaPage()

}

func (l *wal) writeMetaPage() error {
	bytes, err := l.meta.toBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize meta: %v", err)
	}

	slot := l.meta.Version % 2
	offset := slot*meta_page_size + meta_header_size
	_, err = l.metaFile.WriteAt(bytes, int64(offset))
	if err != nil {
		return fmt.Errorf("failed to persist meta: %v", err)
	}

	err = fileutil.Fdatasync(l.metaFile)
	if err != nil {
		return fmt.Errorf("failed to persist meta: %v", err)
	}

	return nil
}

func (l *wal) restoreMetaPage(path string) error {
	// open file if it is present
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return l.createMetaPage(path)
	} else if err != nil {
		return fmt.Errorf("failed to open meta.db file: %v", err)
	}

	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("failed to open meta.db: %v", err)
	}

	if fi.Size() < meta_header_size+2*meta_page_size {
		// torn write: crashed while creating file?!
		return l.createMetaPage(path)
	}

	header_bytes := make([]byte, meta_header_size)
	_, err = f.ReadAt(header_bytes, 0)
	if err != nil {
		return fmt.Errorf("failed to open meta.db: %v", err)
	}

	if !bytes.Equal(header_bytes[:len(meta_magic_header)], meta_magic_header) {
		return fmt.Errorf("failed to open meta.db: unexpected header")
	}

	var metaA, metaB *meta
	var errA, errB error

	metaABytes := make([]byte, meta_page_size)
	_, err = f.ReadAt(metaABytes, meta_header_size)
	if err != nil {
		return fmt.Errorf("failed to open meta.db: failed to read first meta page: %v", err)
	}
	metaA, errA = loadMetaFromBytes(metaABytes)

	if fi.Size() > meta_header_size {
		metaBBytes := make([]byte, meta_page_size)
		_, err = f.ReadAt(metaBBytes, meta_header_size+meta_page_size)
		if err != nil {
			return fmt.Errorf("failed to open meta.db: failed to read second meta page: %v", err)
		}
		metaB, errB = loadMetaFromBytes(metaBBytes)
	}

	if errA != nil && errB != nil {
		return fmt.Errorf("failed to open meta.db: invalid meta pages: %v; %v", errA, errB)
	}

	// metaA is highest transaction
	l.metaFile = f
	if metaA == nil {
		l.meta = metaB
	} else if metaB != nil && metaB.Version > metaA.Version {
		l.meta = metaB
	} else {
		l.meta = metaA
	}

	return nil
}

func (l *wal) createMetaPage(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to open meta.db file: %v", err)
	}

	var bytes [meta_header_size + 2*meta_page_size]byte
	copy(bytes[:], meta_magic_header)

	meta := newMeta()
	metaBytes, err := meta.toBytes()
	if err != nil {
		return fmt.Errorf("failed to create meta.db file: %v", err)
	}

	copy(bytes[meta_header_size:], metaBytes)
	_, err = f.WriteAt(bytes[:], 0)
	if err != nil {
		return fmt.Errorf("failed to create meta.db file: %v", err)
	}

	err = fileutil.Fsync(f)
	if err != nil {
		return fmt.Errorf("failed to persist meta: %v", err)
	}

	l.meta = meta
	l.metaFile = f

	return nil

}
