package raftwal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/hashicorp/go-msgpack/codec"
)

const (
	meta_page_size   = 4 * 1024
	meta_header_size = 512
)

var (
	meta_magic_header = []byte("__RAFT_WAL_META_v0__")
)

type MetaInfo struct {
	Path string
	Version uint64
}

type meta struct {
	Version uint64

	FirstIndex uint64            `codec:"fi"`
	BytesStore map[string][]byte `codec:"bs"`
	UintStore  map[string]uint64 `codec:"us"`
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

	dataBytes := bytes[4:]
	enc := codec.NewEncoderBytes(&dataBytes, msgPackHandle)
	err := enc.Encode(m)

	if err != nil {
		return nil, err
	}

	checksum := crc32.Checksum(bytes[4:], crc32Table)
	binary.BigEndian.PutUint32(bytes[:4], checksum)

	return bytes, nil
}

func loadMetaFromBytes(bytes []byte) (*meta, error) {
	if len(bytes) != meta_page_size {
		return nil, fmt.Errorf("failed to read meta page: invalid length (%v != %v)", len(bytes), meta_page_size)
	}

	storedChecksum := binary.BigEndian.Uint32(bytes[:4])

	dataBytes := bytes[4:]
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

func (w *wal) Get(key []byte) ([]byte, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	v, ok := w.meta.BytesStore[string(key)]
	if !ok {
		return nil, errNotFound
	}

	r := make([]byte, len(v))
	copy(r, v)
	return r, nil
}

func (w *wal) GetUint64(key []byte) (uint64, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	v, ok := w.meta.UintStore[string(key)]
	if !ok {
		return 0, errNotFound
	}

	return v, nil
}

func (w *wal) Set(key []byte, val []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	vc := make([]byte, len(val))
	copy(vc, val)

	w.meta.Version++
	w.meta.BytesStore[string(key)] = vc

	return w.writeMetaPage()
}

func (w *wal) SetUint64(key []byte, val uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.meta.Version++
	w.meta.UintStore[string(key)] = val

	return w.writeMetaPage()
}

func (w *wal) setFirstIndexLocked(idx uint64) error {
	w.meta.Version++
	w.meta.FirstIndex = idx

	return w.writeMetaPage()

}

func (w *wal) writeMetaPage() error {
	bytes, err := w.meta.toBytes()
	if err != nil {
		return fmt.Errorf("failed to serialize meta: %v", err)
	}

	slot := w.meta.Version % 2
	offset := slot*meta_page_size + meta_header_size
	_, err = w.metaFile.WriteAt(bytes, int64(offset))
	if err != nil {
		return fmt.Errorf("failed to persist meta: %v", err)
	}

	if !w.config.NoSync {
		err = fileutil.Fdatasync(w.metaFile)
		if err != nil {
			return fmt.Errorf("failed to persist meta: %v", err)
		}
	}

	return nil
}

func (w *wal) restoreMetaPage(path string) error {
	// open file if it is present
	f, err := os.OpenFile(path, os.O_RDWR, 0600)
	if os.IsNotExist(err) {
		return w.createMetaPage(path)
	} else if err != nil {
		return fmt.Errorf("failed to open meta.db file: %v", err)
	}

	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("failed to open meta.db: %v", err)
	}

	if fi.Size() < meta_header_size+2*meta_page_size {
		// torn write: crashed while creating file?!
		return w.createMetaPage(path)
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
	w.metaFile = f
	if metaA == nil {
		w.meta = metaB
	} else if metaB != nil && metaB.Version > metaA.Version {
		w.meta = metaB
	} else {
		w.meta = metaA
	}

	return nil
}

func (w *wal) createMetaPage(path string) error {
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

	if !w.config.NoSync {
		err = fileutil.Fsync(f)
		if err != nil {
			return fmt.Errorf("failed to persist meta: %v", err)
		}
	}

	w.meta = meta
	w.metaFile = f

	return nil

}

func (w *wal) GetMetaIfNewerVersion(version uint64) (*MetaInfo, error){
	if w.meta.Version == version {
		return nil, nil
	}
	if version > w.meta.Version {
		return nil, fmt.Errorf("requested version is higher than the stored one")
	}

	metaInfo := &MetaInfo{
		Path: filepath.Join(w.dir, "meta"),
		Version: w.meta.Version,
	}
	return metaInfo, nil
}
