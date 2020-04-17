package log

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/hashicorp/go-msgpack/codec"
)

var (
	msgPackHandle = &codec.MsgpackHandle{}
)

type commandType uint32

const (
	cmdInitializing commandType = iota
	cmdTruncatingTail
)

type command struct {
	Type  commandType
	Index uint64
}

type lockFile struct {
	f  *os.File
	lf *fileutil.LockedFile

	noSync bool
}

func newLockFile(p string, noSync bool) (*lockFile, error) {
	rf, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	lf, err := fileutil.TryLockFile(p, 0, 0600)
	if err != nil {
		return nil, err
	}

	return &lockFile{
		f:      rf,
		lf:     lf,
		noSync: noSync,
	}, nil
}

func (f *lockFile) commit() error {
	err := f.f.Truncate(0)
	if err != nil {
		return err
	}

	if !f.noSync {
		return fileutil.Fdatasync(f.f)
	}

	return nil
}

const txEntrySize = 512

func (f *lockFile) startTransaction(c command) error {
	var bytes [txEntrySize]byte
	dataBytes := bytes[4:]

	err := codec.NewEncoderBytes(&dataBytes, msgPackHandle).Encode(c)
	if err != nil {
		return err
	}

	checksum := crc32.Checksum(bytes[4:], crc32Table)
	binary.BigEndian.PutUint32(bytes[:4], checksum)

	_, err = f.f.WriteAt(bytes[:], 0)
	if err != nil {
		return err
	}

	if !f.noSync {
		return fileutil.Fdatasync(f.f)
	}

	return nil
}

func (f *lockFile) currentTransaction() (*command, error) {
	bytes := make([]byte, txEntrySize)

	n, err := f.f.ReadAt(bytes, 0)
	if err == io.EOF && n == 0 {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	storedChecksum := binary.BigEndian.Uint32(bytes[:4])
	foundChecksum := crc32.Checksum(bytes[4:], crc32Table)
	if storedChecksum != foundChecksum {
		return nil, fmt.Errorf("failed to validate checksum")
	}

	var c command
	err = codec.NewDecoderBytes(bytes[4:], msgPackHandle).Decode(&c)
	if err != nil {
		return nil, fmt.Errorf("failed to read log entry: %v", err)
	}

	return &c, nil
}

func (f *lockFile) Close() error {
	f.lf.Close()
	return f.f.Close()
}
