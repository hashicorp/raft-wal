// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package wal

import (
	"io"

	"github.com/hashicorp/raft"
)

const (
	// FirstExternalCodecID is the lowest value an external code may use to
	// identify their codec. Values lower than this are reserved for future
	// internal use.
	FirstExternalCodecID = 1 << 16

	// Codec* constants identify internally-defined codec identifiers.
	CodecBinaryV1 uint64 = iota
)

// Codec is the interface required for encoding/decoding log entries. Callers
// can pass a custom one to manage their own serialization, or to add additional
// layers like encryption or compression of records. Each codec
type Codec interface {
	// ID returns the globally unique identifier for this codec version. This is
	// encoded into segment file headers and must remain consistent over the life
	// of the log. Values up to FirstExternalCodecID are reserved and will error
	// if specified externally.
	ID() uint64

	// Encode the log into the io.Writer. We pass a writer to allow the caller to
	// manage buffer allocation and re-use.
	Encode(l *raft.Log, w io.Writer) error

	// Decode a log from the passed byte slice into the log entry pointed to. This
	// allows the caller to manage allocation and re-use of the bytes and log
	// entry.
	Decode([]byte, *raft.Log) error
}

// BinaryCodec implements a simple hand-rolled binary encoding of raft.Log. We
// must ensure we keep in sync with raft.Log if any new fields are added. Using
// our own codec instead of Msgpack or protobuf or something may seem odd, but
// since we don't own raft.Log struct we have to manually redefine it in a proto
// schema or our own version to allow for a fast, generated codec anyway which
// is preferable to reflection-based codecs. Since raft.Log is small and doesn't
// change much, lets just encode it really simply and quickly.
type BinaryCodec struct {
}

// TODO implement
