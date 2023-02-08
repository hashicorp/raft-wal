// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package wal

const (
	// FirstExternalStableCodecID is the lowest value an external code may use
	// to identify their StableStore codec. Values lower than this are reserved
	// for future internal use.
	FirstExternalStableCodecID = 1 << 16

	// StableCodec* constants identify internally-defined codec identifiers.
	StableCodecPassthruV1 uint64 = iota
)

// StableCodec is the interface required for encoding/decoding log entries. Callers
// can pass a custom one to manage their own serialization, or to add additional
// layers like encryption or compression of records.
type StableCodec interface {
	// ID returns the globally unique identifier for this codec version. This is
	// encoded in the meta file and must remain consistent over the life
	// of the StableStore. Values up to FirstExternalStableCodecID are reserved and
	// will error if specified externally.
	ID() uint64

	// Encode a series of bytes and return it. Used by Set and SetUint64
	StableEncode([]byte) ([]byte, error)

	// Decode a series of bytes and return it. used by Get and GetUint64
	StableDecode([]byte) ([]byte, error)
}

type StableCodecID uint64

// StablePassthruCodec is a Codec that simply returns data passed to it
type StablePassthruCodec struct{}

// ID returns the globally unique identifier for this codec version. This is
// encoded into segment file headers and must remain consistent over the life
// of the log. Values up to FirstExternalCodecID are reserved and will error
// if specified externally.
func (s *StablePassthruCodec) ID() uint64 {
	return StableCodecPassthruV1
}

// Just return
func (s *StablePassthruCodec) StableEncode(bs []byte) ([]byte, error) {
	return bs, nil
}

// Just return
func (s *StablePassthruCodec) StableDecode(es []byte) ([]byte, error) {
	return es, nil
}
