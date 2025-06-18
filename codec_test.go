// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package wal

import (
	"bytes"
	"testing"
	"time"

	fuzz "github.com/google/gofuzz"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

// TestBinaryCodecFuzz tests that our codec can decode whatever it encoded.
// Because we are using a reflection-based fuzzer to assign random values to all
// fields this test will also catch any changes in a later version of raft that
// add new fields since our codec will "loose" them.
func TestBinaryCodecFuzz(t *testing.T) {
	rounds := 1000

	f := fuzz.New().Funcs(
		// Stub time since gofuzz generates unencodable times depending on your
		// local timezone! On my computer in GMT timezone, it will generate Times
		// that are unencodable for some reason I don't understand. All it's doing
		// is picking a random UnixTimestamp but for some reason that is sometimes
		// unencodable?
		func(t *time.Time, c fuzz.Continue) {
			// This is copied from fuzzTime in gofuzz but with a fix until it's
			// accepted upstream.
			var sec, nsec int64
			// Allow for about 1000 years of random time values, which keeps things
			// like JSON parsing reasonably happy.
			sec = c.Int63n(1000 * 365 * 24 * 60 * 60)
			nsec = c.Int63n(999_999_999)
			*t = time.Unix(sec, nsec)
		},
	)
	c := BinaryCodec{}

	require.Equal(t, CodecBinaryV1, c.ID())

	var buf bytes.Buffer

	for i := 0; i < rounds; i++ {
		var log, log2 raft.Log
		f.Fuzz(&log)
		buf.Reset()

		err := c.Encode(&log, &buf)
		require.NoError(t, err)

		err = c.Decode(buf.Bytes(), &log2)
		require.NoError(t, err)

		t.Logf("log %#v. Binary: % x", log, buf.Bytes())

		require.Equal(t, log, log2)
	}
}

func TestBinaryCodecCopysOnDecode(t *testing.T) {
	var in, out raft.Log

	in.Index = 1234
	in.Term = 2
	in.Type = raft.LogCommand
	in.Data = []byte("foo")
	in.Extensions = []byte("ext")

	c := BinaryCodec{}
	var buf bytes.Buffer
	require.NoError(t, c.Encode(&in, &buf))

	rawBytes := buf.Bytes()

	require.NoError(t, c.Decode(rawBytes, &out))

	// Make sure the decoded data is the same
	require.Equal(t, string(out.Data), "foo")
	require.Equal(t, string(out.Extensions), "ext")

	// Intentionally mangle the buffer contents
	for i := 0; i < len(rawBytes); i++ {
		rawBytes[i] = 'x'
	}

	// Make sure the decoded data is still the same (i.e. didn't refer to the
	// underlying bytes)
	require.Equal(t, string(out.Data), "foo")
	require.Equal(t, string(out.Extensions), "ext")

}
