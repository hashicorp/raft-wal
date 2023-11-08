// Copyright (c) HashiCorp, Inc.

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/hashicorp/raft"
	wal "github.com/hashicorp/raft-wal"
	"github.com/hashicorp/raft-wal/fs"
	"github.com/hashicorp/raft-wal/segment"
	"github.com/hashicorp/raft-wal/types"
)

type opts struct {
	Dir    string
	After  uint64
	Before uint64
}

func main() {
	var o opts
	flag.Uint64Var(&o.After, "after", 0, "specified a raft index to use as an exclusive lower bound when dumping log entries.")
	flag.Uint64Var(&o.Before, "before", 0, "specified a raft index to use as an exclusive upper bound when dumping log entries.")

	flag.Parse()

	// Accept dir as positional arg
	o.Dir = flag.Arg(0)
	if o.Dir == "" {
		fmt.Println("Usage: waldump [-after INDEX] [-before INDEX] <path to WAL dir>")
		os.Exit(1)
	}

	vfs := fs.New()
	f := segment.NewFiler(o.Dir, vfs)

	codec := &wal.BinaryCodec{}
	var log raft.Log
	enc := json.NewEncoder(os.Stdout)

	err := f.DumpLogs(o.After, o.Before, func(info types.SegmentInfo, e types.LogEntry) (bool, error) {
		if info.Codec != wal.CodecBinaryV1 {
			return false, fmt.Errorf("unsupported codec %d in file %s", info.Codec, info.FileName())
		}
		if err := codec.Decode(e.Data, &log); err != nil {
			return false, err
		}
		// Output the raft Log struct as JSON
		if err := enc.Encode(log); err != nil {
			return false, err
		}
		return true, nil
	})
	if err != nil {
		fmt.Printf("ERROR: %s\n", err)
		os.Exit(1)
	}
}
