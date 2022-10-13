// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package wal

import "github.com/hashicorp/go-hclog"

// WithCodec is an option that allows a custom Codec to be provided to the WAL.
// If not used the default Codec is used.
func WithCodec(c Codec) walOpt {
	return func(w *WAL) {
		w.codec = c
	}
}

// WithMetaStore is an option that allows a custom MetaStore to be provided to
// the WAL. If not used the default MetaStore is used.
func WithMetaStore(db MetaStore) walOpt {
	return func(w *WAL) {
		w.metaDB = db
	}
}

// WithLogger is an option that allows a custom logger to be used.
func WithLogger(logger hclog.Logger) walOpt {
	return func(w *WAL) {
		w.log = logger
	}
}
