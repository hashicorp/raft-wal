// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package wal

// WithCodec is an option that allows a custom Codec to be provided to the WAL.
// If not used the default Codec is used.
func WithCodec(c Codec) walOpt {
	return func(w *WAL) {
		w.codec = c
	}
}
