// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package metadb

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/hashicorp/raft-wal/types"
	"go.etcd.io/bbolt"
)

const (
	// FileName is the default file name for the bolt db file.
	FileName = "wal-meta.db"

	// *Bucket are the names used for internal bolt buckets
	MetaBucket   = "wal-meta"
	StableBucket = "stable"

	// We just need one key for now so use the byte 'm' for meta arbitrarily.
	MetaKey = "m"
)

var (
	// ErrUnintialized is returned when any call is made before Load has opened
	// the DB file.
	ErrUnintialized = errors.New("uninitialized")
)

// BoltMetaDB implements types.MetaStore using BoltDB as a reliable persistent
// store. See repo README for reasons for this design choice and performance
// implications.
type BoltMetaDB struct {
	dir string
	db  *bbolt.DB
}

func (db *BoltMetaDB) ensureOpen(dir string) error {
	if db.dir != "" && db.dir != dir {
		return fmt.Errorf("can't load dir %s, already open in dir %s", dir, db.dir)
	}
	if db.db != nil {
		return nil
	}

	db.dir = dir

	bb, err := bbolt.Open(filepath.Join(dir, FileName), 0644, nil)
	if err != nil {
		return fmt.Errorf("failed to open %s: %w", FileName, err)
	}
	db.db = bb

	tx, err := db.db.Begin(true)
	defer tx.Rollback()

	if err != nil {
		return err
	}
	_, err = tx.CreateBucketIfNotExists([]byte(MetaBucket))
	if err != nil {
		return err
	}
	_, err = tx.CreateBucketIfNotExists([]byte(StableBucket))
	if err != nil {
		return err
	}
	return tx.Commit()
}

// Load loads the existing persisted state. If there is no existing state
// implementations are expected to create initialize new storage and return an
// empty state.
func (db *BoltMetaDB) Load(dir string) (types.PersistentState, error) {
	var state types.PersistentState

	if err := db.ensureOpen(dir); err != nil {
		return state, err
	}

	tx, err := db.db.Begin(false)
	if err != nil {
		return state, err
	}
	defer tx.Rollback()
	meta := tx.Bucket([]byte(MetaBucket))

	// We just need one key for now so use the byte 'm' for meta arbitrarily.
	raw := meta.Get([]byte(MetaKey))
	if raw == nil {
		// This is valid it's an "empty" log that will be initialized by the WAL.
		return state, nil
	}

	if err := json.Unmarshal(raw, &state); err != nil {
		return state, fmt.Errorf("%w: failed to parse persisted state: %s", types.ErrCorrupt, err)
	}
	return state, nil
}

// CommitState must atomically replace all persisted metadata in the current
// store with the set provided. It must not return until the data is persisted
// durably and in a crash-safe way otherwise the guarantees of the WAL will be
// compromised. The WAL will only ever call this in a single thread at one
// time and it will never be called concurrently with Load however it may be
// called concurrently with Get/SetStable operations.
func (db *BoltMetaDB) CommitState(state types.PersistentState) error {
	if db.db == nil {
		return ErrUnintialized
	}

	encoded, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to encode persisted state: %w", err)
	}

	tx, err := db.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	meta := tx.Bucket([]byte(MetaBucket))

	if err := meta.Put([]byte(MetaKey), encoded); err != nil {
		return err
	}

	return tx.Commit()
}

// GetStable returns a value from stable store or nil if it doesn't exist. May
// be called concurrently by multiple threads.
func (db *BoltMetaDB) GetStable(key []byte) ([]byte, error) {
	if db.db == nil {
		return nil, ErrUnintialized
	}

	tx, err := db.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	stable := tx.Bucket([]byte(StableBucket))

	val := stable.Get(key)
	if val == nil {
		return nil, nil
	}

	// Need to copy the value since bolt only guarantees the slice is valid until
	// end of txn.
	ret := make([]byte, len(val))
	copy(ret, val)
	return ret, nil
}

// SetStable stores a value from stable store. May be called concurrently with
// GetStable.
func (db *BoltMetaDB) SetStable(key []byte, value []byte) error {
	if db.db == nil {
		return ErrUnintialized
	}

	tx, err := db.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stable := tx.Bucket([]byte(StableBucket))

	if value == nil {
		err = stable.Delete(key)
	} else {
		err = stable.Put(key, value)
	}
	if err != nil {
		return err
	}

	return tx.Commit()
}

// Close implements io.Closer
func (db *BoltMetaDB) Close() error {
	if db.db == nil {
		return nil
	}
	err := db.db.Close()
	db.db = nil
	return err
}
