// Copyright IBM Corp. 2020, 2025
// SPDX-License-Identifier: MPL-2.0

package integration

import (
	"sync"

	"github.com/hashicorp/raft-wal/types"
)

type PeekingMetaStore struct {
	mu     sync.Mutex
	meta   types.MetaStore
	state  types.PersistentState
	stable map[string]string
}

func (s *PeekingMetaStore) PeekState() types.PersistentState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

func (s *PeekingMetaStore) PeekStable(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.stable[key]
	return v, ok
}

func (s *PeekingMetaStore) Load(dir string) (types.PersistentState, error) {
	state, err := s.meta.Load(dir)
	if err == nil {
		s.mu.Lock()
		s.state = state
		s.mu.Unlock()
	}
	return state, err
}

func (s *PeekingMetaStore) CommitState(state types.PersistentState) error {
	err := s.meta.CommitState(state)
	if err == nil {
		s.mu.Lock()
		s.state = state
		s.mu.Unlock()
	}
	return nil
}

func (s *PeekingMetaStore) GetStable(key []byte) ([]byte, error) {
	return s.meta.GetStable(key)
}

func (s *PeekingMetaStore) SetStable(key, value []byte) error {
	err := s.meta.SetStable(key, value)
	if err == nil {
		s.mu.Lock()
		s.stable[string(key)] = string(value)
		s.mu.Unlock()
	}
	return err
}

func (s *PeekingMetaStore) Close() error {
	return s.meta.Close()
}
