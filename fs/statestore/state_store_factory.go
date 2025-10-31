/*
 * Copyright 2024 Function Stream Org.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package statestore

import (
	"context"
	"fmt"

	"github.com/functionstream/function-stream/common/config"
	"github.com/functionstream/function-stream/common/model"
	"github.com/functionstream/function-stream/fs/api"
)

// StateStoreFactory implements api.StateStoreFactory interface
// Based on the new StateBackend system, supports Pebble and RocksDB
type StateStoreFactory struct {
	backend StateBackend
	cfName  string // Column family name (RocksDB) or prefix (Pebble)
}

// StateStoreFactoryConfig is the factory configuration
type StateStoreFactoryConfig struct {
	Type   string           `json:"type" mapstructure:"type"`
	Config config.ConfigMap `json:"config" mapstructure:"config"`
	CFName string           `json:"cf-name" mapstructure:"cf-name"`
}

// NewStateStoreFactory creates a new state store factory
func NewStateStoreFactory(cfgMap config.ConfigMap) (api.StateStoreFactory, error) {
	cfg := &StateStoreFactoryConfig{}
	if err := cfgMap.ToConfigStruct(cfg); err != nil {
		return nil, fmt.Errorf("failed to parse factory config: %w", err)
	}

	// Convert to internal configuration
	stateConfig, err := NewConfigFromConfigMap(cfg.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to create state config: %w", err)
	}

	// If factory config specifies type, override internal config
	if cfg.Type != "" {
		stateConfig.Type = BackendType(cfg.Type)
	}

	// Create backend
	backend, err := NewStateBackendFromConfig(stateConfig, cfg.CFName)
	if err != nil {
		return nil, fmt.Errorf("failed to create state backend: %w", err)
	}

	return &StateStoreFactory{
		backend: backend,
		cfName:  cfg.CFName,
	}, nil
}

// NewDefaultStateStoreFactory creates default state store factory (uses Pebble)
func NewDefaultStateStoreFactory() (api.StateStoreFactory, error) {
	cfg := DefaultConfig()
	backend, err := NewStateBackendFromConfig(cfg, "")
	if err != nil {
		return nil, fmt.Errorf("failed to create default state backend: %w", err)
	}

	return &StateStoreFactory{
		backend: backend,
		cfName:  "",
	}, nil
}

// NewStateStore creates a state store instance for the function
func (f *StateStoreFactory) NewStateStore(function *model.Function) (api.StateStore, error) {
	// Use function name as column family/prefix name
	cfName := ""
	if function != nil && function.Name != "" {
		cfName = function.Name
	}

	// If current factory's column family doesn't match function, create new backend
	if f.cfName != cfName {
		// Get current configuration
		cfg := DefaultConfig() // Simplified: use default config
		backend, err := NewStateBackendFromConfig(cfg, cfName)
		if err != nil {
			return nil, fmt.Errorf("failed to create function-specific backend: %w", err)
		}
		return &StateStore{backend: backend}, nil
	}

	return &StateStore{backend: f.backend}, nil
}

// Close closes the factory
func (f *StateStoreFactory) Close() error {
	return f.backend.Close()
}

// StateStore implements api.StateStore interface
// Converts api.StateStore's string key to StateBackend's 4 byte[] keys
type StateStore struct {
	backend StateBackend
}

// PutState stores state
func (s *StateStore) PutState(ctx context.Context, key string, value []byte) error {
	// Convert string key to 4 byte[] keys
	// Simplified handling: keyGroup=empty, key=key, namespace=empty, userKey=empty
	keyBytes := []byte(key)
	return s.backend.Put(ctx, []byte{}, keyBytes, []byte{}, []byte{}, value)
}

// GetState retrieves state
func (s *StateStore) GetState(ctx context.Context, key string) ([]byte, error) {
	keyBytes := []byte(key)
	return s.backend.Get(ctx, []byte{}, keyBytes, []byte{}, []byte{})
}

// ListStates lists state keys
func (s *StateStore) ListStates(ctx context.Context, startInclusive, endExclusive string) ([]string, error) {
	// StateBackend doesn't have ListStates method, simplified implementation here
	// In real applications, may need to add Scan method to StateBackend interface
	return nil, fmt.Errorf("ListStates not implemented in StateBackend")
}

// DeleteState deletes state
func (s *StateStore) DeleteState(ctx context.Context, key string) error {
	keyBytes := []byte(key)
	return s.backend.Delete(ctx, []byte{}, keyBytes, []byte{}, []byte{})
}

// Close closes the state store
func (s *StateStore) Close() error {
	return s.backend.Close()
}
