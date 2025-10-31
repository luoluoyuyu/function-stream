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

	"github.com/cockroachdb/pebble"
	"github.com/functionstream/function-stream/fs/api"
)

// PebbleBackend is the Pebble state backend implementation (single column family: uses prefix to simulate column family isolation)
type PebbleBackend struct {
	db       *pebble.DB
	cfPrefix []byte
}

// PebbleBackendConfig is the Pebble backend configuration
type PebbleBackendConfig struct {
	BasePath              string `json:"base-path" mapstructure:"base-path"`
	MaxOpenFiles          int    `json:"max-open-files" mapstructure:"max-open-files"`
	L0CompactionThreshold int    `json:"l0-compaction-threshold" mapstructure:"l0-compaction-threshold"`
	L0StopWritesThreshold int    `json:"l0-stop-writes-threshold" mapstructure:"l0-stop-writes-threshold"`
	LBaseMaxBytes         int64  `json:"l-base-max-bytes" mapstructure:"l-base-max-bytes"`
	TargetFileSize        int64  `json:"target-file-size" mapstructure:"target-file-size"`
}

// NewPebbleBackend has been removed: please use NewPebbleBackendWithDB with shared DB injected by factory

// NewPebbleBackendWithDB creates a backend using externally injected shared DB (recommended: DB is initialized and managed by factory)
func NewPebbleBackendWithDB(db *pebble.DB, config *PebbleBackendConfig) (*PebbleBackend, error) {
	if db == nil {
		return nil, fmt.Errorf("nil pebble instance")
	}
	return &PebbleBackend{db: db, cfPrefix: []byte{}}, nil
}

// NewPebbleBackendWithDBAndPrefix creates backend with externally injected DB and sets column family prefix (logical column family)
func NewPebbleBackendWithDBAndPrefix(db *pebble.DB, prefix []byte) (*PebbleBackend, error) {
	if db == nil {
		return nil, fmt.Errorf("nil pebble instance")
	}
	return &PebbleBackend{db: db, cfPrefix: append([]byte{}, prefix...)}, nil
}

// Put stores a key-value pair
func (pb *PebbleBackend) Put(ctx context.Context, keyGroup, key, namespace, userKey []byte, value []byte) error {
	keyBytes := buildKey(keyGroup, key, namespace, userKey)
	if len(pb.cfPrefix) > 0 {
		sep := []byte{0x00, 0xFF, 0x00, 0xFF}
		k := make([]byte, 0, len(pb.cfPrefix)+len(sep)+len(keyBytes))
		k = append(k, pb.cfPrefix...)
		k = append(k, sep...)
		k = append(k, keyBytes...)
		keyBytes = k
	}

	// Directly store value without checking if key exists
	err := pb.db.Set(keyBytes, value, pebble.Sync)
	if err != nil {
		return fmt.Errorf("failed to put key: %w", err)
	}

	return nil
}

// Get retrieves a value
func (pb *PebbleBackend) Get(ctx context.Context, keyGroup, key, namespace, userKey []byte) ([]byte, error) {
	keyBytes := buildKey(keyGroup, key, namespace, userKey)
	if len(pb.cfPrefix) > 0 {
		sep := []byte{0x00, 0xFF, 0x00, 0xFF}
		k := make([]byte, 0, len(pb.cfPrefix)+len(sep)+len(keyBytes))
		k = append(k, pb.cfPrefix...)
		k = append(k, sep...)
		k = append(k, keyBytes...)
		keyBytes = k
	}
	value, closer, err := pb.db.Get(keyBytes)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, api.ErrNotFound
		}
		return nil, fmt.Errorf("failed to get key: %w", err)
	}
	defer closer.Close()

	// Return a copy of the data
	result := make([]byte, len(value))
	copy(result, value)

	return result, nil
}

// Delete removes a key
func (pb *PebbleBackend) Delete(ctx context.Context, keyGroup, key, namespace, userKey []byte) error {
	keyBytes := buildKey(keyGroup, key, namespace, userKey)
	if len(pb.cfPrefix) > 0 {
		sep := []byte{0x00, 0xFF, 0x00, 0xFF}
		k := make([]byte, 0, len(pb.cfPrefix)+len(sep)+len(keyBytes))
		k = append(k, pb.cfPrefix...)
		k = append(k, sep...)
		k = append(k, keyBytes...)
		keyBytes = k
	}

	// Check if key exists
	existingValue, closer, err := pb.db.Get(keyBytes)
	if err != nil {
		if err == pebble.ErrNotFound {
			return api.ErrNotFound
		}
		return fmt.Errorf("failed to check existing key: %w", err)
	}
	defer closer.Close()

	// 删除键
	err = pb.db.Delete(keyBytes, pebble.Sync)
	if err != nil {
		return fmt.Errorf("failed to delete key: %w", err)
	}

	return nil
}

// DeleteAll deletes all keys under the specified prefix - uses range deletion
func (pb *PebbleBackend) DeleteAll(ctx context.Context, keyGroup, key, namespace []byte) error {
	// Create start and end keys for range query
	// Start key: keyGroup + key + namespace + empty UserKey
	startKeyBytes := buildKey(keyGroup, key, namespace, []byte{})
	// End key: keyGroup + key + namespace + max UserKey
	endKeyBytes := buildKey(keyGroup, key, namespace, []byte{0xFF, 0xFF, 0xFF, 0xFF})
	if len(pb.cfPrefix) > 0 {
		sep := []byte{0x00, 0xFF, 0x00, 0xFF}
		s := make([]byte, 0, len(pb.cfPrefix)+len(sep)+len(startKeyBytes))
		s = append(s, pb.cfPrefix...)
		s = append(s, sep...)
		s = append(s, startKeyBytes...)
		startKeyBytes = s
		e := make([]byte, 0, len(pb.cfPrefix)+len(sep)+len(endKeyBytes))
		e = append(e, pb.cfPrefix...)
		e = append(e, sep...)
		e = append(e, endKeyBytes...)
		endKeyBytes = e
	}

	// Use Pebble's range deletion feature
	// Pebble supports DeleteRange operation, which is the most efficient way
	if err := pb.db.DeleteRange(startKeyBytes, endKeyBytes, pebble.Sync); err != nil {
		return fmt.Errorf("failed to delete range: %w", err)
	}

	return nil
}

// Merge merges values
func (pb *PebbleBackend) Merge(ctx context.Context, keyGroup, key, namespace, userKey []byte, value []byte) error {
	keyBytes := buildKey(keyGroup, key, namespace, userKey)
	if len(pb.cfPrefix) > 0 {
		sep := []byte{0x00, 0xFF, 0x00, 0xFF}
		k := make([]byte, 0, len(pb.cfPrefix)+len(sep)+len(keyBytes))
		k = append(k, pb.cfPrefix...)
		k = append(k, sep...)
		k = append(k, keyBytes...)
		keyBytes = k
	}

	// Get existing value
	existingValue, closer, err := pb.db.Get(keyBytes)
	if err != nil && err != pebble.ErrNotFound {
		return fmt.Errorf("failed to get existing value: %w", err)
	}

	var mergedValue []byte
	if existingValue != nil {
		// Simple merge: append new value after existing value
		existing := existingValue.Data()
		mergedValue = make([]byte, len(existing)+len(value)+1)
		copy(mergedValue, existing)
		mergedValue[len(existing)] = '\n'
		copy(mergedValue[len(existing)+1:], value)
		closer.Close()
	} else {
		mergedValue = value
	}

	// Store merged value
	err = pb.db.Set(keyBytes, mergedValue, pebble.Sync)
	if err != nil {
		return fmt.Errorf("failed to put merged value: %w", err)
	}

	// no stats

	return nil
}

// Close closes the state backend
func (pb *PebbleBackend) Close() error { return nil }
