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
	"bytes"
	"context"
	"fmt"
	"os"

	"github.com/functionstream/function-stream/common/config"
	"github.com/functionstream/function-stream/common/model"

	"github.com/cockroachdb/pebble"
	"github.com/functionstream/function-stream/fs/api"
	"github.com/pkg/errors"
)

type PebbleStateStoreFactory struct {
	db *pebble.DB
}

type PebbleStateStoreFactoryConfig struct {
	DirName               string `json:"dir_name" validate:"required"`
	MaxOpenFiles          int    `json:"max-open-files" mapstructure:"max-open-files"`
	L0CompactionThreshold int    `json:"l0-compaction-threshold" mapstructure:"l0-compaction-threshold"`
	L0StopWritesThreshold int    `json:"l0-stop-writes-threshold" mapstructure:"l0-stop-writes-threshold"`
	LBaseMaxBytes         int64  `json:"l-base-max-bytes" mapstructure:"l-base-max-bytes"`
}

type PebbleStateStoreConfig struct {
	KeyPrefix string `json:"key_prefix,omitempty"`
}

func NewPebbleStateStoreFactory(config config.ConfigMap) (api.StateStoreFactory, error) {
	c := &PebbleStateStoreFactoryConfig{}
	err := config.ToConfigStruct(c)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// Create Pebble options and apply configuration
	opts := &pebble.Options{}

	if c.MaxOpenFiles > 0 {
		opts.MaxOpenFiles = c.MaxOpenFiles
	}
	if c.L0CompactionThreshold > 0 {
		opts.L0CompactionThreshold = c.L0CompactionThreshold
	}
	if c.L0StopWritesThreshold > 0 {
		opts.L0StopWritesThreshold = c.L0StopWritesThreshold
	}
	if c.LBaseMaxBytes > 0 {
		opts.LBaseMaxBytes = c.LBaseMaxBytes
	}

	// Ensure directory exists
	if err := os.MkdirAll(c.DirName, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	db, err := pebble.Open(c.DirName, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble: %w", err)
	}
	return &PebbleStateStoreFactory{db: db}, nil
}

func NewDefaultPebbleStateStoreFactory() (api.StateStoreFactory, error) {
	dir, err := os.MkdirTemp("", "")
	if err != nil {
		return nil, err
	}
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &PebbleStateStoreFactory{db: db}, nil
}

func (fact *PebbleStateStoreFactory) NewStateStore(f *model.Function) (api.StateStore, error) {
	if f == nil {
		return &PebbleStateStore{
			db:        fact.db,
			keyPrefix: []byte{},
		}, nil
	}
	c := &PebbleStateStoreConfig{}
	err := f.State.ToConfigStruct(c)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}
	var keyPrefix []byte
	if c.KeyPrefix != "" {
		keyPrefix = []byte(c.KeyPrefix)
	} else {
		keyPrefix = []byte(f.Name)
	}
	return &PebbleStateStore{
		db:        fact.db,
		keyPrefix: keyPrefix,
	}, nil
}

func (fact *PebbleStateStoreFactory) Close() error {
	return fact.db.Close()
}

type PebbleStateStore struct {
	db        *pebble.DB
	keyPrefix []byte
}

func (s *PebbleStateStore) getKey(key string) []byte {
	result := make([]byte, 0, len(s.keyPrefix)+len(key))
	result = append(result, s.keyPrefix...)
	result = append(result, []byte(key)...)
	return result
}

func (s *PebbleStateStore) PutState(ctx context.Context, key string, value []byte) error {
	if err := s.db.Set(s.getKey(key), value, pebble.NoSync); err != nil {
		return err
	}
	return nil
}

func (s *PebbleStateStore) GetState(ctx context.Context, key string) ([]byte, error) {
	value, closer, err := s.db.Get(s.getKey(key))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, api.ErrNotFound
		}
		return nil, err
	}
	result := make([]byte, len(value))
	copy(result, value)
	if err := closer.Close(); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *PebbleStateStore) ListStates(
	ctx context.Context, startInclusive string, endExclusive string) ([]string, error) {
	// Build bounds - empty string means no bound
	var lowerBound, upperBound []byte

	if startInclusive != "" {
		lowerBound = s.getKey(startInclusive)
	} else if len(s.keyPrefix) > 0 {
		// Empty startInclusive but has keyPrefix - start from prefix
		lowerBound = s.keyPrefix
	}
	// else: lowerBound is nil, which means start from beginning

	if endExclusive != "" {
		upperBound = s.getKey(endExclusive)
	}
	// else: upperBound is nil, which means no upper bound

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}
	defer func(iter *pebble.Iterator) {
		_ = iter.Close()
	}(iter)

	var keys []string
	seenKeys := make(map[string]bool)

	for iter.First(); iter.Valid(); iter.Next() {
		keyBytes := iter.Key()
		keyStr := string(keyBytes)

		// Remove keyPrefix if present to return the original key name
		var originalKey string
		if len(s.keyPrefix) > 0 && bytes.HasPrefix(keyBytes, s.keyPrefix) {
			// Extract the key after the prefix
			originalKey = keyStr[len(s.keyPrefix):]
		} else {
			originalKey = keyStr
		}

		// Apply range filtering on the original key (not the prefixed key)
		if startInclusive != "" && originalKey < startInclusive {
			continue
		}
		if endExclusive != "" && originalKey >= endExclusive {
			continue
		}

		// Avoid duplicates
		if !seenKeys[originalKey] {
			keys = append(keys, originalKey)
			seenKeys[originalKey] = true
		}
	}

	return keys, nil
}

func (s *PebbleStateStore) DeleteState(ctx context.Context, key string) error {
	if err := s.db.Delete(s.getKey(key), pebble.NoSync); err != nil {
		return err
	}
	return nil
}

// Put stores a key-value pair
func (pb *PebbleStateStore) Put(ctx context.Context, keyGroup, key, namespace, userKey []byte, value []byte) error {
	keyBytes := buildKey(keyGroup, key, namespace, userKey)
	if len(pb.keyPrefix) > 0 {
		result := make([]byte, 0, len(pb.keyPrefix)+len(keyBytes))
		result = append(result, pb.keyPrefix...)
		result = append(result, keyBytes...)
		keyBytes = result
	}

	// Directly store value without checking if key exists
	err := pb.db.Set(keyBytes, value, pebble.Sync)
	if err != nil {
		return fmt.Errorf("failed to put key: %w", err)
	}

	return nil
}

// Get retrieves a value
func (pb *PebbleStateStore) Get(ctx context.Context, keyGroup, key, namespace, userKey []byte) ([]byte, error) {
	keyBytes := buildKey(keyGroup, key, namespace, userKey)
	if len(pb.keyPrefix) > 0 {
		result := make([]byte, 0, len(pb.keyPrefix)+len(keyBytes))
		result = append(result, pb.keyPrefix...)
		result = append(result, keyBytes...)
		keyBytes = result
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
func (pb *PebbleStateStore) Delete(ctx context.Context, keyGroup, key, namespace, userKey []byte) error {
	keyBytes := buildKey(keyGroup, key, namespace, userKey)
	if len(pb.keyPrefix) > 0 {
		result := make([]byte, 0, len(pb.keyPrefix)+len(keyBytes))
		result = append(result, pb.keyPrefix...)
		result = append(result, keyBytes...)
		keyBytes = result
	}

	// Delete the key
	err := pb.db.Delete(keyBytes, pebble.Sync)
	if err != nil {
		return fmt.Errorf("failed to delete key: %w", err)
	}

	return nil
}

// DeleteAll deletes all keys under the specified prefix - uses range deletion
//
// Prefix matching logic:
//   - Start key: keyPrefix + buildKey(keyGroup, key, namespace, [])
//     This is the exact prefix we want to delete
//   - End key: incrementKey(startKey)
//     This creates the smallest key that is greater than any key starting with the prefix
//
// How it works:
//
//	In lexicographic byte order:
//	  - Any key starting with prefix P: [P] < [P, ...] < incrementKey(P)
//	  - Pebble's DeleteRange(start, end) deletes all keys where start <= key < end
//
// Examples:
//
//	Prefix: [0x01, 0x02, 0x03]
//	- startKey = [0x01, 0x02, 0x03]
//	- endKey = incrementKey([0x01, 0x02, 0x03]) = [0x01, 0x02, 0x04]
//	- Deletes: [0x01, 0x02, 0x03], [0x01, 0x02, 0x03, 0x00], [0x01, 0x02, 0x03, 0xFF, ...]
//	- Does NOT delete: [0x01, 0x02, 0x04] (>= endKey)
//
//	Prefix: [0x01, 0x02, 0xFF] (handles overflow correctly)
//	- startKey = [0x01, 0x02, 0xFF]
//	- endKey = incrementKey([0x01, 0x02, 0xFF]) = [0x01, 0x03, 0x00]
//	- Correctly handles 0xFF overflow
func (pb *PebbleStateStore) DeleteAll(ctx context.Context, keyGroup, key, namespace []byte) error {
	// Create start key: keyPrefix + buildKey(keyGroup, key, namespace, [])
	// This is the exact prefix we want to delete
	startKeyBytes := buildKey(keyGroup, key, namespace, []byte{})
	if len(pb.keyPrefix) > 0 {
		result := make([]byte, 0, len(pb.keyPrefix)+len(startKeyBytes))
		result = append(result, pb.keyPrefix...)
		result = append(result, startKeyBytes...)
		startKeyBytes = result
	}

	if len(startKeyBytes) == 0 {
		// Empty key case - shouldn't happen
		return fmt.Errorf("empty key prefix")
	}

	// Check if prefix is all 0xFF - this is a special case where range deletion
	// would miss some keys, so we use iterator-based deletion instead
	if isAll0xFF(startKeyBytes) {
		// Use iterator-based deletion for all-0xFF prefix
		// Note: We cannot use an upper bound here because all-0xFF prefix can extend
		// to arbitrary length. We must iterate and check prefix manually.
		iter, err := pb.db.NewIter(&pebble.IterOptions{
			LowerBound: startKeyBytes,
		})
		if err != nil {
			return fmt.Errorf("failed to create iterator: %w", err)
		}
		defer iter.Close()

		batch := pb.db.NewBatch()
		defer batch.Close()

		// Optimized: batch deletions and commit once
		// Iterate and delete all keys with the prefix
		for iter.First(); iter.Valid(); iter.Next() {
			keyBytes := iter.Key()
			// Check if key still starts with prefix (this is the only way to check
			// since we can't use an upper bound for all-0xFF prefix)
			if !bytes.HasPrefix(keyBytes, startKeyBytes) {
				break
			}
			// Key is owned by iterator, Pebble batch.Delete makes a copy internally
			if err := batch.Delete(keyBytes, nil); err != nil {
				return fmt.Errorf("failed to batch delete: %w", err)
			}
		}

		if err := batch.Commit(pebble.Sync); err != nil {
			return fmt.Errorf("failed to commit batch: %w", err)
		}
		return nil
	}

	// Create end key by incrementing the prefix
	// This creates the smallest key > any key starting with startKeyBytes
	// Example: [0x01, 0x02, 0x03] -> [0x01, 0x02, 0x04]
	// This ensures all keys with prefix [0x01, 0x02, 0x03] are deleted
	//
	// Why incrementKey instead of finding the last actual key?
	// 1. Performance: incrementKey is O(1), finding last key requires O(n) iteration
	// 2. Correctness: incrementKey is mathematically correct (upper bound), independent of actual data
	// 3. Dynamic data: Works correctly even if keys are inserted concurrently
	// 4. Engine optimization: DeleteRange can use engine-level optimizations without reading all keys
	// Note: If we already iterated all keys, we should delete them directly (as in all-0xFF case)
	endKeyBytes := incrementKey(startKeyBytes)

	// Use Pebble's range deletion feature
	// DeleteRange(start, end) deletes all keys where start <= key < end
	if err := pb.db.DeleteRange(startKeyBytes, endKeyBytes, pebble.Sync); err != nil {
		return fmt.Errorf("failed to delete range: %w", err)
	}

	return nil
}

// Merge merges values using Pebble's native Merge operation
// Note: Pebble requires a MergeOperator to be configured when opening the database
// to properly merge values. Without a MergeOperator, Pebble's default behavior is to append.
// To match RocksDB's behavior (where Merge behaves like Put without MergeOperator),
// we implement a fallback: if no existing value, store new value; if exists, replace it (Put behavior).
func (pb *PebbleStateStore) Merge(ctx context.Context, keyGroup, key, namespace, userKey []byte, value []byte) error {
	keyBytes := buildKey(keyGroup, key, namespace, userKey)
	if len(pb.keyPrefix) > 0 {
		result := make([]byte, 0, len(pb.keyPrefix)+len(keyBytes))
		result = append(result, pb.keyPrefix...)
		result = append(result, keyBytes...)
		keyBytes = result
	}

	// Check if a value already exists for this key
	_, closer, err := pb.db.Get(keyBytes)
	if err == pebble.ErrNotFound {
		// No existing value, just store the new value (same as first merge)
		err = pb.db.Set(keyBytes, value, pebble.Sync)
		if err != nil {
			return fmt.Errorf("failed to set value: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to check existing value: %w", err)
	}
	closer.Close()

	// Value exists - without a MergeOperator, Pebble's Merge would append
	// To match expected behavior (like Put/replace), we use Set instead
	// This matches RocksDB's behavior when no MergeOperator is configured
	err = pb.db.Set(keyBytes, value, pebble.Sync)
	if err != nil {
		return fmt.Errorf("failed to merge (replace) value: %w", err)
	}

	return nil
}

func (s *PebbleStateStore) Close() error {
	return nil
}
