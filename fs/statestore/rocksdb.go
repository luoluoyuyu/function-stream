//go:build rocksdb

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
	"path/filepath"
	"strings"

	"github.com/functionstream/function-stream/common/config"
	"github.com/functionstream/function-stream/common/model"

	"github.com/functionstream/function-stream/fs/api"
	"github.com/linxGnu/grocksdb"
)

type RocksDBStateStoreFactory struct {
	db *grocksdb.DB
	ro *grocksdb.ReadOptions
	wo *grocksdb.WriteOptions
}

type RocksDBStateStoreFactoryConfig struct {
	DirName              string `json:"dir_name" validate:"required"`
	MaxOpenFiles         int    `json:"max-open-files" mapstructure:"max-open-files"`
	WriteBufferSize      uint64 `json:"write-buffer-size" mapstructure:"write-buffer-size"`
	MaxWriteBufferNumber int    `json:"max-write-buffer-number" mapstructure:"max-write-buffer-number"`
	TargetFileSizeBase   uint64 `json:"target-file-size-base" mapstructure:"target-file-size-base"`
	MaxBytesForLevelBase uint64 `json:"max-bytes-for-level-base" mapstructure:"max-bytes-for-level-base"`
	Compression          string `json:"compression" mapstructure:"compression"` // Compression type: "none", "snappy", "zlib", "bz2", "lz4", "lz4hc", "xpress", "zstd"
}

type RocksDBStateStoreConfig struct {
	ColumnFamily string `json:"column-family,omitempty" mapstructure:"column-family,omitempty"`
}

func NewRocksDBStateStoreFactory(cfgMap config.ConfigMap) (api.StateStoreFactory, error) {
	c := &RocksDBStateStoreFactoryConfig{}
	err := cfgMap.ToConfigStruct(c)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// Create RocksDB options
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	if c.MaxOpenFiles > 0 {
		opts.SetMaxOpenFiles(c.MaxOpenFiles)
	}
	if c.WriteBufferSize > 0 {
		opts.SetWriteBufferSize(c.WriteBufferSize)
	}
	if c.MaxWriteBufferNumber > 0 {
		opts.SetMaxWriteBufferNumber(c.MaxWriteBufferNumber)
	}
	if c.TargetFileSizeBase > 0 {
		opts.SetTargetFileSizeBase(c.TargetFileSizeBase)
	}
	if c.MaxBytesForLevelBase > 0 {
		opts.SetMaxBytesForLevelBase(c.MaxBytesForLevelBase)
	}

	// Set MergeOperator to enable Merge operations
	opts.SetMergeOperator(&appendOp{})

	// Ensure directory exists
	if err := os.MkdirAll(c.DirName, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	dbPath := filepath.Join(c.DirName, "rocksdb")
	db, err := grocksdb.OpenDb(opts, dbPath)
	if err != nil {
		// Provide helpful error message for compression-related errors
		errMsg := err.Error()
		if strings.Contains(strings.ToLower(errMsg), "compression") && strings.Contains(strings.ToLower(errMsg), "not linked") {
			compressionName := c.Compression
			if compressionName == "" {
				compressionName = "none"
			}
			return nil, fmt.Errorf("failed to open rocksdb: %w. "+
				"Hint: RocksDB was compiled with minimal features (compression disabled). "+
				"Only 'none' compression is available. If compression is needed, please recompile RocksDB.",
				err)
		}
		return nil, fmt.Errorf("failed to open rocksdb: %w", err)
	}

	ro := grocksdb.NewDefaultReadOptions()
	wo := grocksdb.NewDefaultWriteOptions()
	wo.SetSync(false)

	return &RocksDBStateStoreFactory{
		db: db,
		ro: ro,
		wo: wo,
	}, nil
}

func NewDefaultRocksDBStateStoreFactory() (api.StateStoreFactory, error) {
	dir, err := os.MkdirTemp("", "")
	if err != nil {
		return nil, err
	}

	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)

	// Set MergeOperator to enable Merge operations
	opts.SetMergeOperator(&appendOp{})

	db, err := grocksdb.OpenDb(opts, dir)
	if err != nil {
		return nil, err
	}

	ro := grocksdb.NewDefaultReadOptions()
	wo := grocksdb.NewDefaultWriteOptions()
	wo.SetSync(false)

	return &RocksDBStateStoreFactory{
		db: db,
		ro: ro,
		wo: wo,
	}, nil
}

func (fact *RocksDBStateStoreFactory) NewStateStore(f *model.Function) (api.StateStore, error) {
	if f == nil {
		return &RocksDBStateStore{
			db:           fact.db,
			ro:           fact.ro,
			wo:           fact.wo,
			columnFamily: nil,
			cfName:       "",
		}, nil
	}

	c := &RocksDBStateStoreConfig{}
	err := f.State.ToConfigStruct(c)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	var cfName string
	if c.ColumnFamily != "" {
		cfName = c.ColumnFamily
	} else if f.Name != "" {
		// If ColumnFamily is not set, use function name as default
		cfName = f.Name
	}

	// Get or create column family handle
	// Each StateStore manages its own column family handle
	var cfHandle *grocksdb.ColumnFamilyHandle
	if cfName != "" {
		// Create column family with default options
		// Note: If column family already exists, CreateColumnFamily will return an error
		// In that case, we can't get the handle without reopening the DB with all column families
		cfOpts := grocksdb.NewDefaultOptions()
		handle, err := fact.db.CreateColumnFamily(cfOpts, cfName)
		if err != nil {
			// Column family might already exist
			// We can't get its handle without reopening DB, so use default (nil handle)
			cfHandle = nil
		} else {
			cfHandle = handle
		}
	}

	return &RocksDBStateStore{
		db:           fact.db,
		ro:           fact.ro,
		wo:           fact.wo,
		columnFamily: cfHandle,
		cfName:       cfName,
	}, nil
}

func (fact *RocksDBStateStoreFactory) Close() error {
	// Destroy read/write options
	if fact.ro != nil {
		fact.ro.Destroy()
		fact.ro = nil
	}
	if fact.wo != nil {
		fact.wo.Destroy()
		fact.wo = nil
	}

	// Close the database
	// Note: Column family handles are managed by individual StateStore instances
	// They should be destroyed before the factory is closed
	if fact.db != nil {
		fact.db.Close()
		fact.db = nil
	}

	return nil
}

type RocksDBStateStore struct {
	db           *grocksdb.DB
	ro           *grocksdb.ReadOptions
	wo           *grocksdb.WriteOptions
	columnFamily *grocksdb.ColumnFamilyHandle
	cfName       string
}

// getKey is no longer needed as we use column family for isolation

func (s *RocksDBStateStore) PutState(ctx context.Context, key string, value []byte) error {
	keyBytes := []byte(key)
	var err error
	if s.columnFamily != nil {
		err = s.db.PutCF(s.wo, s.columnFamily, keyBytes, value)
	} else {
		err = s.db.Put(s.wo, keyBytes, value)
	}
	if err != nil {
		return fmt.Errorf("failed to put state: %w", err)
	}
	return nil
}

func (s *RocksDBStateStore) GetState(ctx context.Context, key string) ([]byte, error) {
	keyBytes := []byte(key)
	var value *grocksdb.Slice
	var err error
	if s.columnFamily != nil {
		value, err = s.db.GetCF(s.ro, s.columnFamily, keyBytes)
	} else {
		value, err = s.db.Get(s.ro, keyBytes)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get state: %w", err)
	}
	defer value.Free()

	if !value.Exists() {
		return nil, api.ErrNotFound
	}

	// Return a copy of the data
	result := make([]byte, len(value.Data()))
	copy(result, value.Data())
	return result, nil
}

func (s *RocksDBStateStore) ListStates(
	ctx context.Context, startInclusive string, endExclusive string) ([]string, error) {
	startKey := []byte(startInclusive)
	endKey := []byte(endExclusive)

	var iter *grocksdb.Iterator
	if s.columnFamily != nil {
		iter = s.db.NewIteratorCF(s.ro, s.columnFamily)
	} else {
		iter = s.db.NewIterator(s.ro)
	}
	defer iter.Close()

	var keys []string
	seenKeys := make(map[string]bool)
	iter.Seek(startKey)
	for iter.Valid() {
		keyBytes := iter.Key()
		keyData := keyBytes.Data()

		// Check if we've exceeded the end key
		if len(endKey) > 0 {
			if bytes.Compare(keyData, endKey) >= 0 {
				break
			}
		}

		keyStr := string(keyData)
		if !seenKeys[keyStr] {
			if (startInclusive == "" || keyStr >= startInclusive) && (endExclusive == "" || keyStr < endExclusive) {
				keys = append(keys, keyStr)
				seenKeys[keyStr] = true
			}
		}

		iter.Next()
	}

	return keys, nil
}

func (s *RocksDBStateStore) DeleteState(ctx context.Context, key string) error {
	keyBytes := []byte(key)
	var err error
	if s.columnFamily != nil {
		err = s.db.DeleteCF(s.wo, s.columnFamily, keyBytes)
	} else {
		err = s.db.Delete(s.wo, keyBytes)
	}
	if err != nil {
		return fmt.Errorf("failed to delete state: %w", err)
	}
	return nil
}

// Put stores a key-value pair
func (rb *RocksDBStateStore) Put(ctx context.Context, keyGroup, key, namespace, userKey []byte, value []byte) error {
	keyBytes := buildKey(keyGroup, key, namespace, userKey)

	// Directly store value without checking if key exists
	var err error
	if rb.columnFamily != nil {
		err = rb.db.PutCF(rb.wo, rb.columnFamily, keyBytes, value)
	} else {
		err = rb.db.Put(rb.wo, keyBytes, value)
	}
	if err != nil {
		return fmt.Errorf("failed to put key: %w", err)
	}

	return nil
}

// Get retrieves a value
func (rb *RocksDBStateStore) Get(ctx context.Context, keyGroup, key, namespace, userKey []byte) ([]byte, error) {
	keyBytes := buildKey(keyGroup, key, namespace, userKey)

	var value *grocksdb.Slice
	var err error
	if rb.columnFamily != nil {
		value, err = rb.db.GetCF(rb.ro, rb.columnFamily, keyBytes)
	} else {
		value, err = rb.db.Get(rb.ro, keyBytes)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get key: %w", err)
	}
	defer value.Free()

	if !value.Exists() {
		return nil, api.ErrNotFound
	}

	// Return a copy of the data
	result := make([]byte, len(value.Data()))
	copy(result, value.Data())
	return result, nil
}

// Delete removes a key
func (rb *RocksDBStateStore) Delete(ctx context.Context, keyGroup, key, namespace, userKey []byte) error {
	keyBytes := buildKey(keyGroup, key, namespace, userKey)

	// Delete the key
	var err error
	if rb.columnFamily != nil {
		err = rb.db.DeleteCF(rb.wo, rb.columnFamily, keyBytes)
	} else {
		err = rb.db.Delete(rb.wo, keyBytes)
	}
	if err != nil {
		return fmt.Errorf("failed to delete key: %w", err)
	}

	return nil
}

// DeleteAll deletes all keys under the specified prefix - uses range deletion
//
// Prefix matching logic:
//   - Start key: buildKey(keyGroup, key, namespace, [])
//     This is the exact prefix we want to delete
//   - End key: incrementKey(startKey)
//     This creates the smallest key that is greater than any key starting with the prefix
//
// How it works:
//
//	In lexicographic byte order:
//	  - Any key starting with prefix P: [P] < [P, ...] < incrementKey(P)
//	  - RocksDB iterator with range [start, end) returns all keys where start <= key < end
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
func (rb *RocksDBStateStore) DeleteAll(ctx context.Context, keyGroup, key, namespace []byte) error {
	// Create start key: buildKey(keyGroup, key, namespace, [])
	// This is the exact prefix we want to delete
	startKeyBytes := buildKey(keyGroup, key, namespace, []byte{})

	if len(startKeyBytes) == 0 {
		return fmt.Errorf("empty key prefix")
	}

	// Check if prefix is all 0xFF - this is a special case where range deletion
	// would miss some keys, so we use iterator-based deletion instead
	if isAll0xFF(startKeyBytes) {
		// Use iterator-based deletion for all-0xFF prefix
		// Note: We cannot use an upper bound here because all-0xFF prefix can extend
		// to arbitrary length. We must iterate and check prefix manually.
		var iter *grocksdb.Iterator
		if rb.columnFamily != nil {
			iter = rb.db.NewIteratorCF(rb.ro, rb.columnFamily)
		} else {
			iter = rb.db.NewIterator(rb.ro)
		}
		defer iter.Close()

		batch := grocksdb.NewWriteBatch()
		defer batch.Destroy()

		// Optimized: batch deletions and commit once
		iter.Seek(startKeyBytes)
		for iter.Valid() {
			keyBytes := iter.Key()
			keyData := keyBytes.Data()

			// Check if key still starts with prefix
			if !bytes.HasPrefix(keyData, startKeyBytes) {
				break
			}

			// Copy key data since iterator key is only valid until Next()
			k := make([]byte, len(keyData))
			copy(k, keyData)
			if rb.columnFamily != nil {
				batch.DeleteCF(rb.columnFamily, k)
			} else {
				batch.Delete(k)
			}
			iter.Next()
		}

		if err := rb.db.Write(rb.wo, batch); err != nil {
			return fmt.Errorf("failed to batch delete: %w", err)
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
	// 4. Engine optimization: Range deletion can use engine-level optimizations without reading all keys
	// Note: If we already iterated all keys, we should delete them directly (as in all-0xFF case)
	endKeyBytes := incrementKey(startKeyBytes)

	// Use RocksDB's range deletion feature via WriteBatch
	// DeleteRange creates a range tombstone, which is much more efficient than
	// iterating and deleting individual keys. It uses O(1) write operations
	// instead of O(n) where n is the number of keys to delete.
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	if rb.columnFamily != nil {
		// Use DeleteRangeCF for column family
		batch.DeleteRangeCF(rb.columnFamily, startKeyBytes, endKeyBytes)
	} else {
		// Use DeleteRange for default column family
		batch.DeleteRange(startKeyBytes, endKeyBytes)
	}

	// Write the batch to apply the range deletion
	if err := rb.db.Write(rb.wo, batch); err != nil {
		return fmt.Errorf("failed to delete range: %w", err)
	}

	return nil
}

// Merge merges values using RocksDB's native Merge operation
// Requires a MergeOperator to be configured when opening the database.
// We use StringAppendOperator which, when merging, will replace the value (behaves like Put).
func (rb *RocksDBStateStore) Merge(ctx context.Context, keyGroup, key, namespace, userKey []byte, value []byte) error {
	keyBytes := buildKey(keyGroup, key, namespace, userKey)

	// Use RocksDB's native Merge operation
	// The MergeOperator (StringAppendOperator) will handle the merge
	var err error
	if rb.columnFamily != nil {
		err = rb.db.MergeCF(rb.wo, rb.columnFamily, keyBytes, value)
	} else {
		err = rb.db.Merge(rb.wo, keyBytes, value)
	}
	if err != nil {
		return fmt.Errorf("failed to merge value: %w", err)
	}

	return nil
}

func (s *RocksDBStateStore) Close() error {
	// Destroy column family handle if we own one
	// ReadOptions and WriteOptions are owned by factory, don't destroy them here
	// DB is also owned by factory
	if s.columnFamily != nil {
		s.columnFamily.Destroy()
		s.columnFamily = nil
	}
	return nil
}

type appendOp struct{}

func (a *appendOp) FullMerge(key, existingValue []byte, operands [][]byte) (
	[]byte, bool) {
	var buf bytes.Buffer
	for _, op := range operands {
		buf.Write(op)
	}
	if existingValue != nil {
		buf.Write(existingValue)
	}
	return buf.Bytes(), true
}

func (a *appendOp) PartialMerge(key, leftOperand, rightOperand []byte) (
	[]byte, bool) {
	var buf bytes.Buffer
	buf.Write(rightOperand)
	buf.Write(leftOperand)
	return buf.Bytes(), true
}

func (a *appendOp) Name() string { return "appendOp" }
