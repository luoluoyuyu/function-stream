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
	"sync"

	"github.com/functionstream/function-stream/fs/api"
	"github.com/linxGnu/grocksdb"
)

// RocksDBBackend is the RocksDB state backend implementation
type RocksDBBackend struct {
	mu           sync.RWMutex
	db           *grocksdb.DB
	columnFamily *grocksdb.ColumnFamilyHandle
	cfName       string
	opts         *grocksdb.Options
	ro           *grocksdb.ReadOptions
	wo           *grocksdb.WriteOptions
	basePath     string
}

// RocksDBBackendConfig is the RocksDB backend configuration
type RocksDBBackendConfig struct {
	BasePath             string `json:"base-path" mapstructure:"base-path"`
	MaxOpenFiles         int    `json:"max-open-files" mapstructure:"max-open-files"`
	WriteBufferSize      uint64 `json:"write-buffer-size" mapstructure:"write-buffer-size"`
	MaxWriteBufferNumber int    `json:"max-write-buffer-number" mapstructure:"max-write-buffer-number"`
	TargetFileSizeBase   uint64 `json:"target-file-size-base" mapstructure:"target-file-size-base"`
	MaxBytesForLevelBase uint64 `json:"max-bytes-for-level-base" mapstructure:"max-bytes-for-level-base"`
	Compression          int    `json:"compression" mapstructure:"compression"`
}

// NewRocksDBBackend has been removed: please use NewRocksDBBackendWithDB with shared DB injected by factory

// NewRocksDBBackendWithDB creates a backend using externally injected shared DB (recommended: DB is initialized and managed by factory)
func NewRocksDBBackendWithDB(db *grocksdb.DB, config *RocksDBBackendConfig) (*RocksDBBackend, error) {
	if db == nil {
		return nil, fmt.Errorf("nil rocksdb instance")
	}

	ro := grocksdb.NewDefaultReadOptions()
	wo := grocksdb.NewDefaultWriteOptions()
	wo.SetSync(false)

	backend := &RocksDBBackend{
		db:           db,
		columnFamily: nil,
		cfName:       "",
		opts:         nil,
		ro:           ro,
		wo:           wo,
		basePath:     config.BasePath,
	}
	return backend, nil
}

// NewRocksDBBackendWithDBAndCF creates/binds a column family for this instance using shared DB
func NewRocksDBBackendWithDBAndCF(db *grocksdb.DB, config *RocksDBBackendConfig, cfName string) (*RocksDBBackend, error) {
	if db == nil {
		return nil, fmt.Errorf("nil rocksdb instance")
	}

	ro := grocksdb.NewDefaultReadOptions()
	wo := grocksdb.NewDefaultWriteOptions()
	wo.SetSync(false)

	var cfHandle *grocksdb.ColumnFamilyHandle
	if cfName != "" {
		cfOpts := grocksdb.NewDefaultOptions()
		if config != nil {
			cfOpts.SetWriteBufferSize(config.WriteBufferSize)
			cfOpts.SetMaxWriteBufferNumber(config.MaxWriteBufferNumber)
			cfOpts.SetTargetFileSizeBase(config.TargetFileSizeBase)
			cfOpts.SetMaxBytesForLevelBase(config.MaxBytesForLevelBase)
			cfOpts.SetCompression(grocksdb.CompressionType(config.Compression))
		}
		if h, err := db.CreateColumnFamily(cfOpts, cfName); err == nil {
			cfHandle = h
		} else {
			// Column family already exists or creation failed, fallback to default column family (cfHandle=nil means default)
		}
	}

	backend := &RocksDBBackend{
		db:           db,
		columnFamily: cfHandle,
		opts:         nil,
		ro:           ro,
		wo:           wo,
		basePath:     "",
	}
	if config != nil {
		backend.basePath = config.BasePath
	}
	return backend, nil
}

// Single column family model: no need to dynamically create other column families

// Put stores a key-value pair (by column family name)
func (rb *RocksDBBackend) Put(ctx context.Context, keyGroup, key, namespace, userKey []byte, value []byte) error {
	rb.mu.Lock()
	defer rb.mu.Unlock()

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

// Get retrieves a value (by column family name)
func (rb *RocksDBBackend) Get(ctx context.Context, keyGroup, key, namespace, userKey []byte) ([]byte, error) {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

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

// Delete removes a key (by column family name)
func (rb *RocksDBBackend) Delete(ctx context.Context, keyGroup, key, namespace, userKey []byte) error {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	keyBytes := buildKey(keyGroup, key, namespace, userKey)

	// Check if key exists
	var existingValue *grocksdb.Slice
	var err error
	if rb.columnFamily != nil {
		existingValue, err = rb.db.GetCF(rb.ro, rb.columnFamily, keyBytes)
	} else {
		existingValue, err = rb.db.Get(rb.ro, keyBytes)
	}
	if err != nil {
		return fmt.Errorf("failed to check existing key: %w", err)
	}
	defer existingValue.Free()

	if !existingValue.Exists() {
		return api.ErrNotFound
	}

	// Delete the key
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

// DeleteAll deletes all keys under the specified prefix - uses range deletion (by column family name)
func (rb *RocksDBBackend) DeleteAll(ctx context.Context, keyGroup, key, namespace []byte) error {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	// Create start and end keys for range query
	// Start key: keyGroup + key + namespace + empty UserKey
	startKeyBytes := buildKey(keyGroup, key, namespace, []byte{})
	// End key: keyGroup + key + namespace + max UserKey
	endKeyBytes := buildKey(keyGroup, key, namespace, []byte{0xFF, 0xFF, 0xFF, 0xFF})

	// Use iterator + batch deletion
	var iter *grocksdb.Iterator
	if rb.columnFamily != nil {
		iter = rb.db.NewIteratorCF(rb.ro, rb.columnFamily)
	} else {
		iter = rb.db.NewIterator(rb.ro)
	}
	defer iter.Close()
	iter.Seek(startKeyBytes)
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()
	for iter.Valid() {
		if iter.Key().Compare(endKeyBytes) >= 0 {
			break
		}
		k := make([]byte, len(iter.Key().Data()))
		copy(k, iter.Key().Data())
		if rb.columnFamily != nil {
			batch.DeleteCF(rb.columnFamily, k)
		} else {
			batch.Delete(k)
		}
		iter.Next()
	}
	if err := rb.db.Write(rb.wo, batch); err != nil {
		return fmt.Errorf("failed to batch delete range: %w", err)
	}

	return nil
}

// Merge merges values (by column family name)
func (rb *RocksDBBackend) Merge(ctx context.Context, keyGroup, key, namespace, userKey []byte, value []byte) error {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	keyBytes := buildKey(keyGroup, key, namespace, userKey)

	// Get existing value
	var existingValue *grocksdb.Slice
	var err error
	if rb.columnFamily != nil {
		existingValue, err = rb.db.GetCF(rb.ro, rb.columnFamily, keyBytes)
	} else {
		existingValue, err = rb.db.Get(rb.ro, keyBytes)
	}
	if err != nil {
		return fmt.Errorf("failed to get existing value: %w", err)
	}
	defer existingValue.Free()

	var mergedValue []byte
	if existingValue.Exists() {
		// Simple merge: append new value after existing value
		existing := existingValue.Data()
		mergedValue = make([]byte, len(existing)+len(value)+1)
		copy(mergedValue, existing)
		mergedValue[len(existing)] = '\n'
		copy(mergedValue[len(existing)+1:], value)
	} else {
		mergedValue = value
	}

	// Store merged value
	if rb.columnFamily != nil {
		err = rb.db.PutCF(rb.wo, rb.columnFamily, keyBytes, mergedValue)
	} else {
		err = rb.db.Put(rb.wo, keyBytes, mergedValue)
	}
	if err != nil {
		return fmt.Errorf("failed to put merged value: %w", err)
	}

	return nil
}

// Close closes the state backend
func (rb *RocksDBBackend) Close() error {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	var lastErr error

	// Close column family handle (if exists)
	if rb.columnFamily != nil {
		rb.columnFamily.Destroy()
		rb.columnFamily = nil
	}

	// Read/Write Options are owned by backend instance, destroy them here
	if rb.ro != nil {
		rb.ro.Destroy()
		rb.ro = nil
	}

	if rb.wo != nil {
		rb.wo.Destroy()
		rb.wo = nil
	}

	// DB files are managed by factory, not removed in backend

	return lastErr
}
