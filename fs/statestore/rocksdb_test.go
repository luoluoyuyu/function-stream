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

package statestore_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/functionstream/function-stream/common/config"
	"github.com/functionstream/function-stream/common/model"
	"github.com/functionstream/function-stream/fs/api"
	"github.com/functionstream/function-stream/fs/statestore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupRocksDBTest(t *testing.T) (api.StateStoreFactory, func()) {
	dir, err := os.MkdirTemp("", "rocksdb_test_*")
	require.NoError(t, err)

	cfgMap := config.ConfigMap{
		"dir_name": dir,
	}

	factory, err := statestore.NewRocksDBStateStoreFactory(cfgMap)
	require.NoError(t, err)

	cleanup := func() {
		// Close factory first - this will close the database
		if err := factory.Close(); err != nil {
			t.Logf("Error closing factory: %v", err)
		}

		// On Windows, RocksDB files may need a moment to be fully released
		// Attempt to remove with retry
		var removeErr error
		for i := 0; i < 3; i++ {
			removeErr = os.RemoveAll(dir)
			if removeErr == nil {
				return
			}
			// Small delay before retry (especially helpful on Windows)
			if i < 2 {
				time.Sleep(100 * time.Millisecond)
			}
		}

		// Log error but don't fail the test - temp dirs will be cleaned up eventually
		if removeErr != nil {
			t.Logf("Warning: Failed to remove test directory %s after 3 attempts: %v. It may be cleaned up later.", dir, removeErr)
		}
	}

	return factory, cleanup
}

func TestRocksDBStateStore_BasicOperations(t *testing.T) {
	factory, cleanup := setupRocksDBTest(t)
	defer cleanup()

	ctx := context.Background()
	store, err := factory.NewStateStore(nil)
	require.NoError(t, err)
	defer store.Close()

	// Test PutState and GetState
	err = store.PutState(ctx, "key1", []byte("value1"))
	assert.NoError(t, err)

	value, err := store.GetState(ctx, "key1")
	assert.NoError(t, err)
	assert.Equal(t, []byte("value1"), value)

	// Test GetState for non-existent key
	_, err = store.GetState(ctx, "nonexistent")
	assert.ErrorIs(t, err, api.ErrNotFound)

	// Test update
	err = store.PutState(ctx, "key1", []byte("value1_updated"))
	assert.NoError(t, err)

	value, err = store.GetState(ctx, "key1")
	assert.NoError(t, err)
	assert.Equal(t, []byte("value1_updated"), value)

	// Test DeleteState
	err = store.DeleteState(ctx, "key1")
	assert.NoError(t, err)

	_, err = store.GetState(ctx, "key1")
	assert.ErrorIs(t, err, api.ErrNotFound)
}

func TestRocksDBStateStore_ListStates(t *testing.T) {
	factory, cleanup := setupRocksDBTest(t)
	defer cleanup()

	ctx := context.Background()
	store, err := factory.NewStateStore(nil)
	require.NoError(t, err)
	defer store.Close()

	// Insert multiple keys
	keys := []string{"a", "b", "c", "d", "e"}
	for _, key := range keys {
		err := store.PutState(ctx, key, []byte("value"))
		require.NoError(t, err)
	}

	// Test ListStates with range
	list, err := store.ListStates(ctx, "b", "d")
	assert.NoError(t, err)
	assert.Contains(t, list, "b")
	assert.Contains(t, list, "c")
	assert.NotContains(t, list, "a")
	assert.NotContains(t, list, "d")
	assert.NotContains(t, list, "e")

	// Test ListStates without range
	list, err = store.ListStates(ctx, "", "")
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, len(list), len(keys))
}

func TestRocksDBStateStore_NewKeyOperations(t *testing.T) {
	factory, cleanup := setupRocksDBTest(t)
	defer cleanup()

	ctx := context.Background()
	store, err := factory.NewStateStore(nil)
	require.NoError(t, err)
	defer store.Close()

	keyGroup := []byte{0x01, 0x02}
	key := []byte{0x03, 0x04}
	namespace := []byte{0x05, 0x06}
	userKey := []byte{0x07, 0x08}
	value := []byte("test_value")

	// Test Put
	err = store.Put(ctx, keyGroup, key, namespace, userKey, value)
	assert.NoError(t, err)

	// Test Get
	retrieved, err := store.Get(ctx, keyGroup, key, namespace, userKey)
	assert.NoError(t, err)
	assert.Equal(t, value, retrieved)

	// Test Get for non-existent key
	_, err = store.Get(ctx, keyGroup, key, namespace, []byte{0x99})
	assert.ErrorIs(t, err, api.ErrNotFound)

	// Test Delete
	err = store.Delete(ctx, keyGroup, key, namespace, userKey)
	assert.NoError(t, err)

	_, err = store.Get(ctx, keyGroup, key, namespace, userKey)
	assert.ErrorIs(t, err, api.ErrNotFound)
}

func TestRocksDBStateStore_DeleteAll(t *testing.T) {
	factory, cleanup := setupRocksDBTest(t)
	defer cleanup()

	ctx := context.Background()
	store, err := factory.NewStateStore(nil)
	require.NoError(t, err)
	defer store.Close()

	keyGroup := []byte{0x01}
	key := []byte{0x02}
	namespace := []byte{0x03}

	// Insert multiple keys with same prefix
	for i := byte(0); i < 10; i++ {
		userKey := []byte{i}
		value := []byte{0x10 + i}
		err := store.Put(ctx, keyGroup, key, namespace, userKey, value)
		require.NoError(t, err)
	}

	// Verify all keys exist
	for i := byte(0); i < 10; i++ {
		userKey := []byte{i}
		value, err := store.Get(ctx, keyGroup, key, namespace, userKey)
		assert.NoError(t, err)
		assert.Equal(t, []byte{0x10 + i}, value)
	}

	// Test DeleteAll
	err = store.DeleteAll(ctx, keyGroup, key, namespace)
	assert.NoError(t, err)

	// Verify all keys are deleted
	for i := byte(0); i < 10; i++ {
		userKey := []byte{i}
		_, err := store.Get(ctx, keyGroup, key, namespace, userKey)
		assert.ErrorIs(t, err, api.ErrNotFound)
	}

	// Verify keys with different prefix are not deleted
	otherKeyGroup := []byte{0x99}
	err = store.Put(ctx, otherKeyGroup, key, namespace, []byte{0x01}, []byte("other"))
	require.NoError(t, err)

	err = store.DeleteAll(ctx, keyGroup, key, namespace)
	assert.NoError(t, err)

	value, err := store.Get(ctx, otherKeyGroup, key, namespace, []byte{0x01})
	assert.NoError(t, err)
	assert.Equal(t, []byte("other"), value)
}

func TestRocksDBStateStore_DeleteAll_All0xFF(t *testing.T) {
	factory, cleanup := setupRocksDBTest(t)
	defer cleanup()

	ctx := context.Background()
	store, err := factory.NewStateStore(nil)
	require.NoError(t, err)
	defer store.Close()

	// Create prefix that is all 0xFF
	keyGroup := []byte{0xFF, 0xFF}
	key := []byte{0xFF}
	namespace := []byte{}

	// Insert multiple keys
	for i := byte(0); i < 5; i++ {
		userKey := []byte{i}
		value := []byte{0x10 + i}
		err := store.Put(ctx, keyGroup, key, namespace, userKey, value)
		require.NoError(t, err)
	}

	// Test DeleteAll with all-0xFF prefix
	err = store.DeleteAll(ctx, keyGroup, key, namespace)
	assert.NoError(t, err)

	// Verify all keys are deleted
	for i := byte(0); i < 5; i++ {
		userKey := []byte{i}
		_, err := store.Get(ctx, keyGroup, key, namespace, userKey)
		assert.ErrorIs(t, err, api.ErrNotFound)
	}
}

func TestRocksDBStateStore_Merge(t *testing.T) {
	factory, cleanup := setupRocksDBTest(t)
	defer cleanup()

	ctx := context.Background()
	store, err := factory.NewStateStore(nil)
	require.NoError(t, err)
	defer store.Close()

	keyGroup := []byte{0x01}
	key := []byte{0x02}
	namespace := []byte{0x03}
	userKey := []byte{0x04}

	// Test Merge (without MergeOperator, this should behave like Put)
	value1 := []byte("value1")
	err = store.Merge(ctx, keyGroup, key, namespace, userKey, value1)
	assert.NoError(t, err)

	retrieved, err := store.Get(ctx, keyGroup, key, namespace, userKey)
	assert.NoError(t, err)
	// Without MergeOperator, Merge behaves like Put
	assert.Equal(t, value1, retrieved)

	// Test Merge again (should replace)
	value2 := []byte("value2")
	err = store.Merge(ctx, keyGroup, key, namespace, userKey, value2)
	assert.NoError(t, err)

	retrieved, err = store.Get(ctx, keyGroup, key, namespace, userKey)
	assert.NoError(t, err)
	assert.Equal(t, append(value1, value2...), retrieved)
}

func TestRocksDBStateStore_ColumnFamilyIsolation(t *testing.T) {
	factory, cleanup := setupRocksDBTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create two stores with different column families
	func1 := &model.Function{
		Name:  "func1",
		State: config.ConfigMap{"column-family": "cf1"},
	}
	store1, err := factory.NewStateStore(func1)
	require.NoError(t, err)
	defer store1.Close()

	func2 := &model.Function{
		Name:  "func2",
		State: config.ConfigMap{"column-family": "cf2"},
	}
	store2, err := factory.NewStateStore(func2)
	require.NoError(t, err)
	defer store2.Close()

	// Store same key in both column families
	key := "same_key"
	value1 := []byte("value1")
	value2 := []byte("value2")

	err = store1.PutState(ctx, key, value1)
	assert.NoError(t, err)

	err = store2.PutState(ctx, key, value2)
	assert.NoError(t, err)

	// Verify isolation
	retrieved1, err := store1.GetState(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, value1, retrieved1)

	retrieved2, err := store2.GetState(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, value2, retrieved2)
}

func TestRocksDBStateStore_EmptyValues(t *testing.T) {
	factory, cleanup := setupRocksDBTest(t)
	defer cleanup()

	ctx := context.Background()
	store, err := factory.NewStateStore(nil)
	require.NoError(t, err)
	defer store.Close()

	// Test empty value
	err = store.PutState(ctx, "key", []byte{})
	assert.NoError(t, err)

	value, err := store.GetState(ctx, "key")
	assert.NoError(t, err)
	assert.Equal(t, []byte{}, value)

	// Test empty key parts
	err = store.Put(ctx, []byte{}, []byte{}, []byte{}, []byte{}, []byte("value"))
	assert.NoError(t, err)

	retrieved, err := store.Get(ctx, []byte{}, []byte{}, []byte{}, []byte{})
	assert.NoError(t, err)
	assert.Equal(t, []byte("value"), retrieved)
}

func TestRocksDBStateStoreFactory_Config(t *testing.T) {
	dir, err := os.MkdirTemp("", "rocksdb_config_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	cfgMap := config.ConfigMap{
		"dir_name":                 dir,
		"max-open-files":           1000,
		"write-buffer-size":        uint64(1024 * 1024),
		"max-write-buffer-number":  4,
		"target-file-size-base":    uint64(64 * 1024 * 1024),
		"max-bytes-for-level-base": uint64(256 * 1024 * 1024),
		"compression":              "snappy",
	}

	factory, err := statestore.NewRocksDBStateStoreFactory(cfgMap)
	if !assert.NoError(t, err) {
		return // If creation fails, return directly to avoid nil pointer
	}
	assert.NotNil(t, factory)
	defer factory.Close()

	// Verify factory creates stores correctly
	store, err := factory.NewStateStore(nil)
	assert.NoError(t, err)
	assert.NotNil(t, store)

	err = store.Close()
	assert.NoError(t, err)
}

func TestRocksDBStateStoreFactory_WithFunction(t *testing.T) {
	factory, cleanup := setupRocksDBTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create function with column family config
	func1 := &model.Function{
		Name:  "test_func",
		State: config.ConfigMap{"column-family": "test_cf"},
	}

	store, err := factory.NewStateStore(func1)
	require.NoError(t, err)
	defer store.Close()

	// Test operations
	err = store.PutState(ctx, "key", []byte("value"))
	assert.NoError(t, err)

	value, err := store.GetState(ctx, "key")
	assert.NoError(t, err)
	assert.Equal(t, []byte("value"), value)
}

func TestRocksDBStateStoreFactory_DefaultColumnFamily(t *testing.T) {
	factory, cleanup := setupRocksDBTest(t)
	defer cleanup()

	// Create function without column family config, should use function name
	func1 := &model.Function{
		Name:  "default_func",
		State: config.ConfigMap{},
	}

	store, err := factory.NewStateStore(func1)
	require.NoError(t, err)
	defer store.Close()

	ctx := context.Background()
	err = store.PutState(ctx, "key", []byte("value"))
	assert.NoError(t, err)

	value, err := store.GetState(ctx, "key")
	assert.NoError(t, err)
	assert.Equal(t, []byte("value"), value)
}

func TestRocksDBStateStoreFactory_NilFunction(t *testing.T) {
	factory, cleanup := setupRocksDBTest(t)
	defer cleanup()

	store, err := factory.NewStateStore(nil)
	require.NoError(t, err)
	require.NotNil(t, store)
	defer store.Close()

	ctx := context.Background()
	err = store.PutState(ctx, "key", []byte("value"))
	assert.NoError(t, err)

	value, err := store.GetState(ctx, "key")
	assert.NoError(t, err)
	assert.Equal(t, []byte("value"), value)
}

func TestRocksDBStateStore_Close(t *testing.T) {
	factory, cleanup := setupRocksDBTest(t)
	defer cleanup()

	func1 := &model.Function{
		Name:  "func1",
		State: config.ConfigMap{"column-family": "cf1"},
	}

	store, err := factory.NewStateStore(func1)
	require.NoError(t, err)

	ctx := context.Background()
	err = store.PutState(ctx, "key", []byte("value"))
	assert.NoError(t, err)

	// Close should not error
	err = store.Close()
	assert.NoError(t, err)

	// Operations after close should fail or behave unpredictably
	// (depends on implementation, but close should succeed)
}
