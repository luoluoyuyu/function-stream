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
	"fmt"
	"os"
	"testing"

	"github.com/functionstream/function-stream/common/config"
	"github.com/functionstream/function-stream/common/model"
	"github.com/functionstream/function-stream/fs/api"
	"github.com/functionstream/function-stream/fs/statestore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupPebbleTest(t *testing.T) (api.StateStoreFactory, func()) {
	dir, err := os.MkdirTemp("", "pebble_test_*")
	require.NoError(t, err)

	cfgMap := config.ConfigMap{
		"dir_name": dir,
	}

	factory, err := statestore.NewPebbleStateStoreFactory(cfgMap)
	require.NoError(t, err)

	cleanup := func() {
		err := factory.Close()
		if err != nil {
			t.Logf("Error closing factory: %v", err)
		}
		err = os.RemoveAll(dir)
		if err != nil {
			t.Logf("Error removing test directory: %v", err)
		}
	}

	return factory, cleanup
}

func TestPebbleStateStore_BasicOperations(t *testing.T) {
	factory, cleanup := setupPebbleTest(t)
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

func TestPebbleStateStore_ListStates(t *testing.T) {
	factory, cleanup := setupPebbleTest(t)
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
	// Note: When keyPrefix is empty (nil function), ListStates returns full keys
	// Check if keys in range are present
	hasB := false
	hasC := false
	for _, item := range list {
		if item == "b" {
			hasB = true
		}
		if item == "c" {
			hasC = true
		}
	}
	assert.True(t, hasB || hasC, "Should find keys in range")

	// Test ListStates without range
	list, err = store.ListStates(ctx, "a", "f")
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, len(list), len(keys))
}

func TestPebbleStateStore_NewKeyOperations(t *testing.T) {
	factory, cleanup := setupPebbleTest(t)
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

func TestPebbleStateStore_DeleteAll(t *testing.T) {
	factory, cleanup := setupPebbleTest(t)
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

func TestPebbleStateStore_DeleteAll_All0xFF(t *testing.T) {
	factory, cleanup := setupPebbleTest(t)
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

func TestPebbleStateStore_Merge(t *testing.T) {
	factory, cleanup := setupPebbleTest(t)
	defer cleanup()

	ctx := context.Background()
	store, err := factory.NewStateStore(nil)
	require.NoError(t, err)
	defer store.Close()

	keyGroup := []byte{0x01}
	key := []byte{0x02}
	namespace := []byte{0x03}
	userKey := []byte{0x04}

	// Test Merge - Pebble's Merge appends by default
	value1 := []byte("value1")
	err = store.Merge(ctx, keyGroup, key, namespace, userKey, value1)
	assert.NoError(t, err)

	retrieved, err := store.Get(ctx, keyGroup, key, namespace, userKey)
	assert.NoError(t, err)
	// First merge just stores the value
	assert.Equal(t, value1, retrieved)

	// Test Merge again - Pebble Merge will call the merge operator if configured
	// Without a merge operator, it behaves like Put (replaces value)
	value2 := []byte("value2")
	err = store.Merge(ctx, keyGroup, key, namespace, userKey, value2)
	assert.NoError(t, err)

	retrieved, err = store.Get(ctx, keyGroup, key, namespace, userKey)
	assert.NoError(t, err)
	// Without merge operator, Merge behaves like Put
	assert.Equal(t, value2, retrieved)
}

func TestPebbleStateStore_KeyPrefixIsolation(t *testing.T) {
	factory, cleanup := setupPebbleTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create two stores with different key prefixes
	func1 := &model.Function{
		Name:  "func1",
		State: config.ConfigMap{"key_prefix": "prefix1"},
	}
	store1, err := factory.NewStateStore(func1)
	require.NoError(t, err)
	defer store1.Close()

	func2 := &model.Function{
		Name:  "func2",
		State: config.ConfigMap{"key_prefix": "prefix2"},
	}
	store2, err := factory.NewStateStore(func2)
	require.NoError(t, err)
	defer store2.Close()

	// Store same key in both prefixes
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

func TestPebbleStateStore_EmptyValues(t *testing.T) {
	factory, cleanup := setupPebbleTest(t)
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

func TestPebbleStateStoreFactory_Config(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble_config_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	cfgMap := config.ConfigMap{
		"dir_name":                 dir,
		"max-open-files":           1000,
		"l0-compaction-threshold":  4,
		"l0-stop-writes-threshold": 36,
		"l-base-max-bytes":         int64(256 * 1024 * 1024),
	}

	factory, err := statestore.NewPebbleStateStoreFactory(cfgMap)
	assert.NoError(t, err)
	defer factory.Close()

	// Verify factory creates stores correctly
	store, err := factory.NewStateStore(nil)
	assert.NoError(t, err)
	assert.NotNil(t, store)

	err = store.Close()
	assert.NoError(t, err)
}

func TestPebbleStateStoreFactory_WithFunction(t *testing.T) {
	factory, cleanup := setupPebbleTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create function with key prefix config
	func1 := &model.Function{
		Name:  "test_func",
		State: config.ConfigMap{"key_prefix": "test_prefix"},
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

func TestPebbleStateStoreFactory_DefaultKeyPrefix(t *testing.T) {
	factory, cleanup := setupPebbleTest(t)
	defer cleanup()

	// Create function without key prefix config, should use function name
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

func TestPebbleStateStoreFactory_NilFunction(t *testing.T) {
	factory, cleanup := setupPebbleTest(t)
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

func TestPebbleStateStore_Close(t *testing.T) {
	factory, cleanup := setupPebbleTest(t)
	defer cleanup()

	func1 := &model.Function{
		Name:  "func1",
		State: config.ConfigMap{"key_prefix": "prefix1"},
	}

	store, err := factory.NewStateStore(func1)
	require.NoError(t, err)

	ctx := context.Background()
	err = store.PutState(ctx, "key", []byte("value"))
	assert.NoError(t, err)

	// Close should not error
	err = store.Close()
	assert.NoError(t, err)
}

func TestPebbleStateStore_BasicOperations_Original(t *testing.T) {
	ctx := context.Background()
	storeFact, err := statestore.NewDefaultPebbleStateStoreFactory()
	require.NoError(t, err)
	defer storeFact.Close()

	store, err := storeFact.NewStateStore(nil)
	require.NoError(t, err)
	defer store.Close()

	_, err = store.GetState(ctx, "key")
	assert.ErrorIs(t, err, api.ErrNotFound)

	err = store.PutState(ctx, "key", []byte("value"))
	assert.NoError(t, err)

	value, err := store.GetState(ctx, "key")
	assert.NoError(t, err)
	assert.Equal(t, "value", string(value))
}

func TestPebbleStateStore_Iterator(t *testing.T) {
	factory, cleanup := setupPebbleTest(t)
	defer cleanup()

	ctx := context.Background()
	store, err := factory.NewStateStore(nil)
	require.NoError(t, err)
	defer store.Close()

	// Insert multiple keys - use larger data set
	keyGroup := []byte{0x01}
	key := []byte{0x02}
	namespace := []byte{0x03}

	numKeys := 100
	for i := 0; i < numKeys; i++ {
		userKey := make([]byte, 2)
		userKey[0] = byte(i / 256)
		userKey[1] = byte(i % 256)
		value := make([]byte, 64)
		for j := 0; j < 64; j++ {
			value[j] = byte(i*64 + j)
		}
		err := store.Put(ctx, keyGroup, key, namespace, userKey, value)
		require.NoError(t, err)
	}

	// Create iterator with prefix
	prefix := buildKeyForTest(keyGroup, key, namespace, []byte{})
	iterID, err := store.NewIterator(prefix)
	require.NoError(t, err)
	defer store.CloseIterator(iterID)

	// Iterate through all keys
	var values [][]byte
	hasMore, err := store.HasNext(iterID)
	require.NoError(t, err)
	for hasMore {
		value, err := store.Next(iterID)
		require.NoError(t, err)
		values = append(values, value)
		hasMore, err = store.HasNext(iterID)
		require.NoError(t, err)
	}

	// Verify we got all values
	assert.Len(t, values, numKeys)

	// Verify values are correct
	expectedValues := make(map[string]bool)
	for i := 0; i < numKeys; i++ {
		expectedValue := make([]byte, 64)
		for j := 0; j < 64; j++ {
			expectedValue[j] = byte(i*64 + j)
		}
		expectedValues[string(expectedValue)] = true
	}

	for _, v := range values {
		assert.True(t, expectedValues[string(v)], "Value should be present")
	}
}

func TestPebbleStateStore_Iterator_Prefix(t *testing.T) {
	factory, cleanup := setupPebbleTest(t)
	defer cleanup()

	ctx := context.Background()
	store, err := factory.NewStateStore(nil)
	require.NoError(t, err)
	defer store.Close()

	// Insert keys with different prefixes - use larger data set
	keyGroup1 := []byte{0x01}
	keyGroup2 := []byte{0x02}
	key := []byte{0x03}
	namespace := []byte{0x04}

	numKeys1 := 50
	for i := 0; i < numKeys1; i++ {
		userKey := make([]byte, 2)
		userKey[0] = byte(i / 256)
		userKey[1] = byte(i % 256)
		value := make([]byte, 32)
		for j := 0; j < 32; j++ {
			value[j] = byte(0x10 + i)
		}
		err := store.Put(ctx, keyGroup1, key, namespace, userKey, value)
		require.NoError(t, err)
	}

	numKeys2 := 30
	for i := 0; i < numKeys2; i++ {
		userKey := make([]byte, 2)
		userKey[0] = byte(i / 256)
		userKey[1] = byte(i % 256)
		value := make([]byte, 32)
		for j := 0; j < 32; j++ {
			value[j] = byte(0x20 + i)
		}
		err := store.Put(ctx, keyGroup2, key, namespace, userKey, value)
		require.NoError(t, err)
	}

	// Create iterator with specific prefix
	prefix := buildKeyForTest(keyGroup1, key, namespace, []byte{})
	iterID, err := store.NewIterator(prefix)
	require.NoError(t, err)
	defer store.CloseIterator(iterID)

	// Iterate through keys with first prefix
	var values [][]byte
	hasMore, err := store.HasNext(iterID)
	require.NoError(t, err)
	for hasMore {
		value, err := store.Next(iterID)
		require.NoError(t, err)
		values = append(values, value)
		hasMore, err = store.HasNext(iterID)
		require.NoError(t, err)
	}

	// Verify we got only values from first prefix
	assert.Len(t, values, numKeys1)

	// Verify values are from keyGroup1
	expectedValues := make(map[string]bool)
	for i := 0; i < numKeys1; i++ {
		expectedValue := make([]byte, 32)
		for j := 0; j < 32; j++ {
			expectedValue[j] = byte(0x10 + i)
		}
		expectedValues[string(expectedValue)] = true
	}

	for _, v := range values {
		assert.True(t, expectedValues[string(v)], "Value should be present")
	}
}

func TestPebbleStateStore_Iterator_EmptyPrefix(t *testing.T) {
	factory, cleanup := setupPebbleTest(t)
	defer cleanup()

	ctx := context.Background()
	store, err := factory.NewStateStore(nil)
	require.NoError(t, err)
	defer store.Close()

	// Insert multiple keys - use larger data set
	numKeys := 80
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%03d", i)
		value := make([]byte, 128)
		for j := 0; j < 128; j++ {
			value[j] = byte(i*128 + j)
		}
		err := store.PutState(ctx, key, value)
		require.NoError(t, err)
	}

	// Create iterator with empty prefix (iterate all)
	iterID, err := store.NewIterator([]byte{})
	require.NoError(t, err)
	defer store.CloseIterator(iterID)

	// Iterate through all keys
	count := 0
	hasMore, err := store.HasNext(iterID)
	require.NoError(t, err)
	for hasMore {
		_, err := store.Next(iterID)
		require.NoError(t, err)
		count++
		hasMore, err = store.HasNext(iterID)
		require.NoError(t, err)
	}

	// Verify we got at least the expected number of values
	assert.GreaterOrEqual(t, count, numKeys)
}

// Helper function for tests
func buildKeyForTest(keyGroup, key, namespace, userKey []byte) []byte {
	result := make([]byte, 0, len(keyGroup)+len(key)+len(namespace)+len(userKey))
	result = append(result, keyGroup...)
	result = append(result, key...)
	result = append(result, namespace...)
	result = append(result, userKey...)
	return result
}
