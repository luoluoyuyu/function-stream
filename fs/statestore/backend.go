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
)

// StateBackend is the state backend interface
type StateBackend interface {
	// Basic operations - using 4 byte[] as Key
	Put(ctx context.Context, keyGroup, key, namespace, userKey []byte, value []byte) error
	Get(ctx context.Context, keyGroup, key, namespace, userKey []byte) ([]byte, error)
	Delete(ctx context.Context, keyGroup, key, namespace, userKey []byte) error
	DeleteAll(ctx context.Context, keyGroup, key, namespace []byte) error
	Merge(ctx context.Context, keyGroup, key, namespace, userKey []byte, value []byte) error

	// Lifecycle
	Close() error
}

// No global state backend is provided, callers manage their own instances

// buildKey builds a storage key - simple concatenation, no parsing
func buildKey(keyGroup, key, namespace, userKey []byte) []byte {
	// Simple concatenation of 4 byte[], using separator to avoid conflicts
	separator := []byte{0x00, 0xFF, 0x00, 0xFF} // separator

	result := make([]byte, 0, len(keyGroup)+len(key)+len(namespace)+len(userKey)+len(separator)*3)
	result = append(result, keyGroup...)
	result = append(result, separator...)
	result = append(result, key...)
	result = append(result, separator...)
	result = append(result, namespace...)
	result = append(result, separator...)
	result = append(result, userKey...)

	return result
}

// buildKeyWithPrefix builds a storage key with column family name prefix (column family name as logical prefix)
// If columnFamily is empty, it's equivalent to buildKey
// Column family prefix has been removed, users encode isolation via keyGroup

// DefaultColumnFamily is the default column family name (kept as constant for potential external references)
const DefaultColumnFamily = "default"

// No operator registration/management is provided, all managed by callers as needed

// No global convenience operation functions are provided, callers hold and operate on specific instances
