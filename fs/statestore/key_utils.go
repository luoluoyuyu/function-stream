// Package statestore - key utility functions shared between Pebble and RocksDB implementations

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

// buildKey builds a composite key from keyGroup, key, namespace, and userKey
func buildKey(keyGroup, key, namespace, userKey []byte) []byte {
	newKey := make([]byte, 0, len(keyGroup)+len(key)+len(namespace)+len(userKey))
	newKey = append(newKey, keyGroup...)
	newKey = append(newKey, key...)
	newKey = append(newKey, namespace...)
	newKey = append(newKey, userKey...)
	return newKey
}

// incrementKey increments a key by 1 in lexicographic order
// This is used to create an end key for prefix-based range deletion
// Examples:
//   - [0x01, 0x02, 0x03] -> [0x01, 0x02, 0x04]
//   - [0x01, 0x02, 0xFF] -> [0x01, 0x03, 0x00]
//   - [0xFF, 0xFF, 0xFF] -> [0xFF, 0xFF, 0xFF, 0x01]
func incrementKey(key []byte) []byte {
	if len(key) == 0 {
		return []byte{0x01}
	}

	// Create a copy to avoid modifying the original
	result := make([]byte, len(key))
	copy(result, key)

	// Find the last non-0xFF byte and increment it
	i := len(result) - 1
	for i >= 0 && result[i] == 0xFF {
		result[i] = 0
		i--
	}

	if i >= 0 {
		// Found a byte to increment
		result[i]++
	} else {
		// All bytes are 0xFF, append 0x01 to make it larger
		result = append(result, 0x01)
	}

	return result
}

// isAll0xFF checks if all bytes in the key are 0xFF
func isAll0xFF(key []byte) bool {
	if len(key) == 0 {
		return false
	}
	for _, b := range key {
		if b != 0xFF {
			return false
		}
	}
	return true
}
