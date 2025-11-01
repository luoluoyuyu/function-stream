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

package api

import (
	"context"

	"github.com/functionstream/function-stream/common/model"

	"github.com/pkg/errors"
)

var ErrNotFound = errors.New("key not found")

type StateStore interface {
	PutState(ctx context.Context, key string, value []byte) error
	GetState(ctx context.Context, key string) (value []byte, err error)
	ListStates(ctx context.Context, startInclusive string, endExclusive string) (keys []string, err error)
	DeleteState(ctx context.Context, key string) error

	// Basic operations - using 4 byte[] as Key
	Put(ctx context.Context, keyGroup, key, namespace, userKey []byte, value []byte) error
	Get(ctx context.Context, keyGroup, key, namespace, userKey []byte) ([]byte, error)
	Delete(ctx context.Context, keyGroup, key, namespace, userKey []byte) error
	DeleteAll(ctx context.Context, keyGroup, key, namespace []byte) error
	Merge(ctx context.Context, keyGroup, key, namespace, userKey []byte, value []byte) error

	// Iterator operations for efficient prefix-based iteration
	// NewIterator creates a new iterator with the specified prefix and returns its ID
	NewIterator(prefix []byte) (int64, error)
	// HasNext checks if the iterator has a next element
	HasNext(id int64) (bool, error)
	// Next returns the value of the next element and advances the iterator
	Next(id int64) ([]byte, error)
	// CloseIterator closes the iterator with the specified ID
	CloseIterator(id int64) error

	Close() error
}

type StateStoreFactory interface {
	NewStateStore(f *model.Function) (StateStore, error)
	Close() error
}
