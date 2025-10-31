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
	"fmt"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble"
	"github.com/linxGnu/grocksdb"
)

// Shared DB registries (owned by factory)
var (
	sharedPebbleDB *pebble.DB
	sharedRocksDB  *grocksdb.DB
)

// InitSharedPebble initializes or returns existing Pebble shared DB
func InitSharedPebble(config *PebbleBackendConfig) (*pebble.DB, error) {
	if sharedPebbleDB != nil {
		return sharedPebbleDB, nil
	}
	if err := os.MkdirAll(config.BasePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}
	mainDBPath := filepath.Join(config.BasePath, "main")
	db, err := pebble.Open(mainDBPath, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble db: %w", err)
	}
	sharedPebbleDB = db
	return sharedPebbleDB, nil
}

// InitSharedRocksDB initializes or returns existing RocksDB shared DB
func InitSharedRocksDB(config *RocksDBBackendConfig) (*grocksdb.DB, error) {
	if sharedRocksDB != nil {
		return sharedRocksDB, nil
	}
	if err := os.MkdirAll(config.BasePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	dbPath := filepath.Join(config.BasePath, "rocksdb")
	db, err := grocksdb.OpenDb(opts, dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open rocksdb: %w", err)
	}
	sharedRocksDB = db
	return sharedRocksDB, nil
}

// NewStateBackendFromConfig creates a state backend from unified config (supports RocksDB column family name)
// - For Pebble: ignores cfName, uses shared Pebble DB and returns backend instance
// - For RocksDB: if cfName is provided, creates/binds the column family on the shared RocksDB DB for this instance
func NewStateBackendFromConfig(cfg *Config, cfName string) (StateBackend, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	// Default to Pebble
	if cfg.Type == "" {
		cfg.Type = BackendTypePebble
	}

	switch cfg.Type {
	case BackendTypePebble:
		pebbleCfg := cfg.ToPebbleConfig()
		pdb, err := InitSharedPebble(pebbleCfg)
		if err != nil {
			return nil, err
		}
		return NewPebbleBackendWithDBAndPrefix(pdb, []byte(cfName))

	case BackendTypeRocksDB:
		rocksCfg := cfg.ToRocksDBConfig()
		rdb, err := InitSharedRocksDB(rocksCfg)
		if err != nil {
			return nil, err
		}
		return NewRocksDBBackendWithDBAndCF(rdb, rocksCfg, cfName)

	default:
		return nil, fmt.Errorf("unsupported backend type: %s", cfg.Type)
	}
}

// GetBackendInfo gets backend information
func GetBackendInfo(backend StateBackend) map[string]interface{} {
	info := make(map[string]interface{})

	switch b := backend.(type) {
	case *PebbleBackend:
		info["type"] = "pebble"

	case *RocksDBBackend:
		info["type"] = "rocksdb"
		info["base_path"] = b.basePath

	default:
		info["type"] = "unknown"
	}

	return info
}

// ValidateBackendConfig validates backend configuration
func ValidateBackendConfig(backendType BackendType, config interface{}) error {
	switch backendType {
	case BackendTypePebble:
		pebbleConfig, ok := config.(*PebbleBackendConfig)
		if !ok {
			return fmt.Errorf("invalid pebble config type")
		}
		return validatePebbleConfig(pebbleConfig)

	case BackendTypeRocksDB:
		rocksdbConfig, ok := config.(*RocksDBBackendConfig)
		if !ok {
			return fmt.Errorf("invalid rocksdb config type")
		}
		return validateRocksDBConfig(rocksdbConfig)

	default:
		return fmt.Errorf("unsupported backend type: %s", backendType)
	}
}

// validatePebbleConfig validates Pebble configuration
func validatePebbleConfig(config *PebbleBackendConfig) error {
	if config.BasePath == "" {
		return fmt.Errorf("base_path is required")
	}

	// Check if directory is writable
	if err := os.MkdirAll(config.BasePath, 0755); err != nil {
		return fmt.Errorf("failed to create base path: %w", err)
	}

	// Check if directory is writable
	testFile := filepath.Join(config.BasePath, ".test_write")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		return fmt.Errorf("base path is not writable: %w", err)
	}
	os.Remove(testFile)

	return nil
}

// validateRocksDBConfig validates RocksDB configuration
func validateRocksDBConfig(config *RocksDBBackendConfig) error {
	if config.BasePath == "" {
		return fmt.Errorf("base_path is required")
	}

	// Check if directory is writable
	if err := os.MkdirAll(config.BasePath, 0755); err != nil {
		return fmt.Errorf("failed to create base path: %w", err)
	}

	// Check if directory is writable
	testFile := filepath.Join(config.BasePath, ".test_write")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		return fmt.Errorf("base path is not writable: %w", err)
	}
	os.Remove(testFile)

	return nil
}
