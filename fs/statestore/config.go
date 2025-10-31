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

	"github.com/functionstream/function-stream/common/config"
)

// BackendType is the backend type
type BackendType string

const (
	BackendTypePebble  BackendType = "pebble"
	BackendTypeRocksDB BackendType = "rocksdb"
)

// Config is the unified configuration using project standard configuration format
type Config struct {
	// Basic configuration
	Type     BackendType `json:"type" mapstructure:"type"`
	BasePath string      `json:"base-path" mapstructure:"base-path"`

	// Common configuration
	MaxOpenFiles int `json:"max-open-files" mapstructure:"max-open-files"`

	// RocksDB specific configuration
	RocksDBWriteBufferSize      uint64 `json:"rocksdb-write-buffer-size" mapstructure:"rocksdb-write-buffer-size"`
	RocksDBMaxWriteBufferNumber int    `json:"rocksdb-max-write-buffer-number" mapstructure:"rocksdb-max-write-buffer-number"`
	RocksDBTargetFileSizeBase   uint64 `json:"rocksdb-target-file-size-base" mapstructure:"rocksdb-target-file-size-base"`
	RocksDBMaxBytesForLevelBase uint64 `json:"rocksdb-max-bytes-for-level-base" mapstructure:"rocksdb-max-bytes-for-level-base"`
	RocksDBCompression          int    `json:"rocksdb-compression" mapstructure:"rocksdb-compression"`

	// Pebble specific configuration
	PebbleL0CompactionThreshold int   `json:"pebble-l0-compaction-threshold" mapstructure:"pebble-l0-compaction-threshold"`
	PebbleL0StopWritesThreshold int   `json:"pebble-l0-stop-writes-threshold" mapstructure:"pebble-l0-stop-writes-threshold"`
	PebbleLBaseMaxBytes         int64 `json:"pebble-l-base-max-bytes" mapstructure:"pebble-l-base-max-bytes"`
	PebbleTargetFileSize        int64 `json:"pebble-target-file-size" mapstructure:"pebble-target-file-size"`
}

// DefaultConfig returns default configuration
func DefaultConfig() *Config {
	return &Config{
		Type:     BackendTypePebble,
		BasePath: "./function_stream_state" + "/" + string(BackendTypePebble),

		// Common configuration
		MaxOpenFiles: 1000,

		// RocksDB configuration
		RocksDBWriteBufferSize:      64 * 1024 * 1024, // 64MB
		RocksDBMaxWriteBufferNumber: 3,
		RocksDBTargetFileSizeBase:   64 * 1024 * 1024,  // 64MB
		RocksDBMaxBytesForLevelBase: 256 * 1024 * 1024, // 256MB
		RocksDBCompression:          1,                 // SnappyCompression

		// Pebble configuration
		PebbleL0CompactionThreshold: 4,
		PebbleL0StopWritesThreshold: 12,
		PebbleLBaseMaxBytes:         256 * 1024 * 1024, // 256MB
		PebbleTargetFileSize:        64 * 1024 * 1024,  // 64MB
	}
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.Type != BackendTypePebble && c.Type != BackendTypeRocksDB {
		return fmt.Errorf("invalid backend type: %s", c.Type)
	}
	if c.BasePath == "" {
		return fmt.Errorf("base_path is required")
	}
	if c.MaxOpenFiles <= 0 {
		return fmt.Errorf("max_open_files must be positive")
	}
	return nil
}

// ToRocksDBConfig converts to RocksDB configuration
func (c *Config) ToRocksDBConfig() *RocksDBBackendConfig {
	return &RocksDBBackendConfig{
		BasePath:             c.BasePath,
		MaxOpenFiles:         c.MaxOpenFiles,
		WriteBufferSize:      c.RocksDBWriteBufferSize,
		MaxWriteBufferNumber: c.RocksDBMaxWriteBufferNumber,
		TargetFileSizeBase:   c.RocksDBTargetFileSizeBase,
		MaxBytesForLevelBase: c.RocksDBMaxBytesForLevelBase,
		Compression:          c.RocksDBCompression,
	}
}

// ToPebbleConfig converts to Pebble configuration
func (c *Config) ToPebbleConfig() *PebbleBackendConfig {
	return &PebbleBackendConfig{
		BasePath:              c.BasePath,
		MaxOpenFiles:          c.MaxOpenFiles,
		L0CompactionThreshold: c.PebbleL0CompactionThreshold,
		L0StopWritesThreshold: c.PebbleL0StopWritesThreshold,
		LBaseMaxBytes:         c.PebbleLBaseMaxBytes,
		TargetFileSize:        c.PebbleTargetFileSize,
	}
}

// NewConfigFromConfigMap creates state store configuration from generic config map
func NewConfigFromConfigMap(configMap config.ConfigMap) (*Config, error) {
	var stateConfig Config
	err := configMap.ToConfigStruct(&stateConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to convert config map to state config: %w", err)
	}

	// Fill missing fields with default values
	defaultConfig := DefaultConfig()
	if stateConfig.BasePath == "" {
		stateConfig.BasePath = defaultConfig.BasePath
	}
	if stateConfig.Type == "" {
		stateConfig.Type = defaultConfig.Type
	}
	if stateConfig.MaxOpenFiles == 0 {
		stateConfig.MaxOpenFiles = defaultConfig.MaxOpenFiles
	}
	if stateConfig.RocksDBWriteBufferSize == 0 {
		stateConfig.RocksDBWriteBufferSize = defaultConfig.RocksDBWriteBufferSize
	}
	if stateConfig.RocksDBMaxWriteBufferNumber == 0 {
		stateConfig.RocksDBMaxWriteBufferNumber = defaultConfig.RocksDBMaxWriteBufferNumber
	}
	if stateConfig.RocksDBTargetFileSizeBase == 0 {
		stateConfig.RocksDBTargetFileSizeBase = defaultConfig.RocksDBTargetFileSizeBase
	}
	if stateConfig.RocksDBMaxBytesForLevelBase == 0 {
		stateConfig.RocksDBMaxBytesForLevelBase = defaultConfig.RocksDBMaxBytesForLevelBase
	}
	if stateConfig.RocksDBCompression == 0 {
		stateConfig.RocksDBCompression = defaultConfig.RocksDBCompression
	}
	if stateConfig.PebbleL0CompactionThreshold == 0 {
		stateConfig.PebbleL0CompactionThreshold = defaultConfig.PebbleL0CompactionThreshold
	}
	if stateConfig.PebbleL0StopWritesThreshold == 0 {
		stateConfig.PebbleL0StopWritesThreshold = defaultConfig.PebbleL0StopWritesThreshold
	}
	if stateConfig.PebbleLBaseMaxBytes == 0 {
		stateConfig.PebbleLBaseMaxBytes = defaultConfig.PebbleLBaseMaxBytes
	}
	if stateConfig.PebbleTargetFileSize == 0 {
		stateConfig.PebbleTargetFileSize = defaultConfig.PebbleTargetFileSize
	}

	return &stateConfig, nil
}
