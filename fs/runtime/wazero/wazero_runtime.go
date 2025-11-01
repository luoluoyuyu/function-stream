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

package wazero

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/functionstream/function-stream/clients/gofs"

	"github.com/functionstream/function-stream/common/model"

	"github.com/functionstream/function-stream/common"
	"github.com/functionstream/function-stream/fs/api"
	"github.com/functionstream/function-stream/fs/contube"
	"github.com/tetratelabs/wazero"
	wazero_api "github.com/tetratelabs/wazero/api"
	exp_sys "github.com/tetratelabs/wazero/experimental/sys"
	"github.com/tetratelabs/wazero/experimental/sysfs"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"github.com/tetratelabs/wazero/sys"
)

type WazeroFunctionRuntimeFactory struct {
	opts *options
}

type WASMFetcher interface {
	Fetch(url string) ([]byte, error)
}

type FileWASMFetcher struct {
}

func (f *FileWASMFetcher) Fetch(url string) ([]byte, error) {
	return os.ReadFile(url)
}

func NewWazeroFunctionRuntimeFactory() api.FunctionRuntimeFactory {
	return NewWazeroFunctionRuntimeFactoryWithOptions(WithWASMFetcher(&FileWASMFetcher{}))
}

func NewWazeroFunctionRuntimeFactoryWithOptions(opts ...func(*options)) api.FunctionRuntimeFactory {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}
	return &WazeroFunctionRuntimeFactory{
		opts: o,
	}
}

type options struct {
	wasmFetcher WASMFetcher
}

func WithWASMFetcher(fetcher WASMFetcher) func(*options) {
	return func(o *options) {
		o.wasmFetcher = fetcher
	}
}

func (f *WazeroFunctionRuntimeFactory) NewFunctionRuntime(instance api.FunctionInstance,
	rc *model.RuntimeConfig) (api.FunctionRuntime, error) {
	log := instance.Logger()
	r := wazero.NewRuntime(instance.Context())

	// Register env module with abort and stateStore functions
	err := registerEnvModule(r, instance.Context(), instance, log)
	if err != nil {
		return nil, fmt.Errorf("error instantiating env module: %w", err)
	}

	wasmLog := &logWriter{
		log: log,
	}

	processFile := &oneShotFile{}
	registerSchema := &oneShotFile{}
	fileMap := map[string]exp_sys.File{
		"process":        processFile,
		"registerSchema": registerSchema,
	}
	fsConfig := wazero.NewFSConfig().(sysfs.FSConfig).WithSysFSMount(newMemoryFS(fileMap), "")
	config := wazero.NewModuleConfig().
		WithEnv(gofs.FSFunctionName, common.GetNamespacedName(instance.Definition().Namespace,
			instance.Definition().Name).String()).
		WithStdout(wasmLog).WithStderr(wasmLog).WithFSConfig(fsConfig)

	wasi_snapshot_preview1.MustInstantiate(instance.Context(), r)

	if rc.Config == nil {
		return nil, fmt.Errorf("no runtime config found")
	}
	path, exist := rc.Config["archive"]
	if !exist {
		return nil, fmt.Errorf("no wasm archive found")
	}
	pathStr := path.(string)
	if pathStr == "" {
		return nil, fmt.Errorf("empty wasm archive found")
	}
	wasmBytes, err := f.opts.wasmFetcher.Fetch(pathStr)
	if err != nil {
		return nil, fmt.Errorf("error reading wasm file: %w", err)
	}
	mod, err := r.InstantiateWithConfig(instance.Context(), wasmBytes, config)
	if err != nil {
		var exitErr *sys.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() != 0 {
			return nil, fmt.Errorf("failed to instantiate function, function exit with code %d", exitErr.ExitCode())
		}
	}
	if err != nil {
		return nil, fmt.Errorf("error instantiating runtime: %w", err)
	}
	process := mod.ExportedFunction("process")
	if process == nil {
		return nil, fmt.Errorf("no process function found")
	}
	outputSchemaDef := registerSchema.output
	var outputSchema string
	if outputSchemaDef != nil {
		outputSchema = string(outputSchemaDef)
		log.Info("Register the output schema", "schema", outputSchema)
	}
	return &FunctionRuntime{
		callFunc: func(e contube.Record) (contube.Record, error) {
			processFile.input = e.GetPayload()
			_, err := process.Call(instance.Context())
			if err != nil {
				return nil, err
			}
			return contube.NewSchemaRecordImpl(processFile.output, outputSchema, e.Commit), nil
		},
		stopFunc: func() {
			err := r.Close(instance.Context())
			if err != nil {
				log.Error(err, "failed to close the runtime")
			}
		},
		log: log,
	}, nil
}

type FunctionRuntime struct {
	api.FunctionRuntime
	callFunc func(e contube.Record) (contube.Record, error)
	stopFunc func()
	log      *common.Logger
}

func (r *FunctionRuntime) Call(e contube.Record) (contube.Record, error) {
	return r.callFunc(e)
}

func (r *FunctionRuntime) Stop() {
	r.stopFunc()
}

// helper functions to read/write byte arrays from/to wasm memory
//
// Note on pointer size compatibility:
//   - Current WebAssembly implementations (wasm32) use 32-bit memory offsets,
//     limiting addressable memory to 4GB. The wazero runtime's Memory interface
//     uses uint32 for addresses.
//   - Future WebAssembly specifications may introduce wasm64 with 64-bit addresses.
//   - The current implementation uses uint32 for memory operations, which is
//     compatible with wasm32 and the wazero Memory API.
//   - For wasm64 compatibility in the future, these functions would need to be
//     modified to use uint64 for address operations when wazero adds support.
func readBytesFromMemory(module wazero_api.Module, ptr, size uint32) ([]byte, error) {
	if size == 0 {
		return nil, nil
	}
	mem := module.Memory()
	if mem == nil {
		return nil, fmt.Errorf("memory is not available")
	}
	data, ok := mem.Read(ptr, size)
	if !ok {
		return nil, fmt.Errorf("failed to read memory at offset %d, size %d", ptr, size)
	}
	result := make([]byte, size)
	copy(result, data)
	return result, nil
}

func writeBytesToMemory(module wazero_api.Module, data []byte) (uint32, error) {
	if len(data) == 0 {
		return 0, nil
	}

	// Check for potential overflow: max uint32 is 4GB-1
	if uint64(len(data)) > uint64(^uint32(0)) {
		return 0, fmt.Errorf("data size exceeds wasm32 memory limit: %d bytes", len(data))
	}

	mem := module.Memory()
	if mem == nil {
		return 0, fmt.Errorf("memory is not available")
	}
	// Get current memory size in pages
	currentSize := mem.Size()
	neededSize := uint32(len(data))
	totalNeeded := currentSize + neededSize

	// Calculate how many pages we need to add
	pagesToAdd := (totalNeeded + 65535) / 65536 // Round up to pages
	currentPages := currentSize / 65536
	_, ok := mem.Grow(pagesToAdd)
	if !ok {
		return 0, fmt.Errorf("failed to grow memory")
	}

	// Calculate offset: start at end of original memory
	offset := currentPages * 65536
	ok = mem.Write(offset, data)
	if !ok {
		return 0, fmt.Errorf("failed to write memory at offset %d", offset)
	}
	return offset, nil
}

// registerEnvModule registers both abort and StateStore functions to the wasm env module
func registerEnvModule(runtime wazero.Runtime, ctx context.Context, instance api.FunctionInstance, log *common.Logger) error {
	stateStore := instance.FunctionContext().GetStateStore()

	// Helper to read byte array from wasm memory: (ptr, size uint32) -> bytes
	readBytes := func(module wazero_api.Module, ptrSize uint64) ([]byte, error) {
		ptr := uint32(ptrSize >> 32)
		size := uint32(ptrSize)
		return readBytesFromMemory(module, ptr, size)
	}

	// Helper to write byte array to wasm memory: bytes -> (ptr, size uint32)
	writeBytes := func(module wazero_api.Module, data []byte) (uint64, error) {
		ptr, err := writeBytesToMemory(module, data)
		if err != nil {
			return 0, err
		}
		return uint64(ptr)<<32 | uint64(len(data)), nil
	}

	builder := runtime.NewHostModuleBuilder("env")

	// Register abort function
	builder = builder.NewFunctionBuilder().WithFunc(func(ctx context.Context,
		m wazero_api.Module, a, b, c, d uint32) {
		log.Error(fmt.Errorf("abort(%d, %d, %d, %d)", a, b, c, d), "the function is calling abort")
	}).Export("abort")

	// Put(keyGroup, key, namespace, userKey ptrSize, value ptrSize) -> uint64(ptr, size)
	builder = builder.NewFunctionBuilder().WithFunc(func(module wazero_api.Module, kg, k, ns, ukPtrSize, vPtrSize uint64) (resultPtrSize uint64) {
		keyGroup, err := readBytes(module, kg)
		if err != nil {
			log.Error(err, "failed to read keyGroup")
			return 0
		}
		key, err := readBytes(module, k)
		if err != nil {
			log.Error(err, "failed to read key")
			return 0
		}
		namespace, err := readBytes(module, ns)
		if err != nil {
			log.Error(err, "failed to read namespace")
			return 0
		}
		userKey, err := readBytes(module, ukPtrSize)
		if err != nil {
			log.Error(err, "failed to read userKey")
			return 0
		}
		value, err := readBytes(module, vPtrSize)
		if err != nil {
			log.Error(err, "failed to read value")
			return 0
		}

		if err := stateStore.Put(ctx, keyGroup, key, namespace, userKey, value); err != nil {
			log.Error(err, "StateStore.Put failed")
			return 0
		}

		// Return success (empty result)
		resultPtrSize, _ = writeBytes(module, []byte{})
		return resultPtrSize
	}).Export("stateStorePut")

	// Get(keyGroup, key, namespace, userKey ptrSize) -> uint64(ptr, size)
	builder = builder.NewFunctionBuilder().WithFunc(func(module wazero_api.Module, kg, k, ns, ukPtrSize uint64) (resultPtrSize uint64) {
		keyGroup, err := readBytes(module, kg)
		if err != nil {
			log.Error(err, "failed to read keyGroup")
			return 0
		}
		key, err := readBytes(module, k)
		if err != nil {
			log.Error(err, "failed to read key")
			return 0
		}
		namespace, err := readBytes(module, ns)
		if err != nil {
			log.Error(err, "failed to read namespace")
			return 0
		}
		userKey, err := readBytes(module, ukPtrSize)
		if err != nil {
			log.Error(err, "failed to read userKey")
			return 0
		}

		value, err := stateStore.Get(ctx, keyGroup, key, namespace, userKey)
		if err != nil {
			log.Error(err, "StateStore.Get failed")
			return 0
		}

		resultPtrSize, err = writeBytes(module, value)
		if err != nil {
			log.Error(err, "failed to write result")
			return 0
		}
		return resultPtrSize
	}).Export("stateStoreGet")

	// Delete(keyGroup, key, namespace, userKey ptrSize) -> uint64(ptr, size)
	builder = builder.NewFunctionBuilder().WithFunc(func(module wazero_api.Module, kg, k, ns, ukPtrSize uint64) (resultPtrSize uint64) {
		keyGroup, err := readBytes(module, kg)
		if err != nil {
			log.Error(err, "failed to read keyGroup")
			return 0
		}
		key, err := readBytes(module, k)
		if err != nil {
			log.Error(err, "failed to read key")
			return 0
		}
		namespace, err := readBytes(module, ns)
		if err != nil {
			log.Error(err, "failed to read namespace")
			return 0
		}
		userKey, err := readBytes(module, ukPtrSize)
		if err != nil {
			log.Error(err, "failed to read userKey")
			return 0
		}

		if err := stateStore.Delete(ctx, keyGroup, key, namespace, userKey); err != nil {
			log.Error(err, "StateStore.Delete failed")
			return 0
		}

		resultPtrSize, _ = writeBytes(module, []byte{})
		return resultPtrSize
	}).Export("stateStoreDelete")

	// DeleteAll(keyGroup, key, namespace) -> uint64(ptr, size)
	builder = builder.NewFunctionBuilder().WithFunc(func(module wazero_api.Module, kg, k, ns uint64) (resultPtrSize uint64) {
		keyGroup, err := readBytes(module, kg)
		if err != nil {
			log.Error(err, "failed to read keyGroup")
			return 0
		}
		key, err := readBytes(module, k)
		if err != nil {
			log.Error(err, "failed to read key")
			return 0
		}
		namespace, err := readBytes(module, ns)
		if err != nil {
			log.Error(err, "failed to read namespace")
			return 0
		}

		if err := stateStore.DeleteAll(ctx, keyGroup, key, namespace); err != nil {
			log.Error(err, "StateStore.DeleteAll failed")
			return 0
		}

		resultPtrSize, _ = writeBytes(module, []byte{})
		return resultPtrSize
	}).Export("stateStoreDeleteAll")

	// Merge(keyGroup, key, namespace, userKey ptrSize, value ptrSize) -> uint64(ptr, size)
	builder = builder.NewFunctionBuilder().WithFunc(func(module wazero_api.Module, kg, k, ns, ukPtrSize, vPtrSize uint64) (resultPtrSize uint64) {
		keyGroup, err := readBytes(module, kg)
		if err != nil {
			log.Error(err, "failed to read keyGroup")
			return 0
		}
		key, err := readBytes(module, k)
		if err != nil {
			log.Error(err, "failed to read key")
			return 0
		}
		namespace, err := readBytes(module, ns)
		if err != nil {
			log.Error(err, "failed to read namespace")
			return 0
		}
		userKey, err := readBytes(module, ukPtrSize)
		if err != nil {
			log.Error(err, "failed to read userKey")
			return 0
		}
		value, err := readBytes(module, vPtrSize)
		if err != nil {
			log.Error(err, "failed to read value")
			return 0
		}

		if err := stateStore.Merge(ctx, keyGroup, key, namespace, userKey, value); err != nil {
			log.Error(err, "StateStore.Merge failed")
			return 0
		}

		resultPtrSize, _ = writeBytes(module, []byte{})
		return resultPtrSize
	}).Export("stateStoreMerge")

	// NewIterator(prefix ptrSize) -> iteratorId
	builder = builder.NewFunctionBuilder().WithFunc(func(module wazero_api.Module, prefixPtrSize uint64) (iteratorId int64) {
		prefix, err := readBytes(module, prefixPtrSize)
		if err != nil {
			log.Error(err, "failed to read prefix")
			return 0
		}

		iterID, err := stateStore.NewIterator(prefix)
		if err != nil {
			log.Error(err, "StateStore.NewIterator failed")
			return 0
		}

		return iterID
	}).Export("stateStoreNewIterator")

	// HasNext(iteratorId) -> hasNext
	builder = builder.NewFunctionBuilder().WithFunc(func(module wazero_api.Module, iteratorId int64) (hasNext uint32) {
		has, err := stateStore.HasNext(iteratorId)
		if err != nil {
			log.Error(err, "StateStore.HasNext failed")
			return 0
		}

		if has {
			return 1
		}
		return 0
	}).Export("stateStoreIteratorHasNext")

	// Next(iteratorId) -> uint64(ptr, size)
	builder = builder.NewFunctionBuilder().WithFunc(func(module wazero_api.Module, iteratorId int64) (resultPtrSize uint64) {
		value, err := stateStore.Next(iteratorId)
		if err != nil {
			log.Error(err, "StateStore.Next failed")
			return 0
		}

		resultPtrSize, err = writeBytes(module, value)
		if err != nil {
			log.Error(err, "failed to write result")
			return 0
		}
		return resultPtrSize
	}).Export("stateStoreIteratorNext")

	// CloseIterator(iteratorId) -> uint64(ptr, size)
	builder = builder.NewFunctionBuilder().WithFunc(func(module wazero_api.Module, iteratorId int64) (resultPtrSize uint64) {
		if err := stateStore.CloseIterator(iteratorId); err != nil {
			log.Error(err, "StateStore.CloseIterator failed")
			return 0
		}

		resultPtrSize, _ = writeBytes(module, []byte{})
		return resultPtrSize
	}).Export("stateStoreIteratorClose")

	_, err := builder.Instantiate(ctx)
	return err
}
