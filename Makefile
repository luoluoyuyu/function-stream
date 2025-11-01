# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

.PHONY: license setup-rocksdb clean-rocksdb clean-rocksdb-intermediate setup-rocksdb-deps build build-all build-lite test-rocksdb test-statestore-rocksdb test-statestore test-statestore-all install


# Build Lite version (explicit, same as build-all)
build: build-all

# Build with RocksDB support (requires RocksDB installation)
build-all: setup-rocksdb
	@echo ""
	@echo "Building with RocksDB support..."
	@# Check and install dependencies
	@chmod +x scripts/setup-rocksdb-deps.sh
	@./scripts/setup-rocksdb-deps.sh check
	@# Detect operating system and generate link flags
	@ROCKSDB_LIB_DIR=$$(pwd)/bin/function-stream/lib/rocksdb/lib; \
	ROCKSDB_INCLUDE_DIR=$$(pwd)/bin/function-stream/lib/rocksdb/include; \
	TMP_LDFLAGS=$$(mktemp); \
	trap "rm -f $$TMP_LDFLAGS" EXIT; \
	./scripts/setup-rocksdb-deps.sh ldflags "$$ROCKSDB_LIB_DIR" "$$TMP_LDFLAGS"; \
	CGO_LDFLAGS=$$(cat $$TMP_LDFLAGS); \
	PKG_CONFIG_PATH="" \
	CGO_ENABLED=1 CGO_CFLAGS="-I$$ROCKSDB_INCLUDE_DIR" CGO_LDFLAGS="$$CGO_LDFLAGS" \
	go build -v -tags "rocksdb,grocksdb_no_link" -ldflags="-s -w" -o bin/function-stream ./cmd
	@echo ""
	@echo "Checking binary dependencies..."
	@if command -v otool >/dev/null 2>&1; then \
		echo "Binary dependencies:"; \
		otool -L bin/function-stream 2>/dev/null | grep -v "^bin/function-stream:" | grep -v "^\t/usr/lib/" | grep -v "^\t/System/Library/" | head -10 || echo "  ✅ Only system libraries (compatible with all macOS)"; \
	fi
	@echo ""
	@echo "Cleaning RocksDB files for packaging..."
	@chmod +x scripts/build-rocksdb.sh
	@./scripts/build-rocksdb.sh clean-for-package
	@echo "✅ Build completed"
	@echo ""
	@echo "📦 Binary: bin/function-stream"
	@echo "   - RocksDB: ✅ Statically linked"
	@echo "   - System libraries (bz2, z): ✅ Available on all macOS systems"
	@echo "   - Ready to distribute to other macOS systems"

# Build all features - Lite version without RocksDB (only Pebble support)
build-lite:
	@echo "Building Lite version (Pebble only, no RocksDB)..."
	go build -v -o bin/function-stream ./cmd
	@echo "✅ Lite build completed (Pebble only)"

# Setup RocksDB in bin/function-stream/lib directory
setup-rocksdb:
	@echo "Checking and installing RocksDB dependencies..."
	@chmod +x scripts/setup-rocksdb-deps.sh scripts/build-rocksdb.sh
	@./scripts/setup-rocksdb-deps.sh check
	@echo ""
	@echo "Downloading and building RocksDB to bin/function-stream/lib directory..."
	@./scripts/build-rocksdb.sh build

# Clean RocksDB in bin/function-stream/lib directory (complete cleanup, including tarball)
clean-rocksdb:
	@echo "Cleaning RocksDB in bin/function-stream/lib directory..."
	@chmod +x scripts/build-rocksdb.sh
	@./scripts/build-rocksdb.sh clean

# Clean RocksDB intermediate files (keep binaries, clean source code and tarball)
clean-rocksdb-intermediate:
	@echo "Cleaning RocksDB intermediate files (keeping binaries)..."
	@chmod +x scripts/build-rocksdb.sh
	@./scripts/build-rocksdb.sh clean-intermediate

# Install (pre-install RocksDB, then execute installation, finally clean intermediate files)
install: setup-rocksdb
	@echo "✅ RocksDB pre-installation completed"
	@echo ""
	@echo "Executing project installation..."
	@# Add your project installation steps here
	@# For example: go install ./...
	@echo ""
	@echo "Cleaning RocksDB intermediate files..."
	@$(MAKE) clean-rocksdb-intermediate
	@echo "✅ Installation completed"

# Install RocksDB dependencies
setup-rocksdb-deps:
	@chmod +x scripts/setup-rocksdb-deps.sh
	@./scripts/setup-rocksdb-deps.sh install


build-example:
	tinygo build -o bin/example_basic.wasm -target=wasi ./examples/basic
	go build -o bin/example_external_function ./examples/basic

run-example-external-functions:
	FS_SOCKET_PATH=/tmp/fs.sock FS_FUNCTION_NAME=fs/external-function ./bin/example_external_function

lint:
	golangci-lint run

lint-fix:
	golangci-lint run --fix

build-with-examples: build build-example

test:
	go test -race ./... -timeout 10m

# Test RocksDB (automatically detect and install dependencies, generate correct link flags)
test-rocksdb:
	@# Check and install dependencies
	@chmod +x scripts/setup-rocksdb-deps.sh
	@./scripts/setup-rocksdb-deps.sh check
	@# Detect operating system and generate link flags
	@ROCKSDB_LIB_DIR=$$(pwd)/bin/function-stream/lib/rocksdb/lib; \
	ROCKSDB_INCLUDE_DIR=$$(pwd)/bin/function-stream/lib/rocksdb/include; \
	TMP_LDFLAGS=$$(mktemp); \
	trap "rm -f $$TMP_LDFLAGS" EXIT; \
	./scripts/setup-rocksdb-deps.sh ldflags "$$ROCKSDB_LIB_DIR" "$$TMP_LDFLAGS"; \
	CGO_LDFLAGS=$$(cat $$TMP_LDFLAGS); \
	if [ -f "$$ROCKSDB_LIB_DIR/librocksdb.a" ] && [ -d "$$ROCKSDB_INCLUDE_DIR" ]; then \
		echo "✅ Using RocksDB from project lib directory..."; \
		CGO_ENABLED=1 CGO_CFLAGS="-I$$ROCKSDB_INCLUDE_DIR" CGO_LDFLAGS="$$CGO_LDFLAGS" \
		go test -tags rocksdb -race ./... -timeout 10m; \
	else \
		echo "⚠️  RocksDB not found in project lib directory"; \
		echo "Auto-installing to project lib directory (recommended, ensures version compatibility)..."; \
		echo ""; \
		$(MAKE) setup-rocksdb || { \
			echo "❌ RocksDB installation failed"; \
			echo "Please run manually: make setup-rocksdb"; \
			exit 1; \
		}; \
		echo ""; \
		echo "✅ RocksDB installation completed, starting tests..."; \
		echo ""; \
		./scripts/setup-rocksdb-deps.sh ldflags "$$ROCKSDB_LIB_DIR" "$$TMP_LDFLAGS"; \
		CGO_LDFLAGS=$$(cat $$TMP_LDFLAGS); \
		CGO_ENABLED=1 CGO_CFLAGS="-I$$ROCKSDB_INCLUDE_DIR" CGO_LDFLAGS="$$CGO_LDFLAGS" \
		go test -tags rocksdb -race ./... -timeout 10m; \
	fi

test-statestore:
	go test ./fs/statestore/... -timeout 10m -v -run TestPebble

# Test RocksDB StateStore (automatically install RocksDB and dependencies, generate correct link flags)
test-statestore-rocksdb: setup-rocksdb
	@# Check and install dependencies
	@chmod +x scripts/setup-rocksdb-deps.sh
	@./scripts/setup-rocksdb-deps.sh check
	@echo ""
	@echo "✅ RocksDB installed, starting tests..."
	@echo ""
	@# Detect operating system and generate link flags
	@ROCKSDB_LIB_DIR=$$(pwd)/bin/function-stream/lib/rocksdb/lib; \
	ROCKSDB_INCLUDE_DIR=$$(pwd)/bin/function-stream/lib/rocksdb/include; \
	TMP_LDFLAGS=$$(mktemp); \
	trap "rm -f $$TMP_LDFLAGS" EXIT; \
	./scripts/setup-rocksdb-deps.sh ldflags "$$ROCKSDB_LIB_DIR" "$$TMP_LDFLAGS"; \
	CGO_LDFLAGS=$$(cat $$TMP_LDFLAGS); \
	CGO_ENABLED=1 CGO_CFLAGS="-I$$ROCKSDB_INCLUDE_DIR" CGO_LDFLAGS="$$CGO_LDFLAGS" \
	go test -tags rocksdb ./fs/statestore/... -timeout 10m -v -run TestRocksDB

test-statestore-all:
	@echo "=========================================="
	@echo "Testing Pebble (no installation required)"
	@echo "=========================================="
	@$(MAKE) test-statestore
	@echo ""
	@echo "=========================================="
	@echo "Testing RocksDB (requires dependencies)"
	@echo "=========================================="
	@$(MAKE) test-statestore-rocksdb

license:
	@./license-checker/license-checker.sh
