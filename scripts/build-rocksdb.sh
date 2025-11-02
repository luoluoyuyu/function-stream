#!/bin/bash
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

set -e

# Script directory (relative to project root)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# Place lib directory under bin directory to be consistent with other build artifacts
LIB_DIR="$PROJECT_ROOT/bin/function-stream/lib"
ROCKSDB_DIR="$LIB_DIR/rocksdb"
ROCKSDB_SRC_DIR="$ROCKSDB_DIR/src"
# RocksDB version compatible with grocksdb v1.7.16
# According to testing, grocksdb v1.7.16 requires RocksDB 7.x version
# 7.10.2 is a stable 7.x version compatible with grocksdb v1.7.16
ROCKSDB_VERSION="${ROCKSDB_VERSION:-7.10.2}"

# Timing related variables
START_TIME=0
STEP_START_TIME=0

echo_info() {
    echo "[INFO] $1"
}

echo_warn() {
    echo "[WARN] $1"
}

echo_error() {
    echo "[ERROR] $1"
}

echo_time() {
    echo "[TIME] $1"
}

# Start timer
start_timer() {
    START_TIME=$(date +%s)
    STEP_START_TIME=$START_TIME
    echo_time "Total timing started: $(date '+%Y-%m-%d %H:%M:%S')"
}

# Start step timing
step_start() {
    STEP_START_TIME=$(date +%s)
}

# Calculate and display duration
step_duration() {
    local step_name="$1"
    local end_time=$(date +%s)
    local duration=$((end_time - STEP_START_TIME))
    local minutes=$((duration / 60))
    local seconds=$((duration % 60))
    
    if [ $minutes -gt 0 ]; then
        echo_time "${step_name} took: ${minutes}m${seconds}s (${duration}s)"
    else
        echo_time "${step_name} took: ${seconds}s"
    fi
    STEP_START_TIME=$end_time
}

# Display total duration
total_duration() {
    local end_time=$(date +%s)
    local total_duration=$((end_time - START_TIME))
    local minutes=$((total_duration / 60))
    local seconds=$((total_duration % 60))
    
    echo ""
    echo_time "=========================================="
    if [ $minutes -gt 0 ]; then
        echo_time "Total time: ${minutes}m${seconds}s (${total_duration}s)"
    else
        echo_time "Total time: ${seconds}s"
    fi
    echo_time "End time: $(date '+%Y-%m-%d %H:%M:%S')"
    echo_time "=========================================="
}

# Detect operating system
detect_os() {
    if [[ "$OSTYPE" == "darwin"* ]]; then
        echo "darwin"
    elif [[ "$OSTYPE" == "linux-gnu"* ]] || [[ "$OSTYPE" == "linux-musl"* ]]; then
        echo "linux"
    elif [[ -n "$WSL_DISTRO_NAME" ]] || grep -qEi "(Microsoft|WSL)" /proc/version 2>/dev/null; then
        echo "linux"  # WSL is treated as Linux
    else
        echo "unknown"
    fi
}

OS=$(detect_os)

# Set library file name
STATIC_LIB_NAME="librocksdb.a"

# Check dependencies
check_dependencies() {
    local missing_deps=()
    
    if ! command -v git >/dev/null 2>&1; then
        missing_deps+=("git")
    fi
    
    # Linux/macOS need make
    if ! command -v make >/dev/null 2>&1; then
        missing_deps+=("make")
    fi
    
    # Check compiler
    if [[ "$OS" == "darwin"* ]]; then
        if ! command -v clang++ >/dev/null 2>&1 && ! command -v g++ >/dev/null 2>&1; then
            missing_deps+=("clang++ or g++")
        fi
    else
        if ! command -v g++ >/dev/null 2>&1; then
            missing_deps+=("g++")
        fi
    fi
    
    if [ ${#missing_deps[@]} -ne 0 ]; then
        echo_error "Missing dependencies: ${missing_deps[*]}"
        echo ""
        if [[ "$OS" == "darwin"* ]]; then
            echo "macOS installation guide:"
            echo "  Install Xcode Command Line Tools:"
            echo "    xcode-select --install"
        else
            echo "Linux installation guide:"
            echo "  Ubuntu/Debian: sudo apt-get install build-essential git"
            echo "  Fedora/RHEL: sudo dnf install gcc-c++ make git"
        fi
        exit 1
    fi
}

# Download prebuilt RocksDB binaries (if available)
download_prebuilt_rocksdb() {
    step_start
    echo_info "Attempting to download prebuilt RocksDB binaries..."
    
    # Detect platform architecture
    local arch=""
    local lib_name=""
    if [[ "$OS" == "darwin"* ]]; then
        if [[ $(uname -m) == "arm64" ]]; then
            arch="darwin-arm64"
            lib_name="librocksdb.a"
        else
            arch="darwin-amd64"
            lib_name="librocksdb.a"
        fi
    elif [[ "$OS" == "linux"* ]]; then
        if [[ $(uname -m) == "x86_64" ]]; then
            arch="linux-amd64"
            lib_name="librocksdb.a"
        elif [[ $(uname -m) == "aarch64" ]]; then
            arch="linux-arm64"
            lib_name="librocksdb.a"
        else
            return 1
        fi
    else
        return 1
    fi
    
    echo_info "Detected platform: $arch"
    
    # Prioritize checking if custom prebuilt source is configured
    if [ -n "$ROCKSDB_PREBUILT_BASE_URL" ]; then
        local prebuilt_url="${ROCKSDB_PREBUILT_BASE_URL}/rocksdb-${ROCKSDB_VERSION}-${arch}.tar.gz"
        echo_info "Attempting to download from custom source: $prebuilt_url"
        
        if command -v curl >/dev/null 2>&1; then
            if curl -L -f -o "rocksdb-${ROCKSDB_VERSION}-${arch}.tar.gz" "$prebuilt_url" 2>/dev/null; then
                echo_info "✅ Successfully downloaded prebuilt version"
                mkdir -p "$ROCKSDB_DIR/lib" "$ROCKSDB_DIR/include"
                tar -xzf "rocksdb-${ROCKSDB_VERSION}-${arch}.tar.gz" -C "$ROCKSDB_DIR" 2>/dev/null || {
                    # If tar extraction fails, try extracting directly to current directory
                    tar -xzf "rocksdb-${ROCKSDB_VERSION}-${arch}.tar.gz"
                    if [ -f "lib/$lib_name" ] && [ -d "include" ]; then
                        mkdir -p "$ROCKSDB_DIR/lib" "$ROCKSDB_DIR/include"
                        cp "lib/$lib_name" "$ROCKSDB_DIR/lib/"
                        cp -r include/* "$ROCKSDB_DIR/include/"
                    fi
                }
                rm -f "rocksdb-${ROCKSDB_VERSION}-${arch}.tar.gz"
                
                # Verify if files were downloaded successfully
                if [ -f "$ROCKSDB_DIR/lib/$lib_name" ] && [ -d "$ROCKSDB_DIR/include" ]; then
                    step_duration "Download prebuilt version"
                    echo_info "✅ Prebuilt RocksDB binaries are ready"
                    return 0
                fi
            fi
        fi
    fi
    
    # Try GitHub Releases (example, users need to build and upload themselves)
    # A standard GitHub Releases URL pattern can be added here
    # For example: https://github.com/your-org/rocksdb-prebuilt/releases/download/v${ROCKSDB_VERSION}/rocksdb-${arch}.tar.gz
    
    # Currently RocksDB official doesn't provide prebuilt versions, so return failure and fall back to source compilation
    echo_warn "Prebuilt RocksDB binaries not found, will use source compilation"
    echo_info "Hint: You can specify a prebuilt source by setting environment variable ROCKSDB_PREBUILT_BASE_URL"
    step_duration "Check prebuilt version"
    return 1
}

# Download RocksDB source code (if prebuilt is not available)
download_rocksdb() {
    step_start
    mkdir -p "$ROCKSDB_DIR"
    cd "$ROCKSDB_DIR"
    TARBALL_NAME="rocksdb-${ROCKSDB_VERSION}.tar.gz"
    ROCKSDB_URL="https://github.com/facebook/rocksdb/archive/refs/tags/v${ROCKSDB_VERSION}.tar.gz"

    # Check if source directory already exists and is not empty
    if [ -d "$ROCKSDB_SRC_DIR" ] && [ "$(ls -A "$ROCKSDB_SRC_DIR" 2>/dev/null)" ]; then
        echo_info "RocksDB source code already exists, skipping download and extraction"
        echo_info "Source directory: $ROCKSDB_SRC_DIR"
        step_duration "Check source code"
        return 0
    fi

    # Check if tarball already exists
    if [ -f "$TARBALL_NAME" ]; then
        echo_info "Found existing tarball: $TARBALL_NAME, skipping download"
        step_duration "Check tarball"
        # If tarball exists but source directory doesn't, continue with extraction logic
    else
        # Tarball doesn't exist, need to download
        echo_info "Downloading RocksDB v${ROCKSDB_VERSION} source code..."
        # Download tarball
        echo_info "Downloading from ${ROCKSDB_URL}..."
        if command -v curl >/dev/null 2>&1; then
            if curl -L -f -o "$TARBALL_NAME" "$ROCKSDB_URL" 2>/dev/null; then
                echo_info "Successfully downloaded RocksDB v${ROCKSDB_VERSION}"
            else
                echo_error "Download failed: ${ROCKSDB_URL}"
                echo_error "Please check network connection or manually download tarball to: $ROCKSDB_DIR/$TARBALL_NAME"
                exit 1
            fi
        elif command -v wget >/dev/null 2>&1; then
            if wget -O "$TARBALL_NAME" "$ROCKSDB_URL" 2>/dev/null; then
                echo_info "Successfully downloaded RocksDB v${ROCKSDB_VERSION}"
            else
                echo_error "Download failed: ${ROCKSDB_URL}"
                echo_error "Please check network connection or manually download tarball to: $ROCKSDB_DIR/$TARBALL_NAME"
                exit 1
            fi
        else
            echo_error "curl or wget not found, cannot download"
            echo_error "Please manually download ${ROCKSDB_URL} to $ROCKSDB_DIR/$TARBALL_NAME"
            exit 1
        fi
        step_duration "Download tarball"
    fi

    # If directory exists but is empty, clean it (before extraction)
    if [ -d "$ROCKSDB_SRC_DIR" ] && [ -z "$(ls -A "$ROCKSDB_SRC_DIR" 2>/dev/null)" ]; then
        echo_warn "Source directory exists but is empty, cleaning..."
        rm -rf "$ROCKSDB_SRC_DIR"
    fi

    # Check if already extracted (source directory exists)
    if [ -d "src" ] && [ "$(ls -A src 2>/dev/null)" ]; then
        echo_info "Source directory already exists, skipping extraction"
        echo_info "Source directory: $ROCKSDB_SRC_DIR"
        step_duration "Check extraction status"
    else
        # Try extracting tarball
        echo_info "Starting extraction of $TARBALL_NAME..."
        if tar -xzf "$TARBALL_NAME" 2>/dev/null; then
            if [ -d "rocksdb-${ROCKSDB_VERSION}" ]; then
                mv "rocksdb-${ROCKSDB_VERSION}" src
                echo_info "Successfully extracted RocksDB v${ROCKSDB_VERSION}"
                # Keep tarball after successful extraction (for next time use)
                echo_info "Tarball preserved: $TARBALL_NAME (can be used directly next time)"
                step_duration "Extract tarball"
            else
                echo_error "Expected directory not found after extraction: rocksdb-${ROCKSDB_VERSION}"
                echo_error "Tarball may be corrupted, deleted: $TARBALL_NAME"
                rm -f "$TARBALL_NAME"
                exit 1
            fi
        else
            echo_error "Extraction failed, tarball may be corrupted"
            # Only delete corrupted tarball when extraction fails
            rm -f "$TARBALL_NAME"
            echo_error "Deleted corrupted tarball: $TARBALL_NAME"
            exit 1
        fi
    fi

    step_duration "Download source code"
    echo_info "RocksDB download completed"
}

# Build RocksDB (Linux/macOS)
build_rocksdb_unix() {
    step_start
    echo_info "Starting to build RocksDB (Unix systems)..."
    cd "$ROCKSDB_SRC_DIR"
    
    # Detect CPU core count
    if [[ "$OS" == "darwin"* ]]; then
        CPU_COUNT=$(sysctl -n hw.ncpu 2>/dev/null || echo 4)
    else
        CPU_COUNT=$(nproc 2>/dev/null || echo 4)
    fi
    
    echo_info "Using ${CPU_COUNT} CPU cores for compilation"
    
    
    # Build static library with minimal features for size optimization
    # Enable ROCKSDB_LITE mode - removes non-core features to minimize size
    # Disable optional features to reduce library size:
    # - ROCKSDB_LITE=1: Enable lite mode (removes advanced features)
    # - WITH_JEMALLOC=0: Disable jemalloc (use system malloc)
    # - WITH_MD_LIBRARY=0: Disable MD library
    # - WITH_NUMA=0: Disable NUMA support
    # - WITH_TBB=0: Disable Intel TBB
    # - ROCKSDB_BUILD_SHARED=0: Don't build shared libraries
    # - WITH_TOOLS=0: Don't build tools
    # - WITH_EXAMPLES=0: Don't build examples
    # - WITH_TESTS=0: Don't build tests
    # Disable all compression algorithms to minimize size (only "none" compression available):
    # - USE_SNAPPY=0: Disable Snappy compression
    # - USE_LZ4=0: Disable LZ4 compression
    # - USE_ZSTD=0: Disable ZSTD compression
    # - USE_ZLIB=0: Disable Zlib compression
    # - USE_BZIP2=0: Disable BZip2 compression
    # - OPT="-Os": Optimize for size
    echo_info "Executing: make -j\"${CPU_COUNT}\" ROCKSDB_LITE=1 PORTABLE=1 USE_RTTI=1 DEBUG_LEVEL=0 DISABLE_WARNING_AS_ERROR=1 OPT=\"-Os\" WITH_JEMALLOC=0 WITH_MD_LIBRARY=0 WITH_NUMA=0 WITH_TBB=0 ROCKSDB_BUILD_SHARED=0 WITH_TOOLS=0 WITH_EXAMPLES=0 WITH_TESTS=0 USE_SNAPPY=0 USE_LZ4=0 USE_ZSTD=0 USE_ZLIB=0 USE_BZIP2=0 static_lib"
    
    if ! make -j"${CPU_COUNT}" ROCKSDB_LITE=1 PORTABLE=1 USE_RTTI=1 DEBUG_LEVEL=0 DISABLE_WARNING_AS_ERROR=1 OPT="-Os" WITH_JEMALLOC=0 WITH_MD_LIBRARY=0 WITH_NUMA=0 WITH_TBB=0 ROCKSDB_BUILD_SHARED=0 WITH_TOOLS=0 WITH_EXAMPLES=0 WITH_TESTS=0 USE_SNAPPY=0 USE_LZ4=0 USE_ZSTD=0 USE_ZLIB=0 USE_BZIP2=0 static_lib; then
        echo_error "❌ Build failed"
        echo_error "Debug information:"
        echo_error "  Working directory: $(pwd)"
        echo_error "  make_config.mk exists: $([ -f make_config.mk ] && echo 'yes' || echo 'no')"
        if [ -f "make_config.mk" ]; then
            echo_error "  make_config.mk content preview (first 30 lines):"
            head -30 make_config.mk | sed 's/^/    /'
        fi
        echo_error "  Compression library detection (in make_config.mk):"
        grep -E "SNAPPY|LZ4|ZSTD|snappy|lz4|zstd|-DSNAPPY|-DLZ4|-DZSTD" make_config.mk 2>/dev/null | head -10 | sed 's/^/    /' || echo "    Not found"
        exit 1
    fi
    
    # Note: ROCKSDB_LITE mode enabled - minimal features only
    if [ -f "$STATIC_LIB_NAME" ]; then
        echo_info "✅ Library file compiled successfully (ROCKSDB_LITE mode - minimal build)"
        echo_info "Note: ROCKSDB_LITE mode enabled - only core features available"
        echo_info "Note: Only 'none' compression type is available. Compression algorithms are disabled to reduce size."
    fi
    
    step_duration "Build source code"
    
    step_start
    # Create output directories
    mkdir -p "$ROCKSDB_DIR/lib" "$ROCKSDB_DIR/include"
    
    # Copy library file and strip debug symbols to reduce size
    if [ -f "$STATIC_LIB_NAME" ]; then
        cp "$STATIC_LIB_NAME" "$ROCKSDB_DIR/lib/"
        # Strip debug symbols to reduce library size
        if command -v strip >/dev/null 2>&1; then
            echo_info "Stripping debug symbols to reduce library size..."
            strip "$ROCKSDB_DIR/lib/$STATIC_LIB_NAME" 2>/dev/null || {
                echo_warn "⚠️  Failed to strip library, continuing anyway"
            }
        fi
        echo_info "Copied static library to $ROCKSDB_DIR/lib/$STATIC_LIB_NAME"
    else
        echo_error "Build failed: $STATIC_LIB_NAME not found"
        exit 1
    fi
    
    # Copy header files
    if [ -d "include" ]; then
        cp -r include/* "$ROCKSDB_DIR/include/"
        echo_info "Copied header files to $ROCKSDB_DIR/include"
    else
        echo_error "Build failed: include directory not found"
        exit 1
    fi
    
    step_duration "Package files"
    
    # Check if library files were successfully generated, only clean source code after confirming binaries are generated
    if [ -f "$ROCKSDB_DIR/lib/$STATIC_LIB_NAME" ] && [ -d "$ROCKSDB_DIR/include" ]; then
        step_start
        echo_info "✅ Binaries successfully generated, cleaning source code and build files..."
        # Clean source directory and build intermediate files (preserve copied library and header files)
        cd "$ROCKSDB_SRC_DIR"
        # Clean compilation intermediate files (optional, saves space)
        if [ -f "Makefile" ]; then
            make clean > /dev/null 2>&1 || true
        fi
        # Clean entire source directory
        cd "$ROCKSDB_DIR"
        if [ -d "src" ]; then
            rm -rf "src"
            echo_info "Source directory cleaned"
        fi
        step_duration "Clean source code"
    else
        echo_warn "⚠️  Library files not successfully generated, keeping source directory for debugging"
        echo_warn "Expected library file location: $ROCKSDB_DIR/lib/$STATIC_LIB_NAME"
        echo_warn "Expected header file location: $ROCKSDB_DIR/include"
    fi
    
    echo_info "RocksDB build completed"
    echo_info "Library file location: $ROCKSDB_DIR/lib/$STATIC_LIB_NAME"
    echo_info "Header file location: $ROCKSDB_DIR/include"
}

# Build RocksDB
build_rocksdb() {
    # Check if already built (binary files exist)
    # If binary files already exist, skip compilation and don't clean source code
    if [ -f "$ROCKSDB_DIR/lib/$STATIC_LIB_NAME" ] && [ -d "$ROCKSDB_DIR/include" ]; then
        echo_info "✅ RocksDB binaries already exist, skipping build step"
        echo_info "Library file: $ROCKSDB_DIR/lib/$STATIC_LIB_NAME"
        echo_info "Header files: $ROCKSDB_DIR/include"
        if [ -d "$ROCKSDB_SRC_DIR" ]; then
            echo_info "Source directory preserved: $ROCKSDB_SRC_DIR (not cleaned because binaries exist)"
        fi
        echo_info "To rebuild, please delete $ROCKSDB_DIR/lib directory"
        return 0
    fi
    
    # Try to download prebuilt version first
    if download_prebuilt_rocksdb; then
        echo_info "✅ Successfully using prebuilt RocksDB binaries"
        return 0
    fi
    
    # If no prebuilt version, compile from source
    echo_info "Starting to build RocksDB from source..."
    
    # Ensure source code is downloaded (skip download if already exists)
    download_rocksdb
    
    # Check if source directory exists
    if [ ! -d "$ROCKSDB_SRC_DIR" ] || [ -z "$(ls -A "$ROCKSDB_SRC_DIR" 2>/dev/null)" ]; then
        echo_error "RocksDB source directory does not exist or is empty: $ROCKSDB_SRC_DIR"
        echo_error "Please check the download process or manually download the source code"
        exit 1
    fi
    
    build_rocksdb_unix
}

# Clean (complete cleanup, including tarball)
clean() {
    if [ -d "$ROCKSDB_DIR" ]; then
        echo_info "Cleaning RocksDB build files (including tarball)..."
        rm -rf "$ROCKSDB_DIR"
        echo_info "Cleanup completed"
        echo_info "Cleaned: binary library files, header file directory, source directory, tarball"
    else
        echo_info "RocksDB build files do not exist, no cleanup needed"
    fi
}

# Clean intermediate files (keep binaries, clean source code and tarball)
# For daily development use - keeps include directory for future builds
clean_intermediate() {
    TARBALL_NAME="rocksdb-${ROCKSDB_VERSION}.tar.gz"
    TARBALL_PATH="$ROCKSDB_DIR/$TARBALL_NAME"
    
    if [ ! -d "$ROCKSDB_DIR" ] && [ ! -d "$ROCKSDB_DIR/src" ] && [ ! -f "$TARBALL_PATH" ]; then
        echo_info "RocksDB intermediate files do not exist, no cleanup needed"
        return 0
    fi
    
    echo_info "Cleaning RocksDB intermediate files (keeping binaries and headers for development)..."
    
    # Clean tarball
    if [ -f "$TARBALL_PATH" ]; then
        echo_info "Cleaning tarball: $TARBALL_NAME"
        rm -f "$TARBALL_PATH"
    fi
    
    # Clean source directory
    if [ -d "$ROCKSDB_DIR/src" ]; then
        echo_info "Cleaning source directory: $ROCKSDB_DIR/src"
        rm -rf "$ROCKSDB_DIR/src"
    fi
    
    echo_info "Intermediate files cleanup completed"
    echo_info "Cleaned: source directory, tarball"
    echo_info "Preserved: binary library file (librocksdb.a), header file directory (for development)"
}

# Clean for packaging (also clean include directory)
# Use this when building final binary package - removes include as it's not needed in package
clean_for_package() {
    TARBALL_NAME="rocksdb-${ROCKSDB_VERSION}.tar.gz"
    TARBALL_PATH="$ROCKSDB_DIR/$TARBALL_NAME"
    
    echo_info "Cleaning RocksDB files for packaging..."
    
    # Clean tarball
    if [ -f "$TARBALL_PATH" ]; then
        echo_info "Cleaning tarball: $TARBALL_NAME"
        rm -f "$TARBALL_PATH"
    fi
    
    # Clean source directory
    if [ -d "$ROCKSDB_DIR/src" ]; then
        echo_info "Cleaning source directory: $ROCKSDB_DIR/src"
        rm -rf "$ROCKSDB_DIR/src"
    fi
    
    # Clean include directory (not needed in final package)
    if [ -d "$ROCKSDB_DIR/include" ]; then
        echo_info "Cleaning include directory: $ROCKSDB_DIR/include"
        rm -rf "$ROCKSDB_DIR/include"
    fi
    
    echo_info "Package cleanup completed"
    echo_info "Cleaned: source directory, tarball, include directory"
    echo_info "Preserved: binary library file (librocksdb.a)"
}

# Main function
main() {
    cd "$PROJECT_ROOT"
    
    echo_info "Detected operating system: $OS"
    
    case "${1:-build}" in
        build)
            start_timer
            
            step_start
            echo_info "Detected operating system: $OS"
            step_duration "Initialization"
            
            step_start
            check_dependencies
            step_duration "Check dependencies"
            
            build_rocksdb
            
            echo_info "✅ RocksDB build completed!"
            echo_info "Library file: $ROCKSDB_DIR/lib/$STATIC_LIB_NAME"
            echo_info "Header files: $ROCKSDB_DIR/include"
            
            total_duration
            ;;
        clean)
            clean
            ;;
        clean-intermediate)
            clean_intermediate
            ;;
        clean-for-package)
            clean_for_package
            ;;
        *)
            echo "Usage: $0 [build|clean|clean-intermediate|clean-for-package]"
            echo "  build: Download and build RocksDB (default)"
            echo "  clean: Clean downloaded and compiled files (including tarball)"
            echo "  clean-intermediate: Clean intermediate files (keep binaries and headers for development)"
            echo "  clean-for-package: Clean for packaging (remove include directory, keep only binaries)"
            exit 1
            ;;
    esac
}

main "$@"
