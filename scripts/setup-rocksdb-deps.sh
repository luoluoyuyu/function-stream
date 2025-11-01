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

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

echo_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

echo_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Detect operating system
detect_os() {
    case "$(uname -s)" in
        Darwin*)
            echo "darwin"
            ;;
        Linux*)
            echo "linux"
            ;;
        *)
            echo "unknown"
            ;;
    esac
}

# Detect CPU architecture
detect_arch() {
    local arch=$(uname -m)
    case "$arch" in
        x86_64|amd64)
            echo "x86_64"
            ;;
        arm64|aarch64)
            echo "arm64"
            ;;
        armv7*)
            echo "armv7"
            ;;
        arm*)
            echo "arm"
            ;;
        *)
            echo "$arch"
            ;;
    esac
}

# Detect Homebrew path (based on architecture)
detect_homebrew_prefix() {
    if [ -d "/opt/homebrew" ]; then
        # Apple Silicon (arm64)
        echo "/opt/homebrew"
    elif [ -d "/usr/local" ]; then
        # Intel Mac (x86_64) or others
        echo "/usr/local"
    else
        echo ""
    fi
}

# Detect Linux library path (based on architecture)
detect_linux_lib_dir() {
    local arch=$(detect_arch)
    case "$arch" in
        x86_64)
            if [ -d "/usr/lib/x86_64-linux-gnu" ]; then
                echo "/usr/lib/x86_64-linux-gnu"
            elif [ -d "/usr/lib64" ]; then
                echo "/usr/lib64"
            else
                echo "/usr/lib"
            fi
            ;;
        arm64|aarch64)
            if [ -d "/usr/lib/aarch64-linux-gnu" ]; then
                echo "/usr/lib/aarch64-linux-gnu"
            elif [ -d "/usr/lib64" ]; then
                echo "/usr/lib64"
            else
                echo "/usr/lib"
            fi
            ;;
        armv7|arm)
            if [ -d "/usr/lib/arm-linux-gnueabihf" ]; then
                echo "/usr/lib/arm-linux-gnueabihf"
            elif [ -d "/usr/lib/arm-linux-gnueabi" ]; then
                echo "/usr/lib/arm-linux-gnueabi"
            else
                echo "/usr/lib"
            fi
            ;;
        *)
            echo "/usr/lib"
            ;;
    esac
}

# Check if library exists
check_library() {
    local lib_name=$1
    local lib_path=$2
    
    if [ -f "$lib_path" ] || [ -d "$lib_path" ]; then
        return 0
    else
        return 1
    fi
}

# Install dependencies on macOS (using Homebrew)
# Note: Compression libraries are disabled, so no dependencies needed for minimal build
install_deps_darwin() {
    echo_info "Checking macOS dependencies..."
    echo_info "Note: Compression libraries are disabled in minimal build, no dependencies required"
    echo_info "✅ No dependencies needed (compression disabled)"
    return 0
}

# Install dependencies on Linux (using package manager)
# Note: Compression libraries are disabled, so no dependencies needed for minimal build
install_deps_linux() {
    echo_info "Checking Linux dependencies..."
    echo_info "Note: Compression libraries are disabled in minimal build, no dependencies required"
    echo_info "✅ No dependencies needed (compression disabled)"
    return 0
}

# Generate CGO_LDFLAGS
# Note: Even though compression is disabled, RocksDB's compressed_secondary_cache still needs bzip2 and zlib
# These libraries are statically linked to avoid runtime dependencies
generate_cgo_ldflags() {
    local os=$(detect_os)
    local rocksdb_lib_dir=$1
    local output_file=$2
    local lib_dir=""
    
    if [ "$os" = "darwin" ]; then
        # macOS: Statically link rocksdb, link bz2 and z as system libraries
        # Note: Using grocksdb_no_link tag, so we need to provide all necessary libraries
        # Note: macOS system libraries (bz2, z) are available on all macOS systems, so dynamic linking is acceptable
        # Note: RocksDB is statically linked by directly specifying the .a file
        # Note: Using -Wl,-force_load to ensure all symbols from static library are included
        local rocksdb_static="${rocksdb_lib_dir}/librocksdb.a"
        if [ -f "$rocksdb_static" ]; then
            # Force load static library to ensure all symbols are included
            # Link system libraries (bz2, z) - these are available on all macOS systems
            # Use -Wl,-dead_strip to remove unused code
            echo "-Wl,-force_load,${rocksdb_static} -pthread -lstdc++ -ldl -lbz2 -lz" > "$output_file"
        else
            # Fallback: use library path (should not happen if setup-rocksdb completed)
            echo "-L${rocksdb_lib_dir} -lrocksdb -pthread -lstdc++ -ldl -lbz2 -lz" > "$output_file"
        fi
    elif [ "$os" = "linux" ]; then
        # Linux: Use static linking for bz2 and z, keep rocksdb as static library
        # Use -static flag to force static linking of system libraries
        lib_dir=$(detect_linux_lib_dir)
        # Try to find static library paths
        local bz2_static=""
        local z_static=""
        
        # Find static library paths
        if [ -f "${lib_dir}/libbz2.a" ]; then
            bz2_static="${lib_dir}/libbz2.a"
        elif [ -f "/usr/lib/x86_64-linux-gnu/libbz2.a" ]; then
            bz2_static="/usr/lib/x86_64-linux-gnu/libbz2.a"
        elif [ -f "/usr/lib64/libbz2.a" ]; then
            bz2_static="/usr/lib64/libbz2.a"
        elif [ -f "/usr/lib/libbz2.a" ]; then
            bz2_static="/usr/lib/libbz2.a"
        fi
        
        if [ -f "${lib_dir}/libz.a" ]; then
            z_static="${lib_dir}/libz.a"
        elif [ -f "/usr/lib/x86_64-linux-gnu/libz.a" ]; then
            z_static="/usr/lib/x86_64-linux-gnu/libz.a"
        elif [ -f "/usr/lib64/libz.a" ]; then
            z_static="/usr/lib64/libz.a"
        elif [ -f "/usr/lib/libz.a" ]; then
            z_static="/usr/lib/libz.a"
        fi
        
        # Force static linking of rocksdb by directly specifying the static library file
        local rocksdb_static="${rocksdb_lib_dir}/librocksdb.a"
        
        # Build link flags with static libraries if available, otherwise use -static flag
        # Note: Using grocksdb_no_link tag, so we need to provide all necessary libraries
        # Note: On Linux, we can fully static link all libraries for portability
        if [ -n "$bz2_static" ] && [ -n "$z_static" ]; then
            # Use direct static library paths for all libraries - fully static binary
            if [ -f "$rocksdb_static" ]; then
                # Directly link all static library files
                # This creates a fully static binary that can run on any Linux system
                # Add necessary system libraries: pthread, stdc++, dl
                echo "${rocksdb_static} ${bz2_static} ${z_static} -static -pthread -lstdc++ -ldl" > "$output_file"
            else
                # RocksDB static library not found, use library path with static linking
                if [ -d "$lib_dir" ] && [ "$lib_dir" != "/usr/lib" ]; then
                    echo "-L${rocksdb_lib_dir} -L${lib_dir} -Wl,-Bstatic -lrocksdb ${bz2_static} ${z_static} -Wl,-Bdynamic -static -pthread -lstdc++ -ldl" > "$output_file"
                else
                    echo "-L${rocksdb_lib_dir} -Wl,-Bstatic -lrocksdb ${bz2_static} ${z_static} -Wl,-Bdynamic -static -pthread -lstdc++ -ldl" > "$output_file"
                fi
            fi
        else
            # Static libraries not found, use -static flag for full static linking
            if [ -f "$rocksdb_static" ]; then
                # RocksDB is static, use -static for system libraries (fully static binary)
                if [ -d "$lib_dir" ] && [ "$lib_dir" != "/usr/lib" ]; then
                    echo "${rocksdb_static} -L${lib_dir} -static -lbz2 -lz -pthread -lstdc++ -ldl" > "$output_file"
                else
                    echo "${rocksdb_static} -static -lbz2 -lz -pthread -lstdc++ -ldl" > "$output_file"
                fi
            else
                # Fall back to -static flag for static linking (fully static binary)
                if [ -d "$lib_dir" ] && [ "$lib_dir" != "/usr/lib" ]; then
                    echo "-L${rocksdb_lib_dir} -L${lib_dir} -Wl,-Bstatic -lrocksdb -lbz2 -lz -Wl,-Bdynamic -static -pthread -lstdc++ -ldl" > "$output_file"
                else
                    echo "-L${rocksdb_lib_dir} -Wl,-Bstatic -lrocksdb -lbz2 -lz -Wl,-Bdynamic -static -pthread -lstdc++ -ldl" > "$output_file"
                fi
            fi
        fi
    else
        echo_error "Unsupported operating system: $os"
        exit 1
    fi
}

# Main function
main() {
    local os=$(detect_os)
    
    case "$1" in
        install)
            echo_info "Starting RocksDB dependencies installation..."
            if [ "$os" = "darwin" ]; then
                install_deps_darwin
            elif [ "$os" = "linux" ]; then
                install_deps_linux
            else
                echo_error "Unsupported operating system: $os"
                exit 1
            fi
            ;;
        check)
            echo_info "Checking dependencies status..."
            if [ "$os" = "darwin" ]; then
                install_deps_darwin
            elif [ "$os" = "linux" ]; then
                install_deps_linux
            else
                echo_error "Unsupported operating system: $os"
                exit 1
            fi
            ;;
        ldflags)
            if [ -z "$2" ] || [ -z "$3" ]; then
                echo_error "Usage: $0 ldflags <rocksdb_lib_dir> <output_file>"
                exit 1
            fi
            generate_cgo_ldflags "$2" "$3"
            ;;
        *)
            echo "Usage: $0 {install|check|ldflags} [args...]"
            echo ""
            echo "Commands:"
            echo "  install        - Install missing dependencies"
            echo "  check          - Check and install missing dependencies"
            echo "  ldflags <dir> <file> - Generate CGO_LDFLAGS and save to file"
            exit 1
            ;;
    esac
}

main "$@"

