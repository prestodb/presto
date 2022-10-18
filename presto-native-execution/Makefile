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
.PHONY: all cmake build clean debug release unit submodules velox-submodule

BUILD_BASE_DIR=_build
BUILD_DIR=release
BUILD_TYPE=Release
TREAT_WARNINGS_AS_ERRORS ?= 1
ENABLE_WALL ?= 1
NUM_THREADS ?= $(shell getconf _NPROCESSORS_CONF 2>/dev/null || echo 1)
CPU_TARGET ?= "avx"
CMAKE_PREFIX_PATH ?= "/usr/local"

PRESTO_ENABLE_PARQUET ?= "OFF"

CMAKE_FLAGS := -DTREAT_WARNINGS_AS_ERRORS=${TREAT_WARNINGS_AS_ERRORS}
CMAKE_FLAGS += -DENABLE_ALL_WARNINGS=${ENABLE_WALL}
CMAKE_FLAGS += -DCMAKE_PREFIX_PATH=$(CMAKE_PREFIX_PATH)
CMAKE_FLAGS += -DCMAKE_BUILD_TYPE=$(BUILD_TYPE)
CMAKE_FLAGS += -DPRESTO_ENABLE_PARQUET=$(PRESTO_ENABLE_PARQUET)

SHELL := /bin/bash

# Use Ninja if available. If Ninja is used, pass through parallelism control flags.
USE_NINJA ?= 1
ifeq ($(USE_NINJA), 1)
ifneq ($(shell which ninja), )
CMAKE_FLAGS += -GNinja -DMAX_LINK_JOBS=$(MAX_LINK_JOBS) -DMAX_HIGH_MEM_JOBS=$(MAX_HIGH_MEM_JOBS)
endif
endif

ifndef USE_CCACHE
ifneq ($(shell which ccache), )
CMAKE_FLAGS += -DCMAKE_CXX_COMPILER_LAUNCHER=ccache
endif
endif

all: release			#: Build the release version

clean:					#: Delete all build artifacts
	rm -rf $(BUILD_BASE_DIR)

velox-submodule:		#: Check out code for velox submodule
	git submodule sync --recursive
	git submodule update --init --recursive

submodules: velox-submodule

cmake: submodules		#: Use CMake to create a Makefile build system
	cmake -B "$(BUILD_BASE_DIR)/$(BUILD_DIR)" $(FORCE_COLOR) $(CMAKE_FLAGS)

build:					#: Build the software based in BUILD_DIR and BUILD_TYPE variables
	cmake --build $(BUILD_BASE_DIR)/$(BUILD_DIR) -j $(NUM_THREADS)

debug:					#: Build with debugging symbols
	$(MAKE) cmake BUILD_DIR=debug BUILD_TYPE=Debug
	$(MAKE) build BUILD_DIR=debug

release:				#: Build the release version
	$(MAKE) cmake BUILD_DIR=release BUILD_TYPE=Release && \
	$(MAKE) build BUILD_DIR=release

unittest: debug			#: Build with debugging and run unit tests
	cd $(BUILD_BASE_DIR)/debug && ctest -j $(NUM_THREADS) -VV --output-on-failure --exclude-regex velox.*

presto_protocol:		#: Build the presto_protocol serde library
	cd presto_cpp/presto_protocol; $(MAKE) presto_protocol

TypeSignature:		#: Build the Presto TypeSignature parser
	cd presto_cpp/main/types; $(MAKE) TypeSignature

format-fix: 			#: Fix formatting issues in the master branch
	pushd "$$(pwd)/.." && presto-native-execution/velox/scripts/check.py format master --fix && popd

format-check: 			#: Check for formatting issues on the master branch
	clang-format --version
	pushd "$$(pwd)/.." && presto-native-execution/velox/scripts/check.py format master && popd

header-fix:				#: Fix license header issues in the master branch
	pushd "$$(pwd)/.." && presto-native-execution/velox/scripts/check.py header master --fix && popd

header-check:			#: Check for license header issues on the man branch
	pushd "$$(pwd)/.." && presto-native-execution/velox/scripts/check.py header master && popd

tidy-fix: cmake			#: Fix clang-tidy issues in the master branch
	pushd "$$(pwd)/.." && presto-native-execution/velox/scripts/check.py tidy master --fix && popd

tidy-check: cmake		#: Check clang-tidy issues in the master branch
	pushd "$$(pwd)/.." && presto-native-execution/velox/scripts/check.py tidy master && popd

centos-container:			#: Build the linux container for CircleCi
	$(MAKE) linux-container CONTAINER_NAME=centos

linux-container:
	rm -rf /tmp/docker && \
	mkdir -p /tmp/docker && \
	cp scripts/setup-$(CONTAINER_NAME).sh scripts/$(CONTAINER_NAME)-container.dockfile velox/scripts/setup-helper-functions.sh /tmp/docker && \
	cd /tmp/docker && \
	docker build --build-arg cpu_target=$(CPU_TARGET) --tag "prestocpp/prestocpp-$(CPU_TARGET)-$(CONTAINER_NAME):$(USER)-$(shell date +%Y%m%d)" -f $(CONTAINER_NAME)-container.dockfile .

help:					#: Show the help messages
	@cat $(firstword $(MAKEFILE_LIST)) | \
	awk '/^[-a-z]+:/' | \
	awk -F: '{ printf("%-20s   %s\n", $$1, $$NF) }'
