# Copyright (c) Facebook, Inc. and its affiliates.
#
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
.PHONY: all cmake build clean debug release unit

BUILD_BASE_DIR=_build
BUILD_DIR=release
BUILD_TYPE=Release
BENCHMARKS_BASIC_DIR=$(BUILD_BASE_DIR)/$(BUILD_DIR)/velox/benchmarks/basic/
BENCHMARKS_DUMP_DIR=dumps
TREAT_WARNINGS_AS_ERRORS ?= 1
ENABLE_WALL ?= 1

# Option to make a minimal build. By default set to "OFF"; set to
# "ON" to only build a minimal set of components. This may override
# other build options
VELOX_BUILD_MINIMAL ?= "OFF"

# Control whether to build unit tests. By default set to "ON"; set to
# "OFF" to disable.
VELOX_BUILD_TESTING ?= "ON"

CMAKE_FLAGS := -DTREAT_WARNINGS_AS_ERRORS=${TREAT_WARNINGS_AS_ERRORS}
CMAKE_FLAGS += -DENABLE_ALL_WARNINGS=${ENABLE_WALL}

CMAKE_FLAGS += -DVELOX_BUILD_MINIMAL=${VELOX_BUILD_MINIMAL}
CMAKE_FLAGS += -DVELOX_BUILD_TESTING=${VELOX_BUILD_TESTING}

CMAKE_FLAGS += -DCMAKE_BUILD_TYPE=$(BUILD_TYPE)

ifdef AWSSDK_ROOT_DIR
CMAKE_FLAGS += -DAWSSDK_ROOT_DIR=$(AWSSDK_ROOT_DIR)
endif

# Use Ninja if available. If Ninja is used, pass through parallelism control flags.
USE_NINJA ?= 1
ifeq ($(USE_NINJA), 1)
ifneq ($(shell which ninja), )
GENERATOR=-GNinja -DMAX_LINK_JOBS=$(MAX_LINK_JOBS) -DMAX_HIGH_MEM_JOBS=$(MAX_HIGH_MEM_JOBS)
endif
endif

ifndef USE_CCACHE
ifneq ($(shell which ccache), )
USE_CCACHE=-DCMAKE_CXX_COMPILER_LAUNCHER=ccache
endif
endif

NUM_THREADS ?= $(shell getconf _NPROCESSORS_CONF 2>/dev/null || echo 1)
CPU_TARGET ?= "avx"

all: release			#: Build the release version

clean:					#: Delete all build artifacts
	rm -rf $(BUILD_BASE_DIR)

cmake:					#: Use CMake to create a Makefile build system
	mkdir -p $(BUILD_BASE_DIR)/$(BUILD_DIR) && \
	cmake -B \
		"$(BUILD_BASE_DIR)/$(BUILD_DIR)" \
		${CMAKE_FLAGS} \
		$(GENERATOR) \
		$(USE_CCACHE) \
		$(FORCE_COLOR) \
		${EXTRA_CMAKE_FLAGS}

build:					#: Build the software based in BUILD_DIR and BUILD_TYPE variables
	cmake --build $(BUILD_BASE_DIR)/$(BUILD_DIR) -j ${NUM_THREADS}

debug:					#: Build with debugging symbols
	$(MAKE) cmake BUILD_DIR=debug BUILD_TYPE=Debug
	$(MAKE) build BUILD_DIR=debug

release:				#: Build the release version
	$(MAKE) cmake BUILD_DIR=release BUILD_TYPE=Release && \
	$(MAKE) build BUILD_DIR=release

min_debug:				#: Minimal build with debugging symbols
	$(MAKE) cmake BUILD_DIR=debug BUILD_TYPE=debug EXTRA_CMAKE_FLAGS=-DVELOX_BUILD_MINIMAL=ON
	$(MAKE) build BUILD_DIR=debug

benchmarks-basic-build:
	$(MAKE) release EXTRA_CMAKE_FLAGS="-DVELOX_BUILD_MINIMAL=ON -DVELOX_ENABLE_BENCHMARKS_BASIC=ON"

benchmarks-basic-run:
	$(MAKE) benchmarks-basic-build
	scripts/benchmark-runner.py run \
		--path $(BENCHMARKS_BASIC_DIR) ${EXTRA_BENCHMARK_FLAGS}

benchmarks-basic-dump:
	$(MAKE) benchmarks-basic-run EXTRA_BENCHMARK_FLAGS="--dump-path ${BENCHMARKS_DUMP_DIR}"

unittest: debug			#: Build with debugging and run unit tests
	cd $(BUILD_BASE_DIR)/debug && ctest -j ${NUM_THREADS} -VV --output-on-failure

fuzzertest: debug		#: Build with debugging and run expression fuzzer test.
	$(BUILD_BASE_DIR)/debug/velox/expression/tests/velox_expression_fuzzer_test --steps 100000 --logtostderr=1 --minloglevel=0

format-fix: 			#: Fix formatting issues in the current branch
	scripts/check.py format branch --fix

format-check: 			#: Check for formatting issues on the current branch
	clang-format --version
	scripts/check.py format branch

header-fix:				#: Fix license header issues in the current branch
	scripts/check.py header branch --fix

header-check:			#: Check for license header issues on the current branch
	scripts/check.py header branch

circleci-container:			#: Build the linux container for CircleCi
	$(MAKE) linux-container CONTAINER_NAME=circleci

check-container:
	$(MAKE) linux-container CONTAINER_NAME=check

velox-torcharrow-container:
	$(MAKE) linux-container CONTAINER_NAME=velox-torcharrow

linux-container:
	rm -rf /tmp/docker && \
	mkdir -p /tmp/docker && \
	cp scripts/setup-helper-functions.sh scripts/setup-$(CONTAINER_NAME).sh scripts/$(CONTAINER_NAME)-container.dockfile /tmp/docker && \
	cd /tmp/docker && \
	docker build --build-arg cpu_target=$(CPU_TARGET) --tag "prestocpp/velox-$(CPU_TARGET)-$(CONTAINER_NAME):${USER}-$(shell date +%Y%m%d)" -f $(CONTAINER_NAME)-container.dockfile .

help:					#: Show the help messages
	@cat $(firstword $(MAKEFILE_LIST)) | \
	awk '/^[-a-z]+:/' | \
	awk -F: '{ printf("%-20s   %s\n", $$1, $$NF) }'
