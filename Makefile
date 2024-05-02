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

ifdef GCSSDK_ROOT_DIR
CMAKE_FLAGS += -DGCSSDK_ROOT_DIR=$(GCSSDK_ROOT_DIR)
endif

ifdef AZURESDK_ROOT_DIR
CMAKE_FLAGS += -DAZURESDK_ROOT_DIR=$(AZURESDK_ROOT_DIR)
endif

ifdef CUDA_ARCHITECTURES
CMAKE_FLAGS += -DCMAKE_CUDA_ARCHITECTURES="$(CUDA_ARCHITECTURES)"
endif

ifdef CUDA_COMPILER
CMAKE_FLAGS += -DCMAKE_CUDA_COMPILER="$(CUDA_COMPILER)"
endif

ifdef CUDA_FLAGS
CMAKE_FLAGS += -DCMAKE_CUDA_FLAGS="$(CUDA_FLAGS)"
endif

# Use Ninja if available. If Ninja is used, pass through parallelism control flags.
USE_NINJA ?= 1
ifeq ($(USE_NINJA), 1)
ifneq ($(shell which ninja), )
GENERATOR := -GNinja
GENERATOR += -DMAX_LINK_JOBS=$(MAX_LINK_JOBS)
GENERATOR += -DMAX_HIGH_MEM_JOBS=$(MAX_HIGH_MEM_JOBS)

# Ninja makes compilers disable colored output by default.
GENERATOR += -DVELOX_FORCE_COLORED_OUTPUT=ON
endif
endif

NUM_THREADS ?= $(shell getconf _NPROCESSORS_CONF 2>/dev/null || echo 1)
CPU_TARGET ?= "avx"

FUZZER_SEED ?= 123456
FUZZER_DURATION_SEC ?= 60

PYTHON_EXECUTABLE ?= $(shell which python)

all: release			#: Build the release version

clean:					#: Delete all build artifacts
	rm -rf $(BUILD_BASE_DIR)

cmake:					#: Use CMake to create a Makefile build system
	mkdir -p $(BUILD_BASE_DIR)/$(BUILD_DIR) && \
	cmake  -B \
		"$(BUILD_BASE_DIR)/$(BUILD_DIR)" \
		${CMAKE_FLAGS} \
		$(GENERATOR) \
		${EXTRA_CMAKE_FLAGS}

cmake-gpu:
	$(MAKE) EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS} -DVELOX_ENABLE_GPU=ON" cmake

build:					#: Build the software based in BUILD_DIR and BUILD_TYPE variables
	cmake --build $(BUILD_BASE_DIR)/$(BUILD_DIR) -j $(NUM_THREADS)

debug:					#: Build with debugging symbols
	$(MAKE) cmake BUILD_DIR=debug BUILD_TYPE=Debug
	$(MAKE) build BUILD_DIR=debug -j ${NUM_THREADS}

release:				#: Build the release version
	$(MAKE) cmake BUILD_DIR=release BUILD_TYPE=Release && \
	$(MAKE) build BUILD_DIR=release

minimal_debug:			#: Minimal build with debugging symbols
	$(MAKE) cmake BUILD_DIR=debug BUILD_TYPE=debug EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS} -DVELOX_BUILD_MINIMAL=ON"
	$(MAKE) build BUILD_DIR=debug

min_debug: minimal_debug

minimal:				 #: Minimal build
	$(MAKE) cmake BUILD_DIR=release BUILD_TYPE=release EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS} -DVELOX_BUILD_MINIMAL=ON"
	$(MAKE) build BUILD_DIR=release

gpu:						 #: Build with GPU support
	$(MAKE) cmake BUILD_DIR=release BUILD_TYPE=release EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS} -DVELOX_ENABLE_GPU=ON"
	$(MAKE) build BUILD_DIR=release

gpu_debug:			 #: Build with debugging symbols and GPU support
	$(MAKE) cmake BUILD_DIR=debug BUILD_TYPE=debug EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS} -DVELOX_ENABLE_GPU=ON"
	$(MAKE) build BUILD_DIR=debug

dwio:						#: Minimal build with dwio enabled.
	$(MAKE) cmake BUILD_DIR=release BUILD_TYPE=release EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS} \
																										    							  -DVELOX_BUILD_MINIMAL_WITH_DWIO=ON"
	$(MAKE) build BUILD_DIR=release

dwio_debug:			#: Minimal build with dwio debugging symbols.
	$(MAKE) cmake BUILD_DIR=debug BUILD_TYPE=debug EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS} \
																																	  -DVELOX_BUILD_MINIMAL_WITH_DWIO=ON"
	$(MAKE) build BUILD_DIR=debug

benchmarks-basic-build:
	$(MAKE) release EXTRA_CMAKE_FLAGS=" ${EXTRA_CMAKE_FLAGS} \
                                            -DVELOX_BUILD_TESTING=OFF \
                                            -DVELOX_ENABLE_BENCHMARKS_BASIC=ON"

benchmarks-build:
	$(MAKE) release EXTRA_CMAKE_FLAGS=" ${EXTRA_CMAKE_FLAGS} \
                                            -DVELOX_BUILD_TESTING=OFF \
                                            -DVELOX_ENABLE_BENCHMARKS=ON"

benchmarks-basic-run:
	scripts/benchmark-runner.py run \
			--bm_estimate_time \
			--bm_max_secs 10 \
			--bm_max_trials 10000 \
			${EXTRA_BENCHMARK_FLAGS}

unittest: debug			#: Build with debugging and run unit tests
	cd $(BUILD_BASE_DIR)/debug && ctest -j ${NUM_THREADS} -VV --output-on-failure

# Build with debugging and run expression fuzzer test. Use a fixed seed to
# ensure the tests are reproducible.
fuzzertest: debug
	$(BUILD_BASE_DIR)/debug/velox/expression/fuzzer/velox_expression_fuzzer_test \
			--seed $(FUZZER_SEED) \
			--duration_sec $(FUZZER_DURATION_SEC) \
			--repro_persist_path $(FUZZER_REPRO_PERSIST_PATH) \
			--logtostderr=1 \
			--minloglevel=0

format-fix: 			#: Fix formatting issues in the main branch
	scripts/check.py format main --fix

format-check: 			#: Check for formatting issues on the main branch
	clang-format --version
	scripts/check.py format main

header-fix:				#: Fix license header issues in the current branch
	scripts/check.py header main --fix

header-check:			#: Check for license header issues on the main branch
	scripts/check.py header main

circleci-container:			#: Build the linux container for CircleCi
	$(MAKE) linux-container CONTAINER_NAME=circleci

check-container:
	$(MAKE) linux-container CONTAINER_NAME=check

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

python-clean:
	DEBUG=1 ${PYTHON_EXECUTABLE} setup.py clean

python-build:
	DEBUG=1 CMAKE_BUILD_PARALLEL_LEVEL=${NUM_THREADS} ${PYTHON_EXECUTABLE} -m pip install -e .$(extras) --verbose

python-test:
	$(MAKE) python-build extras="[tests]"
	DEBUG=1 ${PYTHON_EXECUTABLE} -m unittest -v
