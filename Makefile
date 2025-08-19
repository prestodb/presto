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

SHELL=/bin/bash
BUILD_BASE_DIR=_build
BUILD_DIR=release
BUILD_TYPE=Release
BENCHMARKS_BASIC_DIR=$(BUILD_BASE_DIR)/$(BUILD_DIR)/velox/benchmarks/basic/
BENCHMARKS_DUMP_DIR=dumps
TREAT_WARNINGS_AS_ERRORS ?= 1
ENABLE_WALL ?= 1
PYTHON_VENV ?= .venv
PIP ?= $(shell command -v uv > /dev/null 2>&1 && echo "uv pip" || echo "python3 -m pip")
VENV ?= $(shell command -v uv > /dev/null 2>&1 && echo "uv venv" || echo "python3 -m venv")

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
else
EXTRA_CMAKE_CUDA_FLAGS = -DCMAKE_CUDA_ARCHITECTURES="native"
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

PYTHON_EXECUTABLE ?= $(shell which python3)

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

cmake-wave:
	$(MAKE) EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS} -DVELOX_ENABLE_WAVE=ON ${EXTRA_CMAKE_CUDA_FLAGS}" cmake

cmake-cudf:
	$(MAKE) EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS} -DVELOX_ENABLE_CUDF=ON ${EXTRA_CMAKE_CUDA_FLAGS}" cmake

build:					#: Build the software based in BUILD_DIR and BUILD_TYPE variables
	cmake --build $(BUILD_BASE_DIR)/$(BUILD_DIR) -j $(NUM_THREADS)

debug:					#: Build with debugging symbols
	$(MAKE) cmake BUILD_DIR=debug BUILD_TYPE=Debug
	$(MAKE) build BUILD_DIR=debug

release:				#: Build the release version
	$(MAKE) cmake BUILD_DIR=release BUILD_TYPE=Release && \
	$(MAKE) build BUILD_DIR=release

minimal_debug:			#: Minimal build with debugging symbols
	$(MAKE) cmake BUILD_DIR=debug BUILD_TYPE=debug \
		EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS} \
			-DVELOX_BUILD_MINIMAL=ON"
	$(MAKE) build BUILD_DIR=debug

min_debug: minimal_debug

minimal:				 #: Minimal build
	$(MAKE) cmake BUILD_DIR=release BUILD_TYPE=release \
		EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS} \
			-DVELOX_BUILD_MINIMAL=ON"
	$(MAKE) build BUILD_DIR=release

wave:			   	         #: Build with Wave GPU support
	$(MAKE) cmake-wave BUILD_DIR=release BUILD_TYPE=release
	$(MAKE) build BUILD_DIR=release

cudf:			   	         #: Build with cuDF GPU support
	$(MAKE) cmake-cudf BUILD_DIR=release BUILD_TYPE=release
	$(MAKE) build BUILD_DIR=release

wave-debug:			 #: Build with debugging symbols and Wave GPU support
	$(MAKE) cmake-wave BUILD_DIR=debug BUILD_TYPE=debug
	$(MAKE) build BUILD_DIR=debug

cudf-debug:			 #: Build with debugging symbols and cuDF GPU support
	$(MAKE) cmake-cudf BUILD_DIR=debug BUILD_TYPE=debug
	$(MAKE) build BUILD_DIR=debug

dwio:						#: Minimal build with dwio enabled.
	$(MAKE) cmake BUILD_DIR=release BUILD_TYPE=release \
		EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS} \
			-DVELOX_BUILD_MINIMAL_WITH_DWIO=ON"
	$(MAKE) build BUILD_DIR=release

dwio_debug:			#: Minimal build with dwio debugging symbols.
	$(MAKE) cmake BUILD_DIR=debug BUILD_TYPE=debug \
		EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS} \
			-DVELOX_BUILD_MINIMAL_WITH_DWIO=ON"
	$(MAKE) build BUILD_DIR=debug

sve_build:		#: Build with SVE-specific configuration
# Check for SVE support and set appropriate compilers
	@SVE_CC=$$(if [ "$$(lscpu | grep -q "sve" && grep -qi "ubuntu" /etc/os-release && echo 1)" = "1" ]; then echo "/usr/bin/gcc-12"; else echo "gcc"; fi); \
	SVE_CXX=$$(if [ "$$(lscpu | grep -q "sve" && grep -qi "ubuntu" /etc/os-release && echo 1)" = "1" ]; then echo "/usr/bin/g++-12"; else echo "g++"; fi); \
	echo "Using CC=$$SVE_CC, CXX=$$SVE_CXX"; \
	export CC=$$SVE_CC; export CXX=$$SVE_CXX; \
	$(MAKE) cmake BUILD_DIR=release BUILD_TYPE=release EXTRA_CMAKE_FLAGS="-DCMAKE_C_COMPILER=$$SVE_CC -DCMAKE_CXX_COMPILER=$$SVE_CXX -DCMAKE_CXX_FLAGS='$(COMPILER_FLAGS) -Wno-error=stringop-overflow $(shell ./scripts/setup-helper-functions.sh detect_sve_flags)'" && \
	$(MAKE) build BUILD_DIR=release

benchmarks-basic-build:
	$(MAKE) release \
		EXTRA_CMAKE_FLAGS=" ${EXTRA_CMAKE_FLAGS} \
			-DVELOX_BUILD_TESTING=OFF \
			-DVELOX_ENABLE_BENCHMARKS_BASIC=ON \
			-DVELOX_BUILD_RUNNER=OFF"

benchmarks-build:
	$(MAKE) release \
		EXTRA_CMAKE_FLAGS=" ${EXTRA_CMAKE_FLAGS} \
			-DVELOX_BUILD_TESTING=OFF \
			-DVELOX_ENABLE_BENCHMARKS=ON \
			-DVELOX_BUILD_RUNNER=OFF"

benchmarks-basic-run:
	scripts/ci/benchmark-runner.py run \
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

help:					#: Show the help messages
	@cat $(firstword $(MAKEFILE_LIST)) | \
	awk '/^[-a-z]+:/' | \
	awk -F: '{ printf("%-20s   %s\n", $$1, $$NF) }'

python-venv:
	@if [ ! -f ${PYTHON_VENV}/pyvenv.cfg ]; then \
		${VENV} $(PYTHON_VENV); \
	fi

check-pip-version: python-venv # We need a recent pip for '-C'
	@if [ "$(PIP)" == "uv pip" ]; then \
		exit 0; \
	fi; \
	source .venv/bin/activate; \
	pip_version=$$($(PIP) --version | sed -E 's/pip ([0-9]+)\.([0-9]+).*/\1/'); \
	if [ "$$pip_version" -lt 25 ]; then \
		$(PIP) install --upgrade pip; \
	fi

python-build: check-pip-version
	source .venv/bin/activate; \
	$(PIP) install pyarrow scikit_build_core setuptools_scm[toml]; \
	${PIP} install --no-build-isolation -Ccmake.build-type=Debug -Cbuild.tool-args="-j${NUM_THREADS}" -v .

python-test: python-build
	source .venv/bin/activate; \
	python3 -m unittest discover -v -s python/test
