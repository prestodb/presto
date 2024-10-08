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
.PHONY: all cmake build cmake-and-build clean debug release unittest submodules velox-submodule

BUILD_BASE_DIR=_build
BUILD_DIR=release
BUILD_TYPE=Release
TREAT_WARNINGS_AS_ERRORS ?= 1
ENABLE_WALL ?= 1
NUM_THREADS ?= $(shell getconf _NPROCESSORS_CONF 2>/dev/null || echo 1)
CPU_TARGET ?= "avx"
CMAKE_PREFIX_PATH ?= "/usr/local"
PRESTOCPP_ROOT_DIR="$(shell pwd)"
PYTHON_VENV ?= .venv

EXTRA_CMAKE_FLAGS ?= ""

ifeq ($(PRESTO_ENABLE_PARQUET), ON)
  EXTRA_CMAKE_FLAGS += -DPRESTO_ENABLE_PARQUET=ON
endif
ifeq ($(PRESTO_ENABLE_S3), ON)
  EXTRA_CMAKE_FLAGS += -DPRESTO_ENABLE_S3=ON
endif
ifeq ($(PRESTO_ENABLE_HDFS), ON)
  EXTRA_CMAKE_FLAGS += -DPRESTO_ENABLE_HDFS=ON
endif
ifeq ($(PRESTO_ENABLE_REMOTE_FUNCTIONS), ON)
  EXTRA_CMAKE_FLAGS += -DPRESTO_ENABLE_REMOTE_FUNCTIONS=ON
endif
ifeq ($(PRESTO_ENABLE_JWT), ON)
  EXTRA_CMAKE_FLAGS += -DPRESTO_ENABLE_JWT=ON
endif
ifneq ($(PRESTO_STATS_REPORTER_TYPE),)
  EXTRA_CMAKE_FLAGS += -DPRESTO_STATS_REPORTER_TYPE=$(PRESTO_STATS_REPORTER_TYPE)
endif

CMAKE_FLAGS := -DTREAT_WARNINGS_AS_ERRORS=${TREAT_WARNINGS_AS_ERRORS}
CMAKE_FLAGS += -DENABLE_ALL_WARNINGS=${ENABLE_WALL}
CMAKE_FLAGS += -DCMAKE_PREFIX_PATH=$(CMAKE_PREFIX_PATH)
CMAKE_FLAGS += -DCMAKE_BUILD_TYPE=$(BUILD_TYPE)

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
	cmake -B "$(BUILD_BASE_DIR)/$(BUILD_DIR)" $(FORCE_COLOR) $(CMAKE_FLAGS) $(EXTRA_CMAKE_FLAGS)

build:					#: Build the software based in BUILD_DIR and BUILD_TYPE variables
	cmake --build $(BUILD_BASE_DIR)/$(BUILD_DIR) -j $(NUM_THREADS)

debug:					#: Build with debugging symbols
	$(MAKE) cmake BUILD_DIR=debug BUILD_TYPE=Debug
	$(MAKE) build BUILD_DIR=debug

release:				#: Build the release version
	$(MAKE) cmake BUILD_DIR=release BUILD_TYPE=Release && \
	$(MAKE) build BUILD_DIR=release

cmake-and-build:			#: cmake and build without updating submodules which requires git
	cmake -B "$(BUILD_BASE_DIR)/$(BUILD_DIR)" $(FORCE_COLOR) $(CMAKE_FLAGS) $(EXTRA_CMAKE_FLAGS)
	cmake --build $(BUILD_BASE_DIR)/$(BUILD_DIR) -j $(NUM_THREADS)

unittest: debug			#: Build with debugging and run unit tests
	cd $(BUILD_BASE_DIR)/debug && ctest -j $(NUM_THREADS) -VV --output-on-failure --exclude-regex velox.*

presto_protocol:		#: Build the presto_protocol serde library
	cd presto_cpp/presto_protocol; $(MAKE) presto_protocol

TypeSignature:		#: Build the Presto TypeSignature parser
	cd presto_cpp/main/types; $(MAKE) TypeSignature

format-fix: 			#: Fix formatting issues in the presto-native-execution directory
ifneq ("$(wildcard ${PYTHON_VENV}/pyvenv.cfg)","")
	source ${PYTHON_VENV}/bin/activate; scripts/check.py format master --fix
else
	scripts/check.py format master --fix
endif

format-check: 			#: Check for formatting issues in the presto-native-execution directory
	clang-format --version
ifneq ("$(wildcard ${PYTHON_VENV}/pyvenv.cfg)","")
	source ${PYTHON_VENV}/bin/activate; scripts/check.py format master
else
	scripts/check.py format master
endif

header-fix:			#: Fix license header issues in the presto-native-execution directory
ifneq ("$(wildcard ${PYTHON_VENV}/pyvenv.cfg)","")
	source ${PYTHON_VENV}/bin/activate; scripts/check.py header master --fix
else
	scripts/check.py header master --fix
endif

header-check:			#: Check for license header issues in the presto-native-execution directory
ifneq ("$(wildcard ${PYTHON_VENV}/pyvenv.cfg)","")
	source ${PYTHON_VENV}/bin/activate; scripts/check.py header master
else
	scripts/check.py header master
endif

help:					#: Show the help messages
	@cat $(firstword $(MAKEFILE_LIST)) | \
	awk '/^[-a-z]+:/' | \
	awk -F: '{ printf("%-20s   %s\n", $$1, $$NF) }'
