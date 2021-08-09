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
TREAT_WARNINGS_AS_ERRORS ?= 1
ENABLE_WALL ?= 1
WARNINGS_AS_ERRORS=-DTREAT_WARNINGS_AS_ERRORS=${TREAT_WARNINGS_AS_ERRORS}
ENABLE_ALL_WARNINGS=-DENABLE_ALL_WARNINGS=${ENABLE_WALL}

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

all: release			#: Build the release version

clean:					#: Delete all build artifacts
	rm -rf $(BUILD_BASE_DIR)

cmake:					#: Use CMake to create a Makefile build system
	mkdir -p $(BUILD_BASE_DIR)/$(BUILD_DIR) && \
	cmake -B "$(BUILD_BASE_DIR)/$(BUILD_DIR)" $(GENERATOR) $(USE_CCACHE) $(FORCE_COLOR) ${WARNINGS_AS_ERRORS} ${ENABLE_ALL_WARNINGS} -DCMAKE_BUILD_TYPE=$(BUILD_TYPE)

build:					#: Build the software based in BUILD_DIR and BUILD_TYPE variables
	cmake --build $(BUILD_BASE_DIR)/$(BUILD_DIR) -j ${NUM_THREADS}

debug:					#: Build with debugging symbols
	$(MAKE) cmake BUILD_DIR=debug BUILD_TYPE=Debug
	$(MAKE) build BUILD_DIR=debug

release:				#: Build the release version
	$(MAKE) cmake BUILD_DIR=release BUILD_TYPE=Release && \
	$(MAKE) build BUILD_DIR=release

unittest: debug			#: Build with debugging and run unit tests
	cd $(BUILD_BASE_DIR)/debug && ctest -j ${NUM_THREADS} -VV --output-on-failure --exclude-regex "MemoryMemoryHeaderTest\.getDefaultScopedMemoryPool|MemoryManagerTest\.GlobalMemoryManager"

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

linux-container:
	rm -rf /tmp/docker && \
	mkdir -p /tmp/docker && \
	cp scripts/setup-$(CONTAINER_NAME).sh scripts/$(CONTAINER_NAME)-container.dockfile /tmp/docker && \
	cd /tmp/docker && \
	docker build --tag "prestocpp/velox-$(CONTAINER_NAME):${USER}-$(shell date +%Y%m%d)" -f $(CONTAINER_NAME)-container.dockfile .

help:					#: Show the help messages
	@cat $(firstword $(MAKEFILE_LIST)) | \
	awk '/^[-a-z]+:/' | \
	awk -F: '{ printf("%-20s   %s\n", $$1, $$NF) }'
