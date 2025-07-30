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
include_guard(GLOBAL)

set(VELOX_FAISS_BUILD_VERSION 1.11.0)
set(VELOX_FAISS_BUILD_SHA256_CHECKSUM
    c5d517da6deb6a6d74290d7145331fc7474426025e2d826fa4a6d40670f4493c)
set(VELOX_FAISS_SOURCE_URL
    "https://github.com/facebookresearch/faiss/archive/refs/tags/v${VELOX_FAISS_BUILD_VERSION}.tar.gz"
)

velox_resolve_dependency_url(FAISS)

# We need these hints for macos to build.
if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
  message(STATUS "Detected Apple platform")
  execute_process(
    COMMAND brew --prefix libomp
    RESULT_VARIABLE BREW_LIBOMP_RESULT
    OUTPUT_VARIABLE BREW_LIBOMP_PREFIX
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  if(BREW_LIBOMP_RESULT EQUAL 0 AND EXISTS "${BREW_LIBOMP_PREFIX}")
    list(APPEND CMAKE_PREFIX_PATH "${BREW_LIBOMP_PREFIX}")
  endif()

  execute_process(
    COMMAND brew --prefix openblas
    RESULT_VARIABLE BREW_OPENBLAS_RESULT
    OUTPUT_VARIABLE BREW_OPENBLAS_PREFIX
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  if(BREW_OPENBLAS_RESULT EQUAL 0 AND EXISTS "${BREW_OPENBLAS_PREFIX}")
    list(APPEND CMAKE_PREFIX_PATH "${BREW_OPENBLAS_PREFIX}")
  endif()
endif()

FetchContent_Declare(
  faiss
  URL ${VELOX_FAISS_SOURCE_URL}
  URL_HASH ${VELOX_FAISS_BUILD_SHA256_CHECKSUM}
  SYSTEM # Once there are targets depending on FAISS we can add
         # `EXCLUDE_FROM_ALL` back in
)

# Set build options
block()
set(BUILD_SHARED_LIBS OFF)
set(CMAKE_BUILD_TYPE Release)
set(FAISS_ENABLE_GPU OFF)
set(FAISS_ENABLE_PYTHON OFF)
set(FAISS_ENABLE_GPU_TESTS OFF)
# Make FAISS available
FetchContent_MakeAvailable(faiss)
endblock()
