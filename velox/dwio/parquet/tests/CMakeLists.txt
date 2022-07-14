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

# for generated headers
include_directories(${CMAKE_BINARY_DIR})

set(TEST_LINK_LIBS
    velox_dwrf_test_utils
    velox_vector_test_lib
    gtest
    gtest_main
    gmock
    ${gflags_LIBRARIES}
    ${GLOG}
    ${FILESYSTEM})

add_subdirectory(duckdb_reader)

if(VELOX_ENABLE_ARROW)

  add_executable(velox_parquet_e2e_filter_test E2EFilterTest.cpp)

  add_test(velox_parquet_e2e_filter_test velox_parquet_e2e_filter_test)

  target_link_libraries(
    velox_parquet_e2e_filter_test
    velox_e2e_filter_test_base
    velox_dwio_parquet_writer
    ${LZ4}
    ${LZO}
    ${ZSTD}
    ${ZLIB_LIBRARIES}
    ${TEST_LINK_LIBS})

endif()
