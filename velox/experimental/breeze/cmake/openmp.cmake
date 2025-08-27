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
#
# Copyright (c) 2024 by Rivos Inc.
# Licensed under the Apache License, Version 2.0, see LICENSE for details.
# SPDX-License-Identifier: Apache-2.0

set(OPENMP_GCC_NO_WARN_FLAGS "-Wno-unknown-pragmas;-Wno-maybe-uninitialized")

if(NOT DEFINED OPENMP_UNROLL_THRESHOLD)
  set(OPENMP_UNROLL_THRESHOLD 32 CACHE STRING "OpenMP loop unroll threshold")
endif()

find_package(OpenMP REQUIRED)

function(breeze_add_openmp_test target source)
  add_executable(${target} ${source})
  target_compile_features(${target} PRIVATE cxx_std_17)
  target_include_directories(${target} PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
  if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    target_compile_options(
      ${target}
      PRIVATE
        -Xclang
        -fopenmp
        -DPLATFORM_OPENMP
        ${CMAKE_CXX_FLAGS}
        ${WARN_FLAGS}
        ${OPT_FLAGS}
        ${SANITIZE_COMPILE_FLAGS}
        -Xclang
        -mllvm
        -Xclang
        -unroll-threshold=${OPENMP_UNROLL_THRESHOLD}
    )
  else()
    target_compile_options(
      ${target}
      PRIVATE
        -fopenmp
        -DPLATFORM_OPENMP
        ${CMAKE_CXX_FLAGS}
        ${WARN_FLAGS}
        ${OPENMP_GCC_NO_WARN_FLAGS}
        ${OPT_FLAGS}
        ${SANITIZE_COMPILE_FLAGS}
    )
  endif()
  target_link_options(${target} PRIVATE ${SANITIZE_LINK_FLAGS})
  target_link_libraries(${target} OpenMP::OpenMP_CXX test_main)
  gtest_discover_tests(${target} TEST_PREFIX openmp: DISCOVERY_MODE PRE_TEST)
endfunction()
