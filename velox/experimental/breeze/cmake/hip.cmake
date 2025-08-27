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

find_program(HIPCC_EXECUTABLE hipcc REQUIRED PATHS /opt/rocm/bin)

function(breeze_add_hip_test target source)
  cmake_parse_arguments(arg "" "" "DEPENDS" ${ARGN})
  add_custom_command(
    OUTPUT ${target}.o
    COMMAND
      ${HIPCC_EXECUTABLE} ${HIP_HIPCC_FLAGS} ${NDEBUG_DEFINE} -DPLATFORM_HIP -I${CMAKE_SOURCE_DIR}
      -I${googletest_SOURCE_DIR}/googletest/include -I${CMAKE_BINARY_DIR} ${CMAKE_CXX_FLAGS}
      ${COMPILER_WARN_FLAGS} ${OPT_FLAGS} -std=c++17 -c ${source} -MD -MF ${target}.o.d -o
      ${target}.o
    DEPFILE ${target}.o.d
    DEPENDS ${arg_DEPENDS}
    COMMENT "Building HIP object ${target}.o"
  )
  add_custom_command(
    OUTPUT ${target}
    COMMAND
      ${HIPCC_EXECUTABLE} -o ${target} ${target}.o $<TARGET_FILE_DIR:GTest::gtest>/libgtest.a
      $<TARGET_FILE_DIR:test_main>/libtest_main.a
      $<$<BOOL:${BUILD_TRACING}>:$<TARGET_FILE_DIR:perfetto>/libperfetto.a> ${ARCH_LINK_FLAGS}
    DEPENDS ${target}.o test_main
    COMMENT "Linking HIP executable ${target}"
  )
  add_executable(${target}_TESTS IMPORTED)
  set_property(
    TARGET ${target}_TESTS
    PROPERTY IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/${target}
  )
  gtest_discover_tests(${target}_TESTS TEST_PREFIX hip: DISCOVERY_MODE PRE_TEST)
endfunction()
