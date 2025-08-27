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

find_program(CLCC_EXECUTABLE clcc REQUIRED)

function(breeze_add_opencl_test_kernels target source)
  cmake_parse_arguments(arg "" "" "DEPENDS" ${ARGN})
  add_custom_command(
    OUTPUT ${target}.so
    COMMAND
      ${CLCC_EXECUTABLE} -o ${target}.so ${source} -MD -MF ${target}.clcpp.d -DPLATFORM_OPENCL
      -I${CMAKE_SOURCE_DIR} -I${CMAKE_BINARY_DIR} ${COMPILER_WARN_FLAGS} ${OPT_FLAGS}
    COMMENT "Building OpenCL kernels ${source} --> ${target}.so"
    DEPENDS ${arg_DEPENDS}
    DEPFILE ${target}.clcpp.d
  )
  add_custom_target(${target} DEPENDS ${target}.so)
endfunction()

function(breeze_add_opencl_test target source shaderlib)
  add_executable(${target} ${source})
  target_compile_features(${target} PRIVATE cxx_std_17)
  target_include_directories(${target} PRIVATE ${CMAKE_OPENCL_INCLUDE} ${CMAKE_BINARY_DIR})
  target_compile_definitions(${target} PUBLIC PLATFORM_OPENCL=1 SHADER_LIB=\"${shaderlib}\")
  target_compile_options(${target} PRIVATE ${WARN_FLAGS} ${OPT_FLAGS} ${SANITIZE_COMPILE_FLAGS})
  target_link_libraries(${target} gtest -lOpenCL test_main)
  target_link_options(
    ${target}
    PRIVATE ${CMAKE_OPENCL_LINK_FLAGS} ${SANITIZE_LINK_FLAGS} ${ARCH_CLANG_LINK_FLAGS}
  )
  gtest_discover_tests(${target} TEST_PREFIX opencl: DISCOVERY_MODE PRE_TEST)
endfunction()
