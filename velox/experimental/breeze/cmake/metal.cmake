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

function(breeze_add_metal_test_kernels target source)
  cmake_parse_arguments(arg "" "" "DEPENDS" ${ARGN})
  add_custom_command(
    OUTPUT ${target}.air
    COMMAND
      xcrun -sdk macosx metal -DPLATFORM_METAL -I${CMAKE_SOURCE_DIR} -I${CMAKE_BINARY_DIR}
      ${WARN_FLAGS} ${OPT_FLAGS} -Wno-c++17-extensions -std=metal3.0 -c ${source} -MD -MF
      ${target}.air.d -o ${target}.air
    COMMENT "Compile ${source} --> ${target}.air"
    DEPENDS ${arg_DEPENDS}
    DEPFILE ${target}.air.d
  )
  add_custom_command(
    OUTPUT ${target}.metallib
    COMMAND xcrun -sdk macosx metallib -o ${target}.metallib ${target}.air
    COMMENT "Compile ${target}.air --> ${target}.metallib"
    DEPENDS ${target}.air
  )
  add_custom_target(${target} DEPENDS ${target}.metallib)
endfunction()

function(breeze_add_metal_test target source shaderlib)
  add_executable(${target} ${source} platforms/metal_test.mm)
  target_compile_features(${target} PRIVATE cxx_std_17)
  target_include_directories(${target} PRIVATE ${CMAKE_BINARY_DIR})
  target_compile_definitions(${target} PUBLIC PLATFORM_METAL=1 SHADER_LIB=\"${shaderlib}\")
  target_compile_options(
    ${target}
    PRIVATE -x objective-c++ ${WARN_FLAGS} ${OPT_FLAGS} ${SANITIZE_COMPILE_FLAGS}
  )
  target_link_libraries(
    ${target}
    "-framework Foundation"
    "-framework CoreGraphics"
    "-framework Metal"
    test_main
  )
  target_link_options(${target} PRIVATE ${SANITIZE_LINK_FLAGS})
  gtest_discover_tests(${target} TEST_PREFIX metal: DISCOVERY_MODE PRE_TEST)
endfunction()
