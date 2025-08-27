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

if(NOT DEFINED CMAKE_NVCC_FLAGS)
  if(DEFINED ENV{NVCCFLAGS})
    set(CMAKE_NVCC_FLAGS "$ENV{NVCCFLAGS}" CACHE STRING "NVCC flags")
  endif()
endif()
if(BREEZE_BUILD_TYPE MATCHES Debug)
  set(CUDA_OPT_FLAGS "-G")
endif()
if(BREEZE_BUILD_TYPE MATCHES RelWithDebInfo)
  set(CUDA_OPT_FLAGS "-lineinfo")
endif()

find_program(NVCC_EXECUTABLE nvcc REQUIRED PATHS /usr/local/cuda/bin)

if(DEFINED CUDA_EXPECTED_RESOURCE_USAGE_DIR)
  if(NOT EXISTS "${CUDA_EXPECTED_RESOURCE_USAGE_DIR}")
    message(FATAL_ERROR "Invalid resource usage dir: ${CUDA_EXPECTED_RESOURCE_USAGE_DIR}")
  endif()
  if(NOT DEFINED CUDA_RESOURCE_USAGE_CMDLINE)
    message(FATAL_ERROR "CUDA_RESOURCE_USAGE_CMDLINE is undefined")
  endif()
  if(NOT DEFINED CUDA_RESOURCE_USAGE_FILTER_CMDLINE)
    message(FATAL_ERROR "CUDA_RESOURCE_USAGE_FILTER_CMDLINE is undefined")
  endif()
  find_program(DIFF_EXECUTABLE diff REQUIRED)
endif()

# use PTX specialization by default for CUDA
if(NOT DEFINED CUDA_PLATFORM_SPECIALIZATION_HEADER)
  set(
    CUDA_PLATFORM_SPECIALIZATION_HEADER
    breeze/platforms/specialization/cuda-ptx.cuh
    CACHE STRING
    "CUDA platform specialization header"
  )
endif()

function(breeze_add_cuda_object target source)
  cmake_parse_arguments(arg "" "" "FLAGS;LIBS;DEPENDS" ${ARGN})
  add_custom_command(
    OUTPUT ${target}.o
    COMMAND
      ${NVCC_EXECUTABLE} -x cu ${CMAKE_NVCC_FLAGS} ${NDEBUG_DEFINE} -DPLATFORM_CUDA
      -DCUDA_PLATFORM_SPECIALIZATION_HEADER=${CUDA_PLATFORM_SPECIALIZATION_HEADER}
      -I${CMAKE_SOURCE_DIR} ${CMAKE_CXX_FLAGS} ${COMPILER_WARN_FLAGS} ${OPT_FLAGS} ${CUDA_OPT_FLAGS}
      ${arg_FLAGS} -std=c++17 -c ${source} -MD -MF ${target}.o.d -o ${target}.o
    DEPFILE ${target}.o.d
    DEPENDS ${arg_DEPENDS}
    COMMENT "Building CUDA object ${target}.o"
  )
endfunction()

function(breeze_add_cuda_test target source)
  cmake_parse_arguments(arg "" "" "FLAGS;LIBS;DEPENDS" ${ARGN})
  list(APPEND arg_FLAGS -I${googletest_SOURCE_DIR}/googletest/include)
  list(APPEND arg_FLAGS -I${CMAKE_SOURCE_DIR})
  list(APPEND arg_FLAGS -I${CMAKE_BINARY_DIR})
  breeze_add_cuda_object(${target} ${source} FLAGS ${arg_FLAGS} DEPENDS ${arg_DEPENDS})
  add_custom_command(
    OUTPUT ${target}
    COMMAND
      ${NVCC_EXECUTABLE} -o ${target} ${target}.o ${arg_LIBS}
      $<TARGET_FILE_DIR:GTest::gtest>/libgtest.a $<TARGET_FILE_DIR:test_main>/libtest_main.a
      $<$<BOOL:${BUILD_TRACING}>:$<TARGET_FILE_DIR:perfetto>/libperfetto.a> ${ARCH_LINK_FLAGS}
    DEPENDS ${target}.o test_main
    COMMENT "Linking CUDA executable ${target}"
  )
  add_executable(${target}_TESTS IMPORTED)
  set_property(
    TARGET ${target}_TESTS
    PROPERTY IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/${target}
  )
  gtest_discover_tests(${target}_TESTS TEST_PREFIX cuda: DISCOVERY_MODE PRE_TEST)
  if(DEFINED CUDA_EXPECTED_RESOURCE_USAGE_DIR)
    if(EXISTS "${CUDA_EXPECTED_RESOURCE_USAGE_DIR}/${target}-expected.txt")
      set(
        GET_RESOURCE_USAGE_CMDLINE
        "${CUDA_RESOURCE_USAGE_CMDLINE} ${CMAKE_CURRENT_BINARY_DIR}/${target} | ${CUDA_RESOURCE_USAGE_FILTER_CMDLINE}"
      )
      set(ACTUAL_USAGE_PATH resource-usage/${target}-actual.txt)
      add_custom_command(
        OUTPUT ${ACTUAL_USAGE_PATH}
        COMMAND sh -c "${GET_RESOURCE_USAGE_CMDLINE} > ${ACTUAL_USAGE_PATH}"
        DEPENDS ${target}
        VERBATIM
      )
      add_custom_target(${target}-resource-usage ALL DEPENDS ${ACTUAL_USAGE_PATH})
      string(ASCII 27 Esc)
      set(RedColor "${Esc}[31m")
      set(YellowColor "${Esc}[33m")
      set(ColorReset "${Esc}[m")
      add_test(
        NAME cuda:${target}:resource-usage
        COMMAND
          sh -c
          "${DIFF_EXECUTABLE} --color=always -u ${CUDA_EXPECTED_RESOURCE_USAGE_DIR}/${target}-expected.txt ${CMAKE_CURRENT_BINARY_DIR}/${ACTUAL_USAGE_PATH} \
            || (echo '\n${RedColor}Resource usage checks FAILED:${ColorReset}\n\nRun the following command to update the baseline if this is expected:' ;
                echo '${YellowColor}cp ${CMAKE_CURRENT_BINARY_DIR}/${ACTUAL_USAGE_PATH} ${CUDA_EXPECTED_RESOURCE_USAGE_DIR}/${target}-expected.txt${ColorReset}' ;
                exit 1\)"
      )
      set_tests_properties(
        cuda:${target}:resource-usage
        PROPERTIES DEPENDS ${target}-resource-usage
      )
    endif()
  endif()
endfunction()
