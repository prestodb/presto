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

include_directories(${CMAKE_SOURCE_DIR})

add_library(test_main test_main.cpp)
target_link_libraries(test_main PUBLIC gtest $<$<BOOL:${BUILD_TRACING}>:perfetto>)
target_compile_features(test_main PRIVATE cxx_std_17)
target_compile_options(test_main PRIVATE ${CMAKE_CXX_FLAGS} ${WARN_FLAGS} ${SANITIZE_COMPILE_FLAGS})

function(breeze_add_tests dir type suite filetype)
  if(BUILD_OPENCL)
    breeze_add_opencl_test_kernels(
      ${type}_opencl_kernels
      ${CMAKE_CURRENT_SOURCE_DIR}/${dir}/kernels.clcpp
      DEPENDS ${_opencl_kernels_src}
    )
  endif()
  if(BUILD_METAL)
    breeze_add_metal_test_kernels(
      ${type}_metal_kernels
      ${CMAKE_CURRENT_SOURCE_DIR}/${dir}/kernels.metal
      DEPENDS ${_metal_kernels_src}
    )
  endif()
  foreach(name IN LISTS ARGN)
    set(_deps)
    if(BUILD_CUDA)
      breeze_add_cuda_test(
        ${name}_${type}${suite}-cuda
        ${CMAKE_CURRENT_SOURCE_DIR}/${dir}/${name}${suite}${filetype}
        FLAGS ${_cuda_flags}
        LIBS ${_cuda_libs}
        DEPENDS ${_cuda_deps} ${_cuda_kernels_src} ${_cuda_test_fixture_src}
      )
      list(APPEND _deps ${name}_${type}${suite}-cuda)
    endif()
    if(BUILD_HIP)
      breeze_add_hip_test(
        ${name}_${type}${suite}-hip
        ${CMAKE_CURRENT_SOURCE_DIR}/${dir}/${name}${suite}${filetype}
        DEPENDS ${_hip_kernels_src} ${_hip_test_fixture_src}
      )
      list(APPEND _deps ${name}_${type}${suite}-hip)
    endif()
    if(BUILD_SYCL)
      breeze_add_sycl_test(
        ${name}_${type}${suite}-sycl
        ${CMAKE_CURRENT_SOURCE_DIR}/${dir}/${name}${suite}${filetype}
        DEPENDS ${_sycl_kernels_src} ${_sycl_test_fixture_src}
      )
      list(APPEND _deps ${name}_${type}${suite}-sycl)
    endif()
    if(BUILD_OPENCL)
      breeze_add_opencl_test(
        ${name}_${type}${suite}-opencl
        ${CMAKE_CURRENT_SOURCE_DIR}/${dir}/${name}${suite}${filetype}
        ${CMAKE_CURRENT_BINARY_DIR}/${type}_opencl_kernels.so
      )
      add_dependencies(
        ${name}_${type}${suite}-opencl
        ${type}_opencl_kernels
        ${_opencl_test_fixture_src}
      )
      list(APPEND _deps ${name}_${type}${suite}-opencl)
    endif()
    if(BUILD_OPENMP)
      breeze_add_openmp_test(
        ${name}_${type}${suite}-openmp
        ${CMAKE_CURRENT_SOURCE_DIR}/${dir}/${name}${suite}${filetype}
      )
      if(DEFINED _openmp_kernels_src)
        add_dependencies(
          ${name}_${type}${suite}-openmp
          ${_openmp_kernels_src}
          ${_openmp_test_fixture_src}
        )
      endif()
      list(APPEND _deps ${name}_${type}${suite}-openmp)
    endif()
    if(BUILD_METAL)
      breeze_add_metal_test(
        ${name}_${type}${suite}-metal
        ${CMAKE_CURRENT_SOURCE_DIR}/${dir}/${name}${suite}${filetype}
        ${CMAKE_CURRENT_BINARY_DIR}/${type}_metal_kernels.metallib
      )
      add_dependencies(
        ${name}_${type}${suite}-metal
        ${type}_metal_kernels
        ${_metal_test_fixture_src}
      )
      list(APPEND _deps ${name}_${type}${suite}-metal)
    endif()
    add_custom_target(${name}_${type}${suite} ALL DEPENDS ${_deps})
  endforeach()
endfunction()

# Unittest kernels are specialized from a single template into code appropriate
# for the backend
function(
  generate_kernel
  target
  dir
  type
  backend
  outfile
)
  if(DEFINED LLVM_PATH)
    set(LLVM_FLAG "--llvm-root='${LLVM_PATH}'")
  endif()
  add_custom_command(
    OUTPUT ${outfile}
    COMMAND
      ${CMAKE_CURRENT_SOURCE_DIR}/kernel_generator.py --backend="${backend}"
      --template="${CMAKE_CURRENT_SOURCE_DIR}/${dir}/${type}-kernels.template.h"
      --out="${CMAKE_CURRENT_BINARY_DIR}/${outfile}" ${LLVM_FLAG}
    DEPENDS
      "${CMAKE_CURRENT_SOURCE_DIR}/${dir}/${type}-kernels.template.h"
      "${CMAKE_CURRENT_SOURCE_DIR}/kernel_generator.py"
      "${CMAKE_CURRENT_SOURCE_DIR}/generator_common.py"
    COMMENT "Specializing kernel code for backend ${backend}"
  )
  add_custom_target(${target} DEPENDS ${outfile})
endfunction()

function(
  generate_test_fixture
  target
  dir
  type
  backend
  outfile
)
  if(DEFINED LLVM_PATH)
    set(LLVM_FLAG "--llvm-root='${LLVM_PATH}'")
  endif()
  add_custom_command(
    OUTPUT ${outfile}
    COMMAND
      ${CMAKE_CURRENT_SOURCE_DIR}/test_fixture_generator.py --backend="${backend}"
      --template="${CMAKE_CURRENT_SOURCE_DIR}/${dir}/${type}_test.template.h"
      --out="${CMAKE_CURRENT_BINARY_DIR}/${outfile}" ${LLVM_FLAG}
    DEPENDS
      "${CMAKE_CURRENT_SOURCE_DIR}/${dir}/${type}_test.template.h"
      "${CMAKE_CURRENT_SOURCE_DIR}/test_fixture_generator.py"
      "${CMAKE_CURRENT_SOURCE_DIR}/generator_common.py"
    COMMENT "Specializing test fixture for backend ${backend}"
  )
  add_custom_target(${target} DEPENDS ${outfile})
endfunction()

function(breeze_add_unittests dir type)
  if(BUILD_GENERATE_TEST_FIXTURES)
    if(BUILD_OPENCL)
      generate_kernel(
        ${type}_opencl_kernels_src
        ${dir}
        ${type}
        "opencl"
        "generated/${dir}/kernels-opencl.h"
      )
      generate_test_fixture(
        ${type}_opencl_test_fixture_src
        ${dir}
        ${type}
        "opencl"
        "generated/${dir}/${type}_test-opencl.h"
      )
      set(_opencl_kernels_src ${type}_opencl_kernels_src)
      set(_opencl_test_fixture_src ${type}_opencl_test_fixture_src)
    endif()
    if(BUILD_METAL)
      generate_kernel(
        ${type}_metal_kernels_src
        ${dir}
        ${type}
        "metal"
        "generated/${dir}/kernels-metal.h"
      )
      generate_test_fixture(
        ${type}_metal_test_fixture_src
        ${dir}
        ${type}
        "metal"
        "generated/${dir}/${type}_test-metal.h"
      )
      set(_metal_kernels_src ${type}_metal_kernels_src)
      set(_metal_test_fixture_src ${type}_metal_test_fixture_src)
    endif()
    if(BUILD_CUDA)
      generate_kernel(
        ${type}_cuda_kernels_src
        ${dir}
        ${type}
        "cuda"
        "generated/${dir}/kernels-cuda.cuh"
      )
      generate_test_fixture(
        ${type}_cuda_test_fixture_src
        ${dir}
        ${type}
        "cuda"
        "generated/${dir}/${type}_test-cuda.cuh"
      )
      set(_cuda_kernels_src ${type}_cuda_kernels_src)
      set(_cuda_test_fixture_src ${type}_cuda_test_fixture_src)
    endif()
    if(BUILD_HIP)
      generate_kernel(
        ${type}_hip_kernels_src
        ${dir}
        ${type}
        "hip"
        "generated/${dir}/kernels-hip.hpp"
      )
      generate_test_fixture(
        ${type}_hip_test_fixture_src
        ${dir}
        ${type}
        "hip"
        "generated/${dir}/${type}_test-hip.hpp"
      )
      set(_hip_kernels_src ${type}_hip_kernels_src)
      set(_hip_test_fixture_src ${type}_hip_test_fixture_src)
    endif()
    if(BUILD_SYCL)
      generate_kernel(
        ${type}_sycl_kernels_src
        ${dir}
        ${type}
        "sycl"
        "generated/${dir}/kernels-sycl.hpp"
      )
      generate_test_fixture(
        ${type}_sycl_test_fixture_src
        ${dir}
        ${type}
        "sycl"
        "generated/${dir}/${type}_test-sycl.hpp"
      )
      set(_sycl_kernels_src ${type}_sycl_kernels_src)
      set(_sycl_test_fixture_src ${type}_sycl_test_fixture_src)
    endif()
    if(BUILD_OPENMP)
      generate_kernel(
        ${type}_openmp_kernels_src
        ${dir}
        ${type}
        "openmp"
        "generated/${dir}/kernels-openmp.h"
      )
      generate_test_fixture(
        ${type}_openmp_test_fixture_src
        ${dir}
        ${type}
        "openmp"
        "generated/${dir}/${type}_test-openmp.h"
      )
      set(_openmp_kernels_src ${type}_openmp_kernels_src)
      set(_openmp_test_fixture_src ${type}_openmp_test_fixture_src)
    endif()
  endif()
  breeze_add_tests(${dir} ${type} "_unittest" ".cpp" ${ARGN})
endfunction()

breeze_add_unittests(
  "functions"
  "function"
  "load"
  "store"
  "reduce"
  "scan"
  "sort"
)
breeze_add_unittests("algorithms" "algorithm" "reduce" "scan" "sort")
