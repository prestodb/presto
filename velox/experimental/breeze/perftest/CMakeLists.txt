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

function(breeze_add_perftests dir type)
  # only add test for platforms we support
  set(BUILD_OPENCL OFF)
  set(BUILD_METAL OFF)
  set(BUILD_SYCL OFF)
  set(BUILD_OPENMP OFF)
  set(BUILD_HIP OFF)

  if(BUILD_CUDA)
    set(_cuda_flags)
    set(_cuda_libs)
    list(APPEND _cuda_flags -DPERFTEST_EXT_TYPES=${PERFTEST_EXT_TYPES})
    list(APPEND _cuda_flags -I${CMAKE_SOURCE_DIR}/perftest)
    if(BUILD_TRACING)
      list(APPEND _cuda_flags -I${CMAKE_SOURCE_DIR}/../../external/perfetto/sdk)
      list(APPEND _cuda_flags -DTRACING=1)
    endif()
    breeze_add_tests(${dir} ${type} "_perftest" ".cu" ${ARGN})
  endif()
endfunction()

breeze_add_perftests("functions" "function" "reduce" "scan" "sort")
breeze_add_perftests("algorithms" "algorithm" "reduce" "scan" "sort")
breeze_add_perftests("queries" "query" "order_by")
