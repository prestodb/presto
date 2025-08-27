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

# Use breeze PTX specialization by default for CUDA.
if(NOT DEFINED CUDA_PLATFORM_SPECIALIZATION_HEADER)
  set(
    CUDA_PLATFORM_SPECIALIZATION_HEADER
    breeze/platforms/specialization/cuda-ptx.cuh
    CACHE STRING
    "CUDA platform specialization header"
  )
endif()

# Add header only library for breeze and CUDA platform.
add_library(breeze_cuda INTERFACE)
target_include_directories(breeze_cuda INTERFACE ../breeze)
target_compile_definitions(
  breeze_cuda
  INTERFACE PLATFORM_CUDA CUDA_PLATFORM_SPECIALIZATION_HEADER=${CUDA_PLATFORM_SPECIALIZATION_HEADER}
)

add_subdirectory(common)
add_subdirectory(exec)
add_subdirectory(vector)
add_subdirectory(dwio)
