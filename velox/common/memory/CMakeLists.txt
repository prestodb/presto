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
if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
endif()

velox_add_library(
  velox_memory
  Allocation.cpp
  AllocationPool.cpp
  ArbitrationOperation.cpp
  ArbitrationParticipant.cpp
  ByteStream.cpp
  HashStringAllocator.cpp
  MallocAllocator.cpp
  Memory.cpp
  MemoryAllocator.cpp
  MemoryArbitrator.cpp
  MemoryPool.cpp
  MmapAllocator.cpp
  MmapArena.cpp
  RawVector.cpp
  SharedArbitrator.cpp
  StreamArena.cpp
)

velox_link_libraries(
  velox_memory
  PUBLIC
    velox_common_base
    velox_common_config
    velox_exception
    velox_flag_definitions
    velox_time
    velox_type
    Folly::folly
    fmt::fmt
    gflags::gflags
    glog::glog
  PRIVATE velox_test_util re2::re2
)
