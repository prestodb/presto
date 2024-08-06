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

velox_add_library(velox_exception Exceptions.cpp VeloxException.cpp
                  Exceptions.h)
velox_link_libraries(
  velox_exception
  PUBLIC velox_flag_definitions
         velox_process
         Folly::folly
         fmt::fmt
         gflags::gflags
         glog::glog)

velox_add_library(
  velox_common_base
  BitUtil.cpp
  Counters.cpp
  Fs.cpp
  PeriodicStatsReporter.cpp
  RandomUtil.cpp
  RawVector.cpp
  RuntimeMetrics.cpp
  SimdUtil.cpp
  SpillConfig.cpp
  SpillStats.cpp
  StatsReporter.cpp
  SuccinctPrinter.cpp)

velox_link_libraries(
  velox_common_base
  PUBLIC velox_exception Folly::folly fmt::fmt xsimd
  PRIVATE velox_common_compression velox_process velox_test_util glog::glog)

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
endif()

if(${VELOX_ENABLE_BENCHMARKS})
  add_subdirectory(benchmarks)
endif()

velox_add_library(velox_id_map BigintIdMap.cpp)
velox_link_libraries(
  velox_id_map
  velox_memory
  velox_flag_definitions
  velox_process
  glog::glog
  Folly::folly
  fmt::fmt
  gflags::gflags)

velox_add_library(velox_status Status.cpp)
velox_link_libraries(
  velox_status
  PUBLIC fmt::fmt Folly::folly
  PRIVATE glog::glog)
