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

add_library(velox_exception Exceptions.cpp VeloxException.cpp Exceptions.h)
target_link_libraries(
  velox_exception
  velox_flag_definitions
  velox_process
  glog::glog
  ${FOLLY_WITH_DEPENDENCIES}
  ${FMT}
  ${GFLAGS_LIBRARIES})

add_library(
  velox_common_base BitUtil.cpp RandomUtil.cpp RawVector.cpp RuntimeMetrics.cpp
                    SimdUtil.cpp SuccinctPrinter.cpp)

target_link_libraries(velox_common_base velox_exception velox_process)

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
endif()

if(${VELOX_ENABLE_BENCHMARKS})
  add_subdirectory(benchmarks)
endif()
