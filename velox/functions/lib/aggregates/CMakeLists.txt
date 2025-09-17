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

add_subdirectory(noisy_aggregation)

velox_add_library(
  velox_functions_aggregates
  AverageAggregateBase.cpp
  CentralMomentsAggregatesBase.cpp
  Compare.cpp
  CovarianceAggregatesBase.cpp
  MinMaxAggregateBase.cpp
  SingleValueAccumulator.cpp
  ValueList.cpp
  ValueSet.cpp
  VarianceAggregatesBase.cpp
)

velox_link_libraries(velox_functions_aggregates velox_exec velox_presto_serializer Folly::folly)

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
elseif(${VELOX_BUILD_TEST_UTILS})
  add_subdirectory(tests/utils)
endif()
