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
add_subdirectory(hyperloglog)
add_subdirectory(tests)
add_subdirectory(benchmarks)

add_library(
  velox_aggregates OBJECT
  ApproxDistinct.cpp
  ApproxPercentile.cpp
  Arbitrary.cpp
  AggregateNames.h
  ArrayAgg.cpp
  AverageAggregates.cpp
  BitwiseAggregates.cpp
  BoolAggregates.cpp
  CountIfAggregate.cpp
  MapAgg.cpp
  MinMaxAggregates.cpp
  MinMaxByAggregates.cpp
  SimpleNumerics.cpp
  SingleValueAccumulator.cpp
  ValueList.cpp)

target_link_libraries(velox_aggregates velox_aggregates_hyperloglog velox_exec
                      ${FOLLY_WITH_DEPENDENCIES})
