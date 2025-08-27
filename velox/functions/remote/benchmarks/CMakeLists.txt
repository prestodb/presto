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

add_executable(velox_benchmark_local_remote_comparison LocalRemoteComparisonBenchmark.cpp)

target_link_libraries(
  velox_benchmark_local_remote_comparison
  PUBLIC
    velox_type
    velox_benchmark_builder
    velox_vector_test_lib
    velox_function_registry
    velox_functions_lib
    velox_functions_remote
    velox_functions_remote_utils
    Folly::folly
    Folly::follybenchmark
    gflags::gflags
    glog::glog
)
