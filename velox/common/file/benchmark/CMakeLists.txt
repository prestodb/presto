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

add_library(velox_read_benchmark_lib ReadBenchmark.cpp)

target_link_libraries(velox_read_benchmark_lib velox_file velox_exception
                      velox_exec_test_util ${FMT} ${FOLLY_WITH_DEPENDENCIES})

add_executable(velox_read_benchmark ReadBenchmarkMain.cpp)

target_link_libraries(velox_read_benchmark velox_read_benchmark_lib)
