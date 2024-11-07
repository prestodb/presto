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

add_subdirectory(tests)

add_library(velox_local_runner LocalRunner.cpp LocalSchema.cpp Runner.cpp)

target_link_libraries(
  velox_local_runner
  velox_common_base
  velox_memory
  velox_hive_connector
  velox_dwio_common
  velox_dwio_dwrf_writer
  velox_exec_test_lib
  velox_exec)
