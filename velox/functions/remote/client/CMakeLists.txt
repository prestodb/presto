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

velox_add_library(velox_functions_remote_thrift_client ThriftClient.cpp)
velox_link_libraries(velox_functions_remote_thrift_client
                     PUBLIC remote_function_thrift FBThrift::thriftcpp2)

velox_add_library(velox_functions_remote Remote.cpp)
velox_link_libraries(
  velox_functions_remote
  PUBLIC velox_expression
         velox_functions_remote_thrift_client
         velox_functions_remote_get_serde
         velox_type_fbhive
         Folly::folly)

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
endif()
