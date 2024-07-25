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

include(FBThriftCppLibrary)

add_fbthrift_cpp_library(remote_function_thrift RemoteFunction.thrift SERVICES
                         RemoteFunctionService)

target_compile_options(remote_function_thrift
                       PRIVATE -Wno-deprecated-declarations)

velox_add_library(velox_functions_remote_get_serde GetSerde.cpp)
velox_link_libraries(velox_functions_remote_get_serde
                     PUBLIC remote_function_thrift velox_presto_serializer)
