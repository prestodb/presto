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

add_library(presto_to_velox_remote_functions PrestoRestFunctionRegistration.cpp)
target_link_libraries(
  presto_to_velox_remote_functions
  presto_functions_remote
  velox_type_fbhive
  Boost::url
)

add_library(presto_functions_remote RestRemoteFunction.cpp)
target_link_libraries(presto_functions_remote presto_functions_rest_client velox_functions_remote)

add_subdirectory(client)

if(${PRESTO_ENABLE_TESTING})
  add_subdirectory(tests)
endif()
