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

add_library(
  presto_function_metadata OBJECT
  BaseFunctionMetadataProvider.cpp
  NativeFunctionMetadata.cpp
)
target_link_libraries(presto_function_metadata presto_common velox_function_registry)

if(PRESTO_ENABLE_CUDF)
  add_library(presto_cudf_function_metadata OBJECT CudfFunctionMetadata.cpp)
  target_link_libraries(
    presto_cudf_function_metadata
    cudf::cudf
    presto_function_metadata
    presto_common
    velox_function_registry
  )
endif()

if(PRESTO_ENABLE_TESTING)
  add_subdirectory(tests)
endif()
