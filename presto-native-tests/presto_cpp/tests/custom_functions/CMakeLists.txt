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
set(PRESTO_AND_VELOX_INCLUDE_DIRS
  ${CMAKE_SOURCE_DIR}/../presto-native-execution
  ${CMAKE_SOURCE_DIR}/../presto-native-execution/velox
)

add_library(presto_cpp_custom_functions_test SHARED CustomFunctions.cpp)

set(CMAKE_DYLIB_TEST_LINK_LIBRARIES fmt::fmt
                                    xsimd)

target_include_directories(presto_cpp_custom_functions_test
  PRIVATE
    ${PRESTO_AND_VELOX_INCLUDE_DIRS}
    presto_dynamic_function_registrar
)

target_link_libraries(presto_cpp_custom_functions_test
                      PRIVATE
                      ${CMAKE_DYLIB_TEST_LINK_LIBRARIES})

if(APPLE)
  set(COMMON_LIBRARY_LINK_OPTIONS "-Wl,-undefined,dynamic_lookup")
else()
  set(COMMON_LIBRARY_LINK_OPTIONS "-Wl,--exclude-libs,ALL")
endif()

target_link_options(presto_cpp_custom_functions_test PRIVATE
                    ${COMMON_LIBRARY_LINK_OPTIONS})
