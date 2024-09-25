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
# To test functions being added by dynamically linked libraries, we compile
# `MyDynamicTestFunction.cpp` as a small .so library, and use the
# MY_DYNAMIC_FUNCTION_LIBRARY_PATH macro to locate the .so binary.
add_compile_definitions(
  MY_DYNAMIC_FUNCTION_LIBRARY_PATH="${CMAKE_CURRENT_BINARY_DIR}")
add_library(presto_function_my_dynamic SHARED MyDynamicTestFunction.cpp)
target_link_libraries(presto_function_my_dynamic PRIVATE xsimd fmt::fmt 
velox_shared_simple_functions velox_vector velox_core velox_common_base )  
# loads with velox_expression but not with velox_shared_simple_functions

# Here's the actual test which will dynamically load the library defined above.
add_executable(presto_function_dynamic_link_test DynamicLinkTest.cpp)

add_test(NAME presto_function_dynamic_link_test
         COMMAND presto_function_dynamic_link_test)

target_link_libraries(
  presto_function_dynamic_link_test
  velox_functions_test_lib
  presto_dynamic_function_loader
  velox_function_registry
  xsimd
  gmock
  ${GTEST_BOTH_LIBRARIES})
  #velox_shared_simple_functions

# these are the testing flags all in a dynamic library
add_library(presto_flags SHARED FlagLibrary.cpp)
target_link_libraries(presto_flags gflags::gflags)

set_target_properties(presto_function_my_dynamic
PROPERTIES
CXX_VISIBILITY_PRESET default
VISIBILITY_INLINES_HIDDEN YES)