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

# Set include directories for Presto and Velox headers
set(PRESTO_AND_VELOX_INCLUDE_DIRS
  ${CMAKE_SOURCE_DIR}/../presto-native-execution
  ${CMAKE_SOURCE_DIR}/../presto-native-execution/velox
)

# Find required packages
find_package(xsimd)
find_package(fmt REQUIRED)

# Set link libraries
set(CMAKE_DYLIB_TEST_LINK_LIBRARIES 
    fmt::fmt
    xsimd)

# On macOS, gflags is required
if(APPLE)
    message(STATUS "APPLE compilation requires gflags library. Appending.")
    find_package(gflags REQUIRED)
    list(APPEND CMAKE_DYLIB_TEST_LINK_LIBRARIES "gflags::gflags")
endif()

# Build the shared library plugin for dynamic loading at runtime
add_library(
  presto_testing_tvf_plugin SHARED
  TestingTableFunctions.cpp
  TestingTableFunctionsPlugin.cpp
)

# Set include directories
target_include_directories(presto_testing_tvf_plugin
  PRIVATE
    ${PRESTO_AND_VELOX_INCLUDE_DIRS}
)

# Link against required libraries
target_link_libraries(presto_testing_tvf_plugin
                      PRIVATE
                      ${CMAKE_DYLIB_TEST_LINK_LIBRARIES})

# Set platform-specific link options for dynamic loading
if(APPLE)
  set(COMMON_LIBRARY_LINK_OPTIONS "-Wl,-undefined,dynamic_lookup")
else()
  set(COMMON_LIBRARY_LINK_OPTIONS "-Wl,--exclude-libs,ALL")
endif()

target_link_options(presto_testing_tvf_plugin PRIVATE
                    ${COMMON_LIBRARY_LINK_OPTIONS})
