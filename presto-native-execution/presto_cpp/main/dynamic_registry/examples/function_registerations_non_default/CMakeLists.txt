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

add_library(presto_non_defualt_function_my_dynamic SHARED
            DynamicNonDefaultFunction.cpp)

set(CMAKE_DYLIB_TEST_LINK_LIBRARIES fmt::fmt gflags::gflags xsimd)
target_link_libraries(presto_non_defualt_function_my_dynamic
                      PRIVATE ${CMAKE_DYLIB_TEST_LINK_LIBRARIES})

if(APPLE)
  target_link_options(presto_non_defualt_function_my_dynamic PRIVATE
                      ${COMMON_LIBRARY_LINK_OPTIONS})
else()
  set(COMMON_LIBRARY_LINK_OPTIONS "-Wl,--exclude-libs,ALL")
endif()
