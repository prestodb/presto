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

add_library(http_filters AccessLogFilter.cpp HttpEndpointLatencyFilter.cpp
                         InternalAuthenticationFilter.cpp StatsFilter.cpp)

if(PRESTO_ENABLE_JWT)
  target_include_directories(
    http_filters PRIVATE ${CMAKE_SOURCE_DIR}/presto_cpp/external/json)
endif()

target_link_libraries(http_filters presto_common ${PROXYGEN_LIBRARIES})

if(PRESTO_ENABLE_TESTING)
  add_subdirectory(tests)
endif()
