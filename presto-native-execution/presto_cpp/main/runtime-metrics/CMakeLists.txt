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

# Add as a source to presto_server to allow global reporter Singleton
# initialization.

find_package(prometheus-cpp CONFIG REQUIRED)

# Prepare a static library to link with prometheus_reporter_test
add_library(presto_prometheus_reporter OBJECT PrometheusStatsReporter.cpp)
target_link_libraries(presto_prometheus_reporter presto_common
                      prometheus-cpp::core)

if(PRESTO_ENABLE_TESTING)
  add_subdirectory(tests)
endif()
