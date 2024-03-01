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

add_library(presto_exception Exception.cpp)
add_library(presto_common Counters.cpp Utils.cpp ConfigReader.cpp Configs.cpp)

target_link_libraries(presto_exception velox_exception)
set_property(TARGET presto_exception PROPERTY JOB_POOL_LINK
                                              presto_link_job_pool)

target_link_libraries(presto_common velox_config velox_core velox_exception)
set_property(TARGET presto_common PROPERTY JOB_POOL_LINK presto_link_job_pool)

if(PRESTO_ENABLE_TESTING)
  add_subdirectory(tests)
endif()
