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
add_library(presto_connectors Registration.cpp PrestoToVeloxConnector.cpp
                              SystemConnector.cpp)

if(PRESTO_ENABLE_ARROW_FLIGHT_CONNECTOR)
  add_subdirectory(arrow_flight)
  target_compile_definitions(presto_connectors
                             PUBLIC PRESTO_ENABLE_ARROW_FLIGHT_CONNECTOR)
  target_link_libraries(presto_connectors presto_flight_connector)
endif()

if(PRESTO_ENABLE_CUDF)
  target_link_libraries(presto_connectors velox_cudf_parquet_connector
                        cudf::cudf)
endif()

target_link_libraries(presto_connectors presto_velox_expr_conversion
                      velox_type_fbhive)
