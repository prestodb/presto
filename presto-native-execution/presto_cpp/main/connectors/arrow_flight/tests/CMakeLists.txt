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
add_subdirectory(utils)

add_executable(presto_flight_connector_infra_test
               TestingArrowFlightServerTest.cpp)

add_test(presto_flight_connector_infra_test presto_flight_connector_infra_test)

target_link_libraries(
  presto_flight_connector_infra_test presto_protocol
  presto_flight_connector_test_lib GTest::gtest GTest::gtest_main ${GLOG})

add_executable(
  presto_flight_connector_test
  ArrowFlightConnectorTest.cpp ArrowFlightConnectorAuthTest.cpp
  ArrowFlightConnectorTlsTest.cpp ArrowFlightConnectorDataTypeTest.cpp
  ArrowFlightConfigTest.cpp)

set(DATA_DIR "${CMAKE_CURRENT_SOURCE_DIR}/data/tls_certs")

add_custom_target(
  copy_flight_test_data ALL
  COMMAND ${CMAKE_COMMAND} -E copy_directory ${DATA_DIR}
          $<TARGET_FILE_DIR:presto_flight_connector_test>/data/tls_certs)

add_test(presto_flight_connector_test presto_flight_connector_test)

target_link_libraries(
  presto_flight_connector_test
  velox_exec_test_lib
  presto_flight_connector
  gtest
  gtest_main
  presto_flight_connector_test_lib
  presto_protocol)
