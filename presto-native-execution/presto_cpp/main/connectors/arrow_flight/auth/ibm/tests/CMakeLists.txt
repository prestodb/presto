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

add_executable(presto_flight_ibm_auth_test IbmFlightConnectorAuthTest.cpp)

add_test(presto_flight_ibm_auth_test presto_flight_ibm_auth_test)

target_link_libraries(
  presto_flight_ibm_auth_test
  presto_protocol
  presto_http
  presto_flight_connector
  presto_flight_connector_test_lib
  presto_flight_connector_ibm_auth
  GTest::gtest
  GTest::gtest_main)
