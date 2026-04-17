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
add_library(
  presto_federation_connector
  OBJECT
  ArrowFederationConnector.cpp
  ArrowFederationPrestoToVeloxConnector.cpp
)

target_compile_definitions(presto_federation_connector PUBLIC PRESTO_ENABLE_ARROW_FLIGHT_CONNECTOR)

target_link_libraries(
  presto_federation_connector
  velox_connector
  ArrowFlight::arrow_flight_shared
  presto_flight_connector_utils
  presto_flight_connector_auth
)
target_link_libraries(presto_federation_connector presto_flight_connector)
