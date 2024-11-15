/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "presto_cpp/main/types/PrestoToVeloxConnector.h"
namespace facebook::presto {

namespace {
std::unordered_map<std::string, std::unique_ptr<const PrestoToVeloxConnector>>&
connectors() {
  static std::
      unordered_map<std::string, std::unique_ptr<const PrestoToVeloxConnector>>
          connectors;
  return connectors;
}
} // namespace

TypePtr stringToType(
    const std::string& typeString,
    const TypeParser& typeParser) {
  return typeParser.parse(typeString);
}
void registerPrestoToVeloxConnector(
    std::unique_ptr<const PrestoToVeloxConnector> connector) {
  auto connectorName = connector->connectorName();
  auto connectorProtocol = connector->createConnectorProtocol();
  VELOX_CHECK(
      connectors().insert({connectorName, std::move(connector)}).second,
      "Connector {} is already registered",
      connectorName);
  protocol::registerConnectorProtocol(
      connectorName, std::move(connectorProtocol));
}

void unregisterPrestoToVeloxConnector(const std::string& connectorName) {
  connectors().erase(connectorName);
  protocol::unregisterConnectorProtocol(connectorName);
}

const PrestoToVeloxConnector& getPrestoToVeloxConnector(
    const std::string& connectorName) {
  auto it = connectors().find(connectorName);
  VELOX_CHECK(
      it != connectors().end(), "Connector {} not registered", connectorName);
  return *(it->second);
}
} // namespace facebook::presto
