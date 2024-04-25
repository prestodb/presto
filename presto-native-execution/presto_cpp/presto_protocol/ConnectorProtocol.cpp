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
#include "presto_cpp/presto_protocol/ConnectorProtocol.h"

namespace facebook::presto::protocol {

namespace {
std::unordered_map<std::string, std::unique_ptr<ConnectorProtocol>>&
protocols() {
  static std::unordered_map<std::string, std::unique_ptr<ConnectorProtocol>>
      protocols;
  return protocols;
}
} // namespace

void registerConnectorProtocol(
    const std::string& connectorName,
    std::unique_ptr<ConnectorProtocol> protocol) {
  VELOX_CHECK(
      protocols().insert({connectorName, std::move(protocol)}).second,
      "Protocol for connector {} is already registered",
      connectorName);
}

void unregisterConnectorProtocol(const std::string& connectorName) {
  protocols().erase(connectorName);
}

const ConnectorProtocol& getConnectorProtocol(
    const std::string& connectorName) {
  auto it = protocols().find(connectorName);
  VELOX_CHECK(
      it != protocols().end(),
      "Protocol for connector {} not registered",
      connectorName);
  return *(it->second);
}
} // namespace facebook::presto::protocol