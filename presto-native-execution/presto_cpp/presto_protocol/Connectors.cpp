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
#include "presto_cpp/presto_protocol/Connectors.h"
#include <unordered_map>
#include "velox/common/base/Exceptions.h"

namespace facebook::presto::protocol {

namespace {

std::unordered_map<std::string, std::string>& connectors() {
  static std::unordered_map<std::string, std::string> keyByName;
  return keyByName;
}
} // namespace

bool registerConnector(
    const std::string& connectorName,
    const std::string& connectorKey) {
  bool ok = connectors().insert({connectorName, connectorKey}).second;
  VELOX_CHECK(
      ok, "Connector with name {} is already registered", connectorName);
  return ok;
}

bool unregisterConnector(const std::string& connectorName) {
  auto count = connectors().erase(connectorName);
  return count == 1;
}

void registerHiveConnectors() {
  registerConnector("hive", "hive");
  registerConnector("hive-hadoop2", "hive");
}

const std::string& getConnectorKey(const std::string& connectorName) {
  auto it = connectors().find(connectorName);
  VELOX_CHECK(
      it != connectors().end(),
      "Connector with name {} not registered",
      connectorName);
  return it->second;
}

} // namespace facebook::presto::protocol
