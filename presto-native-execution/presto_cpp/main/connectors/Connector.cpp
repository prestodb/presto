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

#include "presto_cpp/main/connectors/Connector.h"

namespace facebook::presto {

// Static function to list all registered connector factories
std::vector<std::string> listConnectorFactories() {
  std::vector<std::string> names;
  const auto& factories = detail::connectorFactories();
  names.reserve(factories.size());
  for (const auto& [name, factory] : factories) {
    names.push_back(name);
  }
  return names;
}

} // namespace facebook::presto
