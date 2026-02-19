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

#include "presto_cpp/main/properties/session/SessionPropertiesProvider.h"
#include <boost/algorithm/string.hpp>
#include "presto_cpp/main/common/Utils.h"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"

namespace facebook::presto {

void SessionPropertiesProvider::addSessionProperty(
    const std::string& name,
    const std::string& description,
    const facebook::velox::TypePtr& type,
    bool isHidden,
    const std::optional<std::string> veloxConfig,
    const std::string& defaultValue) {
  sessionProperties_[name] = std::make_shared<SessionProperty>(
      name,
      description,
      boost::algorithm::to_lower_copy(type->toString()),
      isHidden,
      veloxConfig,
      defaultValue);
}

const std::string SessionPropertiesProvider::toVeloxConfig(
    const std::string& name) const {
  auto it = sessionProperties_.find(name);
  if (it != sessionProperties_.end() &&
      it->second->getVeloxConfig().has_value()) {
    return it->second->getVeloxConfig().value();
  }
  return name;
}

json SessionPropertiesProvider::serialize() const {
  json j = json::array();
  json tj;
  for (const auto& sessionProperty : sessionProperties_) {
    protocol::to_json(tj, sessionProperty.second->getMetadata());
    j.push_back(tj);
  }
  return j;
}

} // namespace facebook::presto
