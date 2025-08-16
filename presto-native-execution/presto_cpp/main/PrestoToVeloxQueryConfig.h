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
#pragma once

#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"

namespace facebook::velox::config {
class ConfigBase;
}

namespace facebook::velox::core {
class QueryConfig;
}

namespace facebook::presto {

/// Translates Presto configs to Velox 'QueryConfig'. Presto query session
/// properties take precedence over Presto system config properties.
velox::core::QueryConfig toVeloxConfigs(
    const protocol::SessionRepresentation& session);

std::unordered_map<std::string, std::shared_ptr<velox::config::ConfigBase>>
toConnectorConfigs(const protocol::TaskUpdateRequest& taskUpdateRequest);

} // namespace facebook::presto
