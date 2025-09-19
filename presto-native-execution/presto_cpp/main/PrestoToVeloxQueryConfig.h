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

/// Translates Presto configs to Velox 'QueryConfig' config map. Presto query 
/// session properties take precedence over Presto system config properties.
std::unordered_map<std::string, std::string> toVeloxConfigs(
    const protocol::SessionRepresentation& session);

/// Translates Presto configs to Velox 'QueryConfig' config map. It is the 
/// temporary overload that builds a QueryConfig from session properties and
/// extraCredentials, including all extraCredentials so they can be consumed by 
/// UDFs and connectors.
/// This implementation is a temporary solution until a more unified
/// configuration mechanism (TokenProvider) is available.
velox::core::QueryConfig toVeloxConfigs(
  const protocol::SessionRepresentation& session,
  const std::map<std::string, std::string>& extraCredentials);

std::unordered_map<std::string, std::shared_ptr<velox::config::ConfigBase>>
toConnectorConfigs(const protocol::TaskUpdateRequest& taskUpdateRequest);

} // namespace facebook::presto
