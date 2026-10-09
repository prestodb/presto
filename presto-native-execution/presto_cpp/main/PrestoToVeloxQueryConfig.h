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
#include "velox/core/QueryCtx.h"

namespace facebook::velox::config {
class ConfigBase;
}

namespace facebook::presto {

/// Translates Presto configs to Velox 'QueryConfig' config map. Presto query
/// session properties take precedence over Presto system config properties.
std::unordered_map<std::string, std::string> toVeloxConfigs(
    const protocol::SessionRepresentation& session);

/// The configs a task runs with, and which of their entries hold credentials.
struct TaskConfigs {
  /// Session properties and system config, with the task's extra credentials
  /// flattened in.
  velox::core::QueryConfig queryConfig;

  /// Per catalog, that catalog's session properties with the same credentials
  /// flattened in.
  std::unordered_map<std::string, std::shared_ptr<velox::config::ConfigBase>>
      connectorConfigs;

  /// Where each credential landed. Reported from the writes that produced the
  /// configs above, because a credential and an ordinary setting are
  /// indistinguishable once both are config entries.
  velox::core::CredentialKeys credentialKeys;
};

/// Builds a task's configs from its session and extra credentials, together
/// with a record of which entries the credentials became. Flattening the
/// credentials into configs is a temporary solution until a more unified
/// configuration mechanism (TokenProvider) is available.
TaskConfigs toTaskConfigs(const protocol::TaskUpdateRequest& taskUpdateRequest);

std::unordered_map<std::string, std::string>
toVeloxConfigsFromSessionProperties(
    const std::map<std::string, std::string>& sessionProperties);

} // namespace facebook::presto
