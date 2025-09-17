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

#include "presto_cpp/main/PrestoToVeloxQueryConfig.h"
#include "presto_cpp/main/SessionProperties.h"
#include "presto_cpp/main/common/Configs.h"
#include "velox/common/compression/Compression.h"
#include "velox/core/QueryConfig.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::presto {
namespace {

void updateVeloxConfigsWithSpecialCases(
    std::unordered_map<std::string, std::string>& configStrings) {
  // If `legacy_timestamp` is true, the coordinator expects timestamp
  // conversions without a timezone to be converted to the user's
  // session_timezone.
  auto it = configStrings.find("legacy_timestamp");
  // `legacy_timestamp` default value is true in the coordinator.
  if ((it == configStrings.end()) || (folly::to<bool>(it->second))) {
    configStrings.emplace(
        velox::core::QueryConfig::kAdjustTimestampToTimezone, "true");
  }
  // TODO: remove this once cpu driver slicing config is turned on by default in
  // Velox.
  it = configStrings.find(velox::core::QueryConfig::kDriverCpuTimeSliceLimitMs);
  if (it == configStrings.end()) {
    // Set it to 1 second to be aligned with Presto Java.
    configStrings.emplace(
        velox::core::QueryConfig::kDriverCpuTimeSliceLimitMs, "1000");
  }
}

void updateFromSessionConfigs(
    const protocol::SessionRepresentation& session,
    std::unordered_map<std::string, std::string>& queryConfigs) {
  auto* sessionProperties = SessionProperties::instance();
  std::optional<std::string> traceFragmentId;
  std::optional<std::string> traceShardId;
  for (const auto& it : session.systemProperties) {
    if (it.first == SessionProperties::kQueryTraceFragmentId) {
      traceFragmentId = it.second;
    } else if (it.first == SessionProperties::kQueryTraceShardId) {
      traceShardId = it.second;
    } else if (it.first == SessionProperties::kShuffleCompressionCodec) {
      auto compression = it.second;
      std::transform(
          compression.begin(),
          compression.end(),
          compression.begin(),
          ::tolower);
      velox::common::CompressionKind compressionKind =
          velox::common::stringToCompressionKind(compression);
      queryConfigs[velox::core::QueryConfig::kShuffleCompressionKind] =
          velox::common::compressionKindToString(compressionKind);
    } else {
      queryConfigs[sessionProperties->toVeloxConfig(it.first)] = it.second;
    }
  }

  if (session.source) {
    queryConfigs[velox::core::QueryConfig::kSource] = *session.source;
  }
  if (!session.clientTags.empty()) {
    queryConfigs[velox::core::QueryConfig::kClientTags] =
        folly::join(',', session.clientTags);
  }

  // If there's a timeZoneKey, convert to timezone name and add to the
  // configs. Throws if timeZoneKey can't be resolved.
  if (session.timeZoneKey != 0) {
    queryConfigs.emplace(
        velox::core::QueryConfig::kSessionTimezone,
        velox::tz::getTimeZoneName(session.timeZoneKey));
  }

  // Construct query tracing regex and pass to Velox config.
  // It replaces the given native_query_trace_task_reg_exp if also set.
  if (traceFragmentId.has_value() || traceShardId.has_value()) {
    queryConfigs.emplace(
        velox::core::QueryConfig::kQueryTraceTaskRegExp,
        ".*\\." + traceFragmentId.value_or(".*") + "\\..*\\." +
            traceShardId.value_or(".*") + "\\..*");
  }
}

void updateFromSystemConfigs(
    std::unordered_map<std::string, std::string>& queryConfigs) {
  const auto& systemConfig = SystemConfig::instance();
  struct ConfigMapping {
    std::string prestoSystemConfig;
    std::string veloxConfig;
    std::function<std::string(const std::string&)>
        toVeloxPropertyValueConverter{
            [](const std::string& prestoValue) { return prestoValue; }};
  };

  static const std::vector<ConfigMapping> veloxToPrestoConfigMapping{
      {std::string(SystemConfig::kQueryMaxMemoryPerNode),
       velox::core::QueryConfig::kQueryMaxMemoryPerNode},

      {
          std::string(SystemConfig::kSpillerFileCreateConfig),
          velox::core::QueryConfig::kSpillFileCreateConfig,
      },

      {std::string(SystemConfig::kSpillEnabled),
       velox::core::QueryConfig::kSpillEnabled},

      {std::string(SystemConfig::kJoinSpillEnabled),
       velox::core::QueryConfig::kJoinSpillEnabled},

      {std::string(SystemConfig::kOrderBySpillEnabled),
       velox::core::QueryConfig::kOrderBySpillEnabled},

      {std::string(SystemConfig::kAggregationSpillEnabled),
       velox::core::QueryConfig::kAggregationSpillEnabled},

      {std::string(SystemConfig::kRequestDataSizesMaxWaitSec),
       velox::core::QueryConfig::kRequestDataSizesMaxWaitSec},

      {std::string(SystemConfig::kDriverMaxSplitPreload),
       velox::core::QueryConfig::kMaxSplitPreloadPerDriver},

      {std::string(SystemConfig::kMaxLocalExchangePartitionBufferSize),
       velox::core::QueryConfig::kMaxLocalExchangePartitionBufferSize},

      {std::string(SystemConfig::kUseLegacyArrayAgg),
       velox::core::QueryConfig::kPrestoArrayAggIgnoreNulls},

      {std::string{SystemConfig::kTaskWriterCount},
       velox::core::QueryConfig::kTaskWriterCount},

      {std::string{SystemConfig::kTaskPartitionedWriterCount},
       velox::core::QueryConfig::kTaskPartitionedWriterCount},

      {std::string(SystemConfig::kSinkMaxBufferSize),
       velox::core::QueryConfig::kMaxOutputBufferSize,
       [](const auto& value) {
         return folly::to<std::string>(velox::config::toCapacity(
             value, velox::config::CapacityUnit::BYTE));
       }},

      {std::string(SystemConfig::kDriverMaxPagePartitioningBufferSize),
       velox::core::QueryConfig::kMaxPartitionedOutputBufferSize,
       [](const auto& value) {
         return folly::to<std::string>(velox::config::toCapacity(
             value, velox::config::CapacityUnit::BYTE));
       }},

      {std::string(SystemConfig::kTaskMaxPartialAggregationMemory),
       velox::core::QueryConfig::kMaxPartialAggregationMemory,
       [](const auto& value) {
         return folly::to<std::string>(velox::config::toCapacity(
             value, velox::config::CapacityUnit::BYTE));
       }},
  };

  for (const auto& configMapping : veloxToPrestoConfigMapping) {
    const auto& veloxConfigName = configMapping.veloxConfig;
    const auto& systemConfigName = configMapping.prestoSystemConfig;
    const auto propertyOpt = systemConfig->optionalProperty(systemConfigName);
    if (propertyOpt.has_value()) {
      queryConfigs[veloxConfigName] =
          configMapping.toVeloxPropertyValueConverter(propertyOpt.value());
    }
  }
}
} // namespace

velox::core::QueryConfig toVeloxConfigs(
    const protocol::SessionRepresentation& session) {
  std::unordered_map<std::string, std::string> configs;

  // Firstly apply Presto system properties to Velox query config.
  updateFromSystemConfigs(configs);

  // Secondly apply and possibly override with Presto session properties.
  updateFromSessionConfigs(session, configs);

  // Finally apply special case configs.
  updateVeloxConfigsWithSpecialCases(configs);
  return velox::core::QueryConfig(configs);
}

std::unordered_map<std::string, std::shared_ptr<velox::config::ConfigBase>>
toConnectorConfigs(const protocol::TaskUpdateRequest& taskUpdateRequest) {
  std::unordered_map<std::string, std::shared_ptr<velox::config::ConfigBase>>
      connectorConfigs;
  for (const auto& entry : taskUpdateRequest.session.catalogProperties) {
    std::unordered_map<std::string, std::string> connectorConfig;
    // remove native prefix from native connector session property names
    for (const auto& sessionProperty : entry.second) {
      auto veloxConfig = (sessionProperty.first.rfind("native_", 0) == 0)
          ? sessionProperty.first.substr(7)
          : sessionProperty.first;
      connectorConfig.emplace(veloxConfig, sessionProperty.second);
    }
    connectorConfig.insert(
        taskUpdateRequest.extraCredentials.begin(),
        taskUpdateRequest.extraCredentials.end());
    connectorConfig.insert({"user", taskUpdateRequest.session.user});
    if (taskUpdateRequest.session.source) {
      connectorConfig.insert({"source", *taskUpdateRequest.session.source});
    }
    if (taskUpdateRequest.session.schema) {
      connectorConfig.insert({"schema", *taskUpdateRequest.session.schema});
    }
    connectorConfigs.insert(
        {entry.first,
         std::make_shared<velox::config::ConfigBase>(
             std::move(connectorConfig))});
  }

  return connectorConfigs;
}

} // namespace facebook::presto
