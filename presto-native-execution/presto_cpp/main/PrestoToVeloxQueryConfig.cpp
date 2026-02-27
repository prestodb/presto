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
#include <glog/logging.h>
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/properties/session/CudfSessionProperties.h"
#include "presto_cpp/main/properties/session/SessionProperties.h"
#include "velox/common/compression/Compression.h"
#include "velox/core/QueryConfig.h"
#include "velox/experimental/cudf/common/CudfConfig.h"
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
    } else if (!sessionProperties->hasVeloxConfig(it.first)) {
      sessionProperties->updateSessionPropertyValue(it.first, it.second);
    } else {
      queryConfigs[sessionProperties->toVeloxConfig(it.first)] = it.second;
    }
  }

  if (session.startTime) {
    queryConfigs[velox::core::QueryConfig::kSessionStartTime] =
        std::to_string(session.startTime);
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
      {.prestoSystemConfig = std::string(SystemConfig::kQueryMaxMemoryPerNode),
       .veloxConfig = velox::core::QueryConfig::kQueryMaxMemoryPerNode},

      {
          .prestoSystemConfig =
              std::string(SystemConfig::kSpillerFileCreateConfig),
          .veloxConfig = velox::core::QueryConfig::kSpillFileCreateConfig,
      },

      {.prestoSystemConfig = std::string(SystemConfig::kSpillEnabled),
       .veloxConfig = velox::core::QueryConfig::kSpillEnabled},

      {.prestoSystemConfig = std::string(SystemConfig::kJoinSpillEnabled),
       .veloxConfig = velox::core::QueryConfig::kJoinSpillEnabled},

      {.prestoSystemConfig = std::string(SystemConfig::kOrderBySpillEnabled),
       .veloxConfig = velox::core::QueryConfig::kOrderBySpillEnabled},

      {.prestoSystemConfig =
           std::string(SystemConfig::kAggregationSpillEnabled),
       .veloxConfig = velox::core::QueryConfig::kAggregationSpillEnabled},

      {.prestoSystemConfig =
           std::string(SystemConfig::kRequestDataSizesMaxWaitSec),
       .veloxConfig = velox::core::QueryConfig::kRequestDataSizesMaxWaitSec},

      {.prestoSystemConfig = std::string(SystemConfig::kDriverMaxSplitPreload),
       .veloxConfig = velox::core::QueryConfig::kMaxSplitPreloadPerDriver},

      {.prestoSystemConfig =
           std::string(SystemConfig::kMaxLocalExchangeBufferSize),
       .veloxConfig = velox::core::QueryConfig::kMaxLocalExchangeBufferSize},

      {.prestoSystemConfig =
           std::string(SystemConfig::kMaxLocalExchangePartitionBufferSize),
       .veloxConfig =
           velox::core::QueryConfig::kMaxLocalExchangePartitionBufferSize},

      {.prestoSystemConfig =
           std::string(SystemConfig::kParallelOutputJoinBuildRowsEnabled),
       .veloxConfig =
           velox::core::QueryConfig::kParallelOutputJoinBuildRowsEnabled},

      {.prestoSystemConfig =
           std::string(SystemConfig::kHashProbeBloomFilterPushdownMaxSize),
       .veloxConfig =
           velox::core::QueryConfig::kHashProbeBloomFilterPushdownMaxSize},

      {.prestoSystemConfig = std::string(SystemConfig::kUseLegacyArrayAgg),
       .veloxConfig = velox::core::QueryConfig::kPrestoArrayAggIgnoreNulls},

      {.prestoSystemConfig = std::string{SystemConfig::kTaskWriterCount},
       .veloxConfig = velox::core::QueryConfig::kTaskWriterCount},

      {.prestoSystemConfig =
           std::string{SystemConfig::kTaskPartitionedWriterCount},
       .veloxConfig = velox::core::QueryConfig::kTaskPartitionedWriterCount},

      {.prestoSystemConfig = std::string{SystemConfig::kExchangeMaxBufferSize},
       .veloxConfig = velox::core::QueryConfig::kMaxExchangeBufferSize,
       .toVeloxPropertyValueConverter =
           [](const auto& value) {
             return folly::to<std::string>(velox::config::toCapacity(
                 value, velox::config::CapacityUnit::BYTE));
           }},

      {.prestoSystemConfig = std::string(SystemConfig::kSinkMaxBufferSize),
       .veloxConfig = velox::core::QueryConfig::kMaxOutputBufferSize,
       .toVeloxPropertyValueConverter =
           [](const auto& value) {
             return folly::to<std::string>(velox::config::toCapacity(
                 value, velox::config::CapacityUnit::BYTE));
           }},

      {.prestoSystemConfig =
           std::string(SystemConfig::kDriverMaxPagePartitioningBufferSize),
       .veloxConfig = velox::core::QueryConfig::kMaxPartitionedOutputBufferSize,
       .toVeloxPropertyValueConverter =
           [](const auto& value) {
             return folly::to<std::string>(velox::config::toCapacity(
                 value, velox::config::CapacityUnit::BYTE));
           }},

      {.prestoSystemConfig =
           std::string(SystemConfig::kTaskMaxPartialAggregationMemory),
       .veloxConfig = velox::core::QueryConfig::kMaxPartialAggregationMemory,
       .toVeloxPropertyValueConverter =
           [](const auto& value) {
             return folly::to<std::string>(velox::config::toCapacity(
                 value, velox::config::CapacityUnit::BYTE));
           }},

      {.prestoSystemConfig =
           std::string(SystemConfig::kExchangeLazyFetchingEnabled),
       .veloxConfig = velox::core::QueryConfig::kExchangeLazyFetchingEnabled},
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

void applyCudfConfigs(const protocol::SessionRepresentation& session) {
  using facebook::presto::cudf::CudfSessionProperties;
  using facebook::velox::cudf_velox::CudfConfig;

  std::unordered_map<std::string, std::string> cudfConfigs;
  auto* cudfSessionProperties = CudfSessionProperties::instance();

  // Iterate through session properties and extract cuDF configs
  for (const auto& [sessionPropName, sessionPropValue] :
       session.systemProperties) {
    // Use toVeloxConfig to get the mapped Velox config name
    // CudfConfig::updateConfigs() will only process keys it recognizes
    const auto veloxConfigName =
        cudfSessionProperties->toVeloxConfig(sessionPropName);

    if (!veloxConfigName.empty()) {
      cudfConfigs[veloxConfigName] = sessionPropValue;
    }
  }

  // Update CudfConfig with collected configs.
  if (!cudfConfigs.empty()) {
    try {
      CudfConfig::getInstance().updateConfigs(std::move(cudfConfigs));
    } catch (const std::exception& e) {
      LOG(ERROR) << "[CudfDebug] CudfConfig::updateConfigs() threw exception: "
                 << e.what();
      throw;
    }
  } else {
    LOG(INFO) << "[CudfDebug] No cuDF configs to update";
  }
}

} // namespace

std::unordered_map<std::string, std::string> toVeloxConfigs(
    const protocol::SessionRepresentation& session) {
  LOG(INFO) << "[CudfDebug] toVeloxConfigs() called";
  LOG(INFO) << "[CudfDebug] Session has " << session.systemProperties.size()
            << " system properties";

  std::unordered_map<std::string, std::string> configs;

  // Firstly apply Presto system properties to Velox query config.
  updateFromSystemConfigs(configs);

  // Secondly apply and possibly override with Presto session properties.
  updateFromSessionConfigs(session, configs);

  // Apply cuDF configs if enabled (after session properties to allow
  // overrides).
#ifdef PRESTO_ENABLE_CUDF
  LOG(INFO)
      << "[CudfDebug] PRESTO_ENABLE_CUDF is defined, calling applyCudfConfigs()";
  applyCudfConfigs(session);
#else
  LOG(INFO)
      << "[CudfDebug] PRESTO_ENABLE_CUDF is NOT defined, skipping applyCudfConfigs()";
#endif

  // Finally apply special case configs.
  updateVeloxConfigsWithSpecialCases(configs);
  return configs;
}

velox::core::QueryConfig toVeloxConfigs(
    const protocol::SessionRepresentation& session,
    const std::map<std::string, std::string>& extraCredentials) {
  // Start with the session-based configuration
  auto configs = toVeloxConfigs(session);

  // If there are any extra credentials, add them all to the config
  if (!extraCredentials.empty()) {
    // Create new config map with all extra credentials added
    configs.insert(extraCredentials.begin(), extraCredentials.end());
  }
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
