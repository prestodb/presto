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

#include "SystemSessionProperties.h"
#include "presto_cpp/main/common/Configs.h"
#include "velox/core/QueryConfig.h"

namespace facebook::presto {
namespace {
// Utilty function to get default value from query config.
template <typename T>
std::optional<T> getVeloxDefault(const std::string& configName) {
  std::string value =
      BaseVeloxQueryConfig::instance()->getDefaultValue(configName);
  if (!value.empty()) {
    std::stringstream ss(value);
    T defaultValue;
    if (ss >> defaultValue) {
      return defaultValue;
    }
  }
  return std::nullopt;
}

// Template specialization for bool
template <>
std::optional<bool> getVeloxDefault(const std::string& configName) {
  std::string value =
      BaseVeloxQueryConfig::instance()->getDefaultValue(configName);
  if (!value.empty()) {
    return value == "true" ? true : false;
  }
  return std::nullopt;
}

} // namespace

#define ADD_SESSION_PROPERTY(                                          \
    name, type, description, hidden, velox_config_name, default_value) \
  sessionProperties_.emplace(                                          \
      name,                                                            \
      std::make_unique<SessionPropertyData<type>>(                     \
          name, description, hidden, velox_config_name, default_value))

SystemSessionProperties::SystemSessionProperties() {
  using velox::core::QueryConfig;
  // List of native session properties kept as the source of truth in
  // prestissimo. If you want to override velox default specify the default
  // value directly, else use getVeloxDefault() to use velox default.
  ADD_SESSION_PROPERTY(
      kAggregationSpillMemoryThreshold,
      int,
      "Native Execution only. The max memory that a final aggregation can use"
      " before spilling. If it is 0, then there is no limit",
      false,
      QueryConfig::kAggregationSpillMemoryThreshold,
      getVeloxDefault<int>(QueryConfig::kAggregationSpillMemoryThreshold));

  ADD_SESSION_PROPERTY(
      kExprEvalSimplified,
      bool,
      "Native Execution only. Enable simplified path in expression evaluation",
      false,
      QueryConfig::kExprEvalSimplified,
      getVeloxDefault<bool>(QueryConfig::kExprEvalSimplified));

  ADD_SESSION_PROPERTY(
      kJoinSpillMemoryThreshold,
      int,
      "Native Execution only. The max memory that hash join can use before "
      "spilling. If it is 0, then there is no limit",
      false,
      QueryConfig::kJoinSpillMemoryThreshold,
      getVeloxDefault<int>(QueryConfig::kJoinSpillMemoryThreshold));

  ADD_SESSION_PROPERTY(
      kOrderBySpillMemoryThreshold,
      int,
      "Native Execution only. The max memory that order by can use before "
      "spilling. If it is 0, then there is no limit",
      false,
      QueryConfig::kOrderBySpillMemoryThreshold,
      getVeloxDefault<int>(QueryConfig::kOrderBySpillMemoryThreshold));

  ADD_SESSION_PROPERTY(
      kMaxSpillLevel,
      int,
      "Native Execution only. The maximum allowed spilling level for hash join "
      "build.\n 0 is the initial spilling level, -1 means unlimited.",
      false,
      QueryConfig::kMaxSpillLevel,
      getVeloxDefault<int>(QueryConfig::kMaxSpillLevel));

  ADD_SESSION_PROPERTY(
      kMaxSpillFileSize,
      int,
      "The max allowed spill file size. If it is zero, then there is no limit.",
      false,
      QueryConfig::kMaxSpillFileSize,
      getVeloxDefault<int>(QueryConfig::kMaxSpillFileSize));

  ADD_SESSION_PROPERTY(
      kSpillCompressionCodec,
      std::string,
      "Native Execution only. The compression algorithm type to compress the "
      "spilled data.\n Supported compression codecs are: ZLIB, SNAPPY, LZO, "
      "ZSTD, LZ4 and GZIP. NONE means no compression.",
      false,
      QueryConfig::kSpillCompressionKind,
      getVeloxDefault<std::string>(QueryConfig::kSpillCompressionKind));

  ADD_SESSION_PROPERTY(
      kSpillWriteBufferSize,
      long,
      "Native Execution only. The maximum size in bytes to buffer the serialized "
      "spill data before writing to disk for IO efficiency.\n If set to zero, "
      "buffering is disabled.",
      false,
      QueryConfig::kSpillWriteBufferSize,
      getVeloxDefault<long>(QueryConfig::kSpillWriteBufferSize));

  ADD_SESSION_PROPERTY(
      kSpillFileCreateConfig,
      std::string,
      "Native Execution only. Config used to create spill files. This config is \n"
      "provided to underlying file system and the config is free form. The form should be\n"
      "defined by the underlying file system.",
      false,
      QueryConfig::kSpillFileCreateConfig,
      getVeloxDefault<std::string>(QueryConfig::kSpillFileCreateConfig));

  ADD_SESSION_PROPERTY(
      kJoinSpillEnabled,
      bool,
      "Native Execution only. Enable join spilling on native engine",
      false,
      QueryConfig::kJoinSpillEnabled,
      getVeloxDefault<bool>(QueryConfig::kJoinSpillEnabled));

  ADD_SESSION_PROPERTY(
      kWindowSpillEnabled,
      bool,
      "Native Execution only. Enable window spilling on native engine",
      false,
      QueryConfig::kWindowSpillEnabled,
      getVeloxDefault<bool>(QueryConfig::kWindowSpillEnabled));

  ADD_SESSION_PROPERTY(
      kWriterSpillEnabled,
      bool,
      "Native Execution only. Enable writer spilling on native engine",
      false,
      QueryConfig::kWriterSpillEnabled,
      getVeloxDefault<bool>(QueryConfig::kWriterSpillEnabled));

  ADD_SESSION_PROPERTY(
      kRowNumberSpillEnabled,
      bool,
      "Native Execution only. Enable row number spilling on native engine",
      false,
      QueryConfig::kRowNumberSpillEnabled,
      getVeloxDefault<bool>(QueryConfig::kRowNumberSpillEnabled));

  ADD_SESSION_PROPERTY(
      kJoinSpillPartitionBits,
      int,
      "Native Execution only. The number of bits (N) used to calculate the "
      "spilling partition number for hash join and RowNumber: 2 ^ N",
      false,
      QueryConfig::kJoinSpillPartitionBits,
      getVeloxDefault<int>(QueryConfig::kJoinSpillPartitionBits));

  ADD_SESSION_PROPERTY(
      kTopNRowNumberSpillEnabled,
      bool,
      "Native Execution only. Enable topN row number spilling on native engine",
      false,
      QueryConfig::kTopNRowNumberSpillEnabled,
      getVeloxDefault<bool>(QueryConfig::kTopNRowNumberSpillEnabled));

  ADD_SESSION_PROPERTY(
      kValidateOutputFromOperators,
      bool,
      "If set to true, then during execution of tasks, the output vectors of "
      "every operator are validated for consistency. This is an expensive check "
      "so should only be used for debugging. It can help debug issues where "
      "malformed vector cause failures or crashes by helping identify which "
      "operator is generating them.",
      false,
      QueryConfig::kValidateOutputFromOperators,
      getVeloxDefault<bool>(QueryConfig::kValidateOutputFromOperators));

  ADD_SESSION_PROPERTY(
      kLegacyTimestamp,
      bool,
      "Native Execution only. Use legacy TIME & TIMESTAMP semantics. Warning: "
      "this will be removed",
      false,
      QueryConfig::kAdjustTimestampToTimezone,
      true); // Overrides velox default value.

  ADD_SESSION_PROPERTY(
      kDriverCpuTimeSliceLimitMs,
      int,
      "Native Execution only. The cpu time slice limit in ms that a driver thread."
      "If not zero, can continuously run without yielding. If it is zero,then "
      "there is no limit.",
      false,
      QueryConfig::kDriverCpuTimeSliceLimitMs,
      1000); // Overrides velox default value.
}

#undef ADD_SESSION_PROPERTY

const std::map<std::string, std::unique_ptr<SessionProperty>>&
SystemSessionProperties::getSessionProperties() const {
  return sessionProperties_;
}

void SystemSessionProperties::updateDefaultValues(
    std::unordered_map<std::string, std::string>& configStrings) const {
  for (auto& property : sessionProperties_) {
    // Let's use unified default values defined in the prestissmo.
    if (!property.second->getDefaultValue().empty()) {
      configStrings[property.second->getVeloxConfigName()] =
          property.second->getDefaultValue();
    }
  }
}

void SystemSessionProperties::updateSessionValues(
    std::unordered_map<std::string, std::string>& configStrings,
    const std::map<std::string, std::string>& systemProperties) const {
  for (const auto& it : systemProperties) {
    configStrings[toVeloxConfig(it.first)] = it.second;
  }
}

std::string SystemSessionProperties::toVeloxConfig(
    const std::string& name) const {
  auto it = sessionProperties_.find(name);
  return it == sessionProperties_.end() ? name
                                        : it->second->getVeloxConfigName();
}

} // namespace facebook::presto
