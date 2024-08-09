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
#include "velox/core/QueryConfig.h"

#include "presto_cpp/main/SessionProperties.h"

using namespace facebook::presto;
using namespace facebook::velox;

namespace facebook::presto {

namespace {
const std::string boolToString(bool value) {
  return value ? "true" : "false";
}
} // namespace

void SessionProperties::addSessionProperty(
    const std::string& name,
    const std::string& description,
    const TypePtr& type,
    bool isHidden,
    const std::string& veloxConfigName,
    const std::optional<std::string> veloxDefault) {
  std::string defaultVal;
  if (veloxDefault.has_value()) {
    defaultVal = veloxDefault.value();
  } else {
    defaultVal = "none";
  }
  sessionProperties_[name] = std::make_unique<SessionProperty>(
      name,
      description,
      type->toString(),
      isHidden,
      veloxConfigName,
      defaultVal);
}

// List of native session properties is kept as the source of truth here.
SessionProperties::SessionProperties() {
  // Use empty instance to get default property values.
  velox::core::QueryConfig c{{}};
  using velox::core::QueryConfig;

  addSessionProperty(
      kExprEvalSimplified,
      "Native Execution only. Enable simplified path in expression evaluation",
      BOOLEAN(),
      false,
      QueryConfig::kExprEvalSimplified,
      boolToString(c.exprEvalSimplified()));

  addSessionProperty(
      kMaxSpillLevel,
      "Native Execution only. The maximum allowed spilling level for hash join "
      "build. 0 is the initial spilling level, -1 means unlimited.",
      INTEGER(),
      false,
      QueryConfig::kMaxSpillLevel,
      std::to_string(c.maxSpillLevel()));

  addSessionProperty(
      kMaxSpillFileSize,
      "The max allowed spill file size. If it is zero, then there is no limit.",
      INTEGER(),
      false,
      QueryConfig::kMaxSpillFileSize,
      std::to_string(c.maxSpillFileSize()));

  addSessionProperty(
      kSpillCompressionCodec,
      "Native Execution only. The compression algorithm type to compress the "
      "spilled data.\n Supported compression codecs are: ZLIB, SNAPPY, LZO, "
      "ZSTD, LZ4 and GZIP. NONE means no compression.",
      VARCHAR(),
      false,
      QueryConfig::kSpillCompressionKind,
      c.spillCompressionKind());

  addSessionProperty(
      kSpillWriteBufferSize,
      "Native Execution only. The maximum size in bytes to buffer the serialized "
      "spill data before writing to disk for IO efficiency. If set to zero, "
      "buffering is disabled.",
      BIGINT(),
      false,
      QueryConfig::kSpillWriteBufferSize,
      std::to_string(c.spillWriteBufferSize()));

  addSessionProperty(
      kSpillFileCreateConfig,
      "Native Execution only. Config used to create spill files. This config is "
      "provided to underlying file system and the config is free form. The form should be "
      "defined by the underlying file system.",
      VARCHAR(),
      false,
      QueryConfig::kSpillFileCreateConfig,
      c.spillFileCreateConfig());

  addSessionProperty(
      kJoinSpillEnabled,
      "Native Execution only. Enable join spilling on native engine",
      BOOLEAN(),
      false,
      QueryConfig::kJoinSpillEnabled,
      boolToString(c.joinSpillEnabled()));

  addSessionProperty(
      kWindowSpillEnabled,
      "Native Execution only. Enable window spilling on native engine",
      BOOLEAN(),
      false,
      QueryConfig::kWindowSpillEnabled,
      boolToString(c.windowSpillEnabled()));

  addSessionProperty(
      kWriterSpillEnabled,
      "Native Execution only. Enable writer spilling on native engine",
      BOOLEAN(),
      false,
      QueryConfig::kWriterSpillEnabled,
      boolToString(c.writerSpillEnabled()));

  addSessionProperty(
      kRowNumberSpillEnabled,
      "Native Execution only. Enable row number spilling on native engine",
      BOOLEAN(),
      false,
      QueryConfig::kRowNumberSpillEnabled,
      boolToString(c.rowNumberSpillEnabled()));

  addSessionProperty(
      kJoinSpillPartitionBits,
      "Native Execution only. The number of bits (N) used to calculate the "
      "spilling partition number for hash join and RowNumber: 2 ^ N",
      INTEGER(),
      false,
      QueryConfig::kJoinSpillPartitionBits,
      std::to_string(c.rowNumberSpillEnabled()));

  addSessionProperty(
      kNativeSpillerNumPartitionBits,
      "none",
      TINYINT(),
      false,
      QueryConfig::kSpillNumPartitionBits,
      std::to_string(c.spillNumPartitionBits())),

      addSessionProperty(
          kTopNRowNumberSpillEnabled,
          "Native Execution only. Enable topN row number spilling on native engine",
          BOOLEAN(),
          false,
          QueryConfig::kTopNRowNumberSpillEnabled,
          boolToString(c.topNRowNumberSpillEnabled()));

  addSessionProperty(
      kValidateOutputFromOperators,
      "If set to true, then during execution of tasks, the output vectors of "
      "every operator are validated for consistency. This is an expensive check "
      "so should only be used for debugging. It can help debug issues where "
      "malformed vector cause failures or crashes by helping identify which "
      "operator is generating them.",
      BOOLEAN(),
      false,
      QueryConfig::kValidateOutputFromOperators,
      boolToString(c.validateOutputFromOperators()));

  // If `legacy_timestamp` is true, the coordinator expects timestamp
  // conversions without a timezone to be converted to the user's
  // session_timezone.
  addSessionProperty(
      kLegacyTimestamp,
      "Native Execution only. Use legacy TIME & TIMESTAMP semantics. Warning: "
      "this will be removed",
      BOOLEAN(),
      false,
      QueryConfig::kAdjustTimestampToTimezone,
      // Overrides velox default value. legacy_timestamp default value is true
      // in the coordinator.
      "true");

  // TODO: remove this once cpu driver slicing config is turned on by default in
  // Velox.
  addSessionProperty(
      kDriverCpuTimeSliceLimitMs,
      "Native Execution only. The cpu time slice limit in ms that a driver thread. "
      "If not zero, can continuously run without yielding. If it is zero, then "
      "there is no limit.",
      INTEGER(),
      false,
      QueryConfig::kDriverCpuTimeSliceLimitMs,
      // Overrides velox default value. Set it to 1 second to be aligned with
      // Presto Java.
      std::to_string(1000));
}

const std::unordered_map<std::string, std::shared_ptr<SessionProperty>>&
SessionProperties::getSessionProperties() const {
  return sessionProperties_;
}

std::shared_ptr<SessionProperty> SessionProperties::getSessionProperty(
    const std::string& key) const {
  VELOX_USER_CHECK(
      (sessionProperties_.find(key) != sessionProperties_.end()),
      "Session property {} cannot be found",
      key);
  auto it = sessionProperties_.find(key);
  return it->second;
}

std::string SessionProperties::toVeloxConfig(const std::string& name) const {
  auto it = sessionProperties_.find(name);
  return it == sessionProperties_.end() ? name
                                        : it->second->getVeloxConfigName();
}

json SessionPropertyReporter::getSessionPropertyMetadata(
    const std::string& name) {
  const auto sessionProperty = sessionProperties_->getSessionProperty(name);
  json sessionPropertyJson;
  sessionPropertyJson["name"] = sessionProperty->getName();
  sessionPropertyJson["description"] = sessionProperty->getDescription();
  sessionPropertyJson["typeSignature"] = sessionProperty->getType();
  sessionPropertyJson["defaultValue"] = sessionProperty->getDefaultValue();
  sessionPropertyJson["hidden"] = sessionProperty->isHidden();
  return sessionPropertyJson;
}

json SessionPropertyReporter::getSessionPropertiesMetadata() {
  json j = json::array();
  std::unordered_map<std::string, std::shared_ptr<SessionProperty>>
      sessionProperties = sessionProperties_->getSessionProperties();
  for (const auto& sessionProperty : sessionProperties) {
    j.push_back(getSessionPropertyMetadata(sessionProperty.first));
  }
  return j;
}

} // namespace facebook::presto
