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
#include "presto_cpp/main/SessionProperties.h"
#include "velox/core/QueryConfig.h"

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
    const std::string& veloxConfigName,
    const std::string& veloxDefault) {
  sessionProperties_[name] =
      std::make_unique<SessionProperty>(name, veloxConfigName, veloxDefault);
}

SessionProperties::SessionProperties() {
  // Use empty instance to get default property values.
  velox::core::QueryConfig c{{}};
  using velox::core::QueryConfig;

  addSessionProperty(
      kExprEvalSimplified,
      QueryConfig::kExprEvalSimplified,
      boolToString(c.exprEvalSimplified()));

  addSessionProperty(
      kMaxSpillLevel,
      QueryConfig::kMaxSpillLevel,
      std::to_string(c.maxSpillLevel()));

  addSessionProperty(
      kMaxSpillFileSize,
      QueryConfig::kMaxSpillFileSize,
      std::to_string(c.maxSpillFileSize()));

  addSessionProperty(
      kSpillCompressionCodec,
      QueryConfig::kSpillCompressionKind,
      c.spillCompressionKind());

  addSessionProperty(
      kSpillWriteBufferSize,
      QueryConfig::kSpillWriteBufferSize,
      std::to_string(c.spillWriteBufferSize()));

  addSessionProperty(
      kSpillFileCreateConfig,
      QueryConfig::kSpillFileCreateConfig,
      c.spillFileCreateConfig());

  addSessionProperty(
      kJoinSpillEnabled,
      QueryConfig::kJoinSpillEnabled,
      boolToString(c.joinSpillEnabled()));

  addSessionProperty(
      kWindowSpillEnabled,
      QueryConfig::kWindowSpillEnabled,
      boolToString(c.windowSpillEnabled()));

  addSessionProperty(
      kWriterSpillEnabled,
      QueryConfig::kWriterSpillEnabled,
      boolToString(c.writerSpillEnabled()));

  addSessionProperty(
      kRowNumberSpillEnabled,
      QueryConfig::kRowNumberSpillEnabled,
      boolToString(c.rowNumberSpillEnabled()));

  addSessionProperty(
      kJoinSpillPartitionBits,
      QueryConfig::kJoinSpillPartitionBits,
      std::to_string(c.rowNumberSpillEnabled()));

  addSessionProperty(
      kNativeSpillerNumPartitionBits,
      QueryConfig::kSpillNumPartitionBits,
      std::to_string(c.spillNumPartitionBits())),

      addSessionProperty(
          kTopNRowNumberSpillEnabled,
          QueryConfig::kTopNRowNumberSpillEnabled,
          boolToString(c.topNRowNumberSpillEnabled()));

  addSessionProperty(
      kValidateOutputFromOperators,
      QueryConfig::kValidateOutputFromOperators,
      boolToString(c.validateOutputFromOperators()));

  // If `legacy_timestamp` is true, the coordinator expects timestamp
  // conversions without a timezone to be converted to the user's
  // session_timezone.
  addSessionProperty(
      kLegacyTimestamp,
      QueryConfig::kAdjustTimestampToTimezone,
      // Overrides velox default value. legacy_timestamp default value is true
      // in the coordinator.
      "true");

  // TODO: remove this once cpu driver slicing config is turned on by default in
  // Velox.
  addSessionProperty(
      kDriverCpuTimeSliceLimitMs,
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

} // namespace facebook::presto
