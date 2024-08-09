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

#include "velox/type/Type.h"

#include "presto_cpp/external/json/nlohmann/json.hpp"

using json = nlohmann::json;

namespace facebook::presto {

/// This is the interface of the session property.
/// Note: This interface should align with java coordinator.
class SessionProperty {
 public:
  SessionProperty(
      const std::string& name,
      const std::string& description,
      const std::string& type,
      bool hidden,
      const std::string& veloxConfigName,
      const std::string& defaultValue)
      : name_(name),
        description_(description),
        type_(type),
        hidden_(hidden),
        veloxConfigName_(veloxConfigName),
        defaultValue_(defaultValue) {}

  std::string getName() const {
    return name_;
  }

  std::string getDescription() const {
    return description_;
  }

  // Get the datatype of presto native property.
  std::string getType() const {
    return type_;
  }

  std::string getDefaultValue() const {
    return defaultValue_;
  }

  bool isHidden() const {
    return hidden_;
  }

  std::string getVeloxConfigName() const {
    return veloxConfigName_;
  }

  bool operator==(const SessionProperty& other) const {
    return name_ == other.name_ && description_ == other.description_ &&
        type_ == other.type_ && hidden_ == other.hidden_ &&
        veloxConfigName_ == other.veloxConfigName_ &&
        defaultValue_ == other.defaultValue_;
  }

 private:
  const std::string name_;
  const std::string description_;
  const std::string type_;
  const bool hidden_;
  const std::string veloxConfigName_;
  const std::string defaultValue_;
};

/// Defines all system session properties supported by native worker to ensure
/// that they are the source of truth and to differentiate them from Java based
/// session properties. Also maps the native session properties to velox.
class SessionProperties {
 public:
  /// Enable simplified path in expression evaluation.
  static constexpr const char* kExprEvalSimplified =
      "native_simplified_expression_evaluation_enabled";

  /// Enable join spilling on native engine.
  static constexpr const char* kJoinSpillEnabled = "native_join_spill_enabled";

  /// The maximum allowed spilling level for hash join build.
  static constexpr const char* kMaxSpillLevel = "native_max_spill_level";

  /// The maximum allowed spill file size.
  static constexpr const char* kMaxSpillFileSize = "native_max_spill_file_size";

  /// Enable row number spilling on native engine.
  static constexpr const char* kRowNumberSpillEnabled =
      "native_row_number_spill_enabled";

  /// The compression algorithm type to compress the spilled data.
  static constexpr const char* kSpillCompressionCodec =
      "native_spill_compression_codec";

  /// The maximum size in bytes to buffer the serialized spill data before
  /// writing to disk for IO efficiency.
  static constexpr const char* kSpillWriteBufferSize =
      "native_spill_write_buffer_size";

  /// Config used to create spill files. This config is provided to underlying
  /// file system and the config is free form.
  static constexpr const char* kSpillFileCreateConfig =
      "native_spill_file_create_config";

  /// Enable window spilling on native engine.
  static constexpr const char* kWindowSpillEnabled =
      "native_window_spill_enabled";

  /// Enable writer spilling on native engine.
  static constexpr const char* kWriterSpillEnabled =
      "native_writer_spill_enabled";

  /// The number of bits (N) used to calculate the spilling
  /// partition number for hash join and RowNumber: 2 ^ N
  static constexpr const char* kJoinSpillPartitionBits =
      "native_join_spiller_partition_bits";

  static constexpr const char* kNativeSpillerNumPartitionBits =
      "native_spiller_num_partition_bits";

  /// Enable topN row number spilling on native engine.
  static constexpr const char* kTopNRowNumberSpillEnabled =
      "native_topn_row_number_spill_enabled";

  /// If set to true, then during execution of tasks, the output vectors of
  /// every operator are validated for consistency. This is an expensive check
  /// so should only be used for debugging.
  static constexpr const char* kValidateOutputFromOperators =
      "native_debug_validate_output_from_operators";

  /// Enable timezone-less timestamp conversions.
  static constexpr const char* kLegacyTimestamp = "legacy_timestamp";

  /// Specifies the cpu time slice limit in ms that a driver thread
  /// can continuously run without yielding.
  static constexpr const char* kDriverCpuTimeSliceLimitMs =
      "driver_cpu_time_slice_limit_ms";

  SessionProperties();

  std::shared_ptr<SessionProperty> getSessionProperty(
      const std::string& name) const;

  const std::unordered_map<std::string, std::shared_ptr<SessionProperty>>&
  getSessionProperties() const;

  /// Utility function to translate a config name in Presto to its equivalent in
  /// Velox. Returns 'name' as is if there is no mapping.
  std::string toVeloxConfig(const std::string& name) const;

 protected:
  void addSessionProperty(
      const std::string& name,
      const std::string& description,
      const velox::TypePtr& type,
      bool isHidden,
      const std::string& veloxConfigName,
      const std::optional<std::string> veloxDefault);

  std::unordered_map<std::string, std::shared_ptr<SessionProperty>>
      sessionProperties_;
};

class SessionPropertyReporter {
 public:
  SessionPropertyReporter() {
    sessionProperties_ = std::make_shared<SessionProperties>();
  }

  json getSessionPropertyMetadata(const std::string& name);

  json getSessionPropertiesMetadata();

 protected:
  std::shared_ptr<SessionProperties> sessionProperties_;
};

} // namespace facebook::presto
