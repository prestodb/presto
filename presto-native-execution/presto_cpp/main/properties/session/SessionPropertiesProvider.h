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

#include <memory>
#include <unordered_map>
#include "presto_cpp/external/json/nlohmann/json.hpp"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/type/Type.h"

using json = nlohmann::json;

namespace facebook::presto {

/// This is the interface of the session property.
/// Note: This interface should align with java coordinator.
class SessionProperty {
 public:
  SessionProperty(
      const std::string& name,
      const std::string& description,
      const std::string& typeSignature,
      bool hidden,
      const std::optional<std::string> veloxConfig,
      const std::string& defaultValue)
      : metadata_({name, description, typeSignature, defaultValue, hidden}),
        veloxConfig_(veloxConfig),
        value_(defaultValue) {}

  const protocol::SessionPropertyMetadata getMetadata() {
    return metadata_;
  }

  const std::optional<std::string> getVeloxConfig() {
    return veloxConfig_;
  }

  const std::string getValue() {
    return value_;
  }

  void updateValue(const std::string& value) {
    value_ = value;
  }

  bool operator==(const SessionProperty& other) const {
    const auto otherMetadata = other.metadata_;
    return metadata_.name == otherMetadata.name &&
        metadata_.description == otherMetadata.description &&
        metadata_.typeSignature == otherMetadata.typeSignature &&
        metadata_.hidden == otherMetadata.hidden &&
        metadata_.defaultValue == otherMetadata.defaultValue &&
        veloxConfig_ == other.veloxConfig_;
  }

 private:
  const protocol::SessionPropertyMetadata metadata_;
  const std::optional<std::string> veloxConfig_;
  std::string value_;
};

/// Base class providing default implementations for session property
/// management. Subclasses can specialize initialization while inheriting common
/// serialization and configuration mapping functionality.
class SessionPropertiesProvider {
 public:
  virtual ~SessionPropertiesProvider() = default;

  /// Translate a config name to its equivalent Velox config name.
  /// Returns 'name' as is if there is no mapping.
  const std::string toVeloxConfig(const std::string& name) const;

  /// Serialize all properties to JSON.
  json serialize() const;

  /// Check if a property has a corresponding Velox config.
  inline bool hasVeloxConfig(const std::string& key) {
    auto sessionProperty = sessionProperties_.find(key);
    if (sessionProperty == sessionProperties_.end()) {
      // In this case a queryConfig is being created so we should return
      // true since it will also have a veloxConfig.
      return true;
    }
    return sessionProperty->second->getVeloxConfig().has_value();
  }

  /// Update the value of a session property.
  inline void updateSessionPropertyValue(
      const std::string& key,
      const std::string& value) {
    auto sessionProperty = sessionProperties_.find(key);
    VELOX_CHECK(sessionProperty != sessionProperties_.end());
    sessionProperty->second->updateValue(value);
  }

 protected:
  /// Add a session property with metadata (for use by subclasses during init).
  void addSessionProperty(
      const std::string& name,
      const std::string& description,
      const velox::TypePtr& type,
      bool isHidden,
      const std::optional<std::string> veloxConfig,
      const std::string& defaultValue);

  /// Map of session property name to SessionProperty
  std::unordered_map<std::string, std::shared_ptr<SessionProperty>>
      sessionProperties_;
};

} // namespace facebook::presto
