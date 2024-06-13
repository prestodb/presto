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

#include <optional>
#include <string>
#include <typeinfo>

namespace facebook::presto {

// Defines prossible property types.
enum class PropertyType {
  kInt,
  kBool,
  kLong,
  kString,
  // Add more types as needed.
  kUnknown
};

/// This is the interface of the session property.
/// Note: This interface should align with java coordinator.
class SessionProperty {
 public:
  SessionProperty() = default;

  virtual ~SessionProperty() = default;

  virtual std::string getName() const = 0;

  virtual std::string getDescription() const = 0;

  virtual PropertyType getType() const = 0;

  virtual std::string getDefaultValue() const = 0;

  virtual bool isHidden() const = 0;

  virtual std::string getVeloxConfigName() const = 0;
};

/// Template class for different types of the session property.
template <typename T>
class SessionPropertyData : public SessionProperty {
 public:
  /// Restricting session properties without data.
  SessionPropertyData() = delete;

  SessionPropertyData(
      const std::string& name,
      const std::string& description,
      const bool hidden,
      const std::string& veloxConfigName,
      const std::optional<T>& defaultValue = std::nullopt)
      : name_(name),
        description_(description),
        defaultValue_(defaultValue),
        hidden_(hidden),
        veloxConfigName_(veloxConfigName) {}

  inline std::string getName() const {
    return name_;
  }

  inline std::string getDescription() const {
    return description_;
  }

  inline PropertyType getType() const {
    const std::type_info& typeinfo = typeid(T);
    return (typeinfo == typeid(int))        ? PropertyType::kInt
        : (typeinfo == typeid(bool))        ? PropertyType::kBool
        : (typeinfo == typeid(long))        ? PropertyType::kLong
        : (typeinfo == typeid(std::string)) ? PropertyType::kString
                                            : PropertyType::kUnknown;
  }

  /// Returns default value set in prestissmo worker.
  inline std::string getDefaultValue() const {
    if (defaultValue_.has_value()) {
      return toString(defaultValue_.value());
    } else {
      // Return empty, if there is no default value for property.
      return "";
    }
  }

  inline bool isHidden() const {
    return hidden_;
  }

  inline std::string getVeloxConfigName() const {
    return veloxConfigName_;
  }

 private:
  template <typename U>
  std::string toString(const U& value) const {
    return std::to_string(value);
  }

  /// Template specialization for bool
  template <>
  std::string toString<bool>(const bool& value) const {
    return value ? "true" : "false";
  }

  /// Template specialization for string
  template <>
  std::string toString<std::string>(const std::string& value) const {
    return value;
  }

 private:
  const std::string name_;
  const std::string description_;
  const std::optional<T> defaultValue_;
  const bool hidden_;
  const std::string veloxConfigName_;
};

} // namespace facebook::presto
