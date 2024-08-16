/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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

#include <functional>
#include <map>
#include <shared_mutex>
#include <unordered_map>

#include "folly/Conv.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::velox::config {

enum class CapacityUnit {
  BYTE,
  KILOBYTE,
  MEGABYTE,
  GIGABYTE,
  TERABYTE,
  PETABYTE
};

double toBytesPerCapacityUnit(CapacityUnit unit);

CapacityUnit valueOfCapacityUnit(const std::string& unitStr);

/// Convert capacity string with unit to the capacity number in the specified
/// units
uint64_t toCapacity(const std::string& from, CapacityUnit to);

std::chrono::duration<double> toDuration(const std::string& str);

/// The concrete config class should inherit the config base and define all the
/// entries.
class ConfigBase {
 public:
  template <typename T>
  struct Entry {
    Entry(
        const std::string& _key,
        const T& _val,
        std::function<std::string(const T&)> _toStr =
            [](const T& val) { return folly::to<std::string>(val); },
        std::function<T(const std::string&, const std::string&)> _toT =
            [](const std::string& k, const std::string& v) {
              auto converted = folly::tryTo<T>(v);
              VELOX_CHECK(
                  converted.hasValue(),
                  fmt::format(
                      "Invalid configuration for key '{}'. Value '{}' cannot be converted to type {}.",
                      k,
                      v,
                      folly::demangle(typeid(T))));
              return converted.value();
            })
        : key{_key}, defaultVal{_val}, toStr{_toStr}, toT{_toT} {}

    const std::string key;
    const T defaultVal;
    const std::function<std::string(const T&)> toStr;
    const std::function<T(const std::string&, const std::string&)> toT;
  };

  ConfigBase(
      std::unordered_map<std::string, std::string>&& configs,
      bool _mutable = false)
      : configs_(std::move(configs)), mutable_(_mutable) {}

  virtual ~ConfigBase() {}

  template <typename T>
  ConfigBase& set(const Entry<T>& entry, const T& val) {
    VELOX_CHECK(mutable_, "Cannot set in immutable config");
    std::unique_lock<std::shared_mutex> l(mutex_);
    configs_[entry.key] = entry.toStr(val);
    return *this;
  }

  ConfigBase& set(const std::string& key, const std::string& val);

  template <typename T>
  ConfigBase& unset(const Entry<T>& entry) {
    VELOX_CHECK(mutable_, "Cannot unset in immutable config");
    std::unique_lock<std::shared_mutex> l(mutex_);
    configs_.erase(entry.key);
    return *this;
  }

  ConfigBase& reset();

  template <typename T>
  T get(const Entry<T>& entry) const {
    std::shared_lock<std::shared_mutex> l(mutex_);
    auto iter = configs_.find(entry.key);
    return iter != configs_.end() ? entry.toT(entry.key, iter->second)
                                  : entry.defaultVal;
  }

  template <typename T>
  folly::Optional<T> get(
      const std::string& key,
      std::function<T(std::string, std::string)> toT = [](auto /* unused */,
                                                          auto value) {
        return folly::to<T>(value);
      }) const {
    auto val = get(key);
    if (val.hasValue()) {
      return toT(key, val.value());
    } else {
      return folly::none;
    }
  }

  template <typename T>
  T get(
      const std::string& key,
      const T& defaultValue,
      std::function<T(std::string, std::string)> toT = [](auto /* unused */,
                                                          auto value) {
        return folly::to<T>(value);
      }) const {
    auto val = get(key);
    if (val.hasValue()) {
      return toT(key, val.value());
    } else {
      return defaultValue;
    }
  }

  bool valueExists(const std::string& key) const;

  const std::unordered_map<std::string, std::string>& rawConfigs() const;

  std::unordered_map<std::string, std::string> rawConfigsCopy() const;

 protected:
  mutable std::shared_mutex mutex_;
  std::unordered_map<std::string, std::string> configs_;

 private:
  folly::Optional<std::string> get(const std::string& key) const;

  const bool mutable_;
};

} // namespace facebook::velox::config
