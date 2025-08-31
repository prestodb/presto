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

#include <folly/Synchronized.h>
#include <folly/container/EvictingCacheMap.h>

#include "velox/functions/prestosql/types/BigintEnumType.h"

namespace facebook::velox {

namespace {
std::unordered_map<int64_t, std::string> toFlippedMap(
    const std::unordered_map<std::string, int64_t>& map,
    const std::string& name) {
  std::unordered_map<int64_t, std::string> flippedMap;
  for (const auto& [key, value] : map) {
    bool ok = flippedMap.emplace(value, key).second;
    VELOX_USER_CHECK(
        ok, "Invalid enum type {}, contains duplicate value {}", name, value);
  }
  return flippedMap;
}

std::string flippedMapToString(
    const std::unordered_map<int64_t, std::string>& flippedMap) {
  std::ostringstream oss;
  oss << "{";
  std::map<std::string, int64_t> sortedMap;
  for (const auto& [key, value] : flippedMap) {
    sortedMap[value] = key;
  }
  for (auto it = sortedMap.begin(); it != sortedMap.end(); ++it) {
    if (it != sortedMap.begin()) {
      oss << ", ";
    }
    oss << "\"" << it->first << "\"" << ": " << it->second;
  }
  oss << "}";
  return oss.str();
}

} // namespace

// Should only be called from get() to create a new instance.
BigintEnumType::BigintEnumType(const LongEnumParameter& parameters)
    : parameters_{TypeParameter(parameters)},
      name_{parameters.name},
      flippedMap_{toFlippedMap(parameters.valuesMap, name_)} {}

std::string BigintEnumType::toString() const {
  return fmt::format(
      "{}:BigintEnum({})", name_, flippedMapToString(flippedMap_));
}

const std::optional<std::string> BigintEnumType::keyAt(int64_t value) const {
  auto it = flippedMap_.find(value);
  if (it != flippedMap_.end()) {
    return it->second;
  }
  return std::nullopt;
}

// A thread-safe LRU cache to store instances of BigintEnumType.
using Cache = folly::EvictingCacheMap<
    LongEnumParameter,
    BigintEnumTypePtr,
    LongEnumParameter::Hash>;

BigintEnumTypePtr BigintEnumType::get(const LongEnumParameter& parameter) {
  static const int maxCacheSize = 1000;
  static folly::Synchronized<Cache> kCache{Cache(maxCacheSize)};
  return kCache.withWLock([&](auto& cache) -> BigintEnumTypePtr {
    auto it = cache.find(parameter);
    if (it != cache.end()) {
      return it->second;
    }
    // Can't use std::make_shared because calling private ctor.
    auto instance =
        std::shared_ptr<const BigintEnumType>(new BigintEnumType(parameter));
    cache.insert(parameter, instance);
    return instance;
  });
}

folly::dynamic BigintEnumType::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "Type";
  obj["type"] = name();
  // parameters_[0].longEnumLiteral is assumed to have a value since it is
  // constructed from a LongEnumParameter.
  obj["kLongEnumParam"] =
      parameters_[0].longEnumLiteral.value().serializeEnumParameter();
  return obj;
}

} // namespace facebook::velox
