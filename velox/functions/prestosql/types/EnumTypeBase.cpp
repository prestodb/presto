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

#include "velox/functions/prestosql/types/EnumTypeBase.h"
#include "velox/functions/prestosql/types/BigintEnumType.h"
#include "velox/functions/prestosql/types/VarcharEnumType.h"

namespace facebook::velox {
namespace {
// Flips the keys and values of the original map and validates that all values
// are unique.
template <typename TValue>
static std::unordered_map<TValue, std::string> toFlippedMap(
    const std::unordered_map<std::string, TValue>& map,
    const std::string& name) {
  std::unordered_map<TValue, std::string> flippedMap;
  for (const auto& [key, value] : map) {
    bool ok = flippedMap.emplace(value, key).second;
    VELOX_USER_CHECK(
        ok, "Invalid enum type {}, contains duplicate value {}", name, value);
  }
  return flippedMap;
}
} // namespace

template <typename TValue, typename TParameter, typename TPhysical>
EnumTypeBase<TValue, TParameter, TPhysical>::EnumTypeBase(
    const TParameter& parameters)
    : parameters_{TypeParameter(parameters)},
      name_{parameters.name},
      flippedMap_{toFlippedMap<TValue>(parameters.valuesMap, name_)} {}

template <typename TValue, typename TParameter, typename TPhysical>
std::string EnumTypeBase<TValue, TParameter, TPhysical>::flippedMapToString()
    const {
  std::ostringstream oss;
  oss << "{";
  std::map<std::string, TValue> sortedMap;
  for (const auto& [key, value] : flippedMap_) {
    sortedMap[value] = key;
  }
  for (auto it = sortedMap.begin(); it != sortedMap.end(); ++it) {
    if (it != sortedMap.begin()) {
      oss << ", ";
    }
    oss << "\"" << it->first << "\"" << ": ";
    if constexpr (std::is_same_v<TValue, std::string>) {
      oss << "\"" << it->second << "\"";
    } else {
      oss << it->second;
    }
  }
  oss << "}";
  return oss.str();
}

template <typename TValue, typename TParameter, typename TPhysical>
template <typename EnumType>
std::shared_ptr<const EnumType>
EnumTypeBase<TValue, TParameter, TPhysical>::getCached(
    const TParameter& parameter) {
  static const int maxCacheSize = 1000;
  static folly::Synchronized<Cache> kCache{Cache(maxCacheSize)};
  return kCache.withWLock([&](auto& cache) -> std::shared_ptr<const EnumType> {
    auto it = cache.find(parameter);
    if (it != cache.end()) {
      return std::static_pointer_cast<const EnumType>(it->second);
    }
    // Can't use std::make_shared because calling private ctor.
    auto instance = std::shared_ptr<const EnumType>(new EnumType(parameter));
    cache.insert(parameter, instance);
    return instance;
  });
}

template class EnumTypeBase<int64_t, LongEnumParameter, BigintType>;
template class EnumTypeBase<std::string, VarcharEnumParameter, VarcharType>;

template BigintEnumTypePtr
EnumTypeBase<int64_t, LongEnumParameter, BigintType>::getCached<BigintEnumType>(
    const LongEnumParameter&);
template VarcharEnumTypePtr
EnumTypeBase<std::string, VarcharEnumParameter, VarcharType>::getCached<
    VarcharEnumType>(const VarcharEnumParameter&);
} // namespace facebook::velox
