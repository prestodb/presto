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

#include <folly/Synchronized.h>
#include <folly/container/EvictingCacheMap.h>

#include "velox/type/Type.h"

namespace facebook::velox {

/// Template base class for enum types that provides common functionality
/// for BigintEnumType and VarcharEnumType.
/// @tparam TValue Type of enum value, either int64_t or std::string
/// @tparam TParameter TypeParameter used to instantiate enum type, either
/// LongEnumParameter or VarcharEnumParameter
/// @tparam TPhysical Type extended by the enum type.
template <typename TValue, typename TParameter, typename TPhysical>
class EnumTypeBase : public TPhysical {
 public:
  using TReverseMap = std::unordered_map<TValue, std::string>;

  /// Stores instances of enum types using the type parameters as keys.
  using Cache = folly::EvictingCacheMap<
      TParameter,
      std::shared_ptr<const EnumTypeBase>,
      typename TParameter::Hash>;

  bool equivalent(const Type& other) const override {
    return this == &other;
  }

  const std::vector<TypeParameter>& parameters() const override {
    return parameters_;
  }

  bool containsValue(const TValue& value) const {
    return flippedMap_.contains(value);
  }

  /// Returns the string key given a value. If the value does not exist
  /// in the flippedMap_, return std::nullopt.
  const std::optional<std::string> keyAt(const TValue& value) const {
    auto it = flippedMap_.find(value);
    if (it != flippedMap_.end()) {
      return it->second;
    }
    return std::nullopt;
  }

  const std::string& enumName() const {
    return name_;
  }

 protected:
  explicit EnumTypeBase(const TParameter& parameters);

  /// Converts the flipped map to a string representation for toString() method.
  std::string flippedMapToString() const;

  /// Returns cached instance of enum type or creates and returns a new enum
  /// type if the type has not already been instantiated with the given
  /// parameters.
  template <typename EnumType>
  static std::shared_ptr<const EnumType> getCached(const TParameter& parameter);

  const std::vector<TypeParameter> parameters_;
  const std::string name_;
  const TReverseMap flippedMap_;
};

} // namespace facebook::velox
