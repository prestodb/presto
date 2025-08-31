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

#include "velox/type/Type.h"

namespace facebook::velox {

class BigintEnumType;
using BigintEnumTypePtr = std::shared_ptr<const BigintEnumType>;

/// Represents an enumerated value where the physical type is a bigint. Each
/// enum type has a name and a set of string keys which map to bigint values,
/// passed in as a LongEnumParameter TypeParameterKind.
class BigintEnumType : public BigintType {
 public:
  static BigintEnumTypePtr get(const LongEnumParameter& parameter);

  bool equivalent(const Type& other) const override {
    return this == &other;
  }

  const char* name() const override {
    return "BIGINT_ENUM";
  }

  const std::vector<TypeParameter>& parameters() const override {
    return parameters_;
  }

  std::string toString() const override;

  folly::dynamic serialize() const override;

  bool containsValue(int64_t value) const {
    return flippedMap_.contains(value);
  }

  /// Returns the string key given an integer value. If the value does not exist
  /// in the flippedMap_, return std::nullopt.
  const std::optional<std::string> keyAt(int64_t value) const;

  const std::string& enumName() const {
    return name_;
  }

 private:
  explicit BigintEnumType(const LongEnumParameter& parameters);

  const std::vector<TypeParameter> parameters_;
  const std::string name_;
  const std::unordered_map<int64_t, std::string> flippedMap_;
};

inline BigintEnumTypePtr BIGINT_ENUM(const LongEnumParameter& parameter) {
  return BigintEnumType::get(parameter);
}

FOLLY_ALWAYS_INLINE bool isBigintEnumType(const Type& type) {
  return type.kind() == TypeKind::BIGINT &&
      dynamic_cast<const BigintEnumType*>(&type) != nullptr;
}

FOLLY_ALWAYS_INLINE BigintEnumTypePtr asBigintEnum(const TypePtr& type) {
  return std::dynamic_pointer_cast<const BigintEnumType>(type);
}
} // namespace facebook::velox
