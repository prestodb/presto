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

#include "velox/functions/prestosql/types/EnumTypeBase.h"
#include "velox/type/Type.h"

namespace facebook::velox {

class VarcharEnumType;
using VarcharEnumTypePtr = std::shared_ptr<const VarcharEnumType>;

/// Represents an enumerated value where the physical type is a varchar. Each
/// enum type has a name and a set of string keys which map to string values,
/// passed in as a VarcharEnumParameter TypeParameterKind.
class VarcharEnumType
    : public EnumTypeBase<std::string, VarcharEnumParameter, VarcharType> {
 public:
  static VarcharEnumTypePtr get(const VarcharEnumParameter& parameter);

  const char* name() const override {
    return "VARCHAR_ENUM";
  }

  std::string toString() const override;

  folly::dynamic serialize() const override;

 private:
  explicit VarcharEnumType(const VarcharEnumParameter& parameters);
  friend class EnumTypeBase<std::string, VarcharEnumParameter, VarcharType>;
};

inline VarcharEnumTypePtr VARCHAR_ENUM(const VarcharEnumParameter& parameter) {
  return VarcharEnumType::get(parameter);
}

FOLLY_ALWAYS_INLINE bool isVarcharEnumType(const Type& type) {
  return type.kind() == TypeKind::VARCHAR &&
      dynamic_cast<const VarcharEnumType*>(&type) != nullptr;
}

FOLLY_ALWAYS_INLINE VarcharEnumTypePtr asVarcharEnum(const TypePtr& type) {
  return std::dynamic_pointer_cast<const VarcharEnumType>(type);
}
} // namespace facebook::velox
