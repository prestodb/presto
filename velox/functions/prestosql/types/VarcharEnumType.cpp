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

#include "velox/functions/prestosql/types/VarcharEnumType.h"

namespace facebook::velox {

// Should only be called from get() to create a new instance.
VarcharEnumType::VarcharEnumType(const VarcharEnumParameter& parameters)
    : EnumTypeBase<std::string, VarcharEnumParameter, VarcharType>(parameters) {
}

std::string VarcharEnumType::toString() const {
  return fmt::format("{}:VarcharEnum({})", name_, flippedMapToString());
}

VarcharEnumTypePtr VarcharEnumType::get(const VarcharEnumParameter& parameter) {
  return getCached<VarcharEnumType>(parameter);
}

folly::dynamic VarcharEnumType::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "Type";
  obj["type"] = name();
  // parameters_[0].varcharEnumLiteral is assumed to have a value since it is
  // constructed from a VarcharEnumParameter.
  obj["kVarcharEnumParam"] =
      parameters_[0].varcharEnumLiteral.value().serialize();
  return obj;
}

} // namespace facebook::velox
