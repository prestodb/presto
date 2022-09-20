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

#include "velox/substrait/VeloxSubstraitSignature.h"
#include "velox/functions/FunctionRegistry.h"

namespace facebook::velox::substrait {

std::string VeloxSubstraitSignature::toSubstraitSignature(
    const TypeKind typeKind) {
  switch (typeKind) {
    case TypeKind::BOOLEAN:
      return "bool";
    case TypeKind::TINYINT:
      return "i8";
    case TypeKind::SMALLINT:
      return "i16";
    case TypeKind::INTEGER:
      return "i32";
    case TypeKind::BIGINT:
      return "i64";
    case TypeKind::REAL:
      return "fp32";
    case TypeKind::DOUBLE:
      return "fp64";
    case TypeKind::VARCHAR:
      return "str";
    case TypeKind::VARBINARY:
      return "vbin";
    case TypeKind::TIMESTAMP:
      return "ts";
    case TypeKind::DATE:
      return "date";
    case TypeKind::SHORT_DECIMAL:
      return "dec";
    case TypeKind::LONG_DECIMAL:
      return "dec";
    case TypeKind::ARRAY:
      return "list";
    case TypeKind::MAP:
      return "map";
    case TypeKind::ROW:
      return "struct";
    case TypeKind::UNKNOWN:
      return "u!name";
    default:
      VELOX_UNSUPPORTED(
          "Substrait type signature conversion not supported for type {}.",
          mapTypeKindToName(typeKind));
  }
}

std::string VeloxSubstraitSignature::toSubstraitSignature(
    const std::string& functionName,
    const std::vector<TypePtr>& arguments) {
  if (arguments.empty()) {
    return functionName;
  }
  std::vector<std::string> substraitTypeSignatures;
  substraitTypeSignatures.reserve(arguments.size());
  for (const auto& type : arguments) {
    substraitTypeSignatures.emplace_back(toSubstraitSignature(type->kind()));
  }
  return functionName + ":" + folly::join("_", substraitTypeSignatures);
}

} // namespace facebook::velox::substrait
