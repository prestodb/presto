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

std::string VeloxSubstraitSignature::toSubstraitSignature(const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
      return "bool";
    case TypeKind::TINYINT:
      return "i8";
    case TypeKind::SMALLINT:
      return "i16";
    case TypeKind::INTEGER:
      if (type->isDate()) {
        return "date";
      }
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
          mapTypeKindToName(type->kind()));
  }
}

// static
TypePtr VeloxSubstraitSignature::fromSubstraitSignature(
    const std::string& signature) {
  if (signature == "bool") {
    return BOOLEAN();
  }

  if (signature == "i8") {
    return TINYINT();
  }

  if (signature == "i16") {
    return SMALLINT();
  }

  if (signature == "i32") {
    return INTEGER();
  }

  if (signature == "i64") {
    return BIGINT();
  }

  if (signature == "fp32") {
    return REAL();
  }

  if (signature == "fp64") {
    return DOUBLE();
  }

  if (signature == "str") {
    return VARCHAR();
  }

  if (signature == "vbin") {
    return VARBINARY();
  }

  if (signature == "ts") {
    return TIMESTAMP();
  }

  if (signature == "date") {
    return DATE();
  }

  VELOX_UNSUPPORTED(
      "Substrait type signature conversion to Velox type not supported for {}.",
      signature);
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
    substraitTypeSignatures.emplace_back(toSubstraitSignature(type));
  }
  return functionName + ":" + folly::join("_", substraitTypeSignatures);
}

} // namespace facebook::velox::substrait
