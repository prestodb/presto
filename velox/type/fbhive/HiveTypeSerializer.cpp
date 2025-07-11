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

#include "velox/type/fbhive/HiveTypeSerializer.h"

#include <cstddef>

namespace facebook::velox::type::fbhive {

std::string HiveTypeSerializer::serialize(const TypePtr& type) {
  HiveTypeSerializer serializer;
  return serializer.visit(*type);
}

std::string HiveTypeSerializer::visit(const Type& type) const {
  switch (type.kind()) {
    case TypeKind::BOOLEAN:
      return "boolean";
    case TypeKind::TINYINT:
      return "tinyint";
    case TypeKind::SMALLINT:
      return "smallint";
    case TypeKind::INTEGER:
      return "int";
    case TypeKind::BIGINT:
      return "bigint";
    case TypeKind::REAL:
      return "float";
    case TypeKind::DOUBLE:
      return "double";
    case TypeKind::VARCHAR:
      return "string";
    case TypeKind::VARBINARY:
      return "binary";
    case TypeKind::TIMESTAMP:
      return "timestamp";
    case TypeKind::ARRAY:
      return "array<" + visitChildren(type.asArray()) + ">";
    case TypeKind::MAP:
      return "map<" + visitChildren(type.asMap()) + ">";
    case TypeKind::ROW:
      return "struct<" + visitChildren(type.asRow()) + ">";
    case TypeKind::OPAQUE: {
      auto typeAlias = getOpaqueAliasForTypeId(type.asOpaque().typeIndex());
      return "opaque<" + typeAlias + ">";
    }
    default:
      VELOX_UNSUPPORTED("unsupported type: " + type.toString());
  }
}

std::string HiveTypeSerializer::visitChildren(const RowType& t) const {
  std::string result;
  for (size_t i = 0; i < t.size(); ++i) {
    if (i != 0) {
      result += ",";
    }
    result += t.nameOf(i);
    result += ":";
    result += visit(*t.childAt(i));
  }

  return result;
}

std::string HiveTypeSerializer::visitChildren(const Type& t) const {
  std::string result;
  for (size_t i = 0; i < t.size(); ++i) {
    if (i != 0) {
      result += ",";
    }
    result += visit(*t.childAt(i));
  }

  return result;
}

} // namespace facebook::velox::type::fbhive
