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

#include "velox/type/Type.h"

namespace facebook::velox::substrait {

bool isString(const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return true;
    default:
      break;
  }
  return false;
}

int64_t bytesOfType(const TypePtr& type) {
  auto typeKind = type->kind();
  switch (typeKind) {
    case TypeKind::INTEGER:
      return 4;
    case TypeKind::BIGINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
      return 8;
    default:
      VELOX_NYI("Returning bytes of Type not supported for type {}.", typeKind);
  }
}

TypePtr toVeloxType(const std::string& typeName) {
  auto typeKind = mapNameToTypeKind(typeName);
  switch (typeKind) {
    case TypeKind::BOOLEAN:
      return BOOLEAN();
    case TypeKind::DOUBLE:
      return DOUBLE();
    case TypeKind::VARCHAR:
      return VARCHAR();
    case TypeKind::INTEGER:
      return INTEGER();
    case TypeKind::BIGINT:
      return BIGINT();
    default:
      VELOX_NYI("Velox type conversion not supported for type {}.", typeName);
  }
}

} // namespace facebook::velox::substrait
