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

#include "velox/type/TypeEncodingUtil.h"

// #include <boost/algorithm/string.hpp>
// #include <fmt/format.h>
// #include <folly/Demangle.h>
// #include <re2/re2.h>

// #include <sstream>
// #include <typeindex>

// #include "velox/type/TimestampConversion.h"

namespace facebook::velox {
size_t approximateTypeEncodingwidth(const TypePtr& type) {
  if (type->isPrimitiveType()) {
    return 1;
  }

  switch (type->kind()) {
    case TypeKind::ARRAY:
      return 1 + approximateTypeEncodingwidth(type->asArray().elementType());
    case TypeKind::MAP:
      return 1 + approximateTypeEncodingwidth(type->asMap().keyType()) +
          approximateTypeEncodingwidth(type->asMap().valueType());
    case TypeKind::ROW: {
      size_t fieldWidth = 0;
      for (const auto& child : type->asRow().children()) {
        fieldWidth += approximateTypeEncodingwidth(child);
      }
      return fieldWidth;
    }
    default:
      VELOX_UNREACHABLE("Unsupported type: {}", type->toString());
  }
}
} // namespace facebook::velox
