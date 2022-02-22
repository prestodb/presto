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

#include "velox/functions/prestosql/types/JsonType.h"

#include <string>

#include "folly/Conv.h"
#include "folly/json.h"

#include "velox/common/base/Exceptions.h"

namespace facebook::velox {

bool JsonCastOperator::isSupportedType(const TypePtr& other) const {
  switch (other->kind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::BIGINT:
    case TypeKind::INTEGER:
    case TypeKind::SMALLINT:
    case TypeKind::TINYINT:
    case TypeKind::DOUBLE:
    case TypeKind::REAL:
    case TypeKind::VARCHAR:
    case TypeKind::DATE:
    case TypeKind::TIMESTAMP:
      return true;
    case TypeKind::ARRAY:
      return isSupportedType(other->childAt(0));
    case TypeKind::ROW:
      for (auto& child : other->as<TypeKind::ROW>().children()) {
        if (!isSupportedType(child)) {
          return false;
        }
      }
      return true;
    case TypeKind::MAP:
      return (
          other->childAt(0)->kind() == TypeKind::VARCHAR &&
          isSupportedType(other->childAt(1)));
    default:
      return false;
  }
}

template <typename T>
void generateJsonTyped(
    const SimpleVector<T>& input,
    int row,
    std::string& result) {
  auto value = input.valueAt(row);
  folly::toAppend<std::string, T>(value, &result);
}

template <>
void generateJsonTyped<StringView>(
    const SimpleVector<StringView>& input,
    int row,
    std::string& result) {
  auto value = input.valueAt(row);
  folly::json::escapeString(value, result, folly::json::serialization_opts{});
}

template <TypeKind kind>
void castPrimitiveToJson(
    const BaseVector& input,
    const SelectivityVector& rows,
    FlatVector<StringView>& flatResult) {
  using T = typename TypeTraits<kind>::NativeType;

  auto inputVector = input.as<SimpleVector<T>>();

  std::string result;
  rows.applyToSelected([&](auto row) {
    result.clear();
    generateJsonTyped(*inputVector, row, result);

    flatResult.set(row, StringView{result});
  });
}

void JsonCastOperator::castTo(
    const BaseVector& input,
    const SelectivityVector& rows,
    BaseVector& result) const {
  // result is guaranteed to be a flat writable vector.
  auto* flatResult = result.as<FlatVector<StringView>>();

  switch (input.typeKind()) {
    case TypeKind::BIGINT:
      castPrimitiveToJson<TypeKind::BIGINT>(input, rows, *flatResult);
      return;
    case TypeKind::VARCHAR:
      castPrimitiveToJson<TypeKind::VARCHAR>(input, rows, *flatResult);
      return;
    // TODO: Add support for other from types.
    default:
      break;
  }

  VELOX_NYI(
      "Cast from {} to JSON is not supported yet", input.type()->toString());
}

} // namespace facebook::velox
