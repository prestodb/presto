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

#include "velox/core/FilterToExpression.h"
#include "velox/core/Expressions.h"

namespace facebook::velox::core {

core::TypedExprPtr getTypedExprFromSubfield(
    const common::Subfield& subfield,
    const RowTypePtr& rowType) {
  // Ensure the subfield is valid
  VELOX_CHECK(subfield.valid(), "Invalid subfield");

  // Get the path elements
  const auto& path = subfield.path();

  // Start with the first element, which should be a NestedField
  VELOX_CHECK_GT(path.size(), 0, "Empty subfield path");
  VELOX_CHECK_EQ(
      path[0]->kind(),
      common::SubfieldKind::kNestedField,
      "First element must be a field name");

  const auto* nestedField =
      static_cast<const common::Subfield::NestedField*>(path[0].get());
  const auto& fieldName = nestedField->name();

  // Find the field in the row type
  auto fieldType = rowType->findChild(fieldName);
  VELOX_CHECK_NOT_NULL(fieldType, "Field not found: {}", fieldName);

  // Create the initial field access expression
  core::TypedExprPtr expr =
      std::make_shared<core::FieldAccessTypedExpr>(fieldType, fieldName);

  // If there are more elements in the path, create nested field accesses
  for (size_t i = 1; i < path.size(); ++i) {
    const auto& element = path[i];

    switch (element->kind()) {
      case common::SubfieldKind::kNestedField: {
        // Handle nested field access
        const auto* nestedField =
            static_cast<const common::Subfield::NestedField*>(element.get());
        const auto& nestedName = nestedField->name();

        // Ensure current expression type is a ROW
        auto rowType = std::dynamic_pointer_cast<const RowType>(expr->type());
        VELOX_CHECK_NOT_NULL(
            rowType, "Expected ROW type for nested field access");

        // Find the nested field in the row type
        auto nestedType = rowType->findChild(nestedName);
        VELOX_CHECK_NOT_NULL(
            nestedType, "Nested field not found: {}", nestedName);

        // Create a field access expression for the nested field
        expr = std::make_shared<core::FieldAccessTypedExpr>(
            nestedType, expr, nestedName);
        break;
      }

      case common::SubfieldKind::kLongSubscript:
      case common::SubfieldKind::kStringSubscript:
      case common::SubfieldKind::kAllSubscripts:
        VELOX_NYI(
            "All other SubfieldKind except kNestedField are not implemented in getTypedExprFromSubfield");
        break;
    }
  }

  return expr;
}

core::TypedExprPtr filterToExpr(
    const common::Subfield& subfield,
    const common::Filter* filter,
    const RowTypePtr& rowType,
    memory::MemoryPool* pool) {
  auto subfieldExpr = getTypedExprFromSubfield(subfield, rowType);
  // TODO add logic to handle filter using subfieldExpr along with pool;
  return subfieldExpr;
}
} // namespace facebook::velox::core
