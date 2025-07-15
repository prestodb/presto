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

// Helper function to create a boolean expression from a vector of conditions.
// If conditions is empty, returns a constant TRUE.
// If conditions has one element, returns that element.
// Otherwise, returns an AND expression combining all conditions.
core::TypedExprPtr createBooleanExpr(
    const std::vector<TypedExprPtr>& conditions) {
  if (conditions.empty()) {
    return std::make_shared<ConstantTypedExpr>(BOOLEAN(), variant(true));
  } else if (conditions.size() == 1) {
    return conditions[0];
  } else {
    return std::make_shared<CallTypedExpr>(BOOLEAN(), conditions, "and");
  }
}

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

  // Handle different filter types
  switch (filter->kind()) {
    case common::FilterKind::kAlwaysFalse:
      return std::make_shared<ConstantTypedExpr>(BOOLEAN(), variant(false));

    case common::FilterKind::kAlwaysTrue:
      return std::make_shared<ConstantTypedExpr>(BOOLEAN(), variant(true));

    case common::FilterKind::kIsNull:
      return std::make_shared<CallTypedExpr>(
          BOOLEAN(), std::vector<TypedExprPtr>{subfieldExpr}, "is_null");

    case common::FilterKind::kIsNotNull:
      return std::make_shared<CallTypedExpr>(
          BOOLEAN(), std::vector<TypedExprPtr>{subfieldExpr}, "is_not_null");

    case common::FilterKind::kBoolValue: {
      auto boolFilter = static_cast<const common::BoolValue*>(filter);
      auto boolValue = std::make_shared<ConstantTypedExpr>(
          BOOLEAN(), variant(boolFilter->testBool(true)));

      return std::make_shared<CallTypedExpr>(
          BOOLEAN(), std::vector<TypedExprPtr>{subfieldExpr, boolValue}, "eq");
    }

    case common::FilterKind::kBigintRange: {
      auto rangeFilter = static_cast<const common::BigintRange*>(filter);
      auto lower = std::make_shared<ConstantTypedExpr>(
          BIGINT(), variant(rangeFilter->lower()));
      auto upper = std::make_shared<ConstantTypedExpr>(
          BIGINT(), variant(rangeFilter->upper()));

      if (rangeFilter->lower() == rangeFilter->upper()) {
        // Single value comparison
        return std::make_shared<CallTypedExpr>(
            BOOLEAN(), std::vector<TypedExprPtr>{subfieldExpr, lower}, "eq");
      } else {
        // Range comparison
        auto greaterOrEqual = std::make_shared<CallTypedExpr>(
            BOOLEAN(), std::vector<TypedExprPtr>{subfieldExpr, lower}, "gte");

        auto lessOrEqual = std::make_shared<CallTypedExpr>(
            BOOLEAN(), std::vector<TypedExprPtr>{subfieldExpr, upper}, "lte");

        return std::make_shared<CallTypedExpr>(
            BOOLEAN(),
            std::vector<TypedExprPtr>{greaterOrEqual, lessOrEqual},
            "and");
      }
    }

    case common::FilterKind::kNegatedBigintRange: {
      // Create a BigintRange filter with the same bounds
      auto rangeFilter = static_cast<const common::NegatedBigintRange*>(filter);
      auto bigintRangeFilter = std::make_shared<common::BigintRange>(
          rangeFilter->lower(), rangeFilter->upper(), false);

      // Reuse the BigintRange logic and wrap it in a NOT operation
      auto rangeExpr =
          filterToExpr(subfield, bigintRangeFilter.get(), rowType, pool);

      return std::make_shared<CallTypedExpr>(
          BOOLEAN(), std::vector<TypedExprPtr>{rangeExpr}, "not");
    }

    case common::FilterKind::kDoubleRange: {
      auto doubleFilter = static_cast<const common::DoubleRange*>(filter);
      TypePtr numericType = DOUBLE();

      std::vector<TypedExprPtr> conditions;

      if (!doubleFilter->lowerUnbounded()) {
        auto lower = std::make_shared<ConstantTypedExpr>(
            numericType, variant(doubleFilter->lower()));

        if (doubleFilter->lowerExclusive()) {
          conditions.push_back(std::make_shared<CallTypedExpr>(
              BOOLEAN(), std::vector<TypedExprPtr>{subfieldExpr, lower}, "gt"));
        } else {
          conditions.push_back(std::make_shared<CallTypedExpr>(
              BOOLEAN(),
              std::vector<TypedExprPtr>{subfieldExpr, lower},
              "gte"));
        }
      }

      if (!doubleFilter->upperUnbounded()) {
        auto upper = std::make_shared<ConstantTypedExpr>(
            numericType, variant(doubleFilter->upper()));

        if (doubleFilter->upperExclusive()) {
          conditions.push_back(std::make_shared<CallTypedExpr>(
              BOOLEAN(), std::vector<TypedExprPtr>{subfieldExpr, upper}, "lt"));
        } else {
          conditions.push_back(std::make_shared<CallTypedExpr>(
              BOOLEAN(),
              std::vector<TypedExprPtr>{subfieldExpr, upper},
              "lte"));
        }
      }

      return createBooleanExpr(conditions);
    }

    case common::FilterKind::kFloatRange: {
      auto floatFilter = static_cast<const common::FloatRange*>(filter);
      TypePtr numericType = REAL();

      std::vector<TypedExprPtr> conditions;

      if (!floatFilter->lowerUnbounded()) {
        auto lower = std::make_shared<ConstantTypedExpr>(
            numericType, variant(floatFilter->lower()));

        if (floatFilter->lowerExclusive()) {
          conditions.push_back(std::make_shared<CallTypedExpr>(
              BOOLEAN(), std::vector<TypedExprPtr>{subfieldExpr, lower}, "gt"));
        } else {
          conditions.push_back(std::make_shared<CallTypedExpr>(
              BOOLEAN(),
              std::vector<TypedExprPtr>{subfieldExpr, lower},
              "gte"));
        }
      }

      if (!floatFilter->upperUnbounded()) {
        auto upper = std::make_shared<ConstantTypedExpr>(
            numericType, variant(floatFilter->upper()));

        if (floatFilter->upperExclusive()) {
          conditions.push_back(std::make_shared<CallTypedExpr>(
              BOOLEAN(), std::vector<TypedExprPtr>{subfieldExpr, upper}, "lt"));
        } else {
          conditions.push_back(std::make_shared<CallTypedExpr>(
              BOOLEAN(),
              std::vector<TypedExprPtr>{subfieldExpr, upper},
              "lte"));
        }
      }

      return createBooleanExpr(conditions);
    }

    case common::FilterKind::kBytesRange: {
      auto bytesFilter = static_cast<const common::BytesRange*>(filter);

      std::vector<TypedExprPtr> conditions;

      if (!bytesFilter->isLowerUnbounded()) {
        auto lower = std::make_shared<ConstantTypedExpr>(
            VARCHAR(), variant(bytesFilter->lower()));

        if (bytesFilter->isLowerExclusive()) {
          conditions.push_back(std::make_shared<CallTypedExpr>(
              BOOLEAN(), std::vector<TypedExprPtr>{subfieldExpr, lower}, "gt"));
        } else {
          conditions.push_back(std::make_shared<CallTypedExpr>(
              BOOLEAN(),
              std::vector<TypedExprPtr>{subfieldExpr, lower},
              "gte"));
        }
      }

      if (!bytesFilter->isUpperUnbounded()) {
        auto upper = std::make_shared<ConstantTypedExpr>(
            VARCHAR(), variant(bytesFilter->upper()));

        if (bytesFilter->isUpperExclusive()) {
          conditions.push_back(std::make_shared<CallTypedExpr>(
              BOOLEAN(), std::vector<TypedExprPtr>{subfieldExpr, upper}, "lt"));
        } else {
          conditions.push_back(std::make_shared<CallTypedExpr>(
              BOOLEAN(),
              std::vector<TypedExprPtr>{subfieldExpr, upper},
              "lte"));
        }
      }

      return createBooleanExpr(conditions);
    }

    case common::FilterKind::kBigintValuesUsingHashTable:
    case common::FilterKind::kBigintValuesUsingBitmask: {
      std::vector<int64_t> values;
      if (filter->kind() == common::FilterKind::kBigintValuesUsingHashTable) {
        auto hashFilter =
            static_cast<const common::BigintValuesUsingHashTable*>(filter);
        values = hashFilter->values();
      } else {
        auto bitmaskFilter =
            static_cast<const common::BigintValuesUsingBitmask*>(filter);
        values = bitmaskFilter->values();
      }

      std::vector<TypedExprPtr> valueExprs;
      for (const auto& value : values) {
        valueExprs.push_back(
            std::make_shared<ConstantTypedExpr>(BIGINT(), variant(value)));
      }

      auto arrayExpr = std::make_shared<CallTypedExpr>(
          ARRAY(BIGINT()), valueExprs, "array_constructor");

      return std::make_shared<CallTypedExpr>(
          BOOLEAN(), std::vector<TypedExprPtr>{arrayExpr, subfieldExpr}, "in");
    }

    case common::FilterKind::kNegatedBigintValuesUsingHashTable:
    case common::FilterKind::kNegatedBigintValuesUsingBitmask: {
      // Create the corresponding positive filter with the same values
      std::shared_ptr<common::Filter> positiveFilter;

      if (filter->kind() ==
          common::FilterKind::kNegatedBigintValuesUsingHashTable) {
        auto hashFilter =
            static_cast<const common::NegatedBigintValuesUsingHashTable*>(
                filter);
        const auto& values = hashFilter->values();
        positiveFilter = std::make_shared<common::BigintValuesUsingHashTable>(
            hashFilter->min(), hashFilter->max(), values, false);
      } else {
        auto bitmaskFilter =
            static_cast<const common::NegatedBigintValuesUsingBitmask*>(filter);
        const auto& values = bitmaskFilter->values();
        positiveFilter = std::make_shared<common::BigintValuesUsingBitmask>(
            bitmaskFilter->min(), bitmaskFilter->max(), values, false);
      }

      // Reuse the positive filter logic and wrap it in a NOT operation
      auto positiveExpr =
          filterToExpr(subfield, positiveFilter.get(), rowType, pool);

      return std::make_shared<CallTypedExpr>(
          BOOLEAN(), std::vector<TypedExprPtr>{positiveExpr}, "not");
    }

    case common::FilterKind::kBytesValues: {
      auto bytesFilter = static_cast<const common::BytesValues*>(filter);
      const auto& values = bytesFilter->values();

      std::vector<TypedExprPtr> valueExprs;
      for (const auto& value : values) {
        valueExprs.push_back(
            std::make_shared<ConstantTypedExpr>(VARCHAR(), variant(value)));
      }

      auto arrayExpr = std::make_shared<CallTypedExpr>(
          ARRAY(VARCHAR()), valueExprs, "array_constructor");

      return std::make_shared<CallTypedExpr>(
          BOOLEAN(), std::vector<TypedExprPtr>{arrayExpr, subfieldExpr}, "in");
    }

    case common::FilterKind::kNegatedBytesValues: {
      auto bytesFilter = static_cast<const common::NegatedBytesValues*>(filter);
      // Convert F14FastSet to vector for BytesValues constructor
      const auto& valuesSet = bytesFilter->values();
      std::vector<std::string> valuesVec(valuesSet.begin(), valuesSet.end());
      auto bytesValuesFilter =
          std::make_shared<common::BytesValues>(valuesVec, false);

      // Reuse the BytesValues logic and wrap it in a NOT operation
      auto bytesValuesExpr =
          filterToExpr(subfield, bytesValuesFilter.get(), rowType, pool);

      return std::make_shared<CallTypedExpr>(
          BOOLEAN(), std::vector<TypedExprPtr>{bytesValuesExpr}, "not");
    }

    case common::FilterKind::kTimestampRange: {
      auto timestampFilter = static_cast<const common::TimestampRange*>(filter);
      auto lower = std::make_shared<ConstantTypedExpr>(
          TIMESTAMP(), variant(timestampFilter->lower()));
      auto upper = std::make_shared<ConstantTypedExpr>(
          TIMESTAMP(), variant(timestampFilter->upper()));

      if (timestampFilter->isSingleValue()) {
        // Single value comparison
        return std::make_shared<CallTypedExpr>(
            BOOLEAN(), std::vector<TypedExprPtr>{subfieldExpr, lower}, "eq");
      } else {
        // Range comparison
        auto greaterOrEqual = std::make_shared<CallTypedExpr>(
            BOOLEAN(), std::vector<TypedExprPtr>{subfieldExpr, lower}, "gte");

        auto lessOrEqual = std::make_shared<CallTypedExpr>(
            BOOLEAN(), std::vector<TypedExprPtr>{subfieldExpr, upper}, "lte");

        return std::make_shared<CallTypedExpr>(
            BOOLEAN(),
            std::vector<TypedExprPtr>{greaterOrEqual, lessOrEqual},
            "and");
      }
    }

    case common::FilterKind::kBigintMultiRange: {
      auto multiRangeFilter =
          static_cast<const common::BigintMultiRange*>(filter);
      const auto& ranges = multiRangeFilter->ranges();

      std::vector<TypedExprPtr> rangeExprs;
      for (const auto& range : ranges) {
        auto rangeExpr = filterToExpr(subfield, range.get(), rowType, pool);
        rangeExprs.push_back(rangeExpr);
      }

      return std::make_shared<CallTypedExpr>(BOOLEAN(), rangeExprs, "or");
    }

    case common::FilterKind::kMultiRange: {
      auto multiRangeFilter = static_cast<const common::MultiRange*>(filter);
      const auto& filters = multiRangeFilter->filters();

      std::vector<TypedExprPtr> filterExprs;
      for (const auto& subFilter : filters) {
        auto filterExpr =
            filterToExpr(subfield, subFilter.get(), rowType, pool);
        filterExprs.push_back(filterExpr);
      }

      return std::make_shared<CallTypedExpr>(BOOLEAN(), filterExprs, "or");
    }

    // Handle other filter types as needed
    case common::FilterKind::kHugeintRange:
    case common::FilterKind::kHugeintValuesUsingHashTable:
    case common::FilterKind::kNegatedBytesRange:
    default:
      // For unhandled filter types, return the subfield expression as is
      VELOX_NYI(
          "Filter type not yet implemented in filterToExpr: {}",
          filter->toString());
      return subfieldExpr;
  }
}
} // namespace facebook::velox::core
