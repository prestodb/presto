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

// Helper function to handle nullAllowed logic for filters.
// If nullAllowed is true, returns (expression) OR (field IS NULL).
// If nullAllowed is false, returns the expression as-is.
core::TypedExprPtr handleNullAllowed(
    const core::TypedExprPtr& expression,
    const core::TypedExprPtr& subfieldExpr,
    bool nullAllowed) {
  if (nullAllowed) {
    // If nullAllowed=true: (expression) OR (field IS NULL)
    auto isNullExpr =
        std::make_shared<CallTypedExpr>(BOOLEAN(), "is_null", subfieldExpr);

    return std::make_shared<CallTypedExpr>(
        BOOLEAN(), "or", expression, isNullExpr);
  } else {
    // If nullAllowed=false: just return the expression
    return expression;
  }
}

core::TypedExprPtr getTypedExprFromSubfield(
    const common::Subfield& subfield,
    const RowTypePtr& rowType) {
  VELOX_CHECK(subfield.valid(), "Invalid subfield");

  // Get the path elements
  const auto& path = subfield.path();
  const auto& fieldName = subfield.baseName();

  core::TypedExprPtr expr = std::make_shared<core::FieldAccessTypedExpr>(
      rowType->findChild(fieldName), fieldName);

  // If there are more elements in the path, create nested field accesses.
  for (size_t i = 1; i < path.size(); ++i) {
    const auto& element = path[i];

    switch (element->kind()) {
      case common::SubfieldKind::kNestedField: {
        const auto* nestedField = element->as<common::Subfield::NestedField>();
        const auto& nestedName = nestedField->name();

        VELOX_CHECK(
            expr->type()->isRow(), "Expected ROW type for nested field access");
        const auto& rowType = expr->type()->asRow();

        expr = std::make_shared<core::FieldAccessTypedExpr>(
            rowType.findChild(nestedName), expr, nestedName);
        break;
      }

      default:
        VELOX_NYI(
            "SubfieldKind other than kNestedField are not supported: {}",
            common::SubfieldKindName::toName(element->kind()));
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

  auto subfieldType = subfieldExpr->type();
  auto isComplexType =
      (subfieldType->kind() == TypeKind::ROW ||
       subfieldType->kind() == TypeKind::MAP ||
       subfieldType->kind() == TypeKind::ARRAY);

  // For complex types (ROW, MAP, ARRAY), only IsNull and IsNotNull filters
  // are supported.
  if (isComplexType && filter->kind() != common::FilterKind::kIsNull &&
      filter->kind() != common::FilterKind::kIsNotNull) {
    VELOX_FAIL(
        "Should not get any filter on complex type other than IsNull or IsNotNUll");
  }

  // Handle different filter types
  switch (filter->kind()) {
    case common::FilterKind::kAlwaysFalse:
      return std::make_shared<ConstantTypedExpr>(BOOLEAN(), variant(false));

    case common::FilterKind::kAlwaysTrue:
      return std::make_shared<ConstantTypedExpr>(BOOLEAN(), variant(true));

    case common::FilterKind::kIsNull:
      return std::make_shared<CallTypedExpr>(
          BOOLEAN(), "is_null", subfieldExpr);

    case common::FilterKind::kIsNotNull: {
      auto isNullExpr =
          std::make_shared<CallTypedExpr>(BOOLEAN(), "is_null", subfieldExpr);
      return std::make_shared<CallTypedExpr>(BOOLEAN(), "not", isNullExpr);
    }

    case common::FilterKind::kBoolValue: {
      auto boolFilter = static_cast<const common::BoolValue*>(filter);
      auto boolValue = std::make_shared<ConstantTypedExpr>(
          BOOLEAN(), variant(boolFilter->testBool(true)));

      auto eqExpr = std::make_shared<CallTypedExpr>(
          BOOLEAN(), "eq", subfieldExpr, boolValue);

      return handleNullAllowed(eqExpr, subfieldExpr, boolFilter->nullAllowed());
    }

    case common::FilterKind::kBigintRange: {
      auto rangeFilter = static_cast<const common::BigintRange*>(filter);
      auto subfieldType = subfieldExpr->type();

      // Special handling for TPCH DATE
      if (subfieldType->isDate()) {
        // For DATE type, we need to handle max/min int64 values specially
        // as they can cause overflow when converted to a date
        const int64_t kMaxInt64 = std::numeric_limits<int64_t>::max();
        const int64_t kMinInt64 = std::numeric_limits<int64_t>::min();

        std::vector<TypedExprPtr> conditions;

        // Handle lower bound
        if (rangeFilter->lower() > kMinInt64) {
          auto lower = std::make_shared<ConstantTypedExpr>(
              subfieldType,
              variant(static_cast<int32_t>(rangeFilter->lower())));

          conditions.push_back(std::make_shared<CallTypedExpr>(
              BOOLEAN(), "gte", subfieldExpr, lower));
        }

        // Handle upper bound
        if (rangeFilter->upper() < kMaxInt64) {
          auto upper = std::make_shared<ConstantTypedExpr>(
              subfieldType,
              variant(static_cast<int32_t>(rangeFilter->upper())));

          conditions.push_back(std::make_shared<CallTypedExpr>(
              BOOLEAN(), "lte", subfieldExpr, upper));
        }

        auto rangeExpr = createBooleanExpr(conditions);
        return handleNullAllowed(
            rangeExpr, subfieldExpr, rangeFilter->nullAllowed());
      } else {
        // Regular handling for non-DATE types
        // Create constants with the correct type based on the subfield type
        std::shared_ptr<ConstantTypedExpr> lower, upper;

        switch (subfieldType->kind()) {
          case TypeKind::TINYINT:
            lower = std::make_shared<ConstantTypedExpr>(
                subfieldType,
                variant(static_cast<int8_t>(rangeFilter->lower())));
            upper = std::make_shared<ConstantTypedExpr>(
                subfieldType,
                variant(static_cast<int8_t>(rangeFilter->upper())));
            break;
          case TypeKind::SMALLINT:
            lower = std::make_shared<ConstantTypedExpr>(
                subfieldType,
                variant(static_cast<int16_t>(rangeFilter->lower())));
            upper = std::make_shared<ConstantTypedExpr>(
                subfieldType,
                variant(static_cast<int16_t>(rangeFilter->upper())));
            break;
          case TypeKind::INTEGER:
            lower = std::make_shared<ConstantTypedExpr>(
                subfieldType,
                variant(static_cast<int32_t>(rangeFilter->lower())));
            upper = std::make_shared<ConstantTypedExpr>(
                subfieldType,
                variant(static_cast<int32_t>(rangeFilter->upper())));
            break;
          case TypeKind::BIGINT:
          default:
            lower = std::make_shared<ConstantTypedExpr>(
                subfieldType, variant(rangeFilter->lower()));
            upper = std::make_shared<ConstantTypedExpr>(
                subfieldType, variant(rangeFilter->upper()));
            break;
        }

        std::shared_ptr<CallTypedExpr> rangeExpr;
        if (rangeFilter->lower() == rangeFilter->upper()) {
          // Single value comparison
          rangeExpr = std::make_shared<CallTypedExpr>(
              BOOLEAN(), "eq", subfieldExpr, lower);
        } else {
          // Range comparison
          auto greaterOrEqual = std::make_shared<CallTypedExpr>(
              BOOLEAN(), "gte", subfieldExpr, lower);

          auto lessOrEqual = std::make_shared<CallTypedExpr>(
              BOOLEAN(), "lte", subfieldExpr, upper);

          rangeExpr = std::make_shared<CallTypedExpr>(
              BOOLEAN(), "and", greaterOrEqual, lessOrEqual);
        }

        return handleNullAllowed(
            rangeExpr, subfieldExpr, rangeFilter->nullAllowed());
      }
    }

    case common::FilterKind::kNegatedBigintRange: {
      auto negatedRangeFilter =
          static_cast<const common::NegatedBigintRange*>(filter);
      bool nullAllowed = negatedRangeFilter->nullAllowed();
      const common::Filter* nonNegatedFilter =
          negatedRangeFilter->getNonNegated();

      // Reuse positive logic by calling filterToExpr recursively with the
      // non-negated filter
      auto rangeExpr = filterToExpr(subfield, nonNegatedFilter, rowType, pool);

      // Create NOT range expression
      auto notRangeExpr =
          std::make_shared<CallTypedExpr>(BOOLEAN(), "not", rangeExpr);

      return handleNullAllowed(notRangeExpr, subfieldExpr, nullAllowed);
    }

    case common::FilterKind::kDoubleRange: {
      auto doubleFilter = static_cast<const common::DoubleRange*>(filter);
      auto subfieldType = subfieldExpr->type();

      std::vector<TypedExprPtr> conditions;

      if (!doubleFilter->lowerUnbounded()) {
        std::shared_ptr<ConstantTypedExpr> lower;

        // Handle type casting based on subfield type
        switch (subfieldType->kind()) {
          case TypeKind::REAL:
            lower = std::make_shared<ConstantTypedExpr>(
                subfieldType,
                variant(static_cast<float>(doubleFilter->lower())));
            break;
          case TypeKind::DOUBLE:
          default:
            lower = std::make_shared<ConstantTypedExpr>(
                subfieldType, variant(doubleFilter->lower()));
            break;
        }

        if (doubleFilter->lowerExclusive()) {
          conditions.push_back(std::make_shared<CallTypedExpr>(
              BOOLEAN(), "gt", subfieldExpr, lower));
        } else {
          conditions.push_back(std::make_shared<CallTypedExpr>(
              BOOLEAN(), "gte", subfieldExpr, lower));
        }
      }

      if (!doubleFilter->upperUnbounded()) {
        std::shared_ptr<ConstantTypedExpr> upper;

        // Handle type casting based on subfield type
        switch (subfieldType->kind()) {
          case TypeKind::REAL:
            upper = std::make_shared<ConstantTypedExpr>(
                subfieldType,
                variant(static_cast<float>(doubleFilter->upper())));
            break;
          case TypeKind::DOUBLE:
          default:
            upper = std::make_shared<ConstantTypedExpr>(
                subfieldType, variant(doubleFilter->upper()));
            break;
        }

        if (doubleFilter->upperExclusive()) {
          conditions.push_back(std::make_shared<CallTypedExpr>(
              BOOLEAN(), "lt", subfieldExpr, upper));
        } else {
          conditions.push_back(std::make_shared<CallTypedExpr>(
              BOOLEAN(), "lte", subfieldExpr, upper));
        }
      }

      auto rangeExpr = createBooleanExpr(conditions);
      return handleNullAllowed(
          rangeExpr, subfieldExpr, doubleFilter->nullAllowed());
    }

    case common::FilterKind::kFloatRange: {
      auto floatFilter = static_cast<const common::FloatRange*>(filter);
      auto subfieldType = subfieldExpr->type();

      std::vector<TypedExprPtr> conditions;

      if (!floatFilter->lowerUnbounded()) {
        // Explicitly create variant with float type to avoid promotion to
        // double
        auto lowerValue = floatFilter->lower();
        auto lower = std::make_shared<ConstantTypedExpr>(
            subfieldType, variant(static_cast<float>(lowerValue)));

        if (floatFilter->lowerExclusive()) {
          conditions.push_back(std::make_shared<CallTypedExpr>(
              BOOLEAN(), "gt", subfieldExpr, lower));
        } else {
          conditions.push_back(std::make_shared<CallTypedExpr>(
              BOOLEAN(), "gte", subfieldExpr, lower));
        }
      }

      if (!floatFilter->upperUnbounded()) {
        // Explicitly create variant with float type to avoid promotion to
        // double
        auto upperValue = floatFilter->upper();
        auto upper = std::make_shared<ConstantTypedExpr>(
            subfieldType, variant(static_cast<float>(upperValue)));

        if (floatFilter->upperExclusive()) {
          conditions.push_back(std::make_shared<CallTypedExpr>(
              BOOLEAN(), "lt", subfieldExpr, upper));
        } else {
          conditions.push_back(std::make_shared<CallTypedExpr>(
              BOOLEAN(), "lte", subfieldExpr, upper));
        }
      }

      auto rangeExpr = createBooleanExpr(conditions);
      return handleNullAllowed(
          rangeExpr, subfieldExpr, floatFilter->nullAllowed());
    }

    case common::FilterKind::kBytesRange: {
      auto bytesFilter = static_cast<const common::BytesRange*>(filter);
      auto subfieldType = subfieldExpr->type();

      std::vector<TypedExprPtr> conditions;

      if (!bytesFilter->isLowerUnbounded()) {
        auto lower = std::make_shared<ConstantTypedExpr>(
            subfieldType, variant(bytesFilter->lower()));

        if (bytesFilter->isLowerExclusive()) {
          conditions.push_back(std::make_shared<CallTypedExpr>(
              BOOLEAN(), "gt", subfieldExpr, lower));
        } else {
          conditions.push_back(std::make_shared<CallTypedExpr>(
              BOOLEAN(), "gte", subfieldExpr, lower));
        }
      }

      if (!bytesFilter->isUpperUnbounded()) {
        auto upper = std::make_shared<ConstantTypedExpr>(
            subfieldType, variant(bytesFilter->upper()));

        if (bytesFilter->isUpperExclusive()) {
          conditions.push_back(std::make_shared<CallTypedExpr>(
              BOOLEAN(), "lt", subfieldExpr, upper));
        } else {
          conditions.push_back(std::make_shared<CallTypedExpr>(
              BOOLEAN(), "lte", subfieldExpr, upper));
        }
      }

      auto rangeExpr = createBooleanExpr(conditions);
      return handleNullAllowed(
          rangeExpr, subfieldExpr, bytesFilter->nullAllowed());
    }

    case common::FilterKind::kBigintValuesUsingHashTable:
    case common::FilterKind::kBigintValuesUsingBitmask: {
      std::vector<int64_t> values;
      int64_t min, max;
      bool nullAllowed;

      if (filter->kind() == common::FilterKind::kBigintValuesUsingHashTable) {
        auto hashFilter =
            static_cast<const common::BigintValuesUsingHashTable*>(filter);
        values = hashFilter->values();
        min = hashFilter->min();
        max = hashFilter->max();
        nullAllowed = hashFilter->nullAllowed();
      } else {
        auto bitmaskFilter =
            static_cast<const common::BigintValuesUsingBitmask*>(filter);
        values = bitmaskFilter->values();
        min = bitmaskFilter->min();
        max = bitmaskFilter->max();
        nullAllowed = bitmaskFilter->nullAllowed();
      }

      auto subfieldType = subfieldExpr->type();
      std::vector<TypedExprPtr> valueExprs;
      valueExprs.reserve(values.size());

      // Create constants with the correct type based on the subfield type
      for (const auto& value : values) {
        switch (subfieldType->kind()) {
          case TypeKind::TINYINT:
            valueExprs.push_back(std::make_shared<ConstantTypedExpr>(
                subfieldType, variant(static_cast<int8_t>(value))));
            break;
          case TypeKind::SMALLINT:
            valueExprs.push_back(std::make_shared<ConstantTypedExpr>(
                subfieldType, variant(static_cast<int16_t>(value))));
            break;
          case TypeKind::INTEGER:
            valueExprs.push_back(std::make_shared<ConstantTypedExpr>(
                subfieldType, variant(static_cast<int32_t>(value))));
            break;
          case TypeKind::BIGINT:
          default:
            valueExprs.push_back(std::make_shared<ConstantTypedExpr>(
                subfieldType, variant(value)));
            break;
        }
      }

      auto arrayExpr = std::make_shared<CallTypedExpr>(
          ARRAY(subfieldType), valueExprs, "array_constructor");

      auto inExpr = std::make_shared<CallTypedExpr>(
          BOOLEAN(), "in", subfieldExpr, arrayExpr);

      // Optimization: Add range check (field >= min AND field <= max) before IN
      // check
      std::shared_ptr<ConstantTypedExpr> minConstant, maxConstant;
      switch (subfieldType->kind()) {
        case TypeKind::TINYINT:
          minConstant = std::make_shared<ConstantTypedExpr>(
              subfieldType, variant(static_cast<int8_t>(min)));
          maxConstant = std::make_shared<ConstantTypedExpr>(
              subfieldType, variant(static_cast<int8_t>(max)));
          break;
        case TypeKind::SMALLINT:
          minConstant = std::make_shared<ConstantTypedExpr>(
              subfieldType, variant(static_cast<int16_t>(min)));
          maxConstant = std::make_shared<ConstantTypedExpr>(
              subfieldType, variant(static_cast<int16_t>(max)));
          break;
        case TypeKind::INTEGER:
          minConstant = std::make_shared<ConstantTypedExpr>(
              subfieldType, variant(static_cast<int32_t>(min)));
          maxConstant = std::make_shared<ConstantTypedExpr>(
              subfieldType, variant(static_cast<int32_t>(max)));
          break;
        case TypeKind::BIGINT:
        default:
          minConstant =
              std::make_shared<ConstantTypedExpr>(subfieldType, variant(min));
          maxConstant =
              std::make_shared<ConstantTypedExpr>(subfieldType, variant(max));
          break;
      }

      // Create range check: field >= min AND field <= max
      auto gteMinExpr = std::make_shared<CallTypedExpr>(
          BOOLEAN(), "gte", subfieldExpr, minConstant);
      auto lteMaxExpr = std::make_shared<CallTypedExpr>(
          BOOLEAN(), "lte", subfieldExpr, maxConstant);
      auto rangeCheckExpr = std::make_shared<CallTypedExpr>(
          BOOLEAN(), "and", gteMinExpr, lteMaxExpr);

      // Combine range check with IN check: (field >= min AND field <= max) AND
      // (field IN values)
      auto optimizedInExpr = std::make_shared<CallTypedExpr>(
          BOOLEAN(), "and", rangeCheckExpr, inExpr);

      return handleNullAllowed(optimizedInExpr, subfieldExpr, nullAllowed);
    }

    case common::FilterKind::kNegatedBigintValuesUsingHashTable: {
      auto hashFilter =
          static_cast<const common::NegatedBigintValuesUsingHashTable*>(filter);
      bool nullAllowed = hashFilter->nullAllowed();
      const common::Filter* nonNegatedFilter = hashFilter->getNonNegated();

      // Reuse positive logic by calling filterToExpr recursively with the
      // non-negated filter
      auto inExpr = filterToExpr(subfield, nonNegatedFilter, rowType, pool);

      // Create NOT IN expression
      auto notInExpr =
          std::make_shared<CallTypedExpr>(BOOLEAN(), "not", inExpr);

      return handleNullAllowed(notInExpr, subfieldExpr, nullAllowed);
    }
    case common::FilterKind::kNegatedBigintValuesUsingBitmask: {
      auto bitmaskFilter =
          static_cast<const common::NegatedBigintValuesUsingBitmask*>(filter);
      bool nullAllowed = bitmaskFilter->nullAllowed();
      const common::Filter* nonNegatedFilter = bitmaskFilter->getNonNegated();

      // Reuse positive logic by calling filterToExpr recursively with the
      // non-negated filter
      auto inExpr = filterToExpr(subfield, nonNegatedFilter, rowType, pool);

      // Create NOT IN expression
      auto notInExpr =
          std::make_shared<CallTypedExpr>(BOOLEAN(), "not", inExpr);

      return handleNullAllowed(notInExpr, subfieldExpr, nullAllowed);
    }

    case common::FilterKind::kBytesValues: {
      auto bytesFilter = static_cast<const common::BytesValues*>(filter);
      const auto& values = bytesFilter->values();
      auto subfieldType = subfieldExpr->type();

      std::vector<TypedExprPtr> valueExprs;
      for (const auto& value : values) {
        valueExprs.push_back(
            std::make_shared<ConstantTypedExpr>(subfieldType, variant(value)));
      }

      auto arrayExpr = std::make_shared<CallTypedExpr>(
          ARRAY(subfieldType), valueExprs, "array_constructor");

      auto inExpr = std::make_shared<CallTypedExpr>(
          BOOLEAN(), "in", subfieldExpr, arrayExpr);

      return handleNullAllowed(
          inExpr, subfieldExpr, bytesFilter->nullAllowed());
    }

    case common::FilterKind::kNegatedBytesValues: {
      auto negatedBytesFilter =
          static_cast<const common::NegatedBytesValues*>(filter);
      bool nullAllowed = negatedBytesFilter->nullAllowed();
      const common::Filter* nonNegatedFilter =
          negatedBytesFilter->getNonNegated();

      // Reuse positive logic by calling filterToExpr recursively with the
      // non-negated filter
      auto inExpr = filterToExpr(subfield, nonNegatedFilter, rowType, pool);

      // Create NOT IN expression
      auto notInExpr =
          std::make_shared<CallTypedExpr>(BOOLEAN(), "not", inExpr);

      return handleNullAllowed(notInExpr, subfieldExpr, nullAllowed);
    }

    case common::FilterKind::kTimestampRange: {
      auto timestampFilter = static_cast<const common::TimestampRange*>(filter);
      auto lower = std::make_shared<ConstantTypedExpr>(
          TIMESTAMP(), variant(timestampFilter->lower()));
      auto upper = std::make_shared<ConstantTypedExpr>(
          TIMESTAMP(), variant(timestampFilter->upper()));

      std::shared_ptr<CallTypedExpr> rangeExpr;
      if (timestampFilter->isSingleValue()) {
        // Single value comparison
        rangeExpr = std::make_shared<CallTypedExpr>(
            BOOLEAN(), "eq", subfieldExpr, lower);
      } else {
        // Range comparison
        auto greaterOrEqual = std::make_shared<CallTypedExpr>(
            BOOLEAN(), "gte", subfieldExpr, lower);

        auto lessOrEqual = std::make_shared<CallTypedExpr>(
            BOOLEAN(), "lte", subfieldExpr, upper);

        rangeExpr = std::make_shared<CallTypedExpr>(
            BOOLEAN(), "and", greaterOrEqual, lessOrEqual);
      }

      return handleNullAllowed(
          rangeExpr, subfieldExpr, timestampFilter->nullAllowed());
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

    case common::FilterKind::kNegatedBytesRange: {
      auto negatedBytesFilter =
          static_cast<const common::NegatedBytesRange*>(filter);
      bool nullAllowed = negatedBytesFilter->nullAllowed();
      const common::Filter* nonNegatedFilter =
          negatedBytesFilter->getNonNegated();

      // Reuse positive logic by calling filterToExpr recursively with the
      // non-negated filter
      auto rangeExpr = filterToExpr(subfield, nonNegatedFilter, rowType, pool);

      // Create NOT range expression
      auto notRangeExpr =
          std::make_shared<CallTypedExpr>(BOOLEAN(), "not", rangeExpr);

      return handleNullAllowed(notRangeExpr, subfieldExpr, nullAllowed);
    }

    // Handle other filter types as needed
    case common::FilterKind::kHugeintRange:
    case common::FilterKind::kHugeintValuesUsingHashTable:
    default:
      // For unhandled filter types, return the subfield expression as is
      VELOX_NYI(
          "Filter type not yet implemented in filterToExpr: {}",
          filter->toString());
      return subfieldExpr;
  }
}
} // namespace facebook::velox::core
