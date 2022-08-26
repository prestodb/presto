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

#include <velox/core/QueryCtx.h>
#include <velox/expression/Expr.h>
#include <velox/expression/ExprToSubfieldFilter.h>

using namespace facebook::velox;

namespace facebook::velox::exec {

namespace {

VectorPtr toConstant(
    const core::TypedExprPtr& expr,
    const std::shared_ptr<core::QueryCtx>& queryCtx) {
  auto data = std::make_shared<RowVector>(
      queryCtx->pool(), ROW({}, {}), nullptr, 1, std::vector<VectorPtr>{});
  core::ExecCtx execCtx{queryCtx->pool(), queryCtx.get()};
  ExprSet exprSet({expr}, &execCtx);
  EvalCtx evalCtx(&execCtx, &exprSet, data.get());

  SelectivityVector rows(1);
  std::vector<VectorPtr> results(1);
  exprSet.eval(rows, evalCtx, results);

  return results[0];
}

template <typename T>
T singleValue(const VectorPtr& vector) {
  auto simpleVector = vector->as<SimpleVector<T>>();
  VELOX_CHECK_NOT_NULL(simpleVector);
  return simpleVector->valueAt(0);
}

const core::FieldAccessTypedExpr* asField(
    const core::ITypedExpr* expr,
    int index) {
  return dynamic_cast<const core::FieldAccessTypedExpr*>(
      expr->inputs()[index].get());
}

const core::CallTypedExpr* asCall(const core::ITypedExpr* expr) {
  return dynamic_cast<const core::CallTypedExpr*>(expr);
}

common::BigintRange* asBigintRange(std::unique_ptr<common::Filter>& filter) {
  return dynamic_cast<common::BigintRange*>(filter.get());
}

common::BigintMultiRange* asBigintMultiRange(
    std::unique_ptr<common::Filter>& filter) {
  return dynamic_cast<common::BigintMultiRange*>(filter.get());
}

template <typename T, typename U>
std::unique_ptr<T> asUniquePtr(std::unique_ptr<U> ptr) {
  return std::unique_ptr<T>(static_cast<T*>(ptr.release()));
}

std::unique_ptr<common::Filter> makeOrFilter(
    std::unique_ptr<common::Filter> a,
    std::unique_ptr<common::Filter> b) {
  if (asBigintRange(a) && asBigintRange(b)) {
    return bigintOr(
        asUniquePtr<common::BigintRange>(std::move(a)),
        asUniquePtr<common::BigintRange>(std::move(b)));
  }

  if (asBigintRange(a) && asBigintMultiRange(b)) {
    const auto& ranges = asBigintMultiRange(b)->ranges();
    std::vector<std::unique_ptr<common::BigintRange>> newRanges;
    newRanges.emplace_back(asUniquePtr<common::BigintRange>(std::move(a)));
    for (const auto& range : ranges) {
      newRanges.emplace_back(asUniquePtr<common::BigintRange>(range->clone()));
    }

    std::sort(
        newRanges.begin(), newRanges.end(), [](const auto& a, const auto& b) {
          return a->lower() < b->lower();
        });

    return std::make_unique<common::BigintMultiRange>(
        std::move(newRanges), false);
  }

  if (asBigintMultiRange(a) && asBigintRange(b)) {
    return makeOrFilter(std::move(b), std::move(a));
  }

  return orFilter(std::move(a), std::move(b));
}

std::unique_ptr<common::Filter> makeLessThanOrEqualFilter(
    const core::TypedExprPtr& upperExpr) {
  auto queryCtx = core::QueryCtx::createForTest();
  auto upper = toConstant(upperExpr, queryCtx);
  switch (upper->typeKind()) {
    case TypeKind::TINYINT:
      return lessThanOrEqual(singleValue<int8_t>(upper));
    case TypeKind::SMALLINT:
      return lessThanOrEqual(singleValue<int16_t>(upper));
    case TypeKind::INTEGER:
      return lessThanOrEqual(singleValue<int32_t>(upper));
    case TypeKind::BIGINT:
      return lessThanOrEqual(singleValue<int64_t>(upper));
    case TypeKind::DOUBLE:
      return lessThanOrEqualDouble(singleValue<double>(upper));
    case TypeKind::REAL:
      return lessThanOrEqualFloat(singleValue<float>(upper));
    case TypeKind::VARCHAR:
      return lessThanOrEqual(singleValue<StringView>(upper));
    case TypeKind::DATE:
      return lessThanOrEqual(singleValue<Date>(upper).days());
    default:
      VELOX_NYI(
          "Unsupported value for less than or equals filter: {} <= {}",
          upper->type()->toString(),
          upper->toString(0));
  }
}

std::unique_ptr<common::Filter> makeLessThanFilter(
    const core::TypedExprPtr& upperExpr) {
  auto queryCtx = core::QueryCtx::createForTest();
  auto upper = toConstant(upperExpr, queryCtx);
  switch (upper->typeKind()) {
    case TypeKind::TINYINT:
      return lessThan(singleValue<int8_t>(upper));
    case TypeKind::SMALLINT:
      return lessThan(singleValue<int16_t>(upper));
    case TypeKind::INTEGER:
      return lessThan(singleValue<int32_t>(upper));
    case TypeKind::BIGINT:
      return lessThan(singleValue<int64_t>(upper));
    case TypeKind::DOUBLE:
      return lessThanDouble(singleValue<double>(upper));
    case TypeKind::REAL:
      return lessThanFloat(singleValue<float>(upper));
    case TypeKind::VARCHAR:
      return lessThan(singleValue<StringView>(upper));
    case TypeKind::DATE:
      return lessThan(singleValue<Date>(upper).days());
    default:
      VELOX_NYI(
          "Unsupported value for less than filter: {} <= {}",
          upper->type()->toString(),
          upper->toString(0));
  }
}

std::unique_ptr<common::Filter> makeGreaterThanOrEqualFilter(
    const core::TypedExprPtr& lowerExpr) {
  auto queryCtx = core::QueryCtx::createForTest();
  auto lower = toConstant(lowerExpr, queryCtx);
  switch (lower->typeKind()) {
    case TypeKind::TINYINT:
      return greaterThanOrEqual(singleValue<int8_t>(lower));
    case TypeKind::SMALLINT:
      return greaterThanOrEqual(singleValue<int16_t>(lower));
    case TypeKind::INTEGER:
      return greaterThanOrEqual(singleValue<int32_t>(lower));
    case TypeKind::BIGINT:
      return greaterThanOrEqual(singleValue<int64_t>(lower));
    case TypeKind::DOUBLE:
      return greaterThanOrEqualDouble(singleValue<double>(lower));
    case TypeKind::REAL:
      return greaterThanOrEqualFloat(singleValue<float>(lower));
    case TypeKind::VARCHAR:
      return greaterThanOrEqual(singleValue<StringView>(lower));
    case TypeKind::DATE:
      return greaterThanOrEqual(singleValue<Date>(lower).days());
    default:
      VELOX_NYI(
          "Unsupported value for greater than or equals filter: {} >= {}",
          lower->type()->toString(),
          lower->toString(0));
  }
}

std::unique_ptr<common::Filter> makeGreaterThanFilter(
    const core::TypedExprPtr& lowerExpr) {
  auto queryCtx = core::QueryCtx::createForTest();
  auto lower = toConstant(lowerExpr, queryCtx);
  switch (lower->typeKind()) {
    case TypeKind::TINYINT:
      return greaterThan(singleValue<int8_t>(lower));
    case TypeKind::SMALLINT:
      return greaterThan(singleValue<int16_t>(lower));
    case TypeKind::INTEGER:
      return greaterThan(singleValue<int32_t>(lower));
    case TypeKind::BIGINT:
      return greaterThan(singleValue<int64_t>(lower));
    case TypeKind::DOUBLE:
      return greaterThanDouble(singleValue<double>(lower));
    case TypeKind::REAL:
      return greaterThanFloat(singleValue<float>(lower));
    case TypeKind::VARCHAR:
      return greaterThan(singleValue<StringView>(lower));
    case TypeKind::DATE:
      return greaterThan(singleValue<Date>(lower).days());
    default:
      VELOX_NYI(
          "Unsupported value for greater than filter: {} > {}",
          lower->type()->toString(),
          lower->toString(0));
  }
}

std::unique_ptr<common::Filter> makeEqualFilter(
    const core::TypedExprPtr& valueExpr) {
  auto queryCtx = core::QueryCtx::createForTest();
  auto value = toConstant(valueExpr, queryCtx);
  switch (value->typeKind()) {
    case TypeKind::BOOLEAN:
      return boolEqual(singleValue<bool>(value));
    case TypeKind::TINYINT:
      return equal(singleValue<int8_t>(value));
    case TypeKind::SMALLINT:
      return equal(singleValue<int16_t>(value));
    case TypeKind::INTEGER:
      return equal(singleValue<int32_t>(value));
    case TypeKind::BIGINT:
      return equal(singleValue<int64_t>(value));
    case TypeKind::VARCHAR:
      return equal(singleValue<StringView>(value));
    case TypeKind::DATE:
      return equal(singleValue<Date>(value).days());
    default:
      VELOX_NYI(
          "Unsupported value for equals filter: {} = {}",
          value->type()->toString(),
          value->toString(0));
  }
}

std::unique_ptr<common::Filter> makeNotEqualFilter(
    const core::TypedExprPtr& valueExpr) {
  auto queryCtx = core::QueryCtx::createForTest();
  auto value = toConstant(valueExpr, queryCtx);

  std::unique_ptr<common::Filter> lessThanFilter =
      makeLessThanFilter(valueExpr);
  std::unique_ptr<common::Filter> greaterThanFilter =
      makeGreaterThanFilter(valueExpr);

  if (value->typeKind() == TypeKind::TINYINT ||
      value->typeKind() == TypeKind::SMALLINT ||
      value->typeKind() == TypeKind::INTEGER ||
      value->typeKind() == TypeKind::BIGINT) {
    // Cast lessThanFilter and greaterThanFilter to
    // std::unique_ptr<common::BigintRange>.
    std::vector<std::unique_ptr<common::BigintRange>> ranges;
    auto lessRange =
        dynamic_cast<common::BigintRange*>(lessThanFilter.release());
    VELOX_CHECK_NOT_NULL(lessRange, "Less-than range is null");
    ranges.emplace_back(std::unique_ptr<common::BigintRange>(lessRange));

    auto greaterRange =
        dynamic_cast<common::BigintRange*>(greaterThanFilter.release());
    VELOX_CHECK_NOT_NULL(greaterRange, "Greater-than range is null");
    ranges.emplace_back(std::unique_ptr<common::BigintRange>(greaterRange));

    return std::make_unique<common::BigintMultiRange>(std::move(ranges), false);
  } else {
    std::vector<std::unique_ptr<common::Filter>> filters;
    filters.emplace_back(std::move(lessThanFilter));
    filters.emplace_back(std::move(greaterThanFilter));
    return std::make_unique<common::MultiRange>(
        std::move(filters), false, false);
  }
}

template <typename T>
std::vector<int64_t>
toInt64List(const VectorPtr& vector, vector_size_t start, vector_size_t size) {
  auto ints = vector->as<SimpleVector<T>>();
  std::vector<int64_t> values;
  for (auto i = 0; i < size; i++) {
    values.push_back(ints->valueAt(start + i));
  }
  return values;
}

std::unique_ptr<common::Filter> makeInFilter(const core::TypedExprPtr& expr) {
  auto queryCtx = core::QueryCtx::createForTest();
  auto vector = toConstant(expr, queryCtx);
  VELOX_CHECK_EQ(vector->typeKind(), TypeKind::ARRAY);

  auto arrayVector = vector->valueVector()->as<ArrayVector>();
  auto index = vector->as<ConstantVector<ComplexType>>()->index();
  auto offset = arrayVector->offsetAt(index);
  auto size = arrayVector->sizeAt(index);
  auto elements = arrayVector->elements();

  auto elementType = arrayVector->type()->asArray().elementType();
  switch (elementType->kind()) {
    case TypeKind::TINYINT:
      return in(toInt64List<int16_t>(elements, offset, size));
    case TypeKind::SMALLINT:
      return in(toInt64List<int16_t>(elements, offset, size));
    case TypeKind::INTEGER:
      return in(toInt64List<int32_t>(elements, offset, size));
    case TypeKind::BIGINT:
      return in(toInt64List<int64_t>(elements, offset, size));
    case TypeKind::VARCHAR: {
      auto stringElements = elements->as<SimpleVector<StringView>>();
      std::vector<std::string> values;
      for (auto i = 0; i < size; i++) {
        values.push_back(stringElements->valueAt(offset + i).str());
      }
      return in(values);
    }
    default:
      VELOX_NYI(
          "Unsupported value type for 'in' filter: {}",
          elementType->toString());
  }
}

std::unique_ptr<common::Filter> makeBetweenFilter(
    const core::TypedExprPtr& lowerExpr,
    const core::TypedExprPtr& upperExpr) {
  auto queryCtx = core::QueryCtx::createForTest();
  auto lower = toConstant(lowerExpr, queryCtx);
  auto upper = toConstant(upperExpr, queryCtx);
  switch (lower->typeKind()) {
    case TypeKind::BIGINT:
      return between(singleValue<int64_t>(lower), singleValue<int64_t>(upper));
    case TypeKind::DOUBLE:
      return betweenDouble(
          singleValue<double>(lower), singleValue<double>(upper));
    case TypeKind::REAL:
      return betweenFloat(singleValue<float>(lower), singleValue<float>(upper));
    case TypeKind::DATE:
      return between(
          singleValue<Date>(lower).days(), singleValue<Date>(upper).days());
    case TypeKind::VARCHAR:
      return between(
          singleValue<StringView>(lower), singleValue<StringView>(upper));
    default:
      VELOX_NYI(
          "Unsupported value for 'between' filter: {} BETWEEN {} AND {}",
          lower->type()->toString(),
          lower->toString(0),
          upper->toString(0));
  }
}
} // namespace

std::pair<common::Subfield, std::unique_ptr<common::Filter>> toSubfieldFilter(
    const core::TypedExprPtr& expr) {
  using common::Subfield;
  if (auto call = asCall(expr.get())) {
    if (call->name() == "or") {
      auto left = toSubfieldFilter(call->inputs()[0]);
      auto right = toSubfieldFilter(call->inputs()[1]);
      VELOX_CHECK(left.first == right.first);
      return {
          std::move(left.first),
          makeOrFilter(std::move(left.second), std::move(right.second))};
    } else if (call->name() == "eq") {
      if (auto field = asField(call, 0)) {
        return {Subfield(field->name()), makeEqualFilter(call->inputs()[1])};
      }
    } else if (call->name() == "neq") {
      if (auto field = asField(call, 0)) {
        return {Subfield(field->name()), makeNotEqualFilter(call->inputs()[1])};
      }
    } else if (call->name() == "lte") {
      if (auto field = asField(call, 0)) {
        return {
            Subfield(field->name()),
            makeLessThanOrEqualFilter(call->inputs()[1])};
      }
    } else if (call->name() == "lt") {
      if (auto field = asField(call, 0)) {
        return {Subfield(field->name()), makeLessThanFilter(call->inputs()[1])};
      }
    } else if (call->name() == "gte") {
      if (auto field = asField(call, 0)) {
        return {
            Subfield(field->name()),
            makeGreaterThanOrEqualFilter(call->inputs()[1])};
      }
    } else if (call->name() == "gt") {
      if (auto field = asField(call, 0)) {
        return {
            Subfield(field->name()), makeGreaterThanFilter(call->inputs()[1])};
      }
    } else if (call->name() == "between") {
      if (auto field = asField(call, 0)) {
        return {
            Subfield(field->name()),
            makeBetweenFilter(call->inputs()[1], call->inputs()[2])};
      }
    } else if (call->name() == "in") {
      if (auto field = asField(call, 0)) {
        return {Subfield(field->name()), makeInFilter(call->inputs()[1])};
      }
    } else if (call->name() == "is_null") {
      if (auto field = asField(call, 0)) {
        return {Subfield(field->name()), isNull()};
      }
    } else if (call->name() == "not") {
      if (auto nestedCall = asCall(call->inputs()[0].get())) {
        if (nestedCall->name() == "is_null") {
          if (auto field = asField(nestedCall, 0)) {
            return {Subfield(field->name()), isNotNull()};
          }
        }
      }
    }
  }

  VELOX_NYI("Unsupported expression for range filter: {}", expr->toString());
}
} // namespace facebook::velox::exec
