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

#include "velox/expression/ExprToSubfieldFilter.h"

#include "velox/expression/Expr.h"

using namespace facebook::velox;

namespace facebook::velox::exec {

namespace {

VectorPtr toConstant(
    const core::TypedExprPtr& expr,
    core::ExpressionEvaluator* evaluator) {
  auto exprSet = evaluator->compile(expr);
  if (!exprSet->exprs()[0]->isConstant()) {
    return nullptr;
  }
  RowVector input(
      evaluator->pool(), ROW({}, {}), nullptr, 1, std::vector<VectorPtr>{});
  SelectivityVector rows(1);
  VectorPtr result;
  try {
    evaluator->evaluate(exprSet.get(), rows, input, result);
  } catch (const VeloxUserError&) {
    return nullptr;
  }
  return result;
}

template <typename T>
T singleValue(const VectorPtr& vector) {
  auto simpleVector = vector->as<SimpleVector<T>>();
  VELOX_CHECK_NOT_NULL(simpleVector);
  return simpleVector->valueAt(0);
}

const core::CallTypedExpr* asCall(const core::ITypedExpr* expr) {
  return dynamic_cast<const core::CallTypedExpr*>(expr);
}

bool toSubfield(const core::ITypedExpr* field, common::Subfield& subfield) {
  std::vector<std::unique_ptr<common::Subfield::PathElement>> path;
  for (auto* current = field;;) {
    if (auto* fieldAccess =
            dynamic_cast<const core::FieldAccessTypedExpr*>(current)) {
      path.push_back(
          std::make_unique<common::Subfield::NestedField>(fieldAccess->name()));
    } else if (
        auto* dereference =
            dynamic_cast<const core::DereferenceTypedExpr*>(current)) {
      const auto& name = dereference->name();
      // When the field name is empty string, it typically means that the field
      // name was not set in the parent type.
      if (name.empty()) {
        return false;
      }
      path.push_back(std::make_unique<common::Subfield::NestedField>(name));
    } else if (dynamic_cast<const core::InputTypedExpr*>(current) == nullptr) {
      return false;
    } else {
      break;
    }

    if (current->inputs().empty()) {
      break;
    }
    if (current->inputs().size() != 1) {
      return false;
    }
    current = current->inputs()[0].get();
    if (current == nullptr) {
      return false;
    }
  }
  std::reverse(path.begin(), path.end());
  subfield = common::Subfield(std::move(path));
  return true;
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
    const core::TypedExprPtr& upperExpr,
    core::ExpressionEvaluator* evaluator) {
  auto upper = toConstant(upperExpr, evaluator);
  if (!upper) {
    return nullptr;
  }
  switch (upper->typeKind()) {
    case TypeKind::TINYINT:
      return lessThanOrEqual(singleValue<int8_t>(upper));
    case TypeKind::SMALLINT:
      return lessThanOrEqual(singleValue<int16_t>(upper));
    case TypeKind::INTEGER:
      return lessThanOrEqual(singleValue<int32_t>(upper));
    case TypeKind::BIGINT:
      return lessThanOrEqual(singleValue<int64_t>(upper));
    case TypeKind::HUGEINT:
      return lessThanOrEqualHugeint(singleValue<int128_t>(upper));
    case TypeKind::DOUBLE:
      return lessThanOrEqualDouble(singleValue<double>(upper));
    case TypeKind::REAL:
      return lessThanOrEqualFloat(singleValue<float>(upper));
    case TypeKind::VARCHAR:
      return lessThanOrEqual(singleValue<StringView>(upper));
    case TypeKind::TIMESTAMP:
      return lessThanOrEqual(singleValue<Timestamp>(upper));
    default:
      return nullptr;
  }
}

std::unique_ptr<common::Filter> makeLessThanFilter(
    const core::TypedExprPtr& upperExpr,
    core::ExpressionEvaluator* evaluator) {
  auto upper = toConstant(upperExpr, evaluator);
  if (!upper) {
    return nullptr;
  }
  switch (upper->typeKind()) {
    case TypeKind::TINYINT:
      return lessThan(singleValue<int8_t>(upper));
    case TypeKind::SMALLINT:
      return lessThan(singleValue<int16_t>(upper));
    case TypeKind::INTEGER:
      return lessThan(singleValue<int32_t>(upper));
    case TypeKind::BIGINT:
      return lessThan(singleValue<int64_t>(upper));
    case TypeKind::HUGEINT:
      return lessThanHugeint(singleValue<int128_t>(upper));
    case TypeKind::DOUBLE:
      return lessThanDouble(singleValue<double>(upper));
    case TypeKind::REAL:
      return lessThanFloat(singleValue<float>(upper));
    case TypeKind::VARCHAR:
      return lessThan(singleValue<StringView>(upper));
    case TypeKind::TIMESTAMP:
      return lessThan(singleValue<Timestamp>(upper));
    default:
      return nullptr;
  }
}

std::unique_ptr<common::Filter> makeGreaterThanOrEqualFilter(
    const core::TypedExprPtr& lowerExpr,
    core::ExpressionEvaluator* evaluator) {
  auto lower = toConstant(lowerExpr, evaluator);
  if (!lower) {
    return nullptr;
  }
  switch (lower->typeKind()) {
    case TypeKind::TINYINT:
      return greaterThanOrEqual(singleValue<int8_t>(lower));
    case TypeKind::SMALLINT:
      return greaterThanOrEqual(singleValue<int16_t>(lower));
    case TypeKind::INTEGER:
      return greaterThanOrEqual(singleValue<int32_t>(lower));
    case TypeKind::BIGINT:
      return greaterThanOrEqual(singleValue<int64_t>(lower));
    case TypeKind::HUGEINT:
      return greaterThanOrEqualHugeint(singleValue<int128_t>(lower));
    case TypeKind::DOUBLE:
      return greaterThanOrEqualDouble(singleValue<double>(lower));
    case TypeKind::REAL:
      return greaterThanOrEqualFloat(singleValue<float>(lower));
    case TypeKind::VARCHAR:
      return greaterThanOrEqual(singleValue<StringView>(lower));
    case TypeKind::TIMESTAMP:
      return greaterThanOrEqual(singleValue<Timestamp>(lower));
    default:
      return nullptr;
  }
}

std::unique_ptr<common::Filter> makeGreaterThanFilter(
    const core::TypedExprPtr& lowerExpr,
    core::ExpressionEvaluator* evaluator) {
  auto lower = toConstant(lowerExpr, evaluator);
  if (!lower) {
    return nullptr;
  }
  switch (lower->typeKind()) {
    case TypeKind::TINYINT:
      return greaterThan(singleValue<int8_t>(lower));
    case TypeKind::SMALLINT:
      return greaterThan(singleValue<int16_t>(lower));
    case TypeKind::INTEGER:
      return greaterThan(singleValue<int32_t>(lower));
    case TypeKind::BIGINT:
      return greaterThan(singleValue<int64_t>(lower));
    case TypeKind::HUGEINT:
      return greaterThanHugeint(singleValue<int128_t>(lower));
    case TypeKind::DOUBLE:
      return greaterThanDouble(singleValue<double>(lower));
    case TypeKind::REAL:
      return greaterThanFloat(singleValue<float>(lower));
    case TypeKind::VARCHAR:
      return greaterThan(singleValue<StringView>(lower));
    case TypeKind::TIMESTAMP:
      return greaterThan(singleValue<Timestamp>(lower));
    default:
      return nullptr;
  }
}

std::unique_ptr<common::Filter> makeEqualFilter(
    const core::TypedExprPtr& valueExpr,
    core::ExpressionEvaluator* evaluator) {
  auto value = toConstant(valueExpr, evaluator);
  if (!value) {
    return nullptr;
  }
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
    case TypeKind::HUGEINT:
      return equalHugeint(singleValue<int128_t>(value));
    case TypeKind::VARCHAR:
      return equal(singleValue<StringView>(value));
    case TypeKind::TIMESTAMP:
      return equal(singleValue<Timestamp>(value));
    default:
      return nullptr;
  }
}

std::unique_ptr<common::Filter> makeNotEqualFilter(
    const core::TypedExprPtr& valueExpr,
    core::ExpressionEvaluator* evaluator) {
  auto value = toConstant(valueExpr, evaluator);
  if (!value) {
    return nullptr;
  }

  std::unique_ptr<common::Filter> lessThanFilter =
      makeLessThanFilter(valueExpr, evaluator);
  if (!lessThanFilter) {
    return nullptr;
  }
  std::unique_ptr<common::Filter> greaterThanFilter =
      makeGreaterThanFilter(valueExpr, evaluator);
  if (!greaterThanFilter) {
    return nullptr;
  }

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
  } else if (value->typeKind() == TypeKind::HUGEINT) {
    VELOX_NYI();
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

std::unique_ptr<common::Filter> makeInFilter(
    const core::TypedExprPtr& expr,
    core::ExpressionEvaluator* evaluator,
    bool negated) {
  auto vector = toConstant(expr, evaluator);
  if (!(vector && vector->type()->isArray())) {
    return nullptr;
  }

  auto arrayVector = vector->valueVector()->as<ArrayVector>();
  auto index = vector->as<ConstantVector<ComplexType>>()->index();
  auto offset = arrayVector->offsetAt(index);
  auto size = arrayVector->sizeAt(index);
  auto elements = arrayVector->elements();

  auto elementType = arrayVector->type()->asArray().elementType();
  switch (elementType->kind()) {
    case TypeKind::TINYINT: {
      auto values = toInt64List<int8_t>(elements, offset, size);
      return negated ? notIn(values) : in(values);
    }
    case TypeKind::SMALLINT: {
      auto values = toInt64List<int16_t>(elements, offset, size);
      return negated ? notIn(values) : in(values);
    }
    case TypeKind::INTEGER: {
      auto values = toInt64List<int32_t>(elements, offset, size);
      return negated ? notIn(values) : in(values);
    }
    case TypeKind::BIGINT: {
      auto values = toInt64List<int64_t>(elements, offset, size);
      return negated ? notIn(values) : in(values);
    }
    case TypeKind::VARCHAR: {
      auto stringElements = elements->as<SimpleVector<StringView>>();
      std::vector<std::string> values;
      for (auto i = 0; i < size; i++) {
        values.push_back(stringElements->valueAt(offset + i).str());
      }
      if (negated) {
        return notIn(values);
      }
      return in(values);
    }
    default:
      return nullptr;
  }
}

std::unique_ptr<common::Filter> makeBetweenFilter(
    const core::TypedExprPtr& lowerExpr,
    const core::TypedExprPtr& upperExpr,
    core::ExpressionEvaluator* evaluator,
    bool negated) {
  auto lower = toConstant(lowerExpr, evaluator);
  if (!lower) {
    return nullptr;
  }
  auto upper = toConstant(upperExpr, evaluator);
  if (!upper) {
    return nullptr;
  }
  switch (lower->typeKind()) {
    case TypeKind::INTEGER:
      if (negated) {
        return notBetween(
            singleValue<int32_t>(lower), singleValue<int32_t>(upper));
      }
      return between(singleValue<int32_t>(lower), singleValue<int32_t>(upper));
    case TypeKind::BIGINT:
      if (negated) {
        return notBetween(
            singleValue<int64_t>(lower), singleValue<int64_t>(upper));
      }
      return between(singleValue<int64_t>(lower), singleValue<int64_t>(upper));
    case TypeKind::DOUBLE:
      return negated
          ? nullptr
          : betweenDouble(
                singleValue<double>(lower), singleValue<double>(upper));
    case TypeKind::REAL:
      return negated
          ? nullptr
          : betweenFloat(singleValue<float>(lower), singleValue<float>(upper));
    case TypeKind::VARCHAR:
      if (negated) {
        return notBetween(
            singleValue<StringView>(lower), singleValue<StringView>(upper));
      }
      return between(
          singleValue<StringView>(lower), singleValue<StringView>(upper));
    case TypeKind::TIMESTAMP:
      return negated
          ? nullptr
          : between(
                singleValue<Timestamp>(lower), singleValue<Timestamp>(upper));
    default:
      return nullptr;
  }
}

} // namespace

std::unique_ptr<common::Filter> leafCallToSubfieldFilter(
    const core::CallTypedExpr& call,
    common::Subfield& subfield,
    core::ExpressionEvaluator* evaluator,
    bool negated) {
  if (call.inputs().empty()) {
    return nullptr;
  }

  const auto* leftSide = call.inputs()[0].get();

  if (call.name() == "eq") {
    if (toSubfield(leftSide, subfield)) {
      return negated ? makeNotEqualFilter(call.inputs()[1], evaluator)
                     : makeEqualFilter(call.inputs()[1], evaluator);
    }
  } else if (call.name() == "neq") {
    if (toSubfield(leftSide, subfield)) {
      return negated ? makeEqualFilter(call.inputs()[1], evaluator)
                     : makeNotEqualFilter(call.inputs()[1], evaluator);
    }
  } else if (call.name() == "lte") {
    if (toSubfield(leftSide, subfield)) {
      return negated ? makeGreaterThanFilter(call.inputs()[1], evaluator)
                     : makeLessThanOrEqualFilter(call.inputs()[1], evaluator);
    }
  } else if (call.name() == "lt") {
    if (toSubfield(leftSide, subfield)) {
      return negated ? makeGreaterThanOrEqualFilter(call.inputs()[1], evaluator)
                     : makeLessThanFilter(call.inputs()[1], evaluator);
    }
  } else if (call.name() == "gte") {
    if (toSubfield(leftSide, subfield)) {
      return negated
          ? makeLessThanFilter(call.inputs()[1], evaluator)
          : makeGreaterThanOrEqualFilter(call.inputs()[1], evaluator);
    }
  } else if (call.name() == "gt") {
    if (toSubfield(leftSide, subfield)) {
      return negated ? makeLessThanOrEqualFilter(call.inputs()[1], evaluator)
                     : makeGreaterThanFilter(call.inputs()[1], evaluator);
    }
  } else if (call.name() == "between") {
    if (toSubfield(leftSide, subfield)) {
      return makeBetweenFilter(
          call.inputs()[1], call.inputs()[2], evaluator, negated);
    }
  } else if (call.name() == "in") {
    if (toSubfield(leftSide, subfield)) {
      return makeInFilter(call.inputs()[1], evaluator, negated);
    }
  } else if (call.name() == "is_null") {
    if (toSubfield(leftSide, subfield)) {
      if (negated) {
        return isNotNull();
      }
      return isNull();
    }
  }
  return nullptr;
}

std::pair<common::Subfield, std::unique_ptr<common::Filter>> toSubfieldFilter(
    const core::TypedExprPtr& expr,
    core::ExpressionEvaluator* evaluator) {
  if (auto call = asCall(expr.get())) {
    if (call->name() == "or") {
      auto left = toSubfieldFilter(call->inputs()[0], evaluator);
      auto right = toSubfieldFilter(call->inputs()[1], evaluator);
      VELOX_CHECK(left.first == right.first);
      return {
          std::move(left.first),
          makeOrFilter(std::move(left.second), std::move(right.second))};
    }
    common::Subfield subfield;
    std::unique_ptr<common::Filter> filter;
    if (call->name() == "not") {
      if (auto* inner = asCall(call->inputs()[0].get())) {
        filter = leafCallToSubfieldFilter(*inner, subfield, evaluator, true);
      }
    } else {
      filter = leafCallToSubfieldFilter(*call, subfield, evaluator, false);
    }
    if (filter) {
      return std::make_pair(std::move(subfield), std::move(filter));
    }
  }
  VELOX_UNSUPPORTED(
      "Unsupported expression for range filter: {}", expr->toString());
}

} // namespace facebook::velox::exec
