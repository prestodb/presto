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

#include "velox/functions/sparksql/specialforms/MakeDecimal.h"
#include "velox/expression/ConstantExpr.h"

namespace facebook::velox::functions::sparksql {
namespace {
template <typename T>
class MakeDecimalFunction final : public exec::VectorFunction {
 public:
  MakeDecimalFunction(uint8_t resultPrecision, bool nullOnOverflow)
      : resultPrecision_(resultPrecision), nullOnOverflow_(nullOnOverflow) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args, // Not using const ref so we can reuse args
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const final {
    VELOX_USER_CHECK_GE(
        args.size(), 1, "Make_decimal expects at least one argument.");
    VELOX_USER_CHECK(
        args[0]->isConstantEncoding() || args[0]->isFlatEncoding(),
        "Single-arg deterministic functions receive their only argument as flat or constant vector.");
    context.ensureWritable(rows, outputType, result);
    result->clearNulls(rows);
    auto rawResults = result->asUnchecked<FlatVector<T>>()->mutableRawValues();
    const int128_t bound = DecimalUtil::kPowersOfTen[resultPrecision_];
    if (args[0]->isConstantEncoding()) {
      // Fast path for constant vectors.
      applyConstant(rows, args[0], bound, context, result, rawResults);
    } else {
      // Fast path for flat vectors.
      applyFlat(rows, args[0], bound, context, result, rawResults);
    }
  }

 private:
  void applyConstant(
      const SelectivityVector& rows,
      VectorPtr& arg,
      int128_t bound,
      exec::EvalCtx& context,
      VectorPtr& result,
      T* rawResults) const {
    const auto constant =
        arg->asUnchecked<ConstantVector<int64_t>>()->valueAt(0);
    if constexpr (std::is_same_v<T, int64_t>) {
      const bool overflow = constant <= -bound || constant >= bound;
      if (overflow) {
        // The unscaled value cannot fit in the requested precision.
        if (nullOnOverflow_) {
          result->addNulls(rows);
        } else {
          context.setErrors(
              rows, overflowException(constant, resultPrecision_));
        }
      } else {
        rows.applyToSelected(
            [&](vector_size_t row) { rawResults[row] = constant; });
      }
    } else {
      rows.applyToSelected(
          [&](vector_size_t row) { rawResults[row] = (int128_t)constant; });
    }
  }

  void applyFlat(
      const SelectivityVector& rows,
      VectorPtr& arg,
      int128_t bound,
      exec::EvalCtx& context,
      VectorPtr& result,
      T* rawResults) const {
    auto rawValues =
        arg->asUnchecked<FlatVector<int64_t>>()->mutableRawValues();
    if constexpr (std::is_same_v<T, int64_t>) {
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        const auto unscaled = rawValues[row];
        if (unscaled <= -bound || unscaled >= bound) {
          // The unscaled value cannot fit in the requested precision.
          if (nullOnOverflow_) {
            result->setNull(row, true);
          } else {
            context.setError(
                row, overflowException(unscaled, resultPrecision_));
          }
        } else {
          rawResults[row] = unscaled;
        }
      });
    } else {
      rows.applyToSelected([&](vector_size_t row) {
        rawResults[row] = (int128_t)rawValues[row];
      });
    }
  }

  std::exception_ptr overflowException(
      int64_t unscaled,
      uint8_t resultPrecision) const {
    return std::make_exception_ptr(std::overflow_error(fmt::format(
        "Unscaled value {} too large for precision {}.",
        unscaled,
        resultPrecision)));
  }

  const uint8_t resultPrecision_;
  const bool nullOnOverflow_;
};

std::shared_ptr<exec::VectorFunction> createMakeDecimal(
    const TypePtr& type,
    bool nullOnOverflow) {
  const uint8_t precision = getDecimalPrecisionScale(*type).first;
  if (type->isShortDecimal()) {
    return std::make_shared<MakeDecimalFunction<int64_t>>(
        precision, nullOnOverflow);
  }
  return std::make_shared<MakeDecimalFunction<int128_t>>(
      precision, nullOnOverflow);
}
} // namespace

TypePtr MakeDecimalCallToSpecialForm::resolveType(
    const std::vector<TypePtr>& /*argTypes*/) {
  VELOX_FAIL("Make decimal function does not support type resolution.");
}

exec::ExprPtr MakeDecimalCallToSpecialForm::constructSpecialForm(
    const TypePtr& type,
    std::vector<exec::ExprPtr>&& args,
    bool trackCpuUsage,
    const core::QueryConfig& /*config*/) {
  VELOX_USER_CHECK(
      type->isDecimal(),
      "The result type of make_decimal should be decimal type.");
  VELOX_USER_CHECK_GE(
      args.size(), 1, "Make_decimal expects one or two arguments.");
  VELOX_USER_CHECK_LE(
      args.size(), 2, "Make_decimal expects one or two arguments.");
  VELOX_USER_CHECK_EQ(
      args[0]->type()->kind(),
      TypeKind::BIGINT,
      "The first argument of make_decimal should be of bigint type.");
  bool nullOnOverflow = true;
  if (args.size() > 1) {
    VELOX_USER_CHECK_EQ(
        args[1]->type()->kind(),
        TypeKind::BOOLEAN,
        "The second argument of make_decimal should be of boolean type.");
    auto constantExpr = std::dynamic_pointer_cast<exec::ConstantExpr>(args[1]);
    VELOX_USER_CHECK_NOT_NULL(
        constantExpr,
        "The second argument of make_decimal should be constant expression.");
    VELOX_USER_CHECK(
        constantExpr->value()->isConstantEncoding(),
        "The second argument of make_decimal should be wrapped in constant vector.");
    auto constantVector =
        constantExpr->value()->asUnchecked<ConstantVector<bool>>();
    VELOX_USER_CHECK(
        !constantVector->isNullAt(0),
        "The second argument of make_decimal is non-nullable.");
    nullOnOverflow = constantVector->valueAt(0);
  }
  return std::make_shared<exec::Expr>(
      type,
      std::move(args),
      createMakeDecimal(type, nullOnOverflow),
      exec::VectorFunctionMetadata{},
      kMakeDecimal,
      trackCpuUsage);
}
} // namespace facebook::velox::functions::sparksql
