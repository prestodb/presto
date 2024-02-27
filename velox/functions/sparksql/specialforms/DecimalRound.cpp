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

#include "velox/functions/sparksql/specialforms/DecimalRound.h"
#include "velox/expression/ConstantExpr.h"

namespace facebook::velox::functions::sparksql {
namespace {

template <typename TResult, typename TInput>
class DecimalRoundFunction : public exec::VectorFunction {
 public:
  DecimalRoundFunction(
      int32_t scale,
      uint8_t inputPrecision,
      uint8_t inputScale,
      uint8_t resultPrecision,
      uint8_t resultScale)
      : scale_(
            scale >= 0
                ? std::min(scale, (int32_t)LongDecimalType::kMaxPrecision)
                : std::max(scale, -(int32_t)LongDecimalType::kMaxPrecision)),
        inputPrecision_(inputPrecision),
        inputScale_(inputScale),
        resultPrecision_(resultPrecision),
        resultScale_(resultScale) {
    const auto [p, s] = DecimalRoundCallToSpecialForm::getResultPrecisionScale(
        inputPrecision, inputScale, scale);
    VELOX_USER_CHECK_EQ(
        p,
        resultPrecision,
        "The result precision of decimal_round is inconsistent with Spark expected.");
    VELOX_USER_CHECK_EQ(
        s,
        resultScale,
        "The result scale of decimal_round is inconsistent with Spark expected.");

    // Decide the rescale factor of divide and multiply when rounding to a
    // negative scale.
    auto rescaleFactor = [&](int32_t rescale) {
      VELOX_USER_CHECK_GT(
          rescale, 0, "A non-negative rescale value is expected.");
      return DecimalUtil::kPowersOfTen[std::min(
          rescale, (int32_t)LongDecimalType::kMaxPrecision)];
    };
    if (scale_ < 0) {
      divideFactor_ = rescaleFactor(inputScale_ - scale_);
      multiplyFactor_ = rescaleFactor(-scale_);
    }
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& resultType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_USER_CHECK(
        args[0]->isConstantEncoding() || args[0]->isFlatEncoding(),
        "Single-arg deterministic functions receive their only argument as flat or constant vector.");
    context.ensureWritable(rows, resultType, result);
    result->clearNulls(rows);
    auto rawResults =
        result->asUnchecked<FlatVector<TResult>>()->mutableRawValues();
    if (args[0]->isConstantEncoding()) {
      // Fast path for constant vector.
      applyConstant(rows, args[0], rawResults);
    } else {
      // Fast path for flat vector.
      applyFlat(rows, args[0], rawResults);
    }
  }

  bool supportsFlatNoNullsFastPath() const override {
    return true;
  }

 private:
  inline TResult applyRound(const TInput& input) const {
    if (scale_ >= 0) {
      TResult rescaledValue;
      const auto status = DecimalUtil::rescaleWithRoundUp<TInput, TResult>(
          input,
          inputPrecision_,
          inputScale_,
          resultPrecision_,
          resultScale_,
          rescaledValue);
      VELOX_DCHECK(status.ok());
      return rescaledValue;
    } else {
      TResult rescaledValue;
      DecimalUtil::divideWithRoundUp<TResult, TInput, int128_t>(
          rescaledValue, input, divideFactor_.value(), false, 0, 0);
      rescaledValue *= multiplyFactor_.value();
      return rescaledValue;
    }
  }

  void applyConstant(
      const SelectivityVector& rows,
      const VectorPtr& arg,
      TResult* rawResults) const {
    const TResult rounded =
        applyRound(arg->asUnchecked<ConstantVector<TInput>>()->valueAt(0));
    rows.applyToSelected([&](auto row) { rawResults[row] = rounded; });
  }

  void applyFlat(
      const SelectivityVector& rows,
      const VectorPtr& arg,
      TResult* rawResults) const {
    auto rawValues = arg->asUnchecked<FlatVector<TInput>>()->mutableRawValues();
    rows.applyToSelected(
        [&](auto row) { rawResults[row] = applyRound(rawValues[row]); });
  }

  const int32_t scale_;
  const uint8_t inputPrecision_;
  const uint8_t inputScale_;
  const uint8_t resultPrecision_;
  const uint8_t resultScale_;
  std::optional<int128_t> divideFactor_ = std::nullopt;
  std::optional<int128_t> multiplyFactor_ = std::nullopt;
};

std::shared_ptr<exec::VectorFunction> createDecimalRound(
    const TypePtr& inputType,
    int32_t scale,
    const TypePtr& resultType) {
  const auto [inputPrecision, inputScale] =
      getDecimalPrecisionScale(*inputType);
  const auto [resultPrecision, resultScale] =
      getDecimalPrecisionScale(*resultType);
  if (inputType->isShortDecimal()) {
    if (resultType->isShortDecimal()) {
      return std::make_shared<DecimalRoundFunction<int64_t, int64_t>>(
          scale, inputPrecision, inputScale, resultPrecision, resultScale);
    } else {
      return std::make_shared<DecimalRoundFunction<int128_t, int64_t>>(
          scale, inputPrecision, inputScale, resultPrecision, resultScale);
    }
  } else {
    if (resultType->isShortDecimal()) {
      return std::make_shared<DecimalRoundFunction<int64_t, int128_t>>(
          scale, inputPrecision, inputScale, resultPrecision, resultScale);
    } else {
      return std::make_shared<DecimalRoundFunction<int128_t, int128_t>>(
          scale, inputPrecision, inputScale, resultPrecision, resultScale);
    }
  }
}
} // namespace

std::pair<uint8_t, uint8_t>
DecimalRoundCallToSpecialForm::getResultPrecisionScale(
    uint8_t precision,
    uint8_t scale,
    int32_t roundScale) {
  // After rounding we may need one more digit in the integral part,
  // e.g. 'decimal_round(9.9, 0)' -> '10', 'decimal_round(99, -1)' -> '100'.
  const int32_t integralLeastNumDigits = precision - scale + 1;
  if (roundScale < 0) {
    // Negative scale means we need to adjust `-scale` number of digits before
    // the decimal point, which means we need at least `-scale + 1` digits after
    // rounding, and the result scale is 0.
    const auto newPrecision = std::max(
        integralLeastNumDigits,
        -std::max(roundScale, -(int32_t)LongDecimalType::kMaxPrecision) + 1);
    // We have to accept the risk of overflow as we can't exceed the max
    // precision.
    return {std::min(newPrecision, (int32_t)LongDecimalType::kMaxPrecision), 0};
  }
  const uint8_t newScale = std::min((int32_t)scale, roundScale);
  // We have to accept the risk of overflow as we cannot exceed the max
  // precision.
  return {
      std::min(
          integralLeastNumDigits + newScale,
          (int32_t)LongDecimalType::kMaxPrecision),
      newScale};
}

TypePtr DecimalRoundCallToSpecialForm::resolveType(
    const std::vector<TypePtr>& /*argTypes*/) {
  VELOX_FAIL("Decimal round function does not support type resolution.");
}

exec::ExprPtr DecimalRoundCallToSpecialForm::constructSpecialForm(
    const TypePtr& type,
    std::vector<exec::ExprPtr>&& args,
    bool trackCpuUsage,
    const core::QueryConfig& /*config*/) {
  VELOX_USER_CHECK(
      type->isDecimal(),
      "The result type of decimal_round should be decimal type.");
  VELOX_USER_CHECK_GE(
      args.size(), 1, "Decimal_round expects one or two arguments.");
  VELOX_USER_CHECK_LE(
      args.size(), 2, "Decimal_round expects one or two arguments.");
  VELOX_USER_CHECK(
      args[0]->type()->isDecimal(),
      "The first argument of decimal_round should be of decimal type.");

  int32_t scale = 0;
  if (args.size() > 1) {
    VELOX_USER_CHECK_EQ(
        args[1]->type()->kind(),
        TypeKind::INTEGER,
        "The second argument of decimal_round should be of integer type.");
    auto constantExpr = std::dynamic_pointer_cast<exec::ConstantExpr>(args[1]);
    VELOX_USER_CHECK_NOT_NULL(
        constantExpr,
        "The second argument of decimal_round should be constant expression.");
    VELOX_USER_CHECK(
        constantExpr->value()->isConstantEncoding(),
        "The second argument of decimal_round should be wrapped in constant vector.");
    auto constantVector =
        constantExpr->value()->asUnchecked<ConstantVector<int32_t>>();
    VELOX_USER_CHECK(
        !constantVector->isNullAt(0),
        "The second argument of decimal_round is non-nullable.");
    scale = constantVector->valueAt(0);
  }

  auto decimalRound = createDecimalRound(args[0]->type(), scale, type);

  return std::make_shared<exec::Expr>(
      type,
      std::move(args),
      std::move(decimalRound),
      kRoundDecimal,
      trackCpuUsage);
}
} // namespace facebook::velox::functions::sparksql
