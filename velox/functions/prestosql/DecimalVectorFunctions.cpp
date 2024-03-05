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

#include "velox/expression/DecodedArgs.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/prestosql/ArithmeticImpl.h"
#include "velox/type/DecimalUtil.h"

namespace facebook::velox::functions {
namespace {

struct ExtraParams {
  union params {
    struct round {
      std::optional<int32_t> decimal{};
    } round;
  } params;
};

template <
    typename R /* Result Type */,
    typename A /* Argument1 */,
    typename B /* Argument2 */,
    typename Operation /* Arithmetic operation */>
class DecimalBaseFunction : public exec::VectorFunction {
 public:
  DecimalBaseFunction(uint8_t aRescale, uint8_t bRescale)
      : aRescale_(aRescale), bRescale_(bRescale) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& resultType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    auto rawResults = prepareResults(rows, resultType, context, result);
    if (args[0]->isConstantEncoding() && args[1]->isFlatEncoding()) {
      // Fast path for (const, flat).
      auto constant = args[0]->asUnchecked<SimpleVector<A>>()->valueAt(0);
      auto flatValues = args[1]->asUnchecked<FlatVector<B>>();
      auto rawValues = flatValues->mutableRawValues();
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        Operation::template apply<R, A, B>(
            rawResults[row], constant, rawValues[row], aRescale_, bRescale_);
      });
    } else if (args[0]->isFlatEncoding() && args[1]->isConstantEncoding()) {
      // Fast path for (flat, const).
      auto flatValues = args[0]->asUnchecked<FlatVector<A>>();
      auto constant = args[1]->asUnchecked<SimpleVector<B>>()->valueAt(0);
      auto rawValues = flatValues->mutableRawValues();
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        Operation::template apply<R, A, B>(
            rawResults[row], rawValues[row], constant, aRescale_, bRescale_);
      });
    } else if (args[0]->isFlatEncoding() && args[1]->isFlatEncoding()) {
      // Fast path for (flat, flat).
      auto flatA = args[0]->asUnchecked<FlatVector<A>>();
      auto rawA = flatA->mutableRawValues();
      auto flatB = args[1]->asUnchecked<FlatVector<B>>();
      auto rawB = flatB->mutableRawValues();
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        Operation::template apply<R, A, B>(
            rawResults[row], rawA[row], rawB[row], aRescale_, bRescale_);
      });
    } else {
      // Fast path if one or more arguments are encoded.
      exec::DecodedArgs decodedArgs(rows, args, context);
      auto a = decodedArgs.at(0);
      auto b = decodedArgs.at(1);
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        Operation::template apply<R, A, B>(
            rawResults[row],
            a->valueAt<A>(row),
            b->valueAt<B>(row),
            aRescale_,
            bRescale_);
      });
    }
  }

 private:
  R* prepareResults(
      const SelectivityVector& rows,
      const TypePtr& resultType,
      exec::EvalCtx& context,
      VectorPtr& result) const {
    context.ensureWritable(rows, resultType, result);
    result->clearNulls(rows);
    return result->asUnchecked<FlatVector<R>>()->mutableRawValues();
  }

  const uint8_t aRescale_;
  const uint8_t bRescale_;
};

template <
    typename R /* Result Type */,
    typename A /* Argument */,
    typename Operation /* Arithmetic operation */>
class DecimalUnaryFunction : public exec::VectorFunction {
 public:
  explicit DecimalUnaryFunction(
      uint8_t aPrecision,
      uint8_t aRescale,
      const ExtraParams& param)
      : aPrecision_(aPrecision), aRescale_(aRescale), param_(param) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& resultType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    // Single-arg deterministic functions receive their only
    // argument as flat or constant only.
    auto rawResults = prepareResults(rows, resultType, context, result);
    if (args[0]->isConstantEncoding()) {
      // Fast path for constant vectors.
      auto constant = args[0]->asUnchecked<SimpleVector<A>>()->valueAt(0);
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        Operation::template apply<R, A>(
            rawResults[row], constant, aPrecision_, aRescale_, param_);
      });
    } else {
      // Fast path for flat.
      auto flatA = args[0]->asUnchecked<FlatVector<A>>();
      auto rawA = flatA->mutableRawValues();
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        Operation::template apply<R, A>(
            rawResults[row], rawA[row], aPrecision_, aRescale_, param_);
      });
    }
  }

  bool supportsFlatNoNullsFastPath() const override {
    return true;
  }

 private:
  R* prepareResults(
      const SelectivityVector& rows,
      const TypePtr& resultType,
      exec::EvalCtx& context,
      VectorPtr& result) const {
    context.ensureWritable(rows, resultType, result);
    result->clearNulls(rows);
    return result->asUnchecked<FlatVector<R>>()->mutableRawValues();
  }

  const uint8_t aPrecision_;
  const uint8_t aRescale_;
  const ExtraParams param_;
};

template <typename A /* Argument */>
class DecimalBetweenFunction : public exec::VectorFunction {
 public:
  DecimalBetweenFunction() {}
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& resultType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    prepareResults(rows, resultType, context, result);

    // Fast path when the first argument is a flat vector.
    if (args[0]->isFlatEncoding()) {
      auto rawA = args[0]->asUnchecked<FlatVector<A>>()->mutableRawValues();

      if (args[1]->isConstantEncoding() && args[2]->isConstantEncoding()) {
        auto constantB = args[1]->asUnchecked<SimpleVector<A>>()->valueAt(0);
        auto constantC = args[2]->asUnchecked<SimpleVector<A>>()->valueAt(0);
        context.applyToSelectedNoThrow(rows, [&](auto row) {
          result->asUnchecked<FlatVector<bool>>()->set(
              row, rawA[row] >= constantB && rawA[row] <= constantC);
        });
        return;
      }

      if (args[1]->isConstantEncoding() && args[2]->isFlatEncoding()) {
        auto constantB = args[1]->asUnchecked<SimpleVector<A>>()->valueAt(0);
        auto rawC = args[2]->asUnchecked<FlatVector<A>>()->mutableRawValues();
        context.applyToSelectedNoThrow(rows, [&](auto row) {
          result->asUnchecked<FlatVector<bool>>()->set(
              row, rawA[row] >= constantB && rawA[row] <= rawC[row]);
        });
        return;
      }

      if (args[1]->isFlatEncoding() && args[2]->isConstantEncoding()) {
        auto rawB = args[1]->asUnchecked<FlatVector<A>>()->mutableRawValues();
        auto constantC = args[2]->asUnchecked<SimpleVector<A>>()->valueAt(0);
        context.applyToSelectedNoThrow(rows, [&](auto row) {
          result->asUnchecked<FlatVector<bool>>()->set(
              row, rawA[row] >= rawB[row] && rawA[row] <= constantC);
        });
        return;
      }

      if (args[1]->isFlatEncoding() && args[2]->isFlatEncoding()) {
        auto rawB = args[1]->asUnchecked<FlatVector<A>>()->mutableRawValues();
        auto rawC = args[2]->asUnchecked<FlatVector<A>>()->mutableRawValues();
        context.applyToSelectedNoThrow(rows, [&](auto row) {
          result->asUnchecked<FlatVector<bool>>()->set(
              row, rawA[row] >= rawB[row] && rawA[row] <= rawC[row]);
        });
        return;
      }
    } else {
      // Fast path when the first argument is encoded but the second and third
      // are constants.
      exec::DecodedArgs decodedArgs(rows, args, context);
      auto aDecoded = decodedArgs.at(0);
      auto aDecodedData = aDecoded->data<A>();

      if (args[1]->isConstantEncoding() && args[2]->isConstantEncoding()) {
        auto constantB = args[1]->asUnchecked<SimpleVector<A>>()->valueAt(0);
        auto constantC = args[2]->asUnchecked<SimpleVector<A>>()->valueAt(0);
        context.applyToSelectedNoThrow(rows, [&](auto row) {
          auto value = aDecodedData[aDecoded->index(row)];
          result->asUnchecked<FlatVector<bool>>()->set(
              row, value >= constantB && value <= constantC);
        });
        return;
      }
    }

    // Decode the input in all other cases.
    exec::DecodedArgs decodedArgs(rows, args, context);
    auto aDecoded = decodedArgs.at(0);
    auto bDecoded = decodedArgs.at(1);
    auto cDecoded = decodedArgs.at(2);

    auto aDecodedData = aDecoded->data<A>();
    auto bDecodedData = bDecoded->data<A>();
    auto cDecodedData = cDecoded->data<A>();

    context.applyToSelectedNoThrow(rows, [&](auto row) {
      auto aValue = aDecodedData[aDecoded->index(row)];
      auto bValue = bDecodedData[bDecoded->index(row)];
      auto cValue = cDecodedData[cDecoded->index(row)];
      result->asUnchecked<FlatVector<bool>>()->set(
          row, aValue >= bValue && aValue <= cValue);
    });
  }

 private:
  void prepareResults(
      const SelectivityVector& rows,
      const TypePtr& resultType,
      exec::EvalCtx& context,
      VectorPtr& result) const {
    context.ensureWritable(rows, resultType, result);
    result->clearNulls(rows);
  }
};

class Addition {
 public:
  template <typename R, typename A, typename B>
  inline static void
  apply(R& r, const A& a, const B& b, uint8_t aRescale, uint8_t bRescale)
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
      __attribute__((__no_sanitize__("signed-integer-overflow")))
#endif
#endif
  {
    int128_t aRescaled;
    int128_t bRescaled;
    if (__builtin_mul_overflow(
            a, DecimalUtil::kPowersOfTen[aRescale], &aRescaled) ||
        __builtin_mul_overflow(
            b, DecimalUtil::kPowersOfTen[bRescale], &bRescaled)) {
      VELOX_ARITHMETIC_ERROR("Decimal overflow: {} + {}", a, b);
    }
    r = checkedPlus<R>(R(aRescaled), R(bRescaled));
    DecimalUtil::valueInRange(r);
  }

  inline static uint8_t
  computeRescaleFactor(uint8_t fromScale, uint8_t toScale, uint8_t rScale = 0) {
    return std::max(0, toScale - fromScale);
  }

  inline static std::pair<uint8_t, uint8_t> computeResultPrecisionScale(
      const uint8_t aPrecision,
      const uint8_t aScale,
      const uint8_t bPrecision,
      const uint8_t bScale) {
    return {
        std::min(
            38,
            std::max(aPrecision - aScale, bPrecision - bScale) +
                std::max(aScale, bScale) + 1),
        std::max(aScale, bScale)};
  }
};

class Subtraction {
 public:
  template <typename R, typename A, typename B>
  inline static void
  apply(R& r, const A& a, const B& b, uint8_t aRescale, uint8_t bRescale)
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
      __attribute__((__no_sanitize__("signed-integer-overflow")))
#endif
#endif
  {
    int128_t aRescaled;
    int128_t bRescaled;
    if (__builtin_mul_overflow(
            a, DecimalUtil::kPowersOfTen[aRescale], &aRescaled) ||
        __builtin_mul_overflow(
            b, DecimalUtil::kPowersOfTen[bRescale], &bRescaled)) {
      VELOX_ARITHMETIC_ERROR("Decimal overflow: {} - {}", a, b);
    }
    r = checkedMinus<R>(R(aRescaled), R(bRescaled));
    DecimalUtil::valueInRange(r);
  }

  inline static uint8_t
  computeRescaleFactor(uint8_t fromScale, uint8_t toScale, uint8_t rScale = 0) {
    return std::max(0, toScale - fromScale);
  }

  inline static std::pair<uint8_t, uint8_t> computeResultPrecisionScale(
      const uint8_t aPrecision,
      const uint8_t aScale,
      const uint8_t bPrecision,
      const uint8_t bScale) {
    return Addition::computeResultPrecisionScale(
        aPrecision, aScale, bPrecision, bScale);
  }
};

class Multiply {
 public:
  template <typename R, typename A, typename B>
  inline static void
  apply(R& r, const A& a, const B& b, uint8_t aRescale, uint8_t bRescale) {
    r = checkedMultiply<R>(
        checkedMultiply<R>(R(a), R(b)),
        R(DecimalUtil::kPowersOfTen[aRescale + bRescale]));
    DecimalUtil::valueInRange(r);
  }

  inline static uint8_t
  computeRescaleFactor(uint8_t fromScale, uint8_t toScale, uint8_t rScale = 0) {
    return 0;
  }

  inline static std::pair<uint8_t, uint8_t> computeResultPrecisionScale(
      const uint8_t aPrecision,
      const uint8_t aScale,
      const uint8_t bPrecision,
      const uint8_t bScale) {
    return {std::min(38, aPrecision + bPrecision), aScale + bScale};
  }
};

class Divide {
 public:
  template <typename R, typename A, typename B>
  inline static void
  apply(R& r, const A& a, const B& b, uint8_t aRescale, uint8_t /*bRescale*/) {
    DecimalUtil::divideWithRoundUp<R, A, B>(r, a, b, false, aRescale, 0);
    DecimalUtil::valueInRange(r);
  }

  inline static uint8_t
  computeRescaleFactor(uint8_t fromScale, uint8_t toScale, uint8_t rScale) {
    return rScale - fromScale + toScale;
  }

  inline static std::pair<uint8_t, uint8_t> computeResultPrecisionScale(
      const uint8_t aPrecision,
      const uint8_t aScale,
      const uint8_t /*bPrecision*/,
      const uint8_t bScale) {
    return {
        std::min(38, aPrecision + bScale + std::max(0, bScale - aScale)),
        std::max(aScale, bScale)};
  }
};

class Round {
 public:
  template <typename R, typename A>
  inline static void apply(
      R& r,
      const A& a,
      uint8_t aPrecision,
      uint8_t aRescale,
      const ExtraParams& extraParams) {
    auto decimalParam = extraParams.params.round.decimal;
    if (!decimalParam.has_value()) {
      // Basic round function.
      DecimalUtil::divideWithRoundUp<R, A, int128_t>(
          r, a, DecimalUtil::kPowersOfTen[aRescale], false, 0, 0);
      return;
    }
    // RoundN function.
    if (a == 0 || aPrecision - aRescale + decimalParam.value() <= 0) {
      r = 0;
      return;
    }
    if (decimalParam >= aRescale) {
      r = a;
      return;
    }
    auto reScaleFactor =
        DecimalUtil::kPowersOfTen[aRescale - decimalParam.value()];
    DecimalUtil::divideWithRoundUp<R, A, int128_t>(
        r, a, reScaleFactor, false, 0, 0);
    r = r * reScaleFactor;
  }

  inline static uint8_t computeRescaleFactor(
      uint8_t fromScale,
      uint8_t /*toScale*/,
      uint8_t /*rScale*/) {
    return fromScale;
  }

  inline static std::pair<uint8_t, uint8_t> computeResultPrecisionScale(
      const uint8_t aPrecision,
      const uint8_t aScale,
      const ExtraParams& extraParams) {
    if (!extraParams.params.round.decimal.has_value()) {
      // Return result precision and scale for basic round function.
      return {
          std::min(38, aPrecision - aScale + std::min((uint8_t)1, aScale)), 0};
    }
    return {std::min(38, aPrecision + 1), aScale};
  }
};

class Floor {
 public:
  template <typename R, typename A>
  inline static void apply(
      R& r,
      const A& a,
      uint8_t /*aPrecision*/,
      uint8_t aRescale,
      const ExtraParams& /*param*/) {
    auto rescaleFactor = DecimalUtil::kPowersOfTen[aRescale];
    // Function interpretation is the same as ceil for negative numbers, and as
    // floor for positive numbers.
    auto increment = (a % rescaleFactor) < 0 ? -1 : 0;
    r = a / rescaleFactor + increment;
  }

  inline static uint8_t computeRescaleFactor(
      uint8_t fromScale,
      uint8_t /*toScale*/,
      uint8_t /*rScale*/) {
    return fromScale;
  }

  inline static std::pair<uint8_t, uint8_t> computeResultPrecisionScale(
      const uint8_t aPrecision,
      const uint8_t aScale,
      const ExtraParams& /*extraParams*/) {
    // The result precision is calculated as "p - s + min(s, 1)" in Presto.
    return {
        std::min(38, aPrecision - aScale + std::min((uint8_t)1, aScale)), 0};
  }
};

class Abs {
 public:
  template <typename R, typename A>
  inline static void apply(
      R& r,
      const A& a,
      uint8_t aPrecision,
      uint8_t /*aRescale*/,
      const ExtraParams& /*param*/) {
    if constexpr (std::is_same_v<R, A>) {
      r = a < 0 ? R(-a) : a;
    }
  }

  inline static uint8_t computeRescaleFactor(
      uint8_t fromScale,
      uint8_t /*toScale*/,
      uint8_t /*rScale*/) {
    return fromScale;
  }

  inline static std::pair<uint8_t, uint8_t> computeResultPrecisionScale(
      const uint8_t aPrecision,
      const uint8_t aScale,
      const ExtraParams& param) {
    return {aPrecision, aScale};
  }
};

class Negate {
 public:
  template <typename R, typename A>
  inline static void apply(
      R& r,
      const A& a,
      uint8_t aPrecision,
      uint8_t /*aRescale*/,
      const ExtraParams& /*param*/) {
    if constexpr (std::is_same_v<R, A>) {
      r = R(-a);
    }
  }

  inline static uint8_t computeRescaleFactor(
      uint8_t fromScale,
      uint8_t /*toScale*/,
      uint8_t /*rScale*/) {
    return fromScale;
  }

  inline static std::pair<uint8_t, uint8_t> computeResultPrecisionScale(
      const uint8_t aPrecision,
      const uint8_t aScale,
      const ExtraParams& /*additionalParm*/) {
    return {aPrecision, aScale};
  }
};

std::vector<std::shared_ptr<exec::FunctionSignature>>
decimalMultiplySignature() {
  return {
      exec::FunctionSignatureBuilder()
          .integerVariable("a_precision")
          .integerVariable("a_scale")
          .integerVariable("b_precision")
          .integerVariable("b_scale")
          .integerVariable("r_precision", "min(38, a_precision + b_precision)")
          .integerVariable("r_scale", "a_scale + b_scale")
          .returnType("DECIMAL(r_precision, r_scale)")
          .argumentType("DECIMAL(a_precision, a_scale)")
          .argumentType("DECIMAL(b_precision, b_scale)")
          .build()};
}

std::vector<std::shared_ptr<exec::FunctionSignature>>
decimalAddSubtractSignature() {
  return {
      exec::FunctionSignatureBuilder()
          .integerVariable("a_precision")
          .integerVariable("a_scale")
          .integerVariable("b_precision")
          .integerVariable("b_scale")
          .integerVariable(
              "r_precision",
              "min(38, max(a_precision - a_scale, b_precision - b_scale) + max(a_scale, b_scale) + 1)")
          .integerVariable("r_scale", "max(a_scale, b_scale)")
          .returnType("DECIMAL(r_precision, r_scale)")
          .argumentType("DECIMAL(a_precision, a_scale)")
          .argumentType("DECIMAL(b_precision, b_scale)")
          .build()};
}

std::vector<std::shared_ptr<exec::FunctionSignature>> decimalDivideSignature() {
  return {exec::FunctionSignatureBuilder()
              .integerVariable("a_precision")
              .integerVariable("a_scale")
              .integerVariable("b_precision")
              .integerVariable("b_scale")
              .integerVariable(
                  "r_precision",
                  "min(38, a_precision + b_scale + max(0, b_scale - a_scale))")
              .integerVariable("r_scale", "max(a_scale, b_scale)")
              .returnType("DECIMAL(r_precision, r_scale)")
              .argumentType("DECIMAL(a_precision, a_scale)")
              .argumentType("DECIMAL(b_precision, b_scale)")
              .build()};
}

std::vector<std::shared_ptr<exec::FunctionSignature>> decimalRoundSignature() {
  return {
      exec::FunctionSignatureBuilder()
          .integerVariable("a_precision")
          .integerVariable("a_scale")
          .integerVariable(
              "r_precision", "min(38, a_precision - a_scale + min(1, a_scale))")
          .returnType("DECIMAL(r_precision, 0)")
          .argumentType("DECIMAL(a_precision, a_scale)")
          .build(),
      exec::FunctionSignatureBuilder()
          .integerVariable("a_precision")
          .integerVariable("a_scale")
          .integerVariable("r_precision", "min(38, a_precision + 1)")
          .returnType("DECIMAL(r_precision, a_scale)")
          .argumentType("DECIMAL(a_precision, a_scale)")
          .argumentType("integer")
          .build()};
}

std::vector<std::shared_ptr<exec::FunctionSignature>> decimalFloorSignature() {
  return {
      exec::FunctionSignatureBuilder()
          .integerVariable("a_precision")
          .integerVariable("a_scale")
          .integerVariable(
              "r_precision", "min(38, a_precision - a_scale + min(a_scale, 1))")
          .returnType("DECIMAL(r_precision, 0)")
          .argumentType("DECIMAL(a_precision, a_scale)")
          .build()};
}

std::vector<std::shared_ptr<exec::FunctionSignature>>
decimalAbsNegateSignature() {
  return {exec::FunctionSignatureBuilder()
              .integerVariable("a_precision")
              .integerVariable("a_scale")
              .returnType("DECIMAL(a_precision, a_scale)")
              .argumentType("DECIMAL(a_precision, a_scale)")
              .build()};
}

std::vector<std::shared_ptr<exec::FunctionSignature>>
decimalBetweenSignature() {
  return {exec::FunctionSignatureBuilder()
              .integerVariable("a_precision")
              .integerVariable("a_scale")
              .returnType("boolean")
              .argumentType("DECIMAL(a_precision, a_scale)")
              .argumentType("DECIMAL(a_precision, a_scale)")
              .argumentType("DECIMAL(a_precision, a_scale)")
              .build()};
}

template <typename Operation>
std::shared_ptr<exec::VectorFunction> createDecimalUnary(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  auto aType = inputArgs[0].type;
  auto [aPrecision, aScale] = getDecimalPrecisionScale(*aType);
  ExtraParams extraParams{};
  if (inputArgs.size() == 2) {
    // Round can accept additional argument like number of decimal places.
    VELOX_CHECK_EQ(inputArgs[1].type->kind(), TypeKind::INTEGER);
    extraParams.params.round.decimal =
        inputArgs[1]
            .constantValue->asUnchecked<SimpleVector<int32_t>>()
            ->valueAt(0);
  }
  auto [rPrecision, rScale] =
      Operation::computeResultPrecisionScale(aPrecision, aScale, extraParams);
  uint8_t aRescale = Operation::computeRescaleFactor(aScale, 0, rScale);
  if (aType->isShortDecimal()) {
    return std::make_shared<
        DecimalUnaryFunction<int64_t /*result*/, int64_t, Operation>>(
        aPrecision, aRescale, extraParams);
  } else if (aType->isLongDecimal()) {
    if (rPrecision <= ShortDecimalType::kMaxPrecision) {
      return std::make_shared<
          DecimalUnaryFunction<int64_t /*result*/, int128_t, Operation>>(
          aPrecision, aRescale, extraParams);
    }
    return std::make_shared<
        DecimalUnaryFunction<int128_t /*result*/, int128_t, Operation>>(
        aPrecision, aRescale, extraParams);
  }
  VELOX_UNSUPPORTED();
}

template <typename Operation>
std::shared_ptr<exec::VectorFunction> createDecimalFunction(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  auto aType = inputArgs[0].type;
  auto bType = inputArgs[1].type;
  auto [aPrecision, aScale] = getDecimalPrecisionScale(*aType);
  auto [bPrecision, bScale] = getDecimalPrecisionScale(*bType);
  auto [rPrecision, rScale] = Operation::computeResultPrecisionScale(
      aPrecision, aScale, bPrecision, bScale);
  uint8_t aRescale = Operation::computeRescaleFactor(aScale, bScale, rScale);
  uint8_t bRescale = Operation::computeRescaleFactor(bScale, aScale, rScale);
  if (aType->isShortDecimal()) {
    if (bType->isShortDecimal()) {
      if (rPrecision > ShortDecimalType::kMaxPrecision) {
        // Arguments are short decimals and result is a long decimal.
        return std::make_shared<DecimalBaseFunction<
            int128_t /*result*/,
            int64_t,
            int64_t,
            Operation>>(aRescale, bRescale);
      } else {
        // Arguments are short decimals and result is a short decimal.
        return std::make_shared<DecimalBaseFunction<
            int64_t /*result*/,
            int64_t,
            int64_t,
            Operation>>(aRescale, bRescale);
      }
    } else {
      if (rPrecision > ShortDecimalType::kMaxPrecision) {
        // LHS is short decimal and rhs is a long decimal, result is long
        // decimal.
        return std::make_shared<DecimalBaseFunction<
            int128_t /*result*/,
            int64_t,
            int128_t,
            Operation>>(aRescale, bRescale);
      } else {
        // In some cases such as division, the result type can still be a short
        // decimal even though RHS is a long decimal.
        return std::make_shared<DecimalBaseFunction<
            int64_t /*result*/,
            int64_t,
            int128_t,
            Operation>>(aRescale, bRescale);
      }
    }
  } else {
    if (bType->isShortDecimal()) {
      // LHS is long decimal and rhs is short decimal, result is a long decimal.
      return std::make_shared<DecimalBaseFunction<
          int128_t /*result*/,
          int128_t,
          int64_t,
          Operation>>(aRescale, bRescale);
    } else {
      // Arguments and result are all long decimals.
      return std::make_shared<DecimalBaseFunction<
          int128_t /*result*/,
          int128_t,
          int128_t,
          Operation>>(aRescale, bRescale);
    }
  }
  VELOX_UNSUPPORTED();
}

std::shared_ptr<exec::VectorFunction> createDecimalBetweenFunction(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  auto aType = inputArgs[0].type;
  auto bType = inputArgs[1].type;
  auto cType = inputArgs[2].type;
  if (aType->isShortDecimal()) {
    VELOX_CHECK(bType->isShortDecimal());
    VELOX_CHECK(cType->isShortDecimal());
    // Arguments are short decimals.
    return std::make_shared<DecimalBetweenFunction<int64_t>>();
  } else {
    VELOX_CHECK(bType->isLongDecimal());
    VELOX_CHECK(cType->isLongDecimal());
    // Arguments are long decimals.
    return std::make_shared<DecimalBetweenFunction<int128_t>>();
  }
  VELOX_UNSUPPORTED();
}
} // namespace

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_add,
    decimalAddSubtractSignature(),
    createDecimalFunction<Addition>);

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_sub,
    decimalAddSubtractSignature(),
    createDecimalFunction<Subtraction>);

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_mul,
    decimalMultiplySignature(),
    createDecimalFunction<Multiply>);

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_div,
    decimalDivideSignature(),
    createDecimalFunction<Divide>);

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_round,
    decimalRoundSignature(),
    createDecimalUnary<Round>);

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_floor,
    decimalFloorSignature(),
    createDecimalUnary<Floor>);

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_abs,
    decimalAbsNegateSignature(),
    createDecimalUnary<Abs>);

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_negate,
    decimalAbsNegateSignature(),
    createDecimalUnary<Negate>);

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_between,
    decimalBetweenSignature(),
    createDecimalBetweenFunction);
} // namespace facebook::velox::functions
