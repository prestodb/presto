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
class DecimalUnaryBaseFunction : public exec::VectorFunction {
 public:
  explicit DecimalUnaryBaseFunction(uint8_t aRescale) : aRescale_(aRescale) {}

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
        Operation::template apply<R, A>(rawResults[row], constant, aRescale_);
      });
    } else {
      // Fast path for flat.
      auto flatA = args[0]->asUnchecked<FlatVector<A>>();
      auto rawA = flatA->mutableRawValues();
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        Operation::template apply<R, A>(rawResults[row], rawA[row], aRescale_);
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

  const uint8_t aRescale_;
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
    // Second and third arguments must always be constant.
    VELOX_CHECK(args[1]->isConstantEncoding() && args[2]->isConstantEncoding());
    auto constantB = args[1]->asUnchecked<SimpleVector<A>>()->valueAt(0);
    auto constantC = args[2]->asUnchecked<SimpleVector<A>>()->valueAt(0);
    if (args[0]->isFlatEncoding()) {
      // Fast path if first argument is flat.
      auto flatA = args[0]->asUnchecked<FlatVector<A>>();
      auto rawA = flatA->mutableRawValues();
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        result->asUnchecked<FlatVector<bool>>()->set(
            row,
            rawA[row].unscaledValue() >= constantB.unscaledValue() &&
                rawA[row].unscaledValue() <= constantC.unscaledValue());
      });
    } else {
      // Path if first argument is encoded.
      exec::DecodedArgs decodedArgs(rows, args, context);
      auto a = decodedArgs.at(0);
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        auto value = a->valueAt<A>(row);
        result->asUnchecked<FlatVector<bool>>()->set(
            row,
            value.unscaledValue() >= constantB.unscaledValue() &&
                value.unscaledValue() <= constantC.unscaledValue());
      });
    }
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
            a.unscaledValue(),
            DecimalUtil::kPowersOfTen[aRescale],
            &aRescaled) ||
        __builtin_mul_overflow(
            b.unscaledValue(),
            DecimalUtil::kPowersOfTen[bRescale],
            &bRescaled)) {
      VELOX_ARITHMETIC_ERROR(
          "Decimal overflow: {} + {}", a.unscaledValue(), b.unscaledValue());
    }
    r = checkedPlus<R>(R(aRescaled), R(bRescaled));
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
            a.unscaledValue(),
            DecimalUtil::kPowersOfTen[aRescale],
            &aRescaled) ||
        __builtin_mul_overflow(
            b.unscaledValue(),
            DecimalUtil::kPowersOfTen[bRescale],
            &bRescaled)) {
      VELOX_ARITHMETIC_ERROR(
          "Decimal overflow: {} - {}", a.unscaledValue(), b.unscaledValue());
    }
    r = checkedMinus<R>(R(aRescaled), R(bRescaled));
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
  inline static void apply(R& r, const A& a, uint8_t aRescale) {
    // aRescale holds the scale of the input.
    auto temp = a;
    DecimalUtil::divideWithRoundUp<A, A, int128_t>(
        temp, a, DecimalUtil::kPowersOfTen[aRescale], false, 0, 0);
    r = R(temp.unscaledValue());
  }

  inline static uint8_t computeRescaleFactor(
      uint8_t fromScale,
      uint8_t /*toScale*/,
      uint8_t /*rScale*/) {
    return fromScale;
  }

  inline static std::pair<uint8_t, uint8_t> computeResultPrecisionScale(
      const uint8_t aPrecision,
      const uint8_t aScale) {
    return {
        std::min(38, aPrecision - aScale + std::min((uint8_t)1, aScale)), 0};
  }
};

class Abs {
 public:
  template <typename R, typename A>
  inline static void apply(R& r, const A& a, uint8_t /*aRescale*/) {
    if constexpr (std::is_same_v<R, A>) {
      r = a.unscaledValue() < 0 ? R(-a.unscaledValue()) : a;
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
      const uint8_t aScale) {
    return {aPrecision, aScale};
  }
};

class Negate {
 public:
  template <typename R, typename A>
  inline static void apply(R& r, const A& a, uint8_t /*aRescale*/) {
    if constexpr (std::is_same_v<R, A>) {
      r = R(-a.unscaledValue());
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
      const uint8_t aScale) {
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
          .integerVariable("r_scale", "0")
          .returnType("DECIMAL(r_precision, r_scale)")
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
    const std::string& /*name*/,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  auto aType = inputArgs[0].type;
  auto [aPrecision, aScale] = getDecimalPrecisionScale(*aType);
  auto [rPrecision, rScale] =
      Operation::computeResultPrecisionScale(aPrecision, aScale);
  uint8_t aRescale = Operation::computeRescaleFactor(aScale, 0, rScale);
  if (aType->kind() == TypeKind::SHORT_DECIMAL) {
    return std::make_shared<DecimalUnaryBaseFunction<
        UnscaledShortDecimal /*result*/,
        UnscaledShortDecimal,
        Operation>>(aRescale);
  } else if (aType->kind() == TypeKind::LONG_DECIMAL) {
    if (rPrecision <= DecimalType<TypeKind::SHORT_DECIMAL>::kMaxPrecision) {
      return std::make_shared<DecimalUnaryBaseFunction<
          UnscaledShortDecimal /*result*/,
          UnscaledLongDecimal,
          Operation>>(aRescale);
    }
    return std::make_shared<DecimalUnaryBaseFunction<
        UnscaledLongDecimal /*result*/,
        UnscaledLongDecimal,
        Operation>>(aRescale);
  }
  VELOX_UNSUPPORTED();
}

template <typename Operation>
std::shared_ptr<exec::VectorFunction> createDecimalFunction(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  auto aType = inputArgs[0].type;
  auto bType = inputArgs[1].type;
  auto [aPrecision, aScale] = getDecimalPrecisionScale(*aType);
  auto [bPrecision, bScale] = getDecimalPrecisionScale(*bType);
  auto [rPrecision, rScale] = Operation::computeResultPrecisionScale(
      aPrecision, aScale, bPrecision, bScale);
  uint8_t aRescale = Operation::computeRescaleFactor(aScale, bScale, rScale);
  uint8_t bRescale = Operation::computeRescaleFactor(bScale, aScale, rScale);
  if (aType->kind() == TypeKind::SHORT_DECIMAL) {
    if (bType->kind() == TypeKind::SHORT_DECIMAL) {
      if (rPrecision > DecimalType<TypeKind::SHORT_DECIMAL>::kMaxPrecision) {
        // Arguments are short decimals and result is a long decimal.
        return std::make_shared<DecimalBaseFunction<
            UnscaledLongDecimal /*result*/,
            UnscaledShortDecimal,
            UnscaledShortDecimal,
            Operation>>(aRescale, bRescale);
      } else {
        // Arguments are short decimals and result is a short decimal.
        return std::make_shared<DecimalBaseFunction<
            UnscaledShortDecimal /*result*/,
            UnscaledShortDecimal,
            UnscaledShortDecimal,
            Operation>>(aRescale, bRescale);
      }
    } else {
      // LHS is short decimal and rhs is a long decimal, result is long decimal.
      return std::make_shared<DecimalBaseFunction<
          UnscaledLongDecimal /*result*/,
          UnscaledShortDecimal,
          UnscaledLongDecimal,
          Operation>>(aRescale, bRescale);
    }
  } else {
    if (bType->kind() == TypeKind::SHORT_DECIMAL) {
      // LHS is long decimal and rhs is short decimal, result is a long decimal.
      return std::make_shared<DecimalBaseFunction<
          UnscaledLongDecimal /*result*/,
          UnscaledLongDecimal,
          UnscaledShortDecimal,
          Operation>>(aRescale, bRescale);
    } else {
      // Arguments and result are all long decimals.
      return std::make_shared<DecimalBaseFunction<
          UnscaledLongDecimal /*result*/,
          UnscaledLongDecimal,
          UnscaledLongDecimal,
          Operation>>(aRescale, bRescale);
    }
  }
  VELOX_UNSUPPORTED();
}

std::shared_ptr<exec::VectorFunction> createDecimalBetweenFunction(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  auto aType = inputArgs[0].type;
  auto bType = inputArgs[1].type;
  auto cType = inputArgs[2].type;
  if (aType->kind() == TypeKind::SHORT_DECIMAL) {
    VELOX_CHECK(bType->kind() == TypeKind::SHORT_DECIMAL);
    VELOX_CHECK(cType->kind() == TypeKind::SHORT_DECIMAL);
    // Arguments are short decimals.
    return std::make_shared<DecimalBetweenFunction<UnscaledShortDecimal>>();
  } else {
    VELOX_CHECK(bType->kind() == TypeKind::LONG_DECIMAL);
    VELOX_CHECK(cType->kind() == TypeKind::LONG_DECIMAL);
    // Arguments are long decimals.
    return std::make_shared<DecimalBetweenFunction<UnscaledLongDecimal>>();
  }
  VELOX_UNSUPPORTED();
}
}; // namespace

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
}; // namespace facebook::velox::functions
