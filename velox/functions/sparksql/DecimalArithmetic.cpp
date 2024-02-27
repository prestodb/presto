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

#include "velox/common/base/CheckedArithmetic.h"
#include "velox/expression/DecodedArgs.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/sparksql/DecimalUtil.h"
#include "velox/type/DecimalUtil.h"

namespace facebook::velox::functions::sparksql {
namespace {

std::string getResultScale(std::string precision, std::string scale) {
  return fmt::format(
      "({}) <= 38 ? ({}) : max(({}) - ({}) + 38, min(({}), 6))",
      precision,
      scale,
      scale,
      precision,
      scale);
}

// Returns the whole and fraction parts of a decimal value.
template <typename T>
inline std::pair<T, T> getWholeAndFraction(T value, uint8_t scale) {
  const auto scaleFactor = velox::DecimalUtil::kPowersOfTen[scale];
  const T whole = value / scaleFactor;
  return {whole, value - whole * scaleFactor};
}

// Increases the scale of input value by 'delta'. Returns the input value if
// delta is not positive.
inline int128_t increaseScale(int128_t in, int16_t delta) {
  // No need to consider overflow as 'delta == higher scale - input scale', so
  // the scaled value will not exceed the maximum of long decimal.
  return delta <= 0 ? in : in * velox::DecimalUtil::kPowersOfTen[delta];
}

// Scales up the whole part to result scale, and combine it with fraction part
// to produce a full result for decimal add. Checks whether the result
// overflows.
template <typename T>
inline T
decimalAddResult(T whole, T fraction, uint8_t resultScale, bool& overflow) {
  T scaledWhole = DecimalUtil::multiply<T>(
      whole, velox::DecimalUtil::kPowersOfTen[resultScale], overflow);
  if (FOLLY_UNLIKELY(overflow)) {
    return 0;
  }
  const auto result = scaledWhole + fraction;
  if constexpr (std::is_same_v<T, int64_t>) {
    overflow = (result > velox::DecimalUtil::kShortDecimalMax) ||
        (result < velox::DecimalUtil::kShortDecimalMin);
  } else {
    overflow = (result > velox::DecimalUtil::kLongDecimalMax) ||
        (result < velox::DecimalUtil::kLongDecimalMin);
  }
  return result;
}

// Reduces the scale of input value by 'delta'. Returns the input value if delta
// is not positive.
template <typename T>
inline static T reduceScale(T in, int32_t delta) {
  if (delta <= 0) {
    return in;
  }
  T result;
  bool overflow;
  const auto scaleFactor = velox::DecimalUtil::kPowersOfTen[delta];
  if constexpr (std::is_same_v<T, int64_t>) {
    VELOX_DCHECK_LE(
        scaleFactor,
        std::numeric_limits<int64_t>::max(),
        "Scale factor should not exceed the maximum of int64_t.");
  }
  DecimalUtil::divideWithRoundUp<T, T, T>(
      result, in, T(scaleFactor), 0, overflow);
  VELOX_DCHECK(!overflow);
  return result;
}

// Adds two non-negative values by adding the whole and fraction parts
// separately.
template <typename TResult, typename A, typename B>
inline static TResult addLargeNonNegative(
    A a,
    B b,
    uint8_t aScale,
    uint8_t bScale,
    uint8_t rScale,
    bool& overflow) {
  VELOX_DCHECK_GE(
      a, 0, "Non-negative value is expected in addLargeNonNegative.");
  VELOX_DCHECK_GE(
      b, 0, "Non-negative value is expected in addLargeNonNegative.");

  // Separate whole and fraction parts.
  const auto [aWhole, aFraction] = getWholeAndFraction<A>(a, aScale);
  const auto [bWhole, bFraction] = getWholeAndFraction<B>(b, bScale);

  // Adjust fractional parts to higher scale.
  const auto higherScale = std::max(aScale, bScale);
  const auto aFractionScaled =
      increaseScale((int128_t)aFraction, higherScale - aScale);
  const auto bFractionScaled =
      increaseScale((int128_t)bFraction, higherScale - bScale);

  int128_t fraction;
  bool carryToLeft = false;
  const auto carrier = velox::DecimalUtil::kPowersOfTen[higherScale];
  if (aFractionScaled >= carrier - bFractionScaled) {
    fraction = aFractionScaled + bFractionScaled - carrier;
    carryToLeft = true;
  } else {
    fraction = aFractionScaled + bFractionScaled;
  }

  // Scale up the whole part and scale down the fraction part to combine them.
  fraction = reduceScale(TResult(fraction), higherScale - rScale);
  const auto whole = TResult(aWhole) + TResult(bWhole) + TResult(carryToLeft);
  return decimalAddResult(whole, TResult(fraction), rScale, overflow);
}

// Adds two opposite values by adding the whole and fraction parts separately.
template <typename TResult, typename A, typename B>
inline static TResult addLargeOpposite(
    A a,
    B b,
    uint8_t aScale,
    uint8_t bScale,
    int32_t rScale,
    bool& overflow) {
  VELOX_DCHECK(
      (a < 0 && b > 0) || (a > 0 && b < 0),
      "One positve and one negative value are expected in addLargeOpposite.");

  // Separate whole and fraction parts.
  const auto [aWhole, aFraction] = getWholeAndFraction<A>(a, aScale);
  const auto [bWhole, bFraction] = getWholeAndFraction<B>(b, bScale);

  // Adjust fractional parts to higher scale.
  const auto higherScale = std::max(aScale, bScale);
  const auto aFractionScaled =
      increaseScale((int128_t)aFraction, higherScale - aScale);
  const auto bFractionScaled =
      increaseScale((int128_t)bFraction, higherScale - bScale);

  // No need to consider overflow because two inputs are opposite.
  int128_t whole = (int128_t)aWhole + (int128_t)bWhole;
  int128_t fraction = aFractionScaled + bFractionScaled;

  // If the whole and fractional parts have different signs, adjust them to the
  // same sign.
  const auto scaleFactor = velox::DecimalUtil::kPowersOfTen[higherScale];
  if (whole < 0 && fraction > 0) {
    whole += 1;
    fraction -= scaleFactor;
  } else if (whole > 0 && fraction < 0) {
    whole -= 1;
    fraction += scaleFactor;
  }

  // Scale up the whole part and scale down the fraction part to combine them.
  fraction = reduceScale(TResult(fraction), higherScale - rScale);
  return decimalAddResult(TResult(whole), TResult(fraction), rScale, overflow);
}

template <typename TResult, typename A, typename B>
inline static TResult addLarge(
    A a,
    B b,
    uint8_t aScale,
    uint8_t bScale,
    int32_t rScale,
    bool& overflow) {
  if (a >= 0 && b >= 0) {
    // Both non-negative.
    return addLargeNonNegative<TResult, A, B>(
        a, b, aScale, bScale, rScale, overflow);
  } else if (a <= 0 && b <= 0) {
    // Both non-positive.
    return TResult(-addLargeNonNegative<TResult, A, B>(
        A(-a), B(-b), aScale, bScale, rScale, overflow));
  } else {
    // One positive and the other negative.
    return addLargeOpposite<TResult, A, B>(
        a, b, aScale, bScale, rScale, overflow);
  }
}

template <
    typename R /* Result Type */,
    typename A /* Argument1 */,
    typename B /* Argument2 */,
    typename Operation /* Arithmetic operation */>
class DecimalBaseFunction : public exec::VectorFunction {
 public:
  DecimalBaseFunction(
      uint8_t aRescale,
      uint8_t bRescale,
      uint8_t aPrecision,
      uint8_t aScale,
      uint8_t bPrecision,
      uint8_t bScale,
      uint8_t rPrecision,
      uint8_t rScale)
      : aRescale_(aRescale),
        bRescale_(bRescale),
        aPrecision_(aPrecision),
        aScale_(aScale),
        bPrecision_(bPrecision),
        bScale_(bScale),
        rPrecision_(rPrecision),
        rScale_(rScale) {}

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
        bool overflow = false;
        Operation::template apply<R, A, B>(
            rawResults[row],
            constant,
            rawValues[row],
            aRescale_,
            bRescale_,
            aPrecision_,
            aScale_,
            bPrecision_,
            bScale_,
            rPrecision_,
            rScale_,
            overflow);
        if (overflow ||
            !velox::DecimalUtil::valueInPrecisionRange(
                rawResults[row], rPrecision_)) {
          result->setNull(row, true);
        }
      });
    } else if (args[0]->isFlatEncoding() && args[1]->isConstantEncoding()) {
      // Fast path for (flat, const).
      auto flatValues = args[0]->asUnchecked<FlatVector<A>>();
      auto constant = args[1]->asUnchecked<SimpleVector<B>>()->valueAt(0);
      auto rawValues = flatValues->mutableRawValues();
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        bool overflow = false;
        Operation::template apply<R, A, B>(
            rawResults[row],
            rawValues[row],
            constant,
            aRescale_,
            bRescale_,
            aPrecision_,
            aScale_,
            bPrecision_,
            bScale_,
            rPrecision_,
            rScale_,
            overflow);
        if (overflow ||
            !velox::DecimalUtil::valueInPrecisionRange(
                rawResults[row], rPrecision_)) {
          result->setNull(row, true);
        }
      });
    } else if (args[0]->isFlatEncoding() && args[1]->isFlatEncoding()) {
      // Fast path for (flat, flat).
      auto flatA = args[0]->asUnchecked<FlatVector<A>>();
      auto rawA = flatA->mutableRawValues();
      auto flatB = args[1]->asUnchecked<FlatVector<B>>();
      auto rawB = flatB->mutableRawValues();

      context.applyToSelectedNoThrow(rows, [&](auto row) {
        bool overflow = false;
        Operation::template apply<R, A, B>(
            rawResults[row],
            rawA[row],
            rawB[row],
            aRescale_,
            bRescale_,
            aPrecision_,
            aScale_,
            bPrecision_,
            bScale_,
            rPrecision_,
            rScale_,
            overflow);
        if (overflow ||
            !velox::DecimalUtil::valueInPrecisionRange(
                rawResults[row], rPrecision_)) {
          result->setNull(row, true);
        }
      });
    } else {
      // Fast path if one or more arguments are encoded.
      exec::DecodedArgs decodedArgs(rows, args, context);
      auto a = decodedArgs.at(0);
      auto b = decodedArgs.at(1);
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        bool overflow = false;
        Operation::template apply<R, A, B>(
            rawResults[row],
            a->valueAt<A>(row),
            b->valueAt<B>(row),
            aRescale_,
            bRescale_,
            aPrecision_,
            aScale_,
            bPrecision_,
            bScale_,
            rPrecision_,
            rScale_,
            overflow);
        if (overflow ||
            !velox::DecimalUtil::valueInPrecisionRange(
                rawResults[row], rPrecision_)) {
          result->setNull(row, true);
        }
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
  const uint8_t aPrecision_;
  const uint8_t aScale_;
  const uint8_t bPrecision_;
  const uint8_t bScale_;
  const uint8_t rPrecision_;
  const uint8_t rScale_;
};

class Addition {
 public:
  template <typename TResult, typename A, typename B>
  inline static void apply(
      TResult& r,
      A a,
      B b,
      uint8_t aRescale,
      uint8_t bRescale,
      uint8_t /* aPrecision */,
      uint8_t aScale,
      uint8_t /* bPrecision */,
      uint8_t bScale,
      uint8_t rPrecision,
      uint8_t rScale,
      bool& overflow) {
    if (rPrecision < LongDecimalType::kMaxPrecision) {
      const int128_t aRescaled = a * velox::DecimalUtil::kPowersOfTen[aRescale];
      const int128_t bRescaled = b * velox::DecimalUtil::kPowersOfTen[bRescale];
      r = TResult(aRescaled + bRescaled);
    } else {
      const uint32_t minLeadingZeros =
          DecimalUtil::minLeadingZeros<A, B>(a, b, aRescale, bRescale);
      if (minLeadingZeros >= 3) {
        // Fast path for no overflow. If both numbers contain at least 3 leading
        // zeros, they can be added directly without the risk of overflow.
        // The reason is if a number contains at least 2 leading zeros, it is
        // ensured that the number fits in the maximum of decimal, because
        // '2^126 - 1 < 10^38 - 1'. If both numbers contain at least 3 leading
        // zeros, we are guaranteed that the result will have at least 2 leading
        // zeros.
        int128_t aRescaled = a * velox::DecimalUtil::kPowersOfTen[aRescale];
        int128_t bRescaled = b * velox::DecimalUtil::kPowersOfTen[bRescale];
        r = reduceScale(
            TResult(aRescaled + bRescaled), std::max(aScale, bScale) - rScale);
      } else {
        // The risk of overflow should be considered. Add whole and fraction
        // parts separately, and then combine.
        r = addLarge<TResult, A, B>(a, b, aScale, bScale, rScale, overflow);
      }
    }
  }

  inline static uint8_t
  computeRescaleFactor(uint8_t fromScale, uint8_t toScale, uint8_t rScale = 0) {
    return std::max(0, toScale - fromScale);
  }

  inline static std::pair<uint8_t, uint8_t> computeResultPrecisionScale(
      uint8_t aPrecision,
      uint8_t aScale,
      uint8_t bPrecision,
      uint8_t bScale) {
    auto precision = std::max(aPrecision - aScale, bPrecision - bScale) +
        std::max(aScale, bScale) + 1;
    auto scale = std::max(aScale, bScale);
    return DecimalUtil::adjustPrecisionScale(precision, scale);
  }
};

class Subtraction {
 public:
  template <typename TResult, typename A, typename B>
  inline static void apply(
      TResult& r,
      A a,
      B b,
      uint8_t aRescale,
      uint8_t bRescale,
      uint8_t aPrecision,
      uint8_t aScale,
      uint8_t bPrecision,
      uint8_t bScale,
      uint8_t rPrecision,
      uint8_t rScale,
      bool& overflow) {
    Addition::apply<TResult, A, B>(
        r,
        a,
        B(-b),
        aRescale,
        bRescale,
        aPrecision,
        aScale,
        bPrecision,
        bScale,
        rPrecision,
        rScale,
        overflow);
  }

  inline static uint8_t
  computeRescaleFactor(uint8_t fromScale, uint8_t toScale, uint8_t rScale = 0) {
    return std::max(0, toScale - fromScale);
  }

  inline static std::pair<uint8_t, uint8_t> computeResultPrecisionScale(
      uint8_t aPrecision,
      uint8_t aScale,
      uint8_t bPrecision,
      uint8_t bScale) {
    return Addition::computeResultPrecisionScale(
        aPrecision, aScale, bPrecision, bScale);
  }
};

class Multiply {
 public:
  // Derive from Arrow.
  // https://github.com/apache/arrow/blob/release-12.0.1-rc1/cpp/src/gandiva/precompiled/decimal_ops.cc#L331
  template <typename R, typename A, typename B>
  inline static void apply(
      R& r,
      A a,
      B b,
      uint8_t aRescale,
      uint8_t bRescale,
      uint8_t aPrecision,
      uint8_t aScale,
      uint8_t bPrecision,
      uint8_t bScale,
      uint8_t rPrecision,
      uint8_t rScale,
      bool& overflow) {
    if (rPrecision < 38) {
      R result = DecimalUtil::multiply<R>(R(a), R(b), overflow);
      VELOX_DCHECK(!overflow);
      r = DecimalUtil::multiply<R>(
          result,
          R(velox::DecimalUtil::kPowersOfTen[aRescale + bRescale]),
          overflow);
      VELOX_DCHECK(!overflow);
    } else if (a == 0 && b == 0) {
      // Handle this separately to avoid divide-by-zero errors.
      r = R(0);
    } else {
      auto deltaScale = aScale + bScale - rScale;
      if (deltaScale == 0) {
        // No scale down.
        // Multiply when the out_precision is 38, and there is no trimming of
        // the scale i.e the intermediate value is the same as the final value.
        r = DecimalUtil::multiply<R>(R(a), R(b), overflow);
      } else {
        // Scale down.
        // It's possible that the intermediate value does not fit in 128-bits,
        // but the final value will (after scaling down).
        int32_t totalLeadingZeros =
            bits::countLeadingZeros(DecimalUtil::absValue<A>(a)) +
            bits::countLeadingZeros(DecimalUtil::absValue<B>(b));
        // This check is quick, but conservative. In some cases it will
        // indicate that converting to 256 bits is necessary, when it's not
        // actually the case.
        if (UNLIKELY(totalLeadingZeros <= 128)) {
          // Needs int256.
          int256_t reslarge =
              static_cast<int256_t>(a) * static_cast<int256_t>(b);
          reslarge = reduceScaleBy(reslarge, deltaScale);
          r = DecimalUtil::convert<R>(reslarge, overflow);
        } else {
          if (LIKELY(deltaScale <= 38)) {
            // The largest value that result can have here is (2^64 - 1) * (2^63
            // - 1) = 1.70141E+38,which is greater than
            // DecimalUtil::kLongDecimalMax.
            R result = DecimalUtil::multiply<R>(R(a), R(b), overflow);
            VELOX_DCHECK(!overflow);
            // Since deltaScale is greater than zero, result can now be at most
            // ((2^64 - 1) * (2^63 - 1)) / 10, which is less than
            // DecimalUtil::kLongDecimalMax, so there cannot be any overflow.
            DecimalUtil::divideWithRoundUp<R, R, R>(
                r,
                result,
                R(velox::DecimalUtil::kPowersOfTen[deltaScale]),
                0,
                overflow);
            VELOX_DCHECK(!overflow);
          } else {
            // We are multiplying decimal(38, 38) by decimal(38, 38). The result
            // should be a
            // decimal(38, 37), so delta scale = 38 + 38 - 37 = 39. Since we are
            // not in the 256 bit intermediate value case and we are scaling
            // down by 39, then we are guaranteed that the result is 0 (even if
            // we try to round). The largest possible intermediate result is 38
            // "9"s. If we scale down by 39, the leftmost 9 is now two digits to
            // the right of the rightmost "visible" one. The reason why we have
            // to handle this case separately is because a scale multiplier with
            // a deltaScale 39 does not fit into 128 bit.
            r = R(0);
          }
        }
      }
    }
  }

  inline static uint8_t
  computeRescaleFactor(uint8_t fromScale, uint8_t toScale, uint8_t rScale = 0) {
    return 0;
  }

  inline static std::pair<uint8_t, uint8_t> computeResultPrecisionScale(
      uint8_t aPrecision,
      uint8_t aScale,
      uint8_t bPrecision,
      uint8_t bScale) {
    return DecimalUtil::adjustPrecisionScale(
        aPrecision + bPrecision + 1, aScale + bScale);
  }

 private:
  inline static int256_t reduceScaleBy(int256_t in, int32_t reduceBy) {
    if (reduceBy == 0) {
      return in;
    }

    int256_t divisor = velox::DecimalUtil::kPowersOfTen[reduceBy];
    auto result = in / divisor;
    auto remainder = in % divisor;
    // Round up.
    if (abs(remainder) >= (divisor >> 1)) {
      result += (in > 0 ? 1 : -1);
    }
    return result;
  }
};

class Divide {
 public:
  template <typename R, typename A, typename B>
  inline static void apply(
      R& r,
      A a,
      B b,
      uint8_t aRescale,
      uint8_t /* bRescale */,
      uint8_t /* aPrecision */,
      uint8_t /* aScale */,
      uint8_t /* bPrecision */,
      uint8_t /* bScale */,
      uint8_t /* rPrecision */,
      uint8_t /* rScale */,
      bool& overflow) {
    DecimalUtil::divideWithRoundUp<R, A, B>(r, a, b, aRescale, overflow);
  }

  inline static uint8_t
  computeRescaleFactor(uint8_t fromScale, uint8_t toScale, uint8_t rScale) {
    return rScale - fromScale + toScale;
  }

  inline static std::pair<uint8_t, uint8_t> computeResultPrecisionScale(
      uint8_t aPrecision,
      uint8_t aScale,
      uint8_t bPrecision,
      uint8_t bScale) {
    auto scale = std::max(6, aScale + bPrecision + 1);
    auto precision = aPrecision - aScale + bScale + scale;
    return DecimalUtil::adjustPrecisionScale(precision, scale);
  }
};

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
          .integerVariable(
              "r_scale",
              getResultScale(
                  "max(a_precision - a_scale, b_precision - b_scale) + max(a_scale, b_scale) + 1",
                  "max(a_scale, b_scale)"))
          .returnType("DECIMAL(r_precision, r_scale)")
          .argumentType("DECIMAL(a_precision, a_scale)")
          .argumentType("DECIMAL(b_precision, b_scale)")
          .build()};
}

std::vector<std::shared_ptr<exec::FunctionSignature>>
decimalMultiplySignature() {
  return {exec::FunctionSignatureBuilder()
              .integerVariable("a_precision")
              .integerVariable("a_scale")
              .integerVariable("b_precision")
              .integerVariable("b_scale")
              .integerVariable(
                  "r_precision", "min(38, a_precision + b_precision + 1)")
              .integerVariable(
                  "r_scale",
                  getResultScale(
                      "a_precision + b_precision + 1", "a_scale + b_scale"))
              .returnType("DECIMAL(r_precision, r_scale)")
              .argumentType("DECIMAL(a_precision, a_scale)")
              .argumentType("DECIMAL(b_precision, b_scale)")
              .build()};
}

std::vector<std::shared_ptr<exec::FunctionSignature>> decimalDivideSignature() {
  return {
      exec::FunctionSignatureBuilder()
          .integerVariable("a_precision")
          .integerVariable("a_scale")
          .integerVariable("b_precision")
          .integerVariable("b_scale")
          .integerVariable(
              "r_precision",
              "min(38, a_precision - a_scale + b_scale + max(6, a_scale + b_precision + 1))")
          .integerVariable(
              "r_scale",
              getResultScale(
                  "a_precision - a_scale + b_scale + max(6, a_scale + b_precision + 1)",
                  "max(6, a_scale + b_precision + 1)"))
          .returnType("DECIMAL(r_precision, r_scale)")
          .argumentType("DECIMAL(a_precision, a_scale)")
          .argumentType("DECIMAL(b_precision, b_scale)")
          .build()};
}

template <typename Operation>
std::shared_ptr<exec::VectorFunction> createDecimalFunction(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  const auto& aType = inputArgs[0].type;
  const auto& bType = inputArgs[1].type;
  const auto [aPrecision, aScale] = getDecimalPrecisionScale(*aType);
  const auto [bPrecision, bScale] = getDecimalPrecisionScale(*bType);
  const auto [rPrecision, rScale] = Operation::computeResultPrecisionScale(
      aPrecision, aScale, bPrecision, bScale);
  const uint8_t aRescale =
      Operation::computeRescaleFactor(aScale, bScale, rScale);
  const uint8_t bRescale =
      Operation::computeRescaleFactor(bScale, aScale, rScale);
  if (aType->isShortDecimal()) {
    if (bType->isShortDecimal()) {
      if (rPrecision > ShortDecimalType::kMaxPrecision) {
        return std::make_shared<DecimalBaseFunction<
            int128_t /*result*/,
            int64_t,
            int64_t,
            Operation>>(
            aRescale,
            bRescale,
            aPrecision,
            aScale,
            bPrecision,
            bScale,
            rPrecision,
            rScale);
      } else {
        return std::make_shared<DecimalBaseFunction<
            int64_t /*result*/,
            int64_t,
            int64_t,
            Operation>>(
            aRescale,
            bRescale,
            aPrecision,
            aScale,
            bPrecision,
            bScale,
            rPrecision,
            rScale);
      }
    } else {
      return std::make_shared<DecimalBaseFunction<
          int128_t /*result*/,
          int64_t,
          int128_t,
          Operation>>(
          aRescale,
          bRescale,
          aPrecision,
          aScale,
          bPrecision,
          bScale,
          rPrecision,
          rScale);
    }
  } else {
    if (bType->isShortDecimal()) {
      return std::make_shared<DecimalBaseFunction<
          int128_t /*result*/,
          int128_t,
          int64_t,
          Operation>>(
          aRescale,
          bRescale,
          aPrecision,
          aScale,
          bPrecision,
          bScale,
          rPrecision,
          rScale);
    } else {
      return std::make_shared<DecimalBaseFunction<
          int128_t /*result*/,
          int128_t,
          int128_t,
          Operation>>(
          aRescale,
          bRescale,
          aPrecision,
          aScale,
          bPrecision,
          bScale,
          rPrecision,
          rScale);
    }
  }
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
} // namespace facebook::velox::functions::sparksql
