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

#include "velox/functions/Macros.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/sparksql/DecimalUtil.h"

namespace facebook::velox::functions::sparksql {
namespace {

struct DecimalAddSubtractBase {
 protected:
  void initializeBase(const std::vector<TypePtr>& inputTypes) {
    auto [aPrecision, aScale] = getDecimalPrecisionScale(*inputTypes[0]);
    auto [bPrecision, bScale] = getDecimalPrecisionScale(*inputTypes[1]);
    aScale_ = aScale;
    bScale_ = bScale;
    auto [rPrecision, rScale] =
        computeResultPrecisionScale(aPrecision, aScale_, bPrecision, bScale_);
    rPrecision_ = rPrecision;
    rScale_ = rScale;
    aRescale_ = computeRescaleFactor(aScale_, bScale_);
    bRescale_ = computeRescaleFactor(bScale_, aScale_);
  }

  // Adds the values 'a' and 'b' and stores the result in 'r'. To align the
  // scales of inputs, the value with the smaller scale is rescaled to the
  // larger scale. 'aRescale' and 'bRescale' are the rescale factors needed to
  // rescale 'a' and 'b'. 'rPrecision' and 'rScale' are the precision and scale
  // of the result.
  template <typename TResult, typename A, typename B>
  bool applyAdd(TResult& r, A a, B b) {
    // The overflow flag is set to true if an overflow occurs
    // during the addition.
    bool overflow = false;
    if (rPrecision_ < LongDecimalType::kMaxPrecision) {
      const int128_t aRescaled =
          a * velox::DecimalUtil::kPowersOfTen[aRescale_];
      const int128_t bRescaled =
          b * velox::DecimalUtil::kPowersOfTen[bRescale_];
      r = TResult(aRescaled + bRescaled);
    } else {
      const uint32_t minLeadingZeros =
          sparksql::DecimalUtil::minLeadingZeros<A, B>(
              a, b, aRescale_, bRescale_);
      if (minLeadingZeros >= 3) {
        // Fast path for no overflow. If both numbers contain at least 3 leading
        // zeros, they can be added directly without the risk of overflow.
        // The reason is if a number contains at least 2 leading zeros, it is
        // ensured that the number fits in the maximum of decimal, because
        // '2^126 - 1 < 10^38 - 1'. If both numbers contain at least 3 leading
        // zeros, we are guaranteed that the result will have at least 2 leading
        // zeros.
        int128_t aRescaled = a * velox::DecimalUtil::kPowersOfTen[aRescale_];
        int128_t bRescaled = b * velox::DecimalUtil::kPowersOfTen[bRescale_];
        r = reduceScale(
            TResult(aRescaled + bRescaled),
            std::max(aScale_, bScale_) - rScale_);
      } else {
        // The risk of overflow should be considered. Add whole and fraction
        // parts separately, and then combine.
        r = addLarge<TResult, A, B>(a, b, aScale_, bScale_, rScale_, overflow);
      }
    }
    return !overflow &&
        velox::DecimalUtil::valueInPrecisionRange(r, rPrecision_);
  }

 private:
  // Returns the whole and fraction parts of a decimal value.
  template <typename T>
  static std::pair<T, T> getWholeAndFraction(T value, uint8_t scale) {
    const auto scaleFactor = velox::DecimalUtil::kPowersOfTen[scale];
    const T whole = value / scaleFactor;
    return {whole, value - whole * scaleFactor};
  }

  // Increases the scale of input value by 'delta'. Returns the input value if
  // delta is not positive.
  static int128_t increaseScale(int128_t in, int16_t delta) {
    // No need to consider overflow as 'delta == higher scale - input scale', so
    // the scaled value will not exceed the maximum of long decimal.
    return delta <= 0 ? in : in * velox::DecimalUtil::kPowersOfTen[delta];
  }

  // Scales up the whole part to result scale, and combine it with fraction part
  // to produce a full result for decimal add. Checks whether the result
  // overflows.
  template <typename T>
  static T
  decimalAddResult(T whole, T fraction, uint8_t resultScale, bool& overflow) {
    T scaledWhole = sparksql::DecimalUtil::multiply<T>(
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

  // Reduces the scale of input value by 'delta'. Returns the input value if
  // delta is not positive.
  template <typename T>
  static T reduceScale(T in, int32_t delta) {
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
  static TResult addLargeNonNegative(
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
  static TResult addLargeOpposite(
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

    // If the whole and fractional parts have different signs, adjust them to
    // the same sign.
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
    return decimalAddResult(
        TResult(whole), TResult(fraction), rScale, overflow);
  }

  // Add whole and fraction parts separately, and then combine. The overflow
  // flag will be set to true if an overflow occurs during the addition.
  template <typename TResult, typename A, typename B>
  static TResult addLarge(
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

  // Computes the result precision and scale for decimal add and subtract
  // operations following Hive's formulas.
  // If result is representable with long decimal, the result
  // scale is the maximum of 'aScale' and 'bScale'. If not, reduces result scale
  // and caps the result precision at 38.
  static std::pair<uint8_t, uint8_t> computeResultPrecisionScale(
      uint8_t aPrecision,
      uint8_t aScale,
      uint8_t bPrecision,
      uint8_t bScale) {
    auto precision = std::max(aPrecision - aScale, bPrecision - bScale) +
        std::max(aScale, bScale) + 1;
    auto scale = std::max(aScale, bScale);
    return sparksql::DecimalUtil::adjustPrecisionScale(precision, scale);
  }

  static uint8_t computeRescaleFactor(uint8_t fromScale, uint8_t toScale) {
    return std::max(0, toScale - fromScale);
  }

  uint8_t aScale_;
  uint8_t bScale_;
  uint8_t aRescale_;
  uint8_t bRescale_;
  uint8_t rPrecision_;
  uint8_t rScale_;
};

template <typename TExec>
struct DecimalAddFunction : DecimalAddSubtractBase {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  template <typename A, typename B>
  void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig& /*config*/,
      A* /*a*/,
      B* /*b*/) {
    initializeBase(inputTypes);
  }

  template <typename R, typename A, typename B>
  bool call(R& out, const A& a, const B& b) {
    return applyAdd<R, A, B>(out, a, b);
  }
};

template <typename TExec>
struct DecimalSubtractFunction : DecimalAddSubtractBase {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  template <typename A, typename B>
  void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig& /*config*/,
      A* /*a*/,
      B* /*b*/) {
    initializeBase(inputTypes);
  }

  template <typename R, typename A, typename B>
  bool call(R& out, const A& a, const B& b) {
    return applyAdd<R, A, B>(out, a, B(-b));
  }
};

template <typename TExec>
struct DecimalMultiplyFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  template <typename A, typename B>
  void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig& /*config*/,
      A* /*a*/,
      B* /*b*/) {
    auto [aPrecision, aScale] = getDecimalPrecisionScale(*inputTypes[0]);
    auto [bPrecision, bScale] = getDecimalPrecisionScale(*inputTypes[1]);
    auto [rPrecision, rScale] = DecimalUtil::adjustPrecisionScale(
        aPrecision + bPrecision + 1, aScale + bScale);
    rPrecision_ = rPrecision;
    deltaScale_ = aScale + bScale - rScale;
  }

  template <typename R, typename A, typename B>
  bool call(R& out, const A& a, const B& b) {
    bool overflow = false;
    if (rPrecision_ < 38) {
      out = DecimalUtil::multiply<R>(R(a), R(b), overflow);
      VELOX_DCHECK(!overflow);
    } else if (a == 0 && b == 0) {
      // Handle this separately to avoid divide-by-zero errors.
      out = R(0);
    } else {
      if (deltaScale_ == 0) {
        // No scale down.
        // Multiply when the out_precision is 38, and there is no trimming of
        // the scale i.e the intermediate value is the same as the final value.
        out = DecimalUtil::multiply<R>(R(a), R(b), overflow);
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
          reslarge = reduceScaleBy(reslarge, deltaScale_);
          out = DecimalUtil::convert<R>(reslarge, overflow);
        } else {
          if (LIKELY(deltaScale_ <= 38)) {
            // The largest value that result can have here is (2^64 - 1) * (2^63
            // - 1) = 1.70141E+38,which is greater than
            // DecimalUtil::kLongDecimalMax.
            R result = DecimalUtil::multiply<R>(R(a), R(b), overflow);
            VELOX_DCHECK(!overflow);
            // Since deltaScale is greater than zero, result can now be at most
            // ((2^64 - 1) * (2^63 - 1)) / 10, which is less than
            // DecimalUtil::kLongDecimalMax, so there cannot be any overflow.
            DecimalUtil::divideWithRoundUp<R, R, R>(
                out,
                result,
                R(velox::DecimalUtil::kPowersOfTen[deltaScale_]),
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
            out = R(0);
          }
        }
      }
    }

    return !overflow &&
        velox::DecimalUtil::valueInPrecisionRange(out, rPrecision_);
  }

 private:
  static int256_t reduceScaleBy(int256_t in, int32_t reduceBy) {
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

  uint8_t rPrecision_;
  // The difference between result scale and the sum of aScale and bScale.
  int32_t deltaScale_;
};

template <typename TExec>
struct DecimalDivideFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  template <typename A, typename B>
  void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig& /*config*/,
      A* /*a*/,
      B* /*b*/) {
    auto [aPrecision, aScale] = getDecimalPrecisionScale(*inputTypes[0]);
    auto [bPrecision, bScale] = getDecimalPrecisionScale(*inputTypes[1]);
    auto [rPrecision, rScale] =
        computeResultPrecisionScale(aPrecision, aScale, bPrecision, bScale);
    rPrecision_ = rPrecision;
    aRescale_ = rScale - aScale + bScale;
  }

  template <typename R, typename A, typename B>
  bool call(R& out, const A& a, const B& b) {
    bool overflow = false;
    DecimalUtil::divideWithRoundUp<R, A, B>(out, a, b, aRescale_, overflow);
    return !overflow &&
        velox::DecimalUtil::valueInPrecisionRange(out, rPrecision_);
  }

 private:
  static std::pair<uint8_t, uint8_t> computeResultPrecisionScale(
      uint8_t aPrecision,
      uint8_t aScale,
      uint8_t bPrecision,
      uint8_t bScale) {
    auto scale = std::max(6, aScale + bPrecision + 1);
    auto precision = aPrecision - aScale + bScale + scale;
    return DecimalUtil::adjustPrecisionScale(precision, scale);
  }

  uint8_t aRescale_;
  uint8_t rPrecision_;
};

template <template <class> typename Func>
void registerDecimalBinary(
    const std::string& name,
    std::vector<exec::SignatureVariable> constraints) {
  // (long, long) -> long
  registerFunction<
      Func,
      LongDecimal<P3, S3>,
      LongDecimal<P1, S1>,
      LongDecimal<P2, S2>>({name}, constraints);

  // (short, short) -> short
  registerFunction<
      Func,
      ShortDecimal<P3, S3>,
      ShortDecimal<P1, S1>,
      ShortDecimal<P2, S2>>({name}, constraints);

  // (short, short) -> long
  registerFunction<
      Func,
      LongDecimal<P3, S3>,
      ShortDecimal<P1, S1>,
      ShortDecimal<P2, S2>>({name}, constraints);

  // (short, long) -> long
  registerFunction<
      Func,
      LongDecimal<P3, S3>,
      ShortDecimal<P1, S1>,
      LongDecimal<P2, S2>>({name}, constraints);

  // (long, short) -> long
  registerFunction<
      Func,
      LongDecimal<P3, S3>,
      LongDecimal<P1, S1>,
      ShortDecimal<P2, S2>>({name}, constraints);
}

std::vector<exec::SignatureVariable> makeConstraints(
    const std::string& rPrecision,
    const std::string& rScale) {
  std::string finalScale = fmt::format(
      "({}) <= 38 ? ({}) : max(({}) - ({}) + 38, min(({}), 6))",
      rPrecision,
      rScale,
      rScale,
      rPrecision,
      rScale);
  return {
      exec::SignatureVariable(
          P3::name(),
          fmt::format(
              "min(38, {r_precision})", fmt::arg("r_precision", rPrecision)),
          exec::ParameterType::kIntegerParameter),
      exec::SignatureVariable(
          S3::name(), finalScale, exec::ParameterType::kIntegerParameter)};
}

template <template <class> typename Func>
void registerDecimalAddSubtract(const std::string& name) {
  std::string rPrecision = fmt::format(
      "max({a_precision} - {a_scale}, {b_precision} - {b_scale}) + max({a_scale}, {b_scale}) + 1",
      fmt::arg("a_precision", P1::name()),
      fmt::arg("b_precision", P2::name()),
      fmt::arg("a_scale", S1::name()),
      fmt::arg("b_scale", S2::name()));
  std::string rScale = fmt::format(
      "max({a_scale}, {b_scale})",
      fmt::arg("a_scale", S1::name()),
      fmt::arg("b_scale", S2::name()));
  registerDecimalBinary<Func>(name, makeConstraints(rPrecision, rScale));
}

} // namespace

void registerDecimalAdd(const std::string& prefix) {
  registerDecimalAddSubtract<DecimalAddFunction>(prefix + "add");
}

void registerDecimalSubtract(const std::string& prefix) {
  registerDecimalAddSubtract<DecimalSubtractFunction>(prefix + "subtract");
}

void registerDecimalMultiply(const std::string& prefix) {
  std::string rPrecision = fmt::format(
      "{a_precision} + {b_precision} + 1",
      fmt::arg("a_precision", P1::name()),
      fmt::arg("b_precision", P2::name()));
  std::string rScale = fmt::format(
      "{a_scale} + {b_scale}",
      fmt::arg("a_scale", S1::name()),
      fmt::arg("b_scale", S2::name()));
  registerDecimalBinary<DecimalMultiplyFunction>(
      prefix + "multiply", makeConstraints(rPrecision, rScale));
}

std::vector<exec::SignatureVariable> getDivideConstraints() {
  std::string rPrecision = fmt::format(
      "{a_precision} - {a_scale} + {b_scale} + max(6, {a_scale} + {b_precision} + 1)",
      fmt::arg("a_precision", P1::name()),
      fmt::arg("b_precision", P2::name()),
      fmt::arg("a_scale", S1::name()),
      fmt::arg("b_scale", S2::name()));
  std::string rScale = fmt::format(
      "max(6, {a_scale} + {b_precision} + 1)",
      fmt::arg("a_scale", S1::name()),
      fmt::arg("b_precision", P2::name()));
  return makeConstraints(rPrecision, rScale);
}

void registerDecimalDivide(const std::string& prefix) {
  std::vector<exec::SignatureVariable> constraints = getDivideConstraints();
  registerDecimalBinary<DecimalDivideFunction>(prefix + "divide", constraints);

  // (short, long) -> short
  registerFunction<
      DecimalDivideFunction,
      ShortDecimal<P3, S3>,
      ShortDecimal<P1, S1>,
      LongDecimal<P2, S2>>({prefix + "divide"}, constraints);

  // (long, short) -> short
  registerFunction<
      DecimalDivideFunction,
      ShortDecimal<P3, S3>,
      LongDecimal<P1, S1>,
      ShortDecimal<P2, S2>>({prefix + "divide"}, constraints);
}
} // namespace facebook::velox::functions::sparksql
