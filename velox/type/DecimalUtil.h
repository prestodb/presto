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

#pragma once

#include <string>
#include "velox/common/base/CheckedArithmetic.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/Nulls.h"
#include "velox/common/base/Status.h"
#include "velox/type/Type.h"

namespace facebook::velox {

/// A static class that holds helper functions for DECIMAL type.
class DecimalUtil {
 public:
  static constexpr int128_t kPowersOfTen[LongDecimalType::kMaxPrecision + 1] = {
      1,
      10,
      100,
      1'000,
      10'000,
      100'000,
      1'000'000,
      10'000'000,
      100'000'000,
      1'000'000'000,
      10'000'000'000,
      100'000'000'000,
      1'000'000'000'000,
      10'000'000'000'000,
      100'000'000'000'000,
      1'000'000'000'000'000,
      10'000'000'000'000'000,
      100'000'000'000'000'000,
      1'000'000'000'000'000'000,
      1'000'000'000'000'000'000 * (int128_t)10,
      1'000'000'000'000'000'000 * (int128_t)100,
      1'000'000'000'000'000'000 * (int128_t)1'000,
      1'000'000'000'000'000'000 * (int128_t)10'000,
      1'000'000'000'000'000'000 * (int128_t)100'000,
      1'000'000'000'000'000'000 * (int128_t)1'000'000,
      1'000'000'000'000'000'000 * (int128_t)10'000'000,
      1'000'000'000'000'000'000 * (int128_t)100'000'000,
      1'000'000'000'000'000'000 * (int128_t)1'000'000'000,
      1'000'000'000'000'000'000 * (int128_t)10'000'000'000,
      1'000'000'000'000'000'000 * (int128_t)100'000'000'000,
      1'000'000'000'000'000'000 * (int128_t)1'000'000'000'000,
      1'000'000'000'000'000'000 * (int128_t)10'000'000'000'000,
      1'000'000'000'000'000'000 * (int128_t)100'000'000'000'000,
      1'000'000'000'000'000'000 * (int128_t)1'000'000'000'000'000,
      1'000'000'000'000'000'000 * (int128_t)10'000'000'000'000'000,
      1'000'000'000'000'000'000 * (int128_t)100'000'000'000'000'000,
      1'000'000'000'000'000'000 * (int128_t)1'000'000'000'000'000'000,
      1'000'000'000'000'000'000 * (int128_t)1'000'000'000'000'000'000 *
          (int128_t)10,
      1'000'000'000'000'000'000 * (int128_t)1'000'000'000'000'000'000 *
          (int128_t)100};

  static constexpr int128_t kLongDecimalMin =
      -kPowersOfTen[LongDecimalType::kMaxPrecision] + 1;
  static constexpr int128_t kLongDecimalMax =
      kPowersOfTen[LongDecimalType::kMaxPrecision] - 1;
  static constexpr int128_t kShortDecimalMin =
      -kPowersOfTen[ShortDecimalType::kMaxPrecision] + 1;
  static constexpr int128_t kShortDecimalMax =
      kPowersOfTen[ShortDecimalType::kMaxPrecision] - 1;

  static constexpr uint64_t kInt64Mask = ~(static_cast<uint64_t>(1) << 63);
  static constexpr uint128_t kInt128Mask = (static_cast<uint128_t>(1) << 127);

  FOLLY_ALWAYS_INLINE static void valueInRange(int128_t value) {
    VELOX_USER_CHECK(
        (value >= kLongDecimalMin && value <= kLongDecimalMax),
        "Decimal overflow. Value '{}' is not in the range of Decimal Type",
        value);
  }

  // Returns true if the precision can represent the value.
  template <typename T>
  FOLLY_ALWAYS_INLINE static bool valueInPrecisionRange(
      T value,
      uint8_t precision) {
    return value < kPowersOfTen[precision] && value > -kPowersOfTen[precision];
  }

  /// Helper function to convert a decimal value to string.
  static std::string toString(int128_t value, const TypePtr& type);

  template <typename T>
  inline static void fillDecimals(
      T* decimals,
      const uint64_t* nullsPtr,
      const T* values,
      const int64_t* scales,
      int32_t numValues,
      int32_t targetScale) {
    for (int32_t i = 0; i < numValues; i++) {
      if (!nullsPtr || !bits::isBitNull(nullsPtr, i)) {
        int32_t currentScale = scales[i];
        T value = values[i];
        if constexpr (std::is_same_v<T, std::int64_t>) { // Short Decimal
          if (targetScale > currentScale &&
              targetScale - currentScale <= ShortDecimalType::kMaxPrecision) {
            value *= static_cast<T>(kPowersOfTen[targetScale - currentScale]);
          } else if (
              targetScale < currentScale &&
              currentScale - targetScale <= ShortDecimalType::kMaxPrecision) {
            value /= static_cast<T>(kPowersOfTen[currentScale - targetScale]);
          } else if (targetScale != currentScale) {
            VELOX_FAIL("Decimal scale out of range");
          }
        } else { // Long Decimal
          if (targetScale > currentScale) {
            while (targetScale > currentScale) {
              int32_t scaleAdjust = std::min<int32_t>(
                  ShortDecimalType::kMaxPrecision, targetScale - currentScale);
              value *= kPowersOfTen[scaleAdjust];
              currentScale += scaleAdjust;
            }
          } else if (targetScale < currentScale) {
            while (currentScale > targetScale) {
              int32_t scaleAdjust = std::min<int32_t>(
                  ShortDecimalType::kMaxPrecision, currentScale - targetScale);
              value /= kPowersOfTen[scaleAdjust];
              currentScale -= scaleAdjust;
            }
          }
        }
        decimals[i] = value;
      }
    }
  }

  template <typename TInput, typename TOutput>
  inline static Status rescaleWithRoundUp(
      TInput inputValue,
      int fromPrecision,
      int fromScale,
      int toPrecision,
      int toScale,
      TOutput& output) {
    int128_t rescaledValue = inputValue;
    auto scaleDifference = toScale - fromScale;
    bool isOverflow = false;
    if (scaleDifference >= 0) {
      isOverflow = __builtin_mul_overflow(
          rescaledValue,
          DecimalUtil::kPowersOfTen[scaleDifference],
          &rescaledValue);
    } else {
      scaleDifference = -scaleDifference;
      const auto scalingFactor = DecimalUtil::kPowersOfTen[scaleDifference];
      rescaledValue /= scalingFactor;
      int128_t remainder = inputValue % scalingFactor;
      if (inputValue >= 0 && remainder >= scalingFactor / 2) {
        ++rescaledValue;
      } else if (remainder <= -scalingFactor / 2) {
        --rescaledValue;
      }
    }
    // Check overflow.
    if (!valueInPrecisionRange(rescaledValue, toPrecision) || isOverflow) {
      return Status::UserError(
          "Cannot cast DECIMAL '{}' to DECIMAL({}, {})",
          DecimalUtil::toString(inputValue, DECIMAL(fromPrecision, fromScale)),
          toPrecision,
          toScale);
    }
    output = static_cast<TOutput>(rescaledValue);
    return Status::OK();
  }

  template <typename TInput, typename TOutput>
  inline static std::optional<TOutput>
  rescaleInt(TInput inputValue, int toPrecision, int toScale) {
    int128_t rescaledValue = static_cast<int128_t>(inputValue);
    bool isOverflow = __builtin_mul_overflow(
        rescaledValue, DecimalUtil::kPowersOfTen[toScale], &rescaledValue);
    // Check overflow.
    if (!valueInPrecisionRange(rescaledValue, toPrecision) || isOverflow) {
      VELOX_USER_FAIL(
          "Cannot cast {} '{}' to DECIMAL({}, {})",
          SimpleTypeTrait<TInput>::name,
          inputValue,
          toPrecision,
          toScale);
    }
    return static_cast<TOutput>(rescaledValue);
  }

  /// Rescales a double value to decimal value of given precision and scale. The
  /// output is rescaled value of int128_t or int64_t type. Returns error status
  /// if fails.
  template <typename TOutput>
  inline static Status
  rescaleDouble(double value, int precision, int scale, TOutput& output) {
    if (!std::isfinite(value)) {
      return Status::UserError("The input value should be finite.");
    }

    long double rounded;
    // A double provides 16(±1) decimal digits, so at least 15 digits are
    // precise.
    if (scale > 15) {
      // Convert value to long double type, as double * int128_t returns
      // int128_t and fractional digits are lost. No need to consider 'toValue'
      // becoming infinite as DOUBLE_MAX * 10^38 < LONG_DOUBLE_MAX.
      const auto toValue = (long double)value * DecimalUtil::kPowersOfTen[15];
      rounded = std::round(toValue) * DecimalUtil::kPowersOfTen[scale - 15];
    } else {
      const auto toValue =
          (long double)value * DecimalUtil::kPowersOfTen[scale];
      rounded = std::round(toValue);
    }

    const auto result = folly::tryTo<TOutput>(rounded);
    if (result.hasError()) {
      return Status::UserError("Result overflows.");
    }
    const TOutput rescaledValue = result.value();
    if (!valueInPrecisionRange<TOutput>(rescaledValue, precision)) {
      return Status::UserError(
          "Result cannot fit in the given precision {}.", precision);
    }
    output = rescaledValue;
    return Status::OK();
  }

  template <typename R, typename A, typename B>
  inline static R divideWithRoundUp(
      R& r,
      A a,
      B b,
      bool noRoundUp,
      uint8_t aRescale,
      uint8_t /*bRescale*/) {
    VELOX_USER_CHECK_NE(b, 0, "Division by zero");
    int resultSign = 1;
    R unsignedDividendRescaled(a);
    if (a < 0) {
      resultSign = -1;
      unsignedDividendRescaled *= -1;
    }
    B unsignedDivisor(b);
    if (b < 0) {
      resultSign *= -1;
      unsignedDivisor *= -1;
    }
    unsignedDividendRescaled = checkedMultiply<R>(
        unsignedDividendRescaled,
        R(DecimalUtil::kPowersOfTen[aRescale]),
        "Decimal");
    R quotient = unsignedDividendRescaled / unsignedDivisor;
    R remainder = unsignedDividendRescaled % unsignedDivisor;
    if (!noRoundUp && static_cast<const B>(remainder) * 2 >= unsignedDivisor) {
      ++quotient;
    }
    r = quotient * resultSign;
    return remainder * resultSign;
  }

  /*
   * sum up and return overflow/underflow.
   */
  inline static int64_t addUnsignedValues(
      int128_t& sum,
      int128_t lhs,
      int128_t rhs,
      bool isResultNegative) {
    __uint128_t unsignedSum = (__uint128_t)lhs + (__uint128_t)rhs;
    // Ignore overflow value.
    sum = (int128_t)unsignedSum & ~kOverflowMultiplier;
    sum = isResultNegative ? -sum : sum;
    return (unsignedSum >> 127);
  }

  /// Adds two signed 128-bit numbers (int128_t), calculates the sum, and
  /// returns the overflow. It can be used to track the number of overflow when
  /// adding a batch of input numbers. It takes lhs and rhs as input, and stores
  /// their sum in result. overflow == 1 indicates upward overflow. overflow ==
  /// -1 indicates downward overflow. overflow == 0 indicates no overflow.
  /// Adding negative and non-negative numbers never overflows, so we can
  /// directly add them. Adding two negative or two positive numbers may
  /// overflow. To add numbers that may overflow, first convert both numbers to
  /// unsigned 128-bit number (uint128_t), and perform the addition. The highest
  /// bits in the result indicates overflow. Adjust the signs of sum and
  /// overflow based on the signs of the inputs. The caller must sum up overflow
  /// values and call adjustSumForOverflow after processing all inputs.
  inline static int64_t
  addWithOverflow(int128_t& result, int128_t lhs, int128_t rhs) {
    bool isLhsNegative = lhs < 0;
    bool isRhsNegative = rhs < 0;
    int64_t overflow = 0;
    if (isLhsNegative == isRhsNegative) {
      // Both inputs of same time.
      if (isLhsNegative) {
        // Both negative, ignore signs and add.
        VELOX_DCHECK_NE(lhs, std::numeric_limits<int128_t>::min());
        VELOX_DCHECK_NE(rhs, std::numeric_limits<int128_t>::min());
        overflow = addUnsignedValues(result, -lhs, -rhs, true);
        overflow = -overflow;
      } else {
        overflow = addUnsignedValues(result, lhs, rhs, false);
      }
    } else {
      // If one of them is negative, use addition.
      result = lhs + rhs;
    }
    return overflow;
  }

  /// Corrects the sum result calculated using addWithOverflow. Since the sum
  /// calculated by addWithOverflow only retains the lower 127 bits,
  /// it may miss one calculation of +(1 << 127) or -(1 << 127).
  /// Therefore, we need to make the following adjustments:
  /// 1. If overflow = 1 && sum < 0, the calculation missed +(1 << 127).
  /// Add 1 << 127 to the sum.
  /// 2. If overflow = -1 && sum > 0, the calculation missed -(1 << 127).
  /// Subtract 1 << 127 to the sum.
  /// If an overflow indeed occurs and the result cannot be adjusted,
  /// it will return std::nullopt.
  inline static std::optional<int128_t> adjustSumForOverflow(
      int128_t sum,
      int64_t overflow) {
    // Value is valid if the conditions below are true.
    if ((overflow == 1 && sum < 0) || (overflow == -1 && sum > 0)) {
      return static_cast<int128_t>(
          DecimalUtil::kOverflowMultiplier * overflow + sum);
    }
    if (overflow != 0) {
      // The actual overflow occurred.
      return std::nullopt;
    }

    return sum;
  }

  /// avg = (sum + overflow * kOverflowMultiplier) / count
  static void
  computeAverage(int128_t& avg, int128_t sum, int64_t count, int64_t overflow);

  /// Origins from java side BigInteger#bitLength.
  ///
  /// Returns the number of bits in the minimal two's-complement
  /// representation of this BigInteger, <em>excluding</em> a sign bit.
  /// For positive BigIntegers, this is equivalent to the number of bits in
  /// the ordinary binary representation.  For zero this method returns
  /// {@code 0}.  (Computes {@code (ceil(log2(this < 0 ? -this : this+1)))}.)
  ///
  /// @return number of bits in the minimal two's-complement
  ///         representation of this BigInteger, <em>excluding</em> a sign bit.
  static int32_t getByteArrayLength(int128_t value);

  /// This method return the same result with the BigInterger#toByteArray()
  /// method in Java side.
  ///
  /// Returns a byte array containing the two's-complement representation of
  /// this BigInteger. The byte array will be in big-endian byte-order: the most
  /// significant byte is in the zeroth element. The array will contain the
  /// minimum number of bytes required to represent this BigInteger, including
  /// at least one sign bit, which is (ceil((this.bitLength() + 1)/8)).
  ///
  /// @return The length of out.
  static int32_t toByteArray(int128_t value, char* out);

  static constexpr __uint128_t kOverflowMultiplier = ((__uint128_t)1 << 127);
}; // DecimalUtil
} // namespace facebook::velox
