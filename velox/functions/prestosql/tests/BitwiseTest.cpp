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
#include <gmock/gmock.h>
#include <optional>

#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

namespace facebook::velox {

namespace {

static constexpr auto kMin16 = std::numeric_limits<int16_t>::min();
static constexpr auto kMax16 = std::numeric_limits<int16_t>::max();
static constexpr auto kMin32 = std::numeric_limits<int32_t>::min();
static constexpr auto kMax32 = std::numeric_limits<int32_t>::max();
static constexpr auto kMin64 = std::numeric_limits<int64_t>::min();
static constexpr auto kMax64 = std::numeric_limits<int64_t>::max();
static constexpr int kMaxBits = std::numeric_limits<uint64_t>::digits;

class BitwiseTest : public functions::test::FunctionBaseTest {
 protected:
  template <typename T>
  std::optional<int64_t> bitwiseFunction(
      const std::string& fn,
      std::optional<T> a,
      std::optional<T> b) {
    return evaluateOnce<int64_t>(fmt::format("{}(c0, c1)", fn), a, b);
  }

  template <typename T>
  std::optional<int64_t> bitwiseAnd(std::optional<T> a, std::optional<T> b) {
    return evaluateOnce<int64_t>("bitwise_and(c0, c1)", a, b);
  }

  template <typename T>
  std::optional<int64_t> bitwiseOr(std::optional<T> a, std::optional<T> b) {
    return evaluateOnce<int64_t>("bitwise_or(c0, c1)", a, b);
  }

  template <typename T>
  std::optional<int64_t> bitwiseXor(std::optional<T> a, std::optional<T> b) {
    return evaluateOnce<int64_t>("bitwise_xor(c0, c1)", a, b);
  }

  std::optional<int64_t> bitCount(
      std::optional<int64_t> num,
      std::optional<int64_t> bits) {
    return evaluateOnce<int64_t>("bit_count(c0, c1)", num, bits);
  }

  template <typename T>
  std::optional<int64_t> bitwiseArithmeticShiftRight(
      std::optional<T> a,
      std::optional<T> b) {
    return evaluateOnce<int64_t>(
        "bitwise_arithmetic_shift_right(c0, c1)", a, b);
  }

  template <typename T>
  std::optional<int64_t> bitwiseRightShiftArithmetic(
      std::optional<T> a,
      std::optional<T> b) {
    return evaluateOnce<int64_t>(
        "bitwise_right_shift_arithmetic(c0, c1)", a, b);
  }

  template <typename T>
  std::optional<int64_t> bitwiseLeftShift(
      std::optional<T> a,
      std::optional<T> b) {
    return evaluateOnce<int64_t>("bitwise_left_shift(c0, c1)", a, b);
  }

  template <typename T>
  std::optional<int64_t> bitwiseRightShift(
      std::optional<T> a,
      std::optional<T> b) {
    return evaluateOnce<int64_t>("bitwise_right_shift(c0, c1)", a, b);
  }
};

TEST_F(BitwiseTest, bitwiseAnd) {
  EXPECT_EQ(bitwiseAnd<int32_t>(0, -1), 0);
  EXPECT_EQ(bitwiseAnd<int32_t>(3, 8), 0);
  EXPECT_EQ(bitwiseAnd<int32_t>(-4, 12), 12);
  EXPECT_EQ(bitwiseAnd<int32_t>(60, 21), 20);

  EXPECT_EQ(bitwiseAnd<int16_t>(kMin16, kMax16), 0);
  EXPECT_EQ(bitwiseAnd<int16_t>(kMax16, kMax16), kMax16);
  EXPECT_EQ(bitwiseAnd<int16_t>(kMax16, kMin16), 0);
  EXPECT_EQ(bitwiseAnd<int16_t>(kMin16, kMin16), kMin16);
  EXPECT_EQ(bitwiseAnd<int16_t>(kMax16, 1), 1);
  EXPECT_EQ(bitwiseAnd<int16_t>(kMax16, -1), kMax16);
  EXPECT_EQ(bitwiseAnd<int16_t>(kMin16, 1), 0);
  EXPECT_EQ(bitwiseAnd<int16_t>(kMin16, -1), kMin16);

  EXPECT_EQ(bitwiseAnd<int32_t>(kMin32, kMax32), 0);
  EXPECT_EQ(bitwiseAnd<int32_t>(kMax32, kMax32), kMax32);
  EXPECT_EQ(bitwiseAnd<int32_t>(kMax32, kMin32), 0);
  EXPECT_EQ(bitwiseAnd<int32_t>(kMin32, kMin32), kMin32);
  EXPECT_EQ(bitwiseAnd<int32_t>(kMax32, 1), 1);
  EXPECT_EQ(bitwiseAnd<int32_t>(kMax32, -1), kMax32);
  EXPECT_EQ(bitwiseAnd<int32_t>(kMin32, 1), 0);
  EXPECT_EQ(bitwiseAnd<int32_t>(kMin32, -1), kMin32);

  EXPECT_EQ(bitwiseAnd<int64_t>(kMin64, kMax64), 0);
  EXPECT_EQ(bitwiseAnd<int64_t>(kMax64, kMax64), kMax64);
  EXPECT_EQ(bitwiseAnd<int64_t>(kMax64, kMin64), 0);
  EXPECT_EQ(bitwiseAnd<int64_t>(kMin64, kMin64), kMin64);
  EXPECT_EQ(bitwiseAnd<int64_t>(kMax64, 1), 1);
  EXPECT_EQ(bitwiseAnd<int64_t>(kMax64, -1), kMax64);
  EXPECT_EQ(bitwiseAnd<int64_t>(kMin64, 1), 0);
  EXPECT_EQ(bitwiseAnd<int64_t>(kMin64, -1), kMin64);
}

TEST_F(BitwiseTest, bitCount) {
  EXPECT_EQ(bitCount(9, 8), 2);
  EXPECT_EQ(bitCount(-7, kMaxBits), 62);
  EXPECT_EQ(bitCount(9, kMaxBits), 2);
  EXPECT_EQ(bitCount(-7, 8), 6);
  EXPECT_EQ(bitCount(kMin64, kMaxBits), 1);
  EXPECT_EQ(bitCount(kMax64, kMaxBits), 63);
  EXPECT_EQ(bitCount(-2, 2), 1);

  auto assertInvalidBits = [this](int64_t num, int64_t bits) {
    assertUserError(
        [&]() { this->bitCount(num, bits); },
        fmt::format(
            "Bits specified in bit_count "
            "must be between "
            "2 and 64, got {}",
            bits));
  };

  auto assertNumberTooLarge = [this](int64_t num, int64_t bits) {
    assertUserError(
        [&]() { this->bitCount(num, bits); },
        fmt::format(
            "Number must be representable "
            "with the bits specified. "
            "{} can not be "
            "represented with {} bits",
            num,
            bits));
  };
  assertNumberTooLarge(7, 2);
  assertNumberTooLarge(3, 2);
  assertNumberTooLarge(2, 2);
  assertNumberTooLarge(kMax64, 63);
  assertNumberTooLarge(kMin64, 63);
  assertNumberTooLarge(-64, 6);
  assertNumberTooLarge(64, 6);
  assertInvalidBits(7, 1);
  assertInvalidBits(7, 65);
}

TEST_F(BitwiseTest, bitwiseNot) {
  const auto bitwiseNot = [&](std::optional<int32_t> a) {
    return evaluateOnce<int64_t>("bitwise_not(c0)", a);
  };

  EXPECT_EQ(bitwiseNot(-1), 0);
  EXPECT_EQ(bitwiseNot(0), -1);
  EXPECT_EQ(bitwiseNot(2), -3);
  EXPECT_EQ(bitwiseNot(kMax32), kMin32);
  EXPECT_EQ(bitwiseNot(kMin32), kMax32);
  EXPECT_EQ(bitwiseNot(kMax16), kMin16);
  EXPECT_EQ(bitwiseNot(kMin16), kMax16);
}

TEST_F(BitwiseTest, bitwiseOr) {
  EXPECT_EQ(bitwiseOr<int32_t>(0, -1), -1);
  EXPECT_EQ(bitwiseOr<int32_t>(3, 8), 11);
  EXPECT_EQ(bitwiseOr<int32_t>(-4, 12), -4);
  EXPECT_EQ(bitwiseOr<int32_t>(60, 21), 61);

  EXPECT_EQ(bitwiseOr<int16_t>(kMin16, kMax16), -1);
  EXPECT_EQ(bitwiseOr<int16_t>(kMax16, kMax16), kMax16);
  EXPECT_EQ(bitwiseOr<int16_t>(kMax16, kMin16), -1);
  EXPECT_EQ(bitwiseOr<int16_t>(kMin16, kMin16), kMin16);
  EXPECT_EQ(bitwiseOr<int16_t>(kMax16, 1), kMax16);
  EXPECT_EQ(bitwiseOr<int16_t>(kMax16, -1), -1);
  EXPECT_EQ(bitwiseOr<int16_t>(kMin16, 1), kMin16 + 1);
  EXPECT_EQ(bitwiseOr<int16_t>(kMin16, -1), -1);

  EXPECT_EQ(bitwiseOr<int32_t>(kMin32, kMax32), -1);
  EXPECT_EQ(bitwiseOr<int32_t>(kMax32, kMax32), kMax32);
  EXPECT_EQ(bitwiseOr<int32_t>(kMax32, kMin32), -1);
  EXPECT_EQ(bitwiseOr<int32_t>(kMin32, kMin32), kMin32);
  EXPECT_EQ(bitwiseOr<int32_t>(kMax32, 1), kMax32);
  EXPECT_EQ(bitwiseOr<int32_t>(kMax32, -1), -1);
  EXPECT_EQ(bitwiseOr<int32_t>(kMin32, 1), kMin32 + 1);
  EXPECT_EQ(bitwiseOr<int32_t>(kMin32, -1), -1);

  EXPECT_EQ(bitwiseOr<int64_t>(kMin64, kMax64), -1);
  EXPECT_EQ(bitwiseOr<int64_t>(kMax64, kMax64), kMax64);
  EXPECT_EQ(bitwiseOr<int64_t>(kMax64, kMin64), -1);
  EXPECT_EQ(bitwiseOr<int64_t>(kMin64, kMin64), kMin64);
  EXPECT_EQ(bitwiseOr<int64_t>(kMax64, 1), kMax64);
  EXPECT_EQ(bitwiseOr<int64_t>(kMax64, -1), -1);
  EXPECT_EQ(bitwiseOr<int64_t>(kMin64, 1), kMin64 + 1);
  EXPECT_EQ(bitwiseOr<int64_t>(kMin64, -1), -1);
}

TEST_F(BitwiseTest, bitwiseXor) {
  EXPECT_EQ(bitwiseXor<int32_t>(0, -1), -1);
  EXPECT_EQ(bitwiseXor<int32_t>(3, 8), 11);
  EXPECT_EQ(bitwiseXor<int32_t>(-4, 12), -16);
  EXPECT_EQ(bitwiseXor<int32_t>(60, 21), 41);

  EXPECT_EQ(bitwiseXor<int16_t>(kMin16, kMax16), -1);
  EXPECT_EQ(bitwiseXor<int16_t>(kMax16, kMax16), 0);
  EXPECT_EQ(bitwiseXor<int16_t>(kMax16, kMin16), -1);
  EXPECT_EQ(bitwiseXor<int16_t>(kMin16, kMin16), 0);
  EXPECT_EQ(bitwiseXor<int16_t>(kMax16, 1), kMax16 - 1);
  EXPECT_EQ(bitwiseXor<int16_t>(kMax16, -1), kMin16);
  EXPECT_EQ(bitwiseXor<int16_t>(kMin16, 1), kMin16 + 1);
  EXPECT_EQ(bitwiseXor<int16_t>(kMin16, -1), kMax16);

  EXPECT_EQ(bitwiseXor<int32_t>(kMin32, kMax32), -1);
  EXPECT_EQ(bitwiseXor<int32_t>(kMax32, kMax32), 0);
  EXPECT_EQ(bitwiseXor<int32_t>(kMax32, kMin32), -1);
  EXPECT_EQ(bitwiseXor<int32_t>(kMin32, kMin32), 0);
  EXPECT_EQ(bitwiseXor<int32_t>(kMax32, 1), kMax32 - 1);
  EXPECT_EQ(bitwiseXor<int32_t>(kMax32, -1), kMin32);
  EXPECT_EQ(bitwiseXor<int32_t>(kMin32, 1), kMin32 + 1);
  EXPECT_EQ(bitwiseXor<int32_t>(kMin32, -1), kMax32);

  EXPECT_EQ(bitwiseXor<int64_t>(kMin64, kMax64), -1);
  EXPECT_EQ(bitwiseXor<int64_t>(kMax64, kMax64), 0);
  EXPECT_EQ(bitwiseXor<int64_t>(kMax64, kMin64), -1);
  EXPECT_EQ(bitwiseXor<int64_t>(kMin64, kMin64), 0);
  EXPECT_EQ(bitwiseXor<int64_t>(kMax64, 1), kMax64 - 1);
  EXPECT_EQ(bitwiseXor<int64_t>(kMax64, -1), kMin64);
  EXPECT_EQ(bitwiseXor<int64_t>(kMin64, 1), kMin64 + 1);
  EXPECT_EQ(bitwiseXor<int64_t>(kMin64, -1), kMax64);
}

TEST_F(BitwiseTest, arithmeticShiftRight) {
  EXPECT_EQ(bitwiseArithmeticShiftRight<int32_t>(1, 1), 0);
  EXPECT_EQ(bitwiseArithmeticShiftRight<int32_t>(3, 1), 1);
  EXPECT_EQ(bitwiseArithmeticShiftRight<int32_t>(-3, 1), -2);
  EXPECT_EQ(bitwiseArithmeticShiftRight<int32_t>(3, 0), 3);
  EXPECT_EQ(bitwiseArithmeticShiftRight<int32_t>(3, 3), 0);
  EXPECT_EQ(bitwiseArithmeticShiftRight<int32_t>(-1, 2), -1);
  EXPECT_EQ(bitwiseArithmeticShiftRight<int32_t>(-1, 2), -1);
  EXPECT_EQ(bitwiseArithmeticShiftRight<int32_t>(-100, 65), -50);
  EXPECT_EQ(bitwiseArithmeticShiftRight<int32_t>(-100, 66), -25);

  assertUserError(
      [&]() { bitwiseArithmeticShiftRight<int32_t>(3, -1); },
      "Shift must be positive");

  EXPECT_EQ(bitwiseArithmeticShiftRight<int16_t>(kMin16, kMax16), -1);
  EXPECT_EQ(bitwiseArithmeticShiftRight<int16_t>(kMax16, kMax16), 0);
  EXPECT_EQ(bitwiseArithmeticShiftRight<int16_t>(kMax16, 1), kMax16 >> 1);
  EXPECT_EQ(bitwiseArithmeticShiftRight<int16_t>(kMin16, 1), kMin16 >> 1);

  EXPECT_EQ(bitwiseArithmeticShiftRight<int32_t>(kMin32, kMax32), -1);
  EXPECT_EQ(bitwiseArithmeticShiftRight<int32_t>(kMax32, kMax32), 0);
  EXPECT_EQ(bitwiseArithmeticShiftRight<int32_t>(kMax32, 1), kMax32 >> 1);
  EXPECT_EQ(bitwiseArithmeticShiftRight<int32_t>(kMin32, 1), kMin32 >> 1);

  EXPECT_EQ(bitwiseArithmeticShiftRight<int64_t>(kMin64, kMax64), -1);
  EXPECT_EQ(bitwiseArithmeticShiftRight<int64_t>(kMax64, kMax64), 0);
  EXPECT_EQ(bitwiseArithmeticShiftRight<int64_t>(kMax64, 1), kMax64 >> 1);
  EXPECT_EQ(bitwiseArithmeticShiftRight<int64_t>(kMin64, 1), kMin64 >> 1);
}

TEST_F(BitwiseTest, rightShiftArithmetic) {
  EXPECT_EQ(bitwiseRightShiftArithmetic<int32_t>(1, 1), 0);
  EXPECT_EQ(bitwiseRightShiftArithmetic<int32_t>(3, 1), 1);
  EXPECT_EQ(bitwiseRightShiftArithmetic<int32_t>(-3, 1), -2);
  EXPECT_EQ(bitwiseRightShiftArithmetic<int32_t>(3, 0), 3);
  EXPECT_EQ(bitwiseRightShiftArithmetic<int32_t>(3, 3), 0);
  EXPECT_EQ(bitwiseRightShiftArithmetic<int32_t>(-1, 2), -1);
  EXPECT_EQ(bitwiseRightShiftArithmetic<int32_t>(-1, 65), -1);

  EXPECT_EQ(bitwiseRightShiftArithmetic<int16_t>(kMin16, kMax16), -1);
  EXPECT_EQ(bitwiseRightShiftArithmetic<int16_t>(kMax16, kMax16), 0);
  EXPECT_EQ(bitwiseRightShiftArithmetic<int16_t>(kMax16, 1), kMax16 >> 1);
  EXPECT_EQ(bitwiseRightShiftArithmetic<int16_t>(kMin16, 1), kMin16 >> 1);

  EXPECT_EQ(bitwiseRightShiftArithmetic<int32_t>(kMin32, kMax32), -1);
  EXPECT_EQ(bitwiseRightShiftArithmetic<int32_t>(kMax32, kMax32), 0);
  EXPECT_EQ(bitwiseRightShiftArithmetic<int32_t>(kMax32, 1), kMax32 >> 1);
  EXPECT_EQ(bitwiseRightShiftArithmetic<int32_t>(kMin32, 1), kMin32 >> 1);

  EXPECT_EQ(bitwiseRightShiftArithmetic<int64_t>(kMin64, kMax64), -1);
  EXPECT_EQ(bitwiseRightShiftArithmetic<int64_t>(kMax64, kMax64), 0);
  EXPECT_EQ(bitwiseRightShiftArithmetic<int64_t>(kMax64, 1), kMax64 >> 1);
  EXPECT_EQ(bitwiseRightShiftArithmetic<int64_t>(kMin64, 1), kMin64 >> 1);
  EXPECT_EQ(bitwiseRightShiftArithmetic<int64_t>(1, kMin64), 0);
}

TEST_F(BitwiseTest, leftShift) {
  EXPECT_EQ(bitwiseLeftShift<int32_t>(1, 1), 2);
  EXPECT_EQ(bitwiseLeftShift<int32_t>(-1, 1), -2);
  EXPECT_EQ(bitwiseLeftShift<int32_t>(-1, 3), -8);
  EXPECT_EQ(bitwiseLeftShift<int32_t>(-1, 33), 0);
  EXPECT_EQ(bitwiseLeftShift<int16_t>(-1, 33), 0);
  EXPECT_EQ(bitwiseLeftShift<int16_t>(1, 33), 0);
  EXPECT_EQ(bitwiseLeftShift<int64_t>(-1, 33), -8589934592);
  EXPECT_EQ(bitwiseLeftShift<int32_t>(524287, 3), 4194296);
  EXPECT_EQ(bitwiseLeftShift<int64_t>(-1, -1), 0);

  EXPECT_EQ(bitwiseLeftShift<int16_t>(kMin16, kMax16), 0);
  EXPECT_EQ(bitwiseLeftShift<int16_t>(kMax16, kMax16), 0);
  EXPECT_EQ(bitwiseLeftShift<int16_t>(kMax16, 1), kMax16 << 1);
  EXPECT_EQ(bitwiseLeftShift<int16_t>(kMin16, 1), -65536);

  EXPECT_EQ(bitwiseLeftShift<int32_t>(kMin32, kMax32), 0);
  EXPECT_EQ(bitwiseLeftShift<int32_t>(kMax32, kMax32), 0);
  EXPECT_EQ(bitwiseLeftShift<int32_t>(kMax32, 1), kMax32 << 1);
  EXPECT_EQ(bitwiseLeftShift<int32_t>(kMin32, 1), 0);

  EXPECT_EQ(bitwiseLeftShift<int64_t>(kMin64, kMax64), 0);
  EXPECT_EQ(bitwiseLeftShift<int64_t>(kMax64, kMax64), 0);
  EXPECT_EQ(bitwiseLeftShift<int64_t>(kMax64, 1), kMax64 << 1);
  EXPECT_EQ(bitwiseLeftShift<int64_t>(kMin64, 1), 0);
}

TEST_F(BitwiseTest, rightShift) {
  EXPECT_EQ(bitwiseRightShift<int64_t>(1, 1), 0);
  EXPECT_EQ(bitwiseRightShift<int32_t>(-3, 1), 2147483646);
  EXPECT_EQ(bitwiseRightShift<int32_t>(-1, 32), 0);
  EXPECT_EQ(bitwiseRightShift<int32_t>(-1, -1), 0);

  EXPECT_EQ(bitwiseRightShift<int16_t>(kMin16, kMax16), 0);
  EXPECT_EQ(bitwiseRightShift<int16_t>(kMax16, kMax16), 0);
  EXPECT_EQ(bitwiseRightShift<int16_t>(kMax16, 1), kMax16 >> 1);
  EXPECT_EQ(bitwiseRightShift<int16_t>(kMin16, 1), (kMax16 >> 1) + 1);

  EXPECT_EQ(bitwiseRightShift<int32_t>(kMin32, kMax32), 0);
  EXPECT_EQ(bitwiseRightShift<int32_t>(kMax32, kMax32), 0);
  EXPECT_EQ(bitwiseRightShift<int32_t>(kMax32, 1), kMax32 >> 1);
  EXPECT_EQ(bitwiseRightShift<int32_t>(kMin32, 1), (kMax32 >> 1) + 1);

  EXPECT_EQ(bitwiseRightShift<int64_t>(kMin64, kMax64), 0);
  EXPECT_EQ(bitwiseRightShift<int64_t>(kMax64, kMax64), 0);
  EXPECT_EQ(bitwiseRightShift<int64_t>(kMax64, 1), kMax64 >> 1);
  EXPECT_EQ(bitwiseRightShift<int64_t>(kMin64, 1), (kMax64 >> 1) + 1);
}

TEST_F(BitwiseTest, logicalShiftRight) {
  const auto bitwiseLogicalShiftRight = [&](std::optional<int64_t> number,
                                            std::optional<int64_t> shift,
                                            std::optional<int64_t> bits) {
    return evaluateOnce<int64_t>(
        "bitwise_logical_shift_right(c0, c1, c2)", number, shift, bits);
  };

  EXPECT_EQ(bitwiseLogicalShiftRight(1, 1, 64), 0);
  EXPECT_EQ(bitwiseLogicalShiftRight(-1, 1, 2), 1);
  EXPECT_EQ(bitwiseLogicalShiftRight(-1, 32, 32), 0);
  EXPECT_EQ(bitwiseLogicalShiftRight(-1, 30, 32), 3);
  EXPECT_EQ(bitwiseLogicalShiftRight(kMin64, 10, 32), 0);
  EXPECT_EQ(bitwiseLogicalShiftRight(kMin64, kMax64, 64), -1);
  EXPECT_EQ(bitwiseLogicalShiftRight(kMax64, kMin64, 64), kMax64);

  assertUserError(
      [&]() { bitwiseLogicalShiftRight(3, -1, 3); }, "Shift must be positive");
  assertUserError(
      [&]() { bitwiseLogicalShiftRight(3, 1, 1); },
      "Bits must be between 2 and 64");
}

TEST_F(BitwiseTest, shiftLeft) {
  const auto bitwiseLogicalShiftRight = [&](std::optional<int64_t> number,
                                            std::optional<int64_t> shift,
                                            std::optional<int64_t> bits) {
    return evaluateOnce<int64_t>(
        "bitwise_shift_left(c0, c1, c2)", number, shift, bits);
  };

  EXPECT_EQ(bitwiseLogicalShiftRight(1, 1, 64), 0);
  EXPECT_EQ(bitwiseLogicalShiftRight(-1, 1, 2), 2);
  EXPECT_EQ(bitwiseLogicalShiftRight(-1, 32, 32), 0);
  EXPECT_EQ(bitwiseLogicalShiftRight(-1, 31, 32), 2147483648);
  EXPECT_EQ(bitwiseLogicalShiftRight(kMin64, 10, 32), 0);
  EXPECT_EQ(bitwiseLogicalShiftRight(kMin64, kMax64, 64), -1);
  EXPECT_EQ(bitwiseLogicalShiftRight(kMax64, kMin64, 64), kMax64);

  assertUserError(
      [&]() { bitwiseLogicalShiftRight(3, -1, 3); }, "Shift must be positive");
  assertUserError(
      [&]() { bitwiseLogicalShiftRight(3, 1, 1); },
      "Bits must be between 2 and 64");
}

} // namespace
} // namespace facebook::velox
