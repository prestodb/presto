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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

namespace facebook::velox::functions::sparksql::test {
using namespace facebook::velox::test;

namespace {

static constexpr auto kMin8 = std::numeric_limits<int8_t>::min();
static constexpr auto kMax8 = std::numeric_limits<int8_t>::max();
static constexpr auto kMin16 = std::numeric_limits<int16_t>::min();
static constexpr auto kMax16 = std::numeric_limits<int16_t>::max();
static constexpr auto kMin32 = std::numeric_limits<int32_t>::min();
static constexpr auto kMax32 = std::numeric_limits<int32_t>::max();
static constexpr auto kMin64 = std::numeric_limits<int64_t>::min();
static constexpr auto kMax64 = std::numeric_limits<int64_t>::max();

class BitwiseTest : public SparkFunctionBaseTest {
 protected:
  template <typename T>
  std::optional<T> bitwiseAnd(std::optional<T> a, std::optional<T> b) {
    return evaluateOnce<T>("bitwise_and(c0, c1)", a, b);
  }

  template <typename T>
  std::optional<T> bitwiseNot(std::optional<T> a) {
    return evaluateOnce<T>("bitwise_not(c0)", a);
  }

  template <typename T>
  std::optional<T> bitwiseOr(std::optional<T> a, std::optional<T> b) {
    return evaluateOnce<T>("bitwise_or(c0, c1)", a, b);
  }

  template <typename T>
  std::optional<T> bitwiseXor(std::optional<T> a, std::optional<T> b) {
    return evaluateOnce<T>("bitwise_xor(c0, c1)", a, b);
  }

  template <typename T>
  std::optional<int32_t> bitCount(std::optional<T> a) {
    return evaluateOnce<int32_t>("bit_count(c0)", a);
  }

  template <typename T>
  std::optional<int8_t> bitGet(std::optional<T> a, std::optional<int32_t> b) {
    return evaluateOnce<int8_t>("bit_get(c0, c1)", a, b);
  }

  template <typename T>
  std::optional<T> shiftLeft(std::optional<T> a, std::optional<T> b) {
    return evaluateOnce<T>("shiftleft(c0, c1)", a, b);
  }

  template <typename T>
  std::optional<T> shiftRight(std::optional<T> a, std::optional<T> b) {
    return evaluateOnce<T>("shiftright(c0, c1)", a, b);
  }

  template <typename T1, typename T2>
  std::optional<T1> shiftLeft_twoTypes(
      std::optional<T1> a,
      std::optional<T2> b) {
    return evaluateOnce<T1>("shiftleft(c0, c1)", a, b);
  }

  template <typename T1, typename T2>
  std::optional<T1> shiftRight_twoTypes(
      std::optional<T1> a,
      std::optional<T2> b) {
    return evaluateOnce<T1>("shiftright(c0, c1)", a, b);
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

TEST_F(BitwiseTest, bitwiseNot) {
  EXPECT_EQ(bitwiseNot<int32_t>(0), -1);
  EXPECT_EQ(bitwiseNot<int32_t>(3), -4);
  EXPECT_EQ(bitwiseNot<int32_t>(-4), 3);
  EXPECT_EQ(bitwiseNot<int32_t>(60), -61);

  EXPECT_EQ(bitwiseNot<int8_t>(kMin8), kMax8);
  EXPECT_EQ(bitwiseNot<int8_t>(kMax8), kMin8);

  EXPECT_EQ(bitwiseNot<int16_t>(kMin16), kMax16);
  EXPECT_EQ(bitwiseNot<int16_t>(kMax16), kMin16);

  EXPECT_EQ(bitwiseNot<int32_t>(kMin32), kMax32);
  EXPECT_EQ(bitwiseNot<int32_t>(kMax32), kMin32);

  EXPECT_EQ(bitwiseNot<int64_t>(kMin64), kMax64);
  EXPECT_EQ(bitwiseNot<int64_t>(kMax64), kMin64);
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

  EXPECT_EQ(bitwiseXor<int8_t>(kMin8, kMax8), -1);
  EXPECT_EQ(bitwiseXor<int8_t>(kMax8, kMax8), 0);
  EXPECT_EQ(bitwiseXor<int8_t>(kMax8, kMin8), -1);
  EXPECT_EQ(bitwiseXor<int8_t>(kMin8, kMin8), 0);
  EXPECT_EQ(bitwiseXor<int8_t>(kMax8, 1), kMax8 - 1);
  EXPECT_EQ(bitwiseXor<int8_t>(kMax8, -1), kMin8);
  EXPECT_EQ(bitwiseXor<int8_t>(kMin8, 1), kMin8 + 1);
  EXPECT_EQ(bitwiseXor<int8_t>(kMin8, -1), kMax8);

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

TEST_F(BitwiseTest, bitCount) {
  EXPECT_EQ(bitCount<int8_t>(std::nullopt), std::nullopt);
  EXPECT_EQ(bitCount<bool>(1), 1);
  EXPECT_EQ(bitCount<bool>(false), 0);
  EXPECT_EQ(bitCount<int8_t>(kMin8), 1);
  EXPECT_EQ(bitCount<int8_t>(kMax8), 7);
  EXPECT_EQ(bitCount<int16_t>(kMin16), 1);
  EXPECT_EQ(bitCount<int16_t>(kMax16), 15);
  EXPECT_EQ(bitCount<int32_t>(kMin32), 1);
  EXPECT_EQ(bitCount<int32_t>(kMax32), 31);
  EXPECT_EQ(bitCount<int64_t>(kMin64), 1);
  EXPECT_EQ(bitCount<int64_t>(kMax64), 63);
}

TEST_F(BitwiseTest, bitGet) {
  EXPECT_EQ(bitGet<int8_t>(std::nullopt, 1), std::nullopt);
  EXPECT_EQ(bitGet<int8_t>(kMin8, 0), 0);
  EXPECT_EQ(bitGet<int8_t>(kMin8, 7), 1);
  EXPECT_EQ(bitGet<int8_t>(kMax8, 0), 1);
  EXPECT_EQ(bitGet<int8_t>(kMax8, 7), 0);
  VELOX_ASSERT_THROW(
      bitGet<int8_t>(kMax8, -1),
      "The value of 'pos' argument must be greater than or equal to zero.");
  VELOX_ASSERT_THROW(
      bitGet<int8_t>(kMax8, 8),
      "The value of 'pos' argument must not exceed the number of bits in 'x' - 1.");

  EXPECT_EQ(bitGet<int16_t>(kMin16, 0), 0);
  EXPECT_EQ(bitGet<int16_t>(kMin16, 15), 1);
  EXPECT_EQ(bitGet<int16_t>(kMax16, 0), 1);
  EXPECT_EQ(bitGet<int16_t>(kMax16, 15), 0);
  VELOX_ASSERT_THROW(
      bitGet<int16_t>(kMax16, -1),
      "The value of 'pos' argument must be greater than or equal to zero.");
  VELOX_ASSERT_THROW(
      bitGet<int16_t>(kMax16, 16),
      "The value of 'pos' argument must not exceed the number of bits in 'x' - 1.");

  EXPECT_EQ(bitGet<int32_t>(kMin32, 0), 0);
  EXPECT_EQ(bitGet<int32_t>(kMin32, 31), 1);
  EXPECT_EQ(bitGet<int32_t>(kMax32, 0), 1);
  EXPECT_EQ(bitGet<int32_t>(kMax32, 31), 0);
  VELOX_ASSERT_THROW(
      bitGet<int32_t>(kMax32, -1),
      "The value of 'pos' argument must be greater than or equal to zero.");
  VELOX_ASSERT_THROW(
      bitGet<int32_t>(kMax32, 32),
      "The value of 'pos' argument must not exceed the number of bits in 'x' - 1.");

  EXPECT_EQ(bitGet<int64_t>(kMin64, 0), 0);
  EXPECT_EQ(bitGet<int64_t>(kMin64, 63), 1);
  EXPECT_EQ(bitGet<int64_t>(kMax64, 0), 1);
  EXPECT_EQ(bitGet<int64_t>(kMax64, 63), 0);
  VELOX_ASSERT_THROW(
      bitGet<int64_t>(kMax64, -1),
      "The value of 'pos' argument must be greater than or equal to zero.");
  VELOX_ASSERT_THROW(
      bitGet<int64_t>(kMax64, 64),
      "The value of 'pos' argument must not exceed the number of bits in 'x' - 1.");
}

TEST_F(BitwiseTest, shiftLeft) {
  EXPECT_EQ(shiftLeft<int32_t>(1, 1), 2);
  EXPECT_EQ(shiftLeft<int32_t>(-1, 1), -2);
  EXPECT_EQ(shiftLeft<int32_t>(-1, 3), -8);
  EXPECT_EQ(shiftLeft<int32_t>(-1, 33), -2);
  EXPECT_EQ(shiftLeft<int32_t>(524287, 3), 4194296);

  EXPECT_EQ(shiftLeft<int32_t>(kMin32, kMax32), 0);
  EXPECT_EQ(shiftLeft<int32_t>(kMax32, kMax32), -2147483648);
  EXPECT_EQ(shiftLeft<int32_t>(kMax32, 1), kMax32 << 1);
  EXPECT_EQ(shiftLeft<int32_t>(kMin32, 1), 0);

  EXPECT_EQ((shiftLeft_twoTypes<int64_t, int32_t>(kMin64, kMax64)), 0);
  EXPECT_EQ((shiftLeft_twoTypes<int64_t, int32_t>(kMax64, 1)), kMax64 << 1);
  EXPECT_EQ((shiftLeft_twoTypes<int64_t, int32_t>(kMin64, 1)), 0);
}

TEST_F(BitwiseTest, shiftRight) {
  EXPECT_EQ(shiftRight<int32_t>(-3, 1), -2);
  EXPECT_EQ(shiftRight<int32_t>(-1, 32), -1);
  EXPECT_EQ(shiftRight<int32_t>(-1, -1), -1);

  EXPECT_EQ(shiftRight<int32_t>(kMin32, kMax32), -1);
  EXPECT_EQ(shiftRight<int32_t>(kMax32, kMax32), 0);
  EXPECT_EQ(shiftRight<int32_t>(kMax32, 1), kMax32 >> 1);
  EXPECT_EQ(shiftRight<int32_t>(kMin32, 1), -1073741824);

  EXPECT_EQ((shiftRight_twoTypes<int64_t, int32_t>(kMin64, kMax64)), -1);
  EXPECT_EQ((shiftRight_twoTypes<int64_t, int32_t>(kMax64, kMax64)), 0);
  EXPECT_EQ((shiftRight_twoTypes<int64_t, int32_t>(kMax64, 1)), kMax64 >> 1);
  EXPECT_EQ(
      (shiftRight_twoTypes<int64_t, int32_t>(kMin64, 1)), -4611686018427387904);
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
