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

#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/type/DecimalUtil.h"

namespace facebook::velox {

namespace {

TEST(DecimalTest, toString) {
  EXPECT_EQ(std::to_string(buildInt128(0, 0)), "0");
  EXPECT_EQ(std::to_string(buildInt128(0, 1)), "1");
  EXPECT_EQ(
      std::to_string(buildInt128(0xFFFFFFFFFFFFFFFFull, 0xFFFFFFFFFFFFFFFFull)),
      "-1");
  EXPECT_EQ(std::to_string(buildInt128(1, 0)), "18446744073709551616");
  EXPECT_EQ(
      std::to_string(buildInt128(0xFFFFFFFFFFFFFFFFull, 0)),
      "-18446744073709551616");
  constexpr int128_t kMax =
      buildInt128(0x7FFFFFFFFFFFFFFFull, 0xFFFFFFFFFFFFFFFFull);
  EXPECT_EQ(std::to_string(kMax), "170141183460469231731687303715884105727");
  EXPECT_EQ(
      std::to_string(-kMax - 1), "-170141183460469231731687303715884105728");
}

TEST(DecimalTest, decimalToString) {
  ASSERT_EQ(
      "1000",
      DecimalUtil::toString(UnscaledShortDecimal(1000), DECIMAL(10, 0)));
  ASSERT_EQ(
      "1.000",
      DecimalUtil::toString(UnscaledShortDecimal(1000), DECIMAL(10, 3)));
  ASSERT_EQ(
      "0.001000",
      DecimalUtil::toString(UnscaledShortDecimal(1000), DECIMAL(10, 6)));
  ASSERT_EQ(
      "-0.001000",
      DecimalUtil::toString(UnscaledShortDecimal(-1000), DECIMAL(10, 6)));
  ASSERT_EQ(
      "-123.451000",
      DecimalUtil::toString(UnscaledShortDecimal(-123451000), DECIMAL(10, 6)));

  ASSERT_EQ(
      "1000", DecimalUtil::toString(UnscaledLongDecimal(1000), DECIMAL(20, 0)));
  ASSERT_EQ(
      "1.000",
      DecimalUtil::toString(UnscaledLongDecimal(1000), DECIMAL(20, 3)));
  ASSERT_EQ(
      "0.0000001000",
      DecimalUtil::toString(UnscaledLongDecimal(1000), DECIMAL(20, 10)));
  ASSERT_EQ(
      "-0.001000",
      DecimalUtil::toString(UnscaledLongDecimal(-1000), DECIMAL(20, 6)));
  ASSERT_EQ("0", DecimalUtil::toString(UnscaledLongDecimal(0), DECIMAL(20, 9)));

  const auto minShortDecimal = DecimalUtil::toString(
      std::numeric_limits<UnscaledShortDecimal>::min(), DECIMAL(18, 0));
  ASSERT_EQ("-999999999999999999", minShortDecimal);
  // Additional 1 for negative sign.
  ASSERT_EQ(minShortDecimal.length(), 19);

  const auto maxShortDecimal = DecimalUtil::toString(
      std::numeric_limits<UnscaledShortDecimal>::max(), DECIMAL(18, 0));
  ASSERT_EQ("999999999999999999", maxShortDecimal);
  ASSERT_EQ(maxShortDecimal.length(), 18);

  const auto minLongDecimal = DecimalUtil::toString(
      std::numeric_limits<UnscaledLongDecimal>::min(), DECIMAL(38, 0));
  ASSERT_EQ("-99999999999999999999999999999999999999", minLongDecimal);
  // Additional 1 for negative sign.
  ASSERT_EQ(minLongDecimal.length(), 39);

  const auto maxLongDecimal = DecimalUtil::toString(
      std::numeric_limits<UnscaledLongDecimal>::max(), DECIMAL(38, 0));
  ASSERT_EQ("99999999999999999999999999999999999999", maxLongDecimal);
  ASSERT_EQ(maxLongDecimal.length(), 38);
}

TEST(DecimalTest, overloads) {
  ASSERT_EQ(UnscaledShortDecimal(3), UnscaledShortDecimal(10) / 3);
  ASSERT_EQ(UnscaledLongDecimal(33), UnscaledLongDecimal(100) / 3);
  ASSERT_EQ(UnscaledLongDecimal(300), UnscaledLongDecimal(100) * 3);
}

DEBUG_ONLY_TEST(DecimalTest, limits) {
  VELOX_ASSERT_THROW(
      UnscaledShortDecimal::max() + UnscaledShortDecimal(1),
      "Value '1000000000000000000' is not in the range of ShortDecimal Type");
  VELOX_ASSERT_THROW(
      UnscaledShortDecimal::min() - UnscaledShortDecimal(1),
      "Value '-1000000000000000000' is not in the range of ShortDecimal Type");
  VELOX_ASSERT_THROW(
      UnscaledLongDecimal::max() + UnscaledLongDecimal(1),
      "Value '100000000000000000000000000000000000000' is not in the range of LongDecimal Type");
  VELOX_ASSERT_THROW(
      UnscaledLongDecimal::min() - UnscaledLongDecimal(1),
      "Value '-100000000000000000000000000000000000000' is not in the range of LongDecimal Type");
}

TEST(DecimalTest, addUnsignedValues) {
  int128_t a = -buildInt128(0x4B3B4CA85A86C47A, 0x98A223FFFFFFFFF);
  int128_t sum = a;
  int64_t overflow = 0;
  auto count = 1'000'000;
  // Test underflow
  for (int i = 1; i < count; ++i) {
    overflow += DecimalUtil::addWithOverflow(sum, a, sum);
  }
  ASSERT_EQ(-587747, overflow);
  ASSERT_EQ(UPPER(sum), 0xE98C20AD1C80DBEF);
  ASSERT_EQ(LOWER(sum), 0xFEE2F000000F4240);

  // Test overflow.
  overflow = 0;
  a = -a;
  sum = a;
  for (int i = 1; i < count; ++i) {
    overflow += DecimalUtil::addWithOverflow(sum, a, sum);
  }
  ASSERT_EQ(587747, overflow);
  ASSERT_EQ(UPPER(sum), 0x1673df52e37f2410);
  ASSERT_EQ(LOWER(sum), 0x11d0ffffff0bdc0);
}

TEST(DecimalTest, longDecimalSerDe) {
  char data[100];
  UnscaledLongDecimal::serialize(UnscaledLongDecimal::min(), data);
  auto deserializedData = UnscaledLongDecimal::deserialize(data);
  ASSERT_EQ(deserializedData, UnscaledLongDecimal::min());

  UnscaledLongDecimal::serialize(UnscaledLongDecimal::max(), data);
  deserializedData = UnscaledLongDecimal::deserialize(data);
  ASSERT_EQ(deserializedData, UnscaledLongDecimal::max());

  auto longData = UnscaledLongDecimal(-1);
  UnscaledLongDecimal::serialize(longData, data);
  deserializedData = UnscaledLongDecimal::deserialize(data);
  ASSERT_EQ(deserializedData, longData);

  longData = UnscaledLongDecimal(10);
  UnscaledLongDecimal::serialize(longData, data);
  deserializedData = UnscaledLongDecimal::deserialize(data);
  ASSERT_EQ(deserializedData, longData);
}
} // namespace
} // namespace facebook::velox
