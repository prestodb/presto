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

template <typename T>
void assertRescaleDouble(double value, const TypePtr& type, T expectedValue) {
  const auto [precision, scale] = getDecimalPrecisionScale(*type);
  T actualValue;
  const auto status =
      DecimalUtil::rescaleDouble<T>(value, precision, scale, actualValue);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ(actualValue, expectedValue);
}

template <typename T>
void assertRescaleDoubleFail(
    double value,
    const TypePtr& type,
    const std::string& expectedErrorMessage) {
  const auto [precision, scale] = getDecimalPrecisionScale(*type);
  T actualValue;
  VELOX_ASSERT_ERROR_STATUS(
      DecimalUtil::rescaleDouble<T>(value, precision, scale, actualValue),
      StatusCode::kUserError,
      expectedErrorMessage);
}

void testToByteArray(int128_t value, int8_t* expected, int32_t size) {
  char out[size];
  int32_t length = DecimalUtil::toByteArray(value, out);
  EXPECT_EQ(length, size);
  EXPECT_EQ(DecimalUtil::getByteArrayLength(value), size);
  EXPECT_EQ(std::memcmp(expected, out, length), 0);
}

TEST(DecimalTest, toString) {
  EXPECT_EQ(std::to_string(HugeInt::build(0, 0)), "0");
  EXPECT_EQ(std::to_string(HugeInt::build(0, 1)), "1");
  EXPECT_EQ(
      std::to_string(
          HugeInt::build(0xFFFFFFFFFFFFFFFFull, 0xFFFFFFFFFFFFFFFFull)),
      "-1");
  EXPECT_EQ(std::to_string(HugeInt::build(1, 0)), "18446744073709551616");
  EXPECT_EQ(
      std::to_string(HugeInt::build(0xFFFFFFFFFFFFFFFFull, 0)),
      "-18446744073709551616");
  constexpr int128_t kMax =
      HugeInt::build(0x7FFFFFFFFFFFFFFFull, 0xFFFFFFFFFFFFFFFFull);
  EXPECT_EQ(std::to_string(kMax), "170141183460469231731687303715884105727");
  EXPECT_EQ(
      std::to_string(-kMax - 1), "-170141183460469231731687303715884105728");
}

TEST(DecimalTest, decimalToString) {
  ASSERT_EQ("1000", DecimalUtil::toString(1000, DECIMAL(10, 0)));
  ASSERT_EQ("1.000", DecimalUtil::toString(1000, DECIMAL(10, 3)));
  ASSERT_EQ("0.001000", DecimalUtil::toString(1000, DECIMAL(10, 6)));
  ASSERT_EQ("-0.001000", DecimalUtil::toString(-1000, DECIMAL(10, 6)));
  ASSERT_EQ("-123.451000", DecimalUtil::toString(-123451000, DECIMAL(10, 6)));

  ASSERT_EQ("1000", DecimalUtil::toString(1000, DECIMAL(20, 0)));
  ASSERT_EQ("1.000", DecimalUtil::toString(1000, DECIMAL(20, 3)));
  ASSERT_EQ("0.0000001000", DecimalUtil::toString(1000, DECIMAL(20, 10)));
  ASSERT_EQ("-0.001000", DecimalUtil::toString(-1000, DECIMAL(20, 6)));
  ASSERT_EQ("0.000000000", DecimalUtil::toString(0, DECIMAL(20, 9)));

  const auto minShortDecimal =
      DecimalUtil::toString(DecimalUtil::kShortDecimalMin, DECIMAL(18, 0));
  ASSERT_EQ("-999999999999999999", minShortDecimal);
  // Additional 1 for negative sign.
  ASSERT_EQ(minShortDecimal.length(), 19);

  const auto maxShortDecimal =
      DecimalUtil::toString(DecimalUtil::kShortDecimalMax, DECIMAL(18, 0));
  ASSERT_EQ("999999999999999999", maxShortDecimal);
  ASSERT_EQ(maxShortDecimal.length(), 18);

  const auto minLongDecimal =
      DecimalUtil::toString(DecimalUtil::kLongDecimalMin, DECIMAL(38, 0));
  ASSERT_EQ("-99999999999999999999999999999999999999", minLongDecimal);
  // Additional 1 for negative sign.
  ASSERT_EQ(minLongDecimal.length(), 39);

  const auto maxLongDecimal =
      DecimalUtil::toString(DecimalUtil::kLongDecimalMax, DECIMAL(38, 0));
  ASSERT_EQ("99999999999999999999999999999999999999", maxLongDecimal);
  ASSERT_EQ(maxLongDecimal.length(), 38);
}

TEST(DecimalTest, limits) {
  VELOX_ASSERT_THROW(
      DecimalUtil::valueInRange(DecimalUtil::kLongDecimalMax + 1),
      "Value '100000000000000000000000000000000000000' is not in the range of Decimal Type");
  VELOX_ASSERT_THROW(
      DecimalUtil::valueInRange(DecimalUtil::kLongDecimalMin - 1),
      "Value '-100000000000000000000000000000000000000' is not in the range of Decimal Type");
}

TEST(DecimalTest, addUnsignedValues) {
  int128_t a = -HugeInt::build(0x4B3B4CA85A86C47A, 0x98A223FFFFFFFFF);
  int128_t sum = a;
  int64_t overflow = 0;
  auto count = 1'000'000;
  // Test underflow
  for (int i = 1; i < count; ++i) {
    overflow += DecimalUtil::addWithOverflow(sum, a, sum);
  }
  ASSERT_EQ(-587747, overflow);
  ASSERT_EQ(HugeInt::upper(sum), 0xE98C20AD1C80DBEF);
  ASSERT_EQ(HugeInt::lower(sum), 0xFEE2F000000F4240);

  // Test overflow.
  overflow = 0;
  a = -a;
  sum = a;
  for (int i = 1; i < count; ++i) {
    overflow += DecimalUtil::addWithOverflow(sum, a, sum);
  }
  ASSERT_EQ(587747, overflow);
  ASSERT_EQ(HugeInt::upper(sum), 0x1673df52e37f2410);
  ASSERT_EQ(HugeInt::lower(sum), 0x11d0ffffff0bdc0);
}

TEST(DecimalTest, longDecimalSerDe) {
  char data[100];
  HugeInt::serialize(DecimalUtil::kLongDecimalMin, data);
  auto deserializedData = HugeInt::deserialize(data);
  ASSERT_EQ(deserializedData, DecimalUtil::kLongDecimalMin);

  HugeInt::serialize(DecimalUtil::kLongDecimalMax, data);
  deserializedData = HugeInt::deserialize(data);
  ASSERT_EQ(deserializedData, DecimalUtil::kLongDecimalMax);

  HugeInt::serialize(-1, data);
  deserializedData = HugeInt::deserialize(data);
  ASSERT_EQ(deserializedData, -1);

  HugeInt::serialize(10, data);
  deserializedData = HugeInt::deserialize(data);
  ASSERT_EQ(deserializedData, 10);
}

// The result can be obtained by
// test("biginteger") {
//   val a = new BigInteger("20")
//   val arr = a.toByteArray
//   print("length is " + arr.length + "\n")
//   arr.foreach(r => print(r + ","))
// }
TEST(DecimalTest, toByteArray) {
  int8_t expected0[1] = {0};
  testToByteArray(0, expected0, 1);

  int8_t expected1[1] = {20};
  testToByteArray(20, expected1, 1);

  int8_t expected2[1] = {-20};
  testToByteArray(-20, expected2, 1);

  int8_t expected3[2] = {0, -56};
  testToByteArray(200, expected3, 2);

  int8_t expected4[2] = {78, 32};
  testToByteArray(20000, expected4, 2);

  int8_t expected5[6] = {-2, -32, -114, 4, -5, 77};
  testToByteArray(-1234567890099, expected5, 6);

  int8_t expected6[8] = {13, -32, -74, -77, -89, 99, -1, -1};
  testToByteArray(DecimalUtil::kShortDecimalMax, expected6, 8);

  int8_t expected7[16] = {
      -76, -60, -77, 87, -91, 121, 59, -123, -10, 117, -35, -64, 0, 0, 0, 1};
  testToByteArray(DecimalUtil::kLongDecimalMin, expected7, 16);

  int8_t expected8[16] = {
      75, 59, 76, -88, 90, -122, -60, 122, 9, -118, 34, 63, -1, -1, -1, -1};
  testToByteArray(DecimalUtil::kLongDecimalMax, expected8, 16);
}

TEST(DecimalTest, valueInPrecisionRange) {
  ASSERT_TRUE(DecimalUtil::valueInPrecisionRange<int64_t>(12, 3));
  ASSERT_TRUE(DecimalUtil::valueInPrecisionRange<int64_t>(999, 3));
  ASSERT_FALSE(DecimalUtil::valueInPrecisionRange<int64_t>(1000, 3));
  ASSERT_FALSE(DecimalUtil::valueInPrecisionRange<int64_t>(1234, 3));
  ASSERT_TRUE(DecimalUtil::valueInPrecisionRange<int64_t>(
      DecimalUtil::kShortDecimalMax, ShortDecimalType::kMaxPrecision));
  ASSERT_FALSE(DecimalUtil::valueInPrecisionRange<int64_t>(
      DecimalUtil::kShortDecimalMax + 1, ShortDecimalType::kMaxPrecision));
  ASSERT_TRUE(DecimalUtil::valueInPrecisionRange<int128_t>(
      DecimalUtil::kLongDecimalMax, LongDecimalType::kMaxPrecision));
  ASSERT_FALSE(DecimalUtil::valueInPrecisionRange<int128_t>(
      DecimalUtil::kLongDecimalMax + 1, LongDecimalType::kMaxPrecision));
  ASSERT_FALSE(DecimalUtil::valueInPrecisionRange<int128_t>(
      DecimalUtil::kLongDecimalMin - 1, LongDecimalType::kMaxPrecision));
}

TEST(DecimalTest, computeAverage) {
  auto validateSameValues = [](int128_t value, int64_t maxCount) {
    SCOPED_TRACE(fmt::format("value={} maxCount={}", value, maxCount));
    int128_t sum = 0;
    int64_t overflow = 0;
    for (int64_t i = 1; i <= maxCount; ++i) {
      overflow += DecimalUtil::addWithOverflow(sum, sum, value);
      int128_t avg;
      DecimalUtil::computeAverage(avg, sum, i, overflow);
      ASSERT_EQ(avg, value);
    }
  };
  validateSameValues(DecimalUtil::kLongDecimalMin, 1'000'000);
  validateSameValues(DecimalUtil::kLongDecimalMax, 1'000'000);
}

TEST(DecimalAggregateTest, adjustSumForOverflow) {
  struct SumWithOverflow {
    int128_t sum{0};
    int64_t overflow{0};

    void add(int128_t input) {
      overflow += DecimalUtil::addWithOverflow(sum, sum, input);
    }

    std::optional<int128_t> adjustedSum() const {
      return DecimalUtil::adjustSumForOverflow(sum, overflow);
    }

    void reset() {
      sum = 0;
      overflow = 0;
    }
  };

  SumWithOverflow accumulator;
  // kLongDecimalMax + kLongDecimalMax will trigger one upward overflow, and the
  // final sum result calculated by DecimalUtil::addWithOverflow is negative.
  // DecimalUtil::adjustSumForOverflow can adjust the sum to kLongDecimalMax
  // correctly.
  accumulator.add(DecimalUtil::kLongDecimalMax);
  accumulator.add(DecimalUtil::kLongDecimalMax);
  accumulator.add(DecimalUtil::kLongDecimalMin);
  EXPECT_EQ(accumulator.adjustedSum(), DecimalUtil::kLongDecimalMax);

  accumulator.reset();
  // kLongDecimalMin + kLongDecimalMin will trigger one downward overflow, and
  // the final sum result calculated by DecimalUtil::addWithOverflow is
  // positive. DecimalUtil::adjustSumForOverflow can adjust the sum to
  // kLongDecimalMin correctly.
  accumulator.add(DecimalUtil::kLongDecimalMin);
  accumulator.add(DecimalUtil::kLongDecimalMin);
  accumulator.add(DecimalUtil::kLongDecimalMax);
  EXPECT_EQ(accumulator.adjustedSum(), DecimalUtil::kLongDecimalMin);

  accumulator.reset();
  // These inputs will eventually trigger an upward overflow, and
  // DecimalUtil::adjustSumForOverflow will return std::nullopt.
  accumulator.add(DecimalUtil::kLongDecimalMax);
  accumulator.add(DecimalUtil::kLongDecimalMax);
  EXPECT_FALSE(accumulator.adjustedSum().has_value());

  accumulator.reset();
  // These inputs will eventually trigger a downward overflow, and
  // DecimalUtil::adjustSumForOverflow will return std::nullopt.
  accumulator.add(DecimalUtil::kLongDecimalMin);
  accumulator.add(DecimalUtil::kLongDecimalMin);
  EXPECT_FALSE(accumulator.adjustedSum().has_value());
}

TEST(DecimalTest, rescaleDouble) {
  assertRescaleDouble<int64_t>(-3333.03, DECIMAL(10, 4), -33'330'300);
  assertRescaleDouble<int128_t>(-3333.03, DECIMAL(20, 1), -33'330);
  assertRescaleDouble<int128_t>(-3333.03, DECIMAL(20, 10), -33'330'300'000'000);

  assertRescaleDouble<int64_t>(-2222.02, DECIMAL(10, 4), -22'220'200);
  assertRescaleDouble<int128_t>(-2222.02, DECIMAL(20, 1), -22'220);
  assertRescaleDouble<int128_t>(-2222.02, DECIMAL(20, 10), -22'220'200'000'000);

  assertRescaleDouble<int64_t>(-1.0, DECIMAL(10, 4), -10'000);
  assertRescaleDouble<int128_t>(-1.0, DECIMAL(20, 1), -10);
  assertRescaleDouble<int128_t>(-1.0, DECIMAL(20, 10), -10'000'000'000);

  assertRescaleDouble<int64_t>(0.00, DECIMAL(10, 4), 0);
  assertRescaleDouble<int128_t>(0.00, DECIMAL(20, 1), 0);
  assertRescaleDouble<int128_t>(0.00, DECIMAL(20, 10), 0);

  assertRescaleDouble<int64_t>(100, DECIMAL(10, 4), 1'000'000);
  assertRescaleDouble<int128_t>(100, DECIMAL(20, 1), 1'000);
  assertRescaleDouble<int128_t>(100, DECIMAL(20, 10), 1'000'000'000'000);

  assertRescaleDouble<int64_t>(99999.99, DECIMAL(10, 4), 999'999'900);
  assertRescaleDouble<int128_t>(99999.99, DECIMAL(20, 1), 1'000'000);
  assertRescaleDouble<int128_t>(99999.99, DECIMAL(20, 10), 999'999'900'000'000);

  assertRescaleDouble<int128_t>(
      0.034567890, DECIMAL(38, 18), 34'567'890'000'000'000);
  assertRescaleDouble<int128_t>(
      0.999999999999999, DECIMAL(38, 18), 999'999'999'999'999'000);
  assertRescaleDouble<int128_t>(
      0.123456789123123, DECIMAL(38, 18), 123'456'789'123'123'000);
  assertRescaleDouble<int64_t>(21.54551, DECIMAL(12, 3), 21546);

  assertRescaleDoubleFail<int128_t>(
      std::numeric_limits<double>::max(), DECIMAL(38, 38), "Result overflows.");
  assertRescaleDouble<int128_t>(
      std::numeric_limits<double>::min(), DECIMAL(38, 2), 0);
  assertRescaleDoubleFail<int128_t>(
      std::numeric_limits<double>::lowest(),
      DECIMAL(38, 2),
      "Result overflows.");

  assertRescaleDoubleFail<int64_t>(
      NAN, DECIMAL(10, 2), "The input value should be finite.");
  assertRescaleDoubleFail<int64_t>(
      INFINITY, DECIMAL(10, 2), "The input value should be finite.");

  assertRescaleDoubleFail<int64_t>(
      9999999999999999999999.99, DECIMAL(10, 2), "Result overflows.");
  assertRescaleDoubleFail<int64_t>(
      static_cast<double>(
          static_cast<int128_t>(std::numeric_limits<int64_t>::max()) + 1),
      DECIMAL(10, 2),
      "Result overflows.");
  assertRescaleDoubleFail<int64_t>(
      static_cast<double>(
          static_cast<int128_t>(std::numeric_limits<int64_t>::min()) - 1),
      DECIMAL(10, 2),
      "Result overflows.");
  assertRescaleDoubleFail<int64_t>(
      static_cast<double>(DecimalUtil::kShortDecimalMax),
      DECIMAL(10, 2),
      "Result overflows.");
  assertRescaleDoubleFail<int64_t>(
      static_cast<double>(DecimalUtil::kShortDecimalMin),
      DECIMAL(10, 2),
      "Result overflows.");
  assertRescaleDoubleFail<int128_t>(
      static_cast<double>(DecimalUtil::kLongDecimalMax),
      DECIMAL(20, 2),
      "Result overflows.");
  assertRescaleDoubleFail<int128_t>(
      static_cast<double>(DecimalUtil::kLongDecimalMin),
      DECIMAL(20, 2),
      "Result overflows.");

  assertRescaleDoubleFail<int64_t>(
      99999.99, DECIMAL(6, 4), "Result cannot fit in the given precision 6.");
}

} // namespace
} // namespace facebook::velox
