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

#include <stdint.h>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::functions::sparksql::test {
namespace {

class DateTimeFunctionsTest : public SparkFunctionBaseTest {
 protected:
  void setQueryTimeZone(const std::string& timeZone) {
    queryCtx_->testingOverrideConfigUnsafe({
        {core::QueryConfig::kSessionTimezone, timeZone},
        {core::QueryConfig::kAdjustTimestampToTimezone, "true"},
    });
  }
};

TEST_F(DateTimeFunctionsTest, year) {
  const auto year = [&](std::optional<Timestamp> date) {
    return evaluateOnce<int32_t>("year(c0)", date);
  };
  EXPECT_EQ(std::nullopt, year(std::nullopt));
  EXPECT_EQ(1970, year(Timestamp(0, 0)));
  EXPECT_EQ(1969, year(Timestamp(-1, 9000)));
  EXPECT_EQ(2096, year(Timestamp(4000000000, 0)));
  EXPECT_EQ(2096, year(Timestamp(4000000000, 123000000)));
  EXPECT_EQ(2001, year(Timestamp(998474645, 321000000)));
  EXPECT_EQ(2001, year(Timestamp(998423705, 321000000)));

  setQueryTimeZone("Pacific/Apia");

  EXPECT_EQ(std::nullopt, year(std::nullopt));
  EXPECT_EQ(1969, year(Timestamp(0, 0)));
  EXPECT_EQ(1969, year(Timestamp(-1, 12300000000)));
  EXPECT_EQ(2096, year(Timestamp(4000000000, 0)));
  EXPECT_EQ(2096, year(Timestamp(4000000000, 123000000)));
  EXPECT_EQ(2001, year(Timestamp(998474645, 321000000)));
  EXPECT_EQ(2001, year(Timestamp(998423705, 321000000)));
}

TEST_F(DateTimeFunctionsTest, yearDate) {
  const auto year = [&](std::optional<Date> date) {
    return evaluateOnce<int32_t>("year(c0)", date);
  };
  EXPECT_EQ(std::nullopt, year(std::nullopt));
  EXPECT_EQ(1970, year(Date(0)));
  EXPECT_EQ(1969, year(Date(-1)));
  EXPECT_EQ(2020, year(Date(18262)));
  EXPECT_EQ(1920, year(Date(-18262)));
}

TEST_F(DateTimeFunctionsTest, unixTimestamp) {
  const auto unixTimestamp = [&](std::optional<StringView> dateStr) {
    return evaluateOnce<int64_t>("unix_timestamp(c0)", dateStr);
  };

  EXPECT_EQ(0, unixTimestamp("1970-01-01 00:00:00"));
  EXPECT_EQ(1, unixTimestamp("1970-01-01 00:00:01"));
  EXPECT_EQ(61, unixTimestamp("1970-01-01 00:01:01"));

  setQueryTimeZone("America/Los_Angeles");

  EXPECT_EQ(28800, unixTimestamp("1970-01-01 00:00:00"));
  EXPECT_EQ(1670859931, unixTimestamp("2022-12-12 07:45:31"));

  // Empty or malformed input returns null.
  EXPECT_EQ(std::nullopt, unixTimestamp(std::nullopt));
  EXPECT_EQ(std::nullopt, unixTimestamp("1970-01-01"));
  EXPECT_EQ(std::nullopt, unixTimestamp("00:00:00"));
  EXPECT_EQ(std::nullopt, unixTimestamp(""));
  EXPECT_EQ(std::nullopt, unixTimestamp("malformed input"));
}

TEST_F(DateTimeFunctionsTest, unixTimestampCurrent) {
  // Need a mock row vector so we can pump exactly one record out.
  auto mockRowVector =
      makeRowVector({BaseVector::createNullConstant(UNKNOWN(), 1, pool())});

  // Safe bet that unix epoch (in seconds) should be between 500M and 5B.
  auto epoch = evaluateOnce<int64_t>("unix_timestamp()", mockRowVector);
  EXPECT_GE(epoch, 500'000'000);
  EXPECT_LT(epoch, 5'000'000'000);

  // Spark doesn't seem to adjust based on timezones.
  auto gmtEpoch = evaluateOnce<int64_t>("unix_timestamp()", mockRowVector);
  setQueryTimeZone("America/Los_Angeles");
  auto laEpoch = evaluateOnce<int64_t>("unix_timestamp()", mockRowVector);
  EXPECT_EQ(gmtEpoch, laEpoch);
}

TEST_F(DateTimeFunctionsTest, unixTimestampCustomFormat) {
  const auto unixTimestamp = [&](std::optional<StringView> dateStr,
                                 std::optional<StringView> formatStr) {
    return evaluateOnce<int64_t>("unix_timestamp(c0, c1)", dateStr, formatStr);
  };

  EXPECT_EQ(0, unixTimestamp("1970-01-01", "yyyy-MM-dd"));
  EXPECT_EQ(-31536000, unixTimestamp("1969", "YYYY"));
  EXPECT_EQ(86400, unixTimestamp("1970-01-02", "yyyy-MM-dd"));
  EXPECT_EQ(86410, unixTimestamp("1970-01-02 00:00:10", "yyyy-MM-dd HH:mm:ss"));

  // Literal.
  EXPECT_EQ(
      1670831131,
      unixTimestamp("2022-12-12 asd 07:45:31", "yyyy-MM-dd 'asd' HH:mm:ss"));

  // Invalid format returns null (unclosed quoted literal).
  EXPECT_EQ(
      std::nullopt,
      unixTimestamp("2022-12-12 asd 07:45:31", "yyyy-MM-dd 'asd HH:mm:ss"));
}

// unix_timestamp and to_unix_timestamp are aliases.
TEST_F(DateTimeFunctionsTest, toUnixTimestamp) {
  std::optional<StringView> dateStr = "1970-01-01 08:32:11"_sv;
  std::optional<StringView> formatStr = "YYYY-MM-dd HH:mm:ss"_sv;

  EXPECT_EQ(
      evaluateOnce<int64_t>("unix_timestamp(c0)", dateStr),
      evaluateOnce<int64_t>("to_unix_timestamp(c0)", dateStr));
  EXPECT_EQ(
      evaluateOnce<int64_t>("unix_timestamp(c0, c1)", dateStr, formatStr),
      evaluateOnce<int64_t>("to_unix_timestamp(c0, c1)", dateStr, formatStr));

  // to_unix_timestamp does not provide an overoaded without any parameters.
  EXPECT_THROW(evaluateOnce<int64_t>("to_unix_timestamp()"), VeloxUserError);
}

TEST_F(DateTimeFunctionsTest, makeDate) {
  const auto makeDate = [&](std::optional<int32_t> year,
                            std::optional<int32_t> month,
                            std::optional<int32_t> day) {
    return evaluateOnce<Date>("make_date(c0, c1, c2)", year, month, day);
  };
  Date expectedDate;
  parseTo("1920-01-25", expectedDate);
  EXPECT_EQ(makeDate(1920, 1, 25), expectedDate);

  parseTo("-0010-01-30", expectedDate);
  EXPECT_EQ(makeDate(-10, 1, 30), expectedDate);

  constexpr int32_t kMax = std::numeric_limits<int32_t>::max();
  auto errorMessage = fmt::format("Date out of range: {}-12-15", kMax);
  VELOX_ASSERT_THROW(makeDate(kMax, 12, 15), errorMessage);

  constexpr const int32_t kJodaMaxYear{292278994};
  VELOX_ASSERT_THROW(makeDate(kJodaMaxYear - 10, 12, 15), "Integer overflow");

  VELOX_ASSERT_THROW(makeDate(2021, 13, 1), "Date out of range: 2021-13-1");
  VELOX_ASSERT_THROW(makeDate(2022, 3, 35), "Date out of range: 2022-3-35");

  VELOX_ASSERT_THROW(makeDate(2023, 4, 31), "Date out of range: 2023-4-31");
  parseTo("2023-03-31", expectedDate);
  EXPECT_EQ(makeDate(2023, 3, 31), expectedDate);

  VELOX_ASSERT_THROW(makeDate(2023, 2, 29), "Date out of range: 2023-2-29");
  parseTo("2023-03-29", expectedDate);
  EXPECT_EQ(makeDate(2023, 3, 29), expectedDate);
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
