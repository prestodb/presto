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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::functions::sparksql::test {
namespace {

class DateTimeFunctionsTest : public SparkFunctionBaseTest {
 public:
  static constexpr int32_t kMin = std::numeric_limits<int32_t>::min();
  static constexpr int32_t kMax = std::numeric_limits<int32_t>::max();

 protected:
  void setQueryTimeZone(const std::string& timeZone) {
    queryCtx_->testingOverrideConfigUnsafe({
        {core::QueryConfig::kSessionTimezone, timeZone},
        {core::QueryConfig::kAdjustTimestampToTimezone, "true"},
    });
  }

  int32_t parseDate(const std::string& dateStr) {
    return DATE()->toDays(dateStr);
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
  EXPECT_EQ(1969, year(Timestamp(-1, Timestamp::kMaxNanos)));
  EXPECT_EQ(2096, year(Timestamp(4000000000, 0)));
  EXPECT_EQ(2096, year(Timestamp(4000000000, 123000000)));
  EXPECT_EQ(2001, year(Timestamp(998474645, 321000000)));
  EXPECT_EQ(2001, year(Timestamp(998423705, 321000000)));
}

TEST_F(DateTimeFunctionsTest, yearDate) {
  const auto year = [&](std::optional<int32_t> date) {
    return evaluateOnce<int32_t, int32_t>("year(c0)", {date}, {DATE()});
  };
  EXPECT_EQ(std::nullopt, year(std::nullopt));
  EXPECT_EQ(1970, year(DATE()->toDays("1970-05-05")));
  EXPECT_EQ(1969, year(DATE()->toDays("1969-12-31")));
  EXPECT_EQ(2020, year(DATE()->toDays("2020-01-01")));
  EXPECT_EQ(1920, year(DATE()->toDays("1920-01-01")));
}

TEST_F(DateTimeFunctionsTest, weekOfYear) {
  const auto weekOfYear = [&](const char* dateString) {
    auto date = std::make_optional(parseDate(dateString));
    return evaluateOnce<int32_t, int32_t>("week_of_year(c0)", {date}, {DATE()})
        .value();
  };

  EXPECT_EQ(1, weekOfYear("1919-12-31"));
  EXPECT_EQ(1, weekOfYear("1920-01-01"));
  EXPECT_EQ(1, weekOfYear("1920-01-04"));
  EXPECT_EQ(2, weekOfYear("1920-01-05"));
  EXPECT_EQ(53, weekOfYear("1960-01-01"));
  EXPECT_EQ(53, weekOfYear("1960-01-03"));
  EXPECT_EQ(1, weekOfYear("1960-01-04"));
  EXPECT_EQ(1, weekOfYear("1969-12-31"));
  EXPECT_EQ(1, weekOfYear("1970-01-01"));
  EXPECT_EQ(1, weekOfYear("0001-01-01"));
  EXPECT_EQ(52, weekOfYear("9999-12-31"));
  EXPECT_EQ(8, weekOfYear("2008-02-20"));
  EXPECT_EQ(15, weekOfYear("2015-04-08"));
  EXPECT_EQ(15, weekOfYear("2013-04-08"));
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
    return evaluateOnce<int32_t>("make_date(c0, c1, c2)", year, month, day);
  };
  EXPECT_EQ(makeDate(1920, 1, 25), DATE()->toDays("1920-01-25"));
  EXPECT_EQ(makeDate(-10, 1, 30), DATE()->toDays("-0010-01-30"));

  auto errorMessage = fmt::format("Date out of range: {}-12-15", kMax);
  VELOX_ASSERT_THROW(makeDate(kMax, 12, 15), errorMessage);

  constexpr const int32_t kJodaMaxYear{292278994};
  VELOX_ASSERT_THROW(makeDate(kJodaMaxYear - 10, 12, 15), "Integer overflow");

  VELOX_ASSERT_THROW(makeDate(2021, 13, 1), "Date out of range: 2021-13-1");
  VELOX_ASSERT_THROW(makeDate(2022, 3, 35), "Date out of range: 2022-3-35");

  VELOX_ASSERT_THROW(makeDate(2023, 4, 31), "Date out of range: 2023-4-31");
  EXPECT_EQ(makeDate(2023, 3, 31), DATE()->toDays("2023-03-31"));

  VELOX_ASSERT_THROW(makeDate(2023, 2, 29), "Date out of range: 2023-2-29");
  EXPECT_EQ(makeDate(2023, 3, 29), DATE()->toDays("2023-03-29"));
}

TEST_F(DateTimeFunctionsTest, lastDay) {
  const auto lastDayFunc = [&](const std::optional<int32_t> date) {
    return evaluateOnce<int32_t, int32_t>("last_day(c0)", {date}, {DATE()});
  };

  const auto lastDay = [&](const std::string& dateStr) {
    return lastDayFunc(DATE()->toDays(dateStr));
  };

  const auto parseDateStr = [&](const std::string& dateStr) {
    return DATE()->toDays(dateStr);
  };

  EXPECT_EQ(lastDay("2015-02-28"), parseDateStr("2015-02-28"));
  EXPECT_EQ(lastDay("2015-03-27"), parseDateStr("2015-03-31"));
  EXPECT_EQ(lastDay("2015-04-26"), parseDateStr("2015-04-30"));
  EXPECT_EQ(lastDay("2015-05-25"), parseDateStr("2015-05-31"));
  EXPECT_EQ(lastDay("2015-06-24"), parseDateStr("2015-06-30"));
  EXPECT_EQ(lastDay("2015-07-23"), parseDateStr("2015-07-31"));
  EXPECT_EQ(lastDay("2015-08-01"), parseDateStr("2015-08-31"));
  EXPECT_EQ(lastDay("2015-09-02"), parseDateStr("2015-09-30"));
  EXPECT_EQ(lastDay("2015-10-03"), parseDateStr("2015-10-31"));
  EXPECT_EQ(lastDay("2015-11-04"), parseDateStr("2015-11-30"));
  EXPECT_EQ(lastDay("2015-12-05"), parseDateStr("2015-12-31"));
  EXPECT_EQ(lastDay("2016-01-06"), parseDateStr("2016-01-31"));
  EXPECT_EQ(lastDay("2016-02-07"), parseDateStr("2016-02-29"));
  EXPECT_EQ(lastDayFunc(std::nullopt), std::nullopt);
}

TEST_F(DateTimeFunctionsTest, dateAdd) {
  const auto dateAdd = [&](std::optional<int32_t> date,
                           std::optional<int32_t> value) {
    return evaluateOnce<int32_t, int32_t>(
        "date_add(c0, c1)", {date, value}, {DATE(), INTEGER()});
  };

  // Check null behaviors
  EXPECT_EQ(std::nullopt, dateAdd(std::nullopt, 1));
  EXPECT_EQ(std::nullopt, dateAdd(parseDate("2019-02-28"), std::nullopt));
  EXPECT_EQ(std::nullopt, dateAdd(std::nullopt, std::nullopt));

  // Check simple tests.
  EXPECT_EQ(parseDate("2019-03-01"), dateAdd(parseDate("2019-03-01"), 0));
  EXPECT_EQ(parseDate("2019-03-01"), dateAdd(parseDate("2019-02-28"), 1));

  // Account for the last day of a year-month
  EXPECT_EQ(parseDate("2020-02-29"), dateAdd(parseDate("2019-01-30"), 395));
  EXPECT_EQ(parseDate("2020-02-29"), dateAdd(parseDate("2019-01-30"), 395));

  // Check for negative intervals
  EXPECT_EQ(parseDate("2019-02-28"), dateAdd(parseDate("2020-02-29"), -366));
  EXPECT_EQ(parseDate("2019-02-28"), dateAdd(parseDate("2020-02-29"), -366));

  // Check for minimum and maximum tests.
  EXPECT_EQ(parseDate("5881580-07-11"), dateAdd(parseDate("1970-01-01"), kMax));
  EXPECT_EQ(
      parseDate("1969-12-31"), dateAdd(parseDate("-5877641-06-23"), kMax));
  EXPECT_EQ(
      parseDate("-5877641-06-23"), dateAdd(parseDate("1970-01-01"), kMin));
  EXPECT_EQ(parseDate("1969-12-31"), dateAdd(parseDate("5881580-07-11"), kMin));
}

TEST_F(DateTimeFunctionsTest, dateSub) {
  const auto dateSubFunc = [&](std::optional<int32_t> date,
                               std::optional<int32_t> value) {
    return evaluateOnce<int32_t, int32_t>(
        "date_sub(c0, c1)", {date, value}, {DATE(), INTEGER()});
  };

  const auto dateSub = [&](const std::string& dateStr,
                           std::optional<int32_t> value) {
    return dateSubFunc(parseDate(dateStr), value);
  };

  // Check simple tests.
  EXPECT_EQ(parseDate("2019-03-01"), dateSub("2019-03-01", 0));
  EXPECT_EQ(parseDate("2019-02-28"), dateSub("2019-03-01", 1));

  // Account for the last day of a year-month.
  EXPECT_EQ(parseDate("2019-01-30"), dateSub("2020-02-29", 395));

  // Check for negative intervals.
  EXPECT_EQ(parseDate("2020-02-29"), dateSub("2019-02-28", -366));

  // Check for minimum and maximum tests.
  EXPECT_EQ(parseDate("-5877641-06-23"), dateSub("1969-12-31", kMax));
  EXPECT_EQ(parseDate("1970-01-01"), dateSub("5881580-07-11", kMax));
  EXPECT_EQ(parseDate("1970-01-01"), dateSub("-5877641-06-23", kMin));
  EXPECT_EQ(parseDate("5881580-07-11"), dateSub("1969-12-31", kMin));
}

TEST_F(DateTimeFunctionsTest, dayOfYear) {
  const auto day = [&](std::optional<int32_t> date) {
    return evaluateOnce<int64_t, int32_t>("dayofyear(c0)", {date}, {DATE()});
  };
  EXPECT_EQ(std::nullopt, day(std::nullopt));
  EXPECT_EQ(100, day(parseDate("2016-04-09")));
  EXPECT_EQ(235, day(parseDate("2023-08-23")));
  EXPECT_EQ(1, day(parseDate("1970-01-01")));
}

TEST_F(DateTimeFunctionsTest, dayOfMonth) {
  const auto day = [&](std::optional<int32_t> date) {
    return evaluateOnce<int64_t, int32_t>("dayofmonth(c0)", {date}, {DATE()});
  };
  EXPECT_EQ(std::nullopt, day(std::nullopt));
  EXPECT_EQ(30, day(parseDate("2009-07-30")));
  EXPECT_EQ(23, day(parseDate("2023-08-23")));
}

TEST_F(DateTimeFunctionsTest, dayOfWeekDate) {
  const auto dayOfWeek = [&](std::optional<int32_t> date,
                             const std::string& func) {
    return evaluateOnce<int32_t, int32_t>(
        fmt::format("{}(c0)", func), {date}, {DATE()});
  };

  for (const auto& func : {"dayofweek", "dow"}) {
    EXPECT_EQ(std::nullopt, dayOfWeek(std::nullopt, func));
    EXPECT_EQ(5, dayOfWeek(0, func));
    EXPECT_EQ(4, dayOfWeek(-1, func));
    EXPECT_EQ(7, dayOfWeek(-40, func));
    EXPECT_EQ(5, dayOfWeek(parseDate("2009-07-30"), func));
    EXPECT_EQ(1, dayOfWeek(parseDate("2023-08-20"), func));
    EXPECT_EQ(2, dayOfWeek(parseDate("2023-08-21"), func));
    EXPECT_EQ(3, dayOfWeek(parseDate("2023-08-22"), func));
    EXPECT_EQ(4, dayOfWeek(parseDate("2023-08-23"), func));
    EXPECT_EQ(5, dayOfWeek(parseDate("2023-08-24"), func));
    EXPECT_EQ(6, dayOfWeek(parseDate("2023-08-25"), func));
    EXPECT_EQ(7, dayOfWeek(parseDate("2023-08-26"), func));
    EXPECT_EQ(1, dayOfWeek(parseDate("2023-08-27"), func));

    // test cases from spark's DateExpressionSuite.
    EXPECT_EQ(6, dayOfWeek(util::fromDateString("2011-05-06"), func));
  }
}

TEST_F(DateTimeFunctionsTest, dayofWeekTs) {
  const auto dayOfWeek = [&](std::optional<Timestamp> date,
                             const std::string& func) {
    return evaluateOnce<int32_t>(fmt::format("{}(c0)", func), date);
  };

  for (const auto& func : {"dayofweek", "dow"}) {
    EXPECT_EQ(5, dayOfWeek(Timestamp(0, 0), func));
    EXPECT_EQ(4, dayOfWeek(Timestamp(-1, 0), func));
    EXPECT_EQ(
        1,
        dayOfWeek(util::fromTimestampString("2023-08-20 20:23:00.001"), func));
    EXPECT_EQ(
        2,
        dayOfWeek(util::fromTimestampString("2023-08-21 21:23:00.030"), func));
    EXPECT_EQ(
        3,
        dayOfWeek(util::fromTimestampString("2023-08-22 11:23:00.100"), func));
    EXPECT_EQ(
        4,
        dayOfWeek(util::fromTimestampString("2023-08-23 22:23:00.030"), func));
    EXPECT_EQ(
        5,
        dayOfWeek(util::fromTimestampString("2023-08-24 15:23:00.000"), func));
    EXPECT_EQ(
        6,
        dayOfWeek(util::fromTimestampString("2023-08-25 03:23:04.000"), func));
    EXPECT_EQ(
        7,
        dayOfWeek(util::fromTimestampString("2023-08-26 01:03:00.300"), func));
    EXPECT_EQ(
        1,
        dayOfWeek(util::fromTimestampString("2023-08-27 01:13:00.000"), func));
    // test cases from spark's DateExpressionSuite.
    EXPECT_EQ(
        4, dayOfWeek(util::fromTimestampString("2015-04-08 13:10:15"), func));
    EXPECT_EQ(
        7, dayOfWeek(util::fromTimestampString("2017-05-27 13:10:15"), func));
    EXPECT_EQ(
        6, dayOfWeek(util::fromTimestampString("1582-10-15 13:10:15"), func));
  }
}

TEST_F(DateTimeFunctionsTest, dateDiffDate) {
  const auto dateDiff = [&](std::optional<int32_t> endDate,
                            std::optional<int32_t> startDate) {
    return evaluateOnce<int32_t, int32_t>(
        "datediff(c0, c1)", {endDate, startDate}, {DATE(), DATE()});
  };

  // Simple tests.
  EXPECT_EQ(-1, dateDiff(parseDate("2019-02-28"), parseDate("2019-03-01")));
  EXPECT_EQ(-358, dateDiff(parseDate("2019-02-28"), parseDate("2020-02-21")));
  EXPECT_EQ(0, dateDiff(parseDate("1994-04-20"), parseDate("1994-04-20")));

  // Account for the last day of a year-month.
  EXPECT_EQ(395, dateDiff(parseDate("2020-02-29"), parseDate("2019-01-30")));

  // Check Large date.
  EXPECT_EQ(
      -737790, dateDiff(parseDate("2020-02-29"), parseDate("4040-02-29")));

  // Overflowed result, consistent with spark.
  EXPECT_EQ(
      2147474628,
      dateDiff(parseDate("-5877641-06-23"), parseDate("1994-09-12")));
}

TEST_F(DateTimeFunctionsTest, addMonths) {
  const auto addMonths = [&](const std::string& dateString, int32_t value) {
    return evaluateOnce<int32_t, int32_t>(
        "add_months(c0, c1)",
        {parseDate(dateString), value},
        {DATE(), INTEGER()});
  };

  EXPECT_EQ(addMonths("2015-01-30", 1), parseDate("2015-02-28"));
  EXPECT_EQ(addMonths("2015-01-30", 11), parseDate("2015-12-30"));
  EXPECT_EQ(addMonths("2015-01-01", 10), parseDate("2015-11-01"));
  EXPECT_EQ(addMonths("2015-01-31", 24), parseDate("2017-01-31"));
  EXPECT_EQ(addMonths("2015-01-31", 8), parseDate("2015-09-30"));
  EXPECT_EQ(addMonths("2015-01-30", 0), parseDate("2015-01-30"));
  // The last day of Feb. 2016 is 29th.
  EXPECT_EQ(addMonths("2016-03-30", -1), parseDate("2016-02-29"));
  // The last day of Feb. 2015 is 28th.
  EXPECT_EQ(addMonths("2015-03-31", -1), parseDate("2015-02-28"));
  EXPECT_EQ(addMonths("2015-01-30", -2), parseDate("2014-11-30"));
  EXPECT_EQ(addMonths("2015-04-20", -24), parseDate("2013-04-20"));

  VELOX_ASSERT_THROW(
      addMonths("2023-07-10", kMin),
      fmt::format("Integer overflow in add_months(2023-07-10, {})", kMin));
  VELOX_ASSERT_THROW(
      addMonths("2023-07-10", kMax),
      fmt::format("Integer overflow in add_months(2023-07-10, {})", kMax));
}
} // namespace
} // namespace facebook::velox::functions::sparksql::test
