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
  static constexpr int16_t kMinSmallint = std::numeric_limits<int16_t>::min();
  static constexpr int16_t kMaxSmallint = std::numeric_limits<int16_t>::max();
  static constexpr int8_t kMinTinyint = std::numeric_limits<int8_t>::min();
  static constexpr int8_t kMaxTinyint = std::numeric_limits<int8_t>::max();

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

  template <typename TOutput, typename TValue>
  std::optional<TOutput> evaluateDateFuncOnce(
      const std::string& expr,
      const std::optional<int32_t>& date,
      const std::optional<TValue>& value) {
    return evaluateOnce<TOutput>(
        expr,
        makeRowVector(
            {makeNullableFlatVector(
                 std::vector<std::optional<int32_t>>{date}, DATE()),
             makeNullableFlatVector(
                 std::vector<std::optional<TValue>>{value})}));
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
  const auto dateAdd = [&](const std::string& dateStr,
                           std::optional<int32_t> value) {
    return evaluateDateFuncOnce<int32_t, int32_t>(
        "date_add(c0, c1)", parseDate(dateStr), value);
  };

  // Check simple tests.
  EXPECT_EQ(parseDate("2019-03-01"), dateAdd("2019-03-01", 0));
  EXPECT_EQ(parseDate("2019-03-01"), dateAdd("2019-02-28", 1));

  // Account for the last day of a year-month
  EXPECT_EQ(parseDate("2020-02-29"), dateAdd("2019-01-30", 395));
  EXPECT_EQ(parseDate("2020-02-29"), dateAdd("2019-01-30", 395));

  // Check for negative intervals
  EXPECT_EQ(parseDate("2019-02-28"), dateAdd("2020-02-29", -366));
  EXPECT_EQ(parseDate("2019-02-28"), dateAdd("2020-02-29", -366));

  // Check for minimum and maximum tests.
  EXPECT_EQ(parseDate("5881580-07-11"), dateAdd("1970-01-01", kMax));
  EXPECT_EQ(parseDate("1969-12-31"), dateAdd("-5877641-06-23", kMax));
  EXPECT_EQ(parseDate("-5877641-06-23"), dateAdd("1970-01-01", kMin));
  EXPECT_EQ(parseDate("1969-12-31"), dateAdd("5881580-07-11", kMin));
  EXPECT_EQ(parseDate("5881580-07-10"), dateAdd("1969-12-31", kMax));

  EXPECT_EQ(parseDate("-5877587-07-11"), dateAdd("2024-01-22", kMax - 1));
  EXPECT_EQ(parseDate("-5877587-07-12"), dateAdd("2024-01-22", kMax));
}

TEST_F(DateTimeFunctionsTest, dateAddSmallint) {
  const auto dateAdd = [&](const std::string& dateStr,
                           std::optional<int16_t> value) {
    return evaluateDateFuncOnce<int32_t, int16_t>(
        "date_add(c0, c1)", parseDate(dateStr), value);
  };

  // Check simple tests.
  EXPECT_EQ(parseDate("2019-03-01"), dateAdd("2019-03-01", 0));
  EXPECT_EQ(parseDate("2019-03-01"), dateAdd("2019-02-28", 1));

  // Account for the last day of a year-month
  EXPECT_EQ(parseDate("2020-02-29"), dateAdd("2019-01-30", 395));
  EXPECT_EQ(parseDate("2020-02-29"), dateAdd("2019-01-30", 395));

  // Check for negative intervals
  EXPECT_EQ(parseDate("2019-02-28"), dateAdd("2020-02-29", -366));
  EXPECT_EQ(parseDate("2019-02-28"), dateAdd("2020-02-29", -366));

  // Check for minimum and maximum tests.
  EXPECT_EQ(parseDate("2059-09-17"), dateAdd("1969-12-31", kMaxSmallint));
  EXPECT_EQ(parseDate("1880-04-13"), dateAdd("1969-12-31", kMinSmallint));

  EXPECT_EQ(parseDate("2113-10-09"), dateAdd("2024-01-22", kMaxSmallint));
}

TEST_F(DateTimeFunctionsTest, dateAddTinyint) {
  const auto dateAdd = [&](const std::string& dateStr,
                           std::optional<int8_t> value) {
    return evaluateDateFuncOnce<int32_t, int8_t>(
        "date_add(c0, c1)", parseDate(dateStr), value);
  };
  // Check simple tests.
  EXPECT_EQ(parseDate("2019-03-01"), dateAdd("2019-03-01", 0));
  EXPECT_EQ(parseDate("2019-03-01"), dateAdd("2019-02-28", 1));

  EXPECT_EQ(parseDate("1970-05-07"), dateAdd("1969-12-31", kMaxTinyint));

  EXPECT_EQ(parseDate("1969-08-25"), dateAdd("1969-12-31", kMinTinyint));

  EXPECT_EQ(parseDate("2024-05-28"), dateAdd("2024-01-22", kMaxTinyint));
}

TEST_F(DateTimeFunctionsTest, dateSub) {
  const auto dateSub = [&](const std::string& dateStr,
                           std::optional<int32_t> value) {
    return evaluateDateFuncOnce<int32_t, int32_t>(
        "date_sub(c0, c1)", parseDate(dateStr), value);
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

  EXPECT_EQ(parseDate("-5877588-12-28"), dateSub("2023-07-10", kMin + 1));
  EXPECT_EQ(parseDate("-5877588-12-29"), dateSub("2023-07-10", kMin));
}

TEST_F(DateTimeFunctionsTest, dateSubSmallint) {
  const auto dateSub = [&](const std::string& dateStr,
                           std::optional<int16_t> value) {
    return evaluateDateFuncOnce<int32_t, int16_t>(
        "date_sub(c0, c1)", parseDate(dateStr), value);
  };

  // Check simple tests.
  EXPECT_EQ(parseDate("2019-03-01"), dateSub("2019-03-01", 0));
  EXPECT_EQ(parseDate("2019-02-28"), dateSub("2019-03-01", 1));

  // Account for the last day of a year-month.
  EXPECT_EQ(parseDate("2019-01-30"), dateSub("2020-02-29", 395));

  // Check for negative intervals.
  EXPECT_EQ(parseDate("2020-02-29"), dateSub("2019-02-28", -366));

  EXPECT_EQ(parseDate("1880-04-15"), dateSub("1970-01-01", kMaxSmallint));
  EXPECT_EQ(parseDate("2059-09-19"), dateSub("1970-01-01", kMinSmallint));

  EXPECT_EQ(parseDate("2113-03-28"), dateSub("2023-07-10", kMinSmallint));
}

TEST_F(DateTimeFunctionsTest, dateSubTinyint) {
  const auto dateSub = [&](const std::string& dateStr,
                           std::optional<int8_t> value) {
    return evaluateDateFuncOnce<int32_t, int8_t>(
        "date_sub(c0, c1)", parseDate(dateStr), value);
  };

  // Check simple tests.
  EXPECT_EQ(parseDate("2019-03-01"), dateSub("2019-03-01", 0));
  EXPECT_EQ(parseDate("2019-02-28"), dateSub("2019-03-01", 1));

  EXPECT_EQ(parseDate("1969-08-27"), dateSub("1970-01-01", kMaxTinyint));
  EXPECT_EQ(parseDate("1970-05-09"), dateSub("1970-01-01", kMinTinyint));

  EXPECT_EQ(parseDate("2023-11-15"), dateSub("2023-07-10", kMinTinyint));
}

TEST_F(DateTimeFunctionsTest, dayOfYear) {
  const auto day = [&](std::optional<int32_t> date) {
    return evaluateOnce<int32_t, int32_t>("dayofyear(c0)", {date}, {DATE()});
  };
  EXPECT_EQ(std::nullopt, day(std::nullopt));
  EXPECT_EQ(100, day(parseDate("2016-04-09")));
  EXPECT_EQ(235, day(parseDate("2023-08-23")));
  EXPECT_EQ(1, day(parseDate("1970-01-01")));
}

TEST_F(DateTimeFunctionsTest, dayOfMonth) {
  const auto day = [&](std::optional<int32_t> date) {
    return evaluateOnce<int32_t, int32_t>("dayofmonth(c0)", {date}, {DATE()});
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

TEST_F(DateTimeFunctionsTest, monthDate) {
  const auto month = [&](const std::string& dateString) {
    return evaluateOnce<int32_t, int32_t>(
        "month(c0)", {parseDate(dateString)}, {DATE()});
  };

  EXPECT_EQ(4, month("2015-04-08"));
  EXPECT_EQ(11, month("2013-11-08"));
  EXPECT_EQ(1, month("1987-01-08"));
  EXPECT_EQ(8, month("1954-08-08"));
}

TEST_F(DateTimeFunctionsTest, quarterDate) {
  const auto quarter = [&](const std::string& dateString) {
    return evaluateOnce<int32_t, int32_t>(
        "quarter(c0)", {parseDate(dateString)}, {DATE()});
  };

  EXPECT_EQ(2, quarter("2015-04-08"));
  EXPECT_EQ(4, quarter("2013-11-08"));
  EXPECT_EQ(1, quarter("1987-01-08"));
  EXPECT_EQ(3, quarter("1954-08-08"));
}

TEST_F(DateTimeFunctionsTest, nextDay) {
  const auto nextDay = [&](const std::string& date,
                           const std::string& dayOfWeek) {
    auto startDates =
        makeNullableFlatVector<int32_t>({parseDate(date)}, DATE());
    auto dayOfWeeks = makeNullableFlatVector<std::string>({dayOfWeek});

    auto result = evaluateOnce<int32_t>(
        fmt::format("next_day(c0, '{}')", dayOfWeek),
        makeRowVector({startDates}));

    auto anotherResult = evaluateOnce<int32_t>(
        "next_day(c0, c1)", makeRowVector({startDates, dayOfWeeks}));

    EXPECT_EQ(result, anotherResult);
    std::optional<std::string> res;
    if (result.has_value()) {
      res = DATE()->toString(result.value());
    }
    return res;
  };

  EXPECT_EQ(nextDay("2015-07-23", "Mon"), "2015-07-27");
  EXPECT_EQ(nextDay("2015-07-23", "mo"), "2015-07-27");
  EXPECT_EQ(nextDay("2015-07-23", "monday"), "2015-07-27");
  EXPECT_EQ(nextDay("2015-07-23", "Tue"), "2015-07-28");
  EXPECT_EQ(nextDay("2015-07-23", "tu"), "2015-07-28");
  EXPECT_EQ(nextDay("2015-07-23", "tuesday"), "2015-07-28");
  EXPECT_EQ(nextDay("2015-07-23", "we"), "2015-07-29");
  EXPECT_EQ(nextDay("2015-07-23", "wed"), "2015-07-29");
  EXPECT_EQ(nextDay("2015-07-23", "wednesday"), "2015-07-29");
  EXPECT_EQ(nextDay("2015-07-23", "Thu"), "2015-07-30");
  EXPECT_EQ(nextDay("2015-07-23", "TH"), "2015-07-30");
  EXPECT_EQ(nextDay("2015-07-23", "thursday"), "2015-07-30");
  EXPECT_EQ(nextDay("2015-07-23", "Fri"), "2015-07-24");
  EXPECT_EQ(nextDay("2015-07-23", "fr"), "2015-07-24");
  EXPECT_EQ(nextDay("2015-07-23", "friday"), "2015-07-24");
  EXPECT_EQ(nextDay("2015-07-31", "wed"), "2015-08-05");
  EXPECT_EQ(nextDay("2015-07-23", "saturday"), "2015-07-25");
  EXPECT_EQ(nextDay("2015-07-23", "sunday"), "2015-07-26");
  EXPECT_EQ(nextDay("2015-12-31", "Fri"), "2016-01-01");

  EXPECT_EQ(nextDay("2015-07-23", "xx"), std::nullopt);
  EXPECT_EQ(nextDay("2015-07-23", "\"quote"), std::nullopt);
  EXPECT_EQ(nextDay("2015-07-23", ""), std::nullopt);
}

TEST_F(DateTimeFunctionsTest, getTimestamp) {
  const auto getTimestamp = [&](const std::optional<StringView>& dateString,
                                const std::string& format) {
    return evaluateOnce<Timestamp>(
        fmt::format("get_timestamp(c0, '{}')", format), dateString);
  };

  const auto getTimestampString =
      [&](const std::optional<StringView>& dateString,
          const std::string& format) {
        return getTimestamp(dateString, format).value().toString();
      };

  EXPECT_EQ(getTimestamp("1970-01-01", "yyyy-MM-dd"), Timestamp(0, 0));
  EXPECT_EQ(
      getTimestamp("1970-01-01 00:00:00.010", "yyyy-MM-dd HH:mm:ss.SSS"),
      Timestamp::fromMillis(10));
  auto milliSeconds = (6 * 60 * 60 + 10 * 60 + 59) * 1000 + 19;
  EXPECT_EQ(
      getTimestamp("1970-01-01 06:10:59.019", "yyyy-MM-dd HH:mm:ss.SSS"),
      Timestamp::fromMillis(milliSeconds));

  EXPECT_EQ(
      getTimestampString("1970-01-01", "yyyy-MM-dd"),
      "1970-01-01T00:00:00.000000000");
  EXPECT_EQ(
      getTimestampString("1970/01/01", "yyyy/MM/dd"),
      "1970-01-01T00:00:00.000000000");
  EXPECT_EQ(
      getTimestampString("08/27/2017", "MM/dd/yyy"),
      "2017-08-27T00:00:00.000000000");
  EXPECT_EQ(
      getTimestampString("1970/01/01 12:08:59", "yyyy/MM/dd HH:mm:ss"),
      "1970-01-01T12:08:59.000000000");
  EXPECT_EQ(
      getTimestampString("2023/12/08 08:20:19", "yyyy/MM/dd HH:mm:ss"),
      "2023-12-08T08:20:19.000000000");

  // 8 hours ahead UTC.
  setQueryTimeZone("Asia/Shanghai");
  EXPECT_EQ(
      getTimestampString("1970-01-01", "yyyy-MM-dd"),
      "1969-12-31T16:00:00.000000000");
  EXPECT_EQ(
      getTimestampString("1970/01/01", "yyyy/MM/dd"),
      "1969-12-31T16:00:00.000000000");
  EXPECT_EQ(
      getTimestampString("2023/12/08 08:20:19", "yyyy/MM/dd HH:mm:ss"),
      "2023-12-08T00:20:19.000000000");

  // 8 hours behind UTC.
  setQueryTimeZone("America/Los_Angeles");
  EXPECT_EQ(
      getTimestampString("1970/01/01", "yyyy/MM/dd"),
      "1970-01-01T08:00:00.000000000");
  EXPECT_EQ(
      getTimestampString("2023/12/08 08:20:19", "yyyy/MM/dd HH:mm:ss"),
      "2023-12-08T16:20:19.000000000");

  // Parsing error.
  EXPECT_EQ(
      getTimestamp("1970-01-01 06:10:59.019", "HH:mm:ss.SSS"), std::nullopt);
  EXPECT_EQ(
      getTimestamp("1970-01-01 06:10:59.019", "yyyy/MM/dd HH:mm:ss.SSS"),
      std::nullopt);

  // Invalid date format.
  VELOX_ASSERT_THROW(
      getTimestamp("2020/01/24", "AA/MM/dd"), "Specifier A is not supported");
  VELOX_ASSERT_THROW(
      getTimestamp("2023-07-13 21:34", "yyyy-MM-dd HH:II"),
      "Specifier I is not supported");
}

TEST_F(DateTimeFunctionsTest, hour) {
  const auto hour = [&](const StringView timestampStr) {
    const auto timeStamp =
        std::make_optional(util::fromTimestampString(timestampStr));
    return evaluateOnce<int32_t>("hour(c0)", timeStamp);
  };

  EXPECT_EQ(0, hour("2024-01-08 00:23:00.001"));
  EXPECT_EQ(0, hour("2024-01-08 00:59:59.999"));
  EXPECT_EQ(1, hour("2024-01-08 01:23:00.001"));
  EXPECT_EQ(13, hour("2024-01-20 13:23:00.001"));
  EXPECT_EQ(13, hour("1969-01-01 13:23:00.001"));

  // Set time zone to Pacific/Apia (13 hours ahead of UTC).
  setQueryTimeZone("Pacific/Apia");

  EXPECT_EQ(13, hour("2024-01-08 00:23:00.001"));
  EXPECT_EQ(13, hour("2024-01-08 00:59:59.999"));
  EXPECT_EQ(14, hour("2024-01-08 01:23:00.001"));
  EXPECT_EQ(2, hour("2024-01-20 13:23:00.001"));
  EXPECT_EQ(2, hour("1969-01-01 13:23:00.001"));
}

TEST_F(DateTimeFunctionsTest, fromUnixtime) {
  const auto getUnixTime = [&](const StringView& str) {
    Timestamp t = util::fromTimestampString(str);
    return t.getSeconds();
  };

  const auto fromUnixTime = [&](const std::optional<int64_t>& unixTime,
                                const std::optional<std::string>& timeFormat) {
    return evaluateOnce<std::string>(
        "from_unixtime(c0, c1)", unixTime, timeFormat);
  };

  EXPECT_EQ(fromUnixTime(0, "yyyy-MM-dd HH:mm:ss"), "1970-01-01 00:00:00");
  EXPECT_EQ(fromUnixTime(100, "yyyy-MM-dd"), "1970-01-01");
  EXPECT_EQ(fromUnixTime(120, "yyyy-MM-dd HH:mm"), "1970-01-01 00:02");
  EXPECT_EQ(fromUnixTime(100, "yyyy-MM-dd HH:mm:ss"), "1970-01-01 00:01:40");
  EXPECT_EQ(fromUnixTime(-59, "yyyy-MM-dd HH:mm:ss"), "1969-12-31 23:59:01");
  EXPECT_EQ(fromUnixTime(3600, "yyyy"), "1970");
  EXPECT_EQ(
      fromUnixTime(getUnixTime("2020-06-30 11:29:59"), "yyyy-MM-dd HH:mm:ss"),
      "2020-06-30 11:29:59");
  EXPECT_EQ(
      fromUnixTime(getUnixTime("2020-06-30 11:29:59"), "yyyy-MM-dd"),
      "2020-06-30");
  EXPECT_EQ(fromUnixTime(getUnixTime("2020-06-30 11:29:59"), "MM-dd"), "06-30");
  EXPECT_EQ(
      fromUnixTime(getUnixTime("2020-06-30 11:29:59"), "HH:mm:ss"), "11:29:59");
  EXPECT_EQ(
      fromUnixTime(getUnixTime("2020-06-30 23:59:59"), "yyyy-MM-dd HH:mm:ss"),
      "2020-06-30 23:59:59");

// In debug mode, Timestamp constructor will throw exception if range check
// fails.
#ifdef NDEBUG
  // Integer overflow in the internal conversion from seconds to milliseconds.
  EXPECT_EQ(
      fromUnixTime(std::numeric_limits<int64_t>::max(), "yyyy-MM-dd HH:mm:ss"),
      "1969-12-31 23:59:59");
#endif

  // 8 hours ahead UTC.
  setQueryTimeZone("Asia/Shanghai");
  EXPECT_EQ(fromUnixTime(0, "yyyy-MM-dd HH:mm:ss"), "1970-01-01 08:00:00");
  EXPECT_EQ(fromUnixTime(120, "yyyy-MM-dd HH:mm"), "1970-01-01 08:02");
  EXPECT_EQ(fromUnixTime(-59, "yyyy-MM-dd HH:mm:ss"), "1970-01-01 07:59:01");
  EXPECT_EQ(
      fromUnixTime(getUnixTime("2014-07-21 16:00:00"), "yyyy-MM-dd"),
      "2014-07-22");
  EXPECT_EQ(
      fromUnixTime(getUnixTime("2020-06-30 23:59:59"), "yyyy-MM-dd HH:mm:ss"),
      "2020-07-01 07:59:59");

  // Invalid format.
  VELOX_ASSERT_THROW(
      fromUnixTime(0, "yyyy-AA"), "Specifier A is not supported.");
  VELOX_ASSERT_THROW(
      fromUnixTime(0, "FF/MM/dd"), "Specifier F is not supported");
  VELOX_ASSERT_THROW(
      fromUnixTime(0, "yyyy-MM-dd HH:II"), "Specifier I is not supported");
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
