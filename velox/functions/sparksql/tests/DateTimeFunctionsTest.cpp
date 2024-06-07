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
#include "velox/type/Timestamp.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::functions::sparksql::test {
namespace {

std::string timestampToString(Timestamp ts) {
  TimestampToStringOptions options;
  options.mode = TimestampToStringOptions::Mode::kFull;
  std::string result;
  result.resize(getMaxStringLength(options));
  const auto view = Timestamp::tsToStringView(ts, options, result.data());
  result.resize(view.size());
  return result;
}

class DateTimeFunctionsTest : public SparkFunctionBaseTest {
 public:
  static constexpr int32_t kMin = std::numeric_limits<int32_t>::min();
  static constexpr int32_t kMax = std::numeric_limits<int32_t>::max();
  static constexpr int16_t kMinSmallint = std::numeric_limits<int16_t>::min();
  static constexpr int16_t kMaxSmallint = std::numeric_limits<int16_t>::max();
  static constexpr int8_t kMinTinyint = std::numeric_limits<int8_t>::min();
  static constexpr int8_t kMaxTinyint = std::numeric_limits<int8_t>::max();
  static constexpr int64_t kMinBigint = std::numeric_limits<int64_t>::min();
  static constexpr int64_t kMaxBigint = std::numeric_limits<int64_t>::max();

 protected:
  void setQueryTimeZone(const std::string& timeZone) {
    queryCtx_->testingOverrideConfigUnsafe({
        {core::QueryConfig::kSessionTimezone, timeZone},
        {core::QueryConfig::kAdjustTimestampToTimezone, "true"},
    });
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

TEST_F(DateTimeFunctionsTest, toUtcTimestamp) {
  const auto toUtcTimestamp = [&](const StringView& ts, const std::string& tz) {
    auto timestamp = std::make_optional<Timestamp>(parseTimestamp(ts));
    auto result = evaluateOnce<Timestamp>(
        "to_utc_timestamp(c0, c1)",
        timestamp,
        std::make_optional<std::string>(tz));
    return timestampToString(result.value());
  };
  EXPECT_EQ(
      "2015-07-24T07:00:00.000000000",
      toUtcTimestamp("2015-07-24 00:00:00", "America/Los_Angeles"));
  EXPECT_EQ(
      "2015-01-24T08:00:00.000000000",
      toUtcTimestamp("2015-01-24 00:00:00", "America/Los_Angeles"));
  EXPECT_EQ(
      "2015-01-24T00:00:00.000000000",
      toUtcTimestamp("2015-01-24 00:00:00", "UTC"));
  EXPECT_EQ(
      "2015-01-24T00:00:00.000000000",
      toUtcTimestamp("2015-01-24 05:30:00", "Asia/Kolkata"));
  EXPECT_EQ(
      "2015-01-23T16:00:00.000000000",
      toUtcTimestamp("2015-01-24 00:00:00", "+08:00"));
  VELOX_ASSERT_THROW(
      toUtcTimestamp("2015-01-24 00:00:00", "Asia/Ooty"),
      "Unknown time zone: 'Asia/Ooty'");
}

TEST_F(DateTimeFunctionsTest, fromUtcTimestamp) {
  const auto fromUtcTimestamp = [&](const StringView& ts,
                                    const std::string& tz) {
    auto timestamp = std::make_optional<Timestamp>(parseTimestamp(ts));
    auto result = evaluateOnce<Timestamp>(
        "from_utc_timestamp(c0, c1)",
        timestamp,
        std::make_optional<std::string>(tz));
    return timestampToString(result.value());
  };
  EXPECT_EQ(
      "2015-07-24T00:00:00.000000000",
      fromUtcTimestamp("2015-07-24 07:00:00", "America/Los_Angeles"));
  EXPECT_EQ(
      "2015-01-24T00:00:00.000000000",
      fromUtcTimestamp("2015-01-24 08:00:00", "America/Los_Angeles"));
  EXPECT_EQ(
      "2015-01-24T00:00:00.000000000",
      fromUtcTimestamp("2015-01-24 00:00:00", "UTC"));
  EXPECT_EQ(
      "2015-01-24T05:30:00.000000000",
      fromUtcTimestamp("2015-01-24 00:00:00", "Asia/Kolkata"));
  EXPECT_EQ(
      "2015-01-24T08:00:00.000000000",
      fromUtcTimestamp("2015-01-24 00:00:00", "+08:00"));
  VELOX_ASSERT_THROW(
      fromUtcTimestamp("2015-01-24 00:00:00", "Asia/Ooty"),
      "Unknown time zone: 'Asia/Ooty'");
}

TEST_F(DateTimeFunctionsTest, toFromUtcTimestamp) {
  const auto toFromUtcTimestamp = [&](const StringView& ts,
                                      const std::string& tz) {
    auto timestamp = std::make_optional<Timestamp>(parseTimestamp(ts));
    auto result = evaluateOnce<Timestamp>(
        "to_utc_timestamp(from_utc_timestamp(c0, c1), c1)",
        timestamp,
        std::make_optional<std::string>(tz));
    return timestampToString(result.value());
  };
  EXPECT_EQ(
      "2015-07-24T07:00:00.000000000",
      toFromUtcTimestamp("2015-07-24 07:00:00", "America/Los_Angeles"));
  EXPECT_EQ(
      "2015-01-24T08:00:00.000000000",
      toFromUtcTimestamp("2015-01-24 08:00:00", "America/Los_Angeles"));
  EXPECT_EQ(
      "2015-01-24T00:00:00.000000000",
      toFromUtcTimestamp("2015-01-24 00:00:00", "UTC"));
  EXPECT_EQ(
      "2015-01-24T00:00:00.000000000",
      toFromUtcTimestamp("2015-01-24 00:00:00", "Asia/Kolkata"));
  VELOX_ASSERT_THROW(
      toFromUtcTimestamp("2015-01-24 00:00:00", "Asia/Ooty"),
      "Unknown time zone: 'Asia/Ooty'");
}

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
    return evaluateOnce<int32_t>("year(c0)", DATE(), date);
  };
  EXPECT_EQ(std::nullopt, year(std::nullopt));
  EXPECT_EQ(1970, year(parseDate("1970-05-05")));
  EXPECT_EQ(1969, year(parseDate("1969-12-31")));
  EXPECT_EQ(2020, year(parseDate("2020-01-01")));
  EXPECT_EQ(1920, year(parseDate("1920-01-01")));
}

TEST_F(DateTimeFunctionsTest, weekOfYear) {
  const auto weekOfYear = [&](const char* dateString) {
    auto date = std::make_optional(parseDate(dateString));
    return evaluateOnce<int32_t>("week_of_year(c0)", DATE(), date).value();
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

TEST_F(DateTimeFunctionsTest, unixDate) {
  const auto unixDate = [&](const std::string& date) {
    return evaluateOnce<int32_t>(
        "unix_date(c0)", DATE(), std::make_optional<int32_t>(parseDate(date)));
  };

  EXPECT_EQ(unixDate("1970-01-01"), 0);
  EXPECT_EQ(unixDate("1970-01-02"), 1);
  EXPECT_EQ(unixDate("1969-12-31"), -1);
  EXPECT_EQ(unixDate("1970-02-01"), 31);
  EXPECT_EQ(unixDate("1971-01-31"), 395);
  EXPECT_EQ(unixDate("1971-01-01"), 365);
  EXPECT_EQ(unixDate("1972-02-29"), 365 + 365 + 30 + 29);
  EXPECT_EQ(unixDate("1971-03-01"), 365 + 30 + 28 + 1);
  EXPECT_EQ(unixDate("5881580-07-11"), kMax);
  EXPECT_EQ(unixDate("-5877641-06-23"), kMin);
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
  EXPECT_EQ(makeDate(1920, 1, 25), parseDate("1920-01-25"));
  EXPECT_EQ(makeDate(-10, 1, 30), parseDate("-0010-01-30"));

  auto errorMessage = fmt::format("Date out of range: {}-12-15", kMax);
  VELOX_ASSERT_THROW(makeDate(kMax, 12, 15), errorMessage);

  constexpr const int32_t kJodaMaxYear{292278994};
  VELOX_ASSERT_THROW(makeDate(kJodaMaxYear - 10, 12, 15), "Integer overflow");

  VELOX_ASSERT_THROW(makeDate(2021, 13, 1), "Date out of range: 2021-13-1");
  VELOX_ASSERT_THROW(makeDate(2022, 3, 35), "Date out of range: 2022-3-35");

  VELOX_ASSERT_THROW(makeDate(2023, 4, 31), "Date out of range: 2023-4-31");
  EXPECT_EQ(makeDate(2023, 3, 31), parseDate("2023-03-31"));

  VELOX_ASSERT_THROW(makeDate(2023, 2, 29), "Date out of range: 2023-2-29");
  EXPECT_EQ(makeDate(2023, 3, 29), parseDate("2023-03-29"));
}

TEST_F(DateTimeFunctionsTest, lastDay) {
  const auto lastDayFunc = [&](const std::optional<int32_t> date) {
    return evaluateOnce<int32_t>("last_day(c0)", DATE(), date);
  };

  const auto lastDay = [&](const std::string& dateStr) {
    return lastDayFunc(parseDate(dateStr));
  };

  const auto parseDateStr = [&](const std::string& dateStr) {
    return parseDate(dateStr);
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

TEST_F(DateTimeFunctionsTest, dateFromUnixDate) {
  const auto dateFromUnixDate = [&](std::optional<int32_t> value) {
    return evaluateOnce<int32_t>("date_from_unix_date(c0)", value);
  };

  // Basic tests
  EXPECT_EQ(parseDate("1970-01-01"), dateFromUnixDate(0));
  EXPECT_EQ(parseDate("1970-01-02"), dateFromUnixDate(1));
  EXPECT_EQ(parseDate("1969-12-31"), dateFromUnixDate(-1));
  EXPECT_EQ(parseDate("1970-02-01"), dateFromUnixDate(31));
  EXPECT_EQ(parseDate("1971-01-31"), dateFromUnixDate(395));
  EXPECT_EQ(parseDate("1971-01-01"), dateFromUnixDate(365));

  // Leap year tests
  EXPECT_EQ(parseDate("1972-02-29"), dateFromUnixDate(365 + 365 + 30 + 29));
  EXPECT_EQ(parseDate("1971-03-01"), dateFromUnixDate(365 + 30 + 28 + 1));

  // Min and max value tests
  EXPECT_EQ(parseDate("5881580-07-11"), dateFromUnixDate(kMax));
  EXPECT_EQ(parseDate("-5877641-06-23"), dateFromUnixDate(kMin));
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
    return evaluateOnce<int32_t>("dayofyear(c0)", DATE(), date);
  };
  EXPECT_EQ(std::nullopt, day(std::nullopt));
  EXPECT_EQ(100, day(parseDate("2016-04-09")));
  EXPECT_EQ(235, day(parseDate("2023-08-23")));
  EXPECT_EQ(1, day(parseDate("1970-01-01")));
}

TEST_F(DateTimeFunctionsTest, dayOfMonth) {
  const auto day = [&](std::optional<int32_t> date) {
    return evaluateOnce<int32_t>("dayofmonth(c0)", DATE(), date);
  };
  EXPECT_EQ(std::nullopt, day(std::nullopt));
  EXPECT_EQ(30, day(parseDate("2009-07-30")));
  EXPECT_EQ(23, day(parseDate("2023-08-23")));
}

TEST_F(DateTimeFunctionsTest, dayOfWeekDate) {
  const auto dayOfWeek = [&](std::optional<int32_t> date) {
    return evaluateOnce<int32_t>("dayofweek(c0)", DATE(), date);
  };

  EXPECT_EQ(std::nullopt, dayOfWeek(std::nullopt));
  EXPECT_EQ(5, dayOfWeek(0));
  EXPECT_EQ(4, dayOfWeek(-1));
  EXPECT_EQ(7, dayOfWeek(-40));
  EXPECT_EQ(5, dayOfWeek(parseDate("2009-07-30")));
  EXPECT_EQ(1, dayOfWeek(parseDate("2023-08-20")));
  EXPECT_EQ(2, dayOfWeek(parseDate("2023-08-21")));
  EXPECT_EQ(3, dayOfWeek(parseDate("2023-08-22")));
  EXPECT_EQ(4, dayOfWeek(parseDate("2023-08-23")));
  EXPECT_EQ(5, dayOfWeek(parseDate("2023-08-24")));
  EXPECT_EQ(6, dayOfWeek(parseDate("2023-08-25")));
  EXPECT_EQ(7, dayOfWeek(parseDate("2023-08-26")));
  EXPECT_EQ(1, dayOfWeek(parseDate("2023-08-27")));
  EXPECT_EQ(6, dayOfWeek(parseDate("2011-05-06")));
  EXPECT_EQ(4, dayOfWeek(parseDate("2015-04-08")));
  EXPECT_EQ(7, dayOfWeek(parseDate("2017-05-27")));
  EXPECT_EQ(6, dayOfWeek(parseDate("1582-10-15")));
}

TEST_F(DateTimeFunctionsTest, weekdayDate) {
  const auto weekday = [&](std::optional<int32_t> value) {
    return evaluateOnce<int32_t>("weekday(c0)", DATE(), value);
  };

  EXPECT_EQ(3, weekday(0));
  EXPECT_EQ(2, weekday(-1));
  EXPECT_EQ(5, weekday(-40));
  EXPECT_EQ(3, weekday(parseDate("2009-07-30")));
  EXPECT_EQ(6, weekday(parseDate("2023-08-20")));
  EXPECT_EQ(0, weekday(parseDate("2023-08-21")));
  EXPECT_EQ(1, weekday(parseDate("2023-08-22")));
  EXPECT_EQ(2, weekday(parseDate("2023-08-23")));
  EXPECT_EQ(3, weekday(parseDate("2023-08-24")));
  EXPECT_EQ(4, weekday(parseDate("2023-08-25")));
  EXPECT_EQ(5, weekday(parseDate("2023-08-26")));
  EXPECT_EQ(6, weekday(parseDate("2023-08-27")));
  EXPECT_EQ(5, weekday(parseDate("2017-05-27")));
  EXPECT_EQ(2, weekday(parseDate("2015-04-08")));
  EXPECT_EQ(4, weekday(parseDate("2013-11-08")));
  EXPECT_EQ(4, weekday(parseDate("2011-05-06")));
  EXPECT_EQ(4, weekday(parseDate("1582-10-15")));
}

TEST_F(DateTimeFunctionsTest, dateDiffDate) {
  const auto dateDiff = [&](std::optional<int32_t> endDate,
                            std::optional<int32_t> startDate) {
    return evaluateOnce<int32_t>(
        "datediff(c0, c1)", {DATE(), DATE()}, endDate, startDate);
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
  const auto addMonths = [&](const std::string& dateString,
                             std::optional<int32_t> value) {
    return evaluateOnce<int32_t>(
        "add_months(c0, c1)",
        {DATE(), INTEGER()},
        std::make_optional(parseDate(dateString)),
        value);
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
    return evaluateOnce<int32_t>(
        "month(c0)", DATE(), std::make_optional(parseDate(dateString)));
  };

  EXPECT_EQ(4, month("2015-04-08"));
  EXPECT_EQ(11, month("2013-11-08"));
  EXPECT_EQ(1, month("1987-01-08"));
  EXPECT_EQ(8, month("1954-08-08"));
}

TEST_F(DateTimeFunctionsTest, quarterDate) {
  const auto quarter = [&](const std::string& dateString) {
    return evaluateOnce<int32_t>(
        "quarter(c0)", DATE(), std::make_optional(parseDate(dateString)));
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
    const auto timeStamp = std::make_optional(parseTimestamp(timestampStr));
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

TEST_F(DateTimeFunctionsTest, minute) {
  const auto minute = [&](const StringView& timestampStr) {
    const auto timeStamp = std::make_optional(parseTimestamp(timestampStr));
    return evaluateOnce<int32_t>("minute(c0)", timeStamp);
  };

  EXPECT_EQ(23, minute("2024-01-08 00:23:00.001"));
  EXPECT_EQ(59, minute("2024-01-08 00:59:59.999"));
  EXPECT_EQ(10, minute("2015-04-08 13:10:15"));
  EXPECT_EQ(43, minute("1969-01-01 13:43:00.001"));

  // Set time zone to Pacific/Apia (13 hours ahead of UTC).
  setQueryTimeZone("Pacific/Apia");

  EXPECT_EQ(23, minute("2024-01-08 00:23:00.001"));
  EXPECT_EQ(59, minute("2024-01-08 00:59:59.999"));
  EXPECT_EQ(10, minute("2015-04-08 13:10:15"));
  EXPECT_EQ(43, minute("1969-01-01 13:43:00.001"));

  // Set time zone to Asia/Kolkata (5.5 hours ahead of UTC).
  setQueryTimeZone("Asia/Kolkata");

  EXPECT_EQ(53, minute("2024-01-08 00:23:00.001"));
  EXPECT_EQ(29, minute("2024-01-08 00:59:59.999"));
  EXPECT_EQ(40, minute("2015-04-08 13:10:15"));
  EXPECT_EQ(13, minute("1969-01-01 13:43:00.001"));
}

TEST_F(DateTimeFunctionsTest, second) {
  const auto second = [&](const StringView& timestampStr) {
    const auto timeStamp = std::make_optional(parseTimestamp(timestampStr));
    return evaluateOnce<int32_t>("second(c0)", timeStamp);
  };

  EXPECT_EQ(0, second("2024-01-08 00:23:00.001"));
  EXPECT_EQ(59, second("2024-01-08 00:59:59.999"));
  EXPECT_EQ(15, second("2015-04-08 13:10:15"));
  EXPECT_EQ(0, second("1969-01-01 13:43:00.001"));
}

TEST_F(DateTimeFunctionsTest, fromUnixtime) {
  const auto getUnixTime = [&](const StringView& str) {
    Timestamp t = parseTimestamp(str);
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
// In debug mode, Timestamp constructor will throw exception if range check
// fails.
#ifdef NDEBUG
  // Integer overflow in the internal conversion from seconds to milliseconds.
  EXPECT_EQ(
      fromUnixTime(std::numeric_limits<int64_t>::max(), "yyyy-MM-dd HH:mm:ss"),
      "1970-01-01 07:59:59");
#endif

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

TEST_F(DateTimeFunctionsTest, makeYMInterval) {
  const auto fromYearAndMonth = [&](const std::optional<int32_t>& year,
                                    const std::optional<int32_t>& month) {
    auto result =
        evaluateOnce<int32_t>("make_ym_interval(c0, c1)", year, month);
    VELOX_CHECK(result.has_value());
    return INTERVAL_YEAR_MONTH()->valueToString(result.value());
  };
  const auto fromYear = [&](const std::optional<int32_t>& year) {
    auto result = evaluateOnce<int32_t>("make_ym_interval(c0)", year);
    VELOX_CHECK(result.has_value());
    return INTERVAL_YEAR_MONTH()->valueToString(result.value());
  };

  EXPECT_EQ(fromYearAndMonth(1, 2), "1-2");
  EXPECT_EQ(fromYearAndMonth(0, 1), "0-1");
  EXPECT_EQ(fromYearAndMonth(1, 100), "9-4");
  EXPECT_EQ(fromYear(0), "0-0");
  EXPECT_EQ(fromYear(178956970), "178956970-0");
  EXPECT_EQ(fromYear(-178956970), "-178956970-0");
  {
    // Test signature for no year and month.
    auto result = evaluateOnce<int32_t>(
        "make_ym_interval()",
        makeRowVector(ROW({}), 1),
        std::nullopt,
        {INTERVAL_YEAR_MONTH()});
    VELOX_CHECK(result.has_value());
    EXPECT_EQ(INTERVAL_YEAR_MONTH()->valueToString(result.value()), "0-0");
  }

  VELOX_ASSERT_THROW(
      fromYearAndMonth(178956970, 8),
      "Integer overflow in make_ym_interval(178956970, 8)");
  VELOX_ASSERT_THROW(
      fromYearAndMonth(-178956970, -9),
      "Integer overflow in make_ym_interval(-178956970, -9)");
  VELOX_ASSERT_THROW(
      fromYearAndMonth(178956971, 0),
      "Integer overflow in make_ym_interval(178956971, 0)");
  VELOX_ASSERT_THROW(
      fromYear(178956971), "Integer overflow in make_ym_interval(178956971)");
  VELOX_ASSERT_THROW(
      fromYear(-178956971), "Integer overflow in make_ym_interval(-178956971)");
}

TEST_F(DateTimeFunctionsTest, yearOfWeek) {
  const auto yearOfWeek = [&](std::optional<int32_t> date) {
    return evaluateOnce<int32_t>("year_of_week(c0)", DATE(), date);
  };
  EXPECT_EQ(1970, yearOfWeek(0));
  EXPECT_EQ(1970, yearOfWeek(-1));
  EXPECT_EQ(1969, yearOfWeek(-4));
  EXPECT_EQ(1970, yearOfWeek(-3));
  EXPECT_EQ(1970, yearOfWeek(365));
  EXPECT_EQ(1970, yearOfWeek(367));
  EXPECT_EQ(1971, yearOfWeek(368));
  EXPECT_EQ(2005, yearOfWeek(parseDate("2006-01-01")));
  EXPECT_EQ(2006, yearOfWeek(parseDate("2006-01-02")));
}

TEST_F(DateTimeFunctionsTest, unixSeconds) {
  const auto unixSeconds = [&](const StringView time) {
    return evaluateOnce<int64_t>(
        "unix_seconds(c0)", std::make_optional(parseTimestamp(time)));
  };
  EXPECT_EQ(unixSeconds("1970-01-01 00:00:01"), 1);
  EXPECT_EQ(unixSeconds("1970-01-01 00:00:00.000127"), 0);
  EXPECT_EQ(unixSeconds("1969-12-31 23:59:59.999872"), -1);
  EXPECT_EQ(unixSeconds("1970-01-01 00:35:47.483647"), 2147);
  EXPECT_EQ(unixSeconds("1971-01-01 00:00:01.483647"), 31536001);
}

TEST_F(DateTimeFunctionsTest, microsToTimestamp) {
  const auto microsToTimestamp = [&](std::optional<int64_t> micros) {
    return evaluateOnce<Timestamp>("timestamp_micros(c0)", micros);
  };
  EXPECT_EQ(microsToTimestamp(1000000), parseTimestamp("1970-01-01 00:00:01"));
  EXPECT_EQ(
      microsToTimestamp(1230219000123123),
      parseTimestamp("2008-12-25 15:30:00.123123"));

  EXPECT_EQ(
      microsToTimestamp(kMaxTinyint),
      parseTimestamp("1970-01-01 00:00:00.000127"));
  EXPECT_EQ(
      microsToTimestamp(kMinTinyint),
      parseTimestamp("1969-12-31 23:59:59.999872"));
  EXPECT_EQ(
      microsToTimestamp(kMaxSmallint),
      parseTimestamp("1970-01-01 00:00:00.032767"));
  EXPECT_EQ(
      microsToTimestamp(kMinSmallint),
      parseTimestamp("1969-12-31 23:59:59.967232"));
  EXPECT_EQ(
      microsToTimestamp(kMax), parseTimestamp("1970-01-01 00:35:47.483647"));
  EXPECT_EQ(
      microsToTimestamp(kMin), parseTimestamp("1969-12-31 23:24:12.516352"));
  EXPECT_EQ(
      microsToTimestamp(kMaxBigint),
      parseTimestamp("294247-01-10 04:00:54.775807"));
  EXPECT_EQ(
      microsToTimestamp(kMinBigint),
      parseTimestamp("-290308-12-21 19:59:05.224192"));
}

TEST_F(DateTimeFunctionsTest, millisToTimestamp) {
  const auto millisToTimestamp = [&](int64_t millis) {
    return evaluateOnce<Timestamp, int64_t>("timestamp_millis(c0)", millis);
  };
  EXPECT_EQ(millisToTimestamp(1000), parseTimestamp("1970-01-01 00:00:01"));
  EXPECT_EQ(
      millisToTimestamp(1230219000123),
      parseTimestamp("2008-12-25 15:30:00.123"));

  EXPECT_EQ(
      millisToTimestamp(kMaxTinyint),
      parseTimestamp("1970-01-01 00:00:00.127"));
  EXPECT_EQ(
      millisToTimestamp(kMinTinyint),
      parseTimestamp("1969-12-31 23:59:59.872"));
  EXPECT_EQ(
      millisToTimestamp(kMaxSmallint),
      parseTimestamp("1970-01-01 00:00:32.767"));
  EXPECT_EQ(
      millisToTimestamp(kMinSmallint),
      parseTimestamp("1969-12-31 23:59:27.232"));
  EXPECT_EQ(millisToTimestamp(kMax), parseTimestamp("1970-01-25 20:31:23.647"));
  EXPECT_EQ(millisToTimestamp(kMin), parseTimestamp("1969-12-07 03:28:36.352"));
  EXPECT_EQ(
      millisToTimestamp(kMaxBigint),
      parseTimestamp("292278994-08-17 07:12:55.807"));
  EXPECT_EQ(
      millisToTimestamp(kMinBigint),
      parseTimestamp("-292275055-05-16 16:47:04.192"));
}

TEST_F(DateTimeFunctionsTest, timestampToMicros) {
  const auto timestampToMicros = [&](const StringView time) {
    return evaluateOnce<int64_t, Timestamp>(
        "unix_micros(c0)", parseTimestamp(time));
  };
  EXPECT_EQ(timestampToMicros("1970-01-01 00:00:01"), 1000000);
  EXPECT_EQ(timestampToMicros("2008-12-25 15:30:00.123123"), 1230219000123123);

  EXPECT_EQ(timestampToMicros("1970-01-01 00:00:00.000127"), kMaxTinyint);
  EXPECT_EQ(timestampToMicros("1969-12-31 23:59:59.999872"), kMinTinyint);
  EXPECT_EQ(timestampToMicros("1970-01-01 00:00:00.032767"), kMaxSmallint);
  EXPECT_EQ(timestampToMicros("1969-12-31 23:59:59.967232"), kMinSmallint);
  EXPECT_EQ(timestampToMicros("1970-01-01 00:35:47.483647"), kMax);
  EXPECT_EQ(timestampToMicros("1969-12-31 23:24:12.516352"), kMin);
  EXPECT_EQ(timestampToMicros("294247-01-10 04:00:54.775807"), kMaxBigint);
  EXPECT_EQ(
      timestampToMicros("-290308-12-21 19:59:06.224192"), kMinBigint + 1000000);
}

TEST_F(DateTimeFunctionsTest, timestampToMillis) {
  const auto timestampToMillis = [&](const std::string& time) {
    return evaluateOnce<int64_t, Timestamp>(
        "unix_millis(c0)", parseTimestamp(time));
  };
  EXPECT_EQ(timestampToMillis("1970-01-01 00:00:01"), 1000);
  EXPECT_EQ(timestampToMillis("2008-12-25 15:30:00.123"), 1230219000123);

  EXPECT_EQ(timestampToMillis("1970-01-01 00:00:00.127"), kMaxTinyint);
  EXPECT_EQ(timestampToMillis("1969-12-31 23:59:59.872"), kMinTinyint);
  EXPECT_EQ(timestampToMillis("1970-01-01 00:00:32.767"), kMaxSmallint);
  EXPECT_EQ(timestampToMillis("1969-12-31 23:59:27.232"), kMinSmallint);
  EXPECT_EQ(timestampToMillis("1970-01-25 20:31:23.647"), kMax);
  EXPECT_EQ(timestampToMillis("1969-12-07 03:28:36.352"), kMin);
  EXPECT_EQ(timestampToMillis("292278994-08-17 07:12:55.807"), kMaxBigint);
  EXPECT_EQ(timestampToMillis("-292275055-05-16 16:47:04.192"), kMinBigint);
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
