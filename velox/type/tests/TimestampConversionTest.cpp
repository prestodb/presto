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

#include "velox/type/TimestampConversion.h"
#include <gmock/gmock.h>
#include "velox/common/base/VeloxException.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/type/Timestamp.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::util {
namespace {

Timestamp parseTimestamp(
    const StringView& timestamp,
    TimestampParseMode parseMode = TimestampParseMode::kPrestoCast) {
  return fromTimestampString(timestamp, parseMode)
      .thenOrThrow(folly::identity, [&](const Status& status) {
        VELOX_USER_FAIL("{}", status.message());
      });
}

int32_t parseDate(const StringView& str, ParseMode mode) {
  return fromDateString(str.data(), str.size(), mode)
      .thenOrThrow(folly::identity, [&](const Status& status) {
        VELOX_USER_FAIL("{}", status.message());
      });
}

std::pair<Timestamp, const tz::TimeZone*> parseTimestampWithTimezone(
    const StringView& str,
    TimestampParseMode parseMode = TimestampParseMode::kPrestoCast) {
  return fromTimestampWithTimezoneString(str.data(), str.size(), parseMode)
      .thenOrThrow(folly::identity, [&](const Status& status) {
        VELOX_USER_FAIL("{}", status.message());
      });
}

TEST(DateTimeUtilTest, fromDate) {
  auto testDaysSinceEpochFromDate =
      [](int32_t year, int32_t month, int32_t day) {
        Expected<int64_t> daysSinceEpoch =
            util::daysSinceEpochFromDate(year, month, day);
        EXPECT_FALSE(daysSinceEpoch.hasError());
        return daysSinceEpoch.value();
      };
  EXPECT_EQ(0, testDaysSinceEpochFromDate(1970, 1, 1));
  EXPECT_EQ(1, testDaysSinceEpochFromDate(1970, 1, 2));
  EXPECT_EQ(365, testDaysSinceEpochFromDate(1971, 1, 1));
  EXPECT_EQ(730, testDaysSinceEpochFromDate(1972, 1, 1)); // leap year.
  EXPECT_EQ(1096, testDaysSinceEpochFromDate(1973, 1, 1));

  EXPECT_EQ(10957, testDaysSinceEpochFromDate(2000, 1, 1));
  EXPECT_EQ(18474, testDaysSinceEpochFromDate(2020, 7, 31));

  // Before unix epoch.
  EXPECT_EQ(-1, testDaysSinceEpochFromDate(1969, 12, 31));
  EXPECT_EQ(-365, testDaysSinceEpochFromDate(1969, 1, 1));
  EXPECT_EQ(-731, testDaysSinceEpochFromDate(1968, 1, 1)); // leap year.
  EXPECT_EQ(-719528, testDaysSinceEpochFromDate(0, 1, 1));

  // Negative year - BC.
  EXPECT_EQ(-719529, testDaysSinceEpochFromDate(-1, 12, 31));
  EXPECT_EQ(-719893, testDaysSinceEpochFromDate(-1, 1, 1));
}

TEST(DateTimeUtilTest, fromDateInvalid) {
  auto testDaysSinceEpochFromDateInvalid =
      [](int32_t year, int32_t month, int32_t day, const std::string& error) {
        Expected<int64_t> expected =
            util::daysSinceEpochFromDate(year, month, day);
        EXPECT_TRUE(expected.hasError());
        EXPECT_TRUE(expected.error().isUserError());
        EXPECT_EQ(expected.error().message(), error);
      };
  EXPECT_NO_THROW(testDaysSinceEpochFromDateInvalid(
      1970, 1, -1, "Date out of range: 1970-1--1"));
  EXPECT_NO_THROW(testDaysSinceEpochFromDateInvalid(
      1970, -1, 1, "Date out of range: 1970--1-1"));
  EXPECT_NO_THROW(testDaysSinceEpochFromDateInvalid(
      1970, 0, 1, "Date out of range: 1970-0-1"));
  EXPECT_NO_THROW(testDaysSinceEpochFromDateInvalid(
      1970, 13, 1, "Date out of range: 1970-13-1"));
  EXPECT_NO_THROW(testDaysSinceEpochFromDateInvalid(
      1970, 1, 32, "Date out of range: 1970-1-32"));
  EXPECT_NO_THROW(testDaysSinceEpochFromDateInvalid(
      1970, 2, 29, "Date out of range: 1970-2-29")); // non-leap.
  EXPECT_NO_THROW(testDaysSinceEpochFromDateInvalid(
      1970, 6, 31, "Date out of range: 1970-6-31"));
}

TEST(DateTimeUtilTest, daysSinceEpochFromWeekOfMonthDateLenient) {
  auto daysSinceEpoch =
      [](int32_t year, int32_t month, int32_t weekOfMonth, int32_t dayOfWeek) {
        auto result = util::daysSinceEpochFromWeekOfMonthDate(
            year, month, weekOfMonth, dayOfWeek, true);
        EXPECT_TRUE(!result.hasError());
        return result.value();
      };

  EXPECT_EQ(4, daysSinceEpoch(1970, 1, 2, 1));
  EXPECT_EQ(361, daysSinceEpoch(1971, 1, 1, 1));
  EXPECT_EQ(396, daysSinceEpoch(1971, 2, 1, 1));

  EXPECT_EQ(10952, daysSinceEpoch(2000, 1, 1, 1));
  EXPECT_EQ(19905, daysSinceEpoch(2024, 7, 1, 1));

  // Before unix epoch.
  EXPECT_EQ(-3, daysSinceEpoch(1970, 1, 1, 1));
  EXPECT_EQ(-2, daysSinceEpoch(1970, 1, 1, 2));
  EXPECT_EQ(-31, daysSinceEpoch(1969, 12, 1, 1));
  EXPECT_EQ(-367, daysSinceEpoch(1969, 1, 1, 1));
  EXPECT_EQ(-724, daysSinceEpoch(1968, 1, 2, 1));
  EXPECT_EQ(-719533, daysSinceEpoch(0, 1, 1, 1));

  // Negative year - BC.
  EXPECT_EQ(-719561, daysSinceEpoch(-1, 12, 1, 1));
  EXPECT_EQ(-719897, daysSinceEpoch(-1, 1, 1, 1));

  // Day in the previous month.
  EXPECT_EQ(19783, daysSinceEpoch(2024, 2, 5, 5));
  // Day in the next month.
  EXPECT_EQ(19751, daysSinceEpoch(2024, 2, 1, 1));

  // Out of range day of week.
  EXPECT_EQ(338, daysSinceEpoch(1970, 12, 1, 0));
  EXPECT_EQ(337, daysSinceEpoch(1970, 12, 1, -1));
  EXPECT_EQ(337, daysSinceEpoch(1970, 12, 1, -8));

  EXPECT_EQ(332, daysSinceEpoch(1970, 12, 1, 8));
  EXPECT_EQ(333, daysSinceEpoch(1970, 12, 1, 9));
  EXPECT_EQ(336, daysSinceEpoch(1970, 12, 1, 19));

  // Out of range month.
  EXPECT_EQ(-3, daysSinceEpoch(1970, 1, 1, 1));
  EXPECT_EQ(207, daysSinceEpoch(1970, 8, 1, 1));
  EXPECT_EQ(361, daysSinceEpoch(1970, 13, 1, 1));

  EXPECT_EQ(-31, daysSinceEpoch(1970, 0, 1, 1));
  EXPECT_EQ(-66, daysSinceEpoch(1970, -1, 1, 1));
  EXPECT_EQ(-430, daysSinceEpoch(1970, -13, 1, 1));

  // Out of range year.
  auto result =
      util::daysSinceEpochFromWeekOfMonthDate(292278995, 1, 1, 1, true);
  EXPECT_EQ(result.error().message(), "Date out of range: 292278995-1-1");
}

TEST(DateTimeUtilTest, extractISODayOfTheWeek) {
  EXPECT_EQ(
      4, util::extractISODayOfTheWeek(std::numeric_limits<int64_t>::max()));
  EXPECT_EQ(
      3, util::extractISODayOfTheWeek(std::numeric_limits<int64_t>::min()));
  EXPECT_EQ(1, util::extractISODayOfTheWeek(-10));
  EXPECT_EQ(7, util::extractISODayOfTheWeek(10));
}

TEST(DateTimeUtilTest, daysSinceEpochFromWeekOfMonthDateNonLenient) {
  auto daysSinceEpochReturnError = [](int32_t year,
                                      int32_t month,
                                      int32_t weekOfMonth,
                                      int32_t dayOfWeek,
                                      const std::string& error) {
    auto result = util::daysSinceEpochFromWeekOfMonthDate(
        year, month, weekOfMonth, dayOfWeek, false);
    EXPECT_TRUE(result.error().isUserError());
    EXPECT_EQ(result.error().message(), error);
  };

  EXPECT_NO_THROW(daysSinceEpochReturnError(
      292278995, 1, 1, 1, "Date out of range: 292278995-1-1-1"));
  EXPECT_NO_THROW(daysSinceEpochReturnError(
      2024, 0, 1, 1, "Date out of range: 2024-0-1-1"));
  EXPECT_NO_THROW(daysSinceEpochReturnError(
      2024, 13, 1, 1, "Date out of range: 2024-13-1-1"));
  EXPECT_NO_THROW(daysSinceEpochReturnError(
      2024, 1, 6, 1, "Date out of range: 2024-1-6-1"));
  EXPECT_NO_THROW(daysSinceEpochReturnError(
      2024, 2, 1, 1, "Date out of range: 2024-2-1-1"));
  EXPECT_NO_THROW(daysSinceEpochReturnError(
      2024, 2, 5, 5, "Date out of range: 2024-2-5-5"));

  auto daysSinceEpochReturnValues =
      [](int32_t year, int32_t month, int32_t weekOfMonth, int32_t dayOfWeek) {
        auto result = util::daysSinceEpochFromWeekOfMonthDate(
            year, month, weekOfMonth, dayOfWeek, false);
        EXPECT_TRUE(!result.hasError());
        return result.value();
      };

  EXPECT_EQ(-724, daysSinceEpochReturnValues(1968, 1, 2, 1));
  EXPECT_EQ(4, daysSinceEpochReturnValues(1970, 1, 2, 1));
  EXPECT_EQ(396, daysSinceEpochReturnValues(1971, 2, 1, 1));
  EXPECT_EQ(19905, daysSinceEpochReturnValues(2024, 7, 1, 1));
}

TEST(DateTimeUtilTest, fromDateString) {
  for (ParseMode mode : {ParseMode::kPrestoCast, ParseMode::kSparkCast}) {
    EXPECT_EQ(0, parseDate("1970-01-01", mode));
    EXPECT_EQ(3789742, parseDate("12345-12-18", mode));

    EXPECT_EQ(1, parseDate("1970-1-2", mode));
    EXPECT_EQ(1, parseDate("1970-01-2", mode));
    EXPECT_EQ(1, parseDate("1970-1-02", mode));

    EXPECT_EQ(1, parseDate("+1970-01-02", mode));

    EXPECT_EQ(0, parseDate(" 1970-01-01", mode));
    EXPECT_EQ(0, parseDate("1970-01-01 ", mode));
    EXPECT_EQ(0, parseDate(" 1970-01-01 ", mode));
  }

  EXPECT_EQ(-719893, parseDate("-1-1-1", ParseMode::kPrestoCast));

  EXPECT_EQ(3789391, parseDate("12345", ParseMode::kSparkCast));
  EXPECT_EQ(16436, parseDate("2015", ParseMode::kSparkCast));
  EXPECT_EQ(16495, parseDate("2015-03", ParseMode::kSparkCast));
  EXPECT_EQ(16512, parseDate("2015-03-18T", ParseMode::kSparkCast));
  EXPECT_EQ(16512, parseDate("2015-03-18T123123", ParseMode::kSparkCast));
  EXPECT_EQ(16512, parseDate("2015-03-18 123142", ParseMode::kSparkCast));
  EXPECT_EQ(16512, parseDate("2015-03-18 (BC)", ParseMode::kSparkCast));
}

TEST(DateTimeUtilTest, fromDateStringInvalid) {
  auto testCastFromDateStringInvalid = [&](const StringView& str,
                                           ParseMode mode) {
    if (mode == ParseMode::kPrestoCast) {
      VELOX_ASSERT_THROW(
          parseDate(str, mode),
          fmt::format(
              "Unable to parse date value: \"{}\". "
              "Valid date string pattern is (YYYY-MM-DD), "
              "and can be prefixed with [+-]",
              std::string(str.data(), str.size())));
    } else if (mode == ParseMode::kSparkCast) {
      VELOX_ASSERT_THROW(
          parseDate(str, mode),
          fmt::format(
              "Unable to parse date value: \"{}\". "
              "Valid date string patterns include "
              "([y]y*, [y]y*-[m]m*, [y]y*-[m]m*-[d]d*, "
              "[y]y*-[m]m*-[d]d* *, [y]y*-[m]m*-[d]d*T*), "
              "and any pattern prefixed with [+-]",
              std::string(str.data(), str.size())));
    } else if (mode == ParseMode::kIso8601) {
      VELOX_ASSERT_THROW(
          parseDate(str, mode),
          fmt::format(
              "Unable to parse date value: \"{}\". "
              "Valid date string patterns include "
              "([y]y*, [y]y*-[m]m*, [y]y*-[m]m*-[d]d*, "
              "[y]y*-[m]m*-[d]d* *), "
              "and any pattern prefixed with [+-]",
              std::string(str.data(), str.size())));
    }
  };

  for (ParseMode mode : {ParseMode::kPrestoCast, ParseMode::kSparkCast}) {
    testCastFromDateStringInvalid("2012-Oct-23", mode);
    testCastFromDateStringInvalid("2012-Oct-23", mode);
    testCastFromDateStringInvalid("2015-03-18X", mode);
    testCastFromDateStringInvalid("2015/03/18", mode);
    testCastFromDateStringInvalid("2015.03.18", mode);
    testCastFromDateStringInvalid("20150318", mode);
    testCastFromDateStringInvalid("2015-031-8", mode);
  }

  testCastFromDateStringInvalid("-1-1-1", ParseMode::kSparkCast);
  testCastFromDateStringInvalid("-111-1-1", ParseMode::kSparkCast);

  testCastFromDateStringInvalid("12345", ParseMode::kStrict);
  testCastFromDateStringInvalid("2015", ParseMode::kStrict);
  testCastFromDateStringInvalid("2015-03", ParseMode::kStrict);
  testCastFromDateStringInvalid("2015-03-18 123412", ParseMode::kStrict);
  testCastFromDateStringInvalid("2015-03-18T", ParseMode::kStrict);
  testCastFromDateStringInvalid("2015-03-18T123412", ParseMode::kStrict);
  testCastFromDateStringInvalid("2015-03-18 (BC)", ParseMode::kStrict);
  testCastFromDateStringInvalid("1970-01-01 ", ParseMode::kStrict);
  testCastFromDateStringInvalid(" 1970-01-01 ", ParseMode::kStrict);

  testCastFromDateStringInvalid("1970-01-01T01:00:47", ParseMode::kIso8601);
  testCastFromDateStringInvalid("1970-01-01T01:00:47.000", ParseMode::kIso8601);
}

// bash command to verify:
// $ date -d "2000-01-01 12:21:56Z" +%s
// ('Z' at the end means UTC).
TEST(DateTimeUtilTest, fromTimestampString) {
  EXPECT_EQ(Timestamp(0, 0), parseTimestamp("1970-01-01"));
  EXPECT_EQ(Timestamp(946684800, 0), parseTimestamp("2000-01-01"));

  EXPECT_EQ(Timestamp(0, 0), parseTimestamp("1970-01-01 00:00"));
  EXPECT_EQ(Timestamp(0, 0), parseTimestamp("1970-01-01 00:00:00"));
  EXPECT_EQ(Timestamp(0, 0), parseTimestamp("1970-01-01 00:00:00    "));

  EXPECT_EQ(Timestamp(946729316, 0), parseTimestamp("2000-01-01 12:21:56"));
  EXPECT_EQ(
      Timestamp(946729316, 0),
      parseTimestamp("2000-01-01T12:21:56", TimestampParseMode::kIso8601));
}

TEST(DateTimeUtilTest, fromTimestampStringInvalid) {
  const std::string_view parserError = "Unable to parse timestamp value: ";
  const std::string_view overflowError = "integer overflow: ";
  const std::string_view timezoneError = "Unknown timezone value: ";

  // Needs at least a date.
  VELOX_ASSERT_THROW(parseTimestamp(""), parserError);
  VELOX_ASSERT_THROW(parseTimestamp("00:00:00"), parserError);

  // Integer overflow during timestamp parsing.
  VELOX_ASSERT_THROW(
      parseTimestamp("2773581570-01-01 00:00:00-asd"), overflowError);
  VELOX_ASSERT_THROW(
      parseTimestamp("-2147483648-01-01 00:00:00-asd"), overflowError);

  // Unexpected timezone definition.
  VELOX_ASSERT_THROW(parseTimestamp("1970-01-01 00:00:00     a"), parserError);
  VELOX_ASSERT_THROW(parseTimestamp("1970-01-01 00:00:00Z"), parserError);
  VELOX_ASSERT_THROW(parseTimestamp("1970-01-01 00:00:00Z"), parserError);
  VELOX_ASSERT_THROW(parseTimestamp("1970-01-01 00:00:00 UTC"), parserError);
  VELOX_ASSERT_THROW(
      parseTimestamp("1970-01-01 00:00:00 America/Los_Angeles"), parserError);

  // Cannot have spaces after T.
  VELOX_ASSERT_THROW(
      parseTimestamp("2000-01-01T 12:21:56", TimestampParseMode::kIso8601),
      parserError);

  // Parse timestamp with (broken) timezones.
  VELOX_ASSERT_THROW(
      parseTimestampWithTimezone("1970-01-01 00:00:00-asd"), timezoneError);
  VELOX_ASSERT_THROW(
      parseTimestampWithTimezone("1970-01-01 00:00:00Z UTC"), parserError);
  VELOX_ASSERT_THROW(
      parseTimestampWithTimezone("1970-01-01 00:00:00+00:00:00"),
      timezoneError);

  // Can't have multiple spaces.
  VELOX_ASSERT_THROW(
      parseTimestampWithTimezone("1970-01-01 00:00:00  UTC"), timezoneError);
}

TEST(DateTimeUtilTest, fromTimestampWithTimezoneString) {
  // -1 means no timezone information.
  auto expected =
      std::make_pair<Timestamp, const tz::TimeZone*>(Timestamp(0, 0), nullptr);
  EXPECT_EQ(parseTimestampWithTimezone("1970-01-01 00:00:00"), expected);

  // Test timezone offsets.
  EXPECT_EQ(
      parseTimestampWithTimezone("1970-01-01 00:00:00 -02:00"),
      std::make_pair(Timestamp(0, 0), tz::locateZone("-02:00")));
  EXPECT_EQ(
      parseTimestampWithTimezone("1970-01-01 00:00:00+13:36"),
      std::make_pair(Timestamp(0, 0), tz::locateZone("+13:36")));
  EXPECT_EQ(
      parseTimestampWithTimezone("1970-01-01 00:00:00 -11"),
      std::make_pair(Timestamp(0, 0), tz::locateZone("-11:00")));
  EXPECT_EQ(
      parseTimestampWithTimezone("1970-01-01 00:00:00 +0000"),
      std::make_pair(Timestamp(0, 0), tz::locateZone("+00:00")));

  EXPECT_EQ(
      parseTimestampWithTimezone("1970-01-01 00:00:00Z"),
      std::make_pair(Timestamp(0, 0), tz::locateZone("UTC")));

  EXPECT_EQ(
      parseTimestampWithTimezone("1970-01-01 00:01:00 UTC"),
      std::make_pair(Timestamp(60, 0), tz::locateZone("UTC")));

  EXPECT_EQ(
      parseTimestampWithTimezone("1970-01-01 00:00:01 America/Los_Angeles"),
      std::make_pair(Timestamp(1, 0), tz::locateZone("America/Los_Angeles")));
}

TEST(DateTimeUtilTest, toGMT) {
  auto* laZone = tz::locateZone("America/Los_Angeles");

  // The GMT time when LA gets to "1970-01-01 00:00:00" (8h ahead).
  auto ts = parseTimestamp("1970-01-01 00:00:00");
  ts.toGMT(*laZone);
  EXPECT_EQ(ts, parseTimestamp("1970-01-01 08:00:00"));

  // Set on a random date/time and try some variations.
  ts = parseTimestamp("2020-04-23 04:23:37.926");

  // To LA:
  auto tsCopy = ts;
  tsCopy.toGMT(*laZone);
  EXPECT_EQ(tsCopy, parseTimestamp("2020-04-23 11:23:37.926"));

  // To Sao Paulo:
  tsCopy = ts;
  tsCopy.toGMT(*tz::locateZone("America/Sao_Paulo"));
  EXPECT_EQ(tsCopy, parseTimestamp("2020-04-23 07:23:37.926"));

  // Moscow:
  tsCopy = ts;
  tsCopy.toGMT(*tz::locateZone("Europe/Moscow"));
  EXPECT_EQ(tsCopy, parseTimestamp("2020-04-23 01:23:37.926"));

  // Probe LA's daylight savings boundary (starts at 2021-13-14 02:00am).
  // Before it starts, 8h offset:
  ts = parseTimestamp("2021-03-14 00:00:00");
  ts.toGMT(*laZone);
  EXPECT_EQ(ts, parseTimestamp("2021-03-14 08:00:00"));

  // After it starts, 7h offset:
  ts = parseTimestamp("2021-03-14 08:00:00");
  ts.toGMT(*laZone);
  EXPECT_EQ(ts, parseTimestamp("2021-03-14 15:00:00"));

  // Ambiguous time 2019-11-03 01:00:00.
  // It could be 2019-11-03 01:00:00 PDT == 2019-11-03 08:00:00 UTC
  // or 2019-11-03 01:00:00 PST == 2019-11-03 09:00:00 UTC.
  ts = parseTimestamp("2019-11-03 01:00:00");
  ts.toGMT(*laZone);
  EXPECT_EQ(ts, parseTimestamp("2019-11-03 08:00:00"));

  // Nonexistent time 2019-03-10 02:00:00.
  // It is in a gap between 2019-03-10 02:00:00 PST and 2019-03-10 03:00:00 PDT
  // which are both equivalent to 2019-03-10 10:00:00 UTC.
  ts = parseTimestamp("2019-03-10 02:00:00");
  EXPECT_THROW(ts.toGMT(*laZone), VeloxUserError);
}

TEST(DateTimeUtilTest, toTimezone) {
  auto* laZone = tz::locateZone("America/Los_Angeles");

  // The LA time when GMT gets to "1970-01-01 00:00:00" (8h behind).
  auto ts = parseTimestamp("1970-01-01 00:00:00");
  ts.toTimezone(*laZone);
  EXPECT_EQ(ts, parseTimestamp("1969-12-31 16:00:00"));

  // Set on a random date/time and try some variations.
  ts = parseTimestamp("2020-04-23 04:23:37");

  // To LA:
  auto tsCopy = ts;
  tsCopy.toTimezone(*laZone);
  EXPECT_EQ(tsCopy, parseTimestamp("2020-04-22 21:23:37"));

  // To Sao Paulo:
  tsCopy = ts;
  tsCopy.toTimezone(*tz::locateZone("America/Sao_Paulo"));
  EXPECT_EQ(tsCopy, parseTimestamp("2020-04-23 01:23:37"));

  // Moscow:
  tsCopy = ts;
  tsCopy.toTimezone(*tz::locateZone("Europe/Moscow"));
  EXPECT_EQ(tsCopy, parseTimestamp("2020-04-23 07:23:37"));

  // Probe LA's daylight savings boundary (starts at 2021-13-14 02:00am).
  // Before it starts, 8h offset:
  ts = parseTimestamp("2021-03-14 00:00:00");
  ts.toTimezone(*laZone);
  EXPECT_EQ(ts, parseTimestamp("2021-03-13 16:00:00"));

  // After it starts, 7h offset:
  ts = parseTimestamp("2021-03-15 00:00:00");
  ts.toTimezone(*laZone);
  EXPECT_EQ(ts, parseTimestamp("2021-03-14 17:00:00"));
}

TEST(DateTimeUtilTest, toGMTFromID) {
  // The GMT time when LA gets to "1970-01-01 00:00:00" (8h ahead).
  auto ts = parseTimestamp("1970-01-01 00:00:00");
  ts.toGMT(*tz::locateZone("America/Los_Angeles"));
  EXPECT_EQ(ts, parseTimestamp("1970-01-01 08:00:00"));

  // Set on a random date/time and try some variations.
  ts = parseTimestamp("2020-04-23 04:23:37");

  // To LA:
  auto tsCopy = ts;
  tsCopy.toGMT(*tz::locateZone("America/Los_Angeles"));
  EXPECT_EQ(tsCopy, parseTimestamp("2020-04-23 11:23:37"));

  // To Sao Paulo:
  tsCopy = ts;
  tsCopy.toGMT(*tz::locateZone("America/Sao_Paulo"));
  EXPECT_EQ(tsCopy, parseTimestamp("2020-04-23 07:23:37"));

  // Moscow:
  tsCopy = ts;
  tsCopy.toGMT(*tz::locateZone("Europe/Moscow"));
  EXPECT_EQ(tsCopy, parseTimestamp("2020-04-23 01:23:37"));

  // Numerical time zones: +HH:MM and -HH:MM
  tsCopy = ts;
  tsCopy.toGMT(*tz::locateZone("+14:00"));
  EXPECT_EQ(tsCopy, parseTimestamp("2020-04-22 14:23:37"));

  tsCopy = ts;
  tsCopy.toGMT(*tz::locateZone("-14:00"));
  EXPECT_EQ(tsCopy, parseTimestamp("2020-04-23 18:23:37"));

  tsCopy = ts;
  tsCopy.toGMT(*tz::locateZone("+00:00"));
  EXPECT_EQ(tsCopy, parseTimestamp("2020-04-23 04:23:37"));

  tsCopy = ts;
  tsCopy.toGMT(*tz::locateZone("-00:01"));
  EXPECT_EQ(tsCopy, parseTimestamp("2020-04-23 04:24:37"));

  tsCopy = ts;
  tsCopy.toGMT(*tz::locateZone("+00:01"));
  EXPECT_EQ(tsCopy, parseTimestamp("2020-04-23 04:22:37"));

  // Probe LA's daylight savings boundary (starts at 2021-13-14 02:00am).
  // Before it starts, 8h offset:
  ts = parseTimestamp("2021-03-14 00:00:00");
  ts.toGMT(*tz::locateZone("America/Los_Angeles"));
  EXPECT_EQ(ts, parseTimestamp("2021-03-14 08:00:00"));

  // After it starts, 7h offset:
  ts = parseTimestamp("2021-03-15 00:00:00");
  ts.toGMT(*tz::locateZone("America/Los_Angeles"));
  EXPECT_EQ(ts, parseTimestamp("2021-03-15 07:00:00"));
}

TEST(DateTimeUtilTest, toTimezoneFromID) {
  // The LA time when GMT gets to "1970-01-01 00:00:00" (8h behind).
  auto ts = parseTimestamp("1970-01-01 00:00:00");
  ts.toTimezone(*tz::locateZone("America/Los_Angeles"));
  EXPECT_EQ(ts, parseTimestamp("1969-12-31 16:00:00"));

  // Set on a random date/time and try some variations.
  ts = parseTimestamp("2020-04-23 04:23:37");

  // To LA:
  auto tsCopy = ts;
  tsCopy.toTimezone(*tz::locateZone("America/Los_Angeles"));
  EXPECT_EQ(tsCopy, parseTimestamp("2020-04-22 21:23:37"));

  // To Sao Paulo:
  tsCopy = ts;
  tsCopy.toTimezone(*tz::locateZone("America/Sao_Paulo"));
  EXPECT_EQ(tsCopy, parseTimestamp("2020-04-23 01:23:37"));

  // Moscow:
  tsCopy = ts;
  tsCopy.toTimezone(*tz::locateZone("Europe/Moscow"));
  EXPECT_EQ(tsCopy, parseTimestamp("2020-04-23 07:23:37"));

  // Numerical time zones: +HH:MM and -HH:MM
  tsCopy = ts;
  tsCopy.toTimezone(*tz::locateZone("+14:00"));
  EXPECT_EQ(tsCopy, parseTimestamp("2020-04-23 18:23:37"));

  tsCopy = ts;
  tsCopy.toTimezone(*tz::locateZone("-14:00"));
  EXPECT_EQ(tsCopy, parseTimestamp("2020-04-22 14:23:37"));

  tsCopy = ts;
  tsCopy.toTimezone(*tz::locateZone("+00:00"));
  EXPECT_EQ(tsCopy, parseTimestamp("2020-04-23 04:23:37"));

  tsCopy = ts;
  tsCopy.toTimezone(*tz::locateZone("-00:01"));
  EXPECT_EQ(tsCopy, parseTimestamp("2020-04-23 04:22:37"));

  tsCopy = ts;
  tsCopy.toTimezone(*tz::locateZone("+00:01"));
  EXPECT_EQ(tsCopy, parseTimestamp("2020-04-23 04:24:37"));

  // Probe LA's daylight savings boundary (starts at 2021-13-14 02:00am).
  // Before it starts, 8h offset:
  ts = parseTimestamp("2021-03-14 00:00:00");
  ts.toTimezone(*tz::locateZone("America/Los_Angeles"));
  EXPECT_EQ(ts, parseTimestamp("2021-03-13 16:00:00"));

  // After it starts, 7h offset:
  ts = parseTimestamp("2021-03-15 00:00:00");
  ts.toTimezone(*tz::locateZone("America/Los_Angeles"));
  EXPECT_EQ(ts, parseTimestamp("2021-03-14 17:00:00"));
}

} // namespace
} // namespace facebook::velox::util
