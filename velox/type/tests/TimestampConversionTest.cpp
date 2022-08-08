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
#include <gtest/gtest.h>
#include "velox/common/base/VeloxException.h"
#include "velox/external/date/tz.h"
#include "velox/type/Timestamp.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::util {
namespace {

TEST(DateTimeUtilTest, fromDate) {
  EXPECT_EQ(0, daysSinceEpochFromDate(1970, 1, 1));
  EXPECT_EQ(1, daysSinceEpochFromDate(1970, 1, 2));
  EXPECT_EQ(365, daysSinceEpochFromDate(1971, 1, 1));
  EXPECT_EQ(730, daysSinceEpochFromDate(1972, 1, 1)); // leap year.
  EXPECT_EQ(1096, daysSinceEpochFromDate(1973, 1, 1));

  EXPECT_EQ(10957, daysSinceEpochFromDate(2000, 1, 1));
  EXPECT_EQ(18474, daysSinceEpochFromDate(2020, 7, 31));

  // Before unix epoch.
  EXPECT_EQ(-1, daysSinceEpochFromDate(1969, 12, 31));
  EXPECT_EQ(-365, daysSinceEpochFromDate(1969, 1, 1));
  EXPECT_EQ(-731, daysSinceEpochFromDate(1968, 1, 1)); // leap year.
  EXPECT_EQ(-719528, daysSinceEpochFromDate(0, 1, 1));

  // Negative year - BC.
  EXPECT_EQ(-719529, daysSinceEpochFromDate(-1, 12, 31));
  EXPECT_EQ(-719893, daysSinceEpochFromDate(-1, 1, 1));
}

TEST(DateTimeUtilTest, fromDateInvalid) {
  EXPECT_THROW(daysSinceEpochFromDate(1970, 1, -1), VeloxUserError);
  EXPECT_THROW(daysSinceEpochFromDate(1970, -1, 1), VeloxUserError);
  EXPECT_THROW(daysSinceEpochFromDate(1970, 0, 1), VeloxUserError);
  EXPECT_THROW(daysSinceEpochFromDate(1970, 13, 1), VeloxUserError);
  EXPECT_THROW(daysSinceEpochFromDate(1970, 1, 32), VeloxUserError);
  EXPECT_THROW(
      daysSinceEpochFromDate(1970, 2, 29), VeloxUserError); // non-leap.
  EXPECT_THROW(daysSinceEpochFromDate(1970, 6, 31), VeloxUserError);
}

TEST(DateTimeUtilTest, fromDateString) {
  EXPECT_EQ(10957, fromDateString("2000-01-01"));
  EXPECT_EQ(0, fromDateString("1970-01-01"));
  EXPECT_EQ(1, fromDateString("1970-01-02"));

  // Single character
  EXPECT_EQ(1, fromDateString("1970-1-2"));

  // Old and negative years.
  EXPECT_EQ(-719528, fromDateString("0-1-1"));
  EXPECT_EQ(-719162, fromDateString("1-1-1"));
  EXPECT_EQ(-719893, fromDateString("-1-1-1"));
  EXPECT_EQ(-720258, fromDateString("-2-1-1"));

  // 1BC is equal 0-1-1.
  EXPECT_EQ(-719528, fromDateString("1-1-1 (BC)"));
  EXPECT_EQ(-719893, fromDateString("2-1-1 (BC)"));

  // Leading zeros and spaces.
  EXPECT_EQ(-719162, fromDateString("00001-1-1"));
  EXPECT_EQ(-719162, fromDateString(" 1-1-1"));
  EXPECT_EQ(-719162, fromDateString("     1-1-1"));
  EXPECT_EQ(-719162, fromDateString("\t1-1-1"));
  EXPECT_EQ(-719162, fromDateString("  \t    \n 00001-1-1  \n"));

  // Different separators.
  EXPECT_EQ(-719162, fromDateString("1/1/1"));
  EXPECT_EQ(-719162, fromDateString("1 1 1"));
  EXPECT_EQ(-719162, fromDateString("1\\1\\1"));

  // Other string types.
  EXPECT_EQ(0, fromDateString(StringView("1970-01-01")));
}

TEST(DateTimeUtilTest, fromDateStrInvalid) {
  EXPECT_THROW(fromDateString(""), VeloxUserError);
  EXPECT_THROW(fromDateString("     "), VeloxUserError);
  EXPECT_THROW(fromDateString("2000"), VeloxUserError);

  // Different separators.
  EXPECT_THROW(fromDateString("2000/01-01"), VeloxUserError);
  EXPECT_THROW(fromDateString("2000 01-01"), VeloxUserError);

  // Trailing characters.
  EXPECT_THROW(fromDateString("2000-01-01   asdf"), VeloxUserError);
  EXPECT_THROW(fromDateString("2000-01-01 0"), VeloxUserError);

  // Too large of a year.
  EXPECT_THROW(fromDateString("1000000"), VeloxUserError);
  EXPECT_THROW(fromDateString("-1000000"), VeloxUserError);
}

TEST(DateTimeUtilTest, fromTimeString) {
  EXPECT_EQ(0, fromTimeString("00:00:00"));
  EXPECT_EQ(0, fromTimeString("00:00:00.00"));
  EXPECT_EQ(1, fromTimeString("00:00:00.000001"));
  EXPECT_EQ(10, fromTimeString("00:00:00.00001"));
  EXPECT_EQ(100, fromTimeString("00:00:00.0001"));
  EXPECT_EQ(1000, fromTimeString("00:00:00.001"));
  EXPECT_EQ(10000, fromTimeString("00:00:00.01"));
  EXPECT_EQ(100000, fromTimeString("00:00:00.1"));
  EXPECT_EQ(1'000'000, fromTimeString("00:00:01"));
  EXPECT_EQ(60'000'000, fromTimeString("00:01:00"));
  EXPECT_EQ(3'600'000'000, fromTimeString("01:00:00"));

  // 1 day minus 1 second.
  EXPECT_EQ(86'399'000'000, fromTimeString("23:59:59"));

  // Single digit.
  EXPECT_EQ(0, fromTimeString("0:0:0.0"));
  EXPECT_EQ(3'661'000'000, fromTimeString("1:1:1"));

  // Leading and trailing spaces.
  EXPECT_EQ(0, fromTimeString("   \t \n 00:00:00.00  \t"));
}

TEST(DateTimeUtilTest, fromTimeStrInvalid) {
  EXPECT_THROW(fromTimeString(""), VeloxUserError);
  EXPECT_THROW(fromTimeString("00"), VeloxUserError);
  EXPECT_THROW(fromTimeString("00:00"), VeloxUserError);

  // Invalid hour, minutes and seconds.
  EXPECT_THROW(fromTimeString("24:00:00"), VeloxUserError);
  EXPECT_THROW(fromTimeString("00:61:00"), VeloxUserError);
  EXPECT_THROW(fromTimeString("00:00:61"), VeloxUserError);

  // Trailing characters.
  EXPECT_THROW(fromTimeString("00:00:00   12"), VeloxUserError);
}

// bash command to verify:
// $ date -d "2000-01-01 12:21:56Z" +%s
// ('Z' at the end means UTC).
TEST(DateTimeUtilTest, fromTimestampString) {
  EXPECT_EQ(Timestamp(0, 0), fromTimestampString("1970-01-01"));
  EXPECT_EQ(Timestamp(946684800, 0), fromTimestampString("2000-01-01"));

  EXPECT_EQ(Timestamp(0, 0), fromTimestampString("1970-01-01 00:00:00"));
  EXPECT_EQ(
      Timestamp(946729316, 0), fromTimestampString("2000-01-01 12:21:56"));
  EXPECT_EQ(
      Timestamp(946729316, 0), fromTimestampString("2000-01-01T12:21:56"));
  EXPECT_EQ(
      Timestamp(946729316, 0), fromTimestampString("2000-01-01T 12:21:56"));

  // Test UTC offsets.
  EXPECT_EQ(
      Timestamp(7200, 0), fromTimestampString("1970-01-01 00:00:00-02:00"));
  EXPECT_EQ(
      Timestamp(946697400, 0),
      fromTimestampString("2000-01-01 00:00:00Z-03:30"));
  EXPECT_EQ(
      Timestamp(1587583417, 0),
      fromTimestampString("2020-04-23 04:23:37+09:00"));
}

TEST(DateTimeUtilTest, fromTimestampStrInvalid) {
  // Needs at least a date.
  EXPECT_THROW(fromTimestampString(""), VeloxUserError);
  EXPECT_THROW(fromTimestampString("00:00:00"), VeloxUserError);

  // Broken UTC offsets.
  EXPECT_THROW(fromTimestampString("1970-01-01 00:00:00-asd"), VeloxUserError);
  EXPECT_THROW(
      fromTimestampString("1970-01-01 00:00:00+00:00:00"), VeloxUserError);
}

TEST(DateTimeUtilTest, toGMT) {
  auto* laZone = date::locate_zone("America/Los_Angeles");

  // The GMT time when LA gets to "1970-01-01 00:00:00" (8h ahead).
  auto ts = fromTimestampString("1970-01-01 00:00:00");
  ts.toGMT(*laZone);
  EXPECT_EQ(ts, fromTimestampString("1970-01-01 08:00:00"));

  // Set on a random date/time and try some variations.
  ts = fromTimestampString("2020-04-23 04:23:37");

  // To LA:
  auto tsCopy = ts;
  tsCopy.toGMT(*laZone);
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 11:23:37"));

  // To Sao Paulo:
  tsCopy = ts;
  tsCopy.toGMT(*date::locate_zone("America/Sao_Paulo"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 07:23:37"));

  // Moscow:
  tsCopy = ts;
  tsCopy.toGMT(*date::locate_zone("Europe/Moscow"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 01:23:37"));

  // Probe LA's daylight savings boundary (starts at 2021-13-14 02:00am).
  // Before it starts, 8h offset:
  ts = fromTimestampString("2021-03-14 00:00:00");
  ts.toGMT(*laZone);
  EXPECT_EQ(ts, fromTimestampString("2021-03-14 08:00:00"));

  // After it starts, 7h offset:
  ts = fromTimestampString("2021-03-14 08:00:00");
  ts.toGMT(*laZone);
  EXPECT_EQ(ts, fromTimestampString("2021-03-14 15:00:00"));
}

TEST(DateTimeUtilTest, toTimezone) {
  auto* laZone = date::locate_zone("America/Los_Angeles");

  // The LA time when GMT gets to "1970-01-01 00:00:00" (8h behind).
  auto ts = fromTimestampString("1970-01-01 00:00:00");
  ts.toTimezone(*laZone);
  EXPECT_EQ(ts, fromTimestampString("1969-12-31 16:00:00"));

  // Set on a random date/time and try some variations.
  ts = fromTimestampString("2020-04-23 04:23:37");

  // To LA:
  auto tsCopy = ts;
  tsCopy.toTimezone(*laZone);
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-22 21:23:37"));

  // To Sao Paulo:
  tsCopy = ts;
  tsCopy.toTimezone(*date::locate_zone("America/Sao_Paulo"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 01:23:37"));

  // Moscow:
  tsCopy = ts;
  tsCopy.toTimezone(*date::locate_zone("Europe/Moscow"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 07:23:37"));

  // Probe LA's daylight savings boundary (starts at 2021-13-14 02:00am).
  // Before it starts, 8h offset:
  ts = fromTimestampString("2021-03-14 00:00:00");
  ts.toTimezone(*laZone);
  EXPECT_EQ(ts, fromTimestampString("2021-03-13 16:00:00"));

  // After it starts, 7h offset:
  ts = fromTimestampString("2021-03-15 00:00:00");
  ts.toTimezone(*laZone);
  EXPECT_EQ(ts, fromTimestampString("2021-03-14 17:00:00"));
}

TEST(DateTimeUtilTest, toGMTFromID) {
  // The GMT time when LA gets to "1970-01-01 00:00:00" (8h ahead).
  auto ts = fromTimestampString("1970-01-01 00:00:00");
  ts.toGMT(util::getTimeZoneID("America/Los_Angeles"));
  EXPECT_EQ(ts, fromTimestampString("1970-01-01 08:00:00"));

  // Set on a random date/time and try some variations.
  ts = fromTimestampString("2020-04-23 04:23:37");

  // To LA:
  auto tsCopy = ts;
  tsCopy.toGMT(util::getTimeZoneID("America/Los_Angeles"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 11:23:37"));

  // To Sao Paulo:
  tsCopy = ts;
  tsCopy.toGMT(util::getTimeZoneID("America/Sao_Paulo"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 07:23:37"));

  // Moscow:
  tsCopy = ts;
  tsCopy.toGMT(util::getTimeZoneID("Europe/Moscow"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 01:23:37"));

  // Numerical time zones: +HH:MM and -HH:MM
  tsCopy = ts;
  tsCopy.toGMT(util::getTimeZoneID("+14:00"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-22 14:23:37"));

  tsCopy = ts;
  tsCopy.toGMT(util::getTimeZoneID("-14:00"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 18:23:37"));

  tsCopy = ts;
  tsCopy.toGMT(0); // "+00:00" is not in the time zone id map
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 04:23:37"));

  tsCopy = ts;
  tsCopy.toGMT(util::getTimeZoneID("-00:01"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 04:24:37"));

  tsCopy = ts;
  tsCopy.toGMT(util::getTimeZoneID("+00:01"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 04:22:37"));

  // Probe LA's daylight savings boundary (starts at 2021-13-14 02:00am).
  // Before it starts, 8h offset:
  ts = fromTimestampString("2021-03-14 00:00:00");
  ts.toGMT(util::getTimeZoneID("America/Los_Angeles"));
  EXPECT_EQ(ts, fromTimestampString("2021-03-14 08:00:00"));

  // After it starts, 7h offset:
  ts = fromTimestampString("2021-03-15 00:00:00");
  ts.toGMT(util::getTimeZoneID("America/Los_Angeles"));
  EXPECT_EQ(ts, fromTimestampString("2021-03-15 07:00:00"));
}

TEST(DateTimeUtilTest, toTimezoneFromID) {
  // The LA time when GMT gets to "1970-01-01 00:00:00" (8h behind).
  auto ts = fromTimestampString("1970-01-01 00:00:00");
  ts.toTimezone(util::getTimeZoneID("America/Los_Angeles"));
  EXPECT_EQ(ts, fromTimestampString("1969-12-31 16:00:00"));

  // Set on a random date/time and try some variations.
  ts = fromTimestampString("2020-04-23 04:23:37");

  // To LA:
  auto tsCopy = ts;
  tsCopy.toTimezone(util::getTimeZoneID("America/Los_Angeles"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-22 21:23:37"));

  // To Sao Paulo:
  tsCopy = ts;
  tsCopy.toTimezone(util::getTimeZoneID("America/Sao_Paulo"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 01:23:37"));

  // Moscow:
  tsCopy = ts;
  tsCopy.toTimezone(util::getTimeZoneID("Europe/Moscow"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 07:23:37"));

  // Numerical time zones: +HH:MM and -HH:MM
  tsCopy = ts;
  tsCopy.toTimezone(util::getTimeZoneID("+14:00"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 18:23:37"));

  tsCopy = ts;
  tsCopy.toTimezone(util::getTimeZoneID("-14:00"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-22 14:23:37"));

  tsCopy = ts;
  tsCopy.toTimezone(0); // "+00:00" is not in the time zone id map
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 04:23:37"));

  tsCopy = ts;
  tsCopy.toTimezone(util::getTimeZoneID("-00:01"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 04:22:37"));

  tsCopy = ts;
  tsCopy.toTimezone(util::getTimeZoneID("+00:01"));
  EXPECT_EQ(tsCopy, fromTimestampString("2020-04-23 04:24:37"));

  // Probe LA's daylight savings boundary (starts at 2021-13-14 02:00am).
  // Before it starts, 8h offset:
  ts = fromTimestampString("2021-03-14 00:00:00");
  ts.toTimezone(util::getTimeZoneID("America/Los_Angeles"));
  EXPECT_EQ(ts, fromTimestampString("2021-03-13 16:00:00"));

  // After it starts, 7h offset:
  ts = fromTimestampString("2021-03-15 00:00:00");
  ts.toTimezone(util::getTimeZoneID("America/Los_Angeles"));
  EXPECT_EQ(ts, fromTimestampString("2021-03-14 17:00:00"));
}

} // namespace
} // namespace facebook::velox::util
