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

#include "velox/functions/prestosql/tests/CastBaseTest.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox {
namespace {

class TimestampWithTimeZoneCastTest : public functions::test::CastBaseTest {
 public:
  VectorPtr makeTimestampWithTimeZoneVector(
      vector_size_t size,
      const std::function<int64_t(int32_t row)>& timestampAt,
      const std::function<int16_t(int32_t row)>& timezoneAt) {
    return makeFlatVector<int64_t>(
        size,
        [&](int32_t index) {
          return pack(timestampAt(index), timezoneAt(index));
        },
        nullptr,
        TIMESTAMP_WITH_TIME_ZONE());
  }

 protected:
  void setQueryTimeZone(const std::string& timeZone) {
    queryCtx_->testingOverrideConfigUnsafe({
        {core::QueryConfig::kSessionTimezone, timeZone},
        {core::QueryConfig::kAdjustTimestampToTimezone, "true"},
    });
  }

  void disableAdjustTimestampToTimezone() {
    queryCtx_->testingOverrideConfigUnsafe({
        {core::QueryConfig::kAdjustTimestampToTimezone, "false"},
    });
  }
};

TEST_F(TimestampWithTimeZoneCastTest, fromTimestamp) {
  const auto tsVector = makeNullableFlatVector<Timestamp>(
      {Timestamp(1996, 0), std::nullopt, Timestamp(19920, 0)});
  auto timestamps =
      std::vector<int64_t>{1996 * kMillisInSecond, 0, 19920 * kMillisInSecond};
  auto timezones = std::vector<TimeZoneKey>{0, 0, 0};
  const auto expected = makeTimestampWithTimeZoneVector(
      timestamps.size(),
      [&](int32_t index) { return timestamps[index]; },
      [&](int32_t index) { return timezones[index]; });
  expected->setNull(1, true);

  testCast(tsVector, expected);
}

TEST_F(TimestampWithTimeZoneCastTest, fromVarchar) {
  const auto stringVector = makeNullableFlatVector<StringView>(
      {std::nullopt,
       "2012-10-31 01:00:47 America/Denver",
       "2012-10-31 01:00:47 -06:00",
       "1994-05-06 15:49 Europe/Vienna",
       "1979-02-24 08:33:31 Pacific/Chatham",
       "1979-02-24 08:33:31 +13:45"});

  // Varchar representations above hold local time (relative to specified time
  // zone). For instance, the first string represents a wall-clock displaying
  // '2012-10-31 01:00:47' in Denver. Below, we use UTC representations of the
  // above local wall-clocks, to match the UTC timepoints held in the
  // TimestampWithTimezone type.
  auto denverUTC = parseTimestamp("2012-10-31 07:00:47").toMillis();
  auto viennaUTC = parseTimestamp("1994-05-06 13:49:00").toMillis();
  auto chathamUTC = parseTimestamp("1979-02-23 18:48:31").toMillis();

  auto timestamps = std::vector<int64_t>{
      0, denverUTC, denverUTC, viennaUTC, chathamUTC, chathamUTC};

  auto timezones = std::vector<TimeZoneKey>{
      {0,
       (int16_t)tz::getTimeZoneID("America/Denver"),
       (int16_t)tz::getTimeZoneID("-06:00"),
       (int16_t)tz::getTimeZoneID("Europe/Vienna"),
       (int16_t)tz::getTimeZoneID("Pacific/Chatham"),
       (int16_t)tz::getTimeZoneID("+13:45")}};

  const auto expected = makeTimestampWithTimeZoneVector(
      timestamps.size(),
      [&](int32_t index) { return timestamps[index]; },
      [&](int32_t index) { return timezones[index]; });
  expected->setNull(0, true);

  testCast(stringVector, expected);
}

TEST_F(TimestampWithTimeZoneCastTest, toVarchar) {
  // 1970-01-01 06:11:37.123 UTC in 4 different time zones.
  const int64_t utcMillis =
      6 * kMillisInHour + 11 * kMillisInMinute + 37 * kMillisInSecond + 123;
  auto input = makeFlatVector<int64_t>(
      {
          // -5 hours.
          pack(utcMillis, tz::getTimeZoneID("America/New_York")),
          // -8 hours.
          pack(utcMillis, tz::getTimeZoneID("America/Los_Angeles")),
          // +8 hours.
          pack(utcMillis, tz::getTimeZoneID("Asia/Shanghai")),
          // +5:30 hours.
          pack(utcMillis, tz::getTimeZoneID("Asia/Calcutta")),
      },
      TIMESTAMP_WITH_TIME_ZONE());

  auto expected = makeFlatVector<std::string>({
      "1970-01-01 01:11:37.123 America/New_York",
      "1969-12-31 22:11:37.123 America/Los_Angeles",
      "1970-01-01 14:11:37.123 Asia/Shanghai",
      "1970-01-01 11:41:37.123 Asia/Calcutta",
  });

  auto result = evaluate("cast(c0 as varchar)", makeRowVector({input}));
  test::assertEqualVectors(expected, result);
}

TEST_F(TimestampWithTimeZoneCastTest, fromVarcharWithoutTimezone) {
  setQueryTimeZone("America/Denver");

  const auto stringVector =
      makeNullableFlatVector<StringView>({"2012-10-31 01:00:47"});
  auto denverUTC = parseTimestamp("2012-10-31 07:00:47").toMillis();

  auto timestamps = std::vector<int64_t>{denverUTC};

  auto timezones =
      std::vector<TimeZoneKey>{(int16_t)tz::getTimeZoneID("America/Denver")};

  const auto expected = makeTimestampWithTimeZoneVector(
      timestamps.size(),
      [&](int32_t index) { return timestamps[index]; },
      [&](int32_t index) { return timezones[index]; });

  testCast(stringVector, expected);
}

TEST_F(TimestampWithTimeZoneCastTest, fromVarcharInvalidInput) {
  const auto invalidStringVector1 = makeNullableFlatVector<StringView>(
      {"2012-10-31 01:00:47fooAmerica/Los_Angeles"});

  const auto invalidStringVector2 = makeNullableFlatVector<StringView>(
      {"2012-10-31 01:00:47 America/California"});

  const auto invalidStringVector3 = makeNullableFlatVector<StringView>(
      {"2012-10-31foo01:00:47 America/Los_Angeles"});

  const auto invalidStringVector4 = makeNullableFlatVector<StringView>(
      {"2012-10-31 35:00:47 America/Los_Angeles"});

  auto millis = parseTimestamp("2012-10-31 07:00:47").toMillis();
  auto timestamps = std::vector<int64_t>{millis};

  auto timezones =
      std::vector<TimeZoneKey>{(int16_t)tz::getTimeZoneID("America/Denver")};

  const auto expected = makeTimestampWithTimeZoneVector(
      timestamps.size(),
      [&](int32_t index) { return timestamps[index]; },
      [&](int32_t index) { return timezones[index]; });

  VELOX_ASSERT_THROW(
      testCast(invalidStringVector1, expected),
      "Unknown timezone value: \"fooAmerica/Los_Angeles\"");
  VELOX_ASSERT_THROW(
      testCast(invalidStringVector2, expected),
      "Unknown timezone value: \"America/California\"");
  VELOX_ASSERT_THROW(
      testCast(invalidStringVector3, expected),
      "Unable to parse timestamp value: \"2012-10-31foo01:00:47 America/Los_Angeles\"");
  VELOX_ASSERT_THROW(
      testCast(invalidStringVector4, expected),
      "Unable to parse timestamp value: \"2012-10-31 35:00:47 America/Los_Angeles\"");
}

TEST_F(TimestampWithTimeZoneCastTest, toTimestamp) {
  auto timestamps =
      std::vector<int64_t>{1996 * kMillisInSecond, 0, 19920 * kMillisInSecond};
  auto timezones = std::vector<TimeZoneKey>{0, 0, 1825 /*America/Los_Angeles*/};

  const auto tsWithTZVector = makeTimestampWithTimeZoneVector(
      timestamps.size(),
      [&](int32_t index) { return timestamps[index]; },
      [&](int32_t index) { return timezones[index]; });
  tsWithTZVector->setNull(1, true);

  for (const char* timezone : {"America/Los_Angeles", "America/New_York"}) {
    setQueryTimeZone(timezone);
    auto expected = makeNullableFlatVector<Timestamp>(
        {Timestamp(1996, 0), std::nullopt, Timestamp(19920, 0)});
    testCast(tsWithTZVector, expected);

    // Cast('1969-12-31 16:00:00 -08:00' as timestamp).
    auto result = evaluateOnce<Timestamp>(
        "cast(from_unixtime(c0, 'America/Los_Angeles') as timestamp)",
        makeRowVector({makeFlatVector<double>(std::vector<double>(1, 0))}));
    // 1970-01-01 00:00:00.
    EXPECT_EQ(Timestamp(0, 0), result.value());
  }

  disableAdjustTimestampToTimezone();
  auto expected = makeNullableFlatVector<Timestamp>(
      {Timestamp(1996, 0), std::nullopt, Timestamp(19920 - 8 * 3600, 0)});
  testCast(tsWithTZVector, expected);

  // Cast('1969-12-31 16:00:00 -08:00' as timestamp).
  auto result = evaluateOnce<Timestamp>(
      "cast(from_unixtime(c0, 'America/Los_Angeles') as timestamp)",
      makeRowVector({makeFlatVector<double>(std::vector<double>(1, 0))}));
  // 1969-12-31 16:00:00.
  EXPECT_EQ(Timestamp(-28800, 0), result.value());
}

TEST_F(TimestampWithTimeZoneCastTest, toDate) {
  auto input = makeFlatVector<int64_t>(
      {
          // 6AM UTC is 1AM EST (same day), 10PM PST (previous day), 2PM CST
          // (same day).
          pack(6 * kMillisInHour, tz::getTimeZoneID("America/New_York")),
          pack(6 * kMillisInHour, tz::getTimeZoneID("America/Los_Angeles")),
          pack(6 * kMillisInHour, tz::getTimeZoneID("Asia/Shanghai")),
          // 6PM UTC is 1PM EST (same day), 10AM PST (same day), 2AM CST (next
          // day).
          pack(18 * kMillisInHour, tz::getTimeZoneID("America/New_York")),
          pack(18 * kMillisInHour, tz::getTimeZoneID("America/Los_Angeles")),
          pack(18 * kMillisInHour, tz::getTimeZoneID("Asia/Shanghai")),
      },
      TIMESTAMP_WITH_TIME_ZONE());
  auto expected = makeFlatVector<int32_t>({0, -1, 0, 0, 0, 1}, DATE());

  auto result = evaluate("cast(c0 as date)", makeRowVector({input}));
  test::assertEqualVectors(expected, result);

  // Verify that session time zone doesn't affect the result.

  for (auto tz : {"America/New_York", "America/Los_Angeles", "Asia/Shanghai"}) {
    setQueryTimeZone(tz);
    result = evaluate("cast(c0 as date)", makeRowVector({input}));
    test::assertEqualVectors(expected, result);
  }
}

TEST_F(TimestampWithTimeZoneCastTest, fromDate) {
  auto input = makeFlatVector<int32_t>({-1, 0, 1}, DATE());

  setQueryTimeZone("America/New_York");

  auto tzId = tz::getTimeZoneID("America/New_York");
  auto tzOffset = -5 * kMillisInHour;
  auto expected = makeFlatVector<int64_t>(
      {
          pack(-kMillisInDay - tzOffset, tzId),
          pack(-tzOffset, tzId),
          pack(kMillisInDay - tzOffset, tzId),
      },
      TIMESTAMP_WITH_TIME_ZONE());
  auto result =
      evaluate("cast(c0 as timestamp with time zone)", makeRowVector({input}));
  test::assertEqualVectors(expected, result);

  setQueryTimeZone("Asia/Shanghai");

  tzId = tz::getTimeZoneID("Asia/Shanghai");
  tzOffset = 8 * kMillisInHour;
  expected = makeFlatVector<int64_t>(
      {
          pack(-kMillisInDay - tzOffset, tzId),
          pack(-tzOffset, tzId),
          pack(kMillisInDay - tzOffset, tzId),
      },
      TIMESTAMP_WITH_TIME_ZONE());

  result =
      evaluate("cast(c0 as timestamp with time zone)", makeRowVector({input}));
  test::assertEqualVectors(expected, result);
}

} // namespace
} // namespace facebook::velox
