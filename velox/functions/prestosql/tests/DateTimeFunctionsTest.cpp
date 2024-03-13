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

#include <optional>
#include <string>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/external/date/tz.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/type/tz/TimeZoneMap.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

class DateTimeFunctionsTest : public functions::test::FunctionBaseTest {
 protected:
  std::string daysShort[7] = {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"};

  std::string daysLong[7] = {
      "Monday",
      "Tuesday",
      "Wednesday",
      "Thursday",
      "Friday",
      "Saturday",
      "Sunday"};

  std::string monthsShort[12] = {
      "Jan",
      "Feb",
      "Mar",
      "Apr",
      "May",
      "Jun",
      "Jul",
      "Aug",
      "Sep",
      "Oct",
      "Nov",
      "Dec"};

  std::string monthsLong[12] = {
      "January",
      "February",
      "March",
      "April",
      "May",
      "June",
      "July",
      "August",
      "September",
      "October",
      "November",
      "December"};

  static std::string padNumber(int number) {
    return number < 10 ? "0" + std::to_string(number) : std::to_string(number);
  }

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

 public:
  struct TimestampWithTimezone {
    TimestampWithTimezone(int64_t milliSeconds, int16_t timezoneId)
        : milliSeconds_(milliSeconds), timezoneId_(timezoneId) {}

    int64_t milliSeconds_{0};
    int16_t timezoneId_{0};

    // Provides a nicer printer for gtest.
    friend std::ostream& operator<<(
        std::ostream& os,
        const TimestampWithTimezone& in) {
      return os << "TimestampWithTimezone(milliSeconds: " << in.milliSeconds_
                << ", timezoneId: " << in.timezoneId_ << ")";
    }
  };

  std::optional<TimestampWithTimezone> parseDatetime(
      const std::optional<std::string>& input,
      const std::optional<std::string>& format) {
    auto resultVector = evaluate(
        "parse_datetime(c0, c1)",
        makeRowVector(
            {makeNullableFlatVector<std::string>({input}),
             makeNullableFlatVector<std::string>({format})}));
    EXPECT_EQ(1, resultVector->size());

    if (resultVector->isNullAt(0)) {
      return std::nullopt;
    }

    auto timestampWithTimezone =
        resultVector->as<FlatVector<int64_t>>()->valueAt(0);
    return TimestampWithTimezone{
        unpackMillisUtc(timestampWithTimezone),
        unpackZoneKeyId(timestampWithTimezone)};
  }

  std::optional<Timestamp> dateParse(
      const std::optional<std::string>& input,
      const std::optional<std::string>& format) {
    auto resultVector = evaluate(
        "date_parse(c0, c1)",
        makeRowVector(
            {makeNullableFlatVector<std::string>({input}),
             makeNullableFlatVector<std::string>({format})}));
    EXPECT_EQ(1, resultVector->size());

    if (resultVector->isNullAt(0)) {
      return std::nullopt;
    }
    return resultVector->as<SimpleVector<Timestamp>>()->valueAt(0);
  }

  std::optional<std::string> dateFormat(
      std::optional<Timestamp> timestamp,
      const std::string& format) {
    auto resultVector = evaluate(
        "date_format(c0, c1)",
        makeRowVector(
            {makeNullableFlatVector<Timestamp>({timestamp}),
             makeNullableFlatVector<std::string>({format})}));
    return resultVector->as<SimpleVector<StringView>>()->valueAt(0);
  }

  std::optional<std::string> formatDatetime(
      std::optional<Timestamp> timestamp,
      const std::string& format) {
    auto resultVector = evaluate(
        "format_datetime(c0, c1)",
        makeRowVector(
            {makeNullableFlatVector<Timestamp>({timestamp}),
             makeNullableFlatVector<std::string>({format})}));
    return resultVector->as<SimpleVector<StringView>>()->valueAt(0);
  }

  std::optional<std::string> formatDatetimeWithTimezone(
      std::optional<Timestamp> timestamp,
      std::optional<std::string> timeZoneName,
      const std::string& format) {
    auto resultVector = evaluate(
        "format_datetime(c0, c1)",
        makeRowVector(
            {makeTimestampWithTimeZoneVector(
                 timestamp.value().toMillis(), timeZoneName.value().c_str()),
             makeNullableFlatVector<std::string>({format})}));
    return resultVector->as<SimpleVector<StringView>>()->valueAt(0);
  }

  template <typename T>
  std::optional<T> evaluateWithTimestampWithTimezone(
      const std::string& expression,
      std::optional<int64_t> timestamp,
      const std::optional<std::string>& timeZoneName) {
    if (!timestamp.has_value() || !timeZoneName.has_value()) {
      return evaluateOnce<T>(
          expression,
          makeRowVector({BaseVector::createNullConstant(
              TIMESTAMP_WITH_TIME_ZONE(), 1, pool())}));
    }

    return evaluateOnce<T>(
        expression,
        makeRowVector({makeTimestampWithTimeZoneVector(
            timestamp.value(), timeZoneName.value().c_str())}));
  }

  VectorPtr evaluateWithTimestampWithTimezone(
      const std::string& expression,
      std::optional<int64_t> timestamp,
      const std::optional<std::string>& timeZoneName) {
    if (!timestamp.has_value() || !timeZoneName.has_value()) {
      return evaluate(
          expression,
          makeRowVector({BaseVector::createNullConstant(
              TIMESTAMP_WITH_TIME_ZONE(), 1, pool())}));
    }

    return evaluate(
        expression,
        makeRowVector({makeTimestampWithTimeZoneVector(
            timestamp.value(), timeZoneName.value().c_str())}));
  }

  static int32_t parseDate(const std::string& dateStr) {
    return DATE()->toDays(dateStr);
  }

  VectorPtr makeTimestampWithTimeZoneVector(int64_t timestamp, const char* tz) {
    auto tzid = util::getTimeZoneID(tz);

    return makeNullableFlatVector<int64_t>(
        {pack(timestamp, tzid)}, TIMESTAMP_WITH_TIME_ZONE());
  }

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

  int32_t getCurrentDate(const std::optional<std::string>& timeZone) {
    return parseDate(date::format(
        "%Y-%m-%d",
        timeZone.has_value()
            ? date::make_zoned(
                  timeZone.value(), std::chrono::system_clock::now())
            : std::chrono::system_clock::now()));
  }
};

bool operator==(
    const DateTimeFunctionsTest::TimestampWithTimezone& a,
    const DateTimeFunctionsTest::TimestampWithTimezone& b) {
  return a.milliSeconds_ == b.milliSeconds_ && a.timezoneId_ == b.timezoneId_;
}

TEST_F(DateTimeFunctionsTest, dateTruncSignatures) {
  auto signatures = getSignatureStrings("date_trunc");
  ASSERT_EQ(3, signatures.size());

  ASSERT_EQ(
      1,
      signatures.count(
          "(varchar,timestamp with time zone) -> timestamp with time zone"));
  ASSERT_EQ(1, signatures.count("(varchar,date) -> date"));
  ASSERT_EQ(1, signatures.count("(varchar,timestamp) -> timestamp"));
}

TEST_F(DateTimeFunctionsTest, parseDatetimeSignatures) {
  auto signatures = getSignatureStrings("parse_datetime");
  ASSERT_EQ(1, signatures.size());

  ASSERT_EQ(
      1, signatures.count("(varchar,varchar) -> timestamp with time zone"));
}

TEST_F(DateTimeFunctionsTest, dayOfXxxSignatures) {
  for (const auto& name : {"day", "day_of_month"}) {
    SCOPED_TRACE(name);
    auto signatures = getSignatureStrings(name);
    ASSERT_EQ(4, signatures.size());

    ASSERT_EQ(1, signatures.count("(timestamp with time zone) -> bigint"));
    ASSERT_EQ(1, signatures.count("(date) -> bigint"));
    ASSERT_EQ(1, signatures.count("(timestamp) -> bigint"));
    ASSERT_EQ(1, signatures.count("(interval day to second) -> bigint"));
  }

  for (const auto& name : {"day_of_year", "doy", "day_of_week", "dow"}) {
    SCOPED_TRACE(name);
    auto signatures = getSignatureStrings(name);
    ASSERT_EQ(3, signatures.size());

    ASSERT_EQ(1, signatures.count("(timestamp with time zone) -> bigint"));
    ASSERT_EQ(1, signatures.count("(date) -> bigint"));
    ASSERT_EQ(1, signatures.count("(timestamp) -> bigint"));
  }
}

// Test cases from PrestoDB [1] are covered here as well:
// Timestamp(998474645, 321000000) from "TIMESTAMP '2001-08-22 03:04:05.321'"
// Timestamp(998423705, 321000000) from "TIMESTAMP '2001-08-22 03:04:05.321
// +07:09'"
// [1]https://github.com/prestodb/presto/blob/master/presto-main/src/test/java/com/facebook/presto/operator/scalar/TestDateTimeFunctionsBase.java
TEST_F(DateTimeFunctionsTest, toUnixtime) {
  const auto toUnixtime = [&](std::optional<Timestamp> t) {
    return evaluateOnce<double>("to_unixtime(c0)", t);
  };

  EXPECT_EQ(0, toUnixtime(Timestamp(0, 0)));
  EXPECT_EQ(-0.999991, toUnixtime(Timestamp(-1, 9000)));
  EXPECT_EQ(4000000000, toUnixtime(Timestamp(4000000000, 0)));
  EXPECT_EQ(4000000000.123, toUnixtime(Timestamp(4000000000, 123000000)));
  EXPECT_EQ(-9999999998.9, toUnixtime(Timestamp(-9999999999, 100000000)));
  EXPECT_EQ(998474645.321, toUnixtime(Timestamp(998474645, 321000000)));
  EXPECT_EQ(998423705.321, toUnixtime(Timestamp(998423705, 321000000)));

  const auto toUnixtimeWTZ = [&](int64_t timestamp, const char* tz) {
    auto input = makeTimestampWithTimeZoneVector(timestamp, tz);

    return evaluateOnce<double>("to_unixtime(c0)", makeRowVector({input}));
  };

  // 1639426440000 is milliseconds (from PrestoDb '2021-12-13+20:14+00:00').
  EXPECT_EQ(0, toUnixtimeWTZ(0, "+00:00"));
  EXPECT_EQ(1639426440, toUnixtimeWTZ(1639426440000, "+00:00"));
  EXPECT_EQ(1639426440, toUnixtimeWTZ(1639426440000, "+03:00"));
  EXPECT_EQ(1639426440, toUnixtimeWTZ(1639426440000, "+04:00"));
  EXPECT_EQ(1639426440, toUnixtimeWTZ(1639426440000, "-07:00"));
  EXPECT_EQ(1639426440, toUnixtimeWTZ(1639426440000, "-00:01"));
  EXPECT_EQ(1639426440, toUnixtimeWTZ(1639426440000, "+00:01"));
  EXPECT_EQ(1639426440, toUnixtimeWTZ(1639426440000, "-14:00"));
  EXPECT_EQ(1639426440, toUnixtimeWTZ(1639426440000, "+14:00"));

  // test floating point and negative time
  EXPECT_EQ(16394.26, toUnixtimeWTZ(16394260, "+00:00"));
  EXPECT_EQ(16394.26, toUnixtimeWTZ(16394260, "+03:00"));
  EXPECT_EQ(16394.26, toUnixtimeWTZ(16394260, "-07:00"));
  EXPECT_EQ(-16394.26, toUnixtimeWTZ(-16394260, "+12:00"));
  EXPECT_EQ(-16394.26, toUnixtimeWTZ(-16394260, "-06:00"));
}

TEST_F(DateTimeFunctionsTest, fromUnixtimeRountTrip) {
  const auto testRoundTrip = [&](std::optional<Timestamp> t) {
    auto r = evaluateOnce<Timestamp>("from_unixtime(to_unixtime(c0))", t);
    EXPECT_EQ(r->getSeconds(), t->getSeconds()) << "at " << t->toString();
    EXPECT_NEAR(r->getNanos(), t->getNanos(), 1'000) << "at " << t->toString();
    return r;
  };

  testRoundTrip(Timestamp(0, 0));
  testRoundTrip(Timestamp(-1, 9000000));
  testRoundTrip(Timestamp(4000000000, 0));
  testRoundTrip(Timestamp(4000000000, 123000000));
  testRoundTrip(Timestamp(-9999999999, 100000000));
  testRoundTrip(Timestamp(998474645, 321000000));
  testRoundTrip(Timestamp(998423705, 321000000));
}

TEST_F(DateTimeFunctionsTest, fromUnixtimeWithTimeZone) {
  static const double kNan = std::numeric_limits<double>::quiet_NaN();

  vector_size_t size = 37;

  auto unixtimeAt = [](vector_size_t row) -> double {
    return 1631800000.12345 + row * 11;
  };

  auto unixtimes = makeFlatVector<double>(size, unixtimeAt);

  // Constant timezone parameter.
  {
    auto result =
        evaluate("from_unixtime(c0, '+01:00')", makeRowVector({unixtimes}));

    auto expected = makeTimestampWithTimeZoneVector(
        size,
        [&](auto row) { return unixtimeAt(row) * 1'000; },
        [](auto /*row*/) { return 900; });
    assertEqualVectors(expected, result);

    // NaN timestamp.
    result = evaluate(
        "from_unixtime(c0, '+01:00')",
        makeRowVector({makeFlatVector<double>({kNan, kNan})}));
    expected = makeTimestampWithTimeZoneVector(
        2, [](auto /*row*/) { return 0; }, [](auto /*row*/) { return 900; });
    assertEqualVectors(expected, result);
  }

  // Variable timezone parameter.
  {
    std::vector<TimeZoneKey> timezoneIds = {900, 960, 1020, 1080, 1140};
    std::vector<std::string> timezoneNames = {
        "+01:00", "+02:00", "+03:00", "+04:00", "+05:00"};

    auto timezones = makeFlatVector<StringView>(
        size, [&](auto row) { return StringView(timezoneNames[row % 5]); });

    auto result = evaluate(
        "from_unixtime(c0, c1)", makeRowVector({unixtimes, timezones}));
    auto expected = makeTimestampWithTimeZoneVector(
        size,
        [&](auto row) { return unixtimeAt(row) * 1'000; },
        [&](auto row) { return timezoneIds[row % 5]; });
    assertEqualVectors(expected, result);

    // NaN timestamp.
    result = evaluate(
        "from_unixtime(c0, c1)",
        makeRowVector({
            makeFlatVector<double>({kNan, kNan}),
            makeNullableFlatVector<StringView>({"+01:00", "+02:00"}),
        }));
    auto timezonesVector = std::vector<TimeZoneKey>{900, 960};
    expected = makeTimestampWithTimeZoneVector(
        timezonesVector.size(),
        [](auto /*row*/) { return 0; },
        [&](auto row) { return timezonesVector[row]; });
    assertEqualVectors(expected, result);
  }
}

TEST_F(DateTimeFunctionsTest, fromUnixtime) {
  const auto fromUnixtime = [&](std::optional<double> t) {
    return evaluateOnce<Timestamp>("from_unixtime(c0)", t);
  };

  static const double kInf = std::numeric_limits<double>::infinity();
  static const double kNan = std::numeric_limits<double>::quiet_NaN();

  EXPECT_EQ(Timestamp(0, 0), fromUnixtime(0));
  EXPECT_EQ(Timestamp(-1, 9000000), fromUnixtime(-0.991));
  EXPECT_EQ(Timestamp(1, 0), fromUnixtime(1 - 1e-10));
  EXPECT_EQ(Timestamp(4000000000, 0), fromUnixtime(4000000000));
  EXPECT_EQ(
      Timestamp(9'223'372'036'854'775, 807'000'000), fromUnixtime(3.87111e+37));
  EXPECT_EQ(Timestamp(4000000000, 123000000), fromUnixtime(4000000000.123));
  EXPECT_EQ(Timestamp(9'223'372'036'854'775, 807'000'000), fromUnixtime(kInf));
  EXPECT_EQ(
      Timestamp(-9'223'372'036'854'776, 192'000'000), fromUnixtime(-kInf));
  EXPECT_EQ(Timestamp(0, 0), fromUnixtime(kNan));
}

TEST_F(DateTimeFunctionsTest, year) {
  const auto year = [&](std::optional<Timestamp> date) {
    return evaluateOnce<int64_t>("year(c0)", date);
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
    return evaluateOnce<int64_t, int32_t>("year(c0)", {date}, {DATE()});
  };
  EXPECT_EQ(std::nullopt, year(std::nullopt));
  EXPECT_EQ(1970, year(DATE()->toDays("1970-01-01")));
  EXPECT_EQ(1969, year(DATE()->toDays("1969-12-31")));
  EXPECT_EQ(2020, year(DATE()->toDays("2020-01-01")));
  EXPECT_EQ(1920, year(DATE()->toDays("1920-12-31")));
}

TEST_F(DateTimeFunctionsTest, yearTimestampWithTimezone) {
  EXPECT_EQ(
      1969,
      evaluateWithTimestampWithTimezone<int64_t>("year(c0)", 0, "-01:00"));
  EXPECT_EQ(
      1970,
      evaluateWithTimestampWithTimezone<int64_t>("year(c0)", 0, "+00:00"));
  EXPECT_EQ(
      1973,
      evaluateWithTimestampWithTimezone<int64_t>(
          "year(c0)", 123456789000, "+14:00"));
  EXPECT_EQ(
      1966,
      evaluateWithTimestampWithTimezone<int64_t>(
          "year(c0)", -123456789000, "+03:00"));
  EXPECT_EQ(
      2001,
      evaluateWithTimestampWithTimezone<int64_t>(
          "year(c0)", 987654321000, "-07:00"));
  EXPECT_EQ(
      1938,
      evaluateWithTimestampWithTimezone<int64_t>(
          "year(c0)", -987654321000, "-13:00"));
  EXPECT_EQ(
      std::nullopt,
      evaluateWithTimestampWithTimezone<int64_t>(
          "year(c0)", std::nullopt, std::nullopt));
}

TEST_F(DateTimeFunctionsTest, weekDate) {
  const auto weekDate = [&](const char* dateString) {
    auto date = std::make_optional(parseDate(dateString));
    auto week =
        evaluateOnce<int64_t, int32_t>("week(c0)", {date}, {DATE()}).value();
    auto weekOfYear =
        evaluateOnce<int64_t, int32_t>("week_of_year(c0)", {date}, {DATE()})
            .value();
    VELOX_CHECK_EQ(
        week, weekOfYear, "week and week_of_year must return the same value");
    return week;
  };

  EXPECT_EQ(1, weekDate("1919-12-31"));
  EXPECT_EQ(1, weekDate("1920-01-01"));
  EXPECT_EQ(1, weekDate("1920-01-04"));
  EXPECT_EQ(2, weekDate("1920-01-05"));
  EXPECT_EQ(53, weekDate("1960-01-01"));
  EXPECT_EQ(53, weekDate("1960-01-03"));
  EXPECT_EQ(1, weekDate("1960-01-04"));
  EXPECT_EQ(1, weekDate("1969-12-31"));
  EXPECT_EQ(1, weekDate("1970-01-01"));
  EXPECT_EQ(1, weekDate("0001-01-01"));
  EXPECT_EQ(52, weekDate("9999-12-31"));
}

TEST_F(DateTimeFunctionsTest, week) {
  const auto weekTimestamp = [&](const char* time) {
    auto timestampInSeconds = util::fromTimeString(time) / 1'000'000;
    auto timestamp =
        std::make_optional(Timestamp(timestampInSeconds * 100'000'000, 0));
    auto week = evaluateOnce<int64_t>("week(c0)", timestamp).value();
    auto weekOfYear =
        evaluateOnce<int64_t>("week_of_year(c0)", timestamp).value();
    VELOX_CHECK_EQ(
        week, weekOfYear, "week and week_of_year must return the same value");
    return week;
  };

  EXPECT_EQ(1, weekTimestamp("00:00:00"));
  EXPECT_EQ(10, weekTimestamp("11:59:59"));
  EXPECT_EQ(51, weekTimestamp("06:01:01"));
  EXPECT_EQ(24, weekTimestamp("06:59:59"));
  EXPECT_EQ(27, weekTimestamp("12:00:01"));
  EXPECT_EQ(7, weekTimestamp("12:59:59"));
}

TEST_F(DateTimeFunctionsTest, weekTimestampWithTimezone) {
  const auto weekTimestampTimezone = [&](const char* time,
                                         const char* timezone) {
    auto timestampInSeconds = util::fromTimeString(time) / 1'000'000;
    auto timestamp = timestampInSeconds * 100'000'000;
    auto week = evaluateWithTimestampWithTimezone<int64_t>(
                    "week(c0)", timestamp, timezone)
                    .value();
    auto weekOfYear = evaluateWithTimestampWithTimezone<int64_t>(
                          "week_of_year(c0)", timestamp, timezone)
                          .value();
    VELOX_CHECK_EQ(
        week, weekOfYear, "week and week_of_year must return the same value");
    return week;
  };

  EXPECT_EQ(1, weekTimestampTimezone("00:00:00", "-12:00"));
  EXPECT_EQ(1, weekTimestampTimezone("00:00:00", "+12:00"));
  EXPECT_EQ(47, weekTimestampTimezone("11:59:59", "-12:00"));
  EXPECT_EQ(47, weekTimestampTimezone("11:59:59", "+12:00"));
  EXPECT_EQ(33, weekTimestampTimezone("06:01:01", "-12:00"));
  EXPECT_EQ(34, weekTimestampTimezone("06:01:01", "+12:00"));
  EXPECT_EQ(47, weekTimestampTimezone("12:00:01", "-12:00"));
  EXPECT_EQ(47, weekTimestampTimezone("12:00:01", "+12:00"));
}

TEST_F(DateTimeFunctionsTest, quarter) {
  const auto quarter = [&](std::optional<Timestamp> date) {
    return evaluateOnce<int64_t>("quarter(c0)", date);
  };
  EXPECT_EQ(std::nullopt, quarter(std::nullopt));
  EXPECT_EQ(1, quarter(Timestamp(0, 0)));
  EXPECT_EQ(4, quarter(Timestamp(-1, 9000)));
  EXPECT_EQ(4, quarter(Timestamp(4000000000, 0)));
  EXPECT_EQ(4, quarter(Timestamp(4000000000, 123000000)));
  EXPECT_EQ(2, quarter(Timestamp(990000000, 321000000)));
  EXPECT_EQ(3, quarter(Timestamp(998423705, 321000000)));

  setQueryTimeZone("Pacific/Apia");

  EXPECT_EQ(std::nullopt, quarter(std::nullopt));
  EXPECT_EQ(4, quarter(Timestamp(0, 0)));
  EXPECT_EQ(4, quarter(Timestamp(-1, Timestamp::kMaxNanos)));
  EXPECT_EQ(4, quarter(Timestamp(4000000000, 0)));
  EXPECT_EQ(4, quarter(Timestamp(4000000000, 123000000)));
  EXPECT_EQ(2, quarter(Timestamp(990000000, 321000000)));
  EXPECT_EQ(3, quarter(Timestamp(998423705, 321000000)));
}

TEST_F(DateTimeFunctionsTest, quarterDate) {
  const auto quarter = [&](std::optional<int32_t> date) {
    return evaluateOnce<int64_t, int32_t>("quarter(c0)", {date}, {DATE()});
  };
  EXPECT_EQ(std::nullopt, quarter(std::nullopt));
  EXPECT_EQ(1, quarter(0));
  EXPECT_EQ(4, quarter(-1));
  EXPECT_EQ(4, quarter(-40));
  EXPECT_EQ(2, quarter(110));
  EXPECT_EQ(3, quarter(200));
  EXPECT_EQ(1, quarter(18262));
  EXPECT_EQ(1, quarter(-18262));
}

TEST_F(DateTimeFunctionsTest, quarterTimestampWithTimezone) {
  EXPECT_EQ(
      4,
      evaluateWithTimestampWithTimezone<int64_t>("quarter(c0)", 0, "-01:00"));
  EXPECT_EQ(
      1,
      evaluateWithTimestampWithTimezone<int64_t>("quarter(c0)", 0, "+00:00"));
  EXPECT_EQ(
      4,
      evaluateWithTimestampWithTimezone<int64_t>(
          "quarter(c0)", 123456789000, "+14:00"));
  EXPECT_EQ(
      1,
      evaluateWithTimestampWithTimezone<int64_t>(
          "quarter(c0)", -123456789000, "+03:00"));
  EXPECT_EQ(
      2,
      evaluateWithTimestampWithTimezone<int64_t>(
          "quarter(c0)", 987654321000, "-07:00"));
  EXPECT_EQ(
      3,
      evaluateWithTimestampWithTimezone<int64_t>(
          "quarter(c0)", -987654321000, "-13:00"));
  EXPECT_EQ(
      std::nullopt,
      evaluateWithTimestampWithTimezone<int64_t>(
          "quarter(c0)", std::nullopt, std::nullopt));
}

TEST_F(DateTimeFunctionsTest, month) {
  const auto month = [&](std::optional<Timestamp> date) {
    return evaluateOnce<int64_t>("month(c0)", date);
  };
  EXPECT_EQ(std::nullopt, month(std::nullopt));
  EXPECT_EQ(1, month(Timestamp(0, 0)));
  EXPECT_EQ(12, month(Timestamp(-1, 9000)));
  EXPECT_EQ(10, month(Timestamp(4000000000, 0)));
  EXPECT_EQ(10, month(Timestamp(4000000000, 123000000)));
  EXPECT_EQ(8, month(Timestamp(998474645, 321000000)));
  EXPECT_EQ(8, month(Timestamp(998423705, 321000000)));

  setQueryTimeZone("Pacific/Apia");

  EXPECT_EQ(std::nullopt, month(std::nullopt));
  EXPECT_EQ(12, month(Timestamp(0, 0)));
  EXPECT_EQ(12, month(Timestamp(-1, Timestamp::kMaxNanos)));
  EXPECT_EQ(10, month(Timestamp(4000000000, 0)));
  EXPECT_EQ(10, month(Timestamp(4000000000, 123000000)));
  EXPECT_EQ(8, month(Timestamp(998474645, 321000000)));
  EXPECT_EQ(8, month(Timestamp(998423705, 321000000)));
}

TEST_F(DateTimeFunctionsTest, monthDate) {
  const auto month = [&](std::optional<int32_t> date) {
    return evaluateOnce<int64_t, int32_t>("month(c0)", {date}, {DATE()});
  };
  EXPECT_EQ(std::nullopt, month(std::nullopt));
  EXPECT_EQ(1, month(0));
  EXPECT_EQ(12, month(-1));
  EXPECT_EQ(11, month(-40));
  EXPECT_EQ(2, month(40));
  EXPECT_EQ(1, month(18262));
  EXPECT_EQ(1, month(-18262));
}

TEST_F(DateTimeFunctionsTest, monthTimestampWithTimezone) {
  EXPECT_EQ(
      12, evaluateWithTimestampWithTimezone<int64_t>("month(c0)", 0, "-01:00"));
  EXPECT_EQ(
      1, evaluateWithTimestampWithTimezone<int64_t>("month(c0)", 0, "+00:00"));
  EXPECT_EQ(
      11,
      evaluateWithTimestampWithTimezone<int64_t>(
          "month(c0)", 123456789000, "+14:00"));
  EXPECT_EQ(
      2,
      evaluateWithTimestampWithTimezone<int64_t>(
          "month(c0)", -123456789000, "+03:00"));
  EXPECT_EQ(
      4,
      evaluateWithTimestampWithTimezone<int64_t>(
          "month(c0)", 987654321000, "-07:00"));
  EXPECT_EQ(
      9,
      evaluateWithTimestampWithTimezone<int64_t>(
          "month(c0)", -987654321000, "-13:00"));
  EXPECT_EQ(
      std::nullopt,
      evaluateWithTimestampWithTimezone<int64_t>(
          "month(c0)", std::nullopt, std::nullopt));
}

TEST_F(DateTimeFunctionsTest, hour) {
  const auto hour = [&](std::optional<Timestamp> date) {
    return evaluateOnce<int64_t>("hour(c0)", date);
  };
  EXPECT_EQ(std::nullopt, hour(std::nullopt));
  EXPECT_EQ(0, hour(Timestamp(0, 0)));
  EXPECT_EQ(23, hour(Timestamp(-1, 9000)));
  EXPECT_EQ(23, hour(Timestamp(-1, Timestamp::kMaxNanos)));
  EXPECT_EQ(7, hour(Timestamp(4000000000, 0)));
  EXPECT_EQ(7, hour(Timestamp(4000000000, 123000000)));
  EXPECT_EQ(10, hour(Timestamp(998474645, 321000000)));
  EXPECT_EQ(19, hour(Timestamp(998423705, 321000000)));

  setQueryTimeZone("Pacific/Apia");

  EXPECT_EQ(std::nullopt, hour(std::nullopt));
  EXPECT_EQ(13, hour(Timestamp(0, 0)));
  EXPECT_EQ(12, hour(Timestamp(-1, Timestamp::kMaxNanos)));
  // Disabled for now because the TZ for Pacific/Apia in 2096 varies between
  // systems.
  // EXPECT_EQ(21, hour(Timestamp(4000000000, 0)));
  // EXPECT_EQ(21, hour(Timestamp(4000000000, 123000000)));
  EXPECT_EQ(23, hour(Timestamp(998474645, 321000000)));
  EXPECT_EQ(8, hour(Timestamp(998423705, 321000000)));
}

TEST_F(DateTimeFunctionsTest, hourTimestampWithTimezone) {
  EXPECT_EQ(
      20,
      evaluateWithTimestampWithTimezone<int64_t>(
          "hour(c0)", 998423705000, "+01:00"));
  EXPECT_EQ(
      12,
      evaluateWithTimestampWithTimezone<int64_t>(
          "hour(c0)", 41028000, "+01:00"));
  EXPECT_EQ(
      13,
      evaluateWithTimestampWithTimezone<int64_t>(
          "hour(c0)", 41028000, "+02:00"));
  EXPECT_EQ(
      14,
      evaluateWithTimestampWithTimezone<int64_t>(
          "hour(c0)", 41028000, "+03:00"));
  EXPECT_EQ(
      8,
      evaluateWithTimestampWithTimezone<int64_t>(
          "hour(c0)", 41028000, "-03:00"));
  EXPECT_EQ(
      1,
      evaluateWithTimestampWithTimezone<int64_t>(
          "hour(c0)", 41028000, "+14:00"));
  EXPECT_EQ(
      9,
      evaluateWithTimestampWithTimezone<int64_t>(
          "hour(c0)", -100000, "-14:00"));
  EXPECT_EQ(
      2,
      evaluateWithTimestampWithTimezone<int64_t>(
          "hour(c0)", -41028000, "+14:00"));
  EXPECT_EQ(
      std::nullopt,
      evaluateWithTimestampWithTimezone<int64_t>(
          "hour(c0)", std::nullopt, std::nullopt));
}

TEST_F(DateTimeFunctionsTest, hourDate) {
  const auto hour = [&](std::optional<int32_t> date) {
    return evaluateOnce<int64_t, int32_t>("hour(c0)", {date}, {DATE()});
  };
  EXPECT_EQ(std::nullopt, hour(std::nullopt));
  EXPECT_EQ(0, hour(0));
  EXPECT_EQ(0, hour(-1));
  EXPECT_EQ(0, hour(-40));
  EXPECT_EQ(0, hour(40));
  EXPECT_EQ(0, hour(18262));
  EXPECT_EQ(0, hour(-18262));
}

TEST_F(DateTimeFunctionsTest, dayOfMonth) {
  const auto day = [&](std::optional<Timestamp> date) {
    return evaluateOnce<int64_t>("day_of_month(c0)", date);
  };
  EXPECT_EQ(std::nullopt, day(std::nullopt));
  EXPECT_EQ(1, day(Timestamp(0, 0)));
  EXPECT_EQ(31, day(Timestamp(-1, 9000)));
  EXPECT_EQ(30, day(Timestamp(1632989700, 0)));
  EXPECT_EQ(1, day(Timestamp(1633076100, 0)));
  EXPECT_EQ(6, day(Timestamp(1633508100, 0)));
  EXPECT_EQ(31, day(Timestamp(1635668100, 0)));

  setQueryTimeZone("Pacific/Apia");

  EXPECT_EQ(std::nullopt, day(std::nullopt));
  EXPECT_EQ(31, day(Timestamp(0, 0)));
  EXPECT_EQ(31, day(Timestamp(-1, 9000)));
  EXPECT_EQ(30, day(Timestamp(1632989700, 0)));
  EXPECT_EQ(1, day(Timestamp(1633076100, 0)));
  EXPECT_EQ(6, day(Timestamp(1633508100, 0)));
  EXPECT_EQ(31, day(Timestamp(1635668100, 0)));
}

TEST_F(DateTimeFunctionsTest, dayOfMonthDate) {
  const auto day = [&](std::optional<int32_t> date) {
    return evaluateOnce<int64_t, int32_t>("day_of_month(c0)", {date}, {DATE()});
  };
  EXPECT_EQ(std::nullopt, day(std::nullopt));
  EXPECT_EQ(1, day(0));
  EXPECT_EQ(31, day(-1));
  EXPECT_EQ(22, day(-40));
  EXPECT_EQ(10, day(40));
  EXPECT_EQ(1, day(18262));
  EXPECT_EQ(2, day(-18262));
}

TEST_F(DateTimeFunctionsTest, dayOfMonthInterval) {
  const auto day = [&](int64_t millis) {
    auto result = evaluateOnce<int64_t, int64_t>(
        "day_of_month(c0)", {millis}, {INTERVAL_DAY_TIME()});

    auto result2 = evaluateOnce<int64_t, int64_t>(
        "day(c0)", {millis}, {INTERVAL_DAY_TIME()});

    EXPECT_EQ(result, result2);
    return result;
  };

  EXPECT_EQ(1, day(kMillisInDay));
  EXPECT_EQ(1, day(kMillisInDay + kMillisInHour));
  EXPECT_EQ(10, day(10 * kMillisInDay + 7 * kMillisInHour));
  EXPECT_EQ(-10, day(-10 * kMillisInDay - 7 * kMillisInHour));
}

TEST_F(DateTimeFunctionsTest, plusMinusDateIntervalYearMonth) {
  const auto makeInput = [&](const std::string& date, int32_t interval) {
    return makeRowVector({
        makeNullableFlatVector<int32_t>({parseDate(date)}, DATE()),
        makeNullableFlatVector<int32_t>({interval}, INTERVAL_YEAR_MONTH()),
    });
  };

  const auto plus = [&](const std::string& date, int32_t interval) {
    return evaluateOnce<int32_t>("c0 + c1", makeInput(date, interval));
  };

  const auto minus = [&](const std::string& date, int32_t interval) {
    return evaluateOnce<int32_t>("c0 - c1", makeInput(date, interval));
  };

  EXPECT_EQ(parseDate("2021-10-15"), plus("2020-10-15", 12));
  EXPECT_EQ(parseDate("2022-01-15"), plus("2020-10-15", 15));
  EXPECT_EQ(parseDate("2020-11-15"), plus("2020-10-15", 1));
  EXPECT_EQ(parseDate("2020-11-30"), plus("2020-10-30", 1));
  EXPECT_EQ(parseDate("2020-11-30"), plus("2020-10-31", 1));
  EXPECT_EQ(parseDate("2021-02-28"), plus("2020-02-28", 12));
  EXPECT_EQ(parseDate("2021-02-28"), plus("2020-02-29", 12));
  EXPECT_EQ(parseDate("2016-02-29"), plus("2020-02-29", -48));

  EXPECT_EQ(parseDate("2019-10-15"), minus("2020-10-15", 12));
  EXPECT_EQ(parseDate("2019-07-15"), minus("2020-10-15", 15));
  EXPECT_EQ(parseDate("2020-09-15"), minus("2020-10-15", 1));
  EXPECT_EQ(parseDate("2020-09-30"), minus("2020-10-30", 1));
  EXPECT_EQ(parseDate("2020-09-30"), minus("2020-10-31", 1));
  EXPECT_EQ(parseDate("2019-02-28"), minus("2020-02-29", 12));
  EXPECT_EQ(parseDate("2019-02-28"), minus("2020-02-28", 12));
  EXPECT_EQ(parseDate("2024-02-29"), minus("2020-02-29", -48));
}

TEST_F(DateTimeFunctionsTest, plusMinusDateIntervalDayTime) {
  const auto plus = [&](std::optional<int32_t> date,
                        std::optional<int64_t> interval) {
    return evaluateOnce<int32_t>(
        "c0 + c1",
        makeRowVector({
            makeNullableFlatVector<int32_t>({date}, DATE()),
            makeNullableFlatVector<int64_t>({interval}, INTERVAL_DAY_TIME()),
        }));
  };
  const auto minus = [&](std::optional<int32_t> date,
                         std::optional<int64_t> interval) {
    return evaluateOnce<int32_t>(
        "c0 - c1",
        makeRowVector({
            makeNullableFlatVector<int32_t>({date}, DATE()),
            makeNullableFlatVector<int64_t>({interval}, INTERVAL_DAY_TIME()),
        }));
  };

  const int64_t oneDay(kMillisInDay * 1);
  const int64_t tenDays(kMillisInDay * 10);
  const int64_t partDay(kMillisInHour * 25);
  const int32_t baseDate(20000);
  const int32_t baseDatePlus1(20000 + 1);
  const int32_t baseDatePlus10(20000 + 10);
  const int32_t baseDateMinus1(20000 - 1);
  const int32_t baseDateMinus10(20000 - 10);

  EXPECT_EQ(std::nullopt, plus(std::nullopt, oneDay));
  EXPECT_EQ(std::nullopt, plus(10000, std::nullopt));
  EXPECT_EQ(baseDatePlus1, plus(baseDate, oneDay));
  EXPECT_EQ(baseDatePlus10, plus(baseDate, tenDays));
  EXPECT_EQ(std::nullopt, minus(std::nullopt, oneDay));
  EXPECT_EQ(std::nullopt, minus(10000, std::nullopt));
  EXPECT_EQ(baseDateMinus1, minus(baseDate, oneDay));
  EXPECT_EQ(baseDateMinus10, minus(baseDate, tenDays));

  EXPECT_THROW(plus(baseDate, partDay), VeloxUserError);
  EXPECT_THROW(minus(baseDate, partDay), VeloxUserError);
}

TEST_F(DateTimeFunctionsTest, timestampMinusIntervalYearMonth) {
  const auto minus = [&](std::optional<std::string> timestamp,
                         std::optional<int32_t> interval) {
    return evaluateOnce<std::string>(
        "date_format(date_parse(c0, '%Y-%m-%d %H:%i:%s') - c1, '%Y-%m-%d %H:%i:%s')",
        makeRowVector({
            makeNullableFlatVector<std::string>({timestamp}, VARCHAR()),
            makeNullableFlatVector<int32_t>({interval}, INTERVAL_YEAR_MONTH()),
        }));
  };

  EXPECT_EQ("2001-01-03 04:05:06", minus("2001-02-03 04:05:06", 1));
  EXPECT_EQ("2000-04-03 04:05:06", minus("2001-02-03 04:05:06", 10));
  EXPECT_EQ("1999-06-03 04:05:06", minus("2001-02-03 04:05:06", 20));

  // Some special dates.
  EXPECT_EQ("2001-04-30 04:05:06", minus("2001-05-31 04:05:06", 1));
  EXPECT_EQ("2001-03-30 04:05:06", minus("2001-04-30 04:05:06", 1));
  EXPECT_EQ("2001-02-28 04:05:06", minus("2001-03-30 04:05:06", 1));
  EXPECT_EQ("2000-02-29 04:05:06", minus("2000-03-30 04:05:06", 1));
  EXPECT_EQ("2000-01-29 04:05:06", minus("2000-02-29 04:05:06", 1));
}

TEST_F(DateTimeFunctionsTest, timestampPlusIntervalYearMonth) {
  const auto plus = [&](std::optional<std::string> timestamp,
                        std::optional<int32_t> interval) {
    // timestamp + interval.
    auto result1 = evaluateOnce<std::string>(
        "date_format(date_parse(c0, '%Y-%m-%d %H:%i:%s') + c1, '%Y-%m-%d %H:%i:%s')",
        makeRowVector(
            {makeNullableFlatVector<std::string>({timestamp}, VARCHAR()),
             makeNullableFlatVector<int32_t>(
                 {interval}, INTERVAL_YEAR_MONTH())}));

    // interval + timestamp.
    auto result2 = evaluateOnce<std::string>(
        "date_format(c1 + date_parse(c0, '%Y-%m-%d %H:%i:%s'), '%Y-%m-%d %H:%i:%s')",
        makeRowVector(
            {makeNullableFlatVector<std::string>({timestamp}, VARCHAR()),
             makeNullableFlatVector<int32_t>(
                 {interval}, INTERVAL_YEAR_MONTH())}));

    // They should be the same.
    EXPECT_EQ(result1, result2);

    return result1;
  };

  EXPECT_EQ("2001-02-03 04:05:06", plus("2001-01-03 04:05:06", 1));
  EXPECT_EQ("2001-02-03 04:05:06", plus("2000-04-03 04:05:06", 10));
  EXPECT_EQ("2001-02-03 04:05:06", plus("1999-06-03 04:05:06", 20));

  // Some special dates.
  EXPECT_EQ("2001-06-30 04:05:06", plus("2001-05-31 04:05:06", 1));
  EXPECT_EQ("2001-05-30 04:05:06", plus("2001-04-30 04:05:06", 1));
  EXPECT_EQ("2001-02-28 04:05:06", plus("2001-01-31 04:05:06", 1));
  EXPECT_EQ("2000-02-29 04:05:06", plus("2000-01-31 04:05:06", 1));
  EXPECT_EQ("2000-02-29 04:05:06", plus("2000-01-29 04:05:06", 1));
}

TEST_F(DateTimeFunctionsTest, plusMinusTimestampIntervalDayTime) {
  constexpr int64_t kLongMax = std::numeric_limits<int64_t>::max();
  constexpr int64_t kLongMin = std::numeric_limits<int64_t>::min();

  const auto minus = [&](std::optional<Timestamp> timestamp,
                         std::optional<int64_t> interval) {
    return evaluateOnce<Timestamp>(
        "c0 - c1",
        makeRowVector({
            makeNullableFlatVector<Timestamp>({timestamp}),
            makeNullableFlatVector<int64_t>({interval}, INTERVAL_DAY_TIME()),
        }));
  };

  EXPECT_EQ(std::nullopt, minus(std::nullopt, std::nullopt));
  EXPECT_EQ(std::nullopt, minus(std::nullopt, 1));
  EXPECT_EQ(std::nullopt, minus(Timestamp(0, 0), std::nullopt));
  EXPECT_EQ(Timestamp(0, 0), minus(Timestamp(0, 0), 0));
  EXPECT_EQ(Timestamp(0, 0), minus(Timestamp(10, 0), 10'000));
  EXPECT_EQ(Timestamp(-10, 0), minus(Timestamp(10, 0), 20'000));
  EXPECT_EQ(
      Timestamp(-2, 50 * Timestamp::kNanosecondsInMillisecond),
      minus(Timestamp(0, 50 * Timestamp::kNanosecondsInMillisecond), 2'000));
  EXPECT_EQ(
      Timestamp(-3, 995 * Timestamp::kNanosecondsInMillisecond),
      minus(Timestamp(0, 0), 2'005));
  EXPECT_EQ(
      Timestamp(9223372036854774, 809000000),
      minus(Timestamp(-1, 0), kLongMax));
  EXPECT_EQ(
      Timestamp(-9223372036854775, 192000000),
      minus(Timestamp(1, 0), kLongMin));

  const auto plusAndVerify = [&](std::optional<Timestamp> timestamp,
                                 std::optional<int64_t> interval,
                                 std::optional<Timestamp> expected) {
    EXPECT_EQ(
        expected,
        evaluateOnce<Timestamp>(
            "c0 + c1",
            makeRowVector({
                makeNullableFlatVector<Timestamp>({timestamp}),
                makeNullableFlatVector<int64_t>(
                    {interval}, INTERVAL_DAY_TIME()),
            })));
    EXPECT_EQ(
        expected,
        evaluateOnce<Timestamp>(
            "c1 + c0",
            makeRowVector({
                makeNullableFlatVector<Timestamp>({timestamp}),
                makeNullableFlatVector<int64_t>(
                    {interval}, INTERVAL_DAY_TIME()),
            })));
  };

  plusAndVerify(std::nullopt, std::nullopt, std::nullopt);
  plusAndVerify(std::nullopt, 1, std::nullopt);
  plusAndVerify(Timestamp(0, 0), std::nullopt, std::nullopt);
  plusAndVerify(Timestamp(0, 0), 0, Timestamp(0, 0));
  plusAndVerify(Timestamp(0, 0), 10'000, Timestamp(10, 0));
  plusAndVerify(
      Timestamp(0, 0),
      20'005,
      Timestamp(20, 5 * Timestamp::kNanosecondsInMillisecond));
  plusAndVerify(
      Timestamp(0, 0),
      -30'005,
      Timestamp(-31, 995 * Timestamp::kNanosecondsInMillisecond));
  plusAndVerify(
      Timestamp(1, 0), kLongMax, Timestamp(-9223372036854775, 191000000));
  plusAndVerify(
      Timestamp(0, 0), kLongMin, Timestamp(-9223372036854776, 192000000));
  plusAndVerify(
      Timestamp(-1, 0), kLongMin, Timestamp(9223372036854774, 808000000));
}

TEST_F(DateTimeFunctionsTest, minusTimestamp) {
  const auto minus = [&](std::optional<int64_t> t1, std::optional<int64_t> t2) {
    const auto timestamp1 = (t1.has_value()) ? Timestamp(t1.value(), 0)
                                             : std::optional<Timestamp>();
    const auto timestamp2 = (t2.has_value()) ? Timestamp(t2.value(), 0)
                                             : std::optional<Timestamp>();
    return evaluateOnce<int64_t>(
        "c0 - c1",
        makeRowVector({
            makeNullableFlatVector<Timestamp>({timestamp1}),
            makeNullableFlatVector<Timestamp>({timestamp2}),
        }));
  };

  EXPECT_EQ(std::nullopt, minus(std::nullopt, std::nullopt));
  EXPECT_EQ(std::nullopt, minus(1, std::nullopt));
  EXPECT_EQ(std::nullopt, minus(std::nullopt, 1));
  EXPECT_EQ(1000, minus(1, 0));
  EXPECT_EQ(-1000, minus(1, 2));
  VELOX_ASSERT_THROW(
      minus(Timestamp::kMinSeconds, Timestamp::kMaxSeconds),
      "Could not convert Timestamp(-9223372036854776, 0) to milliseconds");
}

TEST_F(DateTimeFunctionsTest, dayOfMonthTimestampWithTimezone) {
  EXPECT_EQ(
      31,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_month(c0)", 0, "-01:00"));
  EXPECT_EQ(
      1,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_month(c0)", 0, "+00:00"));
  EXPECT_EQ(
      30,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_month(c0)", 123456789000, "+14:00"));
  EXPECT_EQ(
      2,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_month(c0)", -123456789000, "+03:00"));
  EXPECT_EQ(
      18,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_month(c0)", 987654321000, "-07:00"));
  EXPECT_EQ(
      14,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_month(c0)", -987654321000, "-13:00"));
  EXPECT_EQ(
      std::nullopt,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_month(c0)", std::nullopt, std::nullopt));
}

TEST_F(DateTimeFunctionsTest, dayOfWeek) {
  const auto day = [&](std::optional<Timestamp> date) {
    return evaluateOnce<int64_t>("day_of_week(c0)", date);
  };
  EXPECT_EQ(std::nullopt, day(std::nullopt));
  EXPECT_EQ(4, day(Timestamp(0, 0)));
  EXPECT_EQ(3, day(Timestamp(-1, 9000)));
  EXPECT_EQ(1, day(Timestamp(1633940100, 0)));
  EXPECT_EQ(2, day(Timestamp(1634026500, 0)));
  EXPECT_EQ(3, day(Timestamp(1634112900, 0)));
  EXPECT_EQ(4, day(Timestamp(1634199300, 0)));
  EXPECT_EQ(5, day(Timestamp(1634285700, 0)));
  EXPECT_EQ(6, day(Timestamp(1634372100, 0)));
  EXPECT_EQ(7, day(Timestamp(1633853700, 0)));

  setQueryTimeZone("Pacific/Apia");

  EXPECT_EQ(std::nullopt, day(std::nullopt));
  EXPECT_EQ(3, day(Timestamp(0, 0)));
  EXPECT_EQ(3, day(Timestamp(-1, 9000)));
  EXPECT_EQ(1, day(Timestamp(1633940100, 0)));
  EXPECT_EQ(2, day(Timestamp(1634026500, 0)));
  EXPECT_EQ(3, day(Timestamp(1634112900, 0)));
  EXPECT_EQ(4, day(Timestamp(1634199300, 0)));
  EXPECT_EQ(5, day(Timestamp(1634285700, 0)));
  EXPECT_EQ(6, day(Timestamp(1634372100, 0)));
  EXPECT_EQ(7, day(Timestamp(1633853700, 0)));
}

TEST_F(DateTimeFunctionsTest, dayOfWeekDate) {
  const auto day = [&](std::optional<int32_t> date) {
    return evaluateOnce<int64_t, int32_t>("day_of_week(c0)", {date}, {DATE()});
  };
  EXPECT_EQ(std::nullopt, day(std::nullopt));
  EXPECT_EQ(4, day(0));
  EXPECT_EQ(3, day(-1));
  EXPECT_EQ(6, day(-40));
  EXPECT_EQ(2, day(40));
  EXPECT_EQ(3, day(18262));
  EXPECT_EQ(5, day(-18262));
}

TEST_F(DateTimeFunctionsTest, dayOfWeekTimestampWithTimezone) {
  EXPECT_EQ(
      3,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_week(c0)", 0, "-01:00"));
  EXPECT_EQ(
      4,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_week(c0)", 0, "+00:00"));
  EXPECT_EQ(
      5,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_week(c0)", 123456789000, "+14:00"));
  EXPECT_EQ(
      3,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_week(c0)", -123456789000, "+03:00"));
  EXPECT_EQ(
      3,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_week(c0)", 987654321000, "-07:00"));
  EXPECT_EQ(
      3,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_week(c0)", -987654321000, "-13:00"));
  EXPECT_EQ(
      std::nullopt,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_week(c0)", std::nullopt, std::nullopt));
}

TEST_F(DateTimeFunctionsTest, dayOfYear) {
  const auto day = [&](std::optional<Timestamp> date) {
    return evaluateOnce<int64_t>("day_of_year(c0)", date);
  };
  EXPECT_EQ(std::nullopt, day(std::nullopt));
  EXPECT_EQ(1, day(Timestamp(0, 0)));
  EXPECT_EQ(365, day(Timestamp(-1, 9000)));
  EXPECT_EQ(273, day(Timestamp(1632989700, 0)));
  EXPECT_EQ(274, day(Timestamp(1633076100, 0)));
  EXPECT_EQ(279, day(Timestamp(1633508100, 0)));
  EXPECT_EQ(304, day(Timestamp(1635668100, 0)));

  setQueryTimeZone("Pacific/Apia");

  EXPECT_EQ(std::nullopt, day(std::nullopt));
  EXPECT_EQ(365, day(Timestamp(0, 0)));
  EXPECT_EQ(365, day(Timestamp(-1, 9000)));
  EXPECT_EQ(273, day(Timestamp(1632989700, 0)));
  EXPECT_EQ(274, day(Timestamp(1633076100, 0)));
  EXPECT_EQ(279, day(Timestamp(1633508100, 0)));
  EXPECT_EQ(304, day(Timestamp(1635668100, 0)));
}

TEST_F(DateTimeFunctionsTest, dayOfYearDate) {
  const auto day = [&](std::optional<int32_t> date) {
    return evaluateOnce<int64_t, int32_t>("day_of_year(c0)", {date}, {DATE()});
  };
  EXPECT_EQ(std::nullopt, day(std::nullopt));
  EXPECT_EQ(1, day(0));
  EXPECT_EQ(365, day(-1));
  EXPECT_EQ(326, day(-40));
  EXPECT_EQ(41, day(40));
  EXPECT_EQ(1, day(18262));
  EXPECT_EQ(2, day(-18262));
}

TEST_F(DateTimeFunctionsTest, dayOfYearTimestampWithTimezone) {
  EXPECT_EQ(
      365,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_year(c0)", 0, "-01:00"));
  EXPECT_EQ(
      1,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_year(c0)", 0, "+00:00"));
  EXPECT_EQ(
      334,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_year(c0)", 123456789000, "+14:00"));
  EXPECT_EQ(
      33,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_year(c0)", -123456789000, "+03:00"));
  EXPECT_EQ(
      108,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_year(c0)", 987654321000, "-07:00"));
  EXPECT_EQ(
      257,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_year(c0)", -987654321000, "-13:00"));
  EXPECT_EQ(
      std::nullopt,
      evaluateWithTimestampWithTimezone<int64_t>(
          "day_of_year(c0)", std::nullopt, std::nullopt));
}

TEST_F(DateTimeFunctionsTest, yearOfWeek) {
  const auto yow = [&](std::optional<Timestamp> date) {
    return evaluateOnce<int64_t>("year_of_week(c0)", date);
  };
  EXPECT_EQ(std::nullopt, yow(std::nullopt));
  EXPECT_EQ(1970, yow(Timestamp(0, 0)));
  EXPECT_EQ(1970, yow(Timestamp(-1, 0)));
  EXPECT_EQ(1969, yow(Timestamp(-345600, 0)));
  EXPECT_EQ(1970, yow(Timestamp(-259200, 0)));
  EXPECT_EQ(1970, yow(Timestamp(31536000, 0)));
  EXPECT_EQ(1970, yow(Timestamp(31708800, 0)));
  EXPECT_EQ(1971, yow(Timestamp(31795200, 0)));
  EXPECT_EQ(2021, yow(Timestamp(1632989700, 0)));

  setQueryTimeZone("Pacific/Apia");

  EXPECT_EQ(std::nullopt, yow(std::nullopt));
  EXPECT_EQ(1970, yow(Timestamp(0, 0)));
  EXPECT_EQ(1970, yow(Timestamp(-1, 0)));
  EXPECT_EQ(1969, yow(Timestamp(-345600, 0)));
  EXPECT_EQ(1969, yow(Timestamp(-259200, 0)));
  EXPECT_EQ(1970, yow(Timestamp(31536000, 0)));
  EXPECT_EQ(1970, yow(Timestamp(31708800, 0)));
  EXPECT_EQ(1970, yow(Timestamp(31795200, 0)));
  EXPECT_EQ(2021, yow(Timestamp(1632989700, 0)));
}

TEST_F(DateTimeFunctionsTest, yearOfWeekDate) {
  const auto yow = [&](std::optional<int32_t> date) {
    return evaluateOnce<int64_t, int32_t>("year_of_week(c0)", {date}, {DATE()});
  };
  EXPECT_EQ(std::nullopt, yow(std::nullopt));
  EXPECT_EQ(1970, yow(0));
  EXPECT_EQ(1970, yow(-1));
  EXPECT_EQ(1969, yow(-4));
  EXPECT_EQ(1970, yow(-3));
  EXPECT_EQ(1970, yow(365));
  EXPECT_EQ(1970, yow(367));
  EXPECT_EQ(1971, yow(368));
  EXPECT_EQ(2021, yow(18900));
}

TEST_F(DateTimeFunctionsTest, yearOfWeekTimestampWithTimezone) {
  EXPECT_EQ(
      1970,
      evaluateWithTimestampWithTimezone<int64_t>(
          "year_of_week(c0)", 0, "-01:00"));
  EXPECT_EQ(
      1970,
      evaluateWithTimestampWithTimezone<int64_t>(
          "year_of_week(c0)", 0, "+00:00"));
  EXPECT_EQ(
      1973,
      evaluateWithTimestampWithTimezone<int64_t>(
          "year_of_week(c0)", 123456789000, "+14:00"));
  EXPECT_EQ(
      1966,
      evaluateWithTimestampWithTimezone<int64_t>(
          "year_of_week(c0)", -123456789000, "+03:00"));
  EXPECT_EQ(
      2001,
      evaluateWithTimestampWithTimezone<int64_t>(
          "year_of_week(c0)", 987654321000, "-07:00"));
  EXPECT_EQ(
      1938,
      evaluateWithTimestampWithTimezone<int64_t>(
          "year_of_week(c0)", -987654321000, "-13:00"));
  EXPECT_EQ(
      std::nullopt,
      evaluateWithTimestampWithTimezone<int64_t>(
          "year_of_week(c0)", std::nullopt, std::nullopt));
}

TEST_F(DateTimeFunctionsTest, minute) {
  const auto minute = [&](std::optional<Timestamp> date) {
    return evaluateOnce<int64_t>("minute(c0)", date);
  };
  EXPECT_EQ(std::nullopt, minute(std::nullopt));
  EXPECT_EQ(0, minute(Timestamp(0, 0)));
  EXPECT_EQ(59, minute(Timestamp(-1, 9000)));
  EXPECT_EQ(6, minute(Timestamp(4000000000, 0)));
  EXPECT_EQ(6, minute(Timestamp(4000000000, 123000000)));
  EXPECT_EQ(4, minute(Timestamp(998474645, 321000000)));
  EXPECT_EQ(55, minute(Timestamp(998423705, 321000000)));

  setQueryTimeZone("Asia/Kolkata");

  EXPECT_EQ(std::nullopt, minute(std::nullopt));
  EXPECT_EQ(30, minute(Timestamp(0, 0)));
  EXPECT_EQ(29, minute(Timestamp(-1, 9000)));
  EXPECT_EQ(36, minute(Timestamp(4000000000, 0)));
  EXPECT_EQ(36, minute(Timestamp(4000000000, 123000000)));
  EXPECT_EQ(34, minute(Timestamp(998474645, 321000000)));
  EXPECT_EQ(25, minute(Timestamp(998423705, 321000000)));
}

TEST_F(DateTimeFunctionsTest, minuteDate) {
  const auto minute = [&](std::optional<int32_t> date) {
    return evaluateOnce<int64_t, int32_t>("minute(c0)", {date}, {DATE()});
  };
  EXPECT_EQ(std::nullopt, minute(std::nullopt));
  EXPECT_EQ(0, minute(0));
  EXPECT_EQ(0, minute(-1));
  EXPECT_EQ(0, minute(40));
  EXPECT_EQ(0, minute(40));
  EXPECT_EQ(0, minute(18262));
  EXPECT_EQ(0, minute(-18262));
}

TEST_F(DateTimeFunctionsTest, minuteTimestampWithTimezone) {
  EXPECT_EQ(
      std::nullopt,
      evaluateWithTimestampWithTimezone<int64_t>(
          "minute(c0)", std::nullopt, std::nullopt));
  EXPECT_EQ(
      std::nullopt,
      evaluateWithTimestampWithTimezone<int64_t>(
          "minute(c0)", std::nullopt, "Asia/Kolkata"));
  EXPECT_EQ(
      0, evaluateWithTimestampWithTimezone<int64_t>("minute(c0)", 0, "+00:00"));
  EXPECT_EQ(
      30,
      evaluateWithTimestampWithTimezone<int64_t>("minute(c0)", 0, "+05:30"));
  EXPECT_EQ(
      6,
      evaluateWithTimestampWithTimezone<int64_t>(
          "minute(c0)", 4000000000000, "+00:00"));
  EXPECT_EQ(
      36,
      evaluateWithTimestampWithTimezone<int64_t>(
          "minute(c0)", 4000000000000, "+05:30"));
  EXPECT_EQ(
      4,
      evaluateWithTimestampWithTimezone<int64_t>(
          "minute(c0)", 998474645000, "+00:00"));
  EXPECT_EQ(
      34,
      evaluateWithTimestampWithTimezone<int64_t>(
          "minute(c0)", 998474645000, "+05:30"));
  EXPECT_EQ(
      59,
      evaluateWithTimestampWithTimezone<int64_t>(
          "minute(c0)", -1000, "+00:00"));
  EXPECT_EQ(
      29,
      evaluateWithTimestampWithTimezone<int64_t>(
          "minute(c0)", -1000, "+05:30"));
}

TEST_F(DateTimeFunctionsTest, second) {
  const auto second = [&](std::optional<Timestamp> timestamp) {
    return evaluateOnce<int64_t>("second(c0)", timestamp);
  };
  EXPECT_EQ(std::nullopt, second(std::nullopt));
  EXPECT_EQ(0, second(Timestamp(0, 0)));
  EXPECT_EQ(40, second(Timestamp(4000000000, 0)));
  EXPECT_EQ(59, second(Timestamp(-1, 123000000)));
  EXPECT_EQ(59, second(Timestamp(-1, Timestamp::kMaxNanos)));
}

TEST_F(DateTimeFunctionsTest, secondDate) {
  const auto second = [&](std::optional<int32_t> date) {
    return evaluateOnce<int64_t, int32_t>("second(c0)", {date}, {DATE()});
  };
  EXPECT_EQ(std::nullopt, second(std::nullopt));
  EXPECT_EQ(0, second(0));
  EXPECT_EQ(0, second(-1));
  EXPECT_EQ(0, second(-40));
  EXPECT_EQ(0, second(40));
  EXPECT_EQ(0, second(18262));
  EXPECT_EQ(0, second(-18262));
}

TEST_F(DateTimeFunctionsTest, secondTimestampWithTimezone) {
  EXPECT_EQ(
      std::nullopt,
      evaluateWithTimestampWithTimezone<int64_t>(
          "second(c0)", std::nullopt, std::nullopt));
  EXPECT_EQ(
      std::nullopt,
      evaluateWithTimestampWithTimezone<int64_t>(
          "second(c0)", std::nullopt, "+05:30"));
  EXPECT_EQ(
      0, evaluateWithTimestampWithTimezone<int64_t>("second(c0)", 0, "+00:00"));
  EXPECT_EQ(
      0, evaluateWithTimestampWithTimezone<int64_t>("second(c0)", 0, "+05:30"));
  EXPECT_EQ(
      40,
      evaluateWithTimestampWithTimezone<int64_t>(
          "second(c0)", 4000000000000, "+00:00"));
  EXPECT_EQ(
      40,
      evaluateWithTimestampWithTimezone<int64_t>(
          "second(c0)", 4000000000000, "+05:30"));
  EXPECT_EQ(
      59,
      evaluateWithTimestampWithTimezone<int64_t>(
          "second(c0)", -1000, "+00:00"));
  EXPECT_EQ(
      59,
      evaluateWithTimestampWithTimezone<int64_t>(
          "second(c0)", -1000, "+05:30"));
}

TEST_F(DateTimeFunctionsTest, millisecond) {
  const auto millisecond = [&](std::optional<Timestamp> timestamp) {
    return evaluateOnce<int64_t>("millisecond(c0)", timestamp);
  };
  EXPECT_EQ(std::nullopt, millisecond(std::nullopt));
  EXPECT_EQ(0, millisecond(Timestamp(0, 0)));
  EXPECT_EQ(0, millisecond(Timestamp(4000000000, 0)));
  EXPECT_EQ(123, millisecond(Timestamp(-1, 123000000)));
  EXPECT_EQ(999, millisecond(Timestamp(-1, Timestamp::kMaxNanos)));
}

TEST_F(DateTimeFunctionsTest, millisecondDate) {
  const auto millisecond = [&](std::optional<int32_t> date) {
    return evaluateOnce<int64_t, int32_t>("millisecond(c0)", {date}, {DATE()});
  };
  EXPECT_EQ(std::nullopt, millisecond(std::nullopt));
  EXPECT_EQ(0, millisecond(0));
  EXPECT_EQ(0, millisecond(-1));
  EXPECT_EQ(0, millisecond(-40));
  EXPECT_EQ(0, millisecond(40));
  EXPECT_EQ(0, millisecond(18262));
  EXPECT_EQ(0, millisecond(-18262));
}

TEST_F(DateTimeFunctionsTest, millisecondTimestampWithTimezone) {
  EXPECT_EQ(
      std::nullopt,
      evaluateWithTimestampWithTimezone<int64_t>(
          "millisecond(c0)", std::nullopt, std::nullopt));
  EXPECT_EQ(
      std::nullopt,
      evaluateWithTimestampWithTimezone<int64_t>(
          "millisecond(c0)", std::nullopt, "+05:30"));
  EXPECT_EQ(
      0,
      evaluateWithTimestampWithTimezone<int64_t>(
          "millisecond(c0)", 0, "+00:00"));
  EXPECT_EQ(
      0,
      evaluateWithTimestampWithTimezone<int64_t>(
          "millisecond(c0)", 0, "+05:30"));
  EXPECT_EQ(
      123,
      evaluateWithTimestampWithTimezone<int64_t>(
          "millisecond(c0)", 4000000000123, "+00:00"));
  EXPECT_EQ(
      123,
      evaluateWithTimestampWithTimezone<int64_t>(
          "millisecond(c0)", 4000000000123, "+05:30"));
  EXPECT_EQ(
      20,
      evaluateWithTimestampWithTimezone<int64_t>(
          "millisecond(c0)", -980, "+00:00"));
  EXPECT_EQ(
      20,
      evaluateWithTimestampWithTimezone<int64_t>(
          "millisecond(c0)", -980, "+05:30"));
}

TEST_F(DateTimeFunctionsTest, dateTrunc) {
  const auto dateTrunc = [&](const std::string& unit,
                             std::optional<Timestamp> timestamp) {
    return evaluateOnce<Timestamp>(
        fmt::format("date_trunc('{}', c0)", unit), timestamp);
  };

  setQueryTimeZone("America/Los_Angeles");

  EXPECT_EQ(std::nullopt, dateTrunc("second", std::nullopt));
  EXPECT_EQ(Timestamp(0, 0), dateTrunc("second", Timestamp(0, 0)));
  EXPECT_EQ(Timestamp(0, 0), dateTrunc("second", Timestamp(0, 123)));
  EXPECT_EQ(
      Timestamp(998474645, 0),
      dateTrunc("second", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(998474640, 0),
      dateTrunc("minute", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(998474400, 0),
      dateTrunc("hour", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(998463600, 0),
      dateTrunc("day", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(996649200, 0),
      dateTrunc("month", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(993970800, 0),
      dateTrunc("quarter", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(978336000, 0),
      dateTrunc("year", Timestamp(998'474'645, 321'001'234)));

  setQueryTimeZone("Asia/Kolkata");

  EXPECT_EQ(std::nullopt, dateTrunc("second", std::nullopt));
  EXPECT_EQ(Timestamp(0, 0), dateTrunc("second", Timestamp(0, 0)));
  EXPECT_EQ(Timestamp(0, 0), dateTrunc("second", Timestamp(0, 123)));
  EXPECT_EQ(
      Timestamp(998474645, 0),
      dateTrunc("second", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(998474640, 0),
      dateTrunc("minute", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(998472600, 0),
      dateTrunc("hour", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(998418600, 0),
      dateTrunc("day", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(996604200, 0),
      dateTrunc("month", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(993925800, 0),
      dateTrunc("quarter", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(978287400, 0),
      dateTrunc("year", Timestamp(998'474'645, 321'001'234)));
}

TEST_F(DateTimeFunctionsTest, dateTruncDate) {
  const auto dateTrunc = [&](const std::string& unit,
                             std::optional<int32_t> date) {
    return evaluateOnce<int32_t, int32_t>(
        fmt::format("date_trunc('{}', c0)", unit), {date}, {DATE()});
  };

  EXPECT_EQ(std::nullopt, dateTrunc("year", std::nullopt));

  // Date(0) is 1970-01-01.
  EXPECT_EQ(0, dateTrunc("day", 0));
  EXPECT_EQ(0, dateTrunc("year", 0));
  EXPECT_EQ(0, dateTrunc("quarter", 0));
  EXPECT_EQ(0, dateTrunc("month", 0));
  EXPECT_THROW(dateTrunc("second", 0), VeloxUserError);
  EXPECT_THROW(dateTrunc("minute", 0), VeloxUserError);
  EXPECT_THROW(dateTrunc("hour", 0), VeloxUserError);

  // Date(18297) is 2020-02-05.
  EXPECT_EQ(18297, dateTrunc("day", 18297));
  EXPECT_EQ(18293, dateTrunc("month", 18297));
  EXPECT_EQ(18262, dateTrunc("quarter", 18297));
  EXPECT_EQ(18262, dateTrunc("year", 18297));
  EXPECT_THROW(dateTrunc("second", 18297), VeloxUserError);
  EXPECT_THROW(dateTrunc("minute", 18297), VeloxUserError);
  EXPECT_THROW(dateTrunc("hour", 18297), VeloxUserError);

  // Date(-18297) is 1919-11-28.
  EXPECT_EQ(-18297, dateTrunc("day", -18297));
  EXPECT_EQ(-18324, dateTrunc("month", -18297));
  EXPECT_EQ(-18355, dateTrunc("quarter", -18297));
  EXPECT_EQ(-18628, dateTrunc("year", -18297));
  EXPECT_THROW(dateTrunc("second", -18297), VeloxUserError);
  EXPECT_THROW(dateTrunc("minute", -18297), VeloxUserError);
  EXPECT_THROW(dateTrunc("hour", -18297), VeloxUserError);
}

TEST_F(DateTimeFunctionsTest, dateTruncDateForWeek) {
  const auto dateTrunc = [&](const std::string& unit,
                             std::optional<int32_t> date) {
    return evaluateOnce<int32_t, int32_t>(
        fmt::format("date_trunc('{}', c0)", unit), {date}, {DATE()});
  };

  // Date(19576) is 2023-08-07, which is Monday, should return Monday
  EXPECT_EQ(19576, dateTrunc("week", 19576));

  // Date(19579) is 2023-08-10, Thur, should return Monday
  EXPECT_EQ(19576, dateTrunc("week", 19579));

  // Date(19570) is 2023-08-01, A non-Monday(Tue) date at the beginning of a
  // month when the preceding Monday falls in the previous month. should return
  // 2023-07-31(19569), which is previous Monday
  EXPECT_EQ(19569, dateTrunc("week", 19570));

  // Date(19358) is 2023-01-01, A non-Monday(Sunday) date at the beginning of
  // January where the preceding Monday falls in the previous year. should
  // return 2022-12-26(19352), which is previous Monday
  EXPECT_EQ(19352, dateTrunc("week", 19358));

  // Date(19783) is 2024-03-01, A non-Monday(Friday) date which will go over to
  // a leap day (February 29th) in a leap year. should return 2024-02-26(19352),
  // which is previous Monday
  EXPECT_EQ(19779, dateTrunc("week", 19783));
}

// Reference dateTruncDateForWeek for test cases explanaitons
TEST_F(DateTimeFunctionsTest, dateTruncTimeStampForWeek) {
  const auto dateTrunc = [&](const std::string& unit,
                             std::optional<Timestamp> timestamp) {
    return evaluateOnce<Timestamp>(
        fmt::format("date_trunc('{}', c0)", unit), timestamp);
  };

  EXPECT_EQ(
      Timestamp(19576 * 24 * 60 * 60, 0),
      dateTrunc("week", Timestamp(19576 * 24 * 60 * 60, 321'001'234)));

  EXPECT_EQ(
      Timestamp(19576 * 24 * 60 * 60, 0),
      dateTrunc("week", Timestamp(19579 * 24 * 60 * 60 + 500, 321'001'234)));

  EXPECT_EQ(
      Timestamp(19569 * 24 * 60 * 60, 0),
      dateTrunc("week", Timestamp(19570 * 24 * 60 * 60 + 500, 321'001'234)));

  EXPECT_EQ(
      Timestamp(19352 * 24 * 60 * 60, 0),
      dateTrunc("week", Timestamp(19358 * 24 * 60 * 60 + 500, 321'001'234)));

  EXPECT_EQ(
      Timestamp(19779 * 24 * 60 * 60, 0),
      dateTrunc("week", Timestamp(19783 * 24 * 60 * 60 + 500, 321'001'234)));
}

// Logical Steps
// 1. Convert Original Millisecond Input to UTC
// 2. Apply Time Zone Offset
// 3. Truncate to the Nearest "Unit"
// 4. Convert Back to UTC (remove Time Zone offset)
// 5. Convert Back to Milliseconds Since the Unix Epoch
TEST_F(DateTimeFunctionsTest, dateTruncTimeStampWithTimezoneForWeek) {
  const auto evaluateDateTrunc = [&](const std::string& truncUnit,
                                     int64_t inputTimestamp,
                                     const std::string& timeZone,
                                     int64_t expectedTimestamp) {
    assertEqualVectors(
        makeTimestampWithTimeZoneVector(expectedTimestamp, timeZone.c_str()),
        evaluateWithTimestampWithTimezone(
            fmt::format("date_trunc('{}', c0)", truncUnit),
            inputTimestamp,
            timeZone));
  };
  // input 2023-08-07 00:00:00 (19576 days) with timeZone +01:00
  // output 2023-08-06 23:00:00" in UTC.(1691362800000)
  auto inputMilli = int64_t(19576) * 24 * 60 * 60 * 1000;
  auto outputMilli = inputMilli - int64_t(1) * 60 * 60 * 1000;
  evaluateDateTrunc("week", inputMilli, "+01:00", outputMilli);

  // Date(19579) is 2023-08-10, Thur, should return Monday UTC (previous Sunday
  // in +03:00 timezone)
  inputMilli = int64_t(19579) * 24 * 60 * 60 * 1000;
  outputMilli = inputMilli - int64_t(3) * 24 * 60 * 60 * 1000 -
      int64_t(3) * 60 * 60 * 1000;
  evaluateDateTrunc("week", inputMilli, "+03:00", outputMilli);

  // Date(19570) is 2023-08-01, A non-Monday(Tue) date at the beginning of a
  // month when the preceding Monday falls in the previous month. should return
  // 2023-07-31(19569), which is previous Monday EXPECT_EQ(19569,
  // dateTrunc("week", 19570));
  inputMilli = int64_t(19570) * 24 * 60 * 60 * 1000;
  outputMilli = inputMilli - int64_t(1) * 24 * 60 * 60 * 1000 -
      int64_t(3) * 60 * 60 * 1000;
  evaluateDateTrunc("week", inputMilli, "+03:00", outputMilli);

  // Date(19570) is 2023-08-01, which is Tuesday; TimeZone is -05:00, so input
  // will become Monday. 2023-07-31 19:00:00, which will truncate to 2023-07-31
  // 00:00:00
  // TODO : Need to double-check with presto logic
  inputMilli = int64_t(19570) * 24 * 60 * 60 * 1000;
  outputMilli =
      int64_t(19569) * 24 * 60 * 60 * 1000 + int64_t(5) * 60 * 60 * 1000;
  evaluateDateTrunc("week", inputMilli, "-05:00", outputMilli);
}

TEST_F(DateTimeFunctionsTest, dateTruncTimeStampWithTimezoneStringForWeek) {
  const auto evaluateDateTruncFromStrings = [&](const std::string& truncUnit,
                                                const std::string&
                                                    inputTimestamp,
                                                const std::string&
                                                    expectedTimestamp) {
    assertEqualVectors(
        evaluate<FlatVector<int64_t>>(
            "parse_datetime(c0, 'YYYY-MM-dd+HH:mm:ssZZ')",
            makeRowVector({makeNullableFlatVector<StringView>(
                {StringView{expectedTimestamp}})})),
        evaluate<FlatVector<int64_t>>(
            fmt::format(
                "date_trunc('{}', parse_datetime(c0, 'YYYY-MM-dd+HH:mm:ssZZ'))",
                truncUnit),
            makeRowVector({makeNullableFlatVector<StringView>(
                {StringView{inputTimestamp}})})));
  };
  // Monday
  evaluateDateTruncFromStrings(
      "week", "2023-08-07+23:01:02+14:00", "2023-08-07+00:00:00+14:00");

  // Thur
  evaluateDateTruncFromStrings(
      "week", "2023-08-10+23:01:02+14:00", "2023-08-07+00:00:00+14:00");

  // 2023-08-01, A non-Monday(Tue) date at the beginning of a
  // month when the preceding Monday falls in the previous month. should return
  // 2023-07-31, which is previous Monday
  evaluateDateTruncFromStrings(
      "week", "2023-08-01+23:01:02+14:00", "2023-07-31+00:00:00+14:00");

  // 2023-01-01, A non-Monday(Sunday) date at the beginning of
  // January where the preceding Monday falls in the previous year. should
  // return 2022-12-26, which is previous Monday
  evaluateDateTruncFromStrings(
      "week", "2023-01-01+23:01:02+14:00", "2022-12-26+00:00:00+14:00");

  // 2024-03-01, A non-Monday(Friday) date which will go over to
  // a leap day (February 29th) in a leap year. should return 2024-02-26,
  // which is previous Monday
  evaluateDateTruncFromStrings(
      "week", "2024-03-01+23:01:02+14:00", "2024-02-26+00:00:00+14:00");
}
TEST_F(DateTimeFunctionsTest, dateTruncTimestampWithTimezone) {
  const auto evaluateDateTrunc = [&](const std::string& truncUnit,
                                     int64_t inputTimestamp,
                                     const std::string& timeZone,
                                     int64_t expectedTimestamp) {
    assertEqualVectors(
        makeTimestampWithTimeZoneVector(expectedTimestamp, timeZone.c_str()),
        evaluateWithTimestampWithTimezone(
            fmt::format("date_trunc('{}', c0)", truncUnit),
            inputTimestamp,
            timeZone));
  };

  evaluateDateTrunc("second", 123, "+01:00", 0);
  evaluateDateTrunc("second", 1123, "-03:00", 1000);
  evaluateDateTrunc("second", -1123, "+03:00", -2000);
  evaluateDateTrunc("second", 1234567000, "+14:00", 1234567000);
  evaluateDateTrunc("second", -1234567000, "-09:00", -1234567000);

  evaluateDateTrunc("minute", 123, "+01:00", 0);
  evaluateDateTrunc("minute", 1123, "-03:00", 0);
  evaluateDateTrunc("minute", -1123, "+03:00", -60000);
  evaluateDateTrunc("minute", 1234567000, "+14:00", 1234560000);
  evaluateDateTrunc("minute", -1234567000, "-09:00", -1234620000);

  evaluateDateTrunc("hour", 123, "+01:00", 0);
  evaluateDateTrunc("hour", 1123, "-03:00", 0);
  evaluateDateTrunc("hour", -1123, "+05:30", -1800000);
  evaluateDateTrunc("hour", 1234567000, "+14:00", 1231200000);
  evaluateDateTrunc("hour", -1234567000, "-09:30", -1236600000);

  evaluateDateTrunc("day", 123, "+01:00", -3600000);
  evaluateDateTrunc("day", 1123, "-03:00", -86400000 + 3600000 * 3);
  evaluateDateTrunc("day", -1123, "+05:30", 0 - 3600000 * 5 - 1800000);
  evaluateDateTrunc("day", 1234567000, "+14:00", 1159200000);
  evaluateDateTrunc("day", -1234567000, "-09:30", -1261800000);

  evaluateDateTrunc("month", 123, "-01:00", -2674800000);
  evaluateDateTrunc("month", 1234567000, "+14:00", -50400000);
  evaluateDateTrunc("month", -1234567000, "-09:30", -2644200000);

  evaluateDateTrunc("quarter", 123, "-01:00", -7945200000);
  evaluateDateTrunc("quarter", 123456789000, "+14:00", 118231200000);
  evaluateDateTrunc("quarter", -123456789000, "-09:30", -126196200000);

  evaluateDateTrunc("year", 123, "-01:00", -31532400000);
  evaluateDateTrunc("year", 123456789000, "+14:00", 94644000000);
  evaluateDateTrunc("year", -123456789000, "-09:30", -126196200000);

  const auto evaluateDateTruncFromStrings = [&](const std::string& truncUnit,
                                                const std::string&
                                                    inputTimestamp,
                                                const std::string&
                                                    expectedTimestamp) {
    assertEqualVectors(
        evaluate<FlatVector<int64_t>>(
            "parse_datetime(c0, 'YYYY-MM-dd+HH:mm:ssZZ')",
            makeRowVector({makeNullableFlatVector<StringView>(
                {StringView{expectedTimestamp}})})),
        evaluate<FlatVector<int64_t>>(
            fmt::format(
                "date_trunc('{}', parse_datetime(c0, 'YYYY-MM-dd+HH:mm:ssZZ'))",
                truncUnit),
            makeRowVector({makeNullableFlatVector<StringView>(
                {StringView{inputTimestamp}})})));
  };

  evaluateDateTruncFromStrings(
      "minute", "1972-05-20+23:01:02+14:00", "1972-05-20+23:01:00+14:00");
  evaluateDateTruncFromStrings(
      "minute", "1968-05-20+23:01:02+05:30", "1968-05-20+23:01:00+05:30");
  evaluateDateTruncFromStrings(
      "hour", "1972-05-20+23:01:02+03:00", "1972-05-20+23:00:00+03:00");
  evaluateDateTruncFromStrings(
      "hour", "1968-05-20+23:01:02-09:30", "1968-05-20+23:00:00-09:30");
  evaluateDateTruncFromStrings(
      "day", "1972-05-20+23:01:02-03:00", "1972-05-20+00:00:00-03:00");
  evaluateDateTruncFromStrings(
      "day", "1968-05-20+23:01:02+05:30", "1968-05-20+00:00:00+05:30");
  evaluateDateTruncFromStrings(
      "month", "1972-05-20+23:01:02-03:00", "1972-05-01+00:00:00-03:00");
  evaluateDateTruncFromStrings(
      "month", "1968-05-20+23:01:02+05:30", "1968-05-01+00:00:00+05:30");
  evaluateDateTruncFromStrings(
      "quarter", "1972-05-20+23:01:02-03:00", "1972-04-01+00:00:00-03:00");
  evaluateDateTruncFromStrings(
      "quarter", "1968-05-20+23:01:02+05:30", "1968-04-01+00:00:00+05:30");
  evaluateDateTruncFromStrings(
      "year", "1972-05-20+23:01:02-03:00", "1972-01-01+00:00:00-03:00");
  evaluateDateTruncFromStrings(
      "year", "1968-05-20+23:01:02+05:30", "1968-01-01+00:00:00+05:30");
}

TEST_F(DateTimeFunctionsTest, dateAddDate) {
  const auto dateAdd = [&](const std::string& unit,
                           std::optional<int32_t> value,
                           std::optional<int32_t> date) {
    return evaluateOnce<int32_t, int32_t>(
        fmt::format("date_add('{}', c0, c1)", unit),
        {value, date},
        {INTEGER(), DATE()});
  };

  // Check null behaviors
  EXPECT_EQ(std::nullopt, dateAdd("day", 1, std::nullopt));
  EXPECT_EQ(std::nullopt, dateAdd("month", std::nullopt, 0));

  // Check invalid units
  EXPECT_THROW(dateAdd("millisecond", 1, 0), VeloxUserError);
  EXPECT_THROW(dateAdd("second", 1, 0), VeloxUserError);
  EXPECT_THROW(dateAdd("minute", 1, 0), VeloxUserError);
  EXPECT_THROW(dateAdd("hour", 1, 0), VeloxUserError);
  EXPECT_THROW(dateAdd("invalid_unit", 1, 0), VeloxUserError);

  // Simple tests
  EXPECT_EQ(
      parseDate("2019-03-01"), dateAdd("day", 1, parseDate("2019-02-28")));
  EXPECT_EQ(
      parseDate("2020-03-28"), dateAdd("month", 13, parseDate("2019-02-28")));
  EXPECT_EQ(
      parseDate("2020-02-28"), dateAdd("quarter", 4, parseDate("2019-02-28")));
  EXPECT_EQ(
      parseDate("2020-02-28"), dateAdd("year", 1, parseDate("2019-02-28")));

  // Account for the last day of a year-month
  EXPECT_EQ(
      parseDate("2020-02-29"), dateAdd("day", 395, parseDate("2019-01-30")));
  EXPECT_EQ(
      parseDate("2020-02-29"), dateAdd("month", 13, parseDate("2019-01-30")));
  EXPECT_EQ(
      parseDate("2020-02-29"), dateAdd("quarter", 1, parseDate("2019-11-30")));
  EXPECT_EQ(
      parseDate("2030-02-28"), dateAdd("year", 10, parseDate("2020-02-29")));

  // Check for negative intervals
  EXPECT_EQ(
      parseDate("2019-02-28"), dateAdd("day", -366, parseDate("2020-02-29")));
  EXPECT_EQ(
      parseDate("2019-02-28"), dateAdd("month", -12, parseDate("2020-02-29")));
  EXPECT_EQ(
      parseDate("2019-02-28"), dateAdd("quarter", -4, parseDate("2020-02-29")));
  EXPECT_EQ(
      parseDate("2018-02-28"), dateAdd("year", -2, parseDate("2020-02-29")));
}

TEST_F(DateTimeFunctionsTest, dateAddTimestamp) {
  const auto dateAdd = [&](const std::string& unit,
                           std::optional<int32_t> value,
                           std::optional<Timestamp> timestamp) {
    return evaluateOnce<Timestamp>(
        fmt::format("date_add('{}', c0, c1)", unit), value, timestamp);
  };

  // Check null behaviors
  EXPECT_EQ(std::nullopt, dateAdd("second", 1, std::nullopt));
  EXPECT_EQ(std::nullopt, dateAdd("month", std::nullopt, Timestamp(0, 0)));

  // Check invalid units
  auto ts = Timestamp(0, 0);
  VELOX_ASSERT_THROW(
      dateAdd("invalid_unit", 1, ts),
      "Unsupported datetime unit: invalid_unit");
  VELOX_ASSERT_THROW(dateAdd("week", 1, ts), "Unsupported datetime unit: week");

  // Simple tests
  EXPECT_EQ(
      Timestamp(1551348061, 999'999) /*2019-02-28 10:01:01.000*/,
      dateAdd(
          "millisecond",
          60 * 1000 + 500,
          Timestamp(1551348000, 500'999'999) /*2019-02-28 10:00:00.500*/));
  EXPECT_EQ(
      Timestamp(1551434400, 500'999'999) /*2019-03-01 10:00:00.500*/,
      dateAdd(
          "second",
          60 * 60 * 24,
          Timestamp(1551348000, 500'999'999) /*2019-02-28 10:00:00.500*/));
  EXPECT_EQ(
      Timestamp(1551434400, 500'999'999) /*2019-03-01 10:00:00.500*/,
      dateAdd(
          "minute",
          60 * 24,
          Timestamp(1551348000, 500'999'999) /*2019-02-28 10:00:00.500*/));
  EXPECT_EQ(
      Timestamp(1551434400, 500'999'999) /*2019-03-01 10:00:00.500*/,
      dateAdd(
          "hour",
          24,
          Timestamp(1551348000, 500'999'999) /*2019-02-28 10:00:00.500*/));
  EXPECT_EQ(
      Timestamp(1551434400, 500'999'999) /*2019-03-01 10:00:00.500*/,
      dateAdd(
          "day",
          1,
          Timestamp(1551348000, 500'999'999) /*2019-02-28 10:00:00.500*/));
  EXPECT_EQ(
      Timestamp(1585389600, 500'999'999) /*2020-03-28 10:00:00.500*/,
      dateAdd(
          "month",
          12 + 1,
          Timestamp(1551348000, 500'999'999) /*2019-02-28 10:00:00.500*/));
  EXPECT_EQ(
      Timestamp(1582884000, 500'999'999) /*2020-02-28 10:00:00.500*/,
      dateAdd(
          "quarter",
          4,
          Timestamp(1551348000, 500'999'999) /*2019-02-28 10:00:00.500*/));
  EXPECT_EQ(
      Timestamp(1582884000, 500'999'999) /*2020-02-28 10:00:00.500*/,
      dateAdd(
          "year",
          1,
          Timestamp(1551348000, 500'999'999) /*2019-02-28 10:00:00.500*/));

  // Test for daylight saving. Daylight saving in US starts at 2021-03-14
  // 02:00:00 PST.
  // When adjust_timestamp_to_timezone is off, no Daylight saving occurs
  EXPECT_EQ(
      Timestamp(
          1615770000, 500'999'999) /*TIMESTAMP '2021-03-15 01:00:00.500' UTC*/,
      dateAdd(
          "millisecond",
          1000 * 60 * 60 * 24,
          Timestamp(
              1615683600,
              500'999'999) /*TIMESTAMP '2021-03-14 01:00:00.500 UTC'*/));
  EXPECT_EQ(
      Timestamp(
          1615770000, 500'999'999) /*TIMESTAMP '2021-03-15 01:00:00.500 UTC'*/,
      dateAdd(
          "second",
          60 * 60 * 24,
          Timestamp(
              1615683600,
              500'999'999) /*TIMESTAMP '2021-03-14 01:00:00.500 UTC'*/));
  EXPECT_EQ(
      Timestamp(
          1615770000, 500'999'999) /*TIMESTAMP '2021-03-15 01:00:00.500' UTC*/,
      dateAdd(
          "minute",
          60 * 24,
          Timestamp(
              1615683600,
              500'999'999) /*TIMESTAMP '2021-03-14 01:00:00.500' UTC*/));
  EXPECT_EQ(
      Timestamp(
          1615770000, 500'999'999) /*TIMESTAMP '2021-03-15 01:00:00.500' UTC*/,
      dateAdd(
          "hour",
          24,
          Timestamp(
              1615683600,
              500'999'999) /*TIMESTAMP '2021-03-14 01:00:00.500' UTC*/));
  EXPECT_EQ(
      Timestamp(
          1615770000, 500'999'999) /*TIMESTAMP '2021-03-15 01:00:00.500' UTC*/,
      dateAdd(
          "day",
          1,
          Timestamp(
              1615683600,
              500'999'999) /*TIMESTAMP '2021-03-14 01:00:00.500' UTC*/));
  EXPECT_EQ(
      Timestamp(
          1618362000, 500'999'999) /*TIMESTAMP '2021-04-14 01:00:00.500' UTC*/,
      dateAdd(
          "month",
          1,
          Timestamp(
              1615683600,
              500'999'999) /*TIMESTAMP '2021-03-14 01:00:00.500' UTC*/));
  EXPECT_EQ(
      Timestamp(
          1623632400, 500'999'999) /*TIMESTAMP '2021-06-14 01:00:00.500' UTC*/,
      dateAdd(
          "quarter",
          1,
          Timestamp(
              1615683600,
              500'999'999) /*TIMESTAMP '2021-03-14 01:00:00.500' UTC*/));
  EXPECT_EQ(
      Timestamp(
          1647219600, 500'999'999) /*TIMESTAMP '2022-03-14 01:00:00.500' UTC*/,
      dateAdd(
          "year",
          1,
          Timestamp(
              1615683600,
              500'999'999) /*TIMESTAMP '2021-03-14 01:00:00.500' UTC*/));

  // When adjust_timestamp_to_timezone is off, respect Daylight saving in the
  // session time zone
  setQueryTimeZone("America/Los_Angeles");

  EXPECT_EQ(
      Timestamp(1615798800, 0) /*TIMESTAMP '2021-03-15 02:00:00' PST*/,
      dateAdd(
          "millisecond",
          1000 * 60 * 60 * 24,
          Timestamp(1615712400, 0) /*TIMESTAMP '2021-03-14 01:00:00' PST*/));
  EXPECT_EQ(
      Timestamp(1615798800, 0) /*TIMESTAMP '2021-03-15 02:00:00' PST*/,
      dateAdd(
          "second",
          60 * 60 * 24,
          Timestamp(1615712400, 0) /*TIMESTAMP '2021-03-14 01:00:00' PST*/));
  EXPECT_EQ(
      Timestamp(1615798800, 0) /*TIMESTAMP '2021-03-15 02:00:00' PST*/,
      dateAdd(
          "minute",
          60 * 24,
          Timestamp(1615712400, 0) /*TIMESTAMP '2021-03-14 01:00:00' PST*/));
  EXPECT_EQ(
      Timestamp(1615798800, 0) /*TIMESTAMP '2021-03-15 02:00:00' PST*/,
      dateAdd(
          "hour",
          24,
          Timestamp(1615712400, 0) /*TIMESTAMP '2021-03-14 01:00:00' PST*/));
  EXPECT_EQ(
      Timestamp(
          1615795200, 500'999'999) /*TIMESTAMP '2021-03-15 01:00:00.500' PST*/,
      dateAdd(
          "day",
          1,
          Timestamp(
              1615712400,
              500'999'999) /*TIMESTAMP '2021-03-14 01:00:00.500' PST*/));
  EXPECT_EQ(
      Timestamp(
          1618387200, 500'999'999) /*TIMESTAMP '2021-04-14 01:00:00.500' PST*/,
      dateAdd(
          "month",
          1,
          Timestamp(
              1615712400,
              500'999'999) /*TIMESTAMP '2021-03-14 01:00:00.500' PST*/));
  EXPECT_EQ(
      Timestamp(
          1623657600, 500'999'999) /*TIMESTAMP '2021-06-14 01:00:00.500' PST*/,
      dateAdd(
          "quarter",
          1,
          Timestamp(
              1615712400,
              500'999'999) /*TIMESTAMP '2021-03-14 01:00:00.500' PST*/));
  EXPECT_EQ(
      Timestamp(
          1647244800, 500'999'999) /*TIMESTAMP '2022-03-14 01:00:00.500' PST*/,
      dateAdd(
          "year",
          1,
          Timestamp(
              1615712400,
              500'999'999) /*TIMESTAMP '2021-03-14 01:00:00.500' PST*/));

  // Test for coercing to the last day of a year-month
  EXPECT_EQ(
      Timestamp(1582970400, 500'999'999) /*2020-02-29 10:00:00.500*/,
      dateAdd(
          "day",
          365 + 30,
          Timestamp(1548842400, 500'999'999) /*2019-01-30 10:00:00.500*/));
  EXPECT_EQ(
      Timestamp(1582970400, 500'999'999) /*2020-02-29 10:00:00.500*/,
      dateAdd(
          "month",
          12 + 1,
          Timestamp(1548842400, 500'999'999) /*2019-01-30 10:00:00.500*/));
  EXPECT_EQ(
      Timestamp(1582970400, 500'999'999) /*2020-02-29 10:00:00.500*/,
      dateAdd(
          "quarter",
          1,
          Timestamp(1575108000, 500'999'999) /*2019-11-30 10:00:00.500*/));
  EXPECT_EQ(
      Timestamp(1898503200, 500'999'999) /*2030-02-28 10:00:00.500*/,
      dateAdd(
          "year",
          10,
          Timestamp(1582970400, 500'999'999) /*2020-02-29 10:00:00.500*/));

  // Test for negative intervals
  EXPECT_EQ(
      Timestamp(1582934400, 999'999) /*2020-02-29 00:00:00.000*/,
      dateAdd(
          "millisecond",
          -60 * 60 * 24 * 1000 - 500,
          Timestamp(1583020800, 500'999'999) /*2020-03-01 00:00:00.500*/));
  EXPECT_EQ(
      Timestamp(1582934400, 500'999'999) /*2020-02-29 00:00:00.500*/,
      dateAdd(
          "second",
          -60 * 60 * 24,
          Timestamp(1583020800, 500'999'999) /*2020-03-01 00:00:00.500*/));
  EXPECT_EQ(
      Timestamp(1582934400, 500'999'999) /*2020-02-29 00:00:00.500*/,
      dateAdd(
          "minute",
          -60 * 24,
          Timestamp(1583020800, 500'999'999) /*2020-03-01 00:00:00.500*/));
  EXPECT_EQ(
      Timestamp(1582934400, 500'999'999) /*2020-02-29 00:00:00.500*/,
      dateAdd(
          "hour",
          -24,
          Timestamp(1583020800, 500'999'999) /*2020-03-01 00:00:00.500*/));
  EXPECT_EQ(
      Timestamp(1551348000, 500'999'999) /*2019-02-28 10:00:00.500*/,
      dateAdd(
          "day",
          -366,
          Timestamp(1582970400, 500'999'999) /*2020-02-29 10:00:00.500*/));
  EXPECT_EQ(
      Timestamp(1551348000, 500'999'999) /*2019-02-28 10:00:00.500*/,
      dateAdd(
          "month",
          -12,
          Timestamp(1582970400, 500'999'999) /*2020-02-29 10:00:00.500*/));
  EXPECT_EQ(
      Timestamp(1551348000, 500'999'999) /*2019-02-28 10:00:00.500*/,
      dateAdd(
          "quarter",
          -4,
          Timestamp(1582970400, 500'999'999) /*2020-02-29 10:00:00.500*/));
  EXPECT_EQ(
      Timestamp(1519812000, 500'999'999) /*2019-02-28 10:00:00.500*/,
      dateAdd(
          "year",
          -2,
          Timestamp(1582970400, 500'999'999) /*2020-02-29 10:00:00.500*/));
}

TEST_F(DateTimeFunctionsTest, dateAddTimestampWithTimeZone) {
  const auto dateAdd = [&](const std::string& unit,
                           std::optional<int32_t> value,
                           std::optional<int64_t> timestamp,
                           const std::optional<std::string>& timeZoneName) {
    return evaluateWithTimestampWithTimezone(
        fmt::format("date_add('{}', {}, c0)", unit, *value),
        timestamp,
        timeZoneName);
  };

  // 1970-01-01 00:00:00.000 UTC-8
  auto result = dateAdd("day", 5, 0, "-08:00");
  auto expected = makeTimestampWithTimeZoneVector(432000000, "-08:00");
  assertEqualVectors(expected, result);

  // 2023-01-08 00:00:00.000 UTC-8
  result = dateAdd("day", -7, 1673136000, "-08:00");
  expected = makeTimestampWithTimeZoneVector(1068336000, "-08:00");
  assertEqualVectors(expected, result);

  // 2023-01-08 00:00:00.000 UTC-8
  result = dateAdd("millisecond", -7, 1673136000, "-08:00");
  expected = makeTimestampWithTimeZoneVector(1673135993, "-08:00");
  assertEqualVectors(expected, result);

  // 2023-01-08 00:00:00.000 UTC-8
  result = dateAdd("millisecond", +7, 1673136000, "-08:00");
  expected = makeTimestampWithTimeZoneVector(1673136007, "-08:00");
  assertEqualVectors(expected, result);

  const auto evaluateDateAddFromStrings = [&](const std::string& unit,
                                              int32_t value,
                                              const std::string& inputTimestamp,
                                              const std::string&
                                                  expectedTimestamp) {
    assertEqualVectors(
        evaluate<FlatVector<int64_t>>(
            "parse_datetime(c0, 'YYYY-MM-dd+HH:mm:ssZZ')",
            makeRowVector({makeNullableFlatVector<StringView>(
                {StringView{expectedTimestamp}})})),
        evaluate<FlatVector<int64_t>>(
            fmt::format(
                "date_add('{}', {}, parse_datetime(c0, 'YYYY-MM-dd+HH:mm:ssZZ'))",
                unit,
                value),
            makeRowVector({makeNullableFlatVector<StringView>(
                {StringView{inputTimestamp}})})));
  };

  evaluateDateAddFromStrings(
      "second", 3, "1972-05-20+23:01:02+14:00", "1972-05-20+23:01:05+14:00");
  evaluateDateAddFromStrings(
      "minute", 5, "1972-05-20+23:01:02+14:00", "1972-05-20+23:06:02+14:00");
  evaluateDateAddFromStrings(
      "minute", 10, "1968-02-20+23:01:02+14:00", "1968-02-20+23:11:02+14:00");
  evaluateDateAddFromStrings(
      "hour", 5, "1972-05-20+23:01:02+14:00", "1972-05-21+04:01:02+14:00");
  evaluateDateAddFromStrings(
      "hour", 50, "1968-02-20+23:01:02+14:00", "1968-02-23+01:01:02+14:00");
  evaluateDateAddFromStrings(
      "day", 14, "1972-05-20+23:01:02+14:00", "1972-06-03+23:01:02+14:00");
  evaluateDateAddFromStrings(
      "day", 140, "1968-02-20+23:01:02+14:00", "1968-07-09+23:01:02+14:00");
  evaluateDateAddFromStrings(
      "month", 14, "1972-05-20+23:01:02+14:00", "1973-07-20+23:01:02+14:00");
  evaluateDateAddFromStrings(
      "month", 10, "1968-02-20+23:01:02+14:00", "1968-12-20+23:01:02+14:00");
  evaluateDateAddFromStrings(
      "quarter", 3, "1972-05-20+23:01:02+14:00", "1973-02-20+23:01:02+14:00");
  evaluateDateAddFromStrings(
      "quarter", 30, "1968-02-20+23:01:02+14:00", "1975-08-20+23:01:02+14:00");
  evaluateDateAddFromStrings(
      "year", 3, "1972-05-20+23:01:02+14:00", "1975-05-20+23:01:02+14:00");
  evaluateDateAddFromStrings(
      "year", 3, "1968-02-20+23:01:02+14:00", "1971-02-20+23:01:02+14:00");
}

TEST_F(DateTimeFunctionsTest, dateDiffDate) {
  const auto dateDiff = [&](const std::string& unit,
                            std::optional<int32_t> date1,
                            std::optional<int32_t> date2) {
    return evaluateOnce<int64_t, int32_t>(
        fmt::format("date_diff('{}', c0, c1)", unit),
        {date1, date2},
        {DATE(), DATE()});
  };

  // Check null behaviors
  EXPECT_EQ(std::nullopt, dateDiff("day", 1, std::nullopt));
  EXPECT_EQ(std::nullopt, dateDiff("month", std::nullopt, 0));

  // Check invalid units
  VELOX_ASSERT_THROW(
      dateDiff("millisecond", 1, 0), "millisecond is not a valid DATE field");
  VELOX_ASSERT_THROW(
      dateDiff("second", 1, 0), "second is not a valid DATE field");
  VELOX_ASSERT_THROW(
      dateDiff("minute", 1, 0), "minute is not a valid DATE field");
  VELOX_ASSERT_THROW(dateDiff("hour", 1, 0), "hour is not a valid DATE field");
  VELOX_ASSERT_THROW(
      dateDiff("invalid_unit", 1, 0),
      "Unsupported datetime unit: invalid_unit");

  // Simple tests
  EXPECT_EQ(
      1, dateDiff("day", parseDate("2019-02-28"), parseDate("2019-03-01")));
  EXPECT_EQ(
      13, dateDiff("month", parseDate("2019-02-28"), parseDate("2020-03-28")));
  EXPECT_EQ(
      4, dateDiff("quarter", parseDate("2019-02-28"), parseDate("2020-02-28")));
  EXPECT_EQ(
      1, dateDiff("year", parseDate("2019-02-28"), parseDate("2020-02-28")));

  // Verify that units are not case sensitive.
  EXPECT_EQ(
      1, dateDiff("DAY", parseDate("2019-02-28"), parseDate("2019-03-01")));
  EXPECT_EQ(
      1, dateDiff("dAY", parseDate("2019-02-28"), parseDate("2019-03-01")));
  EXPECT_EQ(
      1, dateDiff("Day", parseDate("2019-02-28"), parseDate("2019-03-01")));

  // Account for the last day of a year-month
  EXPECT_EQ(
      395, dateDiff("day", parseDate("2019-01-30"), parseDate("2020-02-29")));
  EXPECT_EQ(
      13, dateDiff("month", parseDate("2019-01-30"), parseDate("2020-02-29")));
  EXPECT_EQ(
      1, dateDiff("quarter", parseDate("2019-11-30"), parseDate("2020-02-29")));
  EXPECT_EQ(
      10, dateDiff("year", parseDate("2020-02-29"), parseDate("2030-02-28")));

  // Check for negative intervals
  EXPECT_EQ(
      -366, dateDiff("day", parseDate("2020-02-29"), parseDate("2019-02-28")));
  EXPECT_EQ(
      -12, dateDiff("month", parseDate("2020-02-29"), parseDate("2019-02-28")));
  EXPECT_EQ(
      -4,
      dateDiff("quarter", parseDate("2020-02-29"), parseDate("2019-02-28")));
  EXPECT_EQ(
      -2, dateDiff("year", parseDate("2020-02-29"), parseDate("2018-02-28")));

  // Check Large date
  EXPECT_EQ(
      737790,
      dateDiff("day", parseDate("2020-02-29"), parseDate("4040-02-29")));
  EXPECT_EQ(
      24240,
      dateDiff("month", parseDate("2020-02-29"), parseDate("4040-02-29")));
  EXPECT_EQ(
      8080,
      dateDiff("quarter", parseDate("2020-02-29"), parseDate("4040-02-29")));
  EXPECT_EQ(
      2020, dateDiff("year", parseDate("2020-02-29"), parseDate("4040-02-29")));
}

TEST_F(DateTimeFunctionsTest, dateDiffTimestamp) {
  const auto dateDiff = [&](const std::string& unit,
                            std::optional<Timestamp> timestamp1,
                            std::optional<Timestamp> timestamp2) {
    return evaluateOnce<int64_t>(
        fmt::format("date_diff('{}', c0, c1)", unit), timestamp1, timestamp2);
  };

  using util::fromTimestampString;

  // Check null behaviors
  EXPECT_EQ(std::nullopt, dateDiff("second", Timestamp(1, 0), std::nullopt));
  EXPECT_EQ(std::nullopt, dateDiff("month", std::nullopt, Timestamp(0, 0)));

  // Check invalid units
  VELOX_ASSERT_THROW(
      dateDiff("invalid_unit", Timestamp(1, 0), Timestamp(0, 0)),
      "Unsupported datetime unit: invalid_unit");
  VELOX_ASSERT_THROW(
      dateDiff("week", Timestamp(1, 0), Timestamp(0, 0)),
      "Unsupported datetime unit: week");

  // Simple tests
  EXPECT_EQ(
      60 * 1000 + 500,
      dateDiff(
          "millisecond",
          fromTimestampString("2019-02-28 10:00:00.500"),
          fromTimestampString("2019-02-28 10:01:01.000")));
  EXPECT_EQ(
      60 * 60 * 24,
      dateDiff(
          "second",
          fromTimestampString("2019-02-28 10:00:00.500"),
          fromTimestampString("2019-03-01 10:00:00.500")));
  EXPECT_EQ(
      60 * 24,
      dateDiff(
          "minute",
          fromTimestampString("2019-02-28 10:00:00.500"),
          fromTimestampString("2019-03-01 10:00:00.500")));
  EXPECT_EQ(
      24,
      dateDiff(
          "hour",
          fromTimestampString("2019-02-28 10:00:00.500"),
          fromTimestampString("2019-03-01 10:00:00.500")));
  EXPECT_EQ(
      1,
      dateDiff(
          "day",
          fromTimestampString("2019-02-28 10:00:00.500"),
          fromTimestampString("2019-03-01 10:00:00.500")));
  EXPECT_EQ(
      12 + 1,
      dateDiff(
          "month",
          fromTimestampString("2019-02-28 10:00:00.500"),
          fromTimestampString("2020-03-28 10:00:00.500")));
  EXPECT_EQ(
      4,
      dateDiff(
          "quarter",
          fromTimestampString("2019-02-28 10:00:00.500"),
          fromTimestampString("2020-02-28 10:00:00.500")));
  EXPECT_EQ(
      1,
      dateDiff(
          "year",
          fromTimestampString("2019-02-28 10:00:00.500"),
          fromTimestampString("2020-02-28 10:00:00.500")));

  // Test for daylight saving. Daylight saving in US starts at 2021-03-14
  // 02:00:00 PST.
  // When adjust_timestamp_to_timezone is off, Daylight saving occurs in UTC
  EXPECT_EQ(
      1000 * 60 * 60 * 24,
      dateDiff(
          "millisecond",
          fromTimestampString("2021-03-14 01:00:00.000"),
          fromTimestampString("2021-03-15 01:00:00.000")));
  EXPECT_EQ(
      60 * 60 * 24,
      dateDiff(
          "second",
          fromTimestampString("2021-03-14 01:00:00.000"),
          fromTimestampString("2021-03-15 01:00:00.000")));
  EXPECT_EQ(
      60 * 24,
      dateDiff(
          "minute",
          fromTimestampString("2021-03-14 01:00:00.000"),
          fromTimestampString("2021-03-15 01:00:00.000")));
  EXPECT_EQ(
      24,
      dateDiff(
          "hour",
          fromTimestampString("2021-03-14 01:00:00.000"),
          fromTimestampString("2021-03-15 01:00:00.000")));
  EXPECT_EQ(
      1,
      dateDiff(
          "day",
          fromTimestampString("2021-03-14 01:00:00.000"),
          fromTimestampString("2021-03-15 01:00:00.000")));
  EXPECT_EQ(
      1,
      dateDiff(
          "month",
          fromTimestampString("2021-03-14 01:00:00.000"),
          fromTimestampString("2021-04-14 01:00:00.000")));
  EXPECT_EQ(
      1,
      dateDiff(
          "quarter",
          fromTimestampString("2021-03-14 01:00:00.000"),
          fromTimestampString("2021-06-14 01:00:00.000")));
  EXPECT_EQ(
      1,
      dateDiff(
          "year",
          fromTimestampString("2021-03-14 01:00:00.000"),
          fromTimestampString("2022-03-14 01:00:00.000")));

  // When adjust_timestamp_to_timezone is on, respect Daylight saving in the
  // session time zone
  setQueryTimeZone("America/Los_Angeles");

  EXPECT_EQ(
      1000 * 60 * 60 * 24,
      dateDiff(
          "millisecond",
          fromTimestampString("2021-03-14 09:00:00.000"),
          fromTimestampString("2021-03-15 09:00:00.000")));
  EXPECT_EQ(
      60 * 60 * 24,
      dateDiff(
          "second",
          fromTimestampString("2021-03-14 09:00:00.000"),
          fromTimestampString("2021-03-15 09:00:00.000")));
  EXPECT_EQ(
      60 * 24,
      dateDiff(
          "minute",
          fromTimestampString("2021-03-14 09:00:00.000"),
          fromTimestampString("2021-03-15 09:00:00.000")));
  EXPECT_EQ(
      24,
      dateDiff(
          "hour",
          fromTimestampString("2021-03-14 09:00:00.000"),
          fromTimestampString("2021-03-15 09:00:00.000")));
  EXPECT_EQ(
      1,
      dateDiff(
          "day",
          fromTimestampString("2021-03-14 09:00:00.000"),
          fromTimestampString("2021-03-15 09:00:00.000")));
  EXPECT_EQ(
      1,
      dateDiff(
          "month",
          fromTimestampString("2021-03-14 09:00:00.000"),
          fromTimestampString("2021-04-14 09:00:00.000")));
  EXPECT_EQ(
      1,
      dateDiff(
          "quarter",
          fromTimestampString("2021-03-14 09:00:00.000"),
          fromTimestampString("2021-06-14 09:00:00.000")));
  EXPECT_EQ(
      1,
      dateDiff(
          "year",
          fromTimestampString("2021-03-14 09:00:00.000"),
          fromTimestampString("2022-03-14 09:00:00.000")));

  // Test for respecting the last day of a year-month
  EXPECT_EQ(
      365 + 30,
      dateDiff(
          "day",
          fromTimestampString("2019-01-30 10:00:00.500"),
          fromTimestampString("2020-02-29 10:00:00.500")));
  EXPECT_EQ(
      12 + 1,
      dateDiff(
          "month",
          fromTimestampString("2019-01-30 10:00:00.500"),
          fromTimestampString("2020-02-29 10:00:00.500")));
  EXPECT_EQ(
      1,
      dateDiff(
          "quarter",
          fromTimestampString("2019-11-30 10:00:00.500"),
          fromTimestampString("2020-02-29 10:00:00.500")));
  EXPECT_EQ(
      10,
      dateDiff(
          "year",
          fromTimestampString("2020-02-29 10:00:00.500"),
          fromTimestampString("2030-02-28 10:00:00.500")));

  // Test for negative difference
  EXPECT_EQ(
      -60 * 60 * 24 * 1000 - 500,
      dateDiff(
          "millisecond",
          fromTimestampString("2020-03-01 00:00:00.500"),
          fromTimestampString("2020-02-29 00:00:00.000")));
  EXPECT_EQ(
      -60 * 60 * 24,
      dateDiff(
          "second",
          fromTimestampString("2020-03-01 00:00:00.500"),
          fromTimestampString("2020-02-29 00:00:00.500")));
  EXPECT_EQ(
      -60 * 24,
      dateDiff(
          "minute",
          fromTimestampString("2020-03-01 00:00:00.500"),
          fromTimestampString("2020-02-29 00:00:00.500")));
  EXPECT_EQ(
      -24,
      dateDiff(
          "hour",
          fromTimestampString("2020-03-01 00:00:00.500"),
          fromTimestampString("2020-02-29 00:00:00.500")));
  EXPECT_EQ(
      -366,
      dateDiff(
          "day",
          fromTimestampString("2020-02-29 10:00:00.500"),
          fromTimestampString("2019-02-28 10:00:00.500")));
  EXPECT_EQ(
      -12,
      dateDiff(
          "month",
          fromTimestampString("2020-02-29 10:00:00.500"),
          fromTimestampString("2019-02-28 10:00:00.500")));
  EXPECT_EQ(
      -4,
      dateDiff(
          "quarter",
          fromTimestampString("2020-02-29 10:00:00.500"),
          fromTimestampString("2019-02-28 10:00:00.500")));
  EXPECT_EQ(
      -2,
      dateDiff(
          "year",
          fromTimestampString("2020-02-29 10:00:00.500"),
          fromTimestampString("2018-02-28 10:00:00.500")));
}

TEST_F(DateTimeFunctionsTest, dateDiffTimestampWithTimezone) {
  const auto dateDiff = [&](const std::string& unit,
                            std::optional<int64_t> timestamp1,
                            const std::optional<std::string>& timeZoneName1,
                            std::optional<int64_t> timestamp2,
                            const std::optional<std::string>& timeZoneName2) {
    auto ts1 = (timestamp1.has_value() && timeZoneName1.has_value())
        ? makeTimestampWithTimeZoneVector(
              timestamp1.value(), timeZoneName1.value().c_str())
        : BaseVector::createNullConstant(TIMESTAMP_WITH_TIME_ZONE(), 1, pool());
    auto ts2 = (timestamp2.has_value() && timeZoneName2.has_value())
        ? makeTimestampWithTimeZoneVector(
              timestamp2.value(), timeZoneName2.value().c_str())
        : BaseVector::createNullConstant(TIMESTAMP_WITH_TIME_ZONE(), 1, pool());

    return evaluateOnce<int64_t>(
        fmt::format("date_diff('{}', c0, c1)", unit),
        makeRowVector({ts1, ts2}));
  };

  // timestamp1: 1970-01-01 00:00:00.000 +00:00 (0)
  // timestamp2: 2020-08-25 16:30:10.123 -08:00 (1'598'373'010'123)
  EXPECT_EQ(
      1598373010123,
      dateDiff(
          "millisecond",
          0,
          "+00:00",
          1'598'373'010'123,
          "America/Los_Angeles"));
  EXPECT_EQ(
      1598373010,
      dateDiff(
          "second", 0, "+00:00", 1'598'373'010'123, "America/Los_Angeles"));
  EXPECT_EQ(
      26639550,
      dateDiff(
          "minute", 0, "+00:00", 1'598'373'010'123, "America/Los_Angeles"));
  EXPECT_EQ(
      443992,
      dateDiff("hour", 0, "+00:00", 1'598'373'010'123, "America/Los_Angeles"));
  EXPECT_EQ(
      18499,
      dateDiff("day", 0, "+00:00", 1'598'373'010'123, "America/Los_Angeles"));
  EXPECT_EQ(
      607,
      dateDiff("month", 0, "+00:00", 1'598'373'010'123, "America/Los_Angeles"));
  EXPECT_EQ(
      202,
      dateDiff(
          "quarter", 0, "+00:00", 1'598'373'010'123, "America/Los_Angeles"));
  EXPECT_EQ(
      50,
      dateDiff("year", 0, "+00:00", 1'598'373'010'123, "America/Los_Angeles"));
}

TEST_F(DateTimeFunctionsTest, parseDatetime) {
  // Check null behavior.
  EXPECT_EQ(std::nullopt, parseDatetime("1970-01-01", std::nullopt));
  EXPECT_EQ(std::nullopt, parseDatetime(std::nullopt, "YYYY-MM-dd"));
  EXPECT_EQ(std::nullopt, parseDatetime(std::nullopt, std::nullopt));

  // Ensure it throws.
  VELOX_ASSERT_THROW(parseDatetime("", ""), "Invalid pattern specification");
  VELOX_ASSERT_THROW(
      parseDatetime("1234", "Y Y"),
      "Invalid format: \"1234\" is malformed at \"\"");

  // Simple tests. More exhaustive tests are provided as part of Joda's
  // implementation.
  EXPECT_EQ(
      TimestampWithTimezone(0, 0), parseDatetime("1970-01-01", "YYYY-MM-dd"));
  EXPECT_EQ(
      TimestampWithTimezone(86400000, 0),
      parseDatetime("1970-01-02", "YYYY-MM-dd"));
  EXPECT_EQ(
      TimestampWithTimezone(86400000, 0),
      parseDatetime("19700102", "YYYYMMdd"));
  EXPECT_EQ(
      TimestampWithTimezone(86400000, 0), parseDatetime("19700102", "YYYYMdd"));
  EXPECT_EQ(
      TimestampWithTimezone(86400000, 0), parseDatetime("19700102", "YYYYMMd"));
  EXPECT_EQ(
      TimestampWithTimezone(86400000, 0), parseDatetime("19700102", "YYYYMd"));
  EXPECT_EQ(
      TimestampWithTimezone(86400000, 0), parseDatetime("19700102", "YYYYMd"));

  // 118860000 is the number of milliseconds since epoch at 1970-01-02
  // 09:01:00.000 UTC.
  EXPECT_EQ(
      TimestampWithTimezone(118860000, util::getTimeZoneID("+00:00")),
      parseDatetime("1970-01-02+09:01+00:00", "YYYY-MM-dd+HH:mmZZ"));
  EXPECT_EQ(
      TimestampWithTimezone(118860000, util::getTimeZoneID("-09:00")),
      parseDatetime("1970-01-02+00:01-09:00", "YYYY-MM-dd+HH:mmZZ"));
  EXPECT_EQ(
      TimestampWithTimezone(118860000, util::getTimeZoneID("-02:00")),
      parseDatetime("1970-01-02+07:01-02:00", "YYYY-MM-dd+HH:mmZZ"));
  EXPECT_EQ(
      TimestampWithTimezone(118860000, util::getTimeZoneID("+14:00")),
      parseDatetime("1970-01-02+23:01+14:00", "YYYY-MM-dd+HH:mmZZ"));
  EXPECT_EQ(
      TimestampWithTimezone(
          198060000, util::getTimeZoneID("America/Los_Angeles")),
      parseDatetime("1970-01-02+23:01 PST", "YYYY-MM-dd+HH:mm z"));
  EXPECT_EQ(
      TimestampWithTimezone(169260000, util::getTimeZoneID("+00:00")),
      parseDatetime("1970-01-02+23:01 GMT", "YYYY-MM-dd+HH:mm z"));

  setQueryTimeZone("Asia/Kolkata");

  // 66600000 is the number of millisecond since epoch at 1970-01-01
  // 18:30:00.000 UTC.
  EXPECT_EQ(
      TimestampWithTimezone(66600000, util::getTimeZoneID("Asia/Kolkata")),
      parseDatetime("1970-01-02+00:00", "YYYY-MM-dd+HH:mm"));
  EXPECT_EQ(
      TimestampWithTimezone(66600000, util::getTimeZoneID("-03:00")),
      parseDatetime("1970-01-01+15:30-03:00", "YYYY-MM-dd+HH:mmZZ"));

  // -66600000 is the number of millisecond since epoch at 1969-12-31
  // 05:30:00.000 UTC.
  EXPECT_EQ(
      TimestampWithTimezone(-66600000, util::getTimeZoneID("Asia/Kolkata")),
      parseDatetime("1969-12-31+11:00", "YYYY-MM-dd+HH:mm"));
  EXPECT_EQ(
      TimestampWithTimezone(-66600000, util::getTimeZoneID("+02:00")),
      parseDatetime("1969-12-31+07:30+02:00", "YYYY-MM-dd+HH:mmZZ"));

  // Joda also lets 'Z' to be UTC|UCT|GMT|GMT0.
  auto ts = TimestampWithTimezone(1708840800000, util::getTimeZoneID("GMT"));
  EXPECT_EQ(
      ts, parseDatetime("2024-02-25+06:00:99 GMT", "yyyy-MM-dd+HH:mm:99 ZZZ"));
  EXPECT_EQ(
      ts, parseDatetime("2024-02-25+06:00:99 GMT0", "yyyy-MM-dd+HH:mm:99 ZZZ"));
  EXPECT_EQ(
      ts, parseDatetime("2024-02-25+06:00:99 UTC", "yyyy-MM-dd+HH:mm:99 ZZZ"));
  EXPECT_EQ(
      ts, parseDatetime("2024-02-25+06:00:99 UTC", "yyyy-MM-dd+HH:mm:99 ZZZ"));

  VELOX_ASSERT_THROW(
      parseDatetime("2024-02-25+06:00:99 PST", "yyyy-MM-dd+HH:mm:99 ZZZ"),
      "Invalid format: \"2024-02-25+06:00:99 PST\" is malformed at \"PST\"");
}

TEST_F(DateTimeFunctionsTest, formatDateTime) {
  using util::fromTimestampString;

  // era test cases - 'G'
  EXPECT_EQ("AD", formatDatetime(fromTimestampString("1970-01-01"), "G"));
  EXPECT_EQ("BC", formatDatetime(fromTimestampString("-100-01-01"), "G"));
  EXPECT_EQ("BC", formatDatetime(fromTimestampString("0-01-01"), "G"));
  EXPECT_EQ("AD", formatDatetime(fromTimestampString("01-01-01"), "G"));
  EXPECT_EQ("AD", formatDatetime(fromTimestampString("01-01-01"), "GGGGGGG"));

  // century of era test cases - 'C'
  EXPECT_EQ("19", formatDatetime(fromTimestampString("1900-01-01"), "C"));
  EXPECT_EQ("19", formatDatetime(fromTimestampString("1955-01-01"), "C"));
  EXPECT_EQ("20", formatDatetime(fromTimestampString("2000-01-01"), "C"));
  EXPECT_EQ("20", formatDatetime(fromTimestampString("2020-01-01"), "C"));
  EXPECT_EQ("0", formatDatetime(fromTimestampString("0-01-01"), "C"));
  EXPECT_EQ("1", formatDatetime(fromTimestampString("-100-01-01"), "C"));
  EXPECT_EQ("19", formatDatetime(fromTimestampString("-1900-01-01"), "C"));
  EXPECT_EQ(
      "000019", formatDatetime(fromTimestampString("1955-01-01"), "CCCCCC"));

  // year of era test cases - 'Y'
  EXPECT_EQ("1970", formatDatetime(fromTimestampString("1970-01-01"), "Y"));
  EXPECT_EQ("2020", formatDatetime(fromTimestampString("2020-01-01"), "Y"));
  EXPECT_EQ("1", formatDatetime(fromTimestampString("0-01-01"), "Y"));
  EXPECT_EQ("101", formatDatetime(fromTimestampString("-100-01-01"), "Y"));
  EXPECT_EQ("70", formatDatetime(fromTimestampString("1970-01-01"), "YY"));
  EXPECT_EQ("70", formatDatetime(fromTimestampString("-1970-01-01"), "YY"));
  EXPECT_EQ("1948", formatDatetime(fromTimestampString("1948-01-01"), "YYY"));
  EXPECT_EQ("1234", formatDatetime(fromTimestampString("1234-01-01"), "YYYY"));
  EXPECT_EQ(
      "0000000001",
      formatDatetime(fromTimestampString("01-01-01"), "YYYYYYYYYY"));

  // day of week number - 'e'
  for (int i = 0; i < 31; i++) {
    std::string date("2022-08-" + std::to_string(i + 1));
    EXPECT_EQ(
        std::to_string(i % 7 + 1),
        formatDatetime(fromTimestampString(StringView{date}), "e"));
  }
  EXPECT_EQ(
      "000001", formatDatetime(fromTimestampString("2022-08-01"), "eeeeee"));

  // day of week text - 'E'
  for (int i = 0; i < 31; i++) {
    std::string date("2022-08-" + std::to_string(i + 1));
    EXPECT_EQ(
        daysShort[i % 7],
        formatDatetime(fromTimestampString(StringView{date}), "E"));
    EXPECT_EQ(
        daysShort[i % 7],
        formatDatetime(fromTimestampString(StringView{date}), "EE"));
    EXPECT_EQ(
        daysShort[i % 7],
        formatDatetime(fromTimestampString(StringView{date}), "EEE"));
    EXPECT_EQ(
        daysLong[i % 7],
        formatDatetime(fromTimestampString(StringView{date}), "EEEE"));
    EXPECT_EQ(
        daysLong[i % 7],
        formatDatetime(fromTimestampString(StringView{date}), "EEEEEEEE"));
  }

  // year test cases - 'y'
  EXPECT_EQ("2022", formatDatetime(fromTimestampString("2022-06-20"), "y"));
  EXPECT_EQ("22", formatDatetime(fromTimestampString("2022-06-20"), "yy"));
  EXPECT_EQ("2022", formatDatetime(fromTimestampString("2022-06-20"), "yyy"));
  EXPECT_EQ("2022", formatDatetime(fromTimestampString("2022-06-20"), "yyyy"));

  EXPECT_EQ("10", formatDatetime(fromTimestampString("10-06-20"), "y"));
  EXPECT_EQ("10", formatDatetime(fromTimestampString("10-06-20"), "yy"));
  EXPECT_EQ("010", formatDatetime(fromTimestampString("10-06-20"), "yyy"));
  EXPECT_EQ("0010", formatDatetime(fromTimestampString("10-06-20"), "yyyy"));

  EXPECT_EQ("-16", formatDatetime(fromTimestampString("-16-06-20"), "y"));
  EXPECT_EQ("16", formatDatetime(fromTimestampString("-16-06-20"), "yy"));
  EXPECT_EQ("-016", formatDatetime(fromTimestampString("-16-06-20"), "yyy"));
  EXPECT_EQ("-0016", formatDatetime(fromTimestampString("-16-06-20"), "yyyy"));

  EXPECT_EQ("00", formatDatetime(fromTimestampString("-1600-06-20"), "yy"));
  EXPECT_EQ("01", formatDatetime(fromTimestampString("-1601-06-20"), "yy"));
  EXPECT_EQ("10", formatDatetime(fromTimestampString("-1610-06-20"), "yy"));

  // day of year test cases - 'D'
  EXPECT_EQ("1", formatDatetime(fromTimestampString("2022-01-01"), "D"));
  EXPECT_EQ("10", formatDatetime(fromTimestampString("2022-01-10"), "D"));
  EXPECT_EQ("100", formatDatetime(fromTimestampString("2022-04-10"), "D"));
  EXPECT_EQ("365", formatDatetime(fromTimestampString("2022-12-31"), "D"));
  EXPECT_EQ(
      "00100", formatDatetime(fromTimestampString("2022-04-10"), "DDDDD"));

  // leap year case
  EXPECT_EQ("60", formatDatetime(fromTimestampString("2020-02-29"), "D"));
  EXPECT_EQ("366", formatDatetime(fromTimestampString("2020-12-31"), "D"));

  // month of year test cases - 'M'
  for (int i = 0; i < 12; i++) {
    auto month = i + 1;
    std::string date("2022-" + std::to_string(month) + "-01");
    EXPECT_EQ(
        std::to_string(month),
        formatDatetime(fromTimestampString(StringView{date}), "M"));
    EXPECT_EQ(
        padNumber(month),
        formatDatetime(fromTimestampString(StringView{date}), "MM"));
    EXPECT_EQ(
        monthsShort[i],
        formatDatetime(fromTimestampString(StringView{date}), "MMM"));
    EXPECT_EQ(
        monthsLong[i],
        formatDatetime(fromTimestampString(StringView{date}), "MMMM"));
    EXPECT_EQ(
        monthsLong[i],
        formatDatetime(fromTimestampString(StringView{date}), "MMMMMMMM"));
  }

  // day of month test cases - 'd'
  EXPECT_EQ("1", formatDatetime(fromTimestampString("2022-01-01"), "d"));
  EXPECT_EQ("10", formatDatetime(fromTimestampString("2022-01-10"), "d"));
  EXPECT_EQ("28", formatDatetime(fromTimestampString("2022-01-28"), "d"));
  EXPECT_EQ("31", formatDatetime(fromTimestampString("2022-01-31"), "d"));
  EXPECT_EQ(
      "00000031",
      formatDatetime(fromTimestampString("2022-01-31"), "dddddddd"));

  // leap year case
  EXPECT_EQ("29", formatDatetime(fromTimestampString("2020-02-29"), "d"));

  // halfday of day test cases - 'a'
  EXPECT_EQ(
      "AM", formatDatetime(fromTimestampString("2022-01-01 00:00:00"), "a"));
  EXPECT_EQ(
      "AM", formatDatetime(fromTimestampString("2022-01-01 11:59:59"), "a"));
  EXPECT_EQ(
      "PM", formatDatetime(fromTimestampString("2022-01-01 12:00:00"), "a"));
  EXPECT_EQ(
      "PM", formatDatetime(fromTimestampString("2022-01-01 23:59:59"), "a"));
  EXPECT_EQ(
      "AM",
      formatDatetime(fromTimestampString("2022-01-01 00:00:00"), "aaaaaaaa"));
  EXPECT_EQ(
      "PM",
      formatDatetime(fromTimestampString("2022-01-01 12:00:00"), "aaaaaaaa"));

  // hour of halfday test cases - 'K'
  for (int i = 0; i < 24; i++) {
    std::string buildString = "2022-01-01 " + padNumber(i) + ":00:00";
    StringView date(buildString);
    EXPECT_EQ(
        std::to_string(i % 12), formatDatetime(fromTimestampString(date), "K"));
  }
  EXPECT_EQ(
      "00000011",
      formatDatetime(fromTimestampString("2022-01-01 11:00:00"), "KKKKKKKK"));

  // clockhour of halfday test cases - 'h'
  for (int i = 0; i < 24; i++) {
    std::string buildString = "2022-01-01 " + padNumber(i) + ":00:00";
    StringView date(buildString);
    EXPECT_EQ(
        std::to_string((i + 11) % 12 + 1),
        formatDatetime(fromTimestampString(date), "h"));
  }
  EXPECT_EQ(
      "00000011",
      formatDatetime(fromTimestampString("2022-01-01 11:00:00"), "hhhhhhhh"));

  // hour of day test cases - 'H'
  for (int i = 0; i < 24; i++) {
    std::string buildString = "2022-01-01 " + padNumber(i) + ":00:00";
    StringView date(buildString);
    EXPECT_EQ(
        std::to_string(i), formatDatetime(fromTimestampString(date), "H"));
  }
  EXPECT_EQ(
      "00000011",
      formatDatetime(fromTimestampString("2022-01-01 11:00:00"), "HHHHHHHH"));

  // clockhour of day test cases - 'k'
  for (int i = 0; i < 24; i++) {
    std::string buildString = "2022-01-01 " + padNumber(i) + ":00:00";
    StringView date(buildString);
    EXPECT_EQ(
        std::to_string((i + 23) % 24 + 1),
        formatDatetime(fromTimestampString(date), "k"));
  }
  EXPECT_EQ(
      "00000011",
      formatDatetime(fromTimestampString("2022-01-01 11:00:00"), "kkkkkkkk"));

  // minute of hour test cases - 'm'
  EXPECT_EQ(
      "0", formatDatetime(fromTimestampString("2022-01-01 00:00:00"), "m"));
  EXPECT_EQ(
      "1", formatDatetime(fromTimestampString("2022-01-01 01:01:00"), "m"));
  EXPECT_EQ(
      "10", formatDatetime(fromTimestampString("2022-01-01 02:10:00"), "m"));
  EXPECT_EQ(
      "30", formatDatetime(fromTimestampString("2022-01-01 03:30:00"), "m"));
  EXPECT_EQ(
      "59", formatDatetime(fromTimestampString("2022-01-01 04:59:00"), "m"));
  EXPECT_EQ(
      "00000042",
      formatDatetime(fromTimestampString("2022-01-01 00:42:42"), "mmmmmmmm"));

  // second of minute test cases - 's'
  EXPECT_EQ(
      "0", formatDatetime(fromTimestampString("2022-01-01 00:00:00"), "s"));
  EXPECT_EQ(
      "1", formatDatetime(fromTimestampString("2022-01-01 01:01:01"), "s"));
  EXPECT_EQ(
      "10", formatDatetime(fromTimestampString("2022-01-01 02:10:10"), "s"));
  EXPECT_EQ(
      "30", formatDatetime(fromTimestampString("2022-01-01 03:30:30"), "s"));
  EXPECT_EQ(
      "59", formatDatetime(fromTimestampString("2022-01-01 04:59:59"), "s"));
  EXPECT_EQ(
      "00000042",
      formatDatetime(fromTimestampString("2022-01-01 00:42:42"), "ssssssss"));

  // fraction of second test cases - 'S'
  EXPECT_EQ(
      "0", formatDatetime(fromTimestampString("2022-01-01 00:00:00.0"), "S"));
  EXPECT_EQ(
      "1", formatDatetime(fromTimestampString("2022-01-01 00:00:00.1"), "S"));
  EXPECT_EQ(
      "1", formatDatetime(fromTimestampString("2022-01-01 01:01:01.11"), "S"));
  EXPECT_EQ(
      "11",
      formatDatetime(fromTimestampString("2022-01-01 02:10:10.11"), "SS"));
  EXPECT_EQ(
      "9", formatDatetime(fromTimestampString("2022-01-01 03:30:30.999"), "S"));
  EXPECT_EQ(
      "99",
      formatDatetime(fromTimestampString("2022-01-01 03:30:30.999"), "SS"));
  EXPECT_EQ(
      "999",
      formatDatetime(fromTimestampString("2022-01-01 03:30:30.999"), "SSS"));
  EXPECT_EQ(
      "12300000",
      formatDatetime(
          fromTimestampString("2022-01-01 03:30:30.123"), "SSSSSSSS"));
  EXPECT_EQ(
      "0990",
      formatDatetime(fromTimestampString("2022-01-01 03:30:30.099"), "SSSS"));
  EXPECT_EQ(
      "0010",
      formatDatetime(fromTimestampString("2022-01-01 03:30:30.001"), "SSSS"));

  // time zone test cases - 'z'
  setQueryTimeZone("Asia/Kolkata");
  EXPECT_EQ(
      "Asia/Kolkata",
      formatDatetime(fromTimestampString("1970-01-01"), "zzzz"));

  // literal test cases
  EXPECT_EQ(
      "hello", formatDatetime(fromTimestampString("1970-01-01"), "'hello'"));
  EXPECT_EQ("'", formatDatetime(fromTimestampString("1970-01-01"), "''"));
  EXPECT_EQ(
      "1970 ' 1970",
      formatDatetime(fromTimestampString("1970-01-01"), "y '' y"));
  EXPECT_EQ(
      "he'llo", formatDatetime(fromTimestampString("1970-01-01"), "'he''llo'"));
  EXPECT_EQ(
      "'he'llo'",
      formatDatetime(fromTimestampString("1970-01-01"), "'''he''llo'''"));
  EXPECT_EQ(
      "1234567890",
      formatDatetime(fromTimestampString("1970-01-01"), "1234567890"));
  EXPECT_EQ(
      "\\\"!@#$%^&*()-+[]{}||`~<>.,?/;:1234567890",
      formatDatetime(
          fromTimestampString("1970-01-01"),
          "\\\"!@#$%^&*()-+[]{}||`~<>.,?/;:1234567890"));

  // Multi-specifier and literal formats
  EXPECT_EQ(
      "AD 19 1970 4 Thu 1970 1 1 1 AM 8 8 8 8 3 11 5 Asia/Kolkata",
      formatDatetime(
          fromTimestampString("1970-01-01 02:33:11.5"),
          "G C Y e E y D M d a K h H k m s S zzzz"));
  EXPECT_EQ(
      "AD 19 1970 4 asdfghjklzxcvbnmqwertyuiop Thu ' 1970 1 1 1 AM 8 8 8 8 3 11 5 1234567890\\\"!@#$%^&*()-+`~{}[];:,./ Asia/Kolkata",
      formatDatetime(
          fromTimestampString("1970-01-01 02:33:11.5"),
          "G C Y e 'asdfghjklzxcvbnmqwertyuiop' E '' y D M d a K h H k m s S 1234567890\\\"!@#$%^&*()-+`~{}[];:,./ zzzz"));

  // User format errors or unsupported errors
  EXPECT_THROW(
      formatDatetime(fromTimestampString("1970-01-01"), "x"), VeloxUserError);
  EXPECT_THROW(
      formatDatetime(fromTimestampString("1970-01-01"), "w"), VeloxUserError);
  EXPECT_THROW(
      formatDatetime(fromTimestampString("1970-01-01"), "z"), VeloxUserError);
  EXPECT_THROW(
      formatDatetime(fromTimestampString("1970-01-01"), "zz"), VeloxUserError);
  EXPECT_THROW(
      formatDatetime(fromTimestampString("1970-01-01"), "zzz"), VeloxUserError);
  EXPECT_THROW(
      formatDatetime(fromTimestampString("1970-01-01"), "q"), VeloxUserError);
  EXPECT_THROW(
      formatDatetime(fromTimestampString("1970-01-01"), "'abcd"),
      VeloxUserError);
}

TEST_F(DateTimeFunctionsTest, formatDateTimeTimezone) {
  using util::fromTimestampString;
  auto zeroTs = fromTimestampString("1970-01-01");

  // No timezone set; default to GMT.
  EXPECT_EQ(
      "1970-01-01 00:00:00", formatDatetime(zeroTs, "YYYY-MM-dd HH:mm:ss"));

  // Check that string is adjusted to the timezone set.
  EXPECT_EQ(
      "1970-01-01 05:30:00",
      formatDatetimeWithTimezone(
          zeroTs, "Asia/Kolkata", "YYYY-MM-dd HH:mm:ss"));

  EXPECT_EQ(
      "1969-12-31 16:00:00",
      formatDatetimeWithTimezone(
          zeroTs, "America/Los_Angeles", "YYYY-MM-dd HH:mm:ss"));
}

TEST_F(DateTimeFunctionsTest, dateFormat) {
  const auto dateFormatOnce = [&](std::optional<Timestamp> timestamp,
                                  const std::string& formatString) {
    return evaluateOnce<std::string>(
        fmt::format("date_format(c0, '{}')", formatString), timestamp);
  };
  using util::fromTimestampString;

  // Check null behaviors
  EXPECT_EQ(std::nullopt, dateFormatOnce(std::nullopt, "%Y"));

  // Normal cases
  EXPECT_EQ(
      "1970-01-01", dateFormat(fromTimestampString("1970-01-01"), "%Y-%m-%d"));
  EXPECT_EQ(
      "2000-02-29 12:00:00 AM",
      dateFormat(
          fromTimestampString("2000-02-29 00:00:00.987"), "%Y-%m-%d %r"));
  EXPECT_EQ(
      "2000-02-29 00:00:00.987000",
      dateFormat(
          fromTimestampString("2000-02-29 00:00:00.987"),
          "%Y-%m-%d %H:%i:%s.%f"));
  EXPECT_EQ(
      "-2000-02-29 00:00:00.987000",
      dateFormat(
          fromTimestampString("-2000-02-29 00:00:00.987"),
          "%Y-%m-%d %H:%i:%s.%f"));

  // Varying digit year cases
  EXPECT_EQ("06", dateFormat(fromTimestampString("-6-06-20"), "%y"));
  EXPECT_EQ("-0006", dateFormat(fromTimestampString("-6-06-20"), "%Y"));
  EXPECT_EQ("16", dateFormat(fromTimestampString("-16-06-20"), "%y"));
  EXPECT_EQ("-0016", dateFormat(fromTimestampString("-16-06-20"), "%Y"));
  EXPECT_EQ("66", dateFormat(fromTimestampString("-166-06-20"), "%y"));
  EXPECT_EQ("-0166", dateFormat(fromTimestampString("-166-06-20"), "%Y"));
  EXPECT_EQ("66", dateFormat(fromTimestampString("-1666-06-20"), "%y"));
  EXPECT_EQ("00", dateFormat(fromTimestampString("-1900-06-20"), "%y"));
  EXPECT_EQ("01", dateFormat(fromTimestampString("-1901-06-20"), "%y"));
  EXPECT_EQ("10", dateFormat(fromTimestampString("-1910-06-20"), "%y"));
  EXPECT_EQ("12", dateFormat(fromTimestampString("-12-06-20"), "%y"));
  EXPECT_EQ("00", dateFormat(fromTimestampString("1900-06-20"), "%y"));
  EXPECT_EQ("01", dateFormat(fromTimestampString("1901-06-20"), "%y"));
  EXPECT_EQ("10", dateFormat(fromTimestampString("1910-06-20"), "%y"));

  // Day of week cases
  for (int i = 0; i < 8; i++) {
    std::string date("1996-01-0" + std::to_string(i + 1));
    // Full length name
    EXPECT_EQ(
        daysLong[i % 7],
        dateFormat(fromTimestampString(StringView{date}), "%W"));
    // Abbreviated name
    EXPECT_EQ(
        daysShort[i % 7],
        dateFormat(fromTimestampString(StringView{date}), "%a"));
  }

  // Month cases
  for (int i = 0; i < 12; i++) {
    std::string date("1996-" + std::to_string(i + 1) + "-01");
    std::string monthNum = std::to_string(i + 1);
    // Full length name
    EXPECT_EQ(
        monthsLong[i % 12],
        dateFormat(fromTimestampString(StringView{date}), "%M"));
    // Abbreviated name
    EXPECT_EQ(
        monthsShort[i % 12],
        dateFormat(fromTimestampString(StringView{date}), "%b"));
    // Numeric
    EXPECT_EQ(
        monthNum, dateFormat(fromTimestampString(StringView{date}), "%c"));
    // Numeric 0-padded
    if (i + 1 < 10) {
      EXPECT_EQ(
          "0" + monthNum,
          dateFormat(fromTimestampString(StringView{date}), "%m"));
    } else {
      EXPECT_EQ(
          monthNum, dateFormat(fromTimestampString(StringView{date}), "%m"));
    }
  }

  // Day of month cases
  for (int i = 1; i <= 31; i++) {
    std::string dayOfMonth = std::to_string(i);
    std::string date("1970-01-" + dayOfMonth);
    EXPECT_EQ(
        dayOfMonth,
        dateFormat(util::fromTimestampString(StringView{date}), "%e"));
    if (i < 10) {
      EXPECT_EQ(
          "0" + dayOfMonth,
          dateFormat(util::fromTimestampString(StringView{date}), "%d"));
    } else {
      EXPECT_EQ(
          dayOfMonth,
          dateFormat(util::fromTimestampString(StringView{date}), "%d"));
    }
  }

  // Fraction of second cases
  EXPECT_EQ(
      "000000", dateFormat(fromTimestampString("2022-01-01 00:00:00.0"), "%f"));
  EXPECT_EQ(
      "100000", dateFormat(fromTimestampString("2022-01-01 00:00:00.1"), "%f"));
  EXPECT_EQ(
      "110000",
      dateFormat(fromTimestampString("2022-01-01 01:01:01.11"), "%f"));
  EXPECT_EQ(
      "110000",
      dateFormat(fromTimestampString("2022-01-01 02:10:10.11"), "%f"));
  EXPECT_EQ(
      "999000",
      dateFormat(fromTimestampString("2022-01-01 03:30:30.999"), "%f"));
  EXPECT_EQ(
      "999000",
      dateFormat(fromTimestampString("2022-01-01 03:30:30.999"), "%f"));
  EXPECT_EQ(
      "999000",
      dateFormat(fromTimestampString("2022-01-01 03:30:30.999"), "%f"));
  EXPECT_EQ(
      "123000",
      dateFormat(fromTimestampString("2022-01-01 03:30:30.123"), "%f"));
  EXPECT_EQ(
      "099000",
      dateFormat(fromTimestampString("2022-01-01 03:30:30.099"), "%f"));
  EXPECT_EQ(
      "001000",
      dateFormat(fromTimestampString("2022-01-01 03:30:30.001234"), "%f"));

  // Hour cases
  for (int i = 0; i < 24; i++) {
    std::string hour = std::to_string(i);
    int clockHour = (i + 11) % 12 + 1;
    std::string clockHourString = std::to_string(clockHour);
    std::string toBuild = "1996-01-01 " + hour + ":00:00";
    StringView date(toBuild);
    EXPECT_EQ(hour, dateFormat(util::fromTimestampString(date), "%k"));
    if (i < 10) {
      EXPECT_EQ("0" + hour, dateFormat(util::fromTimestampString(date), "%H"));
    } else {
      EXPECT_EQ(hour, dateFormat(util::fromTimestampString(date), "%H"));
    }

    EXPECT_EQ(
        clockHourString, dateFormat(util::fromTimestampString(date), "%l"));
    if (clockHour < 10) {
      EXPECT_EQ(
          "0" + clockHourString,
          dateFormat(util::fromTimestampString(date), "%h"));
      EXPECT_EQ(
          "0" + clockHourString,
          dateFormat(util::fromTimestampString(date), "%I"));
    } else {
      EXPECT_EQ(
          clockHourString, dateFormat(util::fromTimestampString(date), "%h"));
      EXPECT_EQ(
          clockHourString, dateFormat(util::fromTimestampString(date), "%I"));
    }
  }

  // Minute cases
  for (int i = 0; i < 60; i++) {
    std::string minute = std::to_string(i);
    std::string toBuild = "1996-01-01 00:" + minute + ":00";
    StringView date(toBuild);
    if (i < 10) {
      EXPECT_EQ("0" + minute, dateFormat(fromTimestampString(date), "%i"));
    } else {
      EXPECT_EQ(minute, dateFormat(fromTimestampString(date), "%i"));
    }
  }

  // Second cases
  for (int i = 0; i < 60; i++) {
    std::string second = std::to_string(i);
    std::string toBuild = "1996-01-01 00:00:" + second;
    StringView date(toBuild);
    if (i < 10) {
      EXPECT_EQ("0" + second, dateFormat(fromTimestampString(date), "%S"));
      EXPECT_EQ("0" + second, dateFormat(fromTimestampString(date), "%s"));
    } else {
      EXPECT_EQ(second, dateFormat(fromTimestampString(date), "%S"));
      EXPECT_EQ(second, dateFormat(fromTimestampString(date), "%s"));
    }
  }

  // Day of year cases
  EXPECT_EQ("001", dateFormat(fromTimestampString("2022-01-01"), "%j"));
  EXPECT_EQ("010", dateFormat(fromTimestampString("2022-01-10"), "%j"));
  EXPECT_EQ("100", dateFormat(fromTimestampString("2022-04-10"), "%j"));
  EXPECT_EQ("365", dateFormat(fromTimestampString("2022-12-31"), "%j"));

  // Halfday of day cases
  EXPECT_EQ("AM", dateFormat(fromTimestampString("2022-01-01 00:00:00"), "%p"));
  EXPECT_EQ("AM", dateFormat(fromTimestampString("2022-01-01 11:59:59"), "%p"));
  EXPECT_EQ("PM", dateFormat(fromTimestampString("2022-01-01 12:00:00"), "%p"));
  EXPECT_EQ("PM", dateFormat(fromTimestampString("2022-01-01 23:59:59"), "%p"));

  // 12-hour time cases
  EXPECT_EQ(
      "12:00:00 AM",
      dateFormat(fromTimestampString("2022-01-01 00:00:00"), "%r"));
  EXPECT_EQ(
      "11:59:59 AM",
      dateFormat(fromTimestampString("2022-01-01 11:59:59"), "%r"));
  EXPECT_EQ(
      "12:00:00 PM",
      dateFormat(fromTimestampString("2022-01-01 12:00:00"), "%r"));
  EXPECT_EQ(
      "11:59:59 PM",
      dateFormat(fromTimestampString("2022-01-01 23:59:59"), "%r"));

  // 24-hour time cases
  EXPECT_EQ(
      "00:00:00", dateFormat(fromTimestampString("2022-01-01 00:00:00"), "%T"));
  EXPECT_EQ(
      "11:59:59", dateFormat(fromTimestampString("2022-01-01 11:59:59"), "%T"));
  EXPECT_EQ(
      "12:00:00", dateFormat(fromTimestampString("2022-01-01 12:00:00"), "%T"));
  EXPECT_EQ(
      "23:59:59", dateFormat(fromTimestampString("2022-01-01 23:59:59"), "%T"));

  // Percent followed by non-existent specifier case
  EXPECT_EQ("q", dateFormat(fromTimestampString("1970-01-01"), "%q"));
  EXPECT_EQ("z", dateFormat(fromTimestampString("1970-01-01"), "%z"));
  EXPECT_EQ("g", dateFormat(fromTimestampString("1970-01-01"), "%g"));

  // With timezone. Indian Standard Time (IST) UTC+5:30.
  setQueryTimeZone("Asia/Kolkata");

  EXPECT_EQ(
      "1970-01-01", dateFormat(fromTimestampString("1970-01-01"), "%Y-%m-%d"));
  EXPECT_EQ(
      "2000-02-29 05:30:00 AM",
      dateFormat(
          fromTimestampString("2000-02-29 00:00:00.987"), "%Y-%m-%d %r"));
  EXPECT_EQ(
      "2000-02-29 05:30:00.987000",
      dateFormat(
          fromTimestampString("2000-02-29 00:00:00.987"),
          "%Y-%m-%d %H:%i:%s.%f"));
  EXPECT_EQ(
      "-2000-02-29 05:53:28.987000",
      dateFormat(
          fromTimestampString("-2000-02-29 00:00:00.987"),
          "%Y-%m-%d %H:%i:%s.%f"));

  // Same timestamps with a different timezone. Pacific Daylight Time (North
  // America) PDT UTC-8:00.
  setQueryTimeZone("America/Los_Angeles");

  EXPECT_EQ(
      "1969-12-31", dateFormat(fromTimestampString("1970-01-01"), "%Y-%m-%d"));
  EXPECT_EQ(
      "2000-02-28 04:00:00 PM",
      dateFormat(
          fromTimestampString("2000-02-29 00:00:00.987"), "%Y-%m-%d %r"));
  EXPECT_EQ(
      "2000-02-28 16:00:00.987000",
      dateFormat(
          fromTimestampString("2000-02-29 00:00:00.987"),
          "%Y-%m-%d %H:%i:%s.%f"));
  EXPECT_EQ(
      "-2000-02-28 16:07:02.987000",
      dateFormat(
          fromTimestampString("-2000-02-29 00:00:00.987"),
          "%Y-%m-%d %H:%i:%s.%f"));

  // User format errors or unsupported errors
  EXPECT_THROW(
      dateFormat(fromTimestampString("-2000-02-29 00:00:00.987"), "%D"),
      VeloxUserError);
  EXPECT_THROW(
      dateFormat(fromTimestampString("-2000-02-29 00:00:00.987"), "%U"),
      VeloxUserError);
  EXPECT_THROW(
      dateFormat(fromTimestampString("-2000-02-29 00:00:00.987"), "%u"),
      VeloxUserError);
  EXPECT_THROW(
      dateFormat(fromTimestampString("-2000-02-29 00:00:00.987"), "%V"),
      VeloxUserError);
  EXPECT_THROW(
      dateFormat(fromTimestampString("-2000-02-29 00:00:00.987"), "%w"),
      VeloxUserError);
  EXPECT_THROW(
      dateFormat(fromTimestampString("-2000-02-29 00:00:00.987"), "%X"),
      VeloxUserError);
  EXPECT_THROW(
      dateFormat(fromTimestampString("-2000-02-29 00:00:00.987"), "%v"),
      VeloxUserError);
  EXPECT_THROW(
      dateFormat(fromTimestampString("-2000-02-29 00:00:00.987"), "%x"),
      VeloxUserError);
}

TEST_F(DateTimeFunctionsTest, dateFormatTimestampWithTimezone) {
  const auto testDateFormat =
      [&](const std::string& formatString,
          std::optional<int64_t> timestamp,
          const std::optional<std::string>& timeZoneName) {
        return evaluateWithTimestampWithTimezone<std::string>(
            fmt::format("date_format(c0, '{}')", formatString),
            timestamp,
            timeZoneName);
      };

  EXPECT_EQ(
      "1969-12-31 11:00:00 PM", testDateFormat("%Y-%m-%d %r", 0, "-01:00"));
  EXPECT_EQ(
      "1973-11-30 12:33:09 AM",
      testDateFormat("%Y-%m-%d %r", 123456789000, "+03:00"));
  EXPECT_EQ(
      "1966-02-01 12:26:51 PM",
      testDateFormat("%Y-%m-%d %r", -123456789000, "-14:00"));
  EXPECT_EQ(
      "2001-04-19 18:25:21.000000",
      testDateFormat("%Y-%m-%d %H:%i:%s.%f", 987654321000, "+14:00"));
  EXPECT_EQ(
      "1938-09-14 23:34:39.000000",
      testDateFormat("%Y-%m-%d %H:%i:%s.%f", -987654321000, "+04:00"));
  EXPECT_EQ(
      "70-August-22 17:55:15 PM",
      testDateFormat("%y-%M-%e %T %p", 20220915000, "-07:00"));
  EXPECT_EQ(
      "69-May-11 20:04:45 PM",
      testDateFormat("%y-%M-%e %T %p", -20220915000, "-03:00"));
}

TEST_F(DateTimeFunctionsTest, fromIso8601Date) {
  const auto fromIso = [&](const std::string& input) {
    return evaluateOnce<int32_t, std::string>("from_iso8601_date(c0)", input);
  };

  EXPECT_EQ(0, fromIso("1970-01-01"));
  EXPECT_EQ(9, fromIso("1970-01-10"));
  EXPECT_EQ(-1, fromIso("1969-12-31"));

  VELOX_ASSERT_THROW(fromIso("2024-01-xx"), "Unable to parse date value");
}

TEST_F(DateTimeFunctionsTest, dateParse) {
  // Check null behavior.
  EXPECT_EQ(std::nullopt, dateParse("1970-01-01", std::nullopt));
  EXPECT_EQ(std::nullopt, dateParse(std::nullopt, "YYYY-MM-dd"));
  EXPECT_EQ(std::nullopt, dateParse(std::nullopt, std::nullopt));

  // Simple tests. More exhaustive tests are provided in DateTimeFormatterTest.
  EXPECT_EQ(Timestamp(86400, 0), dateParse("1970-01-02", "%Y-%m-%d"));
  EXPECT_EQ(Timestamp(0, 0), dateParse("1970-01-01", "%Y-%m-%d"));
  EXPECT_EQ(Timestamp(86400, 0), dateParse("19700102", "%Y%m%d"));

  // Tests for differing query timezones
  // 118860000 is the number of milliseconds since epoch at 1970-01-02
  // 09:01:00.000 UTC.
  EXPECT_EQ(
      Timestamp(118860, 0), dateParse("1970-01-02+09:01", "%Y-%m-%d+%H:%i"));

  setQueryTimeZone("America/Los_Angeles");
  EXPECT_EQ(
      Timestamp(118860, 0), dateParse("1970-01-02+01:01", "%Y-%m-%d+%H:%i"));

  setQueryTimeZone("America/Noronha");
  EXPECT_EQ(
      Timestamp(118860, 0), dateParse("1970-01-02+07:01", "%Y-%m-%d+%H:%i"));

  setQueryTimeZone("+04:00");
  EXPECT_EQ(
      Timestamp(118860, 0), dateParse("1970-01-02+13:01", "%Y-%m-%d+%H:%i"));

  setQueryTimeZone("Asia/Kolkata");
  // 66600000 is the number of millisecond since epoch at 1970-01-01
  // 18:30:00.000 UTC.
  EXPECT_EQ(
      Timestamp(66600, 0), dateParse("1970-01-02+00:00", "%Y-%m-%d+%H:%i"));

  // -66600000 is the number of millisecond since epoch at 1969-12-31
  // 05:30:00.000 UTC.
  EXPECT_EQ(
      Timestamp(-66600, 0), dateParse("1969-12-31+11:00", "%Y-%m-%d+%H:%i"));

  VELOX_ASSERT_THROW(
      dateParse("", "%y+"), "Invalid format: \"\" is malformed at \"\"");
  VELOX_ASSERT_THROW(
      dateParse("1", "%y+"), "Invalid format: \"1\" is malformed at \"1\"");
  VELOX_ASSERT_THROW(
      dateParse("116", "%y+"), "Invalid format: \"116\" is malformed at \"6\"");
}

TEST_F(DateTimeFunctionsTest, dateFunctionVarchar) {
  const auto dateFunction = [&](const std::optional<std::string>& dateString) {
    return evaluateOnce<int32_t>("date(c0)", dateString);
  };

  // Date(0) is 1970-01-01.
  EXPECT_EQ(0, dateFunction("1970-01-01"));
  // Date(18297) is 2020-02-05.
  EXPECT_EQ(18297, dateFunction("2020-02-05"));
  // Date(-18297) is 1919-11-28.
  EXPECT_EQ(-18297, dateFunction("1919-11-28"));

  // Illegal date format.
  VELOX_ASSERT_THROW(
      dateFunction("2020-02-05 11:00"),
      "Unable to parse date value: \"2020-02-05 11:00\", expected format is (YYYY-MM-DD)");
}

TEST_F(DateTimeFunctionsTest, dateFunctionTimestamp) {
  static const int64_t kSecondsInDay = 86'400;
  static const uint64_t kNanosInSecond = 1'000'000'000;

  const auto dateFunction = [&](std::optional<Timestamp> timestamp) {
    return evaluateOnce<int32_t>("date(c0)", timestamp);
  };

  EXPECT_EQ(0, dateFunction(Timestamp()));
  EXPECT_EQ(1, dateFunction(Timestamp(kSecondsInDay, 0)));
  EXPECT_EQ(-1, dateFunction(Timestamp(-kSecondsInDay, 0)));
  EXPECT_EQ(18297, dateFunction(Timestamp(18297 * kSecondsInDay, 0)));
  EXPECT_EQ(18297, dateFunction(Timestamp(18297 * kSecondsInDay, 123)));
  EXPECT_EQ(-18297, dateFunction(Timestamp(-18297 * kSecondsInDay, 0)));
  EXPECT_EQ(-18297, dateFunction(Timestamp(-18297 * kSecondsInDay, 123)));

  // Last second of day 0
  EXPECT_EQ(0, dateFunction(Timestamp(kSecondsInDay - 1, 0)));
  // Last nanosecond of day 0
  EXPECT_EQ(0, dateFunction(Timestamp(kSecondsInDay - 1, kNanosInSecond - 1)));

  // Last second of day -1
  EXPECT_EQ(-1, dateFunction(Timestamp(-1, 0)));
  // Last nanosecond of day -1
  EXPECT_EQ(-1, dateFunction(Timestamp(-1, kNanosInSecond - 1)));

  // Last second of day 18297
  EXPECT_EQ(
      18297,
      dateFunction(Timestamp(18297 * kSecondsInDay + kSecondsInDay - 1, 0)));
  // Last nanosecond of day 18297
  EXPECT_EQ(
      18297,
      dateFunction(Timestamp(
          18297 * kSecondsInDay + kSecondsInDay - 1, kNanosInSecond - 1)));

  // Last second of day -18297
  EXPECT_EQ(
      -18297,
      dateFunction(Timestamp(-18297 * kSecondsInDay + kSecondsInDay - 1, 0)));
  // Last nanosecond of day -18297
  EXPECT_EQ(
      -18297,
      dateFunction(Timestamp(
          -18297 * kSecondsInDay + kSecondsInDay - 1, kNanosInSecond - 1)));
}

TEST_F(DateTimeFunctionsTest, dateFunctionTimestampWithTimezone) {
  static const int64_t kSecondsInDay = 86'400;

  const auto dateFunction =
      [&](std::optional<int64_t> timestamp,
          const std::optional<std::string>& timeZoneName) {
        return evaluateWithTimestampWithTimezone<int32_t>(
            "date(c0)", timestamp, timeZoneName);
      };

  // 1970-01-01 00:00:00.000 +00:00
  EXPECT_EQ(0, dateFunction(0, "+00:00"));
  EXPECT_EQ(0, dateFunction(0, "Europe/London"));
  // 1970-01-01 00:00:00.000 -08:00
  EXPECT_EQ(-1, dateFunction(0, "-08:00"));
  EXPECT_EQ(-1, dateFunction(0, "America/Los_Angeles"));
  // 1970-01-01 00:00:00.000 +08:00
  EXPECT_EQ(0, dateFunction(0, "+08:00"));
  EXPECT_EQ(0, dateFunction(0, "Asia/Chongqing"));
  // 1970-01-01 18:00:00.000 +08:00
  EXPECT_EQ(1, dateFunction(18 * 3'600 * 1'000, "+08:00"));
  EXPECT_EQ(1, dateFunction(18 * 3'600 * 1'000, "Asia/Chongqing"));
  // 1970-01-01 06:00:00.000 -08:00
  EXPECT_EQ(-1, dateFunction(6 * 3'600 * 1'000, "-08:00"));
  EXPECT_EQ(-1, dateFunction(6 * 3'600 * 1'000, "America/Los_Angeles"));

  // 2020-02-05 10:00:00.000 +08:00
  EXPECT_EQ(
      18297,
      dateFunction((18297 * kSecondsInDay + 10 * 3'600) * 1'000, "+08:00"));
  EXPECT_EQ(
      18297,
      dateFunction(
          (18297 * kSecondsInDay + 10 * 3'600) * 1'000, "Asia/Chongqing"));
  // 2020-02-05 20:00:00.000 +08:00
  EXPECT_EQ(
      18298,
      dateFunction((18297 * kSecondsInDay + 20 * 3'600) * 1'000, "+08:00"));
  EXPECT_EQ(
      18298,
      dateFunction(
          (18297 * kSecondsInDay + 20 * 3'600) * 1'000, "Asia/Chongqing"));
  // 2020-02-05 16:00:00.000 -08:00
  EXPECT_EQ(
      18297,
      dateFunction((18297 * kSecondsInDay + 16 * 3'600) * 1'000, "-08:00"));
  EXPECT_EQ(
      18297,
      dateFunction(
          (18297 * kSecondsInDay + 16 * 3'600) * 1'000, "America/Los_Angeles"));
  // 2020-02-05 06:00:00.000 -08:00
  EXPECT_EQ(
      18296,
      dateFunction((18297 * kSecondsInDay + 6 * 3'600) * 1'000, "-08:00"));
  EXPECT_EQ(
      18296,
      dateFunction(
          (18297 * kSecondsInDay + 6 * 3'600) * 1'000, "America/Los_Angeles"));

  // 1919-11-28 10:00:00.000 +08:00
  EXPECT_EQ(
      -18297,
      dateFunction((-18297 * kSecondsInDay + 10 * 3'600) * 1'000, "+08:00"));
  EXPECT_EQ(
      -18297,
      dateFunction(
          (-18297 * kSecondsInDay + 10 * 3'600) * 1'000, "Asia/Chongqing"));
  // 1919-11-28 20:00:00.000 +08:00
  EXPECT_EQ(
      -18296,
      dateFunction((-18297 * kSecondsInDay + 20 * 3'600) * 1'000, "+08:00"));
  EXPECT_EQ(
      -18296,
      dateFunction(
          (-18297 * kSecondsInDay + 20 * 3'600) * 1'000, "Asia/Chongqing"));
  // 1919-11-28 16:00:00.000 -08:00
  EXPECT_EQ(
      -18297,
      dateFunction((-18297 * kSecondsInDay + 16 * 3'600) * 1'000, "-08:00"));
  EXPECT_EQ(
      -18297,
      dateFunction(
          (-18297 * kSecondsInDay + 16 * 3'600) * 1'000,
          "America/Los_Angeles"));
  // 1919-11-28 06:00:00.000 -08:00
  EXPECT_EQ(
      -18298,
      dateFunction((-18297 * kSecondsInDay + 6 * 3'600) * 1'000, "-08:00"));
  EXPECT_EQ(
      -18298,
      dateFunction(
          (-18297 * kSecondsInDay + 6 * 3'600) * 1'000, "America/Los_Angeles"));
}

TEST_F(DateTimeFunctionsTest, castDateForDateFunction) {
  setQueryTimeZone("America/Los_Angeles");

  static const int64_t kSecondsInDay = 86'400;
  static const uint64_t kNanosInSecond = 1'000'000'000;
  const auto castDateTest = [&](std::optional<Timestamp> timestamp) {
    auto r1 = evaluateOnce<int32_t>("cast(c0 as date)", timestamp);
    auto r2 = evaluateOnce<int32_t>("date(c0)", timestamp);
    EXPECT_EQ(r1, r2);
    return r1;
  };

  // Note adjustments for PST timezone.
  EXPECT_EQ(-1, castDateTest(Timestamp()));
  EXPECT_EQ(0, castDateTest(Timestamp(kSecondsInDay, 0)));
  EXPECT_EQ(-2, castDateTest(Timestamp(-kSecondsInDay, 0)));
  EXPECT_EQ(18296, castDateTest(Timestamp(18297 * kSecondsInDay, 0)));
  EXPECT_EQ(18296, castDateTest(Timestamp(18297 * kSecondsInDay, 123)));
  EXPECT_EQ(-18298, castDateTest(Timestamp(-18297 * kSecondsInDay, 0)));
  EXPECT_EQ(-18298, castDateTest(Timestamp(-18297 * kSecondsInDay, 123)));

  // Last second of day 0.
  EXPECT_EQ(0, castDateTest(Timestamp(kSecondsInDay - 1, 0)));
  // Last nanosecond of day 0.
  EXPECT_EQ(0, castDateTest(Timestamp(kSecondsInDay - 1, kNanosInSecond - 1)));

  // Last second of day -1.
  EXPECT_EQ(-1, castDateTest(Timestamp(-1, 0)));
  // Last nanosecond of day -1.
  EXPECT_EQ(-1, castDateTest(Timestamp(-1, kNanosInSecond - 1)));

  // Last second of day 18297.
  EXPECT_EQ(
      18297,
      castDateTest(Timestamp(18297 * kSecondsInDay + kSecondsInDay - 1, 0)));
  // Last nanosecond of day 18297.
  EXPECT_EQ(
      18297,
      castDateTest(Timestamp(
          18297 * kSecondsInDay + kSecondsInDay - 1, kNanosInSecond - 1)));

  // Last second of day -18297.
  EXPECT_EQ(
      -18297,
      castDateTest(Timestamp(-18297 * kSecondsInDay + kSecondsInDay - 1, 0)));
  // Last nanosecond of day -18297.
  EXPECT_EQ(
      -18297,
      castDateTest(Timestamp(
          -18297 * kSecondsInDay + kSecondsInDay - 1, kNanosInSecond - 1)));
}

TEST_F(DateTimeFunctionsTest, currentDateWithTimezone) {
  // Since the execution of the code is slightly delayed, it is difficult for us
  // to get the correct value of current_date. If you compare directly based on
  // the current time, you may get wrong result at the last second of the day,
  // and current_date may be the next day of the comparison value. In order to
  // avoid this situation, we compute a new comparison value after the execution
  // of current_date, so that the result of current_date is either consistent
  // with the first comparison value or the second comparison value, and the
  // difference between the two comparison values is at most one day.
  auto emptyRowVector = makeRowVector(ROW({}), 1);
  auto tz = "America/Los_Angeles";
  setQueryTimeZone(tz);
  auto dateBefore = getCurrentDate(tz);
  auto result = evaluateOnce<int32_t>("current_date()", emptyRowVector);
  auto dateAfter = getCurrentDate(tz);

  EXPECT_TRUE(result.has_value());
  EXPECT_LE(dateBefore, result);
  EXPECT_LE(result, dateAfter);
  EXPECT_LE(dateAfter - dateBefore, 1);
}

TEST_F(DateTimeFunctionsTest, currentDateWithoutTimezone) {
  auto emptyRowVector = makeRowVector(ROW({}), 1);

  // Do not set the timezone, so the timezone obtained from QueryConfig
  // will be nullptr.
  auto dateBefore = getCurrentDate(std::nullopt);
  auto result = evaluateOnce<int32_t>("current_date()", emptyRowVector);
  auto dateAfter = getCurrentDate(std::nullopt);

  EXPECT_TRUE(result.has_value());
  EXPECT_LE(dateBefore, result);
  EXPECT_LE(result, dateAfter);
  EXPECT_LE(dateAfter - dateBefore, 1);
}

TEST_F(DateTimeFunctionsTest, timeZoneHour) {
  const auto timezone_hour = [&](const char* time, const char* timezone) {
    Timestamp ts = util::fromTimestampString(time);
    auto timestamp = ts.toMillis();
    auto hour = evaluateWithTimestampWithTimezone<int64_t>(
                    "timezone_hour(c0)", timestamp, timezone)
                    .value();
    return hour;
  };

  // Asia/Kolkata - should return 5 throughout the year
  EXPECT_EQ(5, timezone_hour("2023-01-01 03:20:00", "Asia/Kolkata"));
  EXPECT_EQ(5, timezone_hour("2023-06-01 03:20:00", "Asia/Kolkata"));

  // America/Los_Angeles - Day light savings is from March 12 to Nov 5
  EXPECT_EQ(-8, timezone_hour("2023-03-11 12:00:00", "America/Los_Angeles"));
  EXPECT_EQ(-8, timezone_hour("2023-03-12 02:30:00", "America/Los_Angeles"));
  EXPECT_EQ(-7, timezone_hour("2023-03-13 12:00:00", "America/Los_Angeles"));
  EXPECT_EQ(-7, timezone_hour("2023-11-05 01:30:00", "America/Los_Angeles"));
  EXPECT_EQ(-8, timezone_hour("2023-12-05 01:30:00", "America/Los_Angeles"));

  // Different time with same date
  EXPECT_EQ(-4, timezone_hour("2023-01-01 03:20:00", "Canada/Atlantic"));
  EXPECT_EQ(-4, timezone_hour("2023-01-01 10:00:00", "Canada/Atlantic"));

  // By definition (+/-) 00:00 offsets should always return the hour part of the
  // offset itself.
  EXPECT_EQ(0, timezone_hour("2023-12-05 01:30:00", "+00:00"));
  EXPECT_EQ(8, timezone_hour("2023-12-05 01:30:00", "+08:00"));
  EXPECT_EQ(-10, timezone_hour("2023-12-05 01:30:00", "-10:00"));

  // Invalid inputs
  VELOX_ASSERT_THROW(
      timezone_hour("invalid_date", "Canada/Atlantic"),
      "Unable to parse timestamp value: \"invalid_date\", expected format is (YYYY-MM-DD HH:MM:SS[.MS])");
  VELOX_ASSERT_THROW(
      timezone_hour("123456", "Canada/Atlantic"),
      "Unable to parse timestamp value: \"123456\", expected format is (YYYY-MM-DD HH:MM:SS[.MS])");
}

TEST_F(DateTimeFunctionsTest, timeZoneMinute) {
  const auto timezone_minute = [&](const char* time, const char* timezone) {
    Timestamp ts = util::fromTimestampString(time);
    auto timestamp = ts.toMillis();
    auto minute = evaluateWithTimestampWithTimezone<int64_t>(
                      "timezone_minute(c0)", timestamp, timezone)
                      .value();
    return minute;
  };

  EXPECT_EQ(30, timezone_minute("1970-01-01 03:20:00", "Asia/Kolkata"));
  EXPECT_EQ(0, timezone_minute("1970-01-01 03:20:00", "America/Los_Angeles"));
  EXPECT_EQ(0, timezone_minute("1970-05-01 04:20:00", "America/Los_Angeles"));
  EXPECT_EQ(0, timezone_minute("1970-01-01 03:20:00", "Canada/Atlantic"));
  EXPECT_EQ(30, timezone_minute("1970-01-01 03:20:00", "Asia/Katmandu"));
  EXPECT_EQ(45, timezone_minute("1970-01-01 03:20:00", "Pacific/Chatham"));

  // By definition (+/-) 00:00 offsets should always return the minute part of
  // the offset itself.
  EXPECT_EQ(0, timezone_minute("2023-12-05 01:30:00", "+00:00"));
  EXPECT_EQ(17, timezone_minute("2023-12-05 01:30:00", "+08:17"));
  EXPECT_EQ(-59, timezone_minute("2023-12-05 01:30:00", "-10:59"));

  VELOX_ASSERT_THROW(
      timezone_minute("abc", "Pacific/Chatham"),
      "Unable to parse timestamp value: \"abc\", expected format is (YYYY-MM-DD HH:MM:SS[.MS])");
  VELOX_ASSERT_THROW(
      timezone_minute("2023-", "Pacific/Chatham"),
      "Unable to parse timestamp value: \"2023-\", expected format is (YYYY-MM-DD HH:MM:SS[.MS])");
}

TEST_F(DateTimeFunctionsTest, timestampWithTimezoneComparisons) {
  auto runAndCompare = [&](std::string expr,
                           std::shared_ptr<RowVector>& inputs,
                           VectorPtr expectedResult) {
    auto actual = evaluate<SimpleVector<bool>>(expr, inputs);
    test::assertEqualVectors(expectedResult, actual);
  };

  /// Timestamp with timezone is internally represented with the milliseconds
  /// already converted to UTC and thus normalized. The timezone does not play
  /// a role in the comparison.
  /// For example, 1970-01-01-06:00:00+02:00 is stored as
  /// TIMESTAMP WITH TIMEZONE value (14400000, 960). 960 being the tzid
  /// representing +02:00. And 1970-01-01-04:00:00+00:00 is stored as (14400000,
  /// 0). These timestamps are equivalent.
  auto timestampsLhs = std::vector<int64_t>{0, 0, 1000};
  auto timezonesLhs = std::vector<TimeZoneKey>{900, 900, 800};
  VectorPtr timestampWithTimezoneLhs = makeTimestampWithTimeZoneVector(
      timestampsLhs.size(),
      [&](auto row) { return timestampsLhs[row]; },
      [&](auto row) { return timezonesLhs[row]; });

  auto timestampsRhs = std::vector<int64_t>{0, 1000, 0};
  auto timezonesRhs = std::vector<TimeZoneKey>{900, 900, 800};
  VectorPtr timestampWithTimezoneRhs = makeTimestampWithTimeZoneVector(
      timestampsRhs.size(),
      [&](auto row) { return timestampsRhs[row]; },
      [&](auto row) { return timezonesRhs[row]; });
  auto inputs =
      makeRowVector({timestampWithTimezoneLhs, timestampWithTimezoneRhs});

  auto expectedEq = makeNullableFlatVector<bool>({true, false, false});
  runAndCompare("c0 = c1", inputs, expectedEq);

  auto expectedNeq = makeNullableFlatVector<bool>({false, true, true});
  runAndCompare("c0 != c1", inputs, expectedNeq);

  auto expectedLt = makeNullableFlatVector<bool>({false, true, false});
  runAndCompare("c0 < c1", inputs, expectedLt);

  auto expectedGt = makeNullableFlatVector<bool>({false, false, true});
  runAndCompare("c0 > c1", inputs, expectedGt);

  auto expectedLte = makeNullableFlatVector<bool>({true, true, false});
  runAndCompare("c0 <= c1", inputs, expectedLte);

  auto expectedGte = makeNullableFlatVector<bool>({true, false, true});
  runAndCompare("c0 >= c1", inputs, expectedGte);
}

TEST_F(DateTimeFunctionsTest, castDateToTimestamp) {
  const int64_t kSecondsInDay = kMillisInDay / 1'000;
  const auto castDateToTimestamp = [&](const std::optional<int32_t> date) {
    return evaluateOnce<Timestamp, int32_t>(
        "cast(c0 AS timestamp)", {date}, {DATE()});
  };

  EXPECT_EQ(Timestamp(0, 0), castDateToTimestamp(DATE()->toDays("1970-01-01")));
  EXPECT_EQ(
      Timestamp(kSecondsInDay, 0),
      castDateToTimestamp(DATE()->toDays("1970-01-02")));
  EXPECT_EQ(
      Timestamp(2 * kSecondsInDay, 0),
      castDateToTimestamp(DATE()->toDays("1970-01-03")));
  EXPECT_EQ(
      Timestamp(18297 * kSecondsInDay, 0),
      castDateToTimestamp(DATE()->toDays("2020-02-05")));
  EXPECT_EQ(
      Timestamp(-1 * kSecondsInDay, 0),
      castDateToTimestamp(DATE()->toDays("1969-12-31")));
  EXPECT_EQ(
      Timestamp(-18297 * kSecondsInDay, 0),
      castDateToTimestamp(DATE()->toDays("1919-11-28")));

  const auto tz = "America/Los_Angeles";
  const auto kTimezoneOffset = 8 * kMillisInHour / 1'000;
  setQueryTimeZone(tz);
  EXPECT_EQ(
      Timestamp(kTimezoneOffset, 0),
      castDateToTimestamp(DATE()->toDays("1970-01-01")));
  EXPECT_EQ(
      Timestamp(kSecondsInDay + kTimezoneOffset, 0),
      castDateToTimestamp(DATE()->toDays("1970-01-02")));
  EXPECT_EQ(
      Timestamp(2 * kSecondsInDay + kTimezoneOffset, 0),
      castDateToTimestamp(DATE()->toDays("1970-01-03")));
  EXPECT_EQ(
      Timestamp(18297 * kSecondsInDay + kTimezoneOffset, 0),
      castDateToTimestamp(DATE()->toDays("2020-02-05")));
  EXPECT_EQ(
      Timestamp(-1 * kSecondsInDay + kTimezoneOffset, 0),
      castDateToTimestamp(DATE()->toDays("1969-12-31")));
  EXPECT_EQ(
      Timestamp(-18297 * kSecondsInDay + kTimezoneOffset, 0),
      castDateToTimestamp(DATE()->toDays("1919-11-28")));

  disableAdjustTimestampToTimezone();
  EXPECT_EQ(Timestamp(0, 0), castDateToTimestamp(DATE()->toDays("1970-01-01")));
  EXPECT_EQ(
      Timestamp(kSecondsInDay, 0),
      castDateToTimestamp(DATE()->toDays("1970-01-02")));
  EXPECT_EQ(
      Timestamp(2 * kSecondsInDay, 0),
      castDateToTimestamp(DATE()->toDays("1970-01-03")));
  EXPECT_EQ(
      Timestamp(18297 * kSecondsInDay, 0),
      castDateToTimestamp(DATE()->toDays("2020-02-05")));
  EXPECT_EQ(
      Timestamp(-1 * kSecondsInDay, 0),
      castDateToTimestamp(DATE()->toDays("1969-12-31")));
  EXPECT_EQ(
      Timestamp(-18297 * kSecondsInDay, 0),
      castDateToTimestamp(DATE()->toDays("1919-11-28")));
}

TEST_F(DateTimeFunctionsTest, lastDayOfMonthDate) {
  const auto lastDayFunc = [&](const std::optional<int32_t> date) {
    return evaluateOnce<int32_t, int32_t>(
        "last_day_of_month(c0)", {date}, {DATE()});
  };

  const auto lastDay = [&](const StringView& dateStr) {
    return lastDayFunc(DATE()->toDays(dateStr));
  };

  EXPECT_EQ(std::nullopt, lastDayFunc(std::nullopt));
  EXPECT_EQ(parseDate("1970-01-31"), lastDay("1970-01-01"));
  EXPECT_EQ(parseDate("2008-02-29"), lastDay("2008-02-01"));
  EXPECT_EQ(parseDate("2023-02-28"), lastDay("2023-02-01"));
  EXPECT_EQ(parseDate("2023-02-28"), lastDay("2023-02-01"));
  EXPECT_EQ(parseDate("2023-03-31"), lastDay("2023-03-11"));
  EXPECT_EQ(parseDate("2023-04-30"), lastDay("2023-04-21"));
  EXPECT_EQ(parseDate("2023-05-31"), lastDay("2023-05-09"));
  EXPECT_EQ(parseDate("2023-06-30"), lastDay("2023-06-01"));
  EXPECT_EQ(parseDate("2023-07-31"), lastDay("2023-07-31"));
  EXPECT_EQ(parseDate("2023-07-31"), lastDay("2023-07-31"));
  EXPECT_EQ(parseDate("2023-07-31"), lastDay("2023-07-11"));
  EXPECT_EQ(parseDate("2023-08-31"), lastDay("2023-08-01"));
  EXPECT_EQ(parseDate("2023-09-30"), lastDay("2023-09-09"));
  EXPECT_EQ(parseDate("2023-10-31"), lastDay("2023-10-01"));
  EXPECT_EQ(parseDate("2023-11-30"), lastDay("2023-11-11"));
  EXPECT_EQ(parseDate("2023-12-31"), lastDay("2023-12-12"));
}

TEST_F(DateTimeFunctionsTest, lastDayOfMonthTimestamp) {
  const auto lastDayFunc = [&](const std::optional<Timestamp>& date) {
    return evaluateOnce<int32_t>("last_day_of_month(c0)", date);
  };

  const auto lastDay = [&](const StringView& dateStr) {
    return lastDayFunc(util::fromTimestampString(dateStr));
  };

  setQueryTimeZone("Pacific/Apia");

  EXPECT_EQ(std::nullopt, lastDayFunc(std::nullopt));
  EXPECT_EQ(parseDate("1970-01-31"), lastDay("1970-01-01 20:23:00.007"));
  EXPECT_EQ(parseDate("1970-01-31"), lastDay("1970-01-01 12:00:00.001"));
  EXPECT_EQ(parseDate("2008-02-29"), lastDay("2008-02-01 12:00:00"));
  EXPECT_EQ(parseDate("2023-02-28"), lastDay("2023-02-01 23:59:59.999"));
  EXPECT_EQ(parseDate("2023-02-28"), lastDay("2023-02-01 12:00:00"));
  EXPECT_EQ(parseDate("2023-03-31"), lastDay("2023-03-11 12:00:00"));
}

TEST_F(DateTimeFunctionsTest, lastDayOfMonthTimestampWithTimezone) {
  EXPECT_EQ(
      parseDate("1970-01-31"),
      evaluateWithTimestampWithTimezone<int32_t>(
          "last_day_of_month(c0)", 0, "+00:00"));
  EXPECT_EQ(
      parseDate("1969-12-31"),
      evaluateWithTimestampWithTimezone<int32_t>(
          "last_day_of_month(c0)", 0, "-02:00"));
  EXPECT_EQ(
      parseDate("2008-02-29"),
      evaluateWithTimestampWithTimezone<int32_t>(
          "last_day_of_month(c0)", 1201881600000, "+02:00"));
  EXPECT_EQ(
      parseDate("2008-01-31"),
      evaluateWithTimestampWithTimezone<int32_t>(
          "last_day_of_month(c0)", 1201795200000, "-02:00"));
}

TEST_F(DateTimeFunctionsTest, fromUnixtimeDouble) {
  auto input = makeFlatVector<double>({
      1623748302.,
      1623748302.0,
      1623748302.02,
      1623748302.023,
      1623748303.123,
      1623748304.009,
      1623748304.001,
      1623748304.999,
      1623748304.001290,
      1623748304.001890,
      1623748304.999390,
      1623748304.999590,
  });
  auto actual =
      evaluate("cast(from_unixtime(c0) as varchar)", makeRowVector({input}));
  auto expected = makeFlatVector<StringView>({
      "2021-06-15 09:11:42.000",
      "2021-06-15 09:11:42.000",
      "2021-06-15 09:11:42.020",
      "2021-06-15 09:11:42.023",
      "2021-06-15 09:11:43.123",
      "2021-06-15 09:11:44.009",
      "2021-06-15 09:11:44.001",
      "2021-06-15 09:11:44.999",
      "2021-06-15 09:11:44.001",
      "2021-06-15 09:11:44.002",
      "2021-06-15 09:11:44.999",
      "2021-06-15 09:11:45.000",
  });
  assertEqualVectors(expected, actual);
}
