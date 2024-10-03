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
  // Helper class to manipulate timestamp with timezone types in tests. Provided
  // only for convenience.
  struct TimestampWithTimezone {
    TimestampWithTimezone(int64_t milliSeconds, std::string_view timezoneName)
        : milliSeconds_(milliSeconds),
          timezone_(tz::locateZone(timezoneName)) {}

    TimestampWithTimezone(int64_t milliSeconds, const tz::TimeZone* timezone)
        : milliSeconds_(milliSeconds), timezone_(timezone) {}

    int64_t milliSeconds_{0};
    const tz::TimeZone* timezone_;

    static std::optional<int64_t> pack(
        std::optional<TimestampWithTimezone> zone) {
      if (zone.has_value()) {
        return facebook::velox::pack(
            zone->milliSeconds_, zone->timezone_->id());
      }
      return std::nullopt;
    }

    static std::optional<TimestampWithTimezone> unpack(
        std::optional<int64_t> input) {
      if (input.has_value()) {
        return TimestampWithTimezone(
            unpackMillisUtc(input.value()),
            tz::getTimeZoneName(unpackZoneKeyId(input.value())));
      }
      return std::nullopt;
    }

    // Provides a nicer printer for gtest.
    friend std::ostream& operator<<(
        std::ostream& os,
        const TimestampWithTimezone& in) {
      return os << "TimestampWithTimezone(milliSeconds: " << in.milliSeconds_
                << ", timezone: " << *in.timezone_ << ")";
    }
  };

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

  VectorPtr makeTimestampWithTimeZoneVector(int64_t timestamp, const char* tz) {
    auto tzid = tz::getTimeZoneID(tz);

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
  return a.milliSeconds_ == b.milliSeconds_ &&
      a.timezone_->id() == b.timezone_->id();
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
  const auto fromUnixtime = [&](std::optional<double> timestamp,
                                std::optional<std::string> timezoneName) {
    return TimestampWithTimezone::unpack(evaluateOnce<int64_t>(
        "from_unixtime(c0, c1)", timestamp, timezoneName));
  };

  // Check null behavior.
  EXPECT_EQ(fromUnixtime(std::nullopt, std::nullopt), std::nullopt);
  EXPECT_EQ(fromUnixtime(std::nullopt, "UTC"), std::nullopt);
  EXPECT_EQ(fromUnixtime(0, std::nullopt), std::nullopt);

  EXPECT_EQ(
      fromUnixtime(1631800000.12345, "-01:00"),
      TimestampWithTimezone(1631800000123, "-01:00"));
  EXPECT_EQ(
      fromUnixtime(123.99, "America/Los_Angeles"),
      TimestampWithTimezone(123990, "America/Los_Angeles"));
  EXPECT_EQ(
      fromUnixtime(1667721600.1, "UTC"),
      TimestampWithTimezone(1667721600100, "UTC"));

  // Nan.
  static const double kNan = std::numeric_limits<double>::quiet_NaN();
  EXPECT_EQ(fromUnixtime(kNan, "-04:36"), TimestampWithTimezone(0, "-04:36"));
}

TEST_F(DateTimeFunctionsTest, fromUnixtimeTzOffset) {
  auto fromOffset = [&](std::optional<double> epoch,
                        std::optional<int64_t> hours,
                        std::optional<int64_t> minutes) {
    auto result = evaluateOnce<int64_t>(
        "from_unixtime(c0, c1, c2)", epoch, hours, minutes);

    auto otherResult = evaluateOnce<int64_t>(
        fmt::format("from_unixtime(c0, {}, {})", *hours, *minutes), epoch);

    VELOX_CHECK_EQ(result.value(), otherResult.value());
    return TimestampWithTimezone::unpack(result.value());
  };

  EXPECT_EQ(TimestampWithTimezone(123'450, "UTC"), fromOffset(123.45, 0, 0));
  EXPECT_EQ(
      TimestampWithTimezone(123'450, "+05:30"), fromOffset(123.45, 5, 30));
  EXPECT_EQ(
      TimestampWithTimezone(123'450, "-08:00"), fromOffset(123.45, -8, 0));
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
    return evaluateOnce<int64_t>("year(c0)", DATE(), date);
  };
  EXPECT_EQ(std::nullopt, year(std::nullopt));
  EXPECT_EQ(1970, year(parseDate("1970-01-01")));
  EXPECT_EQ(1969, year(parseDate("1969-12-31")));
  EXPECT_EQ(2020, year(parseDate("2020-01-01")));
  EXPECT_EQ(1920, year(parseDate("1920-12-31")));
}

TEST_F(DateTimeFunctionsTest, yearTimestampWithTimezone) {
  const auto yearTimestampWithTimezone =
      [&](std::optional<TimestampWithTimezone> timestampWithTimezone) {
        return evaluateOnce<int64_t>(
            "year(c0)",
            TIMESTAMP_WITH_TIME_ZONE(),
            TimestampWithTimezone::pack(timestampWithTimezone));
      };
  EXPECT_EQ(
      1969, yearTimestampWithTimezone(TimestampWithTimezone(0, "-01:00")));
  EXPECT_EQ(
      1970, yearTimestampWithTimezone(TimestampWithTimezone(0, "+00:00")));
  EXPECT_EQ(
      1973,
      yearTimestampWithTimezone(TimestampWithTimezone(123456789000, "+14:00")));
  EXPECT_EQ(
      1966,
      yearTimestampWithTimezone(
          TimestampWithTimezone(-123456789000, "+03:00")));
  EXPECT_EQ(
      2001,
      yearTimestampWithTimezone(TimestampWithTimezone(987654321000, "-07:00")));
  EXPECT_EQ(
      1938,
      yearTimestampWithTimezone(
          TimestampWithTimezone(-987654321000, "-13:00")));
  EXPECT_EQ(std::nullopt, yearTimestampWithTimezone(std::nullopt));
}

TEST_F(DateTimeFunctionsTest, weekDate) {
  const auto weekDate = [&](const char* dateString) {
    auto date = std::make_optional(parseDate(dateString));
    auto week = evaluateOnce<int64_t>("week(c0)", DATE(), date).value();
    auto weekOfYear =
        evaluateOnce<int64_t>("week_of_year(c0)", DATE(), date).value();
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

  // Test various cases where the last week of the previous year extends into
  // the next year.

  // Leap year that ends on Thursday.
  EXPECT_EQ(53, weekDate("2021-01-01"));
  // Leap year that ends on Friday.
  EXPECT_EQ(53, weekDate("2005-01-01"));
  // Leap year that ends on Saturday.
  EXPECT_EQ(52, weekDate("2017-01-01"));
  // Common year that ends on Thursday.
  EXPECT_EQ(53, weekDate("2016-01-01"));
  // Common year that ends on Friday.
  EXPECT_EQ(52, weekDate("2022-01-01"));
  // Common year that ends on Saturday.
  EXPECT_EQ(52, weekDate("2023-01-01"));
}

TEST_F(DateTimeFunctionsTest, week) {
  const auto weekTimestamp = [&](std::string_view time) {
    auto ts = util::fromTimestampString(
                  time.data(), time.size(), util::TimestampParseMode::kIso8601)
                  .thenOrThrow(folly::identity, [&](const Status& status) {
                    VELOX_USER_FAIL("{}", status.message());
                  });
    auto timestamp =
        std::make_optional(Timestamp(ts.getSeconds() * 100'000, 0));

    auto week = evaluateOnce<int64_t>("week(c0)", timestamp).value();
    auto weekOfYear =
        evaluateOnce<int64_t>("week_of_year(c0)", timestamp).value();
    VELOX_CHECK_EQ(
        week, weekOfYear, "week and week_of_year must return the same value");
    return week;
  };

  EXPECT_EQ(1, weekTimestamp("T00:00:00"));
  EXPECT_EQ(47, weekTimestamp("T11:59:59"));
  EXPECT_EQ(33, weekTimestamp("T06:01:01"));
  EXPECT_EQ(44, weekTimestamp("T06:59:59"));
  EXPECT_EQ(47, weekTimestamp("T12:00:01"));
  EXPECT_EQ(16, weekTimestamp("T12:59:59"));
}

TEST_F(DateTimeFunctionsTest, weekTimestampWithTimezone) {
  const auto weekTimestampTimezone = [&](std::string_view time,
                                         const char* timezone) {
    auto ts = util::fromTimestampString(
                  time.data(), time.size(), util::TimestampParseMode::kIso8601)
                  .thenOrThrow(folly::identity, [&](const Status& status) {
                    VELOX_USER_FAIL("{}", status.message());
                  });

    auto timestamp = ts.getSeconds() * 100'000'000;
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

  EXPECT_EQ(1, weekTimestampTimezone("T00:00:00", "-12:00"));
  EXPECT_EQ(1, weekTimestampTimezone("T00:00:00", "+12:00"));
  EXPECT_EQ(47, weekTimestampTimezone("T11:59:59", "-12:00"));
  EXPECT_EQ(47, weekTimestampTimezone("T11:59:59", "+12:00"));
  EXPECT_EQ(33, weekTimestampTimezone("T06:01:01", "-12:00"));
  EXPECT_EQ(34, weekTimestampTimezone("T06:01:01", "+12:00"));
  EXPECT_EQ(47, weekTimestampTimezone("T12:00:01", "-12:00"));
  EXPECT_EQ(47, weekTimestampTimezone("T12:00:01", "+12:00"));
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
    return evaluateOnce<int64_t>("quarter(c0)", DATE(), date);
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
  const auto quarterTimestampWithTimezone =
      [&](std::optional<TimestampWithTimezone> timestampWithTimezone) {
        return evaluateOnce<int64_t>(
            "quarter(c0)",
            TIMESTAMP_WITH_TIME_ZONE(),
            TimestampWithTimezone::pack(timestampWithTimezone));
      };
  EXPECT_EQ(
      4, quarterTimestampWithTimezone(TimestampWithTimezone(0, "-01:00")));
  EXPECT_EQ(
      1, quarterTimestampWithTimezone(TimestampWithTimezone(0, "+00:00")));
  EXPECT_EQ(
      4,
      quarterTimestampWithTimezone(
          TimestampWithTimezone(123456789000, "+14:00")));
  EXPECT_EQ(
      1,
      quarterTimestampWithTimezone(
          TimestampWithTimezone(-123456789000, "+03:00")));
  EXPECT_EQ(
      2,
      quarterTimestampWithTimezone(
          TimestampWithTimezone(987654321000, "-07:00")));
  EXPECT_EQ(
      3,
      quarterTimestampWithTimezone(
          TimestampWithTimezone(-987654321000, "-13:00")));
  EXPECT_EQ(std::nullopt, quarterTimestampWithTimezone(std::nullopt));
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
    return evaluateOnce<int64_t>("month(c0)", DATE(), date);
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
  const auto monthTimestampWithTimezone =
      [&](std::optional<TimestampWithTimezone> timestampWithTimezone) {
        return evaluateOnce<int64_t>(
            "month(c0)",
            TIMESTAMP_WITH_TIME_ZONE(),
            TimestampWithTimezone::pack(timestampWithTimezone));
      };
  EXPECT_EQ(12, monthTimestampWithTimezone(TimestampWithTimezone(0, "-01:00")));
  EXPECT_EQ(1, monthTimestampWithTimezone(TimestampWithTimezone(0, "+00:00")));
  EXPECT_EQ(
      11,
      monthTimestampWithTimezone(
          TimestampWithTimezone(123456789000, "+14:00")));
  EXPECT_EQ(
      2,
      monthTimestampWithTimezone(
          TimestampWithTimezone(-123456789000, "+03:00")));
  EXPECT_EQ(
      4,
      monthTimestampWithTimezone(
          TimestampWithTimezone(987654321000, "-07:00")));
  EXPECT_EQ(
      9,
      monthTimestampWithTimezone(
          TimestampWithTimezone(-987654321000, "-13:00")));
  EXPECT_EQ(std::nullopt, monthTimestampWithTimezone(std::nullopt));
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
  const auto hourTimestampWithTimezone =
      [&](std::optional<TimestampWithTimezone> timestampWithTimezone) {
        return evaluateOnce<int64_t>(
            "hour(c0)",
            TIMESTAMP_WITH_TIME_ZONE(),
            TimestampWithTimezone::pack(timestampWithTimezone));
      };
  EXPECT_EQ(
      20,
      hourTimestampWithTimezone(TimestampWithTimezone(998423705000, "+01:00")));
  EXPECT_EQ(
      12, hourTimestampWithTimezone(TimestampWithTimezone(41028000, "+01:00")));
  EXPECT_EQ(
      13, hourTimestampWithTimezone(TimestampWithTimezone(41028000, "+02:00")));
  EXPECT_EQ(
      14, hourTimestampWithTimezone(TimestampWithTimezone(41028000, "+03:00")));
  EXPECT_EQ(
      8, hourTimestampWithTimezone(TimestampWithTimezone(41028000, "-03:00")));
  EXPECT_EQ(
      1, hourTimestampWithTimezone(TimestampWithTimezone(41028000, "+14:00")));
  EXPECT_EQ(
      9, hourTimestampWithTimezone(TimestampWithTimezone(-100000, "-14:00")));
  EXPECT_EQ(
      2, hourTimestampWithTimezone(TimestampWithTimezone(-41028000, "+14:00")));
  EXPECT_EQ(std::nullopt, hourTimestampWithTimezone(std::nullopt));
}

TEST_F(DateTimeFunctionsTest, hourDate) {
  const auto hour = [&](std::optional<int32_t> date) {
    return evaluateOnce<int64_t>("hour(c0)", DATE(), date);
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
    return evaluateOnce<int64_t>("day_of_month(c0)", DATE(), date);
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
  const auto day = [&](std::optional<int64_t> millis) {
    auto result =
        evaluateOnce<int64_t>("day_of_month(c0)", INTERVAL_DAY_TIME(), millis);

    auto result2 =
        evaluateOnce<int64_t>("day(c0)", INTERVAL_DAY_TIME(), millis);

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

  // Check if it does the right thing if we cross daylight saving boundaries.
  setQueryTimeZone("America/Los_Angeles");
  EXPECT_EQ("2024-01-01 00:00:00", minus("2024-07-01 00:00:00", 6));
  EXPECT_EQ("2023-07-01 00:00:00", minus("2024-01-01 00:00:00", 6));
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

  // Check if it does the right thing if we cross daylight saving boundaries.
  setQueryTimeZone("America/Los_Angeles");
  EXPECT_EQ("2025-01-01 00:00:00", plus("2024-07-01 00:00:00", 6));
  EXPECT_EQ("2024-07-01 00:00:00", plus("2024-01-01 00:00:00", 6));
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

TEST_F(DateTimeFunctionsTest, timestampWithTimeZonePlusIntervalDayTime) {
  auto test = [&](const std::string& timestamp, int64_t interval) {
    // ts + interval == interval + ts == ts - (-interval) ==
    // date_add('millisecond', interval, ts).
    auto plusResult =
        evaluateOnce<std::string>(
            "cast(plus(cast(c0 as timestamp with time zone), c1) as varchar)",
            {VARCHAR(), INTERVAL_DAY_TIME()},
            std::optional(timestamp),
            std::optional(interval))
            .value();

    auto minusResult =
        evaluateOnce<std::string>(
            "cast(minus(cast(c0 as timestamp with time zone), c1) as varchar)",
            {VARCHAR(), INTERVAL_DAY_TIME()},
            std::optional(timestamp),
            std::optional(-interval))
            .value();

    auto otherPlusResult =
        evaluateOnce<std::string>(
            "cast(plus(c1, cast(c0 as timestamp with time zone)) as varchar)",
            {VARCHAR(), INTERVAL_DAY_TIME()},
            std::optional(timestamp),
            std::optional(interval))
            .value();

    auto dateAddResult =
        evaluateOnce<std::string>(
            "cast(date_add('millisecond', c1, cast(c0 as timestamp with time zone)) as varchar)",
            std::optional(timestamp),
            std::optional(interval))
            .value();

    VELOX_CHECK_EQ(plusResult, minusResult);
    VELOX_CHECK_EQ(plusResult, otherPlusResult);
    VELOX_CHECK_EQ(plusResult, dateAddResult);
    return plusResult;
  };

  EXPECT_EQ(
      "2024-10-04 01:50:00.000 America/Los_Angeles",
      test("2024-10-03 01:50 America/Los_Angeles", 1 * kMillisInDay));
  EXPECT_EQ(
      "2024-10-03 02:50:00.000 America/Los_Angeles",
      test("2024-10-03 01:50 America/Los_Angeles", 1 * kMillisInHour));
  EXPECT_EQ(
      "2024-10-03 01:51:00.000 America/Los_Angeles",
      test("2024-10-03 01:50 America/Los_Angeles", 1 * kMillisInMinute));

  // Testing daylight saving transitions.

  // At the beginning there is a 1 hour gap.
  EXPECT_EQ(
      "2024-03-10 01:30:00.000 America/Los_Angeles",
      test("2024-03-10 03:30 America/Los_Angeles", -1 * kMillisInHour));

  // At the end there is a 1 hour duplication.
  EXPECT_EQ(
      "2024-11-03 01:30:00.000 America/Los_Angeles",
      test("2024-11-03 01:30 America/Los_Angeles", 1 * kMillisInHour));
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

TEST_F(DateTimeFunctionsTest, minusTimestampWithTimezone) {
  auto minus = [&](const std::string& a, const std::string& b) {
    const auto sql =
        "cast(c0 as timestamp with time zone) - cast(c1 as timestamp with time zone)";

    auto result =
        evaluateOnce<int64_t>(sql, std::optional(a), std::optional(b));
    auto negativeResult =
        evaluateOnce<int64_t>(sql, std::optional(b), std::optional(a));

    // a - b == -(b - a)
    VELOX_CHECK_EQ(result.value(), -negativeResult.value());
    return result.value();
  };

  EXPECT_EQ(
      0,
      minus(
          "2024-04-15 10:20:33 America/New_York",
          "2024-04-15 10:20:33 America/New_York"));

  EXPECT_EQ(
      0,
      minus(
          "2024-04-15 13:20:33 America/New_York",
          "2024-04-15 10:20:33 America/Los_Angeles"));

  EXPECT_EQ(
      -1 * kMillisInHour - 2 * kMillisInMinute - 3 * kMillisInSecond,
      minus(
          "2024-04-15 10:20:33 America/New_York",
          "2024-04-15 11:22:36 America/New_York"));

  EXPECT_EQ(
      -1 * kMillisInHour - 2 * kMillisInMinute - 3 * kMillisInSecond,
      minus(
          "2024-04-15 07:20:33 America/Los_Angeles",
          "2024-04-15 11:22:36 America/New_York"));
}

TEST_F(DateTimeFunctionsTest, dayOfMonthTimestampWithTimezone) {
  const auto dayOfMonthTimestampWithTimezone =
      [&](std::optional<TimestampWithTimezone> timestampWithTimezone) {
        return evaluateOnce<int64_t>(
            "day_of_month(c0)",
            TIMESTAMP_WITH_TIME_ZONE(),
            TimestampWithTimezone::pack(timestampWithTimezone));
      };
  EXPECT_EQ(
      31, dayOfMonthTimestampWithTimezone(TimestampWithTimezone(0, "-01:00")));
  EXPECT_EQ(
      1, dayOfMonthTimestampWithTimezone(TimestampWithTimezone(0, "+00:00")));
  EXPECT_EQ(
      30,
      dayOfMonthTimestampWithTimezone(
          TimestampWithTimezone(123456789000, "+14:00")));
  EXPECT_EQ(
      2,
      dayOfMonthTimestampWithTimezone(
          TimestampWithTimezone(-123456789000, "+03:00")));
  EXPECT_EQ(
      18,
      dayOfMonthTimestampWithTimezone(
          TimestampWithTimezone(987654321000, "-07:00")));
  EXPECT_EQ(
      14,
      dayOfMonthTimestampWithTimezone(
          TimestampWithTimezone(-987654321000, "-13:00")));
  EXPECT_EQ(std::nullopt, dayOfMonthTimestampWithTimezone(std::nullopt));
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
    return evaluateOnce<int64_t>("day_of_week(c0)", DATE(), date);
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
  const auto dayOfWeekTimestampWithTimezone =
      [&](std::optional<TimestampWithTimezone> timestampWithTimezone) {
        return evaluateOnce<int64_t>(
            "day_of_week(c0)",
            TIMESTAMP_WITH_TIME_ZONE(),
            TimestampWithTimezone::pack(timestampWithTimezone));
      };
  EXPECT_EQ(
      3, dayOfWeekTimestampWithTimezone(TimestampWithTimezone(0, "-01:00")));
  EXPECT_EQ(
      4, dayOfWeekTimestampWithTimezone(TimestampWithTimezone(0, "+00:00")));
  EXPECT_EQ(
      5,
      dayOfWeekTimestampWithTimezone(
          TimestampWithTimezone(123456789000, "+14:00")));
  EXPECT_EQ(
      3,
      dayOfWeekTimestampWithTimezone(
          TimestampWithTimezone(-123456789000, "+03:00")));
  EXPECT_EQ(
      3,
      dayOfWeekTimestampWithTimezone(
          TimestampWithTimezone(987654321000, "-07:00")));
  EXPECT_EQ(
      3,
      dayOfWeekTimestampWithTimezone(
          TimestampWithTimezone(-987654321000, "-13:00")));
  EXPECT_EQ(std::nullopt, dayOfWeekTimestampWithTimezone(std::nullopt));
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
    return evaluateOnce<int64_t>("day_of_year(c0)", DATE(), date);
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
  const auto dayOfYearTimestampWithTimezone =
      [&](std::optional<TimestampWithTimezone> timestampWithTimezone) {
        return evaluateOnce<int64_t>(
            "day_of_year(c0)",
            TIMESTAMP_WITH_TIME_ZONE(),
            TimestampWithTimezone::pack(timestampWithTimezone));
      };
  EXPECT_EQ(
      365, dayOfYearTimestampWithTimezone(TimestampWithTimezone(0, "-01:00")));
  EXPECT_EQ(
      1, dayOfYearTimestampWithTimezone(TimestampWithTimezone(0, "+00:00")));
  EXPECT_EQ(
      334,
      dayOfYearTimestampWithTimezone(
          TimestampWithTimezone(123456789000, "+14:00")));
  EXPECT_EQ(
      33,
      dayOfYearTimestampWithTimezone(
          TimestampWithTimezone(-123456789000, "+03:00")));
  EXPECT_EQ(
      108,
      dayOfYearTimestampWithTimezone(
          TimestampWithTimezone(987654321000, "-07:00")));
  EXPECT_EQ(
      257,
      dayOfYearTimestampWithTimezone(
          TimestampWithTimezone(-987654321000, "-13:00")));
  EXPECT_EQ(std::nullopt, dayOfYearTimestampWithTimezone(std::nullopt));
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
    return evaluateOnce<int64_t>("year_of_week(c0)", DATE(), date);
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
  const auto yearOfWeekTimestampWithTimezone =
      [&](std::optional<TimestampWithTimezone> timestampWithTimezone) {
        return evaluateOnce<int64_t>(
            "year_of_week(c0)",
            TIMESTAMP_WITH_TIME_ZONE(),
            TimestampWithTimezone::pack(timestampWithTimezone));
      };
  EXPECT_EQ(
      1970,
      yearOfWeekTimestampWithTimezone(TimestampWithTimezone(0, "-01:00")));
  EXPECT_EQ(
      1970,
      yearOfWeekTimestampWithTimezone(TimestampWithTimezone(0, "+00:00")));
  EXPECT_EQ(
      1973,
      yearOfWeekTimestampWithTimezone(
          TimestampWithTimezone(123456789000, "+14:00")));
  EXPECT_EQ(
      1966,
      yearOfWeekTimestampWithTimezone(
          TimestampWithTimezone(-123456789000, "+03:00")));
  EXPECT_EQ(
      2001,
      yearOfWeekTimestampWithTimezone(
          TimestampWithTimezone(987654321000, "-07:00")));
  EXPECT_EQ(
      1938,
      yearOfWeekTimestampWithTimezone(
          TimestampWithTimezone(-987654321000, "-13:00")));
  EXPECT_EQ(std::nullopt, yearOfWeekTimestampWithTimezone(std::nullopt));
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
    return evaluateOnce<int64_t>("minute(c0)", DATE(), date);
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
  const auto minuteTimestampWithTimezone =
      [&](std::optional<TimestampWithTimezone> timestampWithTimezone) {
        return evaluateOnce<int64_t>(
            "minute(c0)",
            TIMESTAMP_WITH_TIME_ZONE(),
            TimestampWithTimezone::pack(timestampWithTimezone));
      };
  EXPECT_EQ(std::nullopt, minuteTimestampWithTimezone(std::nullopt));
  EXPECT_EQ(0, minuteTimestampWithTimezone(TimestampWithTimezone(0, "+00:00")));
  EXPECT_EQ(
      30, minuteTimestampWithTimezone(TimestampWithTimezone(0, "+05:30")));
  EXPECT_EQ(
      6,
      minuteTimestampWithTimezone(
          TimestampWithTimezone(4000000000000, "+00:00")));
  EXPECT_EQ(
      36,
      minuteTimestampWithTimezone(
          TimestampWithTimezone(4000000000000, "+05:30")));
  EXPECT_EQ(
      4,
      minuteTimestampWithTimezone(
          TimestampWithTimezone(998474645000, "+00:00")));
  EXPECT_EQ(
      34,
      minuteTimestampWithTimezone(
          TimestampWithTimezone(998474645000, "+05:30")));
  EXPECT_EQ(
      59, minuteTimestampWithTimezone(TimestampWithTimezone(-1000, "+00:00")));
  EXPECT_EQ(
      29, minuteTimestampWithTimezone(TimestampWithTimezone(-1000, "+05:30")));
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
    return evaluateOnce<int64_t>("second(c0)", DATE(), date);
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
  const auto secondTimestampWithTimezone =
      [&](std::optional<TimestampWithTimezone> timestampWithTimezone) {
        return evaluateOnce<int64_t>(
            "second(c0)",
            TIMESTAMP_WITH_TIME_ZONE(),
            TimestampWithTimezone::pack(timestampWithTimezone));
      };
  EXPECT_EQ(std::nullopt, secondTimestampWithTimezone(std::nullopt));
  EXPECT_EQ(0, secondTimestampWithTimezone(TimestampWithTimezone(0, "+00:00")));
  EXPECT_EQ(0, secondTimestampWithTimezone(TimestampWithTimezone(0, "+05:30")));
  EXPECT_EQ(
      40,
      secondTimestampWithTimezone(
          TimestampWithTimezone(4000000000000, "+00:00")));
  EXPECT_EQ(
      40,
      secondTimestampWithTimezone(
          TimestampWithTimezone(4000000000000, "+05:30")));
  EXPECT_EQ(
      59, secondTimestampWithTimezone(TimestampWithTimezone(-1000, "+00:00")));
  EXPECT_EQ(
      59, secondTimestampWithTimezone(TimestampWithTimezone(-1000, "+05:30")));
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
    return evaluateOnce<int64_t>("millisecond(c0)", DATE(), date);
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
  const auto millisecondTimestampWithTimezone =
      [&](std::optional<TimestampWithTimezone> timestampWithTimezone) {
        return evaluateOnce<int64_t>(
            "millisecond(c0)",
            TIMESTAMP_WITH_TIME_ZONE(),
            TimestampWithTimezone::pack(timestampWithTimezone));
      };
  EXPECT_EQ(std::nullopt, millisecondTimestampWithTimezone(std::nullopt));
  EXPECT_EQ(
      0, millisecondTimestampWithTimezone(TimestampWithTimezone(0, "+00:00")));
  EXPECT_EQ(
      0, millisecondTimestampWithTimezone(TimestampWithTimezone(0, "+05:30")));
  EXPECT_EQ(
      123,
      millisecondTimestampWithTimezone(
          TimestampWithTimezone(4000000000123, "+00:00")));
  EXPECT_EQ(
      123,
      millisecondTimestampWithTimezone(
          TimestampWithTimezone(4000000000123, "+05:30")));
  EXPECT_EQ(
      20,
      millisecondTimestampWithTimezone(TimestampWithTimezone(-980, "+00:00")));
  EXPECT_EQ(
      20,
      millisecondTimestampWithTimezone(TimestampWithTimezone(-980, "+05:30")));
}

TEST_F(DateTimeFunctionsTest, extractFromIntervalDayTime) {
  const auto millis = 5 * kMillisInDay + 7 * kMillisInHour +
      11 * kMillisInMinute + 13 * kMillisInSecond + 17;

  auto extract = [&](const std::string& unit, int64_t millis) {
    return evaluateOnce<int64_t>(
               fmt::format("{}(c0)", unit),
               INTERVAL_DAY_TIME(),
               std::optional(millis))
        .value();
  };

  EXPECT_EQ(17, extract("millisecond", millis));
  EXPECT_EQ(13, extract("second", millis));
  EXPECT_EQ(11, extract("minute", millis));
  EXPECT_EQ(7, extract("hour", millis));
  EXPECT_EQ(5, extract("day", millis));
}

TEST_F(DateTimeFunctionsTest, extractFromIntervalYearMonth) {
  const auto months = 3 * 12 + 4;

  auto extract = [&](const std::string& unit, int32_t months) {
    return evaluateOnce<int64_t>(
               fmt::format("{}(c0)", unit),
               INTERVAL_YEAR_MONTH(),
               std::optional(months))
        .value();
  };

  EXPECT_EQ(3, extract("year", months));
  EXPECT_EQ(4, extract("month", months));
}

TEST_F(DateTimeFunctionsTest, dateTrunc) {
  const auto dateTrunc = [&](const std::string& unit,
                             std::optional<Timestamp> timestamp) {
    return evaluateOnce<Timestamp>(
        fmt::format("date_trunc('{}', c0)", unit), timestamp);
  };

  disableAdjustTimestampToTimezone();

  EXPECT_EQ(std::nullopt, dateTrunc("second", std::nullopt));
  EXPECT_EQ(Timestamp(0, 0), dateTrunc("second", Timestamp(0, 0)));
  EXPECT_EQ(Timestamp(0, 0), dateTrunc("second", Timestamp(0, 123)));
  EXPECT_EQ(Timestamp(-1, 0), dateTrunc("second", Timestamp(-1, 0)));
  EXPECT_EQ(Timestamp(-1, 0), dateTrunc("second", Timestamp(-1, 123)));
  EXPECT_EQ(Timestamp(0, 0), dateTrunc("day", Timestamp(0, 123)));
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
      Timestamp(998438400, 0),
      dateTrunc("day", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(998265600, 0),
      dateTrunc("week", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(996624000, 0),
      dateTrunc("month", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(993945600, 0),
      dateTrunc("quarter", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(978307200, 0),
      dateTrunc("year", Timestamp(998'474'645, 321'001'234)));

  setQueryTimeZone("America/Los_Angeles");

  EXPECT_EQ(std::nullopt, dateTrunc("second", std::nullopt));
  EXPECT_EQ(Timestamp(0, 0), dateTrunc("second", Timestamp(0, 0)));
  EXPECT_EQ(Timestamp(0, 0), dateTrunc("second", Timestamp(0, 123)));

  EXPECT_EQ(Timestamp(-57600, 0), dateTrunc("day", Timestamp(0, 0)));
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
      Timestamp(998290800, 0),
      dateTrunc("week", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(996649200, 0),
      dateTrunc("month", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(993970800, 0),
      dateTrunc("quarter", Timestamp(998'474'645, 321'001'234)));
  EXPECT_EQ(
      Timestamp(978336000, 0),
      dateTrunc("year", Timestamp(998'474'645, 321'001'234)));

  // Check that truncation during daylight saving transition where conversions
  // may be ambiguous return the right values.
  EXPECT_EQ(
      Timestamp(1667725200, 0), dateTrunc("hour", Timestamp(1667725200, 0)));
  EXPECT_EQ(
      Timestamp(1667725200, 0), dateTrunc("minute", Timestamp(1667725200, 0)));

  setQueryTimeZone("Asia/Kolkata");

  EXPECT_EQ(std::nullopt, dateTrunc("second", std::nullopt));
  EXPECT_EQ(Timestamp(0, 0), dateTrunc("second", Timestamp(0, 0)));
  EXPECT_EQ(Timestamp(0, 0), dateTrunc("second", Timestamp(0, 123)));
  EXPECT_EQ(Timestamp(-19800, 0), dateTrunc("day", Timestamp(0, 0)));
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
      Timestamp(998245800, 0),
      dateTrunc("week", Timestamp(998'474'645, 321'001'234)));
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
    return evaluateOnce<int32_t>(
        fmt::format("date_trunc('{}', c0)", unit), DATE(), date);
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
    return evaluateOnce<int32_t>(
        fmt::format("date_trunc('{}', c0)", unit), DATE(), date);
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
    return evaluateOnce<int32_t>(
        fmt::format("date_add('{}', c0, c1)", unit),
        {INTEGER(), DATE()},
        value,
        date);
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
      parseDate("2019-03-07"), dateAdd("week", 1, parseDate("2019-02-28")));
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
      parseDate("2020-02-15"), dateAdd("week", -2, parseDate("2020-02-29")));
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
      parseTimestamp("2019-03-07 10:00:00.500"),
      dateAdd("week", 1, parseTimestamp("2019-02-28 10:00:00.500")));
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
  auto dateAdd =
      [&](std::optional<std::string> unit,
          std::optional<int32_t> value,
          std::optional<TimestampWithTimezone> timestampWithTimezone) {
        auto result = evaluateOnce<int64_t>(
            "date_add(c0, c1, c2)",
            {VARCHAR(), INTEGER(), TIMESTAMP_WITH_TIME_ZONE()},
            unit,
            value,
            TimestampWithTimezone::pack(timestampWithTimezone));
        return TimestampWithTimezone::unpack(result);
      };

  // 1970-01-01 00:00:00.000 UTC-8
  EXPECT_EQ(
      TimestampWithTimezone(432000000, "-08:00"),
      dateAdd("day", 5, TimestampWithTimezone(0, "-08:00")));

  // 2023-01-08 00:00:00.000 UTC-8
  EXPECT_EQ(
      TimestampWithTimezone(1068336000, "-08:00"),
      dateAdd("day", -7, TimestampWithTimezone(1673136000, "-08:00")));

  // 2023-01-08 00:00:00.000 UTC-8
  EXPECT_EQ(
      TimestampWithTimezone(1673135993, "-08:00"),
      dateAdd("millisecond", -7, TimestampWithTimezone(1673136000, "-08:00")));

  // 2023-01-08 00:00:00.000 UTC-8
  EXPECT_EQ(
      TimestampWithTimezone(1673136007, "-08:00"),
      dateAdd("millisecond", +7, TimestampWithTimezone(1673136000, "-08:00")));

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

  // Tests date_add() on daylight saving transition boundaries.
  //
  // Presto's semantic is to apply the delta to GMT, which means that at times
  // the observed delta may not be linear, in cases when it hits a daylight
  // savings boundary.
  auto dateAddAndCast = [&](std::optional<std::string> unit,
                            std::optional<int32_t> value,
                            std::optional<std::string> timestampString) {
    return evaluateOnce<std::string>(
        "cast(date_add(c0, c1, cast(c2 as timestamp with time zone)) as VARCHAR)",
        unit,
        value,
        timestampString);
  };

  EXPECT_EQ(
      "2024-03-10 03:50:00.000 America/Los_Angeles",
      dateAddAndCast("hour", 1, "2024-03-10 01:50:00 America/Los_Angeles"));
  EXPECT_EQ(
      "2024-11-03 01:50:00.000 America/Los_Angeles",
      dateAddAndCast("hour", 1, "2024-11-03 01:50:00 America/Los_Angeles"));
  EXPECT_EQ(
      "2024-11-03 00:50:00.000 America/Los_Angeles",
      dateAddAndCast("hour", -1, "2024-11-03 01:50:00 America/Los_Angeles"));
}

TEST_F(DateTimeFunctionsTest, dateDiffDate) {
  const auto dateDiff = [&](const std::string& unit,
                            std::optional<int32_t> date1,
                            std::optional<int32_t> date2) {
    return evaluateOnce<int64_t>(
        fmt::format("date_diff('{}', c0, c1)", unit),
        {DATE(), DATE()},
        date1,
        date2);
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
      0, dateDiff("week", parseDate("2019-02-28"), parseDate("2019-03-01")));
  EXPECT_EQ(
      2, dateDiff("week", parseDate("2019-02-28"), parseDate("2019-03-15")));
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
      0, dateDiff("week", parseDate("2020-02-29"), parseDate("2020-02-25")));
  EXPECT_EQ(
      -64, dateDiff("week", parseDate("2020-02-29"), parseDate("2018-12-02")));
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

  // Check null behaviors
  EXPECT_EQ(std::nullopt, dateDiff("second", Timestamp(1, 0), std::nullopt));
  EXPECT_EQ(std::nullopt, dateDiff("month", std::nullopt, Timestamp(0, 0)));

  // Check invalid units
  VELOX_ASSERT_THROW(
      dateDiff("invalid_unit", Timestamp(1, 0), Timestamp(0, 0)),
      "Unsupported datetime unit: invalid_unit");

  // Check for integer overflow when result unit is month or larger.
  EXPECT_EQ(
      -622191233,
      dateDiff("day", Timestamp(0, 0), Timestamp(9223372036854775, 0)));
  EXPECT_EQ(
      -88884461,
      dateDiff("week", Timestamp(0, 0), Timestamp(9223372036854775, 0)));
  VELOX_ASSERT_THROW(
      dateDiff("month", Timestamp(0, 0), Timestamp(9223372036854775, 0)),
      "Causes arithmetic overflow:");
  VELOX_ASSERT_THROW(
      dateDiff("quarter", Timestamp(0, 0), Timestamp(9223372036854775, 0)),
      "Causes arithmetic overflow:");
  VELOX_ASSERT_THROW(
      dateDiff("year", Timestamp(0, 0), Timestamp(9223372036854775, 0)),
      "Causes arithmetic overflow:");

  // Simple tests
  EXPECT_EQ(
      60 * 1000 + 500,
      dateDiff(
          "millisecond",
          parseTimestamp("2019-02-28 10:00:00.500"),
          parseTimestamp("2019-02-28 10:01:01.000")));
  EXPECT_EQ(
      60 * 60 * 24,
      dateDiff(
          "second",
          parseTimestamp("2019-02-28 10:00:00.500"),
          parseTimestamp("2019-03-01 10:00:00.500")));
  EXPECT_EQ(
      60 * 24,
      dateDiff(
          "minute",
          parseTimestamp("2019-02-28 10:00:00.500"),
          parseTimestamp("2019-03-01 10:00:00.500")));
  EXPECT_EQ(
      24,
      dateDiff(
          "hour",
          parseTimestamp("2019-02-28 10:00:00.500"),
          parseTimestamp("2019-03-01 10:00:00.500")));
  EXPECT_EQ(
      1,
      dateDiff(
          "day",
          parseTimestamp("2019-02-28 10:00:00.500"),
          parseTimestamp("2019-03-01 10:00:00.500")));
  EXPECT_EQ(
      0,
      dateDiff(
          "week",
          parseTimestamp("2019-02-28 10:00:00.500"),
          parseTimestamp("2019-03-01 10:00:00.500")));
  EXPECT_EQ(
      1,
      dateDiff(
          "week",
          parseTimestamp("2019-02-28 10:00:00.500"),
          parseTimestamp("2019-03-10 10:00:00.500")));
  EXPECT_EQ(
      12 + 1,
      dateDiff(
          "month",
          parseTimestamp("2019-02-28 10:00:00.500"),
          parseTimestamp("2020-03-28 10:00:00.500")));
  EXPECT_EQ(
      4,
      dateDiff(
          "quarter",
          parseTimestamp("2019-02-28 10:00:00.500"),
          parseTimestamp("2020-02-28 10:00:00.500")));
  EXPECT_EQ(
      1,
      dateDiff(
          "year",
          parseTimestamp("2019-02-28 10:00:00.500"),
          parseTimestamp("2020-02-28 10:00:00.500")));

  // Test for daylight saving. Daylight saving in US starts at 2021-03-14
  // 02:00:00 PST.
  // When adjust_timestamp_to_timezone is off, Daylight saving occurs in UTC
  EXPECT_EQ(
      1000 * 60 * 60 * 24,
      dateDiff(
          "millisecond",
          parseTimestamp("2021-03-14 01:00:00.000"),
          parseTimestamp("2021-03-15 01:00:00.000")));
  EXPECT_EQ(
      60 * 60 * 24,
      dateDiff(
          "second",
          parseTimestamp("2021-03-14 01:00:00.000"),
          parseTimestamp("2021-03-15 01:00:00.000")));
  EXPECT_EQ(
      60 * 24,
      dateDiff(
          "minute",
          parseTimestamp("2021-03-14 01:00:00.000"),
          parseTimestamp("2021-03-15 01:00:00.000")));
  EXPECT_EQ(
      24,
      dateDiff(
          "hour",
          parseTimestamp("2021-03-14 01:00:00.000"),
          parseTimestamp("2021-03-15 01:00:00.000")));
  EXPECT_EQ(
      1,
      dateDiff(
          "day",
          parseTimestamp("2021-03-14 01:00:00.000"),
          parseTimestamp("2021-03-15 01:00:00.000")));
  EXPECT_EQ(
      1,
      dateDiff(
          "month",
          parseTimestamp("2021-03-14 01:00:00.000"),
          parseTimestamp("2021-04-14 01:00:00.000")));
  EXPECT_EQ(
      1,
      dateDiff(
          "quarter",
          parseTimestamp("2021-03-14 01:00:00.000"),
          parseTimestamp("2021-06-14 01:00:00.000")));
  EXPECT_EQ(
      1,
      dateDiff(
          "year",
          parseTimestamp("2021-03-14 01:00:00.000"),
          parseTimestamp("2022-03-14 01:00:00.000")));

  // When adjust_timestamp_to_timezone is on, respect Daylight saving in the
  // session time zone
  setQueryTimeZone("America/Los_Angeles");

  EXPECT_EQ(
      1000 * 60 * 60 * 24,
      dateDiff(
          "millisecond",
          parseTimestamp("2021-03-14 09:00:00.000"),
          parseTimestamp("2021-03-15 09:00:00.000")));
  EXPECT_EQ(
      60 * 60 * 24,
      dateDiff(
          "second",
          parseTimestamp("2021-03-14 09:00:00.000"),
          parseTimestamp("2021-03-15 09:00:00.000")));
  EXPECT_EQ(
      60 * 24,
      dateDiff(
          "minute",
          parseTimestamp("2021-03-14 09:00:00.000"),
          parseTimestamp("2021-03-15 09:00:00.000")));
  EXPECT_EQ(
      24,
      dateDiff(
          "hour",
          parseTimestamp("2021-03-14 09:00:00.000"),
          parseTimestamp("2021-03-15 09:00:00.000")));
  EXPECT_EQ(
      1,
      dateDiff(
          "day",
          parseTimestamp("2021-03-14 09:00:00.000"),
          parseTimestamp("2021-03-15 09:00:00.000")));
  EXPECT_EQ(
      1,
      dateDiff(
          "month",
          parseTimestamp("2021-03-14 09:00:00.000"),
          parseTimestamp("2021-04-14 09:00:00.000")));
  EXPECT_EQ(
      1,
      dateDiff(
          "quarter",
          parseTimestamp("2021-03-14 09:00:00.000"),
          parseTimestamp("2021-06-14 09:00:00.000")));
  EXPECT_EQ(
      1,
      dateDiff(
          "year",
          parseTimestamp("2021-03-14 09:00:00.000"),
          parseTimestamp("2022-03-14 09:00:00.000")));

  // Test for respecting the last day of a year-month
  EXPECT_EQ(
      365 + 30,
      dateDiff(
          "day",
          parseTimestamp("2019-01-30 10:00:00.500"),
          parseTimestamp("2020-02-29 10:00:00.500")));
  EXPECT_EQ(
      12 + 1,
      dateDiff(
          "month",
          parseTimestamp("2019-01-30 10:00:00.500"),
          parseTimestamp("2020-02-29 10:00:00.500")));
  EXPECT_EQ(
      1,
      dateDiff(
          "quarter",
          parseTimestamp("2019-11-30 10:00:00.500"),
          parseTimestamp("2020-02-29 10:00:00.500")));
  EXPECT_EQ(
      10,
      dateDiff(
          "year",
          parseTimestamp("2020-02-29 10:00:00.500"),
          parseTimestamp("2030-02-28 10:00:00.500")));

  // Test for negative difference
  EXPECT_EQ(
      -60 * 60 * 24 * 1000 - 500,
      dateDiff(
          "millisecond",
          parseTimestamp("2020-03-01 00:00:00.500"),
          parseTimestamp("2020-02-29 00:00:00.000")));
  EXPECT_EQ(
      -60 * 60 * 24,
      dateDiff(
          "second",
          parseTimestamp("2020-03-01 00:00:00.500"),
          parseTimestamp("2020-02-29 00:00:00.500")));
  EXPECT_EQ(
      -60 * 24,
      dateDiff(
          "minute",
          parseTimestamp("2020-03-01 00:00:00.500"),
          parseTimestamp("2020-02-29 00:00:00.500")));
  EXPECT_EQ(
      -24,
      dateDiff(
          "hour",
          parseTimestamp("2020-03-01 00:00:00.500"),
          parseTimestamp("2020-02-29 00:00:00.500")));
  EXPECT_EQ(
      -366,
      dateDiff(
          "day",
          parseTimestamp("2020-02-29 10:00:00.500"),
          parseTimestamp("2019-02-28 10:00:00.500")));
  EXPECT_EQ(
      -12,
      dateDiff(
          "month",
          parseTimestamp("2020-02-29 10:00:00.500"),
          parseTimestamp("2019-02-28 10:00:00.500")));
  EXPECT_EQ(
      -4,
      dateDiff(
          "quarter",
          parseTimestamp("2020-02-29 10:00:00.500"),
          parseTimestamp("2019-02-28 10:00:00.500")));
  EXPECT_EQ(
      -2,
      dateDiff(
          "year",
          parseTimestamp("2020-02-29 10:00:00.500"),
          parseTimestamp("2018-02-28 10:00:00.500")));
}

TEST_F(DateTimeFunctionsTest, dateDiffTimestampWithTimezone) {
  const auto dateDiff = [&](std::optional<std::string> unit,
                            std::optional<TimestampWithTimezone> input1,
                            std::optional<TimestampWithTimezone> input2) {
    return evaluateOnce<int64_t>(
        "date_diff(c0, c1, c2)",
        {VARCHAR(), TIMESTAMP_WITH_TIME_ZONE(), TIMESTAMP_WITH_TIME_ZONE()},
        unit,
        TimestampWithTimezone::pack(input1),
        TimestampWithTimezone::pack(input2));
  };

  // Null behavior.
  EXPECT_EQ(
      std::nullopt,
      dateDiff(
          std::nullopt,
          TimestampWithTimezone(0, "UTC"),
          TimestampWithTimezone(0, "UTC")));

  EXPECT_EQ(
      std::nullopt,
      dateDiff("asdf", std::nullopt, TimestampWithTimezone(0, "UTC")));

  // timestamp1: 1970-01-01 00:00:00.000 +00:00 (0)
  // timestamp2: 2020-08-25 16:30:10.123 -08:00 (1'598'373'010'123)
  EXPECT_EQ(
      1598373010123,
      dateDiff(
          "millisecond",
          TimestampWithTimezone(0, "+00:00"),
          TimestampWithTimezone(1'598'373'010'123, "America/Los_Angeles")));
  EXPECT_EQ(
      1598373010,
      dateDiff(
          "second",
          TimestampWithTimezone(0, "+00:00"),
          TimestampWithTimezone(1'598'373'010'123, "America/Los_Angeles")));
  EXPECT_EQ(
      26639550,
      dateDiff(
          "minute",
          TimestampWithTimezone(0, "+00:00"),
          TimestampWithTimezone(1'598'373'010'123, "America/Los_Angeles")));
  EXPECT_EQ(
      443992,
      dateDiff(
          "hour",
          TimestampWithTimezone(0, "+00:00"),
          TimestampWithTimezone(1'598'373'010'123, "America/Los_Angeles")));
  EXPECT_EQ(
      18499,
      dateDiff(
          "day",
          TimestampWithTimezone(0, "+00:00"),
          TimestampWithTimezone(1'598'373'010'123, "America/Los_Angeles")));
  EXPECT_EQ(
      2642,
      dateDiff(
          "week",
          TimestampWithTimezone(0, "+00:00"),
          TimestampWithTimezone(1'598'373'010'123, "America/Los_Angeles")));
  EXPECT_EQ(
      607,
      dateDiff(
          "month",
          TimestampWithTimezone(0, "+00:00"),
          TimestampWithTimezone(1'598'373'010'123, "America/Los_Angeles")));
  EXPECT_EQ(
      202,
      dateDiff(
          "quarter",
          TimestampWithTimezone(0, "+00:00"),
          TimestampWithTimezone(1'598'373'010'123, "America/Los_Angeles")));
  EXPECT_EQ(
      50,
      dateDiff(
          "year",
          TimestampWithTimezone(0, "+00:00"),
          TimestampWithTimezone(1'598'373'010'123, "America/Los_Angeles")));

  // Test if calculations are being performed in correct zone. Presto behavior
  // is to use the zone of the first parameter. Note that that this UTC interval
  // (a, b) crosses a daylight savings boundary in PST when PST loses one hour.
  // So whenever the calculation is performed in PST, the interval is
  // effectively smaller than 24h and returns zero.
  auto a = parseTimestamp("2024-11-02 17:00:00").toMillis();
  auto b = parseTimestamp("2024-11-03 17:30:00").toMillis();
  EXPECT_EQ(
      1,
      dateDiff(
          "day",
          TimestampWithTimezone(a, "UTC"),
          TimestampWithTimezone(b, "America/Los_Angeles")));
  EXPECT_EQ(
      1,
      dateDiff(
          "day",
          TimestampWithTimezone(a, "UTC"),
          TimestampWithTimezone(b, "UTC")));

  EXPECT_EQ(
      0,
      dateDiff(
          "day",
          TimestampWithTimezone(a, "America/Los_Angeles"),
          TimestampWithTimezone(b, "UTC")));
  EXPECT_EQ(
      0,
      dateDiff(
          "day",
          TimestampWithTimezone(a, "America/Los_Angeles"),
          TimestampWithTimezone(b, "America/Los_Angeles")));
}

TEST_F(DateTimeFunctionsTest, parseDatetime) {
  const auto parseDatetime = [&](const std::optional<std::string>& input,
                                 const std::optional<std::string>& format) {
    auto result =
        evaluateOnce<int64_t>("parse_datetime(c0, c1)", input, format);
    return TimestampWithTimezone::unpack(result);
  };

  // Check null behavior.
  EXPECT_EQ(std::nullopt, parseDatetime("1970-01-01", std::nullopt));
  EXPECT_EQ(std::nullopt, parseDatetime(std::nullopt, "YYYY-MM-dd"));
  EXPECT_EQ(std::nullopt, parseDatetime(std::nullopt, std::nullopt));

  // Ensure it throws.
  VELOX_ASSERT_THROW(parseDatetime("", ""), "Invalid pattern specification");
  VELOX_ASSERT_THROW(
      parseDatetime("1234", "Y Y"), "Invalid date format: '1234'");

  // Simple tests. More exhaustive tests are provided as part of Joda's
  // implementation.
  EXPECT_EQ(
      TimestampWithTimezone(0, "UTC"),
      parseDatetime("1970-01-01", "YYYY-MM-dd"));
  EXPECT_EQ(
      TimestampWithTimezone(86400000, "UTC"),
      parseDatetime("1970-01-02", "YYYY-MM-dd"));
  EXPECT_EQ(
      TimestampWithTimezone(86400000, "UTC"),
      parseDatetime("19700102", "YYYYMMdd"));
  EXPECT_EQ(
      TimestampWithTimezone(86400000, "UTC"),
      parseDatetime("19700102", "YYYYMdd"));
  EXPECT_EQ(
      TimestampWithTimezone(86400000, "UTC"),
      parseDatetime("19700102", "YYYYMMd"));
  EXPECT_EQ(
      TimestampWithTimezone(86400000, "UTC"),
      parseDatetime("19700102", "YYYYMd"));
  EXPECT_EQ(
      TimestampWithTimezone(86400000, "UTC"),
      parseDatetime("19700102", "YYYYMd"));

  // 118860000 is the number of milliseconds since epoch at 1970-01-02
  // 09:01:00.000 UTC.
  EXPECT_EQ(
      TimestampWithTimezone(118860000, "+00:00"),
      parseDatetime("1970-01-02+09:01+00:00", "YYYY-MM-dd+HH:mmZZ"));
  EXPECT_EQ(
      TimestampWithTimezone(118860000, "-09:00"),
      parseDatetime("1970-01-02+00:01-09:00", "YYYY-MM-dd+HH:mmZZ"));
  EXPECT_EQ(
      TimestampWithTimezone(118860000, "-02:00"),
      parseDatetime("1970-01-02+07:01-02:00", "YYYY-MM-dd+HH:mmZZ"));
  EXPECT_EQ(
      TimestampWithTimezone(118860000, "+14:00"),
      parseDatetime("1970-01-02+23:01+14:00", "YYYY-MM-dd+HH:mmZZ"));
  EXPECT_EQ(
      TimestampWithTimezone(198060000, "America/Los_Angeles"),
      parseDatetime("1970-01-02+23:01 PST", "YYYY-MM-dd+HH:mm z"));
  EXPECT_EQ(
      TimestampWithTimezone(169260000, "+00:00"),
      parseDatetime("1970-01-02+23:01 GMT", "YYYY-MM-dd+HH:mm z"));

  setQueryTimeZone("Asia/Kolkata");

  // 66600000 is the number of millisecond since epoch at 1970-01-01
  // 18:30:00.000 UTC.
  EXPECT_EQ(
      TimestampWithTimezone(66600000, "Asia/Kolkata"),
      parseDatetime("1970-01-02+00:00", "YYYY-MM-dd+HH:mm"));
  EXPECT_EQ(
      TimestampWithTimezone(66600000, "-03:00"),
      parseDatetime("1970-01-01+15:30-03:00", "YYYY-MM-dd+HH:mmZZ"));

  // -66600000 is the number of millisecond since epoch at 1969-12-31
  // 05:30:00.000 UTC.
  EXPECT_EQ(
      TimestampWithTimezone(-66600000, "Asia/Kolkata"),
      parseDatetime("1969-12-31+11:00", "YYYY-MM-dd+HH:mm"));
  EXPECT_EQ(
      TimestampWithTimezone(-66600000, "+02:00"),
      parseDatetime("1969-12-31+07:30+02:00", "YYYY-MM-dd+HH:mmZZ"));

  // Joda also lets 'Z' to be UTC|UCT|GMT|GMT0.
  auto ts = TimestampWithTimezone(1708840800000, "GMT");
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
      "Invalid date format: '2024-02-25+06:00:99 PST'");
}

TEST_F(DateTimeFunctionsTest, formatDateTime) {
  const auto formatDatetime = [&](std::optional<Timestamp> timestamp,
                                  std::optional<std::string> format) {
    return evaluateOnce<std::string>(
        "format_datetime(c0, c1)", timestamp, format);
  };

  // Era test cases - 'G'
  EXPECT_EQ("AD", formatDatetime(parseTimestamp("1970-01-01"), "G"));
  EXPECT_EQ("BC", formatDatetime(parseTimestamp("-100-01-01"), "G"));
  EXPECT_EQ("BC", formatDatetime(parseTimestamp("0-01-01"), "G"));
  EXPECT_EQ("AD", formatDatetime(parseTimestamp("01-01-01"), "G"));
  EXPECT_EQ("AD", formatDatetime(parseTimestamp("01-01-01"), "GGGGGGG"));

  // Century of era test cases - 'C'
  EXPECT_EQ("19", formatDatetime(parseTimestamp("1900-01-01"), "C"));
  EXPECT_EQ("19", formatDatetime(parseTimestamp("1955-01-01"), "C"));
  EXPECT_EQ("20", formatDatetime(parseTimestamp("2000-01-01"), "C"));
  EXPECT_EQ("20", formatDatetime(parseTimestamp("2020-01-01"), "C"));
  EXPECT_EQ("0", formatDatetime(parseTimestamp("0-01-01"), "C"));
  EXPECT_EQ("1", formatDatetime(parseTimestamp("-100-01-01"), "C"));
  EXPECT_EQ("19", formatDatetime(parseTimestamp("-1900-01-01"), "C"));
  EXPECT_EQ("000019", formatDatetime(parseTimestamp("1955-01-01"), "CCCCCC"));

  // Year of era test cases - 'Y'
  EXPECT_EQ("1970", formatDatetime(parseTimestamp("1970-01-01"), "Y"));
  EXPECT_EQ("2020", formatDatetime(parseTimestamp("2020-01-01"), "Y"));
  EXPECT_EQ("1", formatDatetime(parseTimestamp("0-01-01"), "Y"));
  EXPECT_EQ("101", formatDatetime(parseTimestamp("-100-01-01"), "Y"));
  EXPECT_EQ("70", formatDatetime(parseTimestamp("1970-01-01"), "YY"));
  EXPECT_EQ("70", formatDatetime(parseTimestamp("-1970-01-01"), "YY"));
  EXPECT_EQ("1948", formatDatetime(parseTimestamp("1948-01-01"), "YYY"));
  EXPECT_EQ("1234", formatDatetime(parseTimestamp("1234-01-01"), "YYYY"));
  EXPECT_EQ(
      "0000000001", formatDatetime(parseTimestamp("01-01-01"), "YYYYYYYYYY"));

  // Day of week number - 'e'
  for (int i = 0; i < 31; i++) {
    std::string date("2022-08-" + std::to_string(i + 1));
    EXPECT_EQ(
        std::to_string(i % 7 + 1),
        formatDatetime(parseTimestamp(StringView{date}), "e"));
  }
  EXPECT_EQ("000001", formatDatetime(parseTimestamp("2022-08-01"), "eeeeee"));

  // Day of week text - 'E'
  for (int i = 0; i < 31; i++) {
    std::string date("2022-08-" + std::to_string(i + 1));
    EXPECT_EQ(
        daysShort[i % 7],
        formatDatetime(parseTimestamp(StringView{date}), "E"));
    EXPECT_EQ(
        daysShort[i % 7],
        formatDatetime(parseTimestamp(StringView{date}), "EE"));
    EXPECT_EQ(
        daysShort[i % 7],
        formatDatetime(parseTimestamp(StringView{date}), "EEE"));
    EXPECT_EQ(
        daysLong[i % 7],
        formatDatetime(parseTimestamp(StringView{date}), "EEEE"));
    EXPECT_EQ(
        daysLong[i % 7],
        formatDatetime(parseTimestamp(StringView{date}), "EEEEEEEE"));
  }

  // Year test cases - 'y'
  EXPECT_EQ("2022", formatDatetime(parseTimestamp("2022-06-20"), "y"));
  EXPECT_EQ("22", formatDatetime(parseTimestamp("2022-06-20"), "yy"));
  EXPECT_EQ("2022", formatDatetime(parseTimestamp("2022-06-20"), "yyy"));
  EXPECT_EQ("2022", formatDatetime(parseTimestamp("2022-06-20"), "yyyy"));

  EXPECT_EQ("10", formatDatetime(parseTimestamp("10-06-20"), "y"));
  EXPECT_EQ("10", formatDatetime(parseTimestamp("10-06-20"), "yy"));
  EXPECT_EQ("010", formatDatetime(parseTimestamp("10-06-20"), "yyy"));
  EXPECT_EQ("0010", formatDatetime(parseTimestamp("10-06-20"), "yyyy"));

  EXPECT_EQ("-16", formatDatetime(parseTimestamp("-16-06-20"), "y"));
  EXPECT_EQ("16", formatDatetime(parseTimestamp("-16-06-20"), "yy"));
  EXPECT_EQ("-016", formatDatetime(parseTimestamp("-16-06-20"), "yyy"));
  EXPECT_EQ("-0016", formatDatetime(parseTimestamp("-16-06-20"), "yyyy"));

  EXPECT_EQ("00", formatDatetime(parseTimestamp("-1600-06-20"), "yy"));
  EXPECT_EQ("01", formatDatetime(parseTimestamp("-1601-06-20"), "yy"));
  EXPECT_EQ("10", formatDatetime(parseTimestamp("-1610-06-20"), "yy"));

  // Day of year test cases - 'D'
  EXPECT_EQ("1", formatDatetime(parseTimestamp("2022-01-01"), "D"));
  EXPECT_EQ("10", formatDatetime(parseTimestamp("2022-01-10"), "D"));
  EXPECT_EQ("100", formatDatetime(parseTimestamp("2022-04-10"), "D"));
  EXPECT_EQ("365", formatDatetime(parseTimestamp("2022-12-31"), "D"));
  EXPECT_EQ("00100", formatDatetime(parseTimestamp("2022-04-10"), "DDDDD"));

  // Leap year case
  EXPECT_EQ("60", formatDatetime(parseTimestamp("2020-02-29"), "D"));
  EXPECT_EQ("366", formatDatetime(parseTimestamp("2020-12-31"), "D"));

  // Month of year test cases - 'M'
  for (int i = 0; i < 12; i++) {
    auto month = i + 1;
    std::string date("2022-" + std::to_string(month) + "-01");
    EXPECT_EQ(
        std::to_string(month),
        formatDatetime(parseTimestamp(StringView{date}), "M"));
    EXPECT_EQ(
        padNumber(month),
        formatDatetime(parseTimestamp(StringView{date}), "MM"));
    EXPECT_EQ(
        monthsShort[i],
        formatDatetime(parseTimestamp(StringView{date}), "MMM"));
    EXPECT_EQ(
        monthsLong[i],
        formatDatetime(parseTimestamp(StringView{date}), "MMMM"));
    EXPECT_EQ(
        monthsLong[i],
        formatDatetime(parseTimestamp(StringView{date}), "MMMMMMMM"));
  }

  // Day of month test cases - 'd'
  EXPECT_EQ("1", formatDatetime(parseTimestamp("2022-01-01"), "d"));
  EXPECT_EQ("10", formatDatetime(parseTimestamp("2022-01-10"), "d"));
  EXPECT_EQ("28", formatDatetime(parseTimestamp("2022-01-28"), "d"));
  EXPECT_EQ("31", formatDatetime(parseTimestamp("2022-01-31"), "d"));
  EXPECT_EQ(
      "00000031", formatDatetime(parseTimestamp("2022-01-31"), "dddddddd"));

  // Leap year case
  EXPECT_EQ("29", formatDatetime(parseTimestamp("2020-02-29"), "d"));

  // Halfday of day test cases - 'a'
  EXPECT_EQ("AM", formatDatetime(parseTimestamp("2022-01-01 00:00:00"), "a"));
  EXPECT_EQ("AM", formatDatetime(parseTimestamp("2022-01-01 11:59:59"), "a"));
  EXPECT_EQ("PM", formatDatetime(parseTimestamp("2022-01-01 12:00:00"), "a"));
  EXPECT_EQ("PM", formatDatetime(parseTimestamp("2022-01-01 23:59:59"), "a"));
  EXPECT_EQ(
      "AM", formatDatetime(parseTimestamp("2022-01-01 00:00:00"), "aaaaaaaa"));
  EXPECT_EQ(
      "PM", formatDatetime(parseTimestamp("2022-01-01 12:00:00"), "aaaaaaaa"));

  // Hour of halfday test cases - 'K'
  for (int i = 0; i < 24; i++) {
    std::string buildString = "2022-01-01 " + padNumber(i) + ":00:00";
    StringView date(buildString);
    EXPECT_EQ(
        std::to_string(i % 12), formatDatetime(parseTimestamp(date), "K"));
  }
  EXPECT_EQ(
      "00000011",
      formatDatetime(parseTimestamp("2022-01-01 11:00:00"), "KKKKKKKK"));

  // Clockhour of halfday test cases - 'h'
  for (int i = 0; i < 24; i++) {
    std::string buildString = "2022-01-01 " + padNumber(i) + ":00:00";
    StringView date(buildString);
    EXPECT_EQ(
        std::to_string((i + 11) % 12 + 1),
        formatDatetime(parseTimestamp(date), "h"));
  }
  EXPECT_EQ(
      "00000011",
      formatDatetime(parseTimestamp("2022-01-01 11:00:00"), "hhhhhhhh"));

  // Hour of day test cases - 'H'
  for (int i = 0; i < 24; i++) {
    std::string buildString = "2022-01-01 " + padNumber(i) + ":00:00";
    StringView date(buildString);
    EXPECT_EQ(std::to_string(i), formatDatetime(parseTimestamp(date), "H"));
  }
  EXPECT_EQ(
      "00000011",
      formatDatetime(parseTimestamp("2022-01-01 11:00:00"), "HHHHHHHH"));

  // Clockhour of day test cases - 'k'
  for (int i = 0; i < 24; i++) {
    std::string buildString = "2022-01-01 " + padNumber(i) + ":00:00";
    StringView date(buildString);
    EXPECT_EQ(
        std::to_string((i + 23) % 24 + 1),
        formatDatetime(parseTimestamp(date), "k"));
  }
  EXPECT_EQ(
      "00000011",
      formatDatetime(parseTimestamp("2022-01-01 11:00:00"), "kkkkkkkk"));

  // Minute of hour test cases - 'm'
  EXPECT_EQ("0", formatDatetime(parseTimestamp("2022-01-01 00:00:00"), "m"));
  EXPECT_EQ("1", formatDatetime(parseTimestamp("2022-01-01 01:01:00"), "m"));
  EXPECT_EQ("10", formatDatetime(parseTimestamp("2022-01-01 02:10:00"), "m"));
  EXPECT_EQ("30", formatDatetime(parseTimestamp("2022-01-01 03:30:00"), "m"));
  EXPECT_EQ("59", formatDatetime(parseTimestamp("2022-01-01 04:59:00"), "m"));
  EXPECT_EQ(
      "00000042",
      formatDatetime(parseTimestamp("2022-01-01 00:42:42"), "mmmmmmmm"));

  // Week of the year test cases - 'w'
  EXPECT_EQ("52", formatDatetime(parseTimestamp("2022-01-01 04:59:00"), "w"));
  EXPECT_EQ("52", formatDatetime(parseTimestamp("2022-01-02"), "w"));
  EXPECT_EQ("1", formatDatetime(parseTimestamp("2022-01-03"), "w"));
  EXPECT_EQ("1", formatDatetime(parseTimestamp("2024-01-01"), "w"));
  EXPECT_EQ("22", formatDatetime(parseTimestamp("1970-05-30"), "w"));

  // Second of minute test cases - 's'
  EXPECT_EQ("0", formatDatetime(parseTimestamp("2022-01-01 00:00:00"), "s"));
  EXPECT_EQ("1", formatDatetime(parseTimestamp("2022-01-01 01:01:01"), "s"));
  EXPECT_EQ("10", formatDatetime(parseTimestamp("2022-01-01 02:10:10"), "s"));
  EXPECT_EQ("30", formatDatetime(parseTimestamp("2022-01-01 03:30:30"), "s"));
  EXPECT_EQ("59", formatDatetime(parseTimestamp("2022-01-01 04:59:59"), "s"));
  EXPECT_EQ(
      "00000042",
      formatDatetime(parseTimestamp("2022-01-01 00:42:42"), "ssssssss"));

  // Fraction of second test cases - 'S'
  EXPECT_EQ("0", formatDatetime(parseTimestamp("2022-01-01 00:00:00.0"), "S"));
  EXPECT_EQ("1", formatDatetime(parseTimestamp("2022-01-01 00:00:00.1"), "S"));
  EXPECT_EQ("1", formatDatetime(parseTimestamp("2022-01-01 01:01:01.11"), "S"));
  EXPECT_EQ(
      "11", formatDatetime(parseTimestamp("2022-01-01 02:10:10.11"), "SS"));
  EXPECT_EQ(
      "9", formatDatetime(parseTimestamp("2022-01-01 03:30:30.999"), "S"));
  EXPECT_EQ(
      "99", formatDatetime(parseTimestamp("2022-01-01 03:30:30.999"), "SS"));
  EXPECT_EQ(
      "999", formatDatetime(parseTimestamp("2022-01-01 03:30:30.999"), "SSS"));
  EXPECT_EQ(
      "12300000",
      formatDatetime(parseTimestamp("2022-01-01 03:30:30.123"), "SSSSSSSS"));
  EXPECT_EQ(
      "0990",
      formatDatetime(parseTimestamp("2022-01-01 03:30:30.099"), "SSSS"));
  EXPECT_EQ(
      "0010",
      formatDatetime(parseTimestamp("2022-01-01 03:30:30.001"), "SSSS"));

  // Time zone test cases - 'z'
  setQueryTimeZone("Asia/Kolkata");
  EXPECT_EQ(
      "Asia/Kolkata", formatDatetime(parseTimestamp("1970-01-01"), "zzzz"));
  EXPECT_EQ("+05:30", formatDatetime(parseTimestamp("1970-01-01"), "ZZ"));

  // Literal test cases.
  EXPECT_EQ("hello", formatDatetime(parseTimestamp("1970-01-01"), "'hello'"));
  EXPECT_EQ("'", formatDatetime(parseTimestamp("1970-01-01"), "''"));
  EXPECT_EQ(
      "1970 ' 1970", formatDatetime(parseTimestamp("1970-01-01"), "y '' y"));
  EXPECT_EQ(
      "he'llo", formatDatetime(parseTimestamp("1970-01-01"), "'he''llo'"));
  EXPECT_EQ(
      "'he'llo'",
      formatDatetime(parseTimestamp("1970-01-01"), "'''he''llo'''"));
  EXPECT_EQ(
      "1234567890", formatDatetime(parseTimestamp("1970-01-01"), "1234567890"));
  EXPECT_EQ(
      "\\\"!@#$%^&*()-+[]{}||`~<>.,?/;:1234567890",
      formatDatetime(
          parseTimestamp("1970-01-01"),
          "\\\"!@#$%^&*()-+[]{}||`~<>.,?/;:1234567890"));

  // Multi-specifier and literal formats.
  EXPECT_EQ(
      "AD 19 1970 4 Thu 1970 1 1 1 AM 8 8 8 8 3 11 5 Asia/Kolkata",
      formatDatetime(
          parseTimestamp("1970-01-01 02:33:11.5"),
          "G C Y e E y D M d a K h H k m s S zzzz"));
  EXPECT_EQ(
      "AD 19 1970 4 asdfghjklzxcvbnmqwertyuiop Thu ' 1970 1 1 1 AM 8 8 8 8 3 11 5 1234567890\\\"!@#$%^&*()-+`~{}[];:,./ Asia/Kolkata",
      formatDatetime(
          parseTimestamp("1970-01-01 02:33:11.5"),
          "G C Y e 'asdfghjklzxcvbnmqwertyuiop' E '' y D M d a K h H k m s S 1234567890\\\"!@#$%^&*()-+`~{}[];:,./ zzzz"));

  disableAdjustTimestampToTimezone();
  EXPECT_EQ(
      "1970-01-01 00:00:00",
      formatDatetime(
          parseTimestamp("1970-01-01 00:00:00"), "YYYY-MM-dd HH:mm:ss"));

  // User format errors or unsupported errors.
  EXPECT_THROW(
      formatDatetime(parseTimestamp("1970-01-01"), "x"), VeloxUserError);
  EXPECT_THROW(
      formatDatetime(parseTimestamp("1970-01-01"), "z"), VeloxUserError);
  EXPECT_THROW(
      formatDatetime(parseTimestamp("1970-01-01"), "zz"), VeloxUserError);
  EXPECT_THROW(
      formatDatetime(parseTimestamp("1970-01-01"), "zzz"), VeloxUserError);
  EXPECT_THROW(
      formatDatetime(parseTimestamp("1970-01-01"), "q"), VeloxUserError);
  EXPECT_THROW(
      formatDatetime(parseTimestamp("1970-01-01"), "'abcd"), VeloxUserError);
}

TEST_F(DateTimeFunctionsTest, formatDateTimeTimezone) {
  const auto formatDatetimeWithTimezone =
      [&](std::optional<TimestampWithTimezone> timestampWithTimezone,
          std::optional<std::string> format) {
        return evaluateOnce<std::string>(
            "format_datetime(c0, c1)",
            {TIMESTAMP_WITH_TIME_ZONE(), VARCHAR()},
            TimestampWithTimezone::pack(timestampWithTimezone),
            format);
      };

  // UTC explicitly set.
  EXPECT_EQ(
      "1970-01-01 00:00:00",
      formatDatetimeWithTimezone(
          TimestampWithTimezone(0, "UTC"), "YYYY-MM-dd HH:mm:ss"));

  // Check that string is adjusted to the timezone set.
  EXPECT_EQ(
      "1970-01-01 05:30:00",
      formatDatetimeWithTimezone(
          TimestampWithTimezone(0, "Asia/Kolkata"), "YYYY-MM-dd HH:mm:ss"));

  EXPECT_EQ(
      "1969-12-31 16:00:00",
      formatDatetimeWithTimezone(
          TimestampWithTimezone(0, "America/Los_Angeles"),
          "YYYY-MM-dd HH:mm:ss"));

  // Make sure format_datetime() works with timezone offsets.
  EXPECT_EQ(
      "1969-12-31 16:00:00",
      formatDatetimeWithTimezone(
          TimestampWithTimezone(0, "-08:00"), "YYYY-MM-dd HH:mm:ss"));
  EXPECT_EQ(
      "1969-12-31 23:45:00",
      formatDatetimeWithTimezone(
          TimestampWithTimezone(0, "-00:15"), "YYYY-MM-dd HH:mm:ss"));
  EXPECT_EQ(
      "1970-01-01 00:07:00",
      formatDatetimeWithTimezone(
          TimestampWithTimezone(0, "+00:07"), "YYYY-MM-dd HH:mm:ss"));
}

TEST_F(DateTimeFunctionsTest, dateFormat) {
  const auto dateFormat = [&](std::optional<Timestamp> timestamp,
                              std::optional<std::string> format) {
    return evaluateOnce<std::string>("date_format(c0, c1)", timestamp, format);
  };

  // Check null behaviors.
  EXPECT_EQ(std::nullopt, dateFormat(std::nullopt, "%Y"));
  EXPECT_EQ(std::nullopt, dateFormat(Timestamp(0, 0), std::nullopt));

  // Normal cases.
  EXPECT_EQ("1970-01-01", dateFormat(parseTimestamp("1970-01-01"), "%Y-%m-%d"));
  EXPECT_EQ(
      "2000-02-29 12:00:00 AM",
      dateFormat(parseTimestamp("2000-02-29 00:00:00.987"), "%Y-%m-%d %r"));
  EXPECT_EQ(
      "2000-02-29 00:00:00.987000",
      dateFormat(
          parseTimestamp("2000-02-29 00:00:00.987"), "%Y-%m-%d %H:%i:%s.%f"));
  EXPECT_EQ(
      "-2000-02-29 00:00:00.987000",
      dateFormat(
          parseTimestamp("-2000-02-29 00:00:00.987"), "%Y-%m-%d %H:%i:%s.%f"));

  // Varying digit year cases.
  EXPECT_EQ("06", dateFormat(parseTimestamp("-6-06-20"), "%y"));
  EXPECT_EQ("-0006", dateFormat(parseTimestamp("-6-06-20"), "%Y"));
  EXPECT_EQ("16", dateFormat(parseTimestamp("-16-06-20"), "%y"));
  EXPECT_EQ("-0016", dateFormat(parseTimestamp("-16-06-20"), "%Y"));
  EXPECT_EQ("66", dateFormat(parseTimestamp("-166-06-20"), "%y"));
  EXPECT_EQ("-0166", dateFormat(parseTimestamp("-166-06-20"), "%Y"));
  EXPECT_EQ("66", dateFormat(parseTimestamp("-1666-06-20"), "%y"));
  EXPECT_EQ("00", dateFormat(parseTimestamp("-1900-06-20"), "%y"));
  EXPECT_EQ("01", dateFormat(parseTimestamp("-1901-06-20"), "%y"));
  EXPECT_EQ("10", dateFormat(parseTimestamp("-1910-06-20"), "%y"));
  EXPECT_EQ("12", dateFormat(parseTimestamp("-12-06-20"), "%y"));
  EXPECT_EQ("00", dateFormat(parseTimestamp("1900-06-20"), "%y"));
  EXPECT_EQ("01", dateFormat(parseTimestamp("1901-06-20"), "%y"));
  EXPECT_EQ("10", dateFormat(parseTimestamp("1910-06-20"), "%y"));

  // Day of week cases.
  for (int i = 0; i < 8; i++) {
    std::string date("1996-01-0" + std::to_string(i + 1));
    // Full length name.
    EXPECT_EQ(
        daysLong[i % 7], dateFormat(parseTimestamp(StringView{date}), "%W"));
    // Abbreviated name.
    EXPECT_EQ(
        daysShort[i % 7], dateFormat(parseTimestamp(StringView{date}), "%a"));
  }

  // Month cases.
  for (int i = 0; i < 12; i++) {
    std::string date("1996-" + std::to_string(i + 1) + "-01");
    std::string monthNum = std::to_string(i + 1);

    // Full length name.
    EXPECT_EQ(
        monthsLong[i % 12], dateFormat(parseTimestamp(StringView{date}), "%M"));

    // Abbreviated name.
    EXPECT_EQ(
        monthsShort[i % 12],
        dateFormat(parseTimestamp(StringView{date}), "%b"));

    // Numeric.
    EXPECT_EQ(monthNum, dateFormat(parseTimestamp(StringView{date}), "%c"));

    // Numeric 0-padded.
    if (i + 1 < 10) {
      EXPECT_EQ(
          "0" + monthNum, dateFormat(parseTimestamp(StringView{date}), "%m"));
    } else {
      EXPECT_EQ(monthNum, dateFormat(parseTimestamp(StringView{date}), "%m"));
    }
  }

  // Day of month cases.
  for (int i = 1; i <= 31; i++) {
    std::string dayOfMonth = std::to_string(i);
    std::string date("1970-01-" + dayOfMonth);
    EXPECT_EQ(dayOfMonth, dateFormat(parseTimestamp(StringView{date}), "%e"));
    if (i < 10) {
      EXPECT_EQ(
          "0" + dayOfMonth, dateFormat(parseTimestamp(StringView{date}), "%d"));
    } else {
      EXPECT_EQ(dayOfMonth, dateFormat(parseTimestamp(StringView{date}), "%d"));
    }
  }

  // Week of the year cases. Follows ISO week date format.
  //   https://en.wikipedia.org/wiki/ISO_week_date
  EXPECT_EQ("01", dateFormat(parseTimestamp("2024-01-01"), "%v"));
  EXPECT_EQ("01", dateFormat(parseTimestamp("2024-01-07"), "%v"));
  EXPECT_EQ("02", dateFormat(parseTimestamp("2024-01-08"), "%v"));
  EXPECT_EQ("52", dateFormat(parseTimestamp("2024-12-29"), "%v"));
  EXPECT_EQ("01", dateFormat(parseTimestamp("2024-12-30"), "%v"));
  EXPECT_EQ("01", dateFormat(parseTimestamp("2024-12-31"), "%v"));
  EXPECT_EQ("01", dateFormat(parseTimestamp("2025-01-01"), "%v"));
  EXPECT_EQ("53", dateFormat(parseTimestamp("2021-01-01"), "%v"));
  EXPECT_EQ("53", dateFormat(parseTimestamp("2021-01-03"), "%v"));
  EXPECT_EQ("01", dateFormat(parseTimestamp("2021-01-04"), "%v"));

  // Fraction of second cases.
  EXPECT_EQ(
      "000000", dateFormat(parseTimestamp("2022-01-01 00:00:00.0"), "%f"));
  EXPECT_EQ(
      "100000", dateFormat(parseTimestamp("2022-01-01 00:00:00.1"), "%f"));
  EXPECT_EQ(
      "110000", dateFormat(parseTimestamp("2022-01-01 01:01:01.11"), "%f"));
  EXPECT_EQ(
      "110000", dateFormat(parseTimestamp("2022-01-01 02:10:10.11"), "%f"));
  EXPECT_EQ(
      "999000", dateFormat(parseTimestamp("2022-01-01 03:30:30.999"), "%f"));
  EXPECT_EQ(
      "999000", dateFormat(parseTimestamp("2022-01-01 03:30:30.999"), "%f"));
  EXPECT_EQ(
      "999000", dateFormat(parseTimestamp("2022-01-01 03:30:30.999"), "%f"));
  EXPECT_EQ(
      "123000", dateFormat(parseTimestamp("2022-01-01 03:30:30.123"), "%f"));
  EXPECT_EQ(
      "099000", dateFormat(parseTimestamp("2022-01-01 03:30:30.099"), "%f"));
  EXPECT_EQ(
      "001000", dateFormat(parseTimestamp("2022-01-01 03:30:30.001234"), "%f"));

  // Hour cases.
  for (int i = 0; i < 24; i++) {
    std::string hour = std::to_string(i);
    int clockHour = (i + 11) % 12 + 1;
    std::string clockHourString = std::to_string(clockHour);
    std::string toBuild = "1996-01-01 " + hour + ":00:00";
    StringView date(toBuild);
    EXPECT_EQ(hour, dateFormat(parseTimestamp(date), "%k"));
    if (i < 10) {
      EXPECT_EQ("0" + hour, dateFormat(parseTimestamp(date), "%H"));
    } else {
      EXPECT_EQ(hour, dateFormat(parseTimestamp(date), "%H"));
    }

    EXPECT_EQ(clockHourString, dateFormat(parseTimestamp(date), "%l"));
    if (clockHour < 10) {
      EXPECT_EQ("0" + clockHourString, dateFormat(parseTimestamp(date), "%h"));
      EXPECT_EQ("0" + clockHourString, dateFormat(parseTimestamp(date), "%I"));
    } else {
      EXPECT_EQ(clockHourString, dateFormat(parseTimestamp(date), "%h"));
      EXPECT_EQ(clockHourString, dateFormat(parseTimestamp(date), "%I"));
    }
  }

  // Minute cases.
  for (int i = 0; i < 60; i++) {
    std::string minute = std::to_string(i);
    std::string toBuild = "1996-01-01 00:" + minute + ":00";
    StringView date(toBuild);
    if (i < 10) {
      EXPECT_EQ("0" + minute, dateFormat(parseTimestamp(date), "%i"));
    } else {
      EXPECT_EQ(minute, dateFormat(parseTimestamp(date), "%i"));
    }
  }

  // Second cases.
  for (int i = 0; i < 60; i++) {
    std::string second = std::to_string(i);
    std::string toBuild = "1996-01-01 00:00:" + second;
    StringView date(toBuild);
    if (i < 10) {
      EXPECT_EQ("0" + second, dateFormat(parseTimestamp(date), "%S"));
      EXPECT_EQ("0" + second, dateFormat(parseTimestamp(date), "%s"));
    } else {
      EXPECT_EQ(second, dateFormat(parseTimestamp(date), "%S"));
      EXPECT_EQ(second, dateFormat(parseTimestamp(date), "%s"));
    }
  }

  // Day of year cases.
  EXPECT_EQ("001", dateFormat(parseTimestamp("2022-01-01"), "%j"));
  EXPECT_EQ("010", dateFormat(parseTimestamp("2022-01-10"), "%j"));
  EXPECT_EQ("100", dateFormat(parseTimestamp("2022-04-10"), "%j"));
  EXPECT_EQ("365", dateFormat(parseTimestamp("2022-12-31"), "%j"));

  // Halfday of day cases.
  EXPECT_EQ("AM", dateFormat(parseTimestamp("2022-01-01 00:00:00"), "%p"));
  EXPECT_EQ("AM", dateFormat(parseTimestamp("2022-01-01 11:59:59"), "%p"));
  EXPECT_EQ("PM", dateFormat(parseTimestamp("2022-01-01 12:00:00"), "%p"));
  EXPECT_EQ("PM", dateFormat(parseTimestamp("2022-01-01 23:59:59"), "%p"));

  // 12-hour time cases.
  EXPECT_EQ(
      "12:00:00 AM", dateFormat(parseTimestamp("2022-01-01 00:00:00"), "%r"));
  EXPECT_EQ(
      "11:59:59 AM", dateFormat(parseTimestamp("2022-01-01 11:59:59"), "%r"));
  EXPECT_EQ(
      "12:00:00 PM", dateFormat(parseTimestamp("2022-01-01 12:00:00"), "%r"));
  EXPECT_EQ(
      "11:59:59 PM", dateFormat(parseTimestamp("2022-01-01 23:59:59"), "%r"));

  // 24-hour time cases.
  EXPECT_EQ(
      "00:00:00", dateFormat(parseTimestamp("2022-01-01 00:00:00"), "%T"));
  EXPECT_EQ(
      "11:59:59", dateFormat(parseTimestamp("2022-01-01 11:59:59"), "%T"));
  EXPECT_EQ(
      "12:00:00", dateFormat(parseTimestamp("2022-01-01 12:00:00"), "%T"));
  EXPECT_EQ(
      "23:59:59", dateFormat(parseTimestamp("2022-01-01 23:59:59"), "%T"));

  // Percent followed by non-existent specifier case.
  EXPECT_EQ("q", dateFormat(parseTimestamp("1970-01-01"), "%q"));
  EXPECT_EQ("z", dateFormat(parseTimestamp("1970-01-01"), "%z"));
  EXPECT_EQ("g", dateFormat(parseTimestamp("1970-01-01"), "%g"));

  // With timezone. Indian Standard Time (IST) UTC+5:30.
  setQueryTimeZone("Asia/Kolkata");

  EXPECT_EQ("1970-01-01", dateFormat(parseTimestamp("1970-01-01"), "%Y-%m-%d"));
  EXPECT_EQ(
      "2000-02-29 05:30:00 AM",
      dateFormat(parseTimestamp("2000-02-29 00:00:00.987"), "%Y-%m-%d %r"));
  EXPECT_EQ(
      "2000-02-29 05:30:00.987000",
      dateFormat(
          parseTimestamp("2000-02-29 00:00:00.987"), "%Y-%m-%d %H:%i:%s.%f"));
  EXPECT_EQ(
      "-2000-02-29 05:53:28.987000",
      dateFormat(
          parseTimestamp("-2000-02-29 00:00:00.987"), "%Y-%m-%d %H:%i:%s.%f"));

  // Same timestamps with a different timezone. Pacific Daylight Time (North
  // America) PDT UTC-8:00.
  setQueryTimeZone("America/Los_Angeles");

  EXPECT_EQ("1969-12-31", dateFormat(parseTimestamp("1970-01-01"), "%Y-%m-%d"));
  EXPECT_EQ(
      "2000-02-28 04:00:00 PM",
      dateFormat(parseTimestamp("2000-02-29 00:00:00.987"), "%Y-%m-%d %r"));
  EXPECT_EQ(
      "2000-02-28 16:00:00.987000",
      dateFormat(
          parseTimestamp("2000-02-29 00:00:00.987"), "%Y-%m-%d %H:%i:%s.%f"));
  EXPECT_EQ(
      "-2000-02-28 16:07:02.987000",
      dateFormat(
          parseTimestamp("-2000-02-29 00:00:00.987"), "%Y-%m-%d %H:%i:%s.%f"));

  // User format errors or unsupported errors.
  const auto timestamp = parseTimestamp("-2000-02-29 00:00:00.987");
  VELOX_ASSERT_THROW(
      dateFormat(timestamp, "%D"),
      "Date format specifier is not supported: %D");
  VELOX_ASSERT_THROW(
      dateFormat(timestamp, "%U"),
      "Date format specifier is not supported: %U");
  VELOX_ASSERT_THROW(
      dateFormat(timestamp, "%u"),
      "Date format specifier is not supported: %u");
  VELOX_ASSERT_THROW(
      dateFormat(timestamp, "%V"),
      "Date format specifier is not supported: %V");
  VELOX_ASSERT_THROW(
      dateFormat(timestamp, "%w"),
      "Date format specifier is not supported: %w");
  VELOX_ASSERT_THROW(
      dateFormat(timestamp, "%X"),
      "Date format specifier is not supported: %X");
  VELOX_ASSERT_THROW(
      dateFormat(timestamp, "%x"),
      "Date format specifier is not supported: WEEK_YEAR");
}

TEST_F(DateTimeFunctionsTest, dateFormatTimestampWithTimezone) {
  const auto dateFormatTimestampWithTimezone =
      [&](const std::string& formatString,
          std::optional<TimestampWithTimezone> timestampWithTimezone) {
        return evaluateOnce<std::string>(
            fmt::format("date_format(c0, '{}')", formatString),
            TIMESTAMP_WITH_TIME_ZONE(),
            TimestampWithTimezone::pack(timestampWithTimezone));
      };

  EXPECT_EQ(
      "1969-12-31 11:00:00 PM",
      dateFormatTimestampWithTimezone(
          "%Y-%m-%d %r", TimestampWithTimezone(0, "-01:00")));
  EXPECT_EQ(
      "1973-11-30 12:33:09 AM",
      dateFormatTimestampWithTimezone(
          "%Y-%m-%d %r", TimestampWithTimezone(123456789000, "+03:00")));
  EXPECT_EQ(
      "1966-02-01 12:26:51 PM",
      dateFormatTimestampWithTimezone(
          "%Y-%m-%d %r", TimestampWithTimezone(-123456789000, "-14:00")));
  EXPECT_EQ(
      "2001-04-19 18:25:21.000000",
      dateFormatTimestampWithTimezone(
          "%Y-%m-%d %H:%i:%s.%f",
          TimestampWithTimezone(987654321000, "+14:00")));
  EXPECT_EQ(
      "1938-09-14 23:34:39.000000",
      dateFormatTimestampWithTimezone(
          "%Y-%m-%d %H:%i:%s.%f",
          TimestampWithTimezone(-987654321000, "+04:00")));
  EXPECT_EQ(
      "70-August-22 17:55:15 PM",
      dateFormatTimestampWithTimezone(
          "%y-%M-%e %T %p", TimestampWithTimezone(20220915000, "-07:00")));
  EXPECT_EQ(
      "69-May-11 20:04:45 PM",
      dateFormatTimestampWithTimezone(
          "%y-%M-%e %T %p", TimestampWithTimezone(-20220915000, "-03:00")));
}

TEST_F(DateTimeFunctionsTest, fromIso8601Date) {
  const auto fromIso = [&](const std::string& input) {
    return evaluateOnce<int32_t, std::string>("from_iso8601_date(c0)", input);
  };

  EXPECT_EQ(0, fromIso("1970-01-01"));
  EXPECT_EQ(9, fromIso("1970-01-10"));
  EXPECT_EQ(-1, fromIso("1969-12-31"));
  EXPECT_EQ(0, fromIso("1970"));
  EXPECT_EQ(0, fromIso("1970-01"));
  EXPECT_EQ(0, fromIso("1970-1"));
  EXPECT_EQ(8, fromIso("1970-1-9"));
  EXPECT_EQ(-31, fromIso("1969-12"));
  EXPECT_EQ(-31, fromIso("1969-12-1"));
  EXPECT_EQ(-31, fromIso("1969-12-01"));
  EXPECT_EQ(-719862, fromIso("-1-2-1"));

  VELOX_ASSERT_THROW(fromIso(" 2024-01-12"), "Unable to parse date value");
  VELOX_ASSERT_THROW(fromIso("2024-01-12  "), "Unable to parse date value");
  VELOX_ASSERT_THROW(fromIso("2024 "), "Unable to parse date value");
  VELOX_ASSERT_THROW(fromIso("2024-01-xx"), "Unable to parse date value");
  VELOX_ASSERT_THROW(
      fromIso("2024-01-02T12:31:00"), "Unable to parse date value");
  VELOX_ASSERT_THROW(
      fromIso("2024-01-02 12:31:00"), "Unable to parse date value");
}

TEST_F(DateTimeFunctionsTest, fromIso8601Timestamp) {
  const auto fromIso = [&](const std::string& input) {
    auto result =
        evaluateOnce<int64_t, std::string>("from_iso8601_timestamp(c0)", input);
    return TimestampWithTimezone::unpack(result);
  };

  // Full strings with different time zones.
  const auto millis = kMillisInDay + 11 * kMillisInHour + 38 * kMillisInMinute +
      56 * kMillisInSecond + 123;
  const std::string ts = "1970-01-02T11:38:56.123";

  EXPECT_EQ(
      TimestampWithTimezone(millis + 5 * kMillisInHour, "-05:00"),
      fromIso(ts + "-05:00"));

  EXPECT_EQ(
      TimestampWithTimezone(millis - 8 * kMillisInHour, "+08:00"),
      fromIso(ts + "+08:00"));

  EXPECT_EQ(TimestampWithTimezone(millis, "UTC"), fromIso(ts + "Z"));

  EXPECT_EQ(TimestampWithTimezone(millis, "UTC"), fromIso(ts));

  // Partial strings with different session time zones.
  struct {
    const tz::TimeZone* timezone;
    int32_t offset;
  } timezones[] = {
      {tz::locateZone("America/New_York"), -5 * kMillisInHour},
      {tz::locateZone("Asia/Kolkata"),
       5 * kMillisInHour + 30 * kMillisInMinute},
  };

  for (const auto& timezone : timezones) {
    setQueryTimeZone(timezone.timezone->name());

    EXPECT_EQ(
        TimestampWithTimezone(
            kMillisInDay + 11 * kMillisInHour + 38 * kMillisInMinute +
                56 * kMillisInSecond + 123 - timezone.offset,
            timezone.timezone),
        fromIso("1970-01-02T11:38:56.123"));

    // Comma separator between seconds and microseconds.
    EXPECT_EQ(
        TimestampWithTimezone(
            kMillisInDay + 11 * kMillisInHour + 38 * kMillisInMinute +
                56 * kMillisInSecond + 123 - timezone.offset,
            timezone.timezone),
        fromIso("1970-01-02T11:38:56,123"));

    EXPECT_EQ(
        TimestampWithTimezone(
            kMillisInDay + 11 * kMillisInHour + 38 * kMillisInMinute +
                56 * kMillisInSecond - timezone.offset,
            timezone.timezone),
        fromIso("1970-01-02T11:38:56"));

    EXPECT_EQ(
        TimestampWithTimezone(
            kMillisInDay + 11 * kMillisInHour + 38 * kMillisInMinute -
                timezone.offset,
            timezone.timezone),
        fromIso("1970-01-02T11:38"));

    EXPECT_EQ(
        TimestampWithTimezone(
            kMillisInDay + 11 * kMillisInHour - timezone.offset,
            timezone.timezone),
        fromIso("1970-01-02T11"));

    // No time.
    EXPECT_EQ(
        TimestampWithTimezone(
            kMillisInDay - timezone.offset, timezone.timezone),
        fromIso("1970-01-02"));

    EXPECT_EQ(
        TimestampWithTimezone(-timezone.offset, timezone.timezone),
        fromIso("1970-01-01"));
    EXPECT_EQ(
        TimestampWithTimezone(-timezone.offset, timezone.timezone),
        fromIso("1970-01"));
    EXPECT_EQ(
        TimestampWithTimezone(-timezone.offset, timezone.timezone),
        fromIso("1970"));

    // No date.
    EXPECT_EQ(
        TimestampWithTimezone(
            11 * kMillisInHour + 38 * kMillisInMinute + 56 * kMillisInSecond +
                123 - timezone.offset,
            timezone.timezone),
        fromIso("T11:38:56.123"));

    EXPECT_EQ(
        TimestampWithTimezone(
            11 * kMillisInHour + 38 * kMillisInMinute + 56 * kMillisInSecond +
                123 - timezone.offset,
            timezone.timezone),
        fromIso("T11:38:56,123"));

    EXPECT_EQ(
        TimestampWithTimezone(
            11 * kMillisInHour + 38 * kMillisInMinute + 56 * kMillisInSecond -
                timezone.offset,
            timezone.timezone),
        fromIso("T11:38:56"));

    EXPECT_EQ(
        TimestampWithTimezone(
            11 * kMillisInHour + 38 * kMillisInMinute - timezone.offset,
            timezone.timezone),
        fromIso("T11:38"));

    EXPECT_EQ(
        TimestampWithTimezone(
            11 * kMillisInHour - timezone.offset, timezone.timezone),
        fromIso("T11"));
  }

  VELOX_ASSERT_THROW(
      fromIso("1970-01-02 11:38"),
      R"(Unable to parse timestamp value: "1970-01-02 11:38")");

  VELOX_ASSERT_THROW(
      fromIso("1970-01-02T11:38:56.123 America/New_York"),
      R"(Unable to parse timestamp value: "1970-01-02T11:38:56.123 America/New_York")");

  VELOX_ASSERT_THROW(fromIso("T"), R"(Unable to parse timestamp value: "T")");

  // Leading and trailing spaces are not allowed.
  VELOX_ASSERT_THROW(
      fromIso(" 1970-01-02"),
      R"(Unable to parse timestamp value: " 1970-01-02")");

  VELOX_ASSERT_THROW(
      fromIso("1970-01-02 "),
      R"(Unable to parse timestamp value: "1970-01-02 ")");
}

TEST_F(DateTimeFunctionsTest, dateParseMonthOfYearText) {
  auto parseAndFormat = [&](std::optional<std::string> input) {
    return evaluateOnce<std::string>(
        "date_format(date_parse(c0, '%M_%Y'), '%Y-%m')", input);
  };

  EXPECT_EQ(parseAndFormat(std::nullopt), std::nullopt);
  EXPECT_EQ(parseAndFormat("jan_2024"), "2024-01");
  EXPECT_EQ(parseAndFormat("JAN_2024"), "2024-01");
  EXPECT_EQ(parseAndFormat("january_2024"), "2024-01");
  EXPECT_EQ(parseAndFormat("JANUARY_2024"), "2024-01");

  EXPECT_EQ(parseAndFormat("feb_2024"), "2024-02");
  EXPECT_EQ(parseAndFormat("FEB_2024"), "2024-02");
  EXPECT_EQ(parseAndFormat("february_2024"), "2024-02");
  EXPECT_EQ(parseAndFormat("FEBRUARY_2024"), "2024-02");

  EXPECT_EQ(parseAndFormat("mar_2024"), "2024-03");
  EXPECT_EQ(parseAndFormat("MAR_2024"), "2024-03");
  EXPECT_EQ(parseAndFormat("march_2024"), "2024-03");
  EXPECT_EQ(parseAndFormat("MARCH_2024"), "2024-03");

  EXPECT_EQ(parseAndFormat("apr_2024"), "2024-04");
  EXPECT_EQ(parseAndFormat("APR_2024"), "2024-04");
  EXPECT_EQ(parseAndFormat("april_2024"), "2024-04");
  EXPECT_EQ(parseAndFormat("APRIL_2024"), "2024-04");

  EXPECT_EQ(parseAndFormat("may_2024"), "2024-05");
  EXPECT_EQ(parseAndFormat("MAY_2024"), "2024-05");

  EXPECT_EQ(parseAndFormat("jun_2024"), "2024-06");
  EXPECT_EQ(parseAndFormat("JUN_2024"), "2024-06");
  EXPECT_EQ(parseAndFormat("june_2024"), "2024-06");
  EXPECT_EQ(parseAndFormat("JUNE_2024"), "2024-06");

  EXPECT_EQ(parseAndFormat("jul_2024"), "2024-07");
  EXPECT_EQ(parseAndFormat("JUL_2024"), "2024-07");
  EXPECT_EQ(parseAndFormat("july_2024"), "2024-07");
  EXPECT_EQ(parseAndFormat("JULY_2024"), "2024-07");

  EXPECT_EQ(parseAndFormat("aug_2024"), "2024-08");
  EXPECT_EQ(parseAndFormat("AUG_2024"), "2024-08");
  EXPECT_EQ(parseAndFormat("august_2024"), "2024-08");
  EXPECT_EQ(parseAndFormat("AUGUST_2024"), "2024-08");

  EXPECT_EQ(parseAndFormat("sep_2024"), "2024-09");
  EXPECT_EQ(parseAndFormat("SEP_2024"), "2024-09");
  EXPECT_EQ(parseAndFormat("september_2024"), "2024-09");
  EXPECT_EQ(parseAndFormat("SEPTEMBER_2024"), "2024-09");

  EXPECT_EQ(parseAndFormat("oct_2024"), "2024-10");
  EXPECT_EQ(parseAndFormat("OCT_2024"), "2024-10");
  EXPECT_EQ(parseAndFormat("october_2024"), "2024-10");
  EXPECT_EQ(parseAndFormat("OCTOBER_2024"), "2024-10");

  EXPECT_EQ(parseAndFormat("nov_2024"), "2024-11");
  EXPECT_EQ(parseAndFormat("NOV_2024"), "2024-11");
  EXPECT_EQ(parseAndFormat("november_2024"), "2024-11");
  EXPECT_EQ(parseAndFormat("NOVEMBER_2024"), "2024-11");

  EXPECT_EQ(parseAndFormat("dec_2024"), "2024-12");
  EXPECT_EQ(parseAndFormat("DEC_2024"), "2024-12");
  EXPECT_EQ(parseAndFormat("december_2024"), "2024-12");
  EXPECT_EQ(parseAndFormat("DECEMBER_2024"), "2024-12");
}

TEST_F(DateTimeFunctionsTest, dateParse) {
  const auto dateParse = [&](std::optional<std::string> input,
                             std::optional<std::string> format) {
    return evaluateOnce<Timestamp>("date_parse(c0, c1)", input, format);
  };

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

  VELOX_ASSERT_THROW(dateParse("", "%y+"), "Invalid date format: ''");
  VELOX_ASSERT_THROW(dateParse("1", "%y+"), "Invalid date format: '1'");
  VELOX_ASSERT_THROW(dateParse("116", "%y+"), "Invalid date format: '116'");
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

  // Allow leading and trailing spaces.
  EXPECT_EQ(18297, dateFunction("   2020-02-05"));
  EXPECT_EQ(18297, dateFunction("  2020-02-05   "));
  EXPECT_EQ(18297, dateFunction("2020-02-05 "));

  // Illegal date format.
  VELOX_ASSERT_THROW(
      dateFunction("2020-02-05 11:00"),
      "Unable to parse date value: \"2020-02-05 11:00\"");
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
        auto r1 = evaluateWithTimestampWithTimezone<int32_t>(
            "date(c0)", timestamp, timeZoneName);
        auto r2 = evaluateWithTimestampWithTimezone<int32_t>(
            "cast(c0 as date)", timestamp, timeZoneName);
        EXPECT_EQ(r1, r2);
        return r1;
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

  // Trying to convert a very large timestamp should fail as velox/external/date
  // can't convert past year 2037. Note that the correct result here should be
  // 376358 ('3000-06-08'), and not 376357 ('3000-06-07').
  VELOX_ASSERT_THROW(
      castDateTest(Timestamp(32517359891, 0)), "Unable to convert timezone");

  // Ensure timezone conversion failures leak through try().
  const auto tryTest = [&](std::optional<Timestamp> timestamp) {
    return evaluateOnce<int32_t>("try(cast(c0 as date))", timestamp);
  };
  VELOX_ASSERT_RUNTIME_THROW(
      tryTest(Timestamp(32517359891, 0)), "Unable to convert timezone");
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
    Timestamp ts = parseTimestamp(time);
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
    Timestamp ts = parseTimestamp(time);
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
  auto runAndCompare = [&](const std::string& expr,
                           const RowVectorPtr& inputs,
                           const VectorPtr& expectedResult) {
    auto actual = evaluate(expr, inputs);
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

  auto expectedEq = makeFlatVector<bool>({true, false, false});
  runAndCompare("c0 = c1", inputs, expectedEq);

  auto expectedNeq = makeFlatVector<bool>({false, true, true});
  runAndCompare("c0 != c1", inputs, expectedNeq);

  auto expectedLt = makeFlatVector<bool>({false, true, false});
  runAndCompare("c0 < c1", inputs, expectedLt);

  auto expectedGt = makeFlatVector<bool>({false, false, true});
  runAndCompare("c0 > c1", inputs, expectedGt);

  auto expectedLte = makeFlatVector<bool>({true, true, false});
  runAndCompare("c0 <= c1", inputs, expectedLte);

  auto expectedGte = makeFlatVector<bool>({true, false, true});
  runAndCompare("c0 >= c1", inputs, expectedGte);

  auto expectedBetween = makeNullableFlatVector<bool>({true, true, false});
  runAndCompare("c0 between c0 and c1", inputs, expectedBetween);
}

TEST_F(DateTimeFunctionsTest, castDateToTimestamp) {
  const int64_t kSecondsInDay = kMillisInDay / 1'000;
  const auto castDateToTimestamp = [&](const std::optional<int32_t> date) {
    return evaluateOnce<Timestamp>("cast(c0 AS timestamp)", DATE(), date);
  };

  EXPECT_EQ(Timestamp(0, 0), castDateToTimestamp(parseDate("1970-01-01")));
  EXPECT_EQ(
      Timestamp(kSecondsInDay, 0),
      castDateToTimestamp(parseDate("1970-01-02")));
  EXPECT_EQ(
      Timestamp(2 * kSecondsInDay, 0),
      castDateToTimestamp(parseDate("1970-01-03")));
  EXPECT_EQ(
      Timestamp(18297 * kSecondsInDay, 0),
      castDateToTimestamp(parseDate("2020-02-05")));
  EXPECT_EQ(
      Timestamp(-1 * kSecondsInDay, 0),
      castDateToTimestamp(parseDate("1969-12-31")));
  EXPECT_EQ(
      Timestamp(-18297 * kSecondsInDay, 0),
      castDateToTimestamp(parseDate("1919-11-28")));

  const auto tz = "America/Los_Angeles";
  const auto kTimezoneOffset = 8 * kMillisInHour / 1'000;
  setQueryTimeZone(tz);
  EXPECT_EQ(
      Timestamp(kTimezoneOffset, 0),
      castDateToTimestamp(parseDate("1970-01-01")));
  EXPECT_EQ(
      Timestamp(kSecondsInDay + kTimezoneOffset, 0),
      castDateToTimestamp(parseDate("1970-01-02")));
  EXPECT_EQ(
      Timestamp(2 * kSecondsInDay + kTimezoneOffset, 0),
      castDateToTimestamp(parseDate("1970-01-03")));
  EXPECT_EQ(
      Timestamp(18297 * kSecondsInDay + kTimezoneOffset, 0),
      castDateToTimestamp(parseDate("2020-02-05")));
  EXPECT_EQ(
      Timestamp(-1 * kSecondsInDay + kTimezoneOffset, 0),
      castDateToTimestamp(parseDate("1969-12-31")));
  EXPECT_EQ(
      Timestamp(-18297 * kSecondsInDay + kTimezoneOffset, 0),
      castDateToTimestamp(parseDate("1919-11-28")));

  disableAdjustTimestampToTimezone();
  EXPECT_EQ(Timestamp(0, 0), castDateToTimestamp(parseDate("1970-01-01")));
  EXPECT_EQ(
      Timestamp(kSecondsInDay, 0),
      castDateToTimestamp(parseDate("1970-01-02")));
  EXPECT_EQ(
      Timestamp(2 * kSecondsInDay, 0),
      castDateToTimestamp(parseDate("1970-01-03")));
  EXPECT_EQ(
      Timestamp(18297 * kSecondsInDay, 0),
      castDateToTimestamp(parseDate("2020-02-05")));
  EXPECT_EQ(
      Timestamp(-1 * kSecondsInDay, 0),
      castDateToTimestamp(parseDate("1969-12-31")));
  EXPECT_EQ(
      Timestamp(-18297 * kSecondsInDay, 0),
      castDateToTimestamp(parseDate("1919-11-28")));
}

TEST_F(DateTimeFunctionsTest, lastDayOfMonthDate) {
  const auto lastDayFunc = [&](const std::optional<int32_t> date) {
    return evaluateOnce<int32_t>("last_day_of_month(c0)", DATE(), date);
  };

  const auto lastDay = [&](const StringView& dateStr) {
    return lastDayFunc(parseDate(dateStr));
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
    return lastDayFunc(parseTimestamp(dateStr));
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
  const auto lastDayOfMonthTimestampWithTimezone =
      [&](std::optional<TimestampWithTimezone> timestampWithTimezone) {
        return evaluateOnce<int32_t>(
            "last_day_of_month(c0)",
            TIMESTAMP_WITH_TIME_ZONE(),
            TimestampWithTimezone::pack(timestampWithTimezone));
      };
  EXPECT_EQ(
      parseDate("1970-01-31"),
      lastDayOfMonthTimestampWithTimezone(TimestampWithTimezone(0, "+00:00")));
  EXPECT_EQ(
      parseDate("1969-12-31"),
      lastDayOfMonthTimestampWithTimezone(TimestampWithTimezone(0, "-02:00")));
  EXPECT_EQ(
      parseDate("2008-02-29"),
      lastDayOfMonthTimestampWithTimezone(
          TimestampWithTimezone(1201881600000, "+02:00")));
  EXPECT_EQ(
      parseDate("2008-01-31"),
      lastDayOfMonthTimestampWithTimezone(
          TimestampWithTimezone(1201795200000, "-02:00")));
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

TEST_F(DateTimeFunctionsTest, toISO8601Date) {
  const auto toISO8601 = [&](const char* dateString) {
    return evaluateOnce<std::string>(
        "to_iso8601(c0)", DATE(), std::make_optional(parseDate(dateString)));
  };

  EXPECT_EQ("1970-01-01", toISO8601("1970-01-01"));
  EXPECT_EQ("2020-02-05", toISO8601("2020-02-05"));
  EXPECT_EQ("1919-11-28", toISO8601("1919-11-28"));
  EXPECT_EQ("4653-07-01", toISO8601("4653-07-01"));
  EXPECT_EQ("1844-10-14", toISO8601("1844-10-14"));
  EXPECT_EQ("0001-01-01", toISO8601("1-01-01"));
  EXPECT_EQ("9999-12-31", toISO8601("9999-12-31"));
  EXPECT_EQ("872343-04-19", toISO8601("872343-04-19"));
  EXPECT_EQ("-3492-10-05", toISO8601("-3492-10-05"));
  EXPECT_EQ("-0653-07-12", toISO8601("-653-07-12"));
}

TEST_F(DateTimeFunctionsTest, toISO8601Timestamp) {
  const auto toIso = [&](const char* timestamp) {
    return evaluateOnce<std::string>(
        "to_iso8601(c0)", std::make_optional(parseTimestamp(timestamp)));
  };
  disableAdjustTimestampToTimezone();
  EXPECT_EQ("2024-11-01T10:00:00.000+00:00", toIso("2024-11-01 10:00"));
  EXPECT_EQ("2024-11-04T10:00:00.000+00:00", toIso("2024-11-04 10:00"));
  EXPECT_EQ("2024-11-04T15:05:34.100+00:00", toIso("2024-11-04 15:05:34.1"));
  EXPECT_EQ("2024-11-04T15:05:34.123+00:00", toIso("2024-11-04 15:05:34.123"));
  EXPECT_EQ("0022-11-01T10:00:00.000+00:00", toIso("22-11-01 10:00"));

  setQueryTimeZone("America/Los_Angeles");
  EXPECT_EQ("2024-11-01T03:00:00.000-07:00", toIso("2024-11-01 10:00"));

  setQueryTimeZone("America/New_York");
  EXPECT_EQ("2024-11-01T06:00:00.000-04:00", toIso("2024-11-01 10:00"));
  EXPECT_EQ("2024-11-04T05:00:00.000-05:00", toIso("2024-11-04 10:00"));
  EXPECT_EQ("2024-11-04T10:05:34.100-05:00", toIso("2024-11-04 15:05:34.1"));
  EXPECT_EQ("2024-11-04T10:05:34.123-05:00", toIso("2024-11-04 15:05:34.123"));
  EXPECT_EQ("0022-11-01T05:03:58.000-04:56:02", toIso("22-11-01 10:00"));

  setQueryTimeZone("Asia/Kathmandu");
  EXPECT_EQ("2024-11-01T15:45:00.000+05:45", toIso("2024-11-01 10:00"));
  EXPECT_EQ("0022-11-01T15:41:16.000+05:41:16", toIso("22-11-01 10:00"));
  EXPECT_EQ("0022-11-01T15:41:16.000+05:41:16", toIso("22-11-01 10:00"));
}

TEST_F(DateTimeFunctionsTest, toISO8601TimestampWithTimezone) {
  const auto toIso = [&](const char* timestamp, const char* timezone) {
    const auto* timeZone = tz::locateZone(timezone);
    auto ts = parseTimestamp(timestamp);
    ts.toGMT(*timeZone);

    return evaluateOnce<std::string>(
        "to_iso8601(c0)",
        TIMESTAMP_WITH_TIME_ZONE(),
        std::make_optional(pack(ts.toMillis(), timeZone->id())));
  };

  EXPECT_EQ(
      "2024-11-01T10:00:00.000-04:00",
      toIso("2024-11-01 10:00", "America/New_York"));
  EXPECT_EQ(
      "2024-11-04T10:00:45.120-05:00",
      toIso("2024-11-04 10:00:45.12", "America/New_York"));
  EXPECT_EQ(
      "0022-11-01T10:00:00.000-04:56:02",
      toIso("22-11-01 10:00", "America/New_York"));

  EXPECT_EQ(
      "2024-11-01T10:00:00.000+05:45",
      toIso("2024-11-01 10:00", "Asia/Kathmandu"));
  EXPECT_EQ(
      "0022-11-01T10:00:00.000+05:41:16",
      toIso("22-11-01 10:00", "Asia/Kathmandu"));
}

TEST_F(DateTimeFunctionsTest, atTimezoneTest) {
  const auto at_timezone = [&](std::optional<int64_t> timestampWithTimezone,
                               std::optional<std::string> targetTimezone) {
    return evaluateOnce<int64_t>(
        "at_timezone(c0, c1)",
        {TIMESTAMP_WITH_TIME_ZONE(), VARCHAR()},
        timestampWithTimezone,
        targetTimezone);
  };

  EXPECT_EQ(
      at_timezone(
          pack(1500101514, tz::getTimeZoneID("Asia/Kathmandu")),
          "America/Boise"),
      pack(1500101514, tz::getTimeZoneID("America/Boise")));

  EXPECT_EQ(
      at_timezone(
          pack(1500101514, tz::getTimeZoneID("America/Boise")),
          "Europe/London"),
      pack(1500101514, tz::getTimeZoneID("Europe/London")));

  EXPECT_EQ(
      at_timezone(
          pack(1500321297, tz::getTimeZoneID("Canada/Yukon")),
          "Australia/Melbourne"),
      pack(1500321297, tz::getTimeZoneID("Australia/Melbourne")));

  EXPECT_EQ(
      at_timezone(
          pack(1500321297, tz::getTimeZoneID("Atlantic/Bermuda")),
          "Pacific/Fiji"),
      pack(1500321297, tz::getTimeZoneID("Pacific/Fiji")));

  EXPECT_EQ(
      at_timezone(
          pack(1500321297, tz::getTimeZoneID("Atlantic/Bermuda")),
          std::nullopt),
      std::nullopt);

  EXPECT_EQ(at_timezone(std::nullopt, "Pacific/Fiji"), std::nullopt);
}

TEST_F(DateTimeFunctionsTest, toMilliseconds) {
  EXPECT_EQ(
      123,
      evaluateOnce<int64_t>(
          "to_milliseconds(c0)",
          INTERVAL_DAY_TIME(),
          std::optional<int64_t>(123)));
}
