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

#include "velox/functions/prestosql/tests/FunctionBaseTest.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/type/Date.h"
#include "velox/type/Timestamp.h"
#include "velox/type/tz/TimeZoneMap.h"

using namespace facebook::velox;

class DateTimeFunctionsTest : public functions::test::FunctionBaseTest {
 protected:
  void setQueryTimeZone(const std::string& timeZone) {
    queryCtx_->setConfigOverridesUnsafe({
        {core::QueryConfig::kSessionTimezone, timeZone},
        {core::QueryConfig::kAdjustTimestampToTimezone, "true"},
    });
  }

 public:
  struct TimestampWithTimezone {
    TimestampWithTimezone(int64_t milliSeconds, int16_t timezoneId)
        : milliSeconds_(milliSeconds), timezoneId_(timezoneId) {}

    int64_t milliSeconds_{0};
    int16_t timezoneId_{0};
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

    auto rowVector = resultVector->as<RowVector>();
    return TimestampWithTimezone{
        rowVector->children()[0]->as<SimpleVector<int64_t>>()->valueAt(0),
        rowVector->children()[1]->as<SimpleVector<int16_t>>()->valueAt(0)};
  }
};

bool operator==(
    const DateTimeFunctionsTest::TimestampWithTimezone& a,
    const DateTimeFunctionsTest::TimestampWithTimezone& b) {
  return a.milliSeconds_ == b.milliSeconds_ && a.timezoneId_ == b.timezoneId_;
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
    const int64_t tzid =
        strcmp(tz, "+00:00") == 0 ? 0 : util::getTimeZoneID(tz);
    return evaluateOnce<double>(
        "to_unixtime(c0)",
        makeRowVector({makeRowVector({
            makeNullableFlatVector<int64_t>({timestamp}),
            makeNullableFlatVector<int16_t>({tzid}),
        })}));
  };

  // 1639426440000 is milliseconds (from PrestoDb '2021-12-13+20:14+00:00').
  EXPECT_EQ(0, toUnixtimeWTZ(0, "+00:00"));
  EXPECT_EQ(1639426440, toUnixtimeWTZ(1639426440000, "+00:00"));
  EXPECT_EQ(1639415640, toUnixtimeWTZ(1639426440000, "+03:00"));
  EXPECT_EQ(1639412040, toUnixtimeWTZ(1639426440000, "+04:00"));
  EXPECT_EQ(1639451640, toUnixtimeWTZ(1639426440000, "-07:00"));
  EXPECT_EQ(1639426500, toUnixtimeWTZ(1639426440000, "-00:01"));
  EXPECT_EQ(1639426380, toUnixtimeWTZ(1639426440000, "+00:01"));
  EXPECT_EQ(1639476840, toUnixtimeWTZ(1639426440000, "-14:00"));
  EXPECT_EQ(1639376040, toUnixtimeWTZ(1639426440000, "+14:00"));
}

TEST_F(DateTimeFunctionsTest, fromUnixtimeRountTrip) {
  const auto testRoundTrip = [&](std::optional<Timestamp> t) {
    auto r = evaluateOnce<Timestamp>("from_unixtime(to_unixtime(c0))", t);
    EXPECT_EQ(r->getSeconds(), t->getSeconds()) << "at " << t->toString();
    EXPECT_NEAR(r->getNanos(), t->getNanos(), 1'000) << "at " << t->toString();
    return r;
  };

  testRoundTrip(Timestamp(0, 0));
  testRoundTrip(Timestamp(-1, 9000));
  testRoundTrip(Timestamp(4000000000, 0));
  testRoundTrip(Timestamp(4000000000, 123000000));
  testRoundTrip(Timestamp(-9999999999, 100000000));
  testRoundTrip(Timestamp(998474645, 321000000));
  testRoundTrip(Timestamp(998423705, 321000000));
}

TEST_F(DateTimeFunctionsTest, fromUnixtimeWithTimeZone) {
  vector_size_t size = 37;

  auto unixtimeAt = [](vector_size_t row) -> double {
    return 1631800000.12345 + row * 11;
  };

  auto unixtimes = makeFlatVector<double>(size, unixtimeAt);

  // Constant timezone parameter.
  {
    auto result = evaluate<RowVector>(
        "from_unixtime(c0, '+01:00')", makeRowVector({unixtimes}));
    ASSERT_TRUE(isTimestampWithTimeZoneType(result->type()));

    auto expected = makeRowVector({
        makeFlatVector<int64_t>(
            size, [&](auto row) { return unixtimeAt(row) * 1'000; }),
        makeConstant((int16_t)900, size),
    });
    assertEqualVectors(expected, result);
  }

  // Variable timezone parameter.
  {
    std::vector<int16_t> timezoneIds = {900, 960, 1020, 1080, 1140};
    std::vector<std::string> timezoneNames = {
        "+01:00", "+02:00", "+03:00", "+04:00", "+05:00"};

    auto timezones = makeFlatVector<StringView>(
        size, [&](auto row) { return StringView(timezoneNames[row % 5]); });

    auto result = evaluate<RowVector>(
        "from_unixtime(c0, c1)", makeRowVector({unixtimes, timezones}));
    ASSERT_TRUE(isTimestampWithTimeZoneType(result->type()));

    auto expected = makeRowVector({
        makeFlatVector<int64_t>(
            size, [&](auto row) { return unixtimeAt(row) * 1'000; }),
        makeFlatVector<int16_t>(
            size, [&](auto row) { return timezoneIds[row % 5]; }),
    });
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
  EXPECT_EQ(Timestamp(-1, 9000), fromUnixtime(-0.999991));
  EXPECT_EQ(Timestamp(4000000000, 0), fromUnixtime(4000000000));
  EXPECT_EQ(
      Timestamp(9'223'372'036'854'775, 807'000'000), fromUnixtime(3.87111e+37));
  // double(123000000) to uint64_t conversion returns 123000144.
  EXPECT_EQ(Timestamp(4000000000, 123000144), fromUnixtime(4000000000.123));
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
  EXPECT_EQ(1969, year(Timestamp(-1, 12300000000)));
  EXPECT_EQ(2096, year(Timestamp(4000000000, 0)));
  EXPECT_EQ(2096, year(Timestamp(4000000000, 123000000)));
  EXPECT_EQ(2001, year(Timestamp(998474645, 321000000)));
  EXPECT_EQ(2001, year(Timestamp(998423705, 321000000)));
}

TEST_F(DateTimeFunctionsTest, yearDate) {
  const auto year = [&](std::optional<Date> date) {
    return evaluateOnce<int64_t>("year(c0)", date);
  };
  EXPECT_EQ(std::nullopt, year(std::nullopt));
  EXPECT_EQ(1970, year(Date(0)));
  EXPECT_EQ(1969, year(Date(-1)));
  EXPECT_EQ(2020, year(Date(18262)));
  EXPECT_EQ(1920, year(Date(-18262)));
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
  EXPECT_EQ(4, quarter(Timestamp(-1, 12300000000)));
  EXPECT_EQ(4, quarter(Timestamp(4000000000, 0)));
  EXPECT_EQ(4, quarter(Timestamp(4000000000, 123000000)));
  EXPECT_EQ(2, quarter(Timestamp(990000000, 321000000)));
  EXPECT_EQ(3, quarter(Timestamp(998423705, 321000000)));
}

TEST_F(DateTimeFunctionsTest, quarterDate) {
  const auto quarter = [&](std::optional<Date> date) {
    return evaluateOnce<int64_t>("quarter(c0)", date);
  };
  EXPECT_EQ(std::nullopt, quarter(std::nullopt));
  EXPECT_EQ(1, quarter(Date(0)));
  EXPECT_EQ(4, quarter(Date(-1)));
  EXPECT_EQ(4, quarter(Date(-40)));
  EXPECT_EQ(2, quarter(Date(110)));
  EXPECT_EQ(3, quarter(Date(200)));
  EXPECT_EQ(1, quarter(Date(18262)));
  EXPECT_EQ(1, quarter(Date(-18262)));
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
  EXPECT_EQ(12, month(Timestamp(-1, 12300000000)));
  EXPECT_EQ(10, month(Timestamp(4000000000, 0)));
  EXPECT_EQ(10, month(Timestamp(4000000000, 123000000)));
  EXPECT_EQ(8, month(Timestamp(998474645, 321000000)));
  EXPECT_EQ(8, month(Timestamp(998423705, 321000000)));
}

TEST_F(DateTimeFunctionsTest, monthDate) {
  const auto month = [&](std::optional<Date> date) {
    return evaluateOnce<int64_t>("month(c0)", date);
  };
  EXPECT_EQ(std::nullopt, month(std::nullopt));
  EXPECT_EQ(1, month(Date(0)));
  EXPECT_EQ(12, month(Date(-1)));
  EXPECT_EQ(11, month(Date(-40)));
  EXPECT_EQ(2, month(Date(40)));
  EXPECT_EQ(1, month(Date(18262)));
  EXPECT_EQ(1, month(Date(-18262)));
}

TEST_F(DateTimeFunctionsTest, hour) {
  const auto hour = [&](std::optional<Timestamp> date) {
    return evaluateOnce<int64_t>("hour(c0)", date);
  };
  EXPECT_EQ(std::nullopt, hour(std::nullopt));
  EXPECT_EQ(0, hour(Timestamp(0, 0)));
  EXPECT_EQ(23, hour(Timestamp(-1, 9000)));
  EXPECT_EQ(7, hour(Timestamp(4000000000, 0)));
  EXPECT_EQ(7, hour(Timestamp(4000000000, 123000000)));
  EXPECT_EQ(10, hour(Timestamp(998474645, 321000000)));
  EXPECT_EQ(19, hour(Timestamp(998423705, 321000000)));

  setQueryTimeZone("Pacific/Apia");

  EXPECT_EQ(std::nullopt, hour(std::nullopt));
  EXPECT_EQ(13, hour(Timestamp(0, 0)));
  EXPECT_EQ(12, hour(Timestamp(-1, 12300000000)));
  // Disabled for now because the TZ for Pacific/Apia in 2096 varies between
  // systems.
  // EXPECT_EQ(21, hour(Timestamp(4000000000, 0)));
  // EXPECT_EQ(21, hour(Timestamp(4000000000, 123000000)));
  EXPECT_EQ(23, hour(Timestamp(998474645, 321000000)));
  EXPECT_EQ(8, hour(Timestamp(998423705, 321000000)));
}

TEST_F(DateTimeFunctionsTest, hourDate) {
  const auto hour = [&](std::optional<Date> date) {
    return evaluateOnce<int64_t>("hour(c0)", date);
  };
  EXPECT_EQ(std::nullopt, hour(std::nullopt));
  EXPECT_EQ(0, hour(Date(0)));
  EXPECT_EQ(0, hour(Date(-1)));
  EXPECT_EQ(0, hour(Date(-40)));
  EXPECT_EQ(0, hour(Date(40)));
  EXPECT_EQ(0, hour(Date(18262)));
  EXPECT_EQ(0, hour(Date(-18262)));
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
  const auto day = [&](std::optional<Date> date) {
    return evaluateOnce<int64_t>("day_of_month(c0)", date);
  };
  EXPECT_EQ(std::nullopt, day(std::nullopt));
  EXPECT_EQ(1, day(Date(0)));
  EXPECT_EQ(31, day(Date(-1)));
  EXPECT_EQ(22, day(Date(-40)));
  EXPECT_EQ(10, day(Date(40)));
  EXPECT_EQ(1, day(Date(18262)));
  EXPECT_EQ(2, day(Date(-18262)));
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
  const auto day = [&](std::optional<Date> date) {
    return evaluateOnce<int64_t>("day_of_week(c0)", date);
  };
  EXPECT_EQ(std::nullopt, day(std::nullopt));
  EXPECT_EQ(4, day(Date(0)));
  EXPECT_EQ(3, day(Date(-1)));
  EXPECT_EQ(6, day(Date(-40)));
  EXPECT_EQ(2, day(Date(40)));
  EXPECT_EQ(3, day(Date(18262)));
  EXPECT_EQ(5, day(Date(-18262)));
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
  const auto day = [&](std::optional<Date> date) {
    return evaluateOnce<int64_t>("day_of_year(c0)", date);
  };
  EXPECT_EQ(std::nullopt, day(std::nullopt));
  EXPECT_EQ(1, day(Date(0)));
  EXPECT_EQ(365, day(Date(-1)));
  EXPECT_EQ(326, day(Date(-40)));
  EXPECT_EQ(41, day(Date(40)));
  EXPECT_EQ(1, day(Date(18262)));
  EXPECT_EQ(2, day(Date(-18262)));
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
  const auto yow = [&](std::optional<Date> date) {
    return evaluateOnce<int64_t>("year_of_week(c0)", date);
  };
  EXPECT_EQ(std::nullopt, yow(std::nullopt));
  EXPECT_EQ(1970, yow(Date(0)));
  EXPECT_EQ(1970, yow(Date(-1)));
  EXPECT_EQ(1969, yow(Date(-4)));
  EXPECT_EQ(1970, yow(Date(-3)));
  EXPECT_EQ(1970, yow(Date(365)));
  EXPECT_EQ(1970, yow(Date(367)));
  EXPECT_EQ(1971, yow(Date(368)));
  EXPECT_EQ(2021, yow(Date(18900)));
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
  const auto minute = [&](std::optional<Date> date) {
    return evaluateOnce<int64_t>("minute(c0)", date);
  };
  EXPECT_EQ(std::nullopt, minute(std::nullopt));
  EXPECT_EQ(0, minute(Date(0)));
  EXPECT_EQ(0, minute(Date(-1)));
  EXPECT_EQ(0, minute(Date(-40)));
  EXPECT_EQ(0, minute(Date(40)));
  EXPECT_EQ(0, minute(Date(18262)));
  EXPECT_EQ(0, minute(Date(-18262)));
}

TEST_F(DateTimeFunctionsTest, second) {
  const auto second = [&](std::optional<Timestamp> timestamp) {
    return evaluateOnce<int64_t>("second(c0)", timestamp);
  };
  EXPECT_EQ(std::nullopt, second(std::nullopt));
  EXPECT_EQ(0, second(Timestamp(0, 0)));
  EXPECT_EQ(40, second(Timestamp(4000000000, 0)));
  EXPECT_EQ(59, second(Timestamp(-1, 123000000)));
  EXPECT_EQ(59, second(Timestamp(-1, 12300000000)));
}

TEST_F(DateTimeFunctionsTest, secondDate) {
  const auto second = [&](std::optional<Date> date) {
    return evaluateOnce<int64_t>("second(c0)", date);
  };
  EXPECT_EQ(std::nullopt, second(std::nullopt));
  EXPECT_EQ(0, second(Date(0)));
  EXPECT_EQ(0, second(Date(-1)));
  EXPECT_EQ(0, second(Date(-40)));
  EXPECT_EQ(0, second(Date(40)));
  EXPECT_EQ(0, second(Date(18262)));
  EXPECT_EQ(0, second(Date(-18262)));
}

TEST_F(DateTimeFunctionsTest, millisecond) {
  const auto millisecond = [&](std::optional<Timestamp> timestamp) {
    return evaluateOnce<int64_t>("millisecond(c0)", timestamp);
  };
  EXPECT_EQ(std::nullopt, millisecond(std::nullopt));
  EXPECT_EQ(0, millisecond(Timestamp(0, 0)));
  EXPECT_EQ(0, millisecond(Timestamp(4000000000, 0)));
  EXPECT_EQ(123, millisecond(Timestamp(-1, 123000000)));
  EXPECT_EQ(12300, millisecond(Timestamp(-1, 12300000000)));
}

TEST_F(DateTimeFunctionsTest, millisecondDate) {
  const auto millisecond = [&](std::optional<Date> date) {
    return evaluateOnce<int64_t>("millisecond(c0)", date);
  };
  EXPECT_EQ(std::nullopt, millisecond(std::nullopt));
  EXPECT_EQ(0, millisecond(Date(0)));
  EXPECT_EQ(0, millisecond(Date(-1)));
  EXPECT_EQ(0, millisecond(Date(-40)));
  EXPECT_EQ(0, millisecond(Date(40)));
  EXPECT_EQ(0, millisecond(Date(18262)));
  EXPECT_EQ(0, millisecond(Date(-18262)));
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
                             std::optional<Date> date) {
    return evaluateOnce<Date>(fmt::format("date_trunc('{}', c0)", unit), date);
  };

  EXPECT_EQ(std::nullopt, dateTrunc("year", std::nullopt));

  EXPECT_EQ(Date(0), dateTrunc("day", Date(0)));
  EXPECT_EQ(Date(0), dateTrunc("year", Date(0)));
  EXPECT_EQ(Date(0), dateTrunc("quarter", Date(0)));
  EXPECT_EQ(Date(0), dateTrunc("month", Date(0)));
  EXPECT_THROW(dateTrunc("second", Date(0)), VeloxUserError);
  EXPECT_THROW(dateTrunc("minute", Date(0)), VeloxUserError);
  EXPECT_THROW(dateTrunc("hour", Date(0)), VeloxUserError);

  // Date(18297) is 2020-02-04
  EXPECT_EQ(Date(18297), dateTrunc("day", Date(18297)));
  EXPECT_EQ(Date(18293), dateTrunc("month", Date(18297)));
  EXPECT_EQ(Date(18262), dateTrunc("quarter", Date(18297)));
  EXPECT_EQ(Date(18262), dateTrunc("year", Date(18297)));
  EXPECT_THROW(dateTrunc("second", Date(18297)), VeloxUserError);
  EXPECT_THROW(dateTrunc("minute", Date(18297)), VeloxUserError);
  EXPECT_THROW(dateTrunc("hour", Date(18297)), VeloxUserError);

  // Date(-18297) is 1919-11-27
  EXPECT_EQ(Date(-18297), dateTrunc("day", Date(-18297)));
  EXPECT_EQ(Date(-18324), dateTrunc("month", Date(-18297)));
  EXPECT_EQ(Date(-18355), dateTrunc("quarter", Date(-18297)));
  EXPECT_EQ(Date(-18628), dateTrunc("year", Date(-18297)));
  EXPECT_THROW(dateTrunc("second", Date(-18297)), VeloxUserError);
  EXPECT_THROW(dateTrunc("minute", Date(-18297)), VeloxUserError);
  EXPECT_THROW(dateTrunc("hour", Date(-18297)), VeloxUserError);
}

TEST_F(DateTimeFunctionsTest, parseDatetime) {
  // Check null behavior.
  EXPECT_EQ(std::nullopt, parseDatetime("1970-01-01", std::nullopt));
  EXPECT_EQ(std::nullopt, parseDatetime(std::nullopt, "YYYY-MM-dd"));
  EXPECT_EQ(std::nullopt, parseDatetime(std::nullopt, std::nullopt));

  // Ensure it throws.
  EXPECT_THROW(parseDatetime("", ""), VeloxUserError);
  EXPECT_THROW(parseDatetime("1234", "Y Y"), VeloxUserError);

  // Simple tests. More exhaustive tests are provided as part of Joda's
  // implementation.
  EXPECT_EQ(
      TimestampWithTimezone(0, 0), parseDatetime("1970-01-01", "YYYY-MM-dd"));
  EXPECT_EQ(
      TimestampWithTimezone(86400000, 0),
      parseDatetime("1970-01-02", "YYYY-MM-dd"));

  EXPECT_EQ(
      TimestampWithTimezone(86400000, util::getTimeZoneID("-09:00")),
      parseDatetime("1970-01-02+00:00-09:00", "YYYY-MM-dd+HH:mmZZ"));

  setQueryTimeZone("Asia/Kolkata");

  EXPECT_EQ(
      TimestampWithTimezone(86400000, util::getTimeZoneID("Asia/Kolkata")),
      parseDatetime("1970-01-02+00:00", "YYYY-MM-dd+HH:mm"));
}
