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
#include <string_view>
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/type/Date.h"
#include "velox/type/Timestamp.h"
#include "velox/type/TimestampConversion.h"
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

  std::string padNumber(int number) {
    return number < 10 ? "0" + std::to_string(number) : std::to_string(number);
  }

  void setQueryTimeZone(const std::string& timeZone) {
    queryCtx_->setConfigOverridesUnsafe({
        {core::QueryConfig::kSessionTimezone, timeZone},
        {core::QueryConfig::kAdjustTimestampToTimezone, "true"},
    });
  }

  void disableAdjustTimestampToTimezone() {
    queryCtx_->setConfigOverridesUnsafe({
        {core::QueryConfig::kAdjustTimestampToTimezone, "false"},
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

  std::optional<TimestampWithTimezone> dateParse(
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
    auto rowVector = resultVector->as<RowVector>();
    return TimestampWithTimezone{
        rowVector->children()[0]->as<SimpleVector<int64_t>>()->valueAt(0),
        rowVector->children()[1]->as<SimpleVector<int16_t>>()->valueAt(0)};
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

  template <typename T>
  std::optional<T> evaluateWithTimestampWithTimezone(
      const std::string& expression,
      std::optional<int64_t> timestamp,
      const std::optional<std::string>& timeZoneName) {
    if (!timestamp.has_value() || !timeZoneName.has_value()) {
      return evaluateOnce<T>(
          expression,
          makeRowVector({makeRowVector(
              {
                  makeNullableFlatVector<int64_t>({std::nullopt}),
                  makeNullableFlatVector<int16_t>({std::nullopt}),
              },
              [](vector_size_t /*row*/) { return true; })}));
    }

    const std::optional<int64_t> tzid =
        util::getTimeZoneID(timeZoneName.value());
    return evaluateOnce<T>(
        expression,
        makeRowVector({makeRowVector({
            makeNullableFlatVector<int64_t>({timestamp}),
            makeNullableFlatVector<int16_t>({tzid}),
        })}));
  }

  VectorPtr evaluateWithTimestampWithTimezone(
      const std::string& expression,
      std::optional<int64_t> timestamp,
      const std::optional<std::string>& timeZoneName) {
    if (!timestamp.has_value() || !timeZoneName.has_value()) {
      return evaluate(
          expression,
          makeRowVector({makeRowVector(
              {
                  makeNullableFlatVector<int64_t>({std::nullopt}),
                  makeNullableFlatVector<int16_t>({std::nullopt}),
              },
              [](vector_size_t /*row*/) { return true; })}));
    }

    const std::optional<int64_t> tzid =
        util::getTimeZoneID(timeZoneName.value());
    return evaluate(
        expression,
        makeRowVector({makeRowVector({
            makeNullableFlatVector<int64_t>({timestamp}),
            makeNullableFlatVector<int16_t>({tzid}),
        })}));
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
    const int64_t tzid = util::getTimeZoneID(tz);
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

TEST_F(DateTimeFunctionsTest, plusMinusDateIntervalDayTime) {
  const auto plus = [&](std::optional<Date> date,
                        std::optional<IntervalDayTime> interval) {
    return evaluateOnce<Date>("c0 + c1", date, interval);
  };
  const auto minus = [&](std::optional<Date> date,
                         std::optional<IntervalDayTime> interval) {
    return evaluateOnce<Date>("c0 - c1", date, interval);
  };

  const IntervalDayTime oneDay(kMillisInDay * 1);
  const IntervalDayTime tenDays(kMillisInDay * 10);
  const IntervalDayTime partDay(kMillisInHour * 25);
  const Date baseDate(20000);
  const Date baseDatePlus1(20000 + 1);
  const Date baseDatePlus10(20000 + 10);
  const Date baseDateMinus1(20000 - 1);
  const Date baseDateMinus10(20000 - 10);

  EXPECT_EQ(std::nullopt, plus(std::nullopt, oneDay));
  EXPECT_EQ(std::nullopt, plus(Date(10000), std::nullopt));
  EXPECT_EQ(baseDatePlus1, plus(baseDate, oneDay));
  EXPECT_EQ(baseDatePlus10, plus(baseDate, tenDays));
  EXPECT_EQ(std::nullopt, minus(std::nullopt, oneDay));
  EXPECT_EQ(std::nullopt, minus(Date(10000), std::nullopt));
  EXPECT_EQ(baseDateMinus1, minus(baseDate, oneDay));
  EXPECT_EQ(baseDateMinus10, minus(baseDate, tenDays));

  EXPECT_THROW(plus(baseDate, partDay), VeloxUserError);
  EXPECT_THROW(minus(baseDate, partDay), VeloxUserError);
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

TEST_F(DateTimeFunctionsTest, dateTruncTimestampWithTimezone) {
  const auto evaluateDateTrunc = [&](const std::string& truncUnit,
                                     int64_t inputTimestamp,
                                     const std::string& timeZone,
                                     int64_t expectedTimestamp) {
    assertEqualVectors(
        makeRowVector(
            {makeNullableFlatVector<int64_t>({expectedTimestamp}),
             makeNullableFlatVector<int16_t>({util::getTimeZoneID(timeZone)})}),
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
        evaluate<RowVector>(
            "parse_datetime(c0, 'YYYY-MM-dd+HH:mm:ssZZ')",
            makeRowVector({makeNullableFlatVector<StringView>(
                {StringView{expectedTimestamp}})})),
        evaluate<RowVector>(
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
                           std::optional<Date> date) {
    return evaluateOnce<Date>(
        fmt::format("date_add('{}', c0, c1)", unit), value, date);
  };

  const auto parseDate = [&](const std::string& strDate) -> Date {
    Date result;
    parseTo(strDate, result);
    return result;
  };

  // Check null behaviors
  EXPECT_EQ(std::nullopt, dateAdd("day", 1, std::nullopt));
  EXPECT_EQ(std::nullopt, dateAdd("month", std::nullopt, Date(0)));

  // Check invalid units
  EXPECT_THROW(dateAdd("millisecond", 1, Date(0)), VeloxUserError);
  EXPECT_THROW(dateAdd("second", 1, Date(0)), VeloxUserError);
  EXPECT_THROW(dateAdd("minute", 1, Date(0)), VeloxUserError);
  EXPECT_THROW(dateAdd("hour", 1, Date(0)), VeloxUserError);
  EXPECT_THROW(dateAdd("invalid_unit", 1, Date(0)), VeloxUserError);

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
  EXPECT_THROW(dateAdd("invalid_unit", 1, Timestamp(0, 0)), VeloxUserError);

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

TEST_F(DateTimeFunctionsTest, dateDiffDate) {
  const auto dateDiff = [&](const std::string& unit,
                            std::optional<Date> date1,
                            std::optional<Date> date2) {
    return evaluateOnce<int64_t>(
        fmt::format("date_diff('{}', c0, c1)", unit), date1, date2);
  };

  const auto parseDate = [&](const std::string& strDate) -> Date {
    Date result;
    parseTo(strDate, result);
    return result;
  };

  // Check null behaviors
  EXPECT_EQ(std::nullopt, dateDiff("day", Date(1), std::nullopt));
  EXPECT_EQ(std::nullopt, dateDiff("month", std::nullopt, Date(0)));

  // Check invalid units
  EXPECT_THROW(dateDiff("millisecond", Date(1), Date(0)), VeloxUserError);
  EXPECT_THROW(dateDiff("second", Date(1), Date(0)), VeloxUserError);
  EXPECT_THROW(dateDiff("minute", Date(1), Date(0)), VeloxUserError);
  EXPECT_THROW(dateDiff("hour", Date(1), Date(0)), VeloxUserError);
  EXPECT_THROW(dateDiff("invalid_unit", Date(1), Date(0)), VeloxUserError);

  // Simple tests
  EXPECT_EQ(
      1, dateDiff("day", parseDate("2019-02-28"), parseDate("2019-03-01")));
  EXPECT_EQ(
      13, dateDiff("month", parseDate("2019-02-28"), parseDate("2020-03-28")));
  EXPECT_EQ(
      4, dateDiff("quarter", parseDate("2019-02-28"), parseDate("2020-02-28")));
  EXPECT_EQ(
      1, dateDiff("year", parseDate("2019-02-28"), parseDate("2020-02-28")));

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
  EXPECT_THROW(
      dateDiff("invalid_unit", Timestamp(1, 0), Timestamp(0, 0)),
      VeloxUserError);

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
    StringView date("2022-08-" + std::to_string(i + 1));
    EXPECT_EQ(
        std::to_string(i % 7 + 1),
        formatDatetime(fromTimestampString(date), "e"));
  }
  EXPECT_EQ(
      "000001", formatDatetime(fromTimestampString("2022-08-01"), "eeeeee"));

  // day of week text - 'E'
  for (int i = 0; i < 31; i++) {
    StringView date("2022-08-" + std::to_string(i + 1));
    EXPECT_EQ(daysShort[i % 7], formatDatetime(fromTimestampString(date), "E"));
    EXPECT_EQ(
        daysShort[i % 7], formatDatetime(fromTimestampString(date), "EE"));
    EXPECT_EQ(
        daysShort[i % 7], formatDatetime(fromTimestampString(date), "EEE"));
    EXPECT_EQ(
        daysLong[i % 7], formatDatetime(fromTimestampString(date), "EEEE"));
    EXPECT_EQ(
        daysLong[i % 7], formatDatetime(fromTimestampString(date), "EEEEEEEE"));
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
    StringView date("2022-" + std::to_string(month) + "-01");
    EXPECT_EQ(
        std::to_string(month), formatDatetime(fromTimestampString(date), "M"));
    EXPECT_EQ(
        padNumber(month), formatDatetime(fromTimestampString(date), "MM"));
    EXPECT_EQ(monthsShort[i], formatDatetime(fromTimestampString(date), "MMM"));
    EXPECT_EQ(monthsLong[i], formatDatetime(fromTimestampString(date), "MMMM"));
    EXPECT_EQ(
        monthsLong[i], formatDatetime(fromTimestampString(date), "MMMMMMMM"));
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
      "AD 19 1970 4 Thu 1970 1 1 1 AM 2 2 2 2 33 11 5 Asia/Kolkata",
      formatDatetime(
          fromTimestampString("1970-01-01 02:33:11.5"),
          "G C Y e E y D M d a K h H k m s S zzzz"));
  EXPECT_EQ(
      "AD 19 1970 4 asdfghjklzxcvbnmqwertyuiop Thu ' 1970 1 1 1 AM 2 2 2 2 33 11 5 1234567890\\\"!@#$%^&*()-+`~{}[];:,./ Asia/Kolkata",
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
    StringView date("1996-01-0" + std::to_string(i + 1));
    // Full length name
    EXPECT_EQ(daysLong[i % 7], dateFormat(fromTimestampString(date), "%W"));
    // Abbreviated name
    EXPECT_EQ(daysShort[i % 7], dateFormat(fromTimestampString(date), "%a"));
  }

  // Month cases
  for (int i = 0; i < 12; i++) {
    StringView date("1996-" + std::to_string(i + 1) + "-01");
    std::string monthNum = std::to_string(i + 1);
    // Full length name
    EXPECT_EQ(monthsLong[i % 12], dateFormat(fromTimestampString(date), "%M"));
    // Abbreviated name
    EXPECT_EQ(monthsShort[i % 12], dateFormat(fromTimestampString(date), "%b"));
    // Numeric
    EXPECT_EQ(monthNum, dateFormat(fromTimestampString(date), "%c"));
    // Numeric 0-padded
    if (i + 1 < 10) {
      EXPECT_EQ("0" + monthNum, dateFormat(fromTimestampString(date), "%m"));
    } else {
      EXPECT_EQ(monthNum, dateFormat(fromTimestampString(date), "%m"));
    }
  }

  // Day of month cases
  for (int i = 1; i <= 31; i++) {
    std::string dayOfMonth = std::to_string(i);
    StringView date("1970-01-" + dayOfMonth);
    EXPECT_EQ(dayOfMonth, dateFormat(util::fromTimestampString(date), "%e"));
    if (i < 10) {
      EXPECT_EQ(
          "0" + dayOfMonth, dateFormat(util::fromTimestampString(date), "%d"));
    } else {
      EXPECT_EQ(dayOfMonth, dateFormat(util::fromTimestampString(date), "%d"));
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

  // With timezone
  setQueryTimeZone("Asia/Kolkata");
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

TEST_F(DateTimeFunctionsTest, dateParse) {
  // Check null behavior.
  EXPECT_EQ(std::nullopt, dateParse("1970-01-01", std::nullopt));
  EXPECT_EQ(std::nullopt, dateParse(std::nullopt, "YYYY-MM-dd"));
  EXPECT_EQ(std::nullopt, dateParse(std::nullopt, std::nullopt));

  // Simple tests. More exhaustive tests are provided in DateTimeFormatterTest.
  EXPECT_EQ(
      TimestampWithTimezone(86400000, 0), dateParse("1970-01-02", "%Y-%m-%d"));
  EXPECT_EQ(TimestampWithTimezone(0, 0), dateParse("1970-01-01", "%Y-%m-%d"));
  EXPECT_EQ(
      TimestampWithTimezone(86400000, 0), dateParse("19700102", "%Y%m%d"));

  // Tests for differing query timezones
  // 118860000 is the number of milliseconds since epoch at 1970-01-02
  // 09:01:00.000 UTC.
  EXPECT_EQ(
      TimestampWithTimezone(118860000, 0),
      dateParse("1970-01-02+09:01", "%Y-%m-%d+%H:%i"));

  setQueryTimeZone("America/Los_Angeles");
  EXPECT_EQ(
      TimestampWithTimezone(
          118860000, util::getTimeZoneID("America/Los_Angeles")),
      dateParse("1970-01-02+01:01", "%Y-%m-%d+%H:%i"));

  setQueryTimeZone("America/Noronha");
  EXPECT_EQ(
      TimestampWithTimezone(118860000, util::getTimeZoneID("America/Noronha")),
      dateParse("1970-01-02+07:01", "%Y-%m-%d+%H:%i"));

  setQueryTimeZone("+04:00");
  EXPECT_EQ(
      TimestampWithTimezone(118860000, util::getTimeZoneID("+04:00")),
      dateParse("1970-01-02+13:01", "%Y-%m-%d+%H:%i"));

  setQueryTimeZone("Asia/Kolkata");
  // 66600000 is the number of millisecond since epoch at 1970-01-01
  // 18:30:00.000 UTC.
  EXPECT_EQ(
      TimestampWithTimezone(66600000, util::getTimeZoneID("Asia/Kolkata")),
      dateParse("1970-01-02+00:00", "%Y-%m-%d+%H:%i"));

  // -66600000 is the number of millisecond since epoch at 1969-12-31
  // 05:30:00.000 UTC.
  EXPECT_EQ(
      TimestampWithTimezone(-66600000, util::getTimeZoneID("Asia/Kolkata")),
      dateParse("1969-12-31+11:00", "%Y-%m-%d+%H:%i"));
}
