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

#include <gtest/gtest.h>
#include <random>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/external/date/tz.h"
#include "velox/type/Timestamp.h"

namespace facebook::velox {
namespace {

std::string timestampToString(
    Timestamp ts,
    const TimestampToStringOptions& options) {
  std::tm tm;
  Timestamp::epochToCalendarUtc(ts.getSeconds(), tm);
  std::string result;
  result.resize(getMaxStringLength(options));
  const auto view = Timestamp::tsToStringView(ts, options, result.data());
  result.resize(view.size());
  return result;
}

TEST(TimestampTest, fromMillisAndMicros) {
  int64_t positiveSecond = 10'000;
  int64_t negativeSecond = -10'000;
  uint64_t nano = 123 * 1'000'000;

  Timestamp ts1(positiveSecond, nano);
  int64_t positiveMillis = positiveSecond * 1'000 + nano / 1'000'000;
  int64_t positiveMicros = positiveSecond * 1'000'000 + nano / 1000;
  EXPECT_EQ(ts1, Timestamp::fromMillis(positiveMillis));
  EXPECT_EQ(ts1, Timestamp::fromMicros(positiveMicros));
  EXPECT_EQ(ts1, Timestamp::fromMillis(ts1.toMillis()));
  EXPECT_EQ(ts1, Timestamp::fromMicros(ts1.toMicros()));

  Timestamp ts2(negativeSecond, nano);
  int64_t negativeMillis = negativeSecond * 1'000 + nano / 1'000'000;
  int64_t negativeMicros = negativeSecond * 1'000'000 + nano / 1000;
  EXPECT_EQ(ts2, Timestamp::fromMillis(negativeMillis));
  EXPECT_EQ(ts2, Timestamp::fromMicros(negativeMicros));
  EXPECT_EQ(ts2, Timestamp::fromMillis(ts2.toMillis()));
  EXPECT_EQ(ts2, Timestamp::fromMicros(ts2.toMicros()));

  Timestamp ts3(negativeSecond, 0);
  EXPECT_EQ(ts3, Timestamp::fromMillis(negativeSecond * 1'000));
  EXPECT_EQ(ts3, Timestamp::fromMicros(negativeSecond * 1'000'000));
  EXPECT_EQ(ts3, Timestamp::fromMillis(ts3.toMillis()));
  EXPECT_EQ(ts3, Timestamp::fromMicros(ts3.toMicros()));
}

TEST(TimestampTest, fromNanos) {
  int64_t positiveSecond = 10'000;
  int64_t negativeSecond = -10'000;
  uint64_t nano = 123'456'789;

  Timestamp ts1(positiveSecond, nano);
  int64_t positiveNanos = positiveSecond * 1'000'000'000 + nano;
  EXPECT_EQ(ts1, Timestamp::fromNanos(positiveNanos));
  EXPECT_EQ(ts1, Timestamp::fromNanos(ts1.toNanos()));

  Timestamp ts2(negativeSecond, nano);
  int64_t negativeNanos = negativeSecond * 1'000'000'000 + nano;
  EXPECT_EQ(ts2, Timestamp::fromNanos(negativeNanos));
  EXPECT_EQ(ts2, Timestamp::fromNanos(ts2.toNanos()));

  Timestamp ts3(negativeSecond, 0);
  EXPECT_EQ(ts3, Timestamp::fromNanos(negativeSecond * 1'000'000'000));
  EXPECT_EQ(ts3, Timestamp::fromNanos(ts3.toNanos()));
}

TEST(TimestampTest, arithmeticOverflow) {
  int64_t positiveSecond = Timestamp::kMaxSeconds;
  int64_t negativeSecond = Timestamp::kMinSeconds;
  uint64_t nano = Timestamp::kMaxNanos;

  Timestamp ts1(positiveSecond, nano);
  VELOX_ASSERT_THROW(
      ts1.toMillis(),
      fmt::format(
          "Could not convert Timestamp({}, {}) to milliseconds",
          positiveSecond,
          nano));
  VELOX_ASSERT_THROW(
      ts1.toMicros(),
      fmt::format(
          "Could not convert Timestamp({}, {}) to microseconds",
          positiveSecond,
          nano));
  VELOX_ASSERT_THROW(
      ts1.toNanos(),
      fmt::format(
          "Could not convert Timestamp({}, {}) to nanoseconds",
          positiveSecond,
          nano));

  Timestamp ts2(negativeSecond, 0);
  VELOX_ASSERT_THROW(
      ts2.toMillis(),
      fmt::format(
          "Could not convert Timestamp({}, {}) to milliseconds",
          negativeSecond,
          0));
  VELOX_ASSERT_THROW(
      ts2.toMicros(),
      fmt::format(
          "Could not convert Timestamp({}, {}) to microseconds",
          negativeSecond,
          0));
  VELOX_ASSERT_THROW(
      ts2.toNanos(),
      fmt::format(
          "Could not convert Timestamp({}, {}) to nanoseconds",
          negativeSecond,
          0));
  ASSERT_NO_THROW(Timestamp::minMillis().toMillis());
  ASSERT_NO_THROW(Timestamp::maxMillis().toMillis());
}

TEST(TimestampTest, toAppend) {
  std::string tsStringZeroValue;
  toAppend(Timestamp(0, 0), &tsStringZeroValue);
  EXPECT_EQ("1970-01-01T00:00:00.000000000", tsStringZeroValue);

  std::string tsStringCommonValue;
  toAppend(Timestamp(946729316, 0), &tsStringCommonValue);
  EXPECT_EQ("2000-01-01T12:21:56.000000000", tsStringCommonValue);

  std::string tsStringFarInFuture;
  toAppend(Timestamp(94668480000, 0), &tsStringFarInFuture);
  EXPECT_EQ("4969-12-04T00:00:00.000000000", tsStringFarInFuture);

  std::string tsStringWithNanos;
  toAppend(Timestamp(946729316, 123), &tsStringWithNanos);
  EXPECT_EQ("2000-01-01T12:21:56.000000123", tsStringWithNanos);

  EXPECT_EQ(
      "2000-01-01T00:00:00.000000000",
      folly::to<std::string>(Timestamp(946684800, 0)));
  EXPECT_EQ(
      "2000-01-01T12:21:56.000000123",
      folly::to<std::string>(Timestamp(946729316, 123)));
  EXPECT_EQ(
      "1970-01-01T02:01:06.000000000",
      folly::to<std::string>(Timestamp(7266, 0)));
  EXPECT_EQ(
      "2000-01-01T12:21:56.129900000",
      folly::to<std::string>(Timestamp(946729316, 129900000)));
}

TEST(TimestampTest, now) {
  using namespace std::chrono;

  auto now = Timestamp::now();

  auto expectedEpochSecs =
      duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
  auto expectedEpochMs =
      duration_cast<milliseconds>(system_clock::now().time_since_epoch())
          .count();

  EXPECT_GE(expectedEpochSecs, now.getSeconds());
  EXPECT_GE(expectedEpochMs, now.toMillis());
}

DEBUG_ONLY_TEST(TimestampTest, invalidInput) {
  constexpr uint64_t kUint64Max = std::numeric_limits<uint64_t>::max();
  constexpr int64_t kInt64Min = std::numeric_limits<int64_t>::min();
  constexpr int64_t kInt64Max = std::numeric_limits<int64_t>::max();
  // Seconds invalid range.
  VELOX_ASSERT_THROW(Timestamp(kInt64Min, 1), "Timestamp seconds out of range");
  VELOX_ASSERT_THROW(Timestamp(kInt64Max, 1), "Timestamp seconds out of range");
  VELOX_ASSERT_THROW(
      Timestamp(Timestamp::kMinSeconds - 1, 1),
      "Timestamp seconds out of range");
  VELOX_ASSERT_THROW(
      Timestamp(Timestamp::kMaxSeconds + 1, 1),
      "Timestamp seconds out of range");

  // Nanos invalid range.
  VELOX_ASSERT_THROW(Timestamp(1, kUint64Max), "Timestamp nanos out of range");
  VELOX_ASSERT_THROW(
      Timestamp(1, Timestamp::kMaxNanos + 1), "Timestamp nanos out of range");
}

TEST(TimestampTest, toString) {
  auto kMin = Timestamp(Timestamp::kMinSeconds, 0);
  auto kMax = Timestamp(Timestamp::kMaxSeconds, Timestamp::kMaxNanos);
  EXPECT_EQ("-292275055-05-16T16:47:04.000000000", kMin.toString());
  EXPECT_EQ("292278994-08-17T07:12:55.999999999", kMax.toString());
  EXPECT_EQ(
      "1-01-01T05:17:32.000000000", Timestamp(-62135577748, 0).toString());
  EXPECT_EQ(
      "-224876953-12-19T16:58:03.000000000",
      Timestamp(-7096493348463717, 0).toString());
  EXPECT_EQ(
      "-1-11-29T19:33:20.000000000", Timestamp(-62170000000, 0).toString());
}

TEST(TimestampTest, toStringPrestoCastBehavior) {
  auto kMin = Timestamp(Timestamp::kMinSeconds, 0);
  auto kMax = Timestamp(Timestamp::kMaxSeconds, Timestamp::kMaxNanos);
  TimestampToStringOptions options = {
      .precision = TimestampToStringOptions::Precision::kMilliseconds,
      .zeroPaddingYear = true,
      .dateTimeSeparator = ' ',
  };
  EXPECT_EQ("-292275055-05-16 16:47:04.000", kMin.toString(options));
  EXPECT_EQ("292278994-08-17 07:12:55.999", kMax.toString(options));
  EXPECT_EQ(
      "0001-01-01 05:17:32.000", Timestamp(-62135577748, 0).toString(options));
  EXPECT_EQ(
      "0000-03-24 13:20:00.000", Timestamp(-62160000000, 0).toString(options));
  EXPECT_EQ(
      "-224876953-12-19 16:58:03.000",
      Timestamp(-7096493348463717, 0).toString(options));
  EXPECT_EQ(
      "-0001-11-29 19:33:20.000", Timestamp(-62170000000, 0).toString(options));
}

namespace {

uint64_t randomSeed() {
  if (const char* env = getenv("VELOX_TEST_USE_RANDOM_SEED")) {
    auto seed = std::random_device{}();
    LOG(INFO) << "Random seed: " << seed;
    return seed;
  } else {
    return 42;
  }
}

std::string toStringAlt(
    const Timestamp& t,
    TimestampToStringOptions::Precision precision) {
  auto seconds = t.getSeconds();
  std::tm tmValue;
  VELOX_CHECK_NOT_NULL(gmtime_r((const time_t*)&seconds, &tmValue));
  auto width = static_cast<int>(precision);
  auto value = precision == TimestampToStringOptions::Precision::kMilliseconds
      ? t.getNanos() / 1'000'000
      : t.getNanos();
  std::ostringstream oss;
  oss << std::put_time(&tmValue, "%FT%T");
  oss << '.' << std::setfill('0') << std::setw(width) << value;
  return oss.str();
}

bool checkUtcToEpoch(int year, int mon, int mday, int hour, int min, int sec) {
  SCOPED_TRACE(fmt::format(
      "{}-{:02}-{:02} {:02}:{:02}:{:02}", year, mon, mday, hour, min, sec));
  std::tm tm{};
  tm.tm_sec = sec;
  tm.tm_min = min;
  tm.tm_hour = hour;
  tm.tm_mday = mday;
  tm.tm_mon = mon;
  tm.tm_year = year;
  errno = 0;
  auto expected = timegm(&tm);
  bool error = expected == -1 && errno != 0;
  auto actual = Timestamp::calendarUtcToEpoch(tm);
  if (!error) {
    EXPECT_EQ(actual, expected);
  }
  return !error;
}

} // namespace

TEST(TimestampTest, compareWithToStringAlt) {
  std::default_random_engine gen(randomSeed());
  std::uniform_int_distribution<int64_t> distSec(
      Timestamp::kMinSeconds, Timestamp::kMaxSeconds);
  std::uniform_int_distribution<uint64_t> distNano(0, Timestamp::kMaxNanos);
  for (int i = 0; i < 10'000; ++i) {
    Timestamp t(distSec(gen), distNano(gen));
    for (auto precision :
         {TimestampToStringOptions::Precision::kMilliseconds,
          TimestampToStringOptions::Precision::kNanoseconds}) {
      TimestampToStringOptions options{};
      options.precision = precision;
      ASSERT_EQ(t.toString(options), toStringAlt(t, precision))
          << t.getSeconds() << ' ' << t.getNanos();
    }
  }
}

TEST(TimestampTest, utcToEpoch) {
  ASSERT_TRUE(checkUtcToEpoch(1970, 1, 1, 0, 0, 0));
  ASSERT_TRUE(checkUtcToEpoch(2001, 11, 12, 18, 31, 1));
  ASSERT_TRUE(checkUtcToEpoch(1969, 12, 31, 23, 59, 59));
  ASSERT_TRUE(checkUtcToEpoch(1969, 12, 31, 23, 59, 58));
  ASSERT_TRUE(checkUtcToEpoch(INT32_MAX, 11, 30, 23, 59, 59));
  ASSERT_TRUE(checkUtcToEpoch(INT32_MIN, 1, 1, 0, 0, 0));
  ASSERT_TRUE(checkUtcToEpoch(
      INT32_MAX - INT32_MAX / 11,
      INT32_MAX,
      INT32_MAX,
      INT32_MAX,
      INT32_MAX,
      INT32_MAX));
  ASSERT_TRUE(checkUtcToEpoch(
      INT32_MIN - INT32_MIN / 11,
      INT32_MIN,
      INT32_MIN,
      INT32_MIN,
      INT32_MIN,
      INT32_MIN));
}

TEST(TimestampTest, utcToEpochRandomInputs) {
  std::default_random_engine gen(randomSeed());
  std::uniform_int_distribution<int32_t> dist(INT32_MIN, INT32_MAX);
  for (int i = 0; i < 10'000; ++i) {
    checkUtcToEpoch(
        dist(gen), dist(gen), dist(gen), dist(gen), dist(gen), dist(gen));
  }
}

TEST(TimestampTest, increaseOperator) {
  auto ts = Timestamp(0, 999999998);
  EXPECT_EQ("1970-01-01T00:00:00.999999998", ts.toString());
  ++ts;
  EXPECT_EQ("1970-01-01T00:00:00.999999999", ts.toString());
  ++ts;
  EXPECT_EQ("1970-01-01T00:00:01.000000000", ts.toString());
  ++ts;
  EXPECT_EQ("1970-01-01T00:00:01.000000001", ts.toString());
  ++ts;
  EXPECT_EQ("1970-01-01T00:00:01.000000002", ts.toString());

  auto kMax = Timestamp(Timestamp::kMaxSeconds, Timestamp::kMaxNanos);
  VELOX_ASSERT_THROW(++kMax, "Timestamp nanos out of range");
}

TEST(TimestampTest, decreaseOperator) {
  auto ts = Timestamp(0, 2);
  EXPECT_EQ("1970-01-01T00:00:00.000000002", ts.toString());
  --ts;
  EXPECT_EQ("1970-01-01T00:00:00.000000001", ts.toString());
  --ts;
  EXPECT_EQ("1970-01-01T00:00:00.000000000", ts.toString());
  --ts;
  EXPECT_EQ("1969-12-31T23:59:59.999999999", ts.toString());
  --ts;
  EXPECT_EQ("1969-12-31T23:59:59.999999998", ts.toString());

  auto kMin = Timestamp(Timestamp::kMinSeconds, 0);
  VELOX_ASSERT_THROW(--kMin, "Timestamp nanos out of range");
}

TEST(TimestampTest, outOfRange) {
  // There are two ranges for timezone conversion.
  //
  // #1. external/date cannot handle years larger than 32k (date::year::max()).
  // Any conversions exceeding that threshold will fail right away.
  auto* timezone = date::locate_zone("GMT");
  Timestamp t1(-3217830796800, 0);

  VELOX_ASSERT_THROW(
      t1.toTimePoint(), "Timestamp is outside of supported range");
  VELOX_ASSERT_THROW(
      t1.toTimezone(*timezone), "Timestamp is outside of supported range");

  // #2. external/date doesn't understand OS_TZDB repetition rules. Therefore,
  // for timezones with pre-defined repetition rules for daylight savings, for
  // example, it will throw for anything larger than 2037 (which is what is
  // currently materialized in OS_TZDBs). America/Los_Angeles is an example of
  // such timezone.
  timezone = date::locate_zone("America/Los_Angeles");
  Timestamp t2(32517359891, 0);
  VELOX_ASSERT_THROW(
      t2.toTimezone(*timezone),
      "Unable to convert timezone 'America/Los_Angeles' past");
}

// In debug mode, Timestamp constructor will throw exception if range check
// fails.
#ifdef NDEBUG
TEST(TimestampTest, overflow) {
  Timestamp t(std::numeric_limits<int64_t>::max(), 0);
  VELOX_ASSERT_THROW(
      t.toTimePoint(false),
      fmt::format(
          "Could not convert Timestamp({}, {}) to milliseconds",
          std::numeric_limits<int64_t>::max(),
          0));
  ASSERT_NO_THROW(t.toTimePoint(true));
}
#endif

void checkTm(const std::tm& actual, const std::tm& expected) {
  ASSERT_EQ(expected.tm_year, actual.tm_year);
  ASSERT_EQ(expected.tm_yday, actual.tm_yday);
  ASSERT_EQ(expected.tm_mon, actual.tm_mon);
  ASSERT_EQ(expected.tm_mday, actual.tm_mday);
  ASSERT_EQ(expected.tm_wday, actual.tm_wday);
  ASSERT_EQ(expected.tm_hour, actual.tm_hour);
  ASSERT_EQ(expected.tm_min, actual.tm_min);
  ASSERT_EQ(expected.tm_sec, actual.tm_sec);
}

std::string tmToString(
    const std::tm& tmValue,
    uint64_t nanos,
    const std::string& format,
    const TimestampToStringOptions& options) {
  auto width = static_cast<int>(options.precision);
  auto value =
      options.precision == TimestampToStringOptions::Precision::kMilliseconds
      ? nanos / 1'000'000
      : nanos;

  std::ostringstream oss;
  oss << std::put_time(&tmValue, format.c_str());

  if (options.mode != TimestampToStringOptions::Mode::kDateOnly) {
    oss << '.' << std::setfill('0') << std::setw(width) << value;
  }

  return oss.str();
}

TEST(TimestampTest, epochToUtc) {
  std::tm tm{};
  ASSERT_FALSE(Timestamp::epochToCalendarUtc(-(1ll << 60), tm));
  ASSERT_FALSE(Timestamp::epochToCalendarUtc(1ll << 60, tm));
}

TEST(TimestampTest, randomEpochToUtc) {
  uint64_t seed = 42;
  std::default_random_engine gen(seed);
  std::uniform_int_distribution<time_t> dist(
      std::numeric_limits<time_t>::min(), std::numeric_limits<time_t>::max());
  std::tm actual{};
  std::tm expected{};
  for (int i = 0; i < 10'000; ++i) {
    auto epoch = dist(gen);
    SCOPED_TRACE(fmt::format("epoch={}", epoch));
    if (gmtime_r(&epoch, &expected)) {
      ASSERT_TRUE(Timestamp::epochToCalendarUtc(epoch, actual));
      checkTm(actual, expected);
    } else {
      ASSERT_FALSE(Timestamp::epochToCalendarUtc(epoch, actual));
    }
  }
}

void testTmToString(
    const std::string& format,
    const TimestampToStringOptions::Mode mode) {
  uint64_t seed = 42;
  std::default_random_engine gen(seed);

  std::uniform_int_distribution<time_t> dist(
      std::numeric_limits<time_t>::min(), std::numeric_limits<time_t>::max());
  std::uniform_int_distribution<int> nanosDist(0, Timestamp::kMaxNanos);

  std::tm actual{};
  std::tm expected{};

  TimestampToStringOptions options;
  options.mode = mode;

  const std::vector<TimestampToStringOptions::Precision> precisions = {
      TimestampToStringOptions::Precision::kMilliseconds,
      TimestampToStringOptions::Precision::kNanoseconds};

  for (auto precision : precisions) {
    options.precision = precision;
    for (int i = 0; i < 10'000; ++i) {
      auto epoch = dist(gen);
      auto nanos = nanosDist(gen);
      SCOPED_TRACE(fmt::format(
          "epoch={}, nanos={}, mode={}, precision={}",
          epoch,
          nanos,
          mode,
          precision));
      if (gmtime_r(&epoch, &expected)) {
        ASSERT_TRUE(Timestamp::epochToCalendarUtc(epoch, actual));
        checkTm(actual, expected);

        std::string actualString;
        actualString.resize(getMaxStringLength(options));
        const auto view = Timestamp::tmToStringView(
            actual, nanos, options, actualString.data());
        actualString.resize(view.size());
        auto expectedString = tmToString(expected, nanos, format, options);
        ASSERT_EQ(expectedString, actualString);
      } else {
        ASSERT_FALSE(Timestamp::epochToCalendarUtc(epoch, actual));
      }
    }
  }
}

TEST(TimestampTest, tmToStringDateOnly) {
  // %F - equivalent to "%Y-%m-%d" (the ISO 8601 date format)
  testTmToString("%F", TimestampToStringOptions::Mode::kDateOnly);
}

TEST(TimestampTest, tmToStringTimeOnly) {
  // %T - equivalent to "%H:%M:%S" (the ISO 8601 time format)
  testTmToString("%T", TimestampToStringOptions::Mode::kTimeOnly);
}

TEST(TimestampTest, tmToStringTimestamp) {
  // %FT%T - equivalent to "%Y-%m-%dT%H:%M:%S" (the ISO 8601 timestamp format)
  testTmToString("%FT%T", TimestampToStringOptions::Mode::kFull);
}

TEST(TimestampTest, leadingPositiveSign) {
  TimestampToStringOptions options = {
      .leadingPositiveSign = true,
      .zeroPaddingYear = true,
      .dateTimeSeparator = ' ',
  };

  ASSERT_EQ(
      timestampToString(Timestamp(253402231016, 0), options),
      "9999-12-31 04:36:56.000000000");
  ASSERT_EQ(
      timestampToString(Timestamp(253405036800, 0), options),
      "+10000-02-01 16:00:00.000000000");
}

TEST(TimestampTest, skipTrailingZeros) {
  TimestampToStringOptions options = {
      .precision = TimestampToStringOptions::Precision::kMicroseconds,
      .skipTrailingZeros = true,
      .zeroPaddingYear = true,
      .dateTimeSeparator = ' ',
  };

  ASSERT_EQ(
      timestampToString(Timestamp(-946684800, 0), options),
      "1940-01-02 00:00:00");
  ASSERT_EQ(timestampToString(Timestamp(0, 0), options), "1970-01-01 00:00:00");
  ASSERT_EQ(
      timestampToString(Timestamp(0, 365), options), "1970-01-01 00:00:00");
  ASSERT_EQ(
      timestampToString(Timestamp(0, 65873), options),
      "1970-01-01 00:00:00.000065");
  ASSERT_EQ(
      timestampToString(Timestamp(94668480000, 0), options),
      "4969-12-04 00:00:00");
  ASSERT_EQ(
      timestampToString(Timestamp(946729316, 129999999), options),
      "2000-01-01 12:21:56.129999");
  ASSERT_EQ(
      timestampToString(Timestamp(946729316, 129990000), options),
      "2000-01-01 12:21:56.12999");
  ASSERT_EQ(
      timestampToString(Timestamp(946729316, 129900000), options),
      "2000-01-01 12:21:56.1299");
  ASSERT_EQ(
      timestampToString(Timestamp(946729316, 129000000), options),
      "2000-01-01 12:21:56.129");
  ASSERT_EQ(
      timestampToString(Timestamp(946729316, 129010000), options),
      "2000-01-01 12:21:56.12901");
  ASSERT_EQ(
      timestampToString(Timestamp(946729316, 129001000), options),
      "2000-01-01 12:21:56.129001");
  ASSERT_EQ(
      timestampToString(Timestamp(-50049331200, 726600000), options),
      "0384-01-01 08:00:00.7266");
}

} // namespace
} // namespace facebook::velox
