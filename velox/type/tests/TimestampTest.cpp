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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/external/date/tz.h"
#include "velox/type/Timestamp.h"

namespace facebook::velox {
namespace {

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
  auto* timezone = date::locate_zone("GMT");
  Timestamp t(-3217830796800, 0);

  VELOX_ASSERT_THROW(
      t.toTimePoint(), "Timestamp is outside of supported range");
  VELOX_ASSERT_THROW(
      t.toTimezone(*timezone), "Timestamp is outside of supported range");
}
} // namespace
} // namespace facebook::velox
