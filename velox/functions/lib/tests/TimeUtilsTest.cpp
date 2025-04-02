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
#include "velox/functions/lib/TimeUtils.h"

namespace facebook::velox::functions {
namespace {

TEST(TimeUtilsTest, fromDateTimeUnitString) {
  // Return null when unit string is invalid and throwIfInvalid is false.
  ASSERT_EQ(std::nullopt, fromDateTimeUnitString("", false));
  ASSERT_EQ(std::nullopt, fromDateTimeUnitString("microsecond", false));
  ASSERT_EQ(std::nullopt, fromDateTimeUnitString("dd", false));
  ASSERT_EQ(std::nullopt, fromDateTimeUnitString("mon", false));
  ASSERT_EQ(std::nullopt, fromDateTimeUnitString("mm", false));
  ASSERT_EQ(std::nullopt, fromDateTimeUnitString("yyyy", false));
  ASSERT_EQ(std::nullopt, fromDateTimeUnitString("yy", false));

  // Throw when unit string is invalid and throwIfInvalid is true.
  VELOX_ASSERT_THROW(
      fromDateTimeUnitString("", true), "Unsupported datetime unit:");
  VELOX_ASSERT_THROW(
      fromDateTimeUnitString("microsecond", true),
      "Unsupported datetime unit:");
  VELOX_ASSERT_THROW(
      fromDateTimeUnitString("dd", true), "Unsupported datetime unit:");
  VELOX_ASSERT_THROW(
      fromDateTimeUnitString("mon", true), "Unsupported datetime unit:");
  VELOX_ASSERT_THROW(
      fromDateTimeUnitString("mm", true), "Unsupported datetime unit:");
  VELOX_ASSERT_THROW(
      fromDateTimeUnitString("yyyy", true), "Unsupported datetime unit:");
  VELOX_ASSERT_THROW(
      fromDateTimeUnitString("yy", true), "Unsupported datetime unit:");

  ASSERT_EQ(
      DateTimeUnit::kMillisecond, fromDateTimeUnitString("millisecond", false));
  ASSERT_EQ(DateTimeUnit::kSecond, fromDateTimeUnitString("second", false));
  ASSERT_EQ(DateTimeUnit::kMinute, fromDateTimeUnitString("minute", false));
  ASSERT_EQ(DateTimeUnit::kHour, fromDateTimeUnitString("hour", false));
  ASSERT_EQ(DateTimeUnit::kDay, fromDateTimeUnitString("day", false));
  ASSERT_EQ(DateTimeUnit::kWeek, fromDateTimeUnitString("week", false));
  ASSERT_EQ(DateTimeUnit::kMonth, fromDateTimeUnitString("month", false));
  ASSERT_EQ(DateTimeUnit::kQuarter, fromDateTimeUnitString("quarter", false));
  ASSERT_EQ(DateTimeUnit::kYear, fromDateTimeUnitString("year", false));

  ASSERT_EQ(
      DateTimeUnit::kMicrosecond,
      fromDateTimeUnitString("microsecond", false, true, true));
  ASSERT_EQ(
      DateTimeUnit::kMillisecond,
      fromDateTimeUnitString("millisecond", false, true, true));
  ASSERT_EQ(
      DateTimeUnit::kSecond,
      fromDateTimeUnitString("second", false, true, true));
  ASSERT_EQ(
      DateTimeUnit::kMinute,
      fromDateTimeUnitString("minute", false, true, true));
  ASSERT_EQ(
      DateTimeUnit::kHour, fromDateTimeUnitString("hour", false, true, true));
  ASSERT_EQ(
      DateTimeUnit::kDay, fromDateTimeUnitString("day", false, true, true));
  ASSERT_EQ(
      DateTimeUnit::kDay, fromDateTimeUnitString("dd", false, true, true));
  ASSERT_EQ(
      DateTimeUnit::kWeek, fromDateTimeUnitString("week", false, true, true));
  ASSERT_EQ(
      DateTimeUnit::kMonth, fromDateTimeUnitString("month", false, true, true));
  ASSERT_EQ(
      DateTimeUnit::kMonth, fromDateTimeUnitString("mon", false, true, true));
  ASSERT_EQ(
      DateTimeUnit::kMonth, fromDateTimeUnitString("mm", false, true, true));
  ASSERT_EQ(
      DateTimeUnit::kQuarter,
      fromDateTimeUnitString("quarter", false, true, true));
  ASSERT_EQ(
      DateTimeUnit::kYear, fromDateTimeUnitString("year", false, true, true));
  ASSERT_EQ(
      DateTimeUnit::kYear, fromDateTimeUnitString("yyyy", false, true, true));
  ASSERT_EQ(
      DateTimeUnit::kYear, fromDateTimeUnitString("yy", false, true, true));
}

TEST(TimeUtilsTest, adjustEpoch) {
  EXPECT_EQ(Timestamp(998474640, 0), adjustEpoch(998474645, 60));
  EXPECT_EQ(Timestamp(998474400, 0), adjustEpoch(998474645, 60 * 60));
  EXPECT_EQ(Timestamp(998438400, 0), adjustEpoch(998474645, 24 * 60 * 60));
  EXPECT_EQ(Timestamp(-120, 0), adjustEpoch(-61, 60));
}

TEST(TimeUtilsTest, truncateTimestamp) {
  auto* timezone = tz::locateZone("GMT");

  EXPECT_EQ(
      Timestamp(0, 0),
      truncateTimestamp(Timestamp(0, 0), DateTimeUnit::kSecond, timezone));
  EXPECT_EQ(
      Timestamp(0, 0),
      truncateTimestamp(Timestamp(0, 123), DateTimeUnit::kSecond, timezone));
  EXPECT_EQ(
      Timestamp(-1, 0),
      truncateTimestamp(Timestamp(-1, 0), DateTimeUnit::kSecond, timezone));

  EXPECT_EQ(
      Timestamp(998474645, 321'001'000),
      truncateTimestamp(
          Timestamp(998'474'645, 321'001'234),
          DateTimeUnit::kMicrosecond,
          timezone));
  EXPECT_EQ(
      Timestamp(998474645, 321'000'000),
      truncateTimestamp(
          Timestamp(998'474'645, 321'001'234),
          DateTimeUnit::kMillisecond,
          timezone));
  EXPECT_EQ(
      Timestamp(998474645, 0),
      truncateTimestamp(
          Timestamp(998'474'645, 321'001'234),
          DateTimeUnit::kSecond,
          timezone));
  EXPECT_EQ(
      Timestamp(998474640, 0),
      truncateTimestamp(
          Timestamp(998'474'645, 321'001'234),
          DateTimeUnit::kMinute,
          timezone));
  EXPECT_EQ(
      Timestamp(998474400, 0),
      truncateTimestamp(
          Timestamp(998'474'645, 321'001'234), DateTimeUnit::kHour, timezone));
  EXPECT_EQ(
      Timestamp(998438400, 0),
      truncateTimestamp(
          Timestamp(998'474'645, 321'001'234), DateTimeUnit::kDay, timezone));
  EXPECT_EQ(
      Timestamp(998265600, 0),
      truncateTimestamp(
          Timestamp(998'474'645, 321'001'234), DateTimeUnit::kWeek, timezone));
  EXPECT_EQ(
      Timestamp(996624000, 0),
      truncateTimestamp(
          Timestamp(998'474'645, 321'001'234), DateTimeUnit::kMonth, timezone));
  EXPECT_EQ(
      Timestamp(993945600, 0),
      truncateTimestamp(
          Timestamp(998'474'645, 321'001'234),
          DateTimeUnit::kQuarter,
          timezone));
  EXPECT_EQ(
      Timestamp(978307200, 0),
      truncateTimestamp(
          Timestamp(998'474'645, 321'001'234), DateTimeUnit::kYear, timezone));

  auto* timezone1 = tz::locateZone("America/Los_Angeles");
  EXPECT_EQ(
      Timestamp(0, 0),
      truncateTimestamp(Timestamp(0, 0), DateTimeUnit::kSecond, timezone1));
  EXPECT_EQ(
      Timestamp(0, 0),
      truncateTimestamp(Timestamp(0, 123), DateTimeUnit::kSecond, timezone1));
  EXPECT_EQ(
      Timestamp(-57600, 0),
      truncateTimestamp(Timestamp(0, 0), DateTimeUnit::kDay, timezone1));

  EXPECT_EQ(
      Timestamp(998474645, 321'001'000),
      truncateTimestamp(
          Timestamp(998'474'645, 321'001'234),
          DateTimeUnit::kMicrosecond,
          timezone1));
  EXPECT_EQ(
      Timestamp(998474645, 321'000'000),
      truncateTimestamp(
          Timestamp(998'474'645, 321'001'234),
          DateTimeUnit::kMillisecond,
          timezone1));
  EXPECT_EQ(
      Timestamp(998474645, 0),
      truncateTimestamp(
          Timestamp(998'474'645, 321'001'234),
          DateTimeUnit::kSecond,
          timezone1));
  EXPECT_EQ(
      Timestamp(998474640, 0),
      truncateTimestamp(
          Timestamp(998'474'645, 321'001'234),
          DateTimeUnit::kMinute,
          timezone1));
  EXPECT_EQ(
      Timestamp(998474400, 0),
      truncateTimestamp(
          Timestamp(998'474'645, 321'001'234), DateTimeUnit::kHour, timezone1));
  EXPECT_EQ(
      Timestamp(998463600, 0),
      truncateTimestamp(
          Timestamp(998'474'645, 321'001'234), DateTimeUnit::kDay, timezone1));
  EXPECT_EQ(
      Timestamp(998290800, 0),
      truncateTimestamp(
          Timestamp(998'474'645, 321'001'234), DateTimeUnit::kWeek, timezone1));
  EXPECT_EQ(
      Timestamp(996649200, 0),
      truncateTimestamp(
          Timestamp(998'474'645, 321'001'234),
          DateTimeUnit::kMonth,
          timezone1));
  EXPECT_EQ(
      Timestamp(993970800, 0),
      truncateTimestamp(
          Timestamp(998'474'645, 321'001'234),
          DateTimeUnit::kQuarter,
          timezone1));
  EXPECT_EQ(
      Timestamp(978336000, 0),
      truncateTimestamp(
          Timestamp(998'474'645, 321'001'234), DateTimeUnit::kYear, timezone1));
}
} // namespace
} // namespace facebook::velox::functions
