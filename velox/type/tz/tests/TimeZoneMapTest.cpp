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

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::util {
namespace {

TEST(TimeZoneMapTest, getTimeZoneName) {
  EXPECT_EQ("America/Los_Angeles", getTimeZoneName(1825));
  EXPECT_EQ("Europe/Moscow", getTimeZoneName(2079));
  EXPECT_EQ("Pacific/Kanton", getTimeZoneName(2231));
  EXPECT_EQ("Europe/Kyiv", getTimeZoneName(2232));
  EXPECT_EQ("America/Ciudad_Juarez", getTimeZoneName(2233));
  EXPECT_EQ("-00:01", getTimeZoneName(840));
}

TEST(TimeZoneMapTest, getTimeZoneID) {
  EXPECT_EQ(1825, getTimeZoneID("America/Los_Angeles"));
  EXPECT_EQ(2079, getTimeZoneID("Europe/Moscow"));
  EXPECT_EQ(2231, getTimeZoneID("Pacific/Kanton"));
  EXPECT_EQ(2232, getTimeZoneID("Europe/Kyiv"));
  EXPECT_EQ(2233, getTimeZoneID("America/Ciudad_Juarez"));
  EXPECT_EQ(0, getTimeZoneID("UTC"));
  EXPECT_EQ(0, getTimeZoneID("GMT"));
  EXPECT_EQ(0, getTimeZoneID("Z"));
  EXPECT_EQ(0, getTimeZoneID("greenwich"));
  EXPECT_EQ(0, getTimeZoneID("ETC/GMT"));
  EXPECT_EQ(0, getTimeZoneID("ETC/GMT0"));
  EXPECT_EQ(0, getTimeZoneID("ETC/UCT"));
  EXPECT_EQ(0, getTimeZoneID("ETC/universal"));
  EXPECT_EQ(0, getTimeZoneID("etc/zulu"));

  // (+/-)XX:MM format.
  EXPECT_EQ(840, getTimeZoneID("-00:01"));
  EXPECT_EQ(0, getTimeZoneID("+00:00"));
  EXPECT_EQ(454, getTimeZoneID("-06:27"));
  EXPECT_EQ(541, getTimeZoneID("-05:00"));
  EXPECT_EQ(1140, getTimeZoneID("+05:00"));

  EXPECT_EQ(0, getTimeZoneID("etc/GMT+0"));
  EXPECT_EQ(0, getTimeZoneID("etc/GMT-0"));
  EXPECT_EQ(1020, getTimeZoneID("etc/GMT-3"));
  EXPECT_EQ(301, getTimeZoneID("etc/GMT+9"));
  EXPECT_EQ(1680, getTimeZoneID("etc/GMT-14"));

  // Case insensitive.
  EXPECT_EQ(0, getTimeZoneID("utc"));
  EXPECT_EQ(1825, getTimeZoneID("america/los_angeles"));
  EXPECT_EQ(1825, getTimeZoneID("aMERICa/los_angeles"));
}

TEST(TimeZoneMapTest, getTimeZoneIDFromOffset) {
  auto nameFromOffset = [&](int32_t offset) {
    return getTimeZoneName(getTimeZoneID(offset));
  };

  EXPECT_EQ("+00:00", nameFromOffset(0));
  EXPECT_EQ("+05:30", nameFromOffset(5 * 60 + 30));
  EXPECT_EQ("-08:00", nameFromOffset(-8 * 60));
  EXPECT_EQ("+02:17", nameFromOffset(2 * 60 + 17));

  VELOX_ASSERT_THROW(getTimeZoneID(15'000), "Invalid timezone offset");
  VELOX_ASSERT_THROW(getTimeZoneID(-15'000), "Invalid timezone offset");
}

TEST(TimeZoneMapTest, invalid) {
  VELOX_ASSERT_THROW(getTimeZoneName(99999999), "Unable to resolve timeZoneID");
  VELOX_ASSERT_THROW(getTimeZoneID("This is a test"), "Unknown time zone");

  VELOX_ASSERT_THROW(getTimeZoneID("ETC/05:00"), "Unknown time zone");
  VELOX_ASSERT_THROW(getTimeZoneID("ETC+05:00"), "Unknown time zone");

  VELOX_ASSERT_THROW(getTimeZoneID("etc/GMT-15"), "Unknown time zone");
  VELOX_ASSERT_THROW(getTimeZoneID("etc/GMT+ab"), "Unknown time zone");
  VELOX_ASSERT_THROW(getTimeZoneID("etc/GMT+300"), "Unknown time zone");
}

} // namespace
} // namespace facebook::velox::util
