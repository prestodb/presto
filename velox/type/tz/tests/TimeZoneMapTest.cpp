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

namespace facebook::velox::tz {
namespace {

using namespace std::chrono;

TEST(TimeZoneMapTest, locateZoneID) {
  auto locateZoneID = [&](std::string_view name) {
    const auto* tz = locateZone(name);
    EXPECT_NE(tz, nullptr);
    return tz->id();
  };

  EXPECT_EQ(0, locateZoneID("UTC"));
  EXPECT_EQ(0, locateZoneID("+00:00"));
  EXPECT_EQ(0, locateZoneID("-00:00"));
  EXPECT_EQ(831, locateZoneID("-00:10"));
  EXPECT_EQ(462, locateZoneID("-06:19"));
  EXPECT_EQ(1315, locateZoneID("+07:55"));
  EXPECT_EQ(1680, locateZoneID("+14:00"));
  EXPECT_EQ(1720, locateZoneID("Africa/Maseru"));
  EXPECT_EQ(2141, locateZoneID("Pacific/Marquesas"));
  EXPECT_EQ(2215, locateZoneID("Asia/Chita"));
  EXPECT_EQ(2233, locateZoneID("America/Ciudad_Juarez"));
}

TEST(TimeZoneMapTest, locateZoneUTCAlias) {
  auto locateZoneID = [&](std::string_view name) {
    const auto* tz = locateZone(name);
    EXPECT_NE(tz, nullptr);
    return tz->name();
  };

  // Ensure all of these aliases resolve to a time zone called "UTC".
  EXPECT_EQ("UTC", locateZoneID("UTC"));
  EXPECT_EQ("UTC", locateZoneID("gmt"));
  EXPECT_EQ("UTC", locateZoneID("Z"));
  EXPECT_EQ("UTC", locateZoneID("zulu"));
  EXPECT_EQ("UTC", locateZoneID("Greenwich"));
  EXPECT_EQ("UTC", locateZoneID("gmt0"));
  EXPECT_EQ("UTC", locateZoneID("GMT"));
  EXPECT_EQ("UTC", locateZoneID("uct"));
  EXPECT_EQ("UTC", locateZoneID("+00:00"));
  EXPECT_EQ("UTC", locateZoneID("-00:00"));
}

TEST(TimeZoneMapTest, offsetToLocal) {
  auto toLocalTime = [&](std::string_view name, size_t ts) {
    const auto* tz = locateZone(name);
    EXPECT_NE(tz, nullptr);
    return tz->to_local(seconds{ts}).count();
  };

  // Ensure all of these aliases resolve to a time zone called "UTC".
  EXPECT_EQ(0, toLocalTime("+00:00", 0));
  EXPECT_EQ(60, toLocalTime("+00:01", 0));
  EXPECT_EQ(-60, toLocalTime("-00:01", 0));
  EXPECT_EQ(3600, toLocalTime("+01:00", 0));
  EXPECT_EQ(-3660, toLocalTime("-01:01", 0));

  // In "2024-07-25", America/Los_Angeles was in daylight savings time (UTC-07).
  size_t ts = 1721890800;
  EXPECT_EQ(toLocalTime("-07:00", ts), toLocalTime("America/Los_Angeles", ts));
  EXPECT_NE(toLocalTime("-08:00", ts), toLocalTime("America/Los_Angeles", ts));

  // In "2024-01-01", it was not (UTC-08).
  ts = 1704096000;
  EXPECT_EQ(toLocalTime("-08:00", ts), toLocalTime("America/Los_Angeles", ts));
  EXPECT_NE(toLocalTime("-07:00", ts), toLocalTime("America/Los_Angeles", ts));
}

TEST(TimeZoneMapTest, offsetToSys) {
  auto toSysTime = [&](std::string_view name, size_t ts) {
    const auto* tz = locateZone(name);
    EXPECT_NE(tz, nullptr);
    return tz->to_sys(seconds{ts}).count();
  };

  // Ensure all of these aliases resolve to a time zone called "UTC".
  EXPECT_EQ(0, toSysTime("+00:00", 0));
  EXPECT_EQ(-60, toSysTime("+00:01", 0));
  EXPECT_EQ(+60, toSysTime("-00:01", 0));
  EXPECT_EQ(-3600, toSysTime("+01:00", 0));
  EXPECT_EQ(+3660, toSysTime("-01:01", 0));

  // In "2024-07-25", America/Los_Angeles was in daylight savings time (UTC-07).
  size_t ts = 1721890800;
  EXPECT_EQ(toSysTime("-07:00", ts), toSysTime("America/Los_Angeles", ts));
  EXPECT_NE(toSysTime("-08:00", ts), toSysTime("America/Los_Angeles", ts));

  // In "2024-01-01", it was not (UTC-08).
  ts = 1704096000;
  EXPECT_EQ(toSysTime("-08:00", ts), toSysTime("America/Los_Angeles", ts));
  EXPECT_NE(toSysTime("-07:00", ts), toSysTime("America/Los_Angeles", ts));
}

TEST(TimeZoneMapTest, getTimeZoneName) {
  EXPECT_EQ("America/Los_Angeles", getTimeZoneName(1825));
  EXPECT_EQ("Europe/Moscow", getTimeZoneName(2079));
  EXPECT_EQ("Pacific/Kanton", getTimeZoneName(2231));
  EXPECT_EQ("Europe/Kyiv", getTimeZoneName(2232));
  EXPECT_EQ("America/Ciudad_Juarez", getTimeZoneName(2233));
  EXPECT_EQ("-00:01", getTimeZoneName(840));
  EXPECT_EQ("UTC", getTimeZoneName(0));
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
  EXPECT_EQ(0, getTimeZoneID("z"));
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

  // "+00:00" is an alias to UTC.
  EXPECT_EQ("UTC", nameFromOffset(0));
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
} // namespace facebook::velox::tz
