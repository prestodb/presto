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
#include "velox/common/testutil/TestValue.h"
#include "velox/external/date/tz.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::tz {
namespace {

using namespace std::chrono;

DEBUG_ONLY_TEST(TimeZoneMapExternalInvalidTest, externalInvalid) {
  const int16_t testZoneId = 1681;
  const std::string testZone = "Africa/Abidjan";
  common::testutil::TestValue::enable();
  SCOPED_TESTVALUE_SET(
      "facebook::velox::tz::locateZoneImpl",
      std::function<void(std::string_view * tz_name)>(
          [&](std::string_view* tz_name) -> void {
            if (*tz_name == testZone) {
              // Emulates an invalid_timezone error thrown from external API
              // date::locate_zone. In real scenarios, this could happen when
              // the timezone is not found in operating system's timezone list.
              throw date::invalid_timezone(std::string(*tz_name));
            }
          }));

  VELOX_ASSERT_THROW(
      getTimeZoneName(testZoneId), "Unable to resolve timeZoneID");
  VELOX_ASSERT_THROW(getTimeZoneID(testZone), "Unknown time zone");

  EXPECT_EQ("UTC", getTimeZoneName(0));
  EXPECT_EQ(0, getTimeZoneID("+00:00"));
  EXPECT_EQ("Africa/Accra", getTimeZoneName(1682));
  EXPECT_EQ(1682, getTimeZoneID("Africa/Accra"));
}
} // namespace
} // namespace facebook::velox::tz
