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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::util {
namespace {

TEST(TimeZoneMapTest, simple) {
  EXPECT_EQ("America/Los_Angeles", getTimeZoneName(1825));
  EXPECT_EQ("Europe/Moscow", getTimeZoneName(2079));
  EXPECT_EQ("-00:01", getTimeZoneName(840));
}

TEST(TimeZoneMapTest, invalid) {
  EXPECT_THROW(getTimeZoneName(0), std::runtime_error);
  EXPECT_THROW(getTimeZoneName(99999999), std::runtime_error);
}

} // namespace
} // namespace facebook::velox::util
