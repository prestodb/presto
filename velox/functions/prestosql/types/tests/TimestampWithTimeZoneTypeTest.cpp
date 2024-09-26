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
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/functions/prestosql/types/tests/TypeTestBase.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::test {

class TimestampWithTimeZoneTypeTest : public testing::Test,
                                      public TypeTestBase {
 public:
  TimestampWithTimeZoneTypeTest() {
    registerTimestampWithTimeZoneType();
  }
};

TEST_F(TimestampWithTimeZoneTypeTest, basic) {
  ASSERT_STREQ(TIMESTAMP_WITH_TIME_ZONE()->name(), "TIMESTAMP WITH TIME ZONE");
  ASSERT_STREQ(TIMESTAMP_WITH_TIME_ZONE()->kindName(), "BIGINT");
  ASSERT_TRUE(TIMESTAMP_WITH_TIME_ZONE()->parameters().empty());
  ASSERT_EQ(TIMESTAMP_WITH_TIME_ZONE()->toString(), "TIMESTAMP WITH TIME ZONE");

  ASSERT_TRUE(hasType("TIMESTAMP WITH TIME ZONE"));
  ASSERT_EQ(
      *getType("TIMESTAMP WITH TIME ZONE", {}), *TIMESTAMP_WITH_TIME_ZONE());
}

TEST_F(TimestampWithTimeZoneTypeTest, serde) {
  testTypeSerde(TIMESTAMP_WITH_TIME_ZONE());
}

TEST_F(TimestampWithTimeZoneTypeTest, pack) {
  std::mt19937 randGen(std::random_device{}());
  // 0xFFF8000000000000 and 0x7FFFFFFFFFFFF are hexadecimal numbers
  // that represent the minimum and maximum values that the
  // TimestampWithTimeZoneType type can represent, respectively.
  std::uniform_int_distribution<int64_t> millisUtcDis(
      0xFFF8000000000000L, 0x7FFFFFFFFFFFF);

  // 2233 represents the maximum value of timeZoneKey,
  // see tzDB in TimeZoneDatabase.cpp
  std::uniform_int_distribution<int16_t> timeZoneKeyDis(0, 2233);

  for (int64_t i = 0; i < 10'000; ++i) {
    auto millisUtc = millisUtcDis(randGen);
    auto timeZoneKey = timeZoneKeyDis(randGen);
    SCOPED_TRACE(
        fmt::format("millisUtc={}, timeZoneKey={}", millisUtc, timeZoneKey));

    auto packedTimeMillis = pack(millisUtc, timeZoneKey);
    ASSERT_EQ(unpackMillisUtc(packedTimeMillis), millisUtc);
    ASSERT_EQ(unpackZoneKeyId(packedTimeMillis), timeZoneKey);
  }
}

TEST_F(TimestampWithTimeZoneTypeTest, compare) {
  auto compare = [](int32_t expected,
                    int64_t millis1,
                    const std::string& tz1,
                    int64_t millis2,
                    const std::string& tz2) {
    int64_t left = pack(millis1, tz::getTimeZoneID(tz1));
    int64_t right = pack(millis2, tz::getTimeZoneID(tz2));

    ASSERT_EQ(expected, TIMESTAMP_WITH_TIME_ZONE()->compare(left, right));
  };

  compare(0, 1639426440000, "+01:00", 1639426440000, "+03:00");
  compare(0, 1639426440000, "+01:00", 1639426440000, "-14:00");
  compare(0, 1639426440000, "+03:00", 1639426440000, "-14:00");
  compare(0, -1639426440000, "+01:00", -1639426440000, "+03:00");

  compare(-1, 1549770072000, "+01:00", 1639426440000, "+03:00");
  compare(-1, 1549770072000, "+01:00", 1639426440000, "-14:00");
  compare(-1, 1549770072000, "+03:00", 1639426440000, "-14:00");
  compare(-1, -1639426440000, "+01:00", -1539426440000, "+03:00");
  compare(-1, -1639426440000, "+01:00", 1639426440000, "-14:00");

  compare(1, 1639426440000, "+01:00", 1549770072000, "+03:00");
  compare(1, 1639426440000, "+01:00", 1549770072000, "-14:00");
  compare(1, 1639426440000, "+03:00", 1549770072000, "-14:00");
  compare(1, 1639426440000, "+01:00", -1639426440000, "+03:00");
  compare(1, -1539426440000, "+01:00", -1639426440000, "-14:00");
}

TEST_F(TimestampWithTimeZoneTypeTest, hash) {
  auto expectHashesEq = [](int64_t millis1,
                           const std::string& tz1,
                           int64_t millis2,
                           const std::string& tz2) {
    int64_t left = pack(millis1, tz::getTimeZoneID(tz1));
    int64_t right = pack(millis2, tz::getTimeZoneID(tz2));

    ASSERT_EQ(
        TIMESTAMP_WITH_TIME_ZONE()->hash(left),
        TIMESTAMP_WITH_TIME_ZONE()->hash(right));
  };

  auto expectHashesNeq = [](int64_t millis1,
                            const std::string& tz1,
                            int64_t millis2,
                            const std::string& tz2) {
    int64_t left = pack(millis1, tz::getTimeZoneID(tz1));
    int64_t right = pack(millis2, tz::getTimeZoneID(tz2));

    ASSERT_NE(
        TIMESTAMP_WITH_TIME_ZONE()->hash(left),
        TIMESTAMP_WITH_TIME_ZONE()->hash(right));
  };

  expectHashesEq(1639426440000, "+01:00", 1639426440000, "+03:00");
  expectHashesEq(1639426440000, "+01:00", 1639426440000, "-14:00");
  expectHashesEq(1639426440000, "+03:00", 1639426440000, "-14:00");
  expectHashesEq(-1639426440000, "+03:00", -1639426440000, "-14:00");

  expectHashesNeq(1549770072000, "+01:00", 1639426440000, "+03:00");
  expectHashesNeq(1549770072000, "+01:00", 1639426440000, "-14:00");
  expectHashesNeq(1549770072000, "+03:00", 1639426440000, "-14:00");
  expectHashesNeq(-1639426440000, "+03:00", 1639426440000, "-14:00");
}

} // namespace facebook::velox::test
