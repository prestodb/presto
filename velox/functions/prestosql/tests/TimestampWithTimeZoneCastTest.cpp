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

#include "velox/functions/prestosql/tests/CastBaseTest.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"

using namespace facebook::velox;

class TimestampWithTimeZoneCastTest : public functions::test::CastBaseTest {
 public:
  RowVectorPtr makeTimestampWithTimeZoneVector(
      const VectorPtr& timestamps,
      const VectorPtr& timezones) {
    VELOX_CHECK_EQ(timestamps->size(), timezones->size());
    return std::make_shared<RowVector>(
        pool(),
        TIMESTAMP_WITH_TIME_ZONE(),
        nullptr,
        timestamps->size(),
        std::vector<VectorPtr>({timestamps, timezones}));
  }
};

TEST_F(TimestampWithTimeZoneCastTest, fromTimestamp) {
  const auto tsVector = makeNullableFlatVector<Timestamp>(
      {Timestamp(1996, 0), std::nullopt, Timestamp(19920, 0)});
  const auto expected = makeTimestampWithTimeZoneVector(
      makeFlatVector<int64_t>(
          {1996 * kMillisInSecond, 0, 19920 * kMillisInSecond}),
      makeFlatVector<int16_t>({0, 0, 0}));
  expected->setNull(1, true);

  testCast<ComplexType>(
      TIMESTAMP(), TIMESTAMP_WITH_TIME_ZONE(), tsVector, expected);
}

TEST_F(TimestampWithTimeZoneCastTest, toTimestamp) {
  const auto tsWithTZVector = makeTimestampWithTimeZoneVector(
      makeFlatVector<int64_t>(
          {1996 * kMillisInSecond, 0, 19920 * kMillisInSecond}),
      makeFlatVector<int16_t>({0, 0, 1825 /*America/Los_Angeles*/}));
  tsWithTZVector->setNull(1, true);
  const auto expected = makeNullableFlatVector<Timestamp>(
      {Timestamp(1996, 0), std::nullopt, Timestamp(19920 - 8 * 3600, 0)});

  testCast<ComplexType>(
      TIMESTAMP_WITH_TIME_ZONE(), TIMESTAMP(), tsWithTZVector, expected);
}
