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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::functions::sparksql::test {
namespace {

class MakeTimestampTest : public SparkFunctionBaseTest {
 protected:
  void setQueryTimeZone(const std::string& timeZone) {
    queryCtx_->testingOverrideConfigUnsafe({
        {core::QueryConfig::kSessionTimezone, timeZone},
        {core::QueryConfig::kAdjustTimestampToTimezone, "true"},
    });
  }
};

TEST_F(MakeTimestampTest, basic) {
  const auto microsType = DECIMAL(16, 6);
  const auto testMakeTimestamp = [&](const RowVectorPtr& data,
                                     const VectorPtr& expected,
                                     bool hasTimeZone) {
    auto result = hasTimeZone
        ? evaluate("make_timestamp(c0, c1, c2, c3, c4, c5, c6)", data)
        : evaluate("make_timestamp(c0, c1, c2, c3, c4, c5)", data);
    facebook::velox::test::assertEqualVectors(expected, result);
  };
  const auto testConstantTimezone = [&](const RowVectorPtr& data,
                                        const std::string& timezone,
                                        const VectorPtr& expected) {
    auto result = evaluate(
        fmt::format("make_timestamp(c0, c1, c2, c3, c4, c5, '{}')", timezone),
        data);
    facebook::velox::test::assertEqualVectors(expected, result);
  };

  // Valid cases w/o time zone argument.
  {
    const auto year = makeFlatVector<int32_t>({2021, 2021, 2021, 2021, 2021});
    const auto month = makeFlatVector<int32_t>({7, 7, 7, 7, 7});
    const auto day = makeFlatVector<int32_t>({11, 11, 11, 11, 11});
    const auto hour = makeFlatVector<int32_t>({6, 6, 6, 6, 6});
    const auto minute = makeFlatVector<int32_t>({30, 30, 30, 30, 30});
    const auto micros = makeNullableFlatVector<int64_t>(
        {45678000, 1e6, 6e7, 59999999, std::nullopt}, microsType);
    auto data = makeRowVector({year, month, day, hour, minute, micros});

    setQueryTimeZone("GMT");
    auto expectedGMT = makeNullableFlatVector<Timestamp>(
        {util::fromTimestampString("2021-07-11 06:30:45.678"),
         util::fromTimestampString("2021-07-11 06:30:01"),
         util::fromTimestampString("2021-07-11 06:31:00"),
         util::fromTimestampString("2021-07-11 06:30:59.999999"),
         std::nullopt});
    testMakeTimestamp(data, expectedGMT, false);
    testConstantTimezone(data, "GMT", expectedGMT);

    setQueryTimeZone("Asia/Shanghai");
    auto expectedSessionTimezone = makeNullableFlatVector<Timestamp>(
        {util::fromTimestampString("2021-07-10 22:30:45.678"),
         util::fromTimestampString("2021-07-10 22:30:01"),
         util::fromTimestampString("2021-07-10 22:31:00"),
         util::fromTimestampString("2021-07-10 22:30:59.999999"),
         std::nullopt});
    testMakeTimestamp(data, expectedSessionTimezone, false);
    // Session time zone will be ignored if time zone is specified in argument.
    testConstantTimezone(data, "GMT", expectedGMT);
  }

  // Valid cases w/ time zone argument.
  {
    setQueryTimeZone("Asia/Shanghai");
    const auto year = makeFlatVector<int32_t>({2021, 2021, 1});
    const auto month = makeFlatVector<int32_t>({07, 07, 1});
    const auto day = makeFlatVector<int32_t>({11, 11, 1});
    const auto hour = makeFlatVector<int32_t>({6, 6, 1});
    const auto minute = makeFlatVector<int32_t>({30, 30, 1});
    const auto micros =
        makeNullableFlatVector<int64_t>({45678000, 45678000, 1e6}, microsType);
    const auto timeZone =
        makeNullableFlatVector<StringView>({"GMT", "CET", std::nullopt});
    auto data =
        makeRowVector({year, month, day, hour, minute, micros, timeZone});
    // Session time zone will be ignored if time zone is specified in argument.
    auto expected = makeNullableFlatVector<Timestamp>(
        {util::fromTimestampString("2021-07-11 06:30:45.678"),
         util::fromTimestampString("2021-07-11 04:30:45.678"),
         std::nullopt});
    testMakeTimestamp(data, expected, true);
  }
}

TEST_F(MakeTimestampTest, errors) {
  const auto microsType = DECIMAL(16, 6);
  const auto testInvalidInputs = [&](const RowVectorPtr& data) {
    std::vector<std::optional<Timestamp>> nullResults(
        data->size(), std::nullopt);
    auto expected = makeNullableFlatVector<Timestamp>(nullResults);
    auto result = evaluate("make_timestamp(c0, c1, c2, c3, c4, c5)", data);
    facebook::velox::test::assertEqualVectors(expected, result);
  };
  const auto testInvalidSeconds = [&](std::optional<int64_t> microsec) {
    auto result = evaluateOnce<Timestamp, int64_t>(
        "make_timestamp(c0, c1, c2, c3, c4, c5)",
        {1, 1, 1, 1, 1, microsec},
        {INTEGER(), INTEGER(), INTEGER(), INTEGER(), INTEGER(), microsType});
    EXPECT_EQ(result, std::nullopt);
  };
  const auto testInvalidArguments = [&](int64_t microsec,
                                        const TypePtr& microsType) {
    return evaluateOnce<Timestamp, int64_t>(
        "make_timestamp(c0, c1, c2, c3, c4, c5)",
        {1, 1, 1, 1, 1, microsec},
        {INTEGER(), INTEGER(), INTEGER(), INTEGER(), INTEGER(), microsType});
  };

  setQueryTimeZone("Asia/Shanghai");
  // Invalid input returns null.
  const auto year = makeFlatVector<int32_t>(
      {facebook::velox::util::kMinYear - 1,
       facebook::velox::util::kMaxYear + 1,
       1,
       1,
       1,
       1,
       1,
       1});
  const auto month = makeFlatVector<int32_t>({1, 1, 0, 13, 1, 1, 1, 1});
  const auto day = makeFlatVector<int32_t>({1, 1, 1, 1, 0, 32, 1, 1});
  const auto hour = makeFlatVector<int32_t>({1, 1, 1, 1, 1, 1, 25, 1});
  const auto minute = makeFlatVector<int32_t>({1, 1, 1, 1, 1, 1, 1, 61});
  const auto micros =
      makeFlatVector<int64_t>({1, 1, 1, 1, 1, 1, 1, 1}, microsType);
  auto data = makeRowVector({year, month, day, hour, minute, micros});
  testInvalidInputs(data);

  // Seconds should be either in the range of [0,59], or 60 with zero
  // microseconds.
  testInvalidSeconds(61e6);
  testInvalidSeconds(99999999);
  testInvalidSeconds(999999999);
  testInvalidSeconds(60007000);

  // Throw if data type for microseconds is invalid.
  VELOX_ASSERT_THROW(
      testInvalidArguments(1e6, DECIMAL(20, 6)),
      "Seconds must be short decimal type but got DECIMAL(20, 6)");
  VELOX_ASSERT_THROW(
      testInvalidArguments(1e6, DECIMAL(16, 8)),
      "Scalar function signature is not supported: "
      "make_timestamp(INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, "
      "DECIMAL(16, 8)).");
  // Throw if no session time zone.
  setQueryTimeZone("");
  VELOX_ASSERT_THROW(
      testInvalidArguments(60007000, DECIMAL(16, 6)),
      "make_timestamp requires session time zone to be set.");
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
