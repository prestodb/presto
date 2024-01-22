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

#include "velox/functions/sparksql/DecimalUtil.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::velox::functions::sparksql::test {
namespace {

class DecimalUtilTest : public testing::Test {
 protected:
  template <typename R, typename A, typename B>
  void testDivideWithRoundUp(
      A a,
      B b,
      int32_t aRescale,
      R expectedResult,
      bool expectedOverflow) {
    R r;
    bool overflow = false;
    DecimalUtil::divideWithRoundUp<R, A, B>(r, a, b, aRescale, overflow);
    ASSERT_EQ(overflow, expectedOverflow);
    ASSERT_EQ(r, expectedResult);
  }
};
} // namespace

TEST_F(DecimalUtilTest, divideWithRoundUp) {
  testDivideWithRoundUp<int64_t, int64_t, int64_t>(60, 30, 3, 2000, false);
  testDivideWithRoundUp<int64_t, int64_t, int64_t>(
      6, velox::DecimalUtil::kPowersOfTen[17], 20, 6000, false);
}

TEST_F(DecimalUtilTest, minLeadingZeros) {
  auto result =
      DecimalUtil::minLeadingZeros<int64_t, int64_t>(10000, 6000000, 10, 12);
  ASSERT_EQ(result, 1);

  result = DecimalUtil::minLeadingZeros<int64_t, int128_t>(
      10000, 6'000'000'000'000'000'000, 10, 12);
  ASSERT_EQ(result, 16);

  result = DecimalUtil::minLeadingZeros<int128_t, int128_t>(
      velox::DecimalUtil::kLongDecimalMax,
      velox::DecimalUtil::kLongDecimalMin,
      10,
      12);
  ASSERT_EQ(result, 0);
}
} // namespace facebook::velox::functions::sparksql::test
