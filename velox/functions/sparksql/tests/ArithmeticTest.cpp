/*
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
#include <cstdint>
#include <limits>
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

namespace facebook::velox::functions::sparksql::test {
namespace {

class PmodTest : public SparkFunctionBaseTest {
 protected:
  template <typename T>
  std::optional<T> pmod(std::optional<T> a, std::optional<T> n) {
    return evaluateOnce<T>("pmod(c0, c1)", a, n);
  };
};

TEST_F(PmodTest, int8) {
  EXPECT_EQ(1, pmod<int8_t>(1, 3));
  EXPECT_EQ(2, pmod<int8_t>(-1, 3));
  EXPECT_EQ(1, pmod<int8_t>(3, -2));
  EXPECT_EQ(-1, pmod<int8_t>(-1, -3));
  EXPECT_EQ(std::nullopt, pmod<int8_t>(1, 0));
  EXPECT_EQ(std::nullopt, pmod<int8_t>(std::nullopt, 3));
  EXPECT_EQ(INT8_MAX, pmod<int8_t>(INT8_MAX, INT8_MIN));
  EXPECT_EQ(INT8_MAX - 1, pmod<int8_t>(INT8_MIN, INT8_MAX));
}

TEST_F(PmodTest, int16) {
  EXPECT_EQ(0, pmod<int16_t>(23286, 3881));
  EXPECT_EQ(1026, pmod<int16_t>(-15892, 8459));
  EXPECT_EQ(127, pmod<int16_t>(7849, -297));
  EXPECT_EQ(-8, pmod<int16_t>(-1052, -12));
  EXPECT_EQ(INT16_MAX, pmod<int16_t>(INT16_MAX, INT16_MIN));
  EXPECT_EQ(INT16_MAX - 1, pmod<int16_t>(INT16_MIN, INT16_MAX));
}

TEST_F(PmodTest, int32) {
  EXPECT_EQ(2095, pmod<int32_t>(391819, 8292));
  EXPECT_EQ(8102, pmod<int32_t>(-16848948, 48163));
  EXPECT_EQ(726, pmod<int32_t>(2145613151, -925));
  EXPECT_EQ(-11, pmod<int32_t>(-15181535, -12));
  EXPECT_EQ(INT32_MAX, pmod<int32_t>(INT32_MAX, INT32_MIN));
  EXPECT_EQ(INT32_MAX - 1, pmod<int32_t>(INT32_MIN, INT32_MAX));
}

TEST_F(PmodTest, int64) {
  EXPECT_EQ(0, pmod<int64_t>(4611791058295013614, 2147532562));
  EXPECT_EQ(10807, pmod<int64_t>(-3828032596, 48163));
  EXPECT_EQ(673, pmod<int64_t>(4293096798, -925));
  EXPECT_EQ(-5, pmod<int64_t>(-15181561541535, -23));
  EXPECT_EQ(INT64_MAX, pmod<int64_t>(INT64_MAX, INT64_MIN));
  EXPECT_EQ(INT64_MAX - 1, pmod<int64_t>(INT64_MIN, INT64_MAX));
}

class RemainderTest : public SparkFunctionBaseTest {
 protected:
  template <typename T>
  std::optional<T> remainder(std::optional<T> a, std::optional<T> n) {
    return evaluateOnce<T>("remainder(c0, c1)", a, n);
  };
};

TEST_F(RemainderTest, int8) {
  EXPECT_EQ(1, remainder<int8_t>(1, 3));
  EXPECT_EQ(-1, remainder<int8_t>(-1, 3));
  EXPECT_EQ(1, remainder<int8_t>(3, -2));
  EXPECT_EQ(-1, remainder<int8_t>(-1, -3));
  EXPECT_EQ(std::nullopt, remainder<int8_t>(1, 0));
  EXPECT_EQ(std::nullopt, remainder<int8_t>(std::nullopt, 3));
  EXPECT_EQ(INT8_MAX, remainder<int8_t>(INT8_MAX, INT8_MIN));
  EXPECT_EQ(-1, remainder<int8_t>(INT8_MIN, INT8_MAX));
}

TEST_F(RemainderTest, int16) {
  EXPECT_EQ(0, remainder<int16_t>(23286, 3881));
  EXPECT_EQ(-7433, remainder<int16_t>(-15892, 8459));
  EXPECT_EQ(127, remainder<int16_t>(7849, -297));
  EXPECT_EQ(-8, remainder<int16_t>(-1052, -12));
  EXPECT_EQ(INT16_MAX, remainder<int16_t>(INT16_MAX, INT16_MIN));
  EXPECT_EQ(-1, remainder<int16_t>(INT16_MIN, INT16_MAX));
}

TEST_F(RemainderTest, int32) {
  EXPECT_EQ(2095, remainder<int32_t>(391819, 8292));
  EXPECT_EQ(-40061, remainder<int32_t>(-16848948, 48163));
  EXPECT_EQ(726, remainder<int32_t>(2145613151, -925));
  EXPECT_EQ(-11, remainder<int32_t>(-15181535, -12));
  EXPECT_EQ(INT32_MAX, remainder<int32_t>(INT32_MAX, INT32_MIN));
  EXPECT_EQ(-1, remainder<int32_t>(INT32_MIN, INT32_MAX));
}

TEST_F(RemainderTest, int64) {
  EXPECT_EQ(0, remainder<int64_t>(4611791058295013614, 2147532562));
  EXPECT_EQ(-37356, remainder<int64_t>(-3828032596, 48163));
  EXPECT_EQ(673, remainder<int64_t>(4293096798, -925));
  EXPECT_EQ(-5, remainder<int64_t>(-15181561541535, -23));
  EXPECT_EQ(INT64_MAX, remainder<int64_t>(INT64_MAX, INT64_MIN));
  EXPECT_EQ(-1, remainder<int64_t>(INT64_MIN, INT64_MAX));
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
