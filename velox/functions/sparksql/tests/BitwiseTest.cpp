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
#include <optional>

#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

namespace facebook::velox::functions::sparksql::test {
using namespace facebook::velox::test;

namespace {

static constexpr auto kMin16 = std::numeric_limits<int16_t>::min();
static constexpr auto kMax16 = std::numeric_limits<int16_t>::max();
static constexpr auto kMin32 = std::numeric_limits<int32_t>::min();
static constexpr auto kMax32 = std::numeric_limits<int32_t>::max();
static constexpr auto kMin64 = std::numeric_limits<int64_t>::min();
static constexpr auto kMax64 = std::numeric_limits<int64_t>::max();
static constexpr int kMaxBits = std::numeric_limits<uint64_t>::digits;

class BitwiseTest : public SparkFunctionBaseTest {
 protected:
  template <typename T>
  std::optional<T> bitwiseAnd(std::optional<T> a, std::optional<T> b) {
    return evaluateOnce<T>("bitwise_and(c0, c1)", a, b);
  }

  template <typename T>
  std::optional<T> bitwiseOr(std::optional<T> a, std::optional<T> b) {
    return evaluateOnce<T>("bitwise_or(c0, c1)", a, b);
  }
};

TEST_F(BitwiseTest, bitwiseAnd) {
  EXPECT_EQ(bitwiseAnd<int32_t>(0, -1), 0);
  EXPECT_EQ(bitwiseAnd<int32_t>(3, 8), 0);
  EXPECT_EQ(bitwiseAnd<int32_t>(-4, 12), 12);
  EXPECT_EQ(bitwiseAnd<int32_t>(60, 21), 20);

  EXPECT_EQ(bitwiseAnd<int16_t>(kMin16, kMax16), 0);
  EXPECT_EQ(bitwiseAnd<int16_t>(kMax16, kMax16), kMax16);
  EXPECT_EQ(bitwiseAnd<int16_t>(kMax16, kMin16), 0);
  EXPECT_EQ(bitwiseAnd<int16_t>(kMin16, kMin16), kMin16);
  EXPECT_EQ(bitwiseAnd<int16_t>(kMax16, 1), 1);
  EXPECT_EQ(bitwiseAnd<int16_t>(kMax16, -1), kMax16);
  EXPECT_EQ(bitwiseAnd<int16_t>(kMin16, 1), 0);
  EXPECT_EQ(bitwiseAnd<int16_t>(kMin16, -1), kMin16);

  EXPECT_EQ(bitwiseAnd<int32_t>(kMin32, kMax32), 0);
  EXPECT_EQ(bitwiseAnd<int32_t>(kMax32, kMax32), kMax32);
  EXPECT_EQ(bitwiseAnd<int32_t>(kMax32, kMin32), 0);
  EXPECT_EQ(bitwiseAnd<int32_t>(kMin32, kMin32), kMin32);
  EXPECT_EQ(bitwiseAnd<int32_t>(kMax32, 1), 1);
  EXPECT_EQ(bitwiseAnd<int32_t>(kMax32, -1), kMax32);
  EXPECT_EQ(bitwiseAnd<int32_t>(kMin32, 1), 0);
  EXPECT_EQ(bitwiseAnd<int32_t>(kMin32, -1), kMin32);

  EXPECT_EQ(bitwiseAnd<int64_t>(kMin64, kMax64), 0);
  EXPECT_EQ(bitwiseAnd<int64_t>(kMax64, kMax64), kMax64);
  EXPECT_EQ(bitwiseAnd<int64_t>(kMax64, kMin64), 0);
  EXPECT_EQ(bitwiseAnd<int64_t>(kMin64, kMin64), kMin64);
  EXPECT_EQ(bitwiseAnd<int64_t>(kMax64, 1), 1);
  EXPECT_EQ(bitwiseAnd<int64_t>(kMax64, -1), kMax64);
  EXPECT_EQ(bitwiseAnd<int64_t>(kMin64, 1), 0);
  EXPECT_EQ(bitwiseAnd<int64_t>(kMin64, -1), kMin64);
}

TEST_F(BitwiseTest, bitwiseOr) {
  EXPECT_EQ(bitwiseOr<int32_t>(0, -1), -1);
  EXPECT_EQ(bitwiseOr<int32_t>(3, 8), 11);
  EXPECT_EQ(bitwiseOr<int32_t>(-4, 12), -4);
  EXPECT_EQ(bitwiseOr<int32_t>(60, 21), 61);

  EXPECT_EQ(bitwiseOr<int16_t>(kMin16, kMax16), -1);
  EXPECT_EQ(bitwiseOr<int16_t>(kMax16, kMax16), kMax16);
  EXPECT_EQ(bitwiseOr<int16_t>(kMax16, kMin16), -1);
  EXPECT_EQ(bitwiseOr<int16_t>(kMin16, kMin16), kMin16);
  EXPECT_EQ(bitwiseOr<int16_t>(kMax16, 1), kMax16);
  EXPECT_EQ(bitwiseOr<int16_t>(kMax16, -1), -1);
  EXPECT_EQ(bitwiseOr<int16_t>(kMin16, 1), kMin16 + 1);
  EXPECT_EQ(bitwiseOr<int16_t>(kMin16, -1), -1);

  EXPECT_EQ(bitwiseOr<int32_t>(kMin32, kMax32), -1);
  EXPECT_EQ(bitwiseOr<int32_t>(kMax32, kMax32), kMax32);
  EXPECT_EQ(bitwiseOr<int32_t>(kMax32, kMin32), -1);
  EXPECT_EQ(bitwiseOr<int32_t>(kMin32, kMin32), kMin32);
  EXPECT_EQ(bitwiseOr<int32_t>(kMax32, 1), kMax32);
  EXPECT_EQ(bitwiseOr<int32_t>(kMax32, -1), -1);
  EXPECT_EQ(bitwiseOr<int32_t>(kMin32, 1), kMin32 + 1);
  EXPECT_EQ(bitwiseOr<int32_t>(kMin32, -1), -1);

  EXPECT_EQ(bitwiseOr<int64_t>(kMin64, kMax64), -1);
  EXPECT_EQ(bitwiseOr<int64_t>(kMax64, kMax64), kMax64);
  EXPECT_EQ(bitwiseOr<int64_t>(kMax64, kMin64), -1);
  EXPECT_EQ(bitwiseOr<int64_t>(kMin64, kMin64), kMin64);
  EXPECT_EQ(bitwiseOr<int64_t>(kMax64, 1), kMax64);
  EXPECT_EQ(bitwiseOr<int64_t>(kMax64, -1), -1);
  EXPECT_EQ(bitwiseOr<int64_t>(kMin64, 1), kMin64 + 1);
  EXPECT_EQ(bitwiseOr<int64_t>(kMin64, -1), -1);
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
