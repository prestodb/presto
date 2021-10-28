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

#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

namespace facebook::velox::functions::sparksql::test {
namespace {

class EqualNullSafeTest : public SparkFunctionBaseTest {
 protected:
  template <typename T>
  std::optional<bool> equaltonullsafe(std::optional<T> a, std::optional<T> b) {
    return evaluateOnce<bool>("equalnullsafe(c0, c1)", a, b);
  }
};

static constexpr auto kNaN = std::numeric_limits<double>::quiet_NaN();

TEST_F(EqualNullSafeTest, basics) {
  EXPECT_EQ(equaltonullsafe<int64_t>(1, 1), true);
  EXPECT_EQ(equaltonullsafe<int32_t>(1, 2), false);
  EXPECT_EQ(equaltonullsafe<float>(std::nullopt, std::nullopt), true);
  EXPECT_EQ(equaltonullsafe<std::string>(std::nullopt, "abcs"), false);
  EXPECT_EQ(equaltonullsafe<std::string>(std::nullopt, std::nullopt), true);
  EXPECT_EQ(equaltonullsafe<double>(1, std::nullopt), false);
  EXPECT_EQ(equaltonullsafe<double>(kNaN, std::nullopt), false);
  EXPECT_EQ(equaltonullsafe<double>(kNaN, 1), false);
  EXPECT_EQ(equaltonullsafe<double>(kNaN, kNaN), true);
}
} // namespace
}; // namespace facebook::velox::functions::sparksql::test
