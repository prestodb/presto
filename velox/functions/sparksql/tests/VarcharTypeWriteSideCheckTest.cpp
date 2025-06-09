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

namespace facebook::velox::functions::sparksql::test {
namespace {

class VarcharTypeWriteSideCheckTest : public SparkFunctionBaseTest {};

TEST_F(VarcharTypeWriteSideCheckTest, varcharTypeWriteSideCheck) {
  const auto varcharTypeWriteSideCheck =
      [&](const std::optional<std::string>& input,
          const std::optional<int32_t>& limit) {
        return evaluateOnce<std::string>(
            "varchar_type_write_side_check(c0, c1)", input, limit);
      };

  // Basic cases - string length <= limit.
  EXPECT_EQ(varcharTypeWriteSideCheck("abc", 3), "abc");
  EXPECT_EQ(varcharTypeWriteSideCheck("ab", 3), "ab");
  EXPECT_EQ(varcharTypeWriteSideCheck("", 5), "");

  // Cases with trailing spaces.
  // Edge cases - input string is longer than limit but trims to exactly limit.
  EXPECT_EQ(varcharTypeWriteSideCheck("abc  ", 3), "abc");
  EXPECT_EQ(varcharTypeWriteSideCheck("abc  ", 4), "abc ");
  EXPECT_EQ(varcharTypeWriteSideCheck("abc  ", 5), "abc  ");

  // Unicode string cases with trailing spaces.
  EXPECT_EQ(varcharTypeWriteSideCheck("世界   ", 2), "世界");
  EXPECT_EQ(varcharTypeWriteSideCheck("世界", 2), "世界");

  // Error cases - string length > limit even after trimming trailing spaces.
  VELOX_ASSERT_USER_THROW(
      varcharTypeWriteSideCheck("abcd", 3),
      "Exceeds allowed length limitation: 3");
  VELOX_ASSERT_USER_THROW(
      varcharTypeWriteSideCheck("世界人", 2),
      "Exceeds allowed length limitation: 2");
  VELOX_ASSERT_USER_THROW(
      varcharTypeWriteSideCheck("abc def", 5),
      "Exceeds allowed length limitation: 5");

  // Null input cases.
  EXPECT_EQ(varcharTypeWriteSideCheck(std::nullopt, 5), std::nullopt);

  // Edge cases - length limit must be positive
  VELOX_ASSERT_USER_THROW(
      varcharTypeWriteSideCheck("abc", 0),
      "The length limit must be greater than 0.");
  VELOX_ASSERT_USER_THROW(
      varcharTypeWriteSideCheck("abc", -1),
      "The length limit must be greater than 0.");

  // Edge cases - input string is all spaces.
  EXPECT_EQ(varcharTypeWriteSideCheck("   ", 2), "  ");
  EXPECT_EQ(varcharTypeWriteSideCheck("   ", 3), "   ");
  EXPECT_EQ(varcharTypeWriteSideCheck("   ", 1), " ");
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
