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

class CharTypeWriteSideCheckTest : public SparkFunctionBaseTest {
 protected:
  auto charTypeWriteSideCheck(
      const std::optional<std::string>& input,
      const std::optional<int32_t>& limit) {
    return evaluateOnce<std::string>(
        "char_type_write_side_check(c0, c1)", input, limit);
  }
};

TEST_F(CharTypeWriteSideCheckTest, ascii) {
  // Case 1: String length equals limit (return as-is).
  EXPECT_EQ(charTypeWriteSideCheck("abc", 3), "abc");

  // Case 2: String length < limit (pad with spaces to reach limit).
  EXPECT_EQ(charTypeWriteSideCheck("a", 3), "a  ");
  EXPECT_EQ(charTypeWriteSideCheck("ab", 3), "ab ");
  EXPECT_EQ(charTypeWriteSideCheck("", 3), "   ");

  // Case 3: String length > limit (try trimming trailing spaces).
  // Case 3a: Successful trimming (exactly fits after trimming).
  EXPECT_EQ(charTypeWriteSideCheck("abc  ", 3), "abc");

  // Case 3b: Successful trimming but contain spare spaces to fit the limit.
  EXPECT_EQ(charTypeWriteSideCheck("a   ", 2), "a ");
}

TEST_F(CharTypeWriteSideCheckTest, unicode) {
  // Case 1: String length equals limit (return as-is).
  EXPECT_EQ(charTypeWriteSideCheck("世界", 2), "世界");
  EXPECT_EQ(charTypeWriteSideCheck("a世", 2), "a世");
  EXPECT_EQ(charTypeWriteSideCheck("Привет", 6), "Привет"); // Cyrillic
  EXPECT_EQ(charTypeWriteSideCheck("Γειά", 4), "Γειά"); // Greek

  // Case 2: String length < limit (pad with spaces to reach limit).
  EXPECT_EQ(charTypeWriteSideCheck("世", 3), "世  ");
  EXPECT_EQ(charTypeWriteSideCheck("世界", 3), "世界 ");
  EXPECT_EQ(charTypeWriteSideCheck("При", 6), "При   ");
  EXPECT_EQ(charTypeWriteSideCheck("Γει", 4), "Γει ");

  // Case 3: String length > limit (try trimming trailing spaces).
  EXPECT_EQ(charTypeWriteSideCheck("世界   ", 2), "世界");
  EXPECT_EQ(charTypeWriteSideCheck("a世  ", 2), "a世");
  EXPECT_EQ(charTypeWriteSideCheck("世   ", 2), "世 ");
  EXPECT_EQ(charTypeWriteSideCheck("Привет   ", 6), "Привет");
  EXPECT_EQ(charTypeWriteSideCheck("Γειά ", 4), "Γειά");
}

TEST_F(CharTypeWriteSideCheckTest, error) {
  // Error cases - string length > limit even after trimming trailing spaces.
  VELOX_ASSERT_USER_THROW(
      charTypeWriteSideCheck("abcd", 3),
      "Exceeds allowed length limitation: 3");
  VELOX_ASSERT_USER_THROW(
      charTypeWriteSideCheck("世界人", 2),
      "Exceeds allowed length limitation: 2");
  VELOX_ASSERT_USER_THROW(
      charTypeWriteSideCheck("a世界b", 3),
      "Exceeds allowed length limitation: 3");
  VELOX_ASSERT_USER_THROW(
      charTypeWriteSideCheck("Приветик", 6),
      "Exceeds allowed length limitation: 6");
  VELOX_ASSERT_USER_THROW(
      charTypeWriteSideCheck("Γειάσου", 4),
      "Exceeds allowed length limitation: 4");

  // Edge cases - length limit must be positive.
  VELOX_ASSERT_USER_THROW(
      charTypeWriteSideCheck("a", 0),
      "The length limit must be greater than 0.");
  VELOX_ASSERT_USER_THROW(
      charTypeWriteSideCheck("abc", -1),
      "The length limit must be greater than 0.");
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
