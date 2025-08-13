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

class ReadSidePaddingFunctionTest : public SparkFunctionBaseTest {
 protected:
  auto readSidePadding(
      const std::optional<std::string>& input,
      const std::optional<int32_t>& limit) {
    return evaluateOnce<std::string>("read_side_padding(c0, c1)", input, limit);
  }
};

TEST_F(ReadSidePaddingFunctionTest, ascii) {
  // String length < limit: pad with spaces.
  EXPECT_EQ(readSidePadding("a", 3), "a  ");

  // String length >= limit: return as-is.
  EXPECT_EQ(readSidePadding("abc", 3), "abc");
  EXPECT_EQ(readSidePadding("abcd", 3), "abcd");
}

TEST_F(ReadSidePaddingFunctionTest, unicode) {
  // String length < limit: pad with spaces.
  EXPECT_EQ(readSidePadding("世", 3), "世  ");
  EXPECT_EQ(readSidePadding("a世", 3), "a世 ");
  EXPECT_EQ(readSidePadding("Привет", 8), "Привет  "); // Cyrillic
  EXPECT_EQ(readSidePadding("Γειά", 5), "Γειά "); // Greek

  // String length >= limit: return as-is.
  EXPECT_EQ(readSidePadding("世界", 2), "世界");
  EXPECT_EQ(readSidePadding("Γειά", 4), "Γειά");
  EXPECT_EQ(readSidePadding("a世界b", 3), "a世界b");
  EXPECT_EQ(readSidePadding("Приветик", 6), "Приветик");
}

TEST_F(ReadSidePaddingFunctionTest, error) {
  // Edge cases - length limit must be positive.
  VELOX_ASSERT_USER_THROW(
      readSidePadding("a", 0), "The length limit must be greater than 0.");
  VELOX_ASSERT_USER_THROW(
      readSidePadding("abc", -1), "The length limit must be greater than 0.");
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
