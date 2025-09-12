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
#include "velox/functions/iceberg/Truncate.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/Expressions.h"
#include "velox/functions/iceberg/tests/IcebergFunctionBaseTest.h"

namespace facebook::velox::functions::iceberg {
namespace {
class TruncateTest : public facebook::velox::functions::iceberg::test::
                         IcebergFunctionBaseTest {
 protected:
  template <typename T>
  std::optional<T> truncate(
      std::optional<int32_t> width,
      std::optional<T> value) {
    return evaluateOnce<T>("truncate(c0, c1)", width, value);
  }

  template <typename T>
  std::optional<T> truncate(
      const TypePtr& type,
      std::optional<int32_t> width,
      std::optional<T> value) {
    return evaluateOnce<T>("truncate(c0, c1)", {INTEGER(), type}, width, value);
  }
};

TEST_F(TruncateTest, tinyInt) {
  EXPECT_EQ(truncate<int8_t>(10, 0), 0);
  EXPECT_EQ(truncate<int8_t>(10, 1), 0);
  EXPECT_EQ(truncate<int8_t>(10, 5), 0);
  EXPECT_EQ(truncate<int8_t>(10, 9), 0);
  EXPECT_EQ(truncate<int8_t>(10, 10), 10);
  EXPECT_EQ(truncate<int8_t>(10, 11), 10);
  EXPECT_EQ(truncate<int8_t>(10, -1), -10);
  EXPECT_EQ(truncate<int8_t>(10, -5), -10);
  EXPECT_EQ(truncate<int8_t>(10, -10), -10);
  EXPECT_EQ(truncate<int8_t>(10, -11), -20);

  // Different widths.
  EXPECT_EQ(truncate<int8_t>(2, -1), -2);

  // Null handling.
  EXPECT_EQ(truncate<int8_t>(10, std::nullopt), std::nullopt);
}

TEST_F(TruncateTest, smallInt) {
  EXPECT_EQ(truncate<int16_t>(10, 0), 0);
  EXPECT_EQ(truncate<int16_t>(10, 1), 0);
  EXPECT_EQ(truncate<int16_t>(10, 5), 0);
  EXPECT_EQ(truncate<int16_t>(10, 9), 0);
  EXPECT_EQ(truncate<int16_t>(10, 10), 10);
  EXPECT_EQ(truncate<int16_t>(10, 11), 10);
  EXPECT_EQ(truncate<int16_t>(10, -1), -10);
  EXPECT_EQ(truncate<int16_t>(10, -5), -10);
  EXPECT_EQ(truncate<int16_t>(10, -10), -10);
  EXPECT_EQ(truncate<int16_t>(10, -11), -20);
  EXPECT_EQ(truncate<int16_t>(7, 22), 21);
  EXPECT_EQ(truncate<int16_t>(2, -1), -2);
  EXPECT_EQ(truncate<int16_t>(10, std::nullopt), std::nullopt);
}

TEST_F(TruncateTest, integer) {
  EXPECT_EQ(truncate<int32_t>(10, 0), 0);
  EXPECT_EQ(truncate<int32_t>(10, 1), 0);
  EXPECT_EQ(truncate<int32_t>(10, 5), 0);
  EXPECT_EQ(truncate<int32_t>(10, 9), 0);
  EXPECT_EQ(truncate<int32_t>(10, 10), 10);
  EXPECT_EQ(truncate<int32_t>(10, 11), 10);
  EXPECT_EQ(truncate<int32_t>(10, -1), -10);
  EXPECT_EQ(truncate<int32_t>(10, -5), -10);
  EXPECT_EQ(truncate<int32_t>(10, -10), -10);
  EXPECT_EQ(truncate<int32_t>(10, -11), -20);

  // Different widths.
  EXPECT_EQ(truncate<int32_t>(2, -1), -2);
  EXPECT_EQ(truncate<int32_t>(300, 1), 0);

  EXPECT_EQ(truncate<int32_t>(10, std::nullopt), std::nullopt);
}

TEST_F(TruncateTest, bigint) {
  EXPECT_EQ(truncate<int64_t>(10, 0), 0);
  EXPECT_EQ(truncate<int64_t>(10, 1), 0);
  EXPECT_EQ(truncate<int64_t>(10, 5), 0);
  EXPECT_EQ(truncate<int64_t>(10, 9), 0);
  EXPECT_EQ(truncate<int64_t>(10, 10), 10);
  EXPECT_EQ(truncate<int64_t>(10, 11), 10);
  EXPECT_EQ(truncate<int64_t>(10, -1), -10);
  EXPECT_EQ(truncate<int64_t>(10, -5), -10);
  EXPECT_EQ(truncate<int64_t>(10, -10), -10);
  EXPECT_EQ(truncate<int64_t>(10, -11), -20);

  EXPECT_EQ(truncate<int64_t>(2, -1), -2);
  EXPECT_EQ(truncate<int64_t>(10, std::nullopt), std::nullopt);
  VELOX_ASSERT_THROW(
      truncate<int64_t>(0, 34),
      "Reason: (0 vs. 0) Invalid truncate width\nExpression: width <= 0");
  VELOX_ASSERT_THROW(
      truncate<int64_t>(-3, 34),
      "Reason: (-3 vs. 0) Invalid truncate width\nExpression: width <= 0");
}

TEST_F(TruncateTest, string) {
  // Basic truncation cases.
  EXPECT_EQ(truncate<std::string>(5, "abcdefg"), "abcde");
  EXPECT_EQ(truncate<std::string>(5, "abc"), "abc");
  EXPECT_EQ(truncate<std::string>(5, "abcde"), "abcde");

  // Empty string handling.
  EXPECT_EQ(truncate<std::string>(10, ""), "");

  // Unicode handling (4-byte characters).
  std::string twoFourByteChars = "\U00010000\U00010000"; // Two 𐀀 characters
  EXPECT_EQ(truncate<std::string>(1, twoFourByteChars), "\U00010000");

  // Mixed character sizes.
  EXPECT_EQ(truncate<std::string>(4, "测试raul试测"), "测试ra");

  // Explicit varchar/char types (treated same as string).
  EXPECT_EQ(truncate<std::string>(4, "测试raul试测"), "测试ra");

  VELOX_ASSERT_THROW(
      truncate<std::string>(0, "abc"),
      "Reason: (0 vs. 0) Invalid truncate width\nExpression: width <= 0");
  VELOX_ASSERT_THROW(
      truncate<std::string>(-3, "abc"),
      "Reason: (-3 vs. 0) Invalid truncate width\nExpression: width <= 0");
  VELOX_ASSERT_THROW(
      truncate<std::string>(0, "测试"),
      "Reason: (0 vs. 0) Invalid truncate width\nExpression: width <= 0");
  VELOX_ASSERT_THROW(
      truncate<std::string>(-3, "测试"),
      "Reason: (-3 vs. 0) Invalid truncate width\nExpression: width <= 0");
}

TEST_F(TruncateTest, unicodeBoundaries) {
  // Japanese characters (3-byte UTF-8).
  std::string japanese = "イロハニホヘト";
  EXPECT_EQ(truncate<std::string>(2, japanese), "イロ");
  EXPECT_EQ(truncate<std::string>(3, japanese), "イロハ");
  EXPECT_EQ(truncate<std::string>(7, japanese), japanese);

  // Chinese characters (3-byte UTF-8).
  EXPECT_EQ(truncate<std::string>(1, "测试"), "测");
}

TEST_F(TruncateTest, binary) {
  EXPECT_EQ(truncate<std::string>(VARBINARY(), 10, "abc\0\0"), "abc\0\0");
  EXPECT_EQ(truncate<std::string>(VARBINARY(), 3, "abcdefg"), "abc");
  EXPECT_EQ(truncate<std::string>(VARBINARY(), 3, "abc"), "abc");
  EXPECT_EQ(truncate<std::string>(VARBINARY(), 6, "测试"), "测试");
}

TEST_F(TruncateTest, decimal) {
  EXPECT_EQ(truncate<int64_t>(DECIMAL(9, 2), 10, 1234), 1230);
  EXPECT_EQ(truncate<int64_t>(DECIMAL(9, 2), 10, 1239), 1230);
  EXPECT_EQ(truncate<int64_t>(DECIMAL(9, 2), 10, 5), 0);
  EXPECT_EQ(truncate<int64_t>(DECIMAL(9, 2), 10, -5), -10);
  EXPECT_EQ(truncate<int64_t>(DECIMAL(9, 3), 10, 12299), 12290);
  EXPECT_EQ(truncate<int64_t>(DECIMAL(9, 3), 1, 1), 1);
  EXPECT_EQ(truncate<int64_t>(DECIMAL(5, 2), 3, 5), 3);
  EXPECT_EQ(truncate<int64_t>(DECIMAL(10, 4), 10, 123453482), 123453480);
  EXPECT_EQ(truncate<int64_t>(DECIMAL(6, 4), 10, -500), -500);
  EXPECT_EQ(truncate<int128_t>(DECIMAL(38, 2), 10, 5), 0);
  EXPECT_EQ(truncate<int128_t>(DECIMAL(38, 2), 10, -5), -10);
}
} // namespace
} // namespace facebook::velox::functions::iceberg
