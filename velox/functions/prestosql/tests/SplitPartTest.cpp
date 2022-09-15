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
#include <gtest/gtest.h>
#include "velox/expression/Expr.h"
#include "velox/functions/Udf.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/parse/Expressions.h"

namespace facebook::velox::functions::test {

namespace {

// Class to test 'split_part' function.
class SplitPartTest : public FunctionBaseTest {
 protected:
  auto split_part(
      std::optional<std::string> input,
      std::optional<std::string> delim,
      std::optional<int64_t> index) {
    return evaluateOnce<std::string>(
        "split_part(c0, c1, c2)", input, delim, index);
  }
};

// Test split_part function
TEST_F(SplitPartTest, basic) {
  std::vector<std::string> inputStrings;
  std::string delim;
  std::vector<int64_t> indices;
  std::shared_ptr<FlatVector<StringView>> actual;

  // Ascii
  EXPECT_EQ("I", split_part("I,he,she,they", ",", 1));
  EXPECT_EQ("he", split_part("I,he,she,they", ",", 2));
  EXPECT_EQ("she", split_part("I,he,she,they", ",", 3));
  EXPECT_EQ("they", split_part("I,he,she,they", ",", 4));
  EXPECT_FALSE(split_part("I,he,she,they", ",", 5).has_value());
  EXPECT_EQ("one", split_part("one,,,four,", ",", 1));
  EXPECT_EQ("", split_part("one,,,four,", ",", 2));
  EXPECT_EQ("", split_part("one,,,four,", ",", 3));
  EXPECT_EQ("four", split_part("one,,,four,", ",", 4));
  EXPECT_EQ("", split_part("one,,,four,", ",", 5));
  EXPECT_FALSE(split_part("one,,,four,", ",", 6));
  EXPECT_EQ("", split_part("", ",", 1));
  EXPECT_EQ("abc", split_part("abc", ",", 1));

  // Non-ascii
  EXPECT_EQ(
      "синяя слива",
      split_part("синяя сливаలేదా赤いトマトలేదా黃苹果లేదాbrown pear", "లేదా", 1));
  EXPECT_EQ(
      "赤いトマト",
      split_part("синяя сливаలేదా赤いトマトలేదా黃苹果లేదాbrown pear", "లేదా", 2));
  EXPECT_EQ(
      "黃苹果",
      split_part("синяя сливаలేదా赤いトマトలేదా黃苹果లేదాbrown pear", "లేదా", 3));
  EXPECT_EQ(
      "brown pear",
      split_part("синяя сливаలేదా赤いトマトలేదా黃苹果లేదాbrown pear", "లేదా", 4));
  EXPECT_FALSE(
      split_part("синяя сливаలేదా赤いトマトలేదా黃苹果లేదాbrown pear", "లేదా", 5)
          .has_value());
  EXPECT_EQ("зелёное небо", split_part("зелёное небоలేదాలేదాలేదా緑の空లేదా", "లేదా", 1));
  EXPECT_EQ("", split_part("зелёное небоలేదాలేదాలేదా緑の空లేదా", "లేదా", 2));
  EXPECT_EQ("", split_part("зелёное небоలేదాలేదాలేదా緑の空లేదా", "లేదా", 3));
  EXPECT_EQ("緑の空", split_part("зелёное небоలేదాలేదాలేదా緑の空లేదా", "లేదా", 4));
  EXPECT_EQ("", split_part("зелёное небоలేదాలేదాలేదా緑の空లేదా", "లేదా", 5));
  EXPECT_FALSE(split_part("зелёное небоలేదాలేదాలేదా緑の空లేదా", "లేదా", 6));
}
} // namespace
} // namespace facebook::velox::functions::test
