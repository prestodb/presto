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

class UpperLowerTest : public SparkFunctionBaseTest {
 protected:
  std::optional<std::string> lower(std::optional<std::string> str) {
    return evaluateOnce<std::string>("lower(c0)", str);
  }

  std::optional<std::string> upper(std::optional<std::string> str) {
    return evaluateOnce<std::string>("upper(c0)", str);
  }
};

TEST_F(UpperLowerTest, lowerAscii) {
  EXPECT_EQ("abcdefg", lower("ABCDEFG"));
  EXPECT_EQ("abcdefg", lower("abcdefg"));
  EXPECT_EQ("a b c d e f g", lower("a B c D e F g"));
}

TEST_F(UpperLowerTest, lowerUnicode) {
  EXPECT_EQ(
      "àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþ",
      lower("ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞ"));
  EXPECT_EQ("αβγδεζηθικλμνξοπρσστυφχψ", lower("ΑΒΓΔΕΖΗΘΙΚΛΜΝΞΟΠΡΣΣΤΥΦΧΨ"));
  EXPECT_EQ(
      "абвгдежзийклмнопрстуфхцчшщъыьэюя",
      lower("АБВГДЕЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ"));
  EXPECT_EQ("i\xCC\x87", lower("\u0130"));
  EXPECT_EQ("i\xCC\x87", lower("I\xCC\x87"));
  EXPECT_EQ("\u010B", lower("\u010A"));
  EXPECT_EQ("\u0117", lower("\u0116"));
  EXPECT_EQ("\u0121", lower("\u0120"));
  EXPECT_EQ("\u017C", lower("\u017B"));
  EXPECT_EQ("\u0227", lower("\u0226"));
  EXPECT_EQ("\u022F", lower("\u022E"));
  EXPECT_EQ("\u1E03", lower("\u1E02"));
  EXPECT_EQ("\u1E0B", lower("\u1E0A"));
  EXPECT_EQ("\u1E1F", lower("\u1E1E"));
  EXPECT_EQ("\u1E23", lower("\u1E22"));
  EXPECT_EQ("\u1E41", lower("\u1E40"));
  EXPECT_EQ("\u1E45", lower("\u1E44"));
  EXPECT_EQ("\u1E57", lower("\u1E56"));
  EXPECT_EQ("\u1E59", lower("\u1E58"));
  EXPECT_EQ("\u1E61", lower("\u1E60"));
  EXPECT_EQ("\u1E65", lower("\u1E64"));
  EXPECT_EQ("\u1E67", lower("\u1E66"));
  EXPECT_EQ("\u1E69", lower("\u1E68"));
  EXPECT_EQ("\u1E6B", lower("\u1E6A"));
  EXPECT_EQ("\u1E87", lower("\u1E86"));
  EXPECT_EQ("\u1E8B", lower("\u1E8A"));
  EXPECT_EQ("\u1E8F", lower("\u1E8E"));
}

TEST_F(UpperLowerTest, upperAscii) {
  EXPECT_EQ("ABCDEFG", upper("abcdefg"));
  EXPECT_EQ("ABCDEFG", upper("ABCDEFG"));
  EXPECT_EQ("A B C D E F G", upper("a B c D e F g"));
}

TEST_F(UpperLowerTest, upperUnicode) {
  EXPECT_EQ(
      "ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞ",
      upper("àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþ"));
  EXPECT_EQ("ΑΒΓΔΕΖΗΘΙΚΛΜΝΞΟΠΡΣΣΤΥΦΧΨ", upper("αβγδεζηθικλμνξοπρςστυφχψ"));
  EXPECT_EQ(
      "АБВГДЕЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ",
      upper("абвгдежзийклмнопрстуфхцчшщъыьэюя"));
  EXPECT_EQ("\u0049", upper("\u0069"));
  EXPECT_EQ("I\xCC\x87", upper("i\xCC\x87"));
  EXPECT_EQ("\u010A", upper("\u010B"));
  EXPECT_EQ("\u0116", upper("\u0117"));
  EXPECT_EQ("\u0120", upper("\u0121"));
  EXPECT_EQ("\u017B", upper("\u017C"));
  EXPECT_EQ("\u0226", upper("\u0227"));
  EXPECT_EQ("\u022E", upper("\u022F"));
  EXPECT_EQ("\u1E02", upper("\u1E03"));
  EXPECT_EQ("\u1E0A", upper("\u1E0B"));
  EXPECT_EQ("\u1E1E", upper("\u1E1F"));
  EXPECT_EQ("\u1E22", upper("\u1E23"));
  EXPECT_EQ("\u1E40", upper("\u1E41"));
  EXPECT_EQ("\u1E44", upper("\u1E45"));
  EXPECT_EQ("\u1E56", upper("\u1E57"));
  EXPECT_EQ("\u1E58", upper("\u1E59"));
  EXPECT_EQ("\u1E60", upper("\u1E61"));
  EXPECT_EQ("\u1E64", upper("\u1E65"));
  EXPECT_EQ("\u1E66", upper("\u1E67"));
  EXPECT_EQ("\u1E68", upper("\u1E69"));
  EXPECT_EQ("\u1E6A", upper("\u1E6B"));
  EXPECT_EQ("\u1E86", upper("\u1E87"));
  EXPECT_EQ("\u1E8A", upper("\u1E8B"));
  EXPECT_EQ("\u1E8E", upper("\u1E8F"));
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
