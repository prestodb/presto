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

#include <optional>
#include <string>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox::functions::test;

namespace facebook::velox::functions {
namespace {
class WordStemTest : public FunctionBaseTest {
 protected:
  std::string wordStem(const std::string& word, const std::string& lang) {
    return evaluateOnce<std::string>(
               "word_stem(c0, c1)", std::optional(word), std::optional(lang))
        .value();
  }

  std::string wordStem(const std::string& word) {
    return evaluateOnce<std::string>("word_stem(c0)", std::optional(word))
        .value();
  }
};

/// Borrow test cases from Presto Java:
/// https://github.com/prestodb/presto/blob/master/presto-main/src/test/java/com/facebook/presto/operator/scalar/TestWordStemFunction.java
TEST_F(WordStemTest, asciiWord) {
  EXPECT_EQ(wordStem(""), "");
  EXPECT_EQ(wordStem("x"), "x");
  EXPECT_EQ(wordStem("abc"), "abc");
  EXPECT_EQ(wordStem("generally"), "general");
  EXPECT_EQ(wordStem("useful"), "use");
  EXPECT_EQ(wordStem("runs"), "run");
  EXPECT_EQ(wordStem("run"), "run");
  EXPECT_EQ(wordStem("authorized", "en"), "author");
  EXPECT_EQ(wordStem("accessories", "en"), "accessori");
  EXPECT_EQ(wordStem("intensifying", "en"), "intensifi");
  EXPECT_EQ(wordStem("resentment", "en"), "resent");
  EXPECT_EQ(wordStem("faithfulness", "en"), "faith");
  EXPECT_EQ(wordStem("continuerait", "fr"), "continu");
  EXPECT_EQ(wordStem("torpedearon", "es"), "torped");
  EXPECT_EQ(wordStem("quilomtricos", "pt"), "quilomtr");
  EXPECT_EQ(wordStem("pronunziare", "it"), "pronunz");
  EXPECT_EQ(wordStem("auferstnde", "de"), "auferstnd");
}

TEST_F(WordStemTest, invalidLang) {
  VELOX_ASSERT_THROW(
      wordStem("hello", "xx"), "Unsupported stemmer language: xx");
}

TEST_F(WordStemTest, unicodeWord) {
  EXPECT_EQ(
      wordStem(
          "\u004b\u0069\u0074\u0061\u0062\u0131\u006d\u0131\u007a\u0064\u0131",
          "tr"),
      "kitap");
  EXPECT_EQ(
      wordStem("\u0432\u0435\u0441\u0435\u043d\u043d\u0438\u0439", "ru"),
      "\u0432\u0435\u0441\u0435\u043d");
}

} // namespace
} // namespace facebook::velox::functions
