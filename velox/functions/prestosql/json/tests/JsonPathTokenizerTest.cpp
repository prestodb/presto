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

#include "velox/functions/prestosql/json/JsonPathTokenizer.h"

#include <vector>
#include "gtest/gtest.h"

using namespace std::string_literals;
using TokenList = std::vector<std::string>;

namespace facebook::velox::functions {
namespace {

std::optional<TokenList> getTokens(std::string_view path) {
  JsonPathTokenizer tokenizer;
  if (!tokenizer.reset(path)) {
    return std::nullopt;
  }

  TokenList tokens;
  while (tokenizer.hasNext()) {
    if (auto token = tokenizer.getNext()) {
      tokens.emplace_back(token.value());
    } else {
      tokens.clear();
      return std::nullopt;
    }
  }
  return tokens;
}

// 'path' should start with '$'.
void assertValidPath(std::string_view path, const TokenList& expectedTokens) {
  auto tokens = getTokens(path);

  EXPECT_TRUE(tokens.has_value()) << "Invalid JSON path: " << path;
  if (!tokens.has_value()) {
    return;
  }

  EXPECT_EQ(expectedTokens, tokens.value());
}

void assertQuotedToken(
    const std::string& token,
    const TokenList& expectedTokens) {
  auto quotedPath = "$[\"" + token + "\"]";
  assertValidPath(quotedPath, expectedTokens);

  auto invalidPath = "$." + token;
  EXPECT_FALSE(getTokens(invalidPath));
}

// The test is ported from Presto for compatibility.
TEST(JsonPathTokenizerTest, validPaths) {
  assertValidPath("$"s, TokenList());
  assertValidPath("$.foo"s, TokenList{"foo"s});
  assertValidPath("$[\"foo\"]"s, TokenList{"foo"s});
  assertValidPath("$[\"foo.bar\"]"s, TokenList{"foo.bar"s});
  assertValidPath("$[42]"s, TokenList{"42"s});
  assertValidPath("$[-1]"s, TokenList{"-1"s});
  assertValidPath("$.42"s, TokenList{"42"s});
  assertValidPath("$.42.63"s, TokenList{"42"s, "63"s});
  assertValidPath("$.foo.42.bar.63"s, TokenList{"foo"s, "42"s, "bar"s, "63"s});
  assertValidPath("$.x.foo"s, TokenList{"x"s, "foo"s});
  assertValidPath("$.x[\"foo\"]"s, TokenList{"x"s, "foo"s});
  assertValidPath("$.x[42]"s, TokenList{"x"s, "42"s});
  assertValidPath("$.foo_42._bar63"s, TokenList{"foo_42"s, "_bar63"s});
  assertValidPath("$[foo_42][_bar63]"s, TokenList{"foo_42"s, "_bar63"s});
  assertValidPath("$.foo:42.:bar63"s, TokenList{"foo:42"s, ":bar63"s});
  assertValidPath(
      "$[\"foo:42\"][\":bar63\"]"s, TokenList{"foo:42"s, ":bar63"s});
  assertValidPath("$['foo:42'][':bar63']"s, TokenList{"foo:42"s, ":bar63"s});
  assertValidPath(
      "$.store.fruit[*].weight",
      TokenList{"store"s, "fruit"s, "*"s, "weight"s});
  assertValidPath(
      "$.store.book[*].author", TokenList{"store", "book", "*", "author"});
  assertValidPath("$.store.*", TokenList({"store", "*"}));

  // Paths without leading '$'.
  assertValidPath("foo", TokenList({"foo"}));
  assertValidPath("foo[12].bar", TokenList({"foo", "12", "bar"}));
  assertValidPath("foo.bar.baz", TokenList({"foo", "bar", "baz"}));
  assertValidPath(R"(["foo"])", TokenList({"foo"}));
  assertValidPath(R"(["foo"].bar)", TokenList({"foo", "bar"}));
  assertValidPath(R"([0][1].foo)", TokenList({"0", "1", "foo"}));

  // Paths with redundant '.'s.
  assertValidPath("$.[0].[1].[2]", TokenList({"0", "1", "2"}));
  assertValidPath("$[0].[1].[2]", TokenList({"0", "1", "2"}));
  assertValidPath("$[0].[1][2]", TokenList({"0", "1", "2"}));
  assertValidPath("$.[0].[1].foo.[2]", TokenList({"0", "1", "foo", "2"}));
  assertValidPath("$[0].[1].foo.[2]", TokenList({"0", "1", "foo", "2"}));
  assertValidPath("$[0][1].foo.[2]", TokenList({"0", "1", "foo", "2"}));

  assertQuotedToken("!@#$%^&*()[]{}/?'"s, TokenList{"!@#$%^&*()[]{}/?'"s});
  assertQuotedToken("ab\u0001c"s, TokenList{"ab\u0001c"s});
  assertQuotedToken("ab\0c"s, TokenList{"ab\0c"s});
  assertQuotedToken("ab\t\n\rc"s, TokenList{"ab\t\n\rc"s});
  assertQuotedToken("."s, TokenList{"."s});
  assertQuotedToken("$"s, TokenList{"$"s});
  assertQuotedToken("]"s, TokenList{"]"s});
  assertQuotedToken("["s, TokenList{"["s});
  assertQuotedToken("'"s, TokenList{"'"s});
  assertQuotedToken(
      "!@#$%^&*(){}[]<>?/|.,`~\r\n\t \0"s,
      TokenList{"!@#$%^&*(){}[]<>?/|.,`~\r\n\t \0"s});
  assertQuotedToken("a\\\\b\\\""s, TokenList{"a\\b\""s});
  assertQuotedToken("ab\\\"cd\\\"ef"s, TokenList{"ab\"cd\"ef"s});
}

TEST(JsonPathTokenizerTest, invalidPaths) {
  JsonPathTokenizer tokenizer;

  // Empty path.
  EXPECT_FALSE(getTokens(""));

  // Backslash not followed by valid escape.
  EXPECT_FALSE(getTokens("$[\"a\\ \"]"s));

  // Colon in subscript must be quoted.
  EXPECT_FALSE(getTokens("$[foo:bar]"s));

  // Open bracket without close bracket.
  EXPECT_FALSE(getTokens("$.store.book["));

  // Unmatched double quote.
  EXPECT_FALSE(getTokens(R"($["foo'])"));

  // Unmatched single quote.
  EXPECT_FALSE(getTokens(R"($['foo"])"));

  // Unsupported deep scan operator.
  EXPECT_FALSE(getTokens("$..store"));
  EXPECT_FALSE(getTokens("..store"));
  EXPECT_FALSE(getTokens("$..[3].foo"));
  EXPECT_FALSE(getTokens("..[3].foo"));

  // Paths without leading '$'.
  EXPECT_FALSE(getTokens(".[1].foo"));
  EXPECT_FALSE(getTokens(".foo.bar.baz"));
}

} // namespace
} // namespace facebook::velox::functions
