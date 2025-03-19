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

namespace facebook::velox::functions {
using Selector = JsonPathTokenizer::Selector;
using TokenList = std::vector<JsonPathTokenizer::Token>;
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
  EXPECT_EQ(expectedTokens.size(), tokens.value().size());
  for (auto i = 0; i < expectedTokens.size(); ++i) {
    EXPECT_EQ(expectedTokens[i], tokens.value()[i])
        << "Tokens mismatch at " << i << ": Expected "
        << expectedTokens[i].toString() << ", got "
        << tokens.value()[i].toString() << " for path " << path;
  }
}

void assertQuotedToken(
    const std::string& token,
    const TokenList& expectedTokens) {
  auto quotedPath = "$[\"" + token + "\"]";
  assertValidPath(quotedPath, expectedTokens);

  auto invalidPath = "$." + token;
  EXPECT_FALSE(getTokens(invalidPath))
      << "Expected no tokens for path: " << invalidPath;
}

// The test is ported from Presto for compatibility.
TEST(JsonPathTokenizerTest, validPaths) {
  assertValidPath("$"s, TokenList());
  assertValidPath("$.foo"s, TokenList{{"foo", Selector::KEY_OR_INDEX}});
  assertValidPath("$[\"foo\"]"s, TokenList{{"foo", Selector::KEY}});
  assertValidPath("$[\"foo.bar\"]"s, TokenList{{"foo.bar", Selector::KEY}});
  assertValidPath("$[42]"s, TokenList{{"42", Selector::KEY_OR_INDEX}});
  assertValidPath("$[-1]"s, TokenList{{"-1", Selector::KEY_OR_INDEX}});
  assertValidPath("$.42"s, TokenList{{"42", Selector::KEY_OR_INDEX}});
  assertValidPath(
      "$.42.63"s,
      TokenList{
          {"42", Selector::KEY_OR_INDEX}, {"63", Selector::KEY_OR_INDEX}});
  assertValidPath(
      "$.foo.42.bar.63"s,
      TokenList{
          {"foo", Selector::KEY_OR_INDEX},
          {"42", Selector::KEY_OR_INDEX},
          {"bar", Selector::KEY_OR_INDEX},
          {"63", Selector::KEY_OR_INDEX}});
  assertValidPath(
      "$.x.foo"s,
      TokenList{
          {"x", Selector::KEY_OR_INDEX}, {"foo", Selector::KEY_OR_INDEX}});
  assertValidPath(
      "$.x[\"foo\"]"s,
      TokenList{{"x", Selector::KEY_OR_INDEX}, {"foo", Selector::KEY}});
  assertValidPath(
      "$.x[42]"s,
      TokenList{{"x", Selector::KEY_OR_INDEX}, {"42", Selector::KEY_OR_INDEX}});
  assertValidPath(
      "$.foo_42._bar63"s,
      TokenList{
          {"foo_42", Selector::KEY_OR_INDEX},
          {"_bar63", Selector::KEY_OR_INDEX}});
  assertValidPath(
      "$[foo_42][_bar63]"s,
      TokenList{
          {"foo_42", Selector::KEY_OR_INDEX},
          {"_bar63", Selector::KEY_OR_INDEX}});
  assertValidPath(
      "$.foo:42.:bar63"s,
      TokenList{
          {"foo:42", Selector::KEY_OR_INDEX},
          {":bar63", Selector::KEY_OR_INDEX}});
  assertValidPath(
      "$[\"foo:42\"][\":bar63\"]"s,
      TokenList{{"foo:42", Selector::KEY}, {":bar63", Selector::KEY}});
  assertValidPath(
      "$['foo:42'][':bar63']"s,
      TokenList{{"foo:42", Selector::KEY}, {":bar63", Selector::KEY}});
  assertValidPath(
      "$.store.fruit[*].weight",
      TokenList{
          {"store", Selector::KEY_OR_INDEX},
          {"fruit", Selector::KEY_OR_INDEX},
          {"", Selector::WILDCARD},
          {"weight", Selector::KEY_OR_INDEX}});
  assertValidPath(
      "$.store.fruit.[*].weight",
      TokenList{
          {"store", Selector::KEY_OR_INDEX},
          {"fruit", Selector::KEY_OR_INDEX},
          {"", Selector::WILDCARD},
          {"weight", Selector::KEY_OR_INDEX}});
  assertValidPath(
      "$.store.*",
      TokenList{{"store", Selector::KEY_OR_INDEX}, {"", Selector::WILDCARD}});

  // Paths without leading '$'.
  assertValidPath("foo", TokenList{{"foo", Selector::KEY_OR_INDEX}});
  assertValidPath(
      "foo[12].bar",
      TokenList{
          {"foo", Selector::KEY_OR_INDEX},
          {"12", Selector::KEY_OR_INDEX},
          {"bar", Selector::KEY_OR_INDEX}});
  assertValidPath(
      "foo.bar.baz",
      TokenList{
          {"foo", Selector::KEY_OR_INDEX},
          {"bar", Selector::KEY_OR_INDEX},
          {"baz", Selector::KEY_OR_INDEX}});
  assertValidPath(R"(["foo"])", TokenList{{"foo", Selector::KEY}});
  assertValidPath(
      R"(["foo"].bar)",
      TokenList{{"foo", Selector::KEY}, {"bar", Selector::KEY_OR_INDEX}});
  assertValidPath(
      R"([0][1].foo)",
      TokenList{
          {"0", Selector::KEY_OR_INDEX},
          {"1", Selector::KEY_OR_INDEX},
          {"foo", Selector::KEY_OR_INDEX}});

  // Paths with redundant '.'s.
  assertValidPath(
      "$.[0].[1].[2]",
      TokenList{
          {"0", Selector::KEY_OR_INDEX},
          {"1", Selector::KEY_OR_INDEX},
          {"2", Selector::KEY_OR_INDEX}});
  assertValidPath(
      "$[0].[1].[2]",
      TokenList{
          {"0", Selector::KEY_OR_INDEX},
          {"1", Selector::KEY_OR_INDEX},
          {"2", Selector::KEY_OR_INDEX}});
  assertValidPath(
      "$[0].[1][2]",
      TokenList{
          {"0", Selector::KEY_OR_INDEX},
          {"1", Selector::KEY_OR_INDEX},
          {"2", Selector::KEY_OR_INDEX}});
  assertValidPath(
      "$.[0].[1].foo.[2]",
      TokenList{
          {"0", Selector::KEY_OR_INDEX},
          {"1", Selector::KEY_OR_INDEX},
          {"foo", Selector::KEY_OR_INDEX},
          {"2", Selector::KEY_OR_INDEX}});
  assertValidPath(
      "$[0].[1].foo.[2]",
      TokenList{
          {"0", Selector::KEY_OR_INDEX},
          {"1", Selector::KEY_OR_INDEX},
          {"foo", Selector::KEY_OR_INDEX},
          {"2", Selector::KEY_OR_INDEX}});
  assertValidPath(
      "$[0][1].foo.[2]",
      TokenList{
          {"0", Selector::KEY_OR_INDEX},
          {"1", Selector::KEY_OR_INDEX},
          {"foo", Selector::KEY_OR_INDEX},
          {"2", Selector::KEY_OR_INDEX}});

  // * as an identifier or wildcard operator.
  assertValidPath(
      "$[*]",
      TokenList{
          {"", Selector::WILDCARD},
      });
  assertValidPath(
      "$.[*]",
      TokenList{
          {"", Selector::WILDCARD},
      });
  assertValidPath(
      "$.*",
      TokenList{
          {"", Selector::WILDCARD},
      });
  assertValidPath(
      "$['*']",
      TokenList{
          {"*", Selector::KEY},
      });
  assertValidPath(
      "$[*a]",
      TokenList{
          {"*a", Selector::KEY_OR_INDEX},
      });
  assertValidPath(
      "$.[*a]",
      TokenList{
          {"*a", Selector::KEY_OR_INDEX},
      });
  assertValidPath(
      "$['*a']",
      TokenList{
          {"*a", Selector::KEY},
      });
  assertValidPath(
      "$[a*a]",
      TokenList{
          {"a*a", Selector::KEY_OR_INDEX},
      });
  assertValidPath(
      "$.[a*a]",
      TokenList{
          {"a*a", Selector::KEY_OR_INDEX},
      });
  assertValidPath(
      "$.a*a",
      TokenList{
          {"a*a", Selector::KEY_OR_INDEX},
      });
  assertValidPath(
      "$['a*a']",
      TokenList{
          {"a*a", Selector::KEY},
      });
  assertValidPath(
      "$.foo[*]",
      TokenList{
          {"foo", Selector::KEY_OR_INDEX},
          {"", Selector::WILDCARD},
      });
  assertValidPath(
      "$.foo[*].bar",
      TokenList{
          {"foo", Selector::KEY_OR_INDEX},
          {"", Selector::WILDCARD},
          {"bar", Selector::KEY_OR_INDEX},
      });

  // String after dot operator.
  assertValidPath(
      "$.$",
      TokenList{
          {"$", Selector::KEY_OR_INDEX},
      });
  assertValidPath(
      "$.@",
      TokenList{
          {"@", Selector::KEY_OR_INDEX},
      });
  assertValidPath(
      "$.'abc'",
      TokenList{
          {"'abc'", Selector::KEY_OR_INDEX},
      });
  assertValidPath(
      "$.\"abc\"",
      TokenList{
          {"\"abc\"", Selector::KEY_OR_INDEX},
      });
  assertValidPath(
      "$.:",
      TokenList{
          {":", Selector::KEY_OR_INDEX},
      });
  assertValidPath(
      "$.,",
      TokenList{
          {",", Selector::KEY_OR_INDEX},
      });

  // Whitespaces between identifiers and brackets like $.[  1   ].
  assertValidPath(
      "$.foo[  1   ]",
      TokenList{
          {"foo", Selector::KEY_OR_INDEX},
          {"1", Selector::KEY_OR_INDEX},
      });
  assertValidPath(
      "$.foo[1   ]",
      TokenList{
          {"foo", Selector::KEY_OR_INDEX},
          {"1", Selector::KEY_OR_INDEX},
      });
  assertValidPath(
      "$.foo[  1].bar",
      TokenList{
          {"foo", Selector::KEY_OR_INDEX},
          {"1", Selector::KEY_OR_INDEX},
          {"bar", Selector::KEY_OR_INDEX}});
  assertValidPath(
      "$.foo[  'bar'   ]",
      TokenList{
          {"foo", Selector::KEY_OR_INDEX},
          {"bar", Selector::KEY},
      });
  assertValidPath(
      "$.foo['bar'   ]",
      TokenList{
          {"foo", Selector::KEY_OR_INDEX},
          {"bar", Selector::KEY},
      });
  assertValidPath(
      "$.foo[  'bar'].baz",
      TokenList{
          {"foo", Selector::KEY_OR_INDEX},
          {"bar", Selector::KEY},
          {"baz", Selector::KEY_OR_INDEX}});
  assertValidPath(
      "$.foo[  bar   ]",
      TokenList{
          {"foo", Selector::KEY_OR_INDEX},
          {"bar", Selector::KEY_OR_INDEX},
      });
  assertValidPath(
      "$.foo[bar   ]",
      TokenList{
          {"foo", Selector::KEY_OR_INDEX},
          {"bar", Selector::KEY_OR_INDEX},
      });
  assertValidPath(
      "$.foo[  bar].baz",
      TokenList{
          {"foo", Selector::KEY_OR_INDEX},
          {"bar", Selector::KEY_OR_INDEX},
          {"baz", Selector::KEY_OR_INDEX}});

  // Using @ instead of $ at the beginning of the path.
  assertValidPath(
      "@.foo",
      TokenList{
          {"foo", Selector::KEY_OR_INDEX},
      });

  assertQuotedToken(
      "!@#$%^&*()[]{}/?'"s, TokenList{{"!@#$%^&*()[]{}/?'"s, Selector::KEY}});

  assertQuotedToken("ab\t\n\rc"s, TokenList{{"ab\t\n\rc"s, Selector::KEY}});
  assertQuotedToken("."s, TokenList{{"."s, Selector::KEY}});
  assertQuotedToken("["s, TokenList{{"["s, Selector::KEY}});
  assertQuotedToken(
      "!@#$%^&*(){}[]<>?/|.,`~\r\n\t \0"s,
      TokenList{{"!@#$%^&*(){}[]<>?/|.,`~\r\n\t \0"s, Selector::KEY}});
}

TEST(JsonPathTokenizerTest, recursiveOperator) {
  // Used at the start
  assertValidPath(
      "$..foo"s,
      TokenList{
          {"", Selector::RECURSIVE},
          {"foo", Selector::KEY_OR_INDEX},
      });
  assertValidPath(
      ".foo"s,
      TokenList{
          {"", Selector::RECURSIVE},
          {"foo", Selector::KEY_OR_INDEX},
      });
  assertValidPath(
      "$..[foo]"s,
      TokenList{
          {"", Selector::RECURSIVE},
          {"foo", Selector::KEY_OR_INDEX},
      });
  assertValidPath(
      "$..[\"foo\"]"s,
      TokenList{
          {"", Selector::RECURSIVE},
          {"foo", Selector::KEY},
      });

  // Used in conjunction with wildcard
  assertValidPath(
      "$..*"s,
      TokenList{
          {"", Selector::RECURSIVE},
          {"", Selector::WILDCARD},
      });
  assertValidPath(
      ".*"s,
      TokenList{
          {"", Selector::RECURSIVE},
          {"", Selector::WILDCARD},
      });
  assertValidPath(
      "$..[*]"s,
      TokenList{
          {"", Selector::RECURSIVE},
          {"", Selector::WILDCARD},
      });
  assertValidPath(
      "$..[\"*\"]"s,
      TokenList{
          {"", Selector::RECURSIVE},
          {"*", Selector::KEY},
      });

  // Used in between specifiers
  assertValidPath(
      "$.foo..bar"s,
      TokenList{
          {"foo", Selector::KEY_OR_INDEX},
          {"", Selector::RECURSIVE},
          {"bar", Selector::KEY_OR_INDEX},
      });
  assertValidPath(
      "$.foo..[bar]"s,
      TokenList{
          {"foo", Selector::KEY_OR_INDEX},
          {"", Selector::RECURSIVE},
          {"bar", Selector::KEY_OR_INDEX},
      });
  assertValidPath(
      "$.foo..[\"bar\"]"s,
      TokenList{
          {"foo", Selector::KEY_OR_INDEX},
          {"", Selector::RECURSIVE},
          {"bar", Selector::KEY},
      });

  // Used in between specifiers with but before wildcard
  assertValidPath(
      "$.foo..*"s,
      TokenList{
          {"foo", Selector::KEY_OR_INDEX},
          {"", Selector::RECURSIVE},
          {"", Selector::WILDCARD},
      });
  assertValidPath(
      "$.foo..[*]"s,
      TokenList{
          {"foo", Selector::KEY_OR_INDEX},
          {"", Selector::RECURSIVE},
          {"", Selector::WILDCARD},
      });
  assertValidPath(
      "$.foo..[\"*\"]"s,
      TokenList{
          {"foo", Selector::KEY_OR_INDEX},
          {"", Selector::RECURSIVE},
          {"*", Selector::KEY},
      });

  // Used in between specifiers with but after wildcard
  assertValidPath(
      "$.*..bar"s,
      TokenList{
          {"", Selector::WILDCARD},
          {"", Selector::RECURSIVE},
          {"bar", Selector::KEY_OR_INDEX},
      });
  assertValidPath(
      "$.*..[bar]"s,
      TokenList{
          {"", Selector::WILDCARD},
          {"", Selector::RECURSIVE},
          {"bar", Selector::KEY_OR_INDEX},
      });
  assertValidPath(
      "$.*..[\"bar\"]"s,
      TokenList{
          {"", Selector::WILDCARD},
          {"", Selector::RECURSIVE},
          {"bar", Selector::KEY},
      });

  // Used multiple times
  assertValidPath(
      "$..foo..bar"s,
      TokenList{
          {"", Selector::RECURSIVE},
          {"foo", Selector::KEY_OR_INDEX},
          {"", Selector::RECURSIVE},
          {"bar", Selector::KEY_OR_INDEX},
      });
  assertValidPath(
      ".foo..[bar]"s,
      TokenList{
          {"", Selector::RECURSIVE},
          {"foo", Selector::KEY_OR_INDEX},
          {"", Selector::RECURSIVE},
          {"bar", Selector::KEY_OR_INDEX},
      });
  assertValidPath(
      "$..foo..[bar]"s,
      TokenList{
          {"", Selector::RECURSIVE},
          {"foo", Selector::KEY_OR_INDEX},
          {"", Selector::RECURSIVE},
          {"bar", Selector::KEY_OR_INDEX},
      });
  assertValidPath(
      "$..foo..[\"bar\"]"s,
      TokenList{
          {"", Selector::RECURSIVE},
          {"foo", Selector::KEY_OR_INDEX},
          {"", Selector::RECURSIVE},
          {"bar", Selector::KEY},
      });

  // Invalid cases
  EXPECT_FALSE(getTokens("$.."));
  EXPECT_FALSE(getTokens("$..."));
  EXPECT_FALSE(getTokens("$..["));
  EXPECT_FALSE(getTokens("$.foo.."));
  EXPECT_FALSE(getTokens("$.foo..."));
  EXPECT_FALSE(getTokens("$.foo..["));
}

TEST(JsonPathTokenizerTest, dotKeyFormat) {
  assertValidPath(
      "$.~foo~"s,
      TokenList{
          {"~foo~", Selector::KEY_OR_INDEX},
      });
  assertValidPath(
      "$.foo_bar"s,
      TokenList{
          {"foo_bar", Selector::KEY_OR_INDEX},
      });
  assertValidPath(
      "$.foo]"s,
      TokenList{
          {"foo]", Selector::KEY_OR_INDEX},
      });
  assertValidPath(
      "$.foo)"s,
      TokenList{
          {"foo)", Selector::KEY_OR_INDEX},
      });
  assertValidPath(
      "$.foo+bar^fizz*soda&bazz/jazz<opt>"s,
      TokenList{
          {"foo+bar^fizz*soda&bazz/jazz<opt>", Selector::KEY_OR_INDEX},
      });
  assertValidPath(
      "$.$foo"s,
      TokenList{
          {"$foo", Selector::KEY_OR_INDEX},
      });

  assertValidPath(
      "a\\b\""s,
      TokenList{
          {"a\\b\""s, Selector::KEY_OR_INDEX},
      });
  assertValidPath(
      "ab\"cd\"ef"s,
      TokenList{
          {"ab\"cd\"ef"s, Selector::KEY_OR_INDEX},
      });
  assertValidPath(
      "ab\u0001c"s,
      TokenList{
          {"ab\u0001c"s, Selector::KEY_OR_INDEX},
      });
  assertValidPath(
      "$.ab\0c"s,
      TokenList{
          {"ab\0c"s, Selector::KEY_OR_INDEX},
      });
  assertValidPath(
      "屬性"s,
      TokenList{
          {"屬性"s, Selector::KEY_OR_INDEX},
      });

  EXPECT_FALSE(getTokens("$.foo(@!)"));
  EXPECT_FALSE(getTokens("$.fo o"));
  EXPECT_FALSE(getTokens("$. foo"));
  EXPECT_FALSE(getTokens("$.*a"));
}

TEST(JsonPathTokenizerTest, invalidPaths) {
  JsonPathTokenizer tokenizer;

  // Empty path.
  EXPECT_FALSE(getTokens(""));
  EXPECT_FALSE(getTokens("$."));

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

  // Whitespace in after dot operator.
  EXPECT_FALSE(getTokens("$. foo"s));
  EXPECT_FALSE(getTokens("$. 1"s));

  // Unicode characters in unquoted string inside bracket
  EXPECT_FALSE(getTokens("$.[屬性]"));

  // Array Slice
  EXPECT_FALSE(getTokens("$[1:3]"));
  EXPECT_FALSE(getTokens("$[0:4:2]"));
  EXPECT_FALSE(getTokens("$[1:3:]"));
  EXPECT_FALSE(getTokens("$[::2]"));

  // Union
  EXPECT_FALSE(getTokens("$[0,1]"));
  EXPECT_FALSE(getTokens("$['a','a']"));
  EXPECT_FALSE(getTokens("$.*['c','d']"));

  // Filter Expr
  EXPECT_FALSE(getTokens("$[?(@.key)]"));
  EXPECT_FALSE(getTokens("$[?(@.key>0 && false)]"));

  // Script expression
  EXPECT_FALSE(getTokens("$[(@.length-1)]"));
}

} // namespace
} // namespace facebook::velox::functions
