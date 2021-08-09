/*
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

#include "velox/functions/common/json/JsonPathTokenizer.h"

#include <vector>

#include "gtest/gtest.h"

using namespace std::string_literals;
using facebook::velox::functions::JsonPathTokenizer;
using TokenList = std::vector<std::string>;

#define EXPECT_TOKEN_EQ(path, expected)  \
  {                                      \
    auto jsonPath = path;                \
    auto tokens = getTokens(jsonPath);   \
    EXPECT_TRUE(bool(tokens));           \
    EXPECT_EQ(expected, tokens.value()); \
  }

#define EXPECT_TOKEN_INVALID(path) \
  {                                \
    auto tokens = getTokens(path); \
    EXPECT_FALSE(bool(tokens));    \
  }

#define EXPECT_QUOTED_TOKEN_EQ(path, expected) \
  {                                            \
    auto quotedPath = "$[\"" + path + "\"]";   \
    EXPECT_TOKEN_EQ(quotedPath, expected);     \
  }

#define EXPECT_UNQUOTED_TOKEN_INVALID(path) \
  {                                         \
    auto invalidPath = "$." + path;         \
    EXPECT_TOKEN_INVALID(invalidPath);      \
  }

// The test is ported from Presto for compatibility
folly::Expected<TokenList, bool> getTokens(const std::string& path) {
  JsonPathTokenizer tokenizer;
  tokenizer.reset(path);
  TokenList tokens;
  while (tokenizer.hasNext()) {
    if (auto token = tokenizer.getNext()) {
      tokens.push_back(token.value());
    } else {
      tokens.clear();
      return folly::makeUnexpected(false);
    }
  }
  return tokens;
}

TEST(JsonPathTokenizerTest, tokenizeTest) {
  EXPECT_TOKEN_EQ("$"s, TokenList());
  EXPECT_TOKEN_EQ("$.foo"s, TokenList{"foo"s});
  EXPECT_TOKEN_EQ("$[\"foo\"]"s, TokenList{"foo"s});
  EXPECT_TOKEN_EQ("$[\"foo.bar\"]"s, TokenList{"foo.bar"s});
  EXPECT_TOKEN_EQ("$[42]"s, TokenList{"42"s});
  EXPECT_TOKEN_EQ("$.42"s, TokenList{"42"s});
  EXPECT_TOKEN_EQ("$.42.63"s, (TokenList{"42"s, "63"s}));
  EXPECT_TOKEN_EQ(
      "$.foo.42.bar.63"s, (TokenList{"foo"s, "42"s, "bar"s, "63"s}));
  EXPECT_TOKEN_EQ("$.x.foo"s, (TokenList{"x"s, "foo"s}));
  EXPECT_TOKEN_EQ("$.x[\"foo\"]"s, (TokenList{"x"s, "foo"s}));
  EXPECT_TOKEN_EQ("$.x[42]"s, (TokenList{"x"s, "42"s}));
  EXPECT_TOKEN_EQ("$.foo_42._bar63"s, (TokenList{"foo_42"s, "_bar63"s}));
  EXPECT_TOKEN_EQ("$[foo_42][_bar63]"s, (TokenList{"foo_42"s, "_bar63"s}));
  EXPECT_TOKEN_EQ("$.foo:42.:bar63"s, (TokenList{"foo:42"s, ":bar63"s}));
  EXPECT_TOKEN_EQ(
      "$[\"foo:42\"][\":bar63\"]"s, (TokenList{"foo:42"s, ":bar63"s}));
  EXPECT_TOKEN_EQ(
      "$.store.fruit[*].weight",
      (TokenList{"store"s, "fruit"s, "*"s, "weight"s}));
  EXPECT_QUOTED_TOKEN_EQ("!@#$%^&*()[]{}/?'"s, TokenList{"!@#$%^&*()[]{}/?'"s});
  EXPECT_UNQUOTED_TOKEN_INVALID("!@#$%^&*()[]{}/?'"s);
  EXPECT_QUOTED_TOKEN_EQ("ab\u0001c"s, TokenList{"ab\u0001c"s});
  EXPECT_UNQUOTED_TOKEN_INVALID("ab\u0001c"s);
  EXPECT_QUOTED_TOKEN_EQ("ab\0c"s, TokenList{"ab\0c"s});
  EXPECT_UNQUOTED_TOKEN_INVALID("ab\0c"s);
  EXPECT_QUOTED_TOKEN_EQ("ab\t\n\rc"s, TokenList{"ab\t\n\rc"s});
  EXPECT_UNQUOTED_TOKEN_INVALID("ab\t\n\rc"s);
  EXPECT_QUOTED_TOKEN_EQ("."s, TokenList{"."s});
  EXPECT_UNQUOTED_TOKEN_INVALID("."s);
  EXPECT_QUOTED_TOKEN_EQ("$"s, TokenList{"$"s});
  EXPECT_UNQUOTED_TOKEN_INVALID("$"s);
  EXPECT_QUOTED_TOKEN_EQ("]"s, TokenList{"]"s});
  EXPECT_UNQUOTED_TOKEN_INVALID("]"s);
  EXPECT_QUOTED_TOKEN_EQ("["s, TokenList{"["s});
  EXPECT_UNQUOTED_TOKEN_INVALID("["s);
  EXPECT_QUOTED_TOKEN_EQ("'"s, TokenList{"'"s});
  EXPECT_UNQUOTED_TOKEN_INVALID("'"s);
  EXPECT_QUOTED_TOKEN_EQ(
      "!@#$%^&*(){}[]<>?/|.,`~\r\n\t \0"s,
      TokenList{"!@#$%^&*(){}[]<>?/|.,`~\r\n\t \0"s});
  EXPECT_UNQUOTED_TOKEN_INVALID("!@#$%^&*(){}[]<>?/|.,`~\r\n\t \0"s);
  EXPECT_QUOTED_TOKEN_EQ("a\\\\b\\\""s, TokenList{"a\\b\""s});
  EXPECT_UNQUOTED_TOKEN_INVALID("a\\\\b\\\""s);
  EXPECT_QUOTED_TOKEN_EQ("ab\\\"cd\\\"ef"s, TokenList{"ab\"cd\"ef"s});

  // backslash not followed by valid escape
  EXPECT_TOKEN_INVALID("$[\"a\\ \"]"s);

  // colon in subscript must be quoted
  EXPECT_TOKEN_INVALID("$[foo:bar]"s);

  EXPECT_TOKEN_INVALID("$.store.book[");
}
