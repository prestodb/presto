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

namespace facebook::velox::functions {

namespace {
const char ROOT = '$';
const char DOT = '.';
const char DASH = '-';
const char DOUBLE_QUOTE = '"';
const char SINGLE_QUOTE = '\'';
const char STAR = '*';
const char BACK_SLASH = '\\';
const char UNDER_SCORE = '_';
const char OPEN_BRACKET = '[';
const char CLOSE_BRACKET = ']';
const char AT = '@';
const char OPEN_PARANTHESIS = '(';

bool isUnquotedBracketKeyFormat(char c) {
  return c == UNDER_SCORE || c == STAR || c == DASH || std::isalnum(c);
}

// Whitespaces are not allowed in dot key format. We check for it explicitly
// outside of this function.
bool isDotKeyFormat(char c) {
  return c != OPEN_BRACKET && c != DOT && c != OPEN_PARANTHESIS;
}

} // namespace

bool JsonPathTokenizer::reset(std::string_view path) {
  previouslyProcessedDotOp_ = false;
  if (path.empty()) {
    index_ = 0;
    path_ = {};
    return false;
  }

  if (path[0] == ROOT || path[0] == AT) {
    index_ = 1;
    path_ = path;
    return true;
  }

  // We are parsing first token in a path that doesn't start with '$'. In this
  // case, we assume the path starts with '$.'. NOTE: This logic converts paths
  // like '.foo' into '$..foo' which uses deep scan operator (..)
  previouslyProcessedDotOp_ = true;
  index_ = 0;
  path_ = path;

  return true;
}

bool JsonPathTokenizer::hasNext() const {
  return index_ < path_.size();
}

std::optional<JsonPathTokenizer::Token> JsonPathTokenizer::getNext() {
  if (previouslyProcessedDotOp_ || match(DOT)) {
    previouslyProcessedDotOp_ = false;
    if (hasNext()) {
      if (match(DOT)) {
        // '..' Recrusive operator
        if (!hasNext() || path_[index_] == DOT) {
          // '..' Recrusive operator cannot exist by itself or followed by a dot
          return std::nullopt;
        }
        previouslyProcessedDotOp_ = true;
        return JsonPathTokenizer::Token{"", Selector::RECURSIVE};
      } else if (path_[index_] != OPEN_BRACKET) {
        return matchDotKey();
      }
    }
    // Simply ignore the '.' followed by '['. This allows non-standard paths
    // like '$.[0].[1].[2]' supported by Jayway / Presto.
  }

  if (match(OPEN_BRACKET)) {
    skipWhitespace();
    auto token = match(DOUBLE_QUOTE)
        ? matchQuotedSubscriptKey(DOUBLE_QUOTE)
        : (match(SINGLE_QUOTE) ? matchQuotedSubscriptKey(SINGLE_QUOTE)
                               : matchUnquotedSubscriptKey());
    skipWhitespace();
    if (!token || !match(CLOSE_BRACKET)) {
      return std::nullopt;
    }
    return token;
  }

  return std::nullopt;
}

bool JsonPathTokenizer::match(char expected) {
  if (hasNext() && path_[index_] == expected) {
    index_++;
    return true;
  }
  return false;
}

std::optional<JsonPathTokenizer::Token> JsonPathTokenizer::matchDotKey() {
  if (hasNext() && path_[index_] == STAR) {
    index_++;
    return JsonPathTokenizer::Token{"", Selector::WILDCARD};
  }
  auto start = index_;
  while (hasNext() && isDotKeyFormat(path_[index_])) {
    if (std::isspace(path_[index_])) {
      return std::nullopt;
    }
    index_++;
  }
  if (index_ == start) {
    return std::nullopt;
  }
  auto key = path_.substr(start, index_ - start);
  return JsonPathTokenizer::Token{std::string(key), Selector::KEY_OR_INDEX};
}

void JsonPathTokenizer::skipWhitespace() {
  while (hasNext() && std::isspace(path_[index_])) {
    index_++;
  }
}

std::optional<JsonPathTokenizer::Token>
JsonPathTokenizer::matchUnquotedSubscriptKey() {
  auto start = index_;
  while (hasNext() && isUnquotedBracketKeyFormat(path_[index_])) {
    index_++;
  }
  if (index_ == start) {
    return std::nullopt;
  }

  auto key = path_.substr(start, index_ - start);
  if (key.size() == 1 && key[0] == STAR) {
    return JsonPathTokenizer::Token{"", Selector::WILDCARD};
  }

  return JsonPathTokenizer::Token{std::string(key), Selector::KEY_OR_INDEX};
}

// Reference Presto logic in
// src/test/java/io/prestosql/operator/scalar/TestJsonExtract.java and
// src/main/java/io/prestosql/operator/scalar/JsonExtract.java
std::optional<JsonPathTokenizer::Token>
JsonPathTokenizer::matchQuotedSubscriptKey(char quote) {
  bool escaped = false;
  std::string token;
  while (hasNext() && (escaped || path_[index_] != quote)) {
    if (escaped) {
      if (path_[index_] != quote && path_[index_] != BACK_SLASH) {
        return std::nullopt;
      }
      escaped = false;
      token.append(1, path_[index_]);
    } else {
      if (path_[index_] == BACK_SLASH) {
        escaped = true;
      } else if (path_[index_] == quote) {
        return std::nullopt;
      } else {
        token.append(1, path_[index_]);
      }
    }
    index_++;
  }
  if (escaped || token.empty() || !match(quote)) {
    return std::nullopt;
  }
  return JsonPathTokenizer::Token{token, Selector::KEY};
}

static std::string SelectorToString(JsonPathTokenizer::Selector selector) {
  switch (selector) {
    case JsonPathTokenizer::Selector::KEY:
      return "KEY";
    case JsonPathTokenizer::Selector::WILDCARD:
      return "WILDCARD";
    case JsonPathTokenizer::Selector::KEY_OR_INDEX:
      return "KEY_OR_INDEX";
    default:
      return "Unknown";
  }
}
std::string JsonPathTokenizer::Token::toString() const {
  return value + " : " + SelectorToString(selector);
}

} // namespace facebook::velox::functions
