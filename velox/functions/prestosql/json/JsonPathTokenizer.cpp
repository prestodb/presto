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
const char COLON = ':';
const char DASH = '-';
const char QUOTE = '"';
const char STAR = '*';
const char BACK_SLASH = '\\';
const char UNDER_SCORE = '_';
const char OPEN_BRACKET = '[';
const char CLOSE_BRACKET = ']';

bool isUnquotedBracketKeyFormat(char c) {
  return c == UNDER_SCORE || c == STAR || std::isalnum(c);
}

bool isDotKeyFormat(char c) {
  return c == COLON || c == DASH || isUnquotedBracketKeyFormat(c);
}

} // namespace

bool JsonPathTokenizer::reset(std::string_view path) {
  if (path.empty()) {
    index_ = 0;
    path_ = {};
    return false;
  }

  if (path[0] == ROOT) {
    index_ = 1;
    path_ = path;
    return true;
  }

  // Presto supplements basic JsonPath implementation with Jayway engine,
  // which allows paths that do not start with '$'. Jayway simply prepends
  // such paths with '$.'. This logic supports paths like 'foo' by converting
  // them to '$.foo'. However, this logic converts paths like '.foo' into
  // '$..foo' which uses deep scan operator (..). This changes the meaning of
  // the path in unexpected way.
  //
  // Here, we allow paths like 'foo', 'foo.bar' and similar. We do not allow
  // paths like '.foo' or '[0]'.

  index_ = 0;
  if (path[0] == DOT || path[0] == OPEN_BRACKET) {
    path_ = {};
    return false;
  }
  path_ = path;

  return true;
}

bool JsonPathTokenizer::hasNext() const {
  return index_ < path_.size();
}

std::optional<std::string> JsonPathTokenizer::getNext() {
  if (index_ == 0) {
    // We are parsing first token in a path that doesn't start with '$'. In
    // this case, we assume the path starts with '$.'.
    return matchDotKey();
  }

  if (match(DOT)) {
    if (hasNext() && path_[index_] != OPEN_BRACKET) {
      return matchDotKey();
    }
    // Simply ignore the '.' followed by '['. This allows non-standard paths
    // like '$.[0].[1].[2]' supported by Jayway / Presto.
  }

  if (match(OPEN_BRACKET)) {
    auto token =
        match(QUOTE) ? matchQuotedSubscriptKey() : matchUnquotedSubscriptKey();
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

std::optional<std::string> JsonPathTokenizer::matchDotKey() {
  auto start = index_;
  while (hasNext() && isDotKeyFormat(path_[index_])) {
    index_++;
  }
  if (index_ == start) {
    return std::nullopt;
  }
  return std::string(path_.substr(start, index_ - start));
}

std::optional<std::string> JsonPathTokenizer::matchUnquotedSubscriptKey() {
  auto start = index_;
  while (hasNext() && isUnquotedBracketKeyFormat(path_[index_])) {
    index_++;
  }
  if (index_ == start) {
    return std::nullopt;
  }
  return std::string(path_.substr(start, index_ - start));
}

// Reference Presto logic in
// src/test/java/io/prestosql/operator/scalar/TestJsonExtract.java and
// src/main/java/io/prestosql/operator/scalar/JsonExtract.java
std::optional<std::string> JsonPathTokenizer::matchQuotedSubscriptKey() {
  bool escaped = false;
  std::string token;
  while (hasNext() && (escaped || path_[index_] != QUOTE)) {
    if (escaped) {
      if (path_[index_] != QUOTE && path_[index_] != BACK_SLASH) {
        return std::nullopt;
      }
      escaped = false;
      token.append(1, path_[index_]);
    } else {
      if (path_[index_] == BACK_SLASH) {
        escaped = true;
      } else if (path_[index_] == QUOTE) {
        return std::nullopt;
      } else {
        token.append(1, path_[index_]);
      }
    }
    index_++;
  }
  if (escaped || token.empty() || !match(QUOTE)) {
    return std::nullopt;
  }
  return token;
}

} // namespace facebook::velox::functions
