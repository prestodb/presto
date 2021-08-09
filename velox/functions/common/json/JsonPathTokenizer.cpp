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

namespace facebook::velox::functions {

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

bool JsonPathTokenizer::reset(folly::StringPiece path) {
  if (path.empty() || path[0] != ROOT) {
    return false;
  }
  index_ = 1;
  path_ = path;
  return true;
}

bool JsonPathTokenizer::hasNext() const {
  return index_ < path_.size();
}

ParseResult JsonPathTokenizer::getNext() {
  if (match(DOT)) {
    return matchDotKey();
  }
  if (match(OPEN_BRACKET)) {
    auto token =
        match(QUOTE) ? matchQuotedSubscriptKey() : matchUnquotedSubscriptKey();
    if (!token || !match(CLOSE_BRACKET)) {
      return folly::makeUnexpected(false);
    }
    return token;
  }
  return folly::makeUnexpected(false);
}

bool JsonPathTokenizer::match(char expected) {
  if (index_ < path_.size() && path_[index_] == expected) {
    index_++;
    return true;
  }
  return false;
}

ParseResult JsonPathTokenizer::matchDotKey() {
  auto start = index_;
  while ((index_ < path_.size()) && isDotKeyFormat(path_[index_])) {
    index_++;
  }
  if (index_ <= start) {
    return folly::makeUnexpected(false);
  }
  return path_.subpiece(start, index_ - start).str();
}

ParseResult JsonPathTokenizer::matchUnquotedSubscriptKey() {
  auto start = index_;
  while (index_ < path_.size() && isUnquotedBracketKeyFormat(path_[index_])) {
    index_++;
  }
  if (index_ <= start) {
    return folly::makeUnexpected(false);
  }
  return path_.subpiece(start, index_ - start).str();
}

// Reference Presto logic in
// src/test/java/io/prestosql/operator/scalar/TestJsonExtract.java and
// src/main/java/io/prestosql/operator/scalar/JsonExtract.java
ParseResult JsonPathTokenizer::matchQuotedSubscriptKey() {
  bool escaped = false;
  std::string token;
  while ((index_ < path_.size()) && (escaped || path_[index_] != QUOTE)) {
    if (escaped) {
      if (path_[index_] != QUOTE && path_[index_] != BACK_SLASH) {
        return folly::makeUnexpected(false);
      }
      escaped = false;
      token.append(1, path_[index_]);
    } else {
      if (path_[index_] == BACK_SLASH) {
        escaped = true;
      } else if (path_[index_] == QUOTE) {
        return folly::makeUnexpected(false);
      } else {
        token.append(1, path_[index_]);
      }
    }
    index_++;
  }
  if (escaped || token.empty() || !match(QUOTE)) {
    return folly::makeUnexpected(false);
  }
  return token;
}

bool JsonPathTokenizer::isDotKeyFormat(char c) {
  return c == COLON || c == DASH || isUnquotedBracketKeyFormat(c);
}

bool JsonPathTokenizer::isUnquotedBracketKeyFormat(char c) {
  return c == UNDER_SCORE || c == STAR || std::isalnum(c);
}

} // namespace facebook::velox::functions
