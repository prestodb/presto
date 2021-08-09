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

#pragma once

#include <folly/Expected.h>
#include <folly/Range.h>

namespace facebook::velox::functions {

using ParseResult = folly::Expected<std::string, bool>;

class JsonPathTokenizer {
 public:
  bool reset(folly::StringPiece path);

  bool hasNext() const;

  ParseResult getNext();

 private:
  bool match(char expected);
  ParseResult matchDotKey();

  ParseResult matchUnquotedSubscriptKey();

  ParseResult matchQuotedSubscriptKey();

  bool isDotKeyFormat(char c);

  bool isUnquotedBracketKeyFormat(char c);

 private:
  size_t index_;
  folly::StringPiece path_;
};

} // namespace facebook::velox::functions
