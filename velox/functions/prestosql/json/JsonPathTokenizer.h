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

#pragma once

#include <optional>
#include <string>

namespace facebook::velox::functions {

/// Splits a JSON path into individual tokens.
///
/// Supports a subset of JSONPath syntax:
/// https://goessner.net/articles/JsonPath/
///
///   $ - the root element
///   . or [] - child operator
///   * - wildcard (all objects/elements regardless their names)
///
/// Supports quoted keys.
///
/// Notably, doesn't support deep scan, e.g. $..author.
///
/// The leading '$.' is optional. Paths '$.foo.bar' and 'foo.bar' are
/// equivalent. This is not part of JSONPath syntax, but this is the behavior of
/// Jayway implementation used by Presto.
///
/// It is allowed to use dot-notation redundantly, e.g. non-standard path
/// '$.[0].foo' is allowed and is equivalent to '$[0].foo'. Similarly, paths
/// '$.[0].[1].[2]' and '$[0].[1][2]' are allowed and equivalent to
/// '$[0][1][2]'.
///
/// Examples:
///   "$"
///   "$.store.book[0].author"
///   "store.book[0].author"
///   "store.book.[0].author"
///   "$[0].foo.bar"
class JsonPathTokenizer {
 public:
  /// Resets the tokenizer to a new path. This method must be called and return
  /// true before calling hasNext or getNext.
  ///
  /// @return true if path is not empty and first character suggests a possibly
  /// valid path.
  bool reset(std::string_view path);

  /// Returns true if there are more tokens to be extracted.
  bool hasNext() const;

  /// Returns the next token or std::nullopt if there are no more tokens left or
  /// the remaining path is invalid.
  std::optional<std::string> getNext();

 private:
  bool match(char expected);

  std::optional<std::string> matchDotKey();

  std::optional<std::string> matchUnquotedSubscriptKey();

  std::optional<std::string> matchQuotedSubscriptKey();

  // The index of the next character to process. This is at least one for
  // standard paths that start with '$'. This can be zero if 'reset' was called
  // with a non-standard path that doesn't have a leading '$'.
  size_t index_;

  // The path specified in 'reset'.
  std::string_view path_;
};

} // namespace facebook::velox::functions
