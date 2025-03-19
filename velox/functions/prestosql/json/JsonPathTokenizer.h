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
///   @ - current node (but only when specified at the beginning)
///
/// Supports single- and double-quoted keys.
///
/// Notably, doesn't support deep scan, e.g. $..author.
///
/// Jawyay specific behaviour that velox adopts:
///
/// (1) Leading '$.' is optional:
/// Paths '$.foo.bar' and 'foo.bar' are
/// equivalent. This is not part of JSONPath syntax, but this is the behavior of
/// Jayway implementation used by Presto.
///
/// The leading '$' is optional in paths like '$[0]' or '$["foo"]'. Paths '[0]'
/// and '$[0]' are equivalent.  This is not part of JSONPath syntax, but this is
/// the behavior of Jayway implementation used by Presto.
///
/// NOTE: Jayway implementation does not automatically attach a '$.' if the path
/// starts with '@' as '@' points to the current node which if used at the
/// beginning is equivalent to '$'. Velox also honors this.
///
/// (2) Redundant dot-notation:
/// It is allowed to use dot-notation redundantly, e.g. non-standard path
/// '$.[0].foo' is allowed and is equivalent to '$[0].foo'. Similarly, paths
/// '$.[0].[1].[2]' and '$[0].[1][2]' are allowed and equivalent to
/// '$[0][1][2]'.
///
/// NOTE: Jayway changes the interpretation of the string inside a bracket
/// depending on whether a dot precedes the brackets or not. If a dot precedes
/// the brackets, then an unquoted string inside the brackets can only consist
/// of a wildcard or an index. For example, in Jayway, $.foo[bar] is valid, but
/// $.foo.[bar] is not. Therefore, even though $.foo[0] and $.foo.[0] are both
/// valid, in the former, '0' can serve both as a key and an index, but
/// in the latter, '0' can only be an index. In Velox, we do not make this
/// distinction and always treat an unquoted string inside a bracket as both a
/// key and an index.
///
/// (3) String after dot-notation is consumed as if inside quotes:
/// Some special characters in json path syntax like AT '@', SINGLE-QUOTE ''',
/// COLON ':' and DOUBLE-QUOTE '"' are not treated specially and consumed. For
/// eg. '$.$' is equivalent to '$.['$']. Similarly, '$."abc"' is equivalent to
/// '$.["\"abc\""]'
///
/// (4) Whitespaces are ignored between brackets [] and unquoted strings:
/// Jayway only ignores whitespaces for unquoted strings containing only numbers
/// whereas in velox we allow it for other permissible characters as well. For
/// eg. $.[ foo ] is invalid in Jayway but ALLOWED in velox. This was done to
/// make the behavior of ignoring whitespaces more consistent as whitespaces are
/// ignored if there is a quoted string within brackets.
///
/// (5) Dot and Bracket notation are interpreted the same after a wildcard or
///     recursive operator:
/// Jayway changes how it inteprets unquoted strings after dot and bracket
/// notations. That is, tokens after dot notation are considered as either a
/// key(if there is an object) or an index(if there is an array), however
/// in Jayway if this is preceeded by a wildcard then it is only considered as
/// a key. For eg: SELECT json_extract(json '[["array0", "array1"]]',
/// '$.*.0') => return [] in jayway but SELECT json_extract(json '[["array0",
/// "array1"]]', '$.0.0') => returns "array0"
/// Similarly, tokens after bracket notation are also considered as either a
/// key or an index but in Jawyway if this is preceeded by a wildcard
/// then it is only considered as an index. For eg: SELECT json_extract(json
/// '[{"0": "obj"}]', '$.*.[0]') => returns [] in jayway but SELECT
/// json_extract(json '[{"0": "obj"}]', '$.0.[0]') => returns "obj"
/// In Velox, we do not make this distinction and always treat an unquoted
/// string inside a bracket or after a dot as both a key and an index.
///
/// Examples:
///   "$"
///   "$.store.book[0].author"
///   "store.book[0].author"
///   "store.book.[0].author"
///   "$[0].foo.bar"
///   "$[-1]"
///   "[0][1]"
///   "$['store'][book][1]"
///
/// TODO: Add support for optional dot after closing square bracket, eg.
/// $.[0]key should be valid.
class JsonPathTokenizer {
 public:
  enum class Selector { KEY, WILDCARD, KEY_OR_INDEX, RECURSIVE };

  struct Token {
    std::string value;
    Selector selector;

    bool operator==(const Token& other) const {
      return (value == other.value && selector == other.selector);
    }

    std::string toString() const;
  };

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
  std::optional<Token> getNext();

 private:
  bool match(char expected);

  std::optional<Token> matchDotKey();

  std::optional<Token> matchUnquotedSubscriptKey();

  std::optional<Token> matchQuotedSubscriptKey(char quote);

  void skipWhitespace();

  // The index of the next character to process. This is at least one for
  // standard paths that start with '$'. This can be zero if 'reset' was called
  // with a non-standard path that doesn't have a leading '$'.
  size_t index_;

  // The path specified in 'reset'.
  std::string_view path_;

  // Intermediate state variable used to signify that we previous processed the
  // DOT operator. This is used in cases where the path does not start with a
  // '$' where we assume the path starts with '$.' OR when we have just
  // processed the recursive operator '..' and expect either a dotKey or a
  // bracket.
  bool previouslyProcessedDotOp_ = false;
};

} // namespace facebook::velox::functions
