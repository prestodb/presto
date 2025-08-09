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

#include "velox/functions/lib/Utf8Utils.h"

namespace facebook::velox::functions::sparksql {

/// split(string, delimiter[, limit]) -> array(varchar)
///
/// Splits string on delimiter and returns an array of size at most limit.
/// delimiter is a string representing regular expression.
/// limit is an integer which controls the number of times the regex is applied.
/// By default, limit is -1, which means 'no limit', the delimiter will be
/// applied as many times as possible.
template <typename T>
struct Split {
  Split() : cache_(0) {}

  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Results refer to strings in the first argument.
  static constexpr int32_t reuse_strings_from_arg = 0;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig& config,
      const arg_type<Varchar>* input,
      const arg_type<Varchar>* delimiter) {
    initialize(inputTypes, config, input, delimiter, nullptr);
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*input*/,
      const arg_type<Varchar>* delimiter,
      const arg_type<int32_t>* /*limit*/) {
    cache_.setMaxCompiledRegexes(config.exprMaxCompiledRegexes());
    if (delimiter) {
      initializeFastPath(delimiter->data(), delimiter->size());
      isConstantDelimiter_ = true;
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Array<Varchar>>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& delimiter) {
    doCall(result, input, delimiter, INT32_MAX);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Array<Varchar>>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& delimiter,
      const arg_type<int32_t>& limit) {
    doCall(result, input, delimiter, limit > 0 ? limit : INT32_MAX);
  }

 private:
  void doCall(
      out_type<Array<Varchar>>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& delimiter,
      int32_t limit) {
    if (delimiter.empty()) {
      splitEmptyDelimiter(result, input, limit);
    } else {
      split(result, input, delimiter, limit);
    }
  }

  // Checks if a string represents an octal-escaped ascii character (e.g.,
  // \123). If valid, converts it to its decimal value.
  bool isOctalString(const char* str, char& decimal, size_t size) const {
    if (size > 4 || str[0] != '\\') {
      return false;
    }
    for (int i = 1; i < size; ++i) {
      char ch = str[i];
      if (ch < '0' || ch > '7') {
        return false;
      }
    }
    // The ascii octal number should not exceed 177.
    if (std::stoi(str + 1) > 177) {
      return false;
    }
    decimal = std::stoi(str + 1, nullptr, 8);
    return true;
  }

  // When pattern is empty, split each character out. Since Spark 3.4, when
  // delimiter is empty, the result does not include an empty tail string, e.g.
  // split('abc', '') outputs ["a", "b", "c"] instead of ["a", "b", "c", ""].
  // The result does not include remaining string when limit is smaller than the
  // string size, e.g. split('abc', '', 2) outputs ["a", "b"] instead of ["a",
  // "bc"].
  void splitEmptyDelimiter(
      out_type<Array<Varchar>>& result,
      const arg_type<Varchar>& input,
      int32_t limit) const {
    if (input.size() == 0) {
      result.add_item().setNoCopy(StringView());
      return;
    }

    const size_t end = input.size();
    const char* start = input.data();
    size_t pos = 0;
    int32_t count = 0;
    while (pos < end && count < limit) {
      int32_t codePoint;
      auto charLength = tryGetUtf8CharLength(start + pos, end - pos, codePoint);
      if (charLength <= 0) {
        // Invalid UTF-8 character, the length of the invalid
        // character is the absolute value of result of `tryGetUtf8CharLength`.
        charLength = -charLength;
      }
      result.add_item().setNoCopy(StringView(start + pos, charLength));
      pos += charLength;
      count += 1;
    }
  }

  // Fast path if the delimiter is a
  // (1)one-char string and this character is not one of the RegEx's meta
  // characters ".$|()[{^?*+\\", or
  // (2)two-char string and the first char is the backslash and the second is
  // not the ascii digit or ascii letter, or
  // (3)octal string that falls within the range of ASCII characters.
  void initializeFastPath(const char* delimiterStr, size_t delimiterSize) {
    // Referencing from
    // https://github.com/openjdk/jdk8/blob/master/jdk/src/share/classes/java/lang/String.java#L2325
    // to align with Spark.
    const std::string reservedChars = ".$|()[{^?*+\\";

    if (delimiterSize == 1) {
      singleCharDelimiter_ = delimiterStr[0];
      if (reservedChars.find(singleCharDelimiter_) == std::string::npos) {
        fastPath_ = true;
      }
    } else if (delimiterSize == 2 && delimiterStr[0] == '\\') {
      singleCharDelimiter_ = delimiterStr[1];
      if (!std::isdigit(singleCharDelimiter_) &&
          !std::isalpha(singleCharDelimiter_)) {
        fastPath_ = true;
      }
    } else if (isOctalString(
                   delimiterStr, singleCharDelimiter_, delimiterSize)) {
      fastPath_ = true;
    } else {
      fastPath_ = false;
    }
  }

  // Split with a non-empty delimiter. If limit > 0, The resulting array's
  // length will not be more than limit and the resulting array's last entry
  // will contain all input beyond the last matched regex. If limit <= 0,
  // delimiter will be applied as many times as possible, and the resulting
  // array can be of any size.
  void split(
      out_type<Array<Varchar>>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& delimiter,
      int32_t limit) {
    VELOX_DCHECK(!delimiter.empty(), "Non-empty delimiter is expected");

    // Trivial case of converting string to array with 1 element.
    if (limit == 1) {
      result.add_item().setNoCopy(input);
      return;
    }

    // Splits input string using the delimiter and adds the cutting-off pieces
    // to elements vector until the string's end or the limit is reached.
    int32_t addedElements{0};
    const size_t end = input.size();
    const char* start = input.data();
    size_t pos = 0;

    if (!isConstantDelimiter_) {
      initializeFastPath(delimiter.data(), delimiter.size());
    }

    if (fastPath_) {
      for (size_t i = 0; i < end; ++i) {
        if (start[i] == singleCharDelimiter_) {
          result.add_item().setNoCopy(StringView(start + pos, i - pos));
          pos = i + 1;
          ++addedElements;
          if (addedElements + 1 == limit) {
            break;
          }
        }
      }
    } else {
      auto* re = cache_.findOrCompile(delimiter);
      const auto re2String = re2::StringPiece(start, end);
      re2::StringPiece subMatches[1];
      // Matches a regular expression against a portion of the input string,
      // starting from 'pos' to the end of the input string. The match is not
      // anchored, which means it can start at any position in the string. If a
      // match is found, the matched portion of the string is stored in
      // 'subMatches'. The '1' indicates that we are only interested in the
      // first match found from the current position 'pos' in each iteration of
      // the loop.
      while (re->Match(
          re2String, pos, end, RE2::Anchor::UNANCHORED, subMatches, 1)) {
        const auto fullMatch = subMatches[0];
        auto offset = fullMatch.data() - start;
        const auto size = fullMatch.size();
        if (offset >= end) {
          break;
        }

        // When hitting an empty match, split the character at the current 'pos'
        // of the input string and put it into the result array, followed by an
        // empty tail string at last, e.g., the result array for
        // split('abc','d|') is ["a","b","c",""].
        if (size == 0) {
          int32_t codePoint;
          auto charLength =
              tryGetUtf8CharLength(start + pos, end - pos, codePoint);
          if (charLength <= 0) {
            // Invalid UTF-8 character, the length of the invalid
            // character is the absolute value of result of
            // `tryGetUtf8CharLength`.
            charLength = -charLength;
          }
          offset += charLength;
        }
        result.add_item().setNoCopy(StringView(start + pos, offset - pos));
        pos = offset + size;

        ++addedElements;
        // If the next element should be the last, leave the loop.
        if (addedElements + 1 == limit) {
          break;
        }
      }
    }

    // Add the rest of the string and we are done.
    // Note that the rest of the string can be empty - we still add it.
    result.add_item().setNoCopy(StringView(start + pos, end - pos));
  }

  mutable facebook::velox::functions::detail::ReCache cache_;
  bool fastPath_{false};
  // Single character delimiter for fast path.
  char singleCharDelimiter_;
  bool isConstantDelimiter_{false};
};
} // namespace facebook::velox::functions::sparksql
