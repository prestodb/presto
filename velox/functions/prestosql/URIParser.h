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

#include <boost/regex.hpp>
#include "velox/type/StringView.h"

namespace facebook::velox::functions {
namespace detail {
FOLLY_ALWAYS_INLINE StringView submatch(const boost::cmatch& match, int idx) {
  const auto& sub = match[idx];
  return StringView(sub.first, sub.length());
}
} // namespace detail
/// A struct containing the parts of the URI that were extracted during parsing.
/// If the field was not found, it is empty.
///
/// For fields that can contain percent-encoded characters, the `...HasEncoded`
/// flag indicates whether the field contains any percent-encoded characters.
struct URI {
  StringView scheme;
  StringView path;
  bool pathHasEncoded = false;
  StringView query;
  bool queryHasEncoded = false;
  StringView fragment;
  bool fragmentHasEncoded = false;
  StringView host;
  bool hostHasEncoded = false;
  StringView port;
};

/// Parse a URI string into a URI struct according to RFC 3986.
bool parseUri(const StringView& uriStr, URI& uri);

/// If the string starting at str is a valid IPv6 address, returns true and pos
/// is updated to the first character after the IP address. Otherwise returns
/// false and pos is unchanged.
bool tryConsumeIPV6Address(const char* str, const size_t len, int32_t& pos);

template <typename T>
FOLLY_ALWAYS_INLINE bool isMultipleInvalidSequences(
    const T& inputBuffer,
    size_t inputIndex) {
  return
      // 0xe0 followed by a value less than 0xe0 or 0xf0 followed by a
      // value less than 0x90 is considered an overlong encoding.
      (inputBuffer[inputIndex] == '\xe0' &&
       (inputBuffer[inputIndex + 1] & 0xe0) == 0x80) ||
      (inputBuffer[inputIndex] == '\xf0' &&
       (inputBuffer[inputIndex + 1] & 0xf0) == 0x80) ||
      // 0xf4 followed by a byte >= 0x90 looks valid to
      // tryGetUtf8CharLength, but is actually outside the range of valid
      // code points.
      (inputBuffer[inputIndex] == '\xf4' &&
       (inputBuffer[inputIndex + 1] & 0xf0) != 0x80) ||
      // The bytes 0xf5-0xff, 0xc0, and 0xc1 look like the start of
      // multi-byte code points to tryGetUtf8CharLength, but are not part of
      // any valid code point.
      (unsigned char)inputBuffer[inputIndex] > 0xf4 ||
      inputBuffer[inputIndex] == '\xc0' || inputBuffer[inputIndex] == '\xc1';
}

/// Find an extract the value for the parameter with key `param` from the query
/// portion of a URI `query`. `query` should already be decoded if necessary.
template <typename TString>
std::optional<StringView> extractParameter(
    const StringView& query,
    const TString& param) {
  if (!query.empty()) {
    // Parse query string.
    static const boost::regex kQueryParamRegex(
        "(^|&)" // start of query or start of parameter "&"
        "([^=&]*)=?" // parameter name and "=" if value is expected
        "([^&]*)" // parameter value (allows "=" to appear)
        "(?=(&|$))" // forward reference, next should be end of query or
                    // start of next parameter
    );

    const boost::cregex_iterator begin(
        query.data(), query.data() + query.size(), kQueryParamRegex);
    boost::cregex_iterator end;

    for (auto it = begin; it != end; ++it) {
      if (it->length(2) != 0 && (*it)[2].matched) { // key shouldnt be empty.
        auto key = detail::submatch((*it), 2);
        if (param.compare(key) == 0) {
          return detail::submatch((*it), 3);
        }
      }
    }
  }
  return std::nullopt;
}
} // namespace facebook::velox::functions
