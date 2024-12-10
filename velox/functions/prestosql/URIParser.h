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
#include "velox/functions/lib/Utf8Utils.h"
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
