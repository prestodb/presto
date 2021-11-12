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
#include <cctype>
#include "velox/functions/Macros.h"

namespace facebook::velox::functions {

namespace {
FOLLY_ALWAYS_INLINE StringView submatch(const boost::cmatch& match, int idx) {
  const auto& sub = match[idx];
  return StringView(sub.first, sub.length());
}

template <typename TInString>
bool parse(const TInString& rawUrl, boost::cmatch& match) {
  static const boost::regex kUriRegex(
      "([a-zA-Z][a-zA-Z0-9+.-]*):" // scheme:
      "([^?#]*)" // authority and path
      "(?:\\?([^#]*))?" // ?query
      "(?:#(.*))?"); // #fragment

  return boost::regex_match(
      rawUrl.data(), rawUrl.data() + rawUrl.size(), match, kUriRegex);
}

} // namespace

bool matchAuthorityAndPath(
    const boost::cmatch& urlMatch,
    boost::cmatch& authAndPathMatch,
    boost::cmatch& authorityMatch,
    bool& hasAuthority);

template <typename T>
struct UrlExtractProtocolFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Results refer to strings in the first argument.
  static constexpr int32_t reuse_strings_from_arg = 0;

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& url) {
    boost::cmatch match;
    if (!parse(url, match)) {
      result.setEmpty();
    } else {
      result.setNoCopy(submatch(match, 1));
    }
    return true;
  }
};

template <typename T>
struct UrlExtractFragmentFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Results refer to strings in the first argument.
  static constexpr int32_t reuse_strings_from_arg = 0;

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& url) {
    boost::cmatch match;
    if (!parse(url, match)) {
      result.setEmpty();
    } else {
      result.setNoCopy(submatch(match, 4));
    }
    return true;
  }
};

template <typename T>
struct UrlExtractHostFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Results refer to strings in the first argument.
  static constexpr int32_t reuse_strings_from_arg = 0;

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& url) {
    boost::cmatch match;
    if (!parse(url, match)) {
      result.setEmpty();
      return true;
    }
    boost::cmatch authAndPathMatch;
    boost::cmatch authorityMatch;
    bool hasAuthority;

    if (matchAuthorityAndPath(
            match, authAndPathMatch, authorityMatch, hasAuthority) &&
        hasAuthority) {
      result.setNoCopy(submatch(authorityMatch, 3));
    } else {
      result.setEmpty();
    }
    return true;
  }
};

template <typename T>
struct UrlExtractPortFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(int64_t& result, const arg_type<Varchar>& url) {
    boost::cmatch match;
    if (!parse(url, match)) {
      return false;
    }

    boost::cmatch authAndPathMatch;
    boost::cmatch authorityMatch;
    bool hasAuthority;
    if (matchAuthorityAndPath(
            match, authAndPathMatch, authorityMatch, hasAuthority) &&
        hasAuthority) {
      auto port = submatch(authorityMatch, 4);
      if (!port.empty()) {
        try {
          result = to<int64_t>(port);
          return true;
        } catch (folly::ConversionError const&) {
        }
      }
    }
    return false;
  }
};

template <typename T>
struct UrlExtractPathFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Results refer to strings in the first argument.
  static constexpr int32_t reuse_strings_from_arg = 0;

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& url) {
    boost::cmatch match;
    if (!parse(url, match)) {
      result.setEmpty();
      return true;
    }

    boost::cmatch authAndPathMatch;
    boost::cmatch authorityMatch;
    bool hasAuthority;

    if (matchAuthorityAndPath(
            match, authAndPathMatch, authorityMatch, hasAuthority)) {
      if (hasAuthority) {
        result.setNoCopy(submatch(authAndPathMatch, 2));
      } else {
        result.setNoCopy(submatch(match, 2));
      }
    }

    return true;
  }
};

template <typename T>
struct UrlExtractQueryFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Results refer to strings in the first argument.
  static constexpr int32_t reuse_strings_from_arg = 0;

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& url) {
    boost::cmatch match;
    if (!parse(url, match)) {
      result.setEmpty();
      return true;
    }

    auto query = submatch(match, 3);
    result.setNoCopy(query);
    return true;
  }
};

template <typename T>
struct UrlExtractParameterFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Results refer to strings in the first argument.
  static constexpr int32_t reuse_strings_from_arg = 0;

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& url,
      const arg_type<Varchar>& param) {
    boost::cmatch match;
    if (!parse(url, match)) {
      result.setEmpty();
      return false;
    }

    auto query = submatch(match, 3);
    if (!query.empty()) {
      // Parse query string.
      static const boost::regex kQueryParamRegex(
          "(^|&)" // start of query or start of parameter "&"
          "([^=&]*)=?" // parameter name and "=" if value is expected
          "([^=&]*)" // parameter value
          "(?=(&|$))" // forward reference, next should be end of query or
                      // start of next parameter
      );

      const boost::cregex_iterator begin(
          query.data(), query.data() + query.size(), kQueryParamRegex);
      boost::cregex_iterator end;

      for (auto it = begin; it != end; ++it) {
        if (it->length(2) != 0) { // key shouldnt be empty.
          auto key = submatch((*it), 2);
          if (param.compare(key) == 0) {
            auto value = submatch((*it), 3);
            result.setNoCopy(value);
            return true;
          }
        }
      }
    }

    return false;
  }
};

} // namespace facebook::velox::functions
