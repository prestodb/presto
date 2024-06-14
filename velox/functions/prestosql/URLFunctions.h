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
#include <optional>
#include "velox/functions/Macros.h"
#include "velox/functions/lib/string/StringImpl.h"

namespace facebook::velox::functions {

namespace detail {

const auto kScheme = 2;
const auto kAuthority = 3;
const auto kPath = 5;
const auto kQuery = 7;
const auto kFragment = 9;
const auto kHost = 3; // From the authority and path regex.
const auto kPort = 4; // From the authority and path regex.

FOLLY_ALWAYS_INLINE StringView submatch(const boost::cmatch& match, int idx) {
  const auto& sub = match[idx];
  return StringView(sub.first, sub.length());
}

FOLLY_ALWAYS_INLINE bool
parse(const char* rawUrlData, size_t rawUrlsize, boost::cmatch& match) {
  /// This regex is taken from RFC - 3986.
  /// See: https://www.rfc-editor.org/rfc/rfc3986#appendix-B
  /// The basic groups are:
  ///      scheme    = $2
  ///      authority = $4
  ///      path      = $5
  ///      query     = $7
  ///      fragment  = $9
  /// For example a URI like below :
  ///  http://www.ics.uci.edu/pub/ietf/uri/#Related
  ///
  ///   results in the following subexpression matches:
  ///
  ///      $1 = http:
  ///      $2 = http
  ///      $3 = //www.ics.uci.edu
  ///      $4 = www.ics.uci.edu
  ///      $5 = /pub/ietf/uri/
  ///      $6 = <undefined>
  ///      $7 = <undefined>
  ///      $8 = #Related
  ///      $9 = Related
  static const boost::regex kUriRegex(
      "^(([^:\\/?#]+):)?" // scheme:
      "(\\/\\/([^\\/?#]*))?([^?#]*)" // authority and path
      "(\\?([^#]*))?" // ?query
      "(#(.*))?"); // #fragment

  return boost::regex_match(
      rawUrlData, rawUrlData + rawUrlsize, match, kUriRegex);
}

/// Parses the url and returns the matching subgroup if the particular sub group
/// is matched by the call to parse call above.
FOLLY_ALWAYS_INLINE std::optional<StringView> parse(
    StringView rawUrl,
    int subGroup) {
  boost::cmatch match;
  if (!parse(rawUrl.data(), rawUrl.size(), match)) {
    return std::nullopt;
  }

  VELOX_CHECK_LT(subGroup, match.size());

  if (match[subGroup].matched) {
    return submatch(match, subGroup);
  }

  return std::nullopt;
}

FOLLY_ALWAYS_INLINE unsigned char toHex(unsigned char c) {
  return c < 10 ? (c + '0') : (c + 'A' - 10);
}

FOLLY_ALWAYS_INLINE void charEscape(unsigned char c, char* output) {
  output[0] = '%';
  output[1] = toHex(c / 16);
  output[2] = toHex(c % 16);
}

/// Escapes ``input`` by encoding it so that it can be safely included in
/// URL query parameter names and values:
///
///  * Alphanumeric characters are not encoded.
///  * The characters ``.``, ``-``, ``*`` and ``_`` are not encoded.
///  * The ASCII space character is encoded as ``+``.
///  * All other characters are converted to UTF-8 and the bytes are encoded
///    as the string ``%XX`` where ``XX`` is the uppercase hexadecimal
///    value of the UTF-8 byte.
template <typename TOutString, typename TInString>
FOLLY_ALWAYS_INLINE void urlEscape(TOutString& output, const TInString& input) {
  auto inputSize = input.size();
  output.reserve(inputSize * 3);

  auto inputBuffer = input.data();
  auto outputBuffer = output.data();

  size_t outIndex = 0;
  for (auto i = 0; i < inputSize; ++i) {
    unsigned char p = inputBuffer[i];

    if ((p >= 'a' && p <= 'z') || (p >= 'A' && p <= 'Z') ||
        (p >= '0' && p <= '9') || p == '-' || p == '_' || p == '.' ||
        p == '*') {
      outputBuffer[outIndex++] = p;
    } else if (p == ' ') {
      outputBuffer[outIndex++] = '+';
    } else {
      charEscape(p, outputBuffer + outIndex);
      outIndex += 3;
    }
  }
  output.resize(outIndex);
}

/// Performs initial validation of the URI.
/// Checks if the URI contains ascii whitespaces or
/// unescaped '%' chars.
FOLLY_ALWAYS_INLINE bool isValidURI(StringView input) {
  const char* p = input.data();
  const char* end = p + input.size();
  char buf[3];
  buf[2] = '\0';
  char* endptr;
  for (; p < end; ++p) {
    if (stringImpl::isAsciiWhiteSpace(*p)) {
      return false;
    }

    if (*p == '%') {
      if (p + 2 < end) {
        buf[0] = p[1];
        buf[1] = p[2];
        strtol(buf, &endptr, 16);
        p += 2;
        if (endptr != buf + 2) {
          return false;
        }
      } else {
        return false;
      }
    }
  }
  return true;
}

template <typename TOutString, typename TInString>
FOLLY_ALWAYS_INLINE void urlUnescape(
    TOutString& output,
    const TInString& input) {
  auto inputSize = input.size();
  output.reserve(inputSize);

  auto outputBuffer = output.data();
  const char* p = input.data();
  const char* end = p + inputSize;
  char buf[3];
  buf[2] = '\0';
  char* endptr;
  for (; p < end; ++p) {
    if (*p == '+') {
      *outputBuffer++ = ' ';
    } else if (*p == '%') {
      if (p + 2 < end) {
        buf[0] = p[1];
        buf[1] = p[2];
        int val = strtol(buf, &endptr, 16);
        if (endptr == buf + 2) {
          *outputBuffer++ = (char)val;
          p += 2;
        } else {
          VELOX_USER_FAIL(
              "Illegal hex characters in escape (%) pattern: {}", buf);
        }
      } else {
        VELOX_USER_FAIL("Incomplete trailing escape (%) pattern");
      }
    } else {
      *outputBuffer++ = *p;
    }
  }
  output.resize(outputBuffer - output.data());
}

/// Matches the authority (i.e host[:port], ipaddress), and path from a string
/// representing the authority and path. Returns true if the regex matches, and
/// sets the appropriate groups matching authority in authorityMatch.
std::optional<StringView> matchAuthorityAndPath(
    StringView authorityAndPath,
    boost::cmatch& authorityMatch,
    int subGroup);

} // namespace detail

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
    if (!detail::isValidURI(url)) {
      return false;
    }

    if (auto protocol = detail::parse(url, detail::kScheme)) {
      result.setNoCopy(protocol.value());
    } else {
      result.setEmpty();
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
    if (!detail::isValidURI(url)) {
      return false;
    }

    if (auto fragment = detail::parse(url, detail::kFragment)) {
      result.setNoCopy(fragment.value());
    } else {
      result.setEmpty();
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
    if (!detail::isValidURI(url)) {
      return false;
    }

    auto authAndPath = detail::parse(url, detail::kAuthority);
    if (!authAndPath) {
      result.setEmpty();
      return true;
    }
    boost::cmatch authorityMatch;

    if (auto host = detail::matchAuthorityAndPath(
            authAndPath.value(), authorityMatch, detail::kHost)) {
      result.setNoCopy(host.value());
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
    if (!detail::isValidURI(url)) {
      return false;
    }

    auto authAndPath = detail::parse(url, detail::kAuthority);
    if (!authAndPath) {
      return false;
    }

    boost::cmatch authorityMatch;
    if (auto port = detail::matchAuthorityAndPath(
            authAndPath.value(), authorityMatch, detail::kPort)) {
      if (!port.value().empty()) {
        try {
          result = to<int64_t>(port.value());
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

  // Input is always ASCII, but result may or may not be ASCII.

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& url) {
    if (!detail::isValidURI(url)) {
      return false;
    }

    auto path = detail::parse(url, detail::kPath);
    VELOX_USER_CHECK(
        path.has_value(), "Unable to determine path for URL: {}", url);
    detail::urlUnescape(result, path.value());

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
    if (!detail::isValidURI(url)) {
      return false;
    }

    if (auto query = detail::parse(url, detail::kQuery)) {
      result.setNoCopy(query.value());
    } else {
      result.setEmpty();
    }

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
    if (!detail::isValidURI(url)) {
      return false;
    }

    auto query = detail::parse(url, detail::kQuery);
    if (!query) {
      return false;
    }

    if (!query.value().empty()) {
      // Parse query string.
      static const boost::regex kQueryParamRegex(
          "(^|&)" // start of query or start of parameter "&"
          "([^=&]*)=?" // parameter name and "=" if value is expected
          "([^=&]*)" // parameter value
          "(?=(&|$))" // forward reference, next should be end of query or
                      // start of next parameter
      );

      const boost::cregex_iterator begin(
          query.value().data(),
          query.value().data() + query.value().size(),
          kQueryParamRegex);
      boost::cregex_iterator end;

      for (auto it = begin; it != end; ++it) {
        if (it->length(2) != 0 && (*it)[2].matched) { // key shouldnt be empty.
          auto key = detail::submatch((*it), 2);
          if (param.compare(key) == 0) {
            auto value = detail::submatch((*it), 3);
            detail::urlUnescape(result, value);
            return true;
          }
        }
      }
    }

    return false;
  }
};

template <typename T>
struct UrlEncodeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varbinary>& input) {
    detail::urlEscape(result, input);
  }
};

template <typename T>
struct UrlDecodeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varbinary>& input) {
    detail::urlUnescape(result, input);
  }
};

} // namespace facebook::velox::functions
