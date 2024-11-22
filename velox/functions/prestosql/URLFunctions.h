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
#include "velox/functions/Macros.h"
#include "velox/functions/lib/Utf8Utils.h"
#include "velox/functions/prestosql/URIParser.h"

namespace facebook::velox::functions {

namespace detail {
constexpr std::array<std::string_view, 6> kEncodedReplacementCharacterStrings =
    {"%EF%BF%BD",
     "%EF%BF%BD%EF%BF%BD",
     "%EF%BF%BD%EF%BF%BD%EF%BF%BD",
     "%EF%BF%BD%EF%BF%BD%EF%BF%BD%EF%BF%BD",
     "%EF%BF%BD%EF%BF%BD%EF%BF%BD%EF%BF%BD%EF%BF%BD",
     "%EF%BF%BD%EF%BF%BD%EF%BF%BD%EF%BF%BD%EF%BF%BD%EF%BF%BD"};

FOLLY_ALWAYS_INLINE StringView submatch(const boost::cmatch& match, int idx) {
  const auto& sub = match[idx];
  return StringView(sub.first, sub.length());
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
///  * If the character is invalid UTF-8 each maximal subpart of an
///    ill-formed subsequence (defined below) is converted to %EF%BF%BD.
template <typename TOutString, typename TInString>
FOLLY_ALWAYS_INLINE void urlEscape(TOutString& output, const TInString& input) {
  auto inputSize = input.size();
  // In the worst case every byte is an invalid UTF-8 character.
  output.reserve(inputSize * kEncodedReplacementCharacterStrings[0].size());

  auto inputBuffer = input.data();
  auto outputBuffer = output.data();

  size_t inputIndex = 0;
  size_t outIndex = 0;
  while (inputIndex < inputSize) {
    unsigned char p = inputBuffer[inputIndex];

    if ((p >= 'a' && p <= 'z') || (p >= 'A' && p <= 'Z') ||
        (p >= '0' && p <= '9') || p == '-' || p == '_' || p == '.' ||
        p == '*') {
      outputBuffer[outIndex++] = p;
      inputIndex++;
    } else if (p == ' ') {
      outputBuffer[outIndex++] = '+';
      inputIndex++;
    } else {
      int32_t codePoint;
      const auto charLength = tryGetUtf8CharLength(
          inputBuffer + inputIndex, inputSize - inputIndex, codePoint);
      if (charLength > 0) {
        for (int i = 0; i < charLength; ++i) {
          charEscape(inputBuffer[inputIndex + i], outputBuffer + outIndex);
          outIndex += 3;
        }

        inputIndex += charLength;
      } else {
        // According to the Unicode standard the "maximal subpart of an
        // ill-formed subsequence" is the longest code unit subsequenece that is
        // either well-formed or of length 1. A replacement character should be
        // written for each of these.  In practice tryGetUtf8CharLength breaks
        // most cases into maximal subparts, the exceptions are overlong
        // encodings or subsequences outside the range of valid 4 byte
        // sequences.  In both these cases we should just write out a
        // replacement character for every byte in the sequence.
        size_t replaceCharactersToWriteOut = 1;
        if (inputIndex < inputSize - 1) {
          bool isMultipleInvalidSequences =
              // 0xe0 followed by a value less than 0xe0 or 0xf0 followed by a
              // value less than 0x90 is considered an overlong encoding.
              (inputBuffer[inputIndex] == '\xe0' &&
               (inputBuffer[inputIndex + 1] & 0xe0) == 0x80) ||
              (inputBuffer[inputIndex] == '\xf0' &&
               (inputBuffer[inputIndex + 1] & 0xf0) == 0x80) ||
              // 0xf4 followed by a byte >= 0x90 looks valid to
              // tryGetUtf8CharLength, but is actually outside the range of
              // valid code points.
              (inputBuffer[inputIndex] == '\xf4' &&
               (inputBuffer[inputIndex + 1] & 0xf0) != 0x80) ||
              // The bytes 0xf5-0xff, 0xc0, and 0xc1 look like the start of
              // multi-byte code points to tryGetUtf8CharLength, but are not
              // part of any valid code point.
              (unsigned char)inputBuffer[inputIndex] > 0xf4 ||
              inputBuffer[inputIndex] == '\xc0' ||
              inputBuffer[inputIndex] == '\xc1';

          if (isMultipleInvalidSequences) {
            replaceCharactersToWriteOut = charLength * -1;
          }
        }

        const auto& replacementCharacterString =
            kEncodedReplacementCharacterStrings
                [replaceCharactersToWriteOut - 1];
        std::memcpy(
            outputBuffer + outIndex,
            replacementCharacterString.data(),
            replacementCharacterString.size());
        outIndex += replacementCharacterString.size();

        inputIndex += -charLength;
      }
    }
  }
  output.resize(outIndex);
}

template <typename TOutString, typename TInString, bool unescapePlus = false>
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
    if constexpr (unescapePlus) {
      if (*p == '+') {
        *outputBuffer++ = ' ';
        continue;
      }
    }
    if (*p == '%') {
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
    URI uri;
    if (!parseUri(url, uri)) {
      return false;
    }

    result.setNoCopy(uri.scheme);

    return true;
  }
};

template <typename T>
struct UrlExtractFragmentFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Results refer to strings in the first argument.
  static constexpr int32_t reuse_strings_from_arg = 0;

  // Input is always ASCII, but result may or may not be ASCII.

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& url) {
    URI uri;
    if (!parseUri(url, uri)) {
      return false;
    }

    if (uri.fragmentHasEncoded) {
      detail::urlUnescape(result, uri.fragment);
    } else {
      result.setNoCopy(uri.fragment);
    }

    return true;
  }
};

template <typename T>
struct UrlExtractHostFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Results refer to strings in the first argument.
  static constexpr int32_t reuse_strings_from_arg = 0;

  // Input is always ASCII, but result may or may not be ASCII.

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& url) {
    URI uri;
    if (!parseUri(url, uri)) {
      return false;
    }

    if (uri.hostHasEncoded) {
      detail::urlUnescape(result, uri.host);
    } else {
      result.setNoCopy(uri.host);
    }

    return true;
  }
};

template <typename T>
struct UrlExtractPortFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(int64_t& result, const arg_type<Varchar>& url) {
    URI uri;
    if (!parseUri(url, uri)) {
      return false;
    }

    if (!uri.port.empty()) {
      try {
        result = to<int64_t>(uri.port);
        return true;
      } catch (folly::ConversionError const&) {
      }
    }

    return false;
  }
};

template <typename T>
struct UrlExtractPathFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Input is always ASCII, but result may or may not be ASCII.

  // Results refer to strings in the first argument.
  static constexpr int32_t reuse_strings_from_arg = 0;

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& url) {
    URI uri;
    if (!parseUri(url, uri)) {
      return false;
    }

    if (uri.pathHasEncoded) {
      detail::urlUnescape(result, uri.path);
    } else {
      result.setNoCopy(uri.path);
    }

    return true;
  }
};

template <typename T>
struct UrlExtractQueryFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Results refer to strings in the first argument.
  static constexpr int32_t reuse_strings_from_arg = 0;

  // Input is always ASCII, but result may or may not be ASCII.

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& url) {
    URI uri;
    if (!parseUri(url, uri)) {
      return false;
    }

    if (uri.queryHasEncoded) {
      detail::urlUnescape(result, uri.query);
    } else {
      result.setNoCopy(uri.query);
    }

    return true;
  }
};

template <typename T>
struct UrlExtractParameterFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Results refer to strings in the first argument.
  static constexpr int32_t reuse_strings_from_arg = 0;

  // Input is always ASCII, but result may or may not be ASCII.

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& url,
      const arg_type<Varchar>& param) {
    URI uri;
    if (!parseUri(url, uri)) {
      return false;
    }

    if (!uri.query.empty()) {
      // Parse query string.
      static const boost::regex kQueryParamRegex(
          "(^|&)" // start of query or start of parameter "&"
          "([^=&]*)=?" // parameter name and "=" if value is expected
          "([^=&]*)" // parameter value
          "(?=(&|$))" // forward reference, next should be end of query or
                      // start of next parameter
      );

      const boost::cregex_iterator begin(
          uri.query.data(),
          uri.query.data() + uri.query.size(),
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
    detail::urlUnescape<out_type<Varchar>, arg_type<Varbinary>, true>(
        result, input);
  }
};

} // namespace facebook::velox::functions
