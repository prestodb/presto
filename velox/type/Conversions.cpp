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

#include "velox/type/Conversions.h"
#include "velox/functions/lib/string/StringImpl.h"

DEFINE_bool(
    experimental_enable_legacy_cast,
    false,
    "Experimental feature flag for backward compatibility with previous output"
    " format of type conversions used for casting. This is a temporary solution"
    " that aims to facilitate a seamless transition for users who rely on the"
    " legacy behavior and hence can change in the future.");

namespace facebook::velox::util {

/// folly's tryTo doesn't ignore control characters or other unicode whitespace.
/// We trim the string for control and unicode whitespace
/// from both directions and return a StringView of the result.
StringView trimWhiteSpace(const char* data, size_t length) {
  if (length == 0) {
    return StringView(data, 0);
  }

  int startIndex = 0;
  int endIndex = length - 1;
  const auto end = data + length;
  int size = 0;

  // We need to trim unicode chars and control chars
  // from left side of the string.
  for (auto i = 0; i < length;) {
    size = 0;
    auto isWhiteSpaceOrControlChar = false;

    if (data[i] & 0x80) {
      // Unicode - only check for whitespace.
      auto codePoint = utf8proc_codepoint(data + i, end, size);
      isWhiteSpaceOrControlChar =
          velox::functions::stringImpl::isUnicodeWhiteSpace(codePoint);
    } else {
      // Ascii - Check for both whitespace and control chars
      isWhiteSpaceOrControlChar =
          velox::functions::stringImpl::isAsciiWhiteSpace(data[i]) ||
          (data[i] > 0 && data[i] < 32);
    }

    if (!isWhiteSpaceOrControlChar) {
      startIndex = i;
      break;
    }

    i += size > 0 ? size : 1;
  }

  // Trim whitespace from right side.
  for (auto i = length - 1; i > startIndex;) {
    size = 0;
    auto isWhiteSpaceOrControlChar = false;

    if (data[i] & 0x80) {
      // Unicode - only check for whitespace.
      utf8proc_int32_t codePoint;
      // Find the right codepoint
      while ((codePoint = utf8proc_codepoint(data + i, end, size)) < 0 &&
             i > startIndex) {
        i--;
      }
      isWhiteSpaceOrControlChar =
          velox::functions::stringImpl::isUnicodeWhiteSpace(codePoint);
    } else {
      // Ascii - check if control char or whitespace
      isWhiteSpaceOrControlChar =
          velox::functions::stringImpl::isAsciiWhiteSpace(data[i]) ||
          (data[i] > 0 && data[i] < 32);
    }

    if (!isWhiteSpaceOrControlChar) {
      endIndex = i;
      break;
    }

    if (i > 0) {
      i--;
    }
  }

  // If we end on a unicode char make sure we add that to the end.
  auto charSize = size > 0 ? size : 1;
  return StringView(data + startIndex, endIndex - startIndex + charSize);
}

} // namespace facebook::velox::util
