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

#include "velox/functions/prestosql/Utf8Utils.h"

namespace facebook::velox::functions::sparksql {

// mask(string) -> string
// mask(string, upperChar) -> string
// mask(string, upperChar, lowerChar) -> string
// mask(string, upperChar, lowerChar, digitChar) -> string
// mask(string, upperChar, lowerChar, digitChar, otherChar) -> string
//
// Masks the characters of the given string value with the provided specific
// characters respectively. Upper-case characters are replaced with the second
// argument. Default value is 'X'. Lower-case characters are replaced with the
// third argument. Default value is 'x'. Digit characters are replaced with the
// fourth argument. Default value is 'n'. Other characters are replaced with the
// last argument. Default value is NULL and the original character is retained.
// If the provided nth argument is NULL, the related original character is
// retained.
template <typename T>
struct MaskFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input) {
    doCall(
        result,
        std::string_view(input),
        kMaskedUpperCase_,
        kMaskedLowerCase_,
        kMaskedDigit_,
        std::nullopt);
  }

  FOLLY_ALWAYS_INLINE bool callNullable(
      out_type<Varchar>& result,
      const arg_type<Varchar>* inputPtr,
      const arg_type<Varchar>* upperCharPtr) {
    if (inputPtr == nullptr) {
      return false;
    }

    doCall(
        result,
        std::string_view(*inputPtr),
        getMaskedChar(upperCharPtr),
        kMaskedLowerCase_,
        kMaskedDigit_,
        std::nullopt);
    return true;
  }

  FOLLY_ALWAYS_INLINE bool callNullable(
      out_type<Varchar>& result,
      const arg_type<Varchar>* inputPtr,
      const arg_type<Varchar>* upperCharPtr,
      const arg_type<Varchar>* lowerCharPtr) {
    if (inputPtr == nullptr) {
      return false;
    }

    doCall(
        result,
        std::string_view(*inputPtr),
        getMaskedChar(upperCharPtr),
        getMaskedChar(lowerCharPtr),
        kMaskedDigit_,
        std::nullopt);
    return true;
  }

  FOLLY_ALWAYS_INLINE bool callNullable(
      out_type<Varchar>& result,
      const arg_type<Varchar>* inputPtr,
      const arg_type<Varchar>* upperCharPtr,
      const arg_type<Varchar>* lowerCharPtr,
      const arg_type<Varchar>* digitCharPtr) {
    if (inputPtr == nullptr) {
      return false;
    }

    doCall(
        result,
        std::string_view(*inputPtr),
        getMaskedChar(upperCharPtr),
        getMaskedChar(lowerCharPtr),
        getMaskedChar(digitCharPtr),
        std::nullopt);
    return true;
  }

  FOLLY_ALWAYS_INLINE bool callNullable(
      out_type<Varchar>& result,
      const arg_type<Varchar>* inputPtr,
      const arg_type<Varchar>* upperCharPtr,
      const arg_type<Varchar>* lowerCharPtr,
      const arg_type<Varchar>* digitCharPtr,
      const arg_type<Varchar>* otherCharPtr) {
    if (inputPtr == nullptr) {
      return false;
    }

    doCall(
        result,
        std::string_view(*inputPtr),
        getMaskedChar(upperCharPtr),
        getMaskedChar(lowerCharPtr),
        getMaskedChar(digitCharPtr),
        getMaskedChar(otherCharPtr));
    return true;
  }

 private:
  void doCall(
      out_type<Varchar>& result,
      std::string_view input,
      const std::optional<std::string_view> upperChar,
      const std::optional<std::string_view> lowerChar,
      const std::optional<std::string_view> digitChar,
      const std::optional<std::string_view> otherChar) const {
    auto inputBuffer = input.data();
    const size_t inputSize = input.size();
    result.reserve(inputSize * 4);
    auto outputBuffer = result.data();
    size_t inputIdx = 0;
    size_t outputIdx = 0;
    while (inputIdx < inputSize) {
      int charByteSize;
      auto curCodePoint = utf8proc_codepoint(
          &inputBuffer[inputIdx], inputBuffer + inputSize, charByteSize);
      if (curCodePoint == -1) {
        // That means it is a invalid UTF-8 character for example '\xED',
        // treat it as char with size 1.
        charByteSize = 1;
      }
      auto maskedChar = &inputBuffer[inputIdx];
      auto maskedCharByteSize = charByteSize;
      // Treat invalid UTF-8 character as other char.
      utf8proc_propval_t category = utf8proc_category(curCodePoint);
      if (isUpperChar(category) && upperChar.has_value()) {
        maskedChar = upperChar.value().data();
        maskedCharByteSize = upperChar.value().size();
      } else if (isLowerChar(category) && lowerChar.has_value()) {
        maskedChar = lowerChar.value().data();
        maskedCharByteSize = lowerChar.value().size();
      } else if (isDigitChar(category) && digitChar.has_value()) {
        maskedChar = digitChar.value().data();
        maskedCharByteSize = digitChar.value().size();
      } else if (
          !isUpperChar(category) && !isLowerChar(category) &&
          !isDigitChar(category) && otherChar.has_value()) {
        maskedChar = otherChar.value().data();
        maskedCharByteSize = otherChar.value().size();
      }

      for (auto i = 0; i < maskedCharByteSize; i++) {
        outputBuffer[outputIdx++] = maskedChar[i];
      }

      inputIdx += charByteSize;
    }
    result.resize(outputIdx);
  }

  bool isUpperChar(utf8proc_propval_t& category) const {
    return category == UTF8PROC_CATEGORY_LU;
  }

  bool isLowerChar(utf8proc_propval_t& category) const {
    return category == UTF8PROC_CATEGORY_LL;
  }

  bool isDigitChar(utf8proc_propval_t& category) const {
    return category == UTF8PROC_CATEGORY_ND;
  }

  std::optional<std::string_view> getMaskedChar(
      const arg_type<Varchar>* maskChar) {
    if (maskChar) {
      auto maskCharData = maskChar->data();
      auto maskCharSize = maskChar->size();
      if (maskCharSize == 1) {
        return std::string_view(maskCharData);
      }

      VELOX_USER_CHECK_NE(
          maskCharSize,
          0,
          "Replacement string must contain a single character and cannot be empty.");

      // Calculates the byte length of the first unicode character, and compares
      // it with the length of replacing character. Inequality indicates the
      // replacing character includes more than one unicode characters.
      int size;
      auto codePoint = utf8proc_codepoint(
          &maskCharData[0], maskCharData + maskCharSize, size);
      VELOX_USER_CHECK_EQ(
          maskCharSize,
          size,
          "Replacement string must contain a single character and cannot be empty.");

      return std::string_view(maskCharData, maskCharSize);
    }
    return std::nullopt;
  }

  static constexpr std::string_view kMaskedUpperCase_{"X"};
  static constexpr std::string_view kMaskedLowerCase_{"x"};
  static constexpr std::string_view kMaskedDigit_{"n"};
};
} // namespace facebook::velox::functions::sparksql
