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

#include "velox/expression/StringWriter.h"

namespace facebook::velox::functions {

struct ToHexUtil {
  FOLLY_ALWAYS_INLINE static void toHex(
      StringView input,
      exec::StringWriter<false>& result) {
    // Lookup table to translate unsigned char to its hexadecimal format.
    static const char* const kHexTable =
        "000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F"
        "202122232425262728292A2B2C2D2E2F303132333435363738393A3B3C3D3E3F"
        "404142434445464748494A4B4C4D4E4F505152535455565758595A5B5C5D5E5F"
        "606162636465666768696A6B6C6D6E6F707172737475767778797A7B7C7D7E7F"
        "808182838485868788898A8B8C8D8E8F909192939495969798999A9B9C9D9E9F"
        "A0A1A2A3A4A5A6A7A8A9AAABACADAEAFB0B1B2B3B4B5B6B7B8B9BABBBCBDBEBF"
        "C0C1C2C3C4C5C6C7C8C9CACBCCCDCECFD0D1D2D3D4D5D6D7D8D9DADBDCDDDEDF"
        "E0E1E2E3E4E5E6E7E8E9EAEBECEDEEEFF0F1F2F3F4F5F6F7F8F9FAFBFCFDFEFF";

    const int64_t inputSize = input.size();
    const unsigned char* inputBuffer =
        reinterpret_cast<const unsigned char*>(input.data());
    result.resize(inputSize * 2);
    char* resultBuffer = result.data();

    for (auto i = 0; i < inputSize; ++i) {
      resultBuffer[i * 2] = kHexTable[inputBuffer[i] * 2];
      resultBuffer[i * 2 + 1] = kHexTable[inputBuffer[i] * 2 + 1];
    }
  }

  FOLLY_ALWAYS_INLINE static void toHex(
      uint64_t input,
      exec::StringWriter<false>& result) {
    static const char* const kHexTable = "0123456789ABCDEF";
    if (input == 0) {
      result = "0";
      return;
    }

    const auto resultSize = ((64 - bits::countLeadingZeros(input)) + 3) / 4;
    result.resize(resultSize);
    char* buffer = result.data();

    int32_t len = 0;
    do {
      len += 1;
      buffer[resultSize - len] = kHexTable[input & 0xF];
      input >>= 4;
    } while (input != 0);
  }
};

} // namespace facebook::velox::functions
