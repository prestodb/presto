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

#include "velox/type/DecimalUtil.h"
#include "velox/type/HugeInt.h"

namespace facebook::velox {
namespace {
std::string formatDecimal(uint8_t scale, int128_t unscaledValue) {
  VELOX_DCHECK_GE(scale, 0);
  VELOX_DCHECK_LT(
      static_cast<size_t>(scale), sizeof(DecimalUtil::kPowersOfTen));
  const bool isFraction = (scale > 0);
  if (unscaledValue == 0) {
    if (isFraction) {
      return fmt::format("{:.{}f}", 0.0, scale);
    }
    return "0";
  }

  bool isNegative = (unscaledValue < 0);
  if (isNegative) {
    unscaledValue = ~unscaledValue + 1;
  }
  int128_t integralPart = unscaledValue / DecimalUtil::kPowersOfTen[scale];

  std::string fractionString;
  if (isFraction) {
    auto fraction =
        std::to_string(unscaledValue % DecimalUtil::kPowersOfTen[scale]);
    fractionString += ".";
    // Append leading zeros.
    fractionString += std::string(
        std::max(scale - static_cast<int>(fraction.size()), 0), '0');
    // Append the fraction part.
    fractionString += fraction;
  }

  return fmt::format(
      "{}{}{}", isNegative ? "-" : "", integralPart, fractionString);
}
} // namespace

std::string DecimalUtil::toString(int128_t value, const TypePtr& type) {
  auto [precision, scale] = getDecimalPrecisionScale(*type);
  return formatDecimal(scale, value);
}

int32_t DecimalUtil::getByteArrayLength(int128_t value) {
  if (value < 0) {
    value = ~value;
  }
  int nbits;
  if (auto hi = HugeInt::upper(value)) {
    nbits = 128 - bits::countLeadingZeros(hi);
  } else if (auto lo = HugeInt::lower(value)) {
    nbits = 64 - bits::countLeadingZeros(lo);
  } else {
    nbits = 0;
  }
  return 1 + nbits / 8;
}

int32_t DecimalUtil::toByteArray(int128_t value, char* out) {
  int32_t length = getByteArrayLength(value);
  auto lowBig = folly::Endian::big<int64_t>(value);
  uint8_t* lowAddr = reinterpret_cast<uint8_t*>(&lowBig);
  if (length <= sizeof(int64_t)) {
    memcpy(out, lowAddr + sizeof(int64_t) - length, length);
  } else {
    auto highBig = folly::Endian::big<int64_t>(value >> 64);
    uint8_t* highAddr = reinterpret_cast<uint8_t*>(&highBig);
    memcpy(out, highAddr + sizeof(int128_t) - length, length - sizeof(int64_t));
    memcpy(out + length - sizeof(int64_t), lowAddr, sizeof(int64_t));
  }
  return length;
}

void DecimalUtil::computeAverage(
    int128_t& avg,
    int128_t sum,
    int64_t count,
    int64_t overflow) {
  if (overflow == 0) {
    divideWithRoundUp<int128_t, int128_t, int64_t>(
        avg, sum, count, false, 0, 0);
  } else {
    VELOX_DCHECK_LE(overflow, count);
    __uint128_t quotMul, remMul;
    __int128_t quotSum, remSum;
    __int128_t remTotal;
    remMul = DecimalUtil::divideWithRoundUp<__uint128_t, __uint128_t, int64_t>(
        quotMul, kOverflowMultiplier, count, true, 0, 0);
    remMul *= overflow;
    quotMul *= overflow;
    remSum = DecimalUtil::divideWithRoundUp<__int128_t, __int128_t, int64_t>(
        quotSum, sum, count, true, 0, 0);
    DecimalUtil::divideWithRoundUp<__int128_t, __int128_t, int64_t>(
        remTotal, remMul + remSum, count, true, 0, 0);
    avg = quotMul + quotSum + remTotal;
  }
}

int32_t DecimalUtil::maxStringViewSize(int precision, int scale) {
  int32_t rowSize = precision + 1; // Number and symbol.
  if (scale > 0) {
    ++rowSize; // A dot.
  }
  if (precision == scale) {
    ++rowSize; // Leading zero.
  }
  return rowSize;
}

} // namespace facebook::velox
