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

#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/process/ProcessBase.h"

namespace facebook::velox::bits {

namespace {
// Naive implementation that does not rely on BMI2.
void scatterBitsSimple(
    int32_t numSource,
    int32_t numTarget,
    const char* source,
    const uint64_t* targetMask,
    char* target) {
  int64_t from = numSource - 1;
  for (int64_t to = numTarget - 1; to >= 0; to--) {
    bool maskIsSet = bits::isBitSet(targetMask, to);
    bits::setBit(target, to, maskIsSet && bits::isBitSet(source, from));
    from -= maskIsSet ? 1 : 0;
  }
}

// Fetches 'numBits' bits of data, from data starting at lastBit -
// numbits (inclusive) and ending at lastBit (exclusive). 'lastBit' is
// updated to be the bit offset of the lowest returned bit. Successive
// calls will go through 'data' from high to low in consecutive chunks
// of up to 64 bits each.
uint64_t getBitField(const char* data, int32_t numBits, int32_t& lastBit) {
  int32_t highByte = lastBit / 8;
  int32_t lowByte = (lastBit - numBits) / 8;
  int32_t lowBit = (lastBit - numBits) & 7;
  uint64_t bits = *reinterpret_cast<const uint64_t*>(data + lowByte) >> lowBit;
  if (numBits + lowBit > 64) {
    auto fromNextByte = numBits + lowBit - 64;
    uint8_t lastBits = *reinterpret_cast<const uint8_t*>(data + highByte) &
        bits::lowMask(fromNextByte);
    bits |= static_cast<uint64_t>(lastBits) << (64 - lowBit);
  }
  lastBit -= numBits;
  return bits;
}
} // namespace

void scatterBits(
    int32_t numSource,
    int32_t numTarget,
    const char* source,
    const uint64_t* targetMask,
    char* target) {
  if (!process::hasBmi2()) {
    scatterBitsSimple(numSource, numTarget, source, targetMask, target);
    return;
  }
#ifdef __BMI2__
  int32_t highByte = numTarget / 8;
  int32_t highBit = numTarget & 7;
  int lowByte = std::max(0, highByte - 7);
  auto maskAsBytes = reinterpret_cast<const char*>(targetMask);
  // Loop from top to bottom of 'targetMask' up to 64 bits at a time,
  // with a partial word at either end. Count the set bits and fetch
  // as many consecutive bits of source data. Scatter the source bits
  // over the set bits of the target mask with pdep and write the
  // result into 'target'.
  for (;;) {
    auto numBitsToWrite = (highByte - lowByte) * 8 + highBit;
    if (numBitsToWrite == 64) {
      uint64_t mask =
          *(reinterpret_cast<const uint64_t*>(maskAsBytes + lowByte));
      int32_t consume = __builtin_popcountll(mask);
      uint64_t bits = getBitField(source, consume, numSource);
      *reinterpret_cast<uint64_t*>(target + lowByte) = _pdep_u64(bits, mask);
    } else {
      auto writeMask = bits::lowMask(numBitsToWrite);
      uint64_t mask =
          *(reinterpret_cast<const uint64_t*>(maskAsBytes + lowByte)) &
          writeMask;
      int32_t consume = __builtin_popcountll(mask);
      uint64_t bits = getBitField(source, consume, numSource);
      auto targetPtr = reinterpret_cast<uint64_t*>(target + lowByte);
      uint64_t newBits = _pdep_u64(bits, mask);
      *targetPtr = (*targetPtr & ~writeMask) | (newBits & writeMask);
    }
    VELOX_DCHECK_GE(numSource, 0);
    if (!lowByte) {
      break;
    }
    highByte = lowByte;
    highBit = 0;
    lowByte = std::max(lowByte - 8, 0);
  }
  VELOX_DCHECK_EQ(
      numSource,
      0,
      "scatterBits expects to have numSource bits set in targetMask");
#else
  VELOX_UNREACHABLE();
#endif
}

} // namespace facebook::velox::bits
