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

#include "velox/dwio/common/IntDecoder.h"
#include "velox/common/base/SimdUtil.h"
#include "velox/dwio/common/DirectDecoder.h"

namespace facebook::velox::dwio::common {

template <bool isSigned>
void IntDecoder<isSigned>::skipLongs(uint64_t numValues) {
  if (useVInts) {
    while (numValues > 0) {
      if (readByte() >= 0) {
        --numValues;
      }
    }
  } else {
    uint64_t numBytesToRead = numValues * numBytes;
    while (numBytesToRead > 0) {
      readByte();
      --numBytesToRead;
    }
  }
}

template void IntDecoder<true>::skipLongs(uint64_t numValues);
template void IntDecoder<false>::skipLongs(uint64_t numValues);

template <bool isSigned>
FOLLY_ALWAYS_INLINE void IntDecoder<isSigned>::skipVarints(uint64_t items) {
  while (items > 0) {
    items -= skipVarintsInBuffer(items);
  }
}

template <bool isSigned>
FOLLY_ALWAYS_INLINE uint64_t
IntDecoder<isSigned>::skipVarintsInBuffer(uint64_t items) {
  static constexpr uint64_t kVarintMask = 0x8080808080808080L;
  if (bufferStart == bufferEnd) {
    const void* bufferPointer;
    int32_t size;
    if (!inputStream->Next(&bufferPointer, &size)) {
      VELOX_CHECK(false, "Skipping past end of strean");
    }
    bufferStart = static_cast<const char*>(bufferPointer);
    bufferEnd = bufferStart + size;
  }
  uint64_t toSkip = items;
  while (bufferEnd - bufferStart >= sizeof(uint64_t)) {
    uint64_t controlBits =
        (~*reinterpret_cast<const uint64_t*>(bufferStart) & kVarintMask);
    auto endCount = __builtin_popcountll(controlBits);
    if (endCount >= toSkip) {
      // The range to skip ends within 'word'. Clear all but the
      // last end marker bits and count trailing zeros to see what
      // byte is the last to skip.
      for (int32_t i = 1; i < toSkip; ++i) {
        controlBits &= controlBits - 1;
      }
      auto zeros = __builtin_ctzll(controlBits);
      bufferStart += (zeros + 1) / 8;
      return items;
    }
    toSkip -= endCount;
    bufferStart += sizeof(uint64_t);
  }

  while (toSkip && bufferEnd > bufferStart) {
    if ((*reinterpret_cast<const uint8_t*>(bufferStart) & 0x80) == 0) {
      --toSkip;
    }
    ++bufferStart;
  }
  return items - toSkip;
}

template <bool isSigned>
void IntDecoder<isSigned>::skipLongsFast(uint64_t numValues) {
  if (useVInts) {
    skipVarints(numValues);
  } else {
    skipBytes(numValues * numBytes, inputStream.get(), bufferStart, bufferEnd);
  }
}

template void IntDecoder<true>::skipLongsFast(uint64_t numValues);
template void IntDecoder<false>::skipLongsFast(uint64_t numValues);

template <bool isSigned>
template <typename T>
void IntDecoder<isSigned>::bulkReadFixed(uint64_t size, T* result) {
  if (isSigned) {
    switch (numBytes) {
      case 2:
        dwio::common::readContiguous<int16_t>(
            size, *inputStream, result, bufferStart, bufferEnd);
        break;
      case 4:
        dwio::common::readContiguous<int32_t>(
            size, *inputStream, result, bufferStart, bufferEnd);
        break;
      case 8:
        dwio::common::readContiguous<int64_t>(
            size, *inputStream, result, bufferStart, bufferEnd);
        break;
      default:
        VELOX_FAIL("Bad fixed width {}", numBytes);
    }
  } else {
    switch (numBytes) {
      case 2:
        dwio::common::readContiguous<uint16_t>(
            size, *inputStream, result, bufferStart, bufferEnd);
        break;
      case 4:
        dwio::common::readContiguous<uint32_t>(
            size, *inputStream, result, bufferStart, bufferEnd);
        break;
      case 8:
        dwio::common::readContiguous<uint64_t>(
            size, *inputStream, result, bufferStart, bufferEnd);
        break;
      default:
        VELOX_FAIL("Bad fixed width {}", numBytes);
    }
  }
}

template <bool isSigned>
template <typename T>
void IntDecoder<isSigned>::bulkReadRowsFixed(
    RowSet rows,
    int32_t initialRow,
    T* result) {
  if (isSigned) {
    switch (numBytes) {
      case 2:
        dwio::common::readRows<int16_t>(
            rows, initialRow, *inputStream, result, bufferStart, bufferEnd);
        break;
      case 4:
        dwio::common::readRows<int32_t>(
            rows, initialRow, *inputStream, result, bufferStart, bufferEnd);
        break;
      case 8:
        dwio::common::readRows<int64_t>(
            rows, initialRow, *inputStream, result, bufferStart, bufferEnd);
        break;
      default:
        VELOX_FAIL("Bad fixed width {}", numBytes);
    }
  } else {
    switch (numBytes) {
      case 2:
        dwio::common::readRows<uint16_t>(
            rows, initialRow, *inputStream, result, bufferStart, bufferEnd);
        break;
      case 4:
        dwio::common::readRows<uint32_t>(
            rows, initialRow, *inputStream, result, bufferStart, bufferEnd);
        break;
      case 8:
        dwio::common::readRows<uint64_t>(
            rows, initialRow, *inputStream, result, bufferStart, bufferEnd);
        break;
      default:
        VELOX_FAIL("Bad fixed width {}", numBytes);
    }
  }
}

template <typename T>
inline void bulkZigzagDecode(int32_t size, T* data) {
  // Round up to get an even number of 256 bit iterations
  auto rounded = bits::roundUp(size, 32 / sizeof(T));
  using U = typename std::make_unsigned<T>::type;
  for (auto i = 0; i < rounded; ++i) {
    auto n = static_cast<U>(data[i]);
    data[i] = ((n >> 1) ^ (~(n & 1) + 1));
  }
}

#if XSIMD_WITH_AVX2

// Stores each byte of 'bytes' as a int64_t at output[0] ... output[3]
inline void unpack4x1(int32_t bytes, uint64_t*& output) {
  *reinterpret_cast<__m256i_u*>(output) =
      _mm256_cvtepi8_epi64(_mm_set1_epi32(bytes));
  output += 4;
}

inline void unpack4x1(int32_t bytes, int64_t*& output) {
  *reinterpret_cast<__m256i_u*>(output) =
      _mm256_cvtepi8_epi64(_mm_set1_epi32(bytes));
  output += 4;
}

// Stores each byte of 'bytes' as a int32_t at output[0] ... output[3]
inline void unpack4x1(int32_t bytes, int32_t*& output) {
  *reinterpret_cast<__m256i_u*>(output) =
      _mm256_cvtepi8_epi32(_mm_set1_epi32(bytes));
  output += 4;
}

// Stores each byte of 'bytes' as a int16_t at output[0] ... output[3]
inline void unpack4x1(int32_t bytes, int16_t*& output) {
  *reinterpret_cast<__m256i_u*>(output) =
      _mm256_cvtepi8_epi16(_mm_set1_epi32(bytes));
  output += 4;
}

#else

template <typename T>
void unpack4x1(int32_t bytes, T*& output) {
  for (int i = 0; i < 4; ++i) {
    if constexpr (std::is_signed_v<T>) {
      *output++ = static_cast<int8_t>(bytes & 0xFF);
    } else {
      *output++ = static_cast<uint8_t>(bytes & 0xFF);
    }
    bytes >>= 8;
  }
}

#endif

// Returns true if the next extract is part of 'rows'.
inline bool isEnabled(
    int32_t& row,
    int32_t& nextRow,
    int32_t& nextRowIndex,
    const int32_t* rows) {
  if (row++ == nextRow) {
    nextRow = rows[++nextRowIndex];
    return true;
  }
  return false;
}

// Applies a carryover from an earlier decoding round to the number at
// output[0]. Used when after bulk decoding varint components.
template <typename T>
inline void
applyCarryover(int32_t carryoverBits, uint64_t carryover, T* output) {
  if (carryoverBits) {
    // Shift as uint64_t because shifting by more than T width is
    // undefined and makes errors with sanitizers. The situation
    // occurs when reading dictionary values where not in dictionary
    // negative values get represented as unsigned 10-byte ints with
    // high bit set. These overflow T when decoding but come out right
    // when narrowed to T.
    *output = (static_cast<uint64_t>(*output) << carryoverBits) | carryover;
  }
}

// Compensates for speculative store of 4 elements where one or two last may not
// have been single bytes.
template <typename T>
inline void clipTrailingStores(
    int32_t startBit,
    uint64_t controlBits,
    const char*& pos,
    T*& output) {
  if (startBit == 3) {
    // If bit 6 is set, the last written is not a single byte vint.
    if ((controlBits & 64)) {
      --output;
    } else {
      ++pos;
    }
  } else if (startBit == 4) {
    // Two extra writes. If bits 6 and 7 are 0, these were valid. If bit 6 is 0,
    // the first was valid, else neither was valid.
    if ((controlBits & 192) == 0) {
      pos += 2;
    } else if ((controlBits & 64) == 0) {
      ++pos;
      --output;
    } else {
      output -= 2;
    }
  }
}

template <typename T>
FOLLY_ALWAYS_INLINE void varintSwitch(
    uint64_t word,
    uint64_t controlBits,
    const char*& pos,
    T*& output,
    uint64_t& carryover,
    int32_t& carryoverBits) {
  switch (controlBits & 0x3f) {
    case 0ULL: {
      unpack4x1(word >> 0, output);
      applyCarryover(carryoverBits, carryover, output - 4);
      unpack4x1(word >> 32, output);
      clipTrailingStores(4, controlBits, pos, output);
      carryover = 0ULL;
      carryoverBits = 0;
      break;
    }
    case 1ULL: {
      const uint64_t firstValue =
          bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
      *output++ = (firstValue << carryoverBits) | carryover;
      unpack4x1(word >> 16, output);
      carryover = 0ULL;
      carryoverBits = 0;
      break;
    }
    case 2ULL: {
      const uint64_t firstValue =
          static_cast<uint64_t>(static_cast<uint8_t>(word));
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = bits::extractBits<uint64_t>(word, 0x00000000007f7f00ULL);
      unpack4x1(word >> 24, output);
      clipTrailingStores(3, controlBits, pos, output);
      carryover = 0ULL;
      carryoverBits = 0;
      break;
    }
    case 3ULL: {
      const uint64_t firstValue =
          bits::extractBits<uint64_t>(word, 0x00000000007f7f7fULL);
      *output++ = (firstValue << carryoverBits) | carryover;
      unpack4x1(word >> 24, output);
      clipTrailingStores(3, controlBits, pos, output);
      carryover = 0ULL;
      carryoverBits = 0;
      break;
    }
    case 4ULL: {
      const uint64_t firstValue =
          static_cast<uint64_t>(static_cast<uint8_t>(word));
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = ((word >> 8) & 0x7f);
      *output++ = bits::extractBits<uint64_t>(word, 0x000000007f7f0000ULL);
      unpack4x1(word >> 32, output);
      clipTrailingStores(4, controlBits, pos, output);
      carryover = 0ULL;
      carryoverBits = 0;
      break;
    }
    case 5ULL: {
      const uint64_t firstValue =
          bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = bits::extractBits<uint64_t>(word, 0x000000007f7f0000ULL);
      unpack4x1(word >> 32, output);
      clipTrailingStores(4, controlBits, pos, output);
      carryover = 0ULL;
      carryoverBits = 0;
      break;
    }
    case 6ULL: {
      const uint64_t firstValue =
          static_cast<uint64_t>(static_cast<uint8_t>(word));
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = bits::extractBits<uint64_t>(word, 0x000000007f7f7f00ULL);
      unpack4x1(word >> 32, output);
      clipTrailingStores(4, controlBits, pos, output);
      carryover = 0ULL;
      carryoverBits = 0;
      break;
    }
    case 7ULL: {
      const uint64_t firstValue =
          bits::extractBits<uint64_t>(word, 0x000000007f7f7f7fULL);
      *output++ = (firstValue << carryoverBits) | carryover;
      unpack4x1(word >> 32, output);
      clipTrailingStores(4, controlBits, pos, output);
      carryover = 0ULL;
      carryoverBits = 0;
      break;
    }
    case 8ULL: {
      const uint64_t firstValue =
          static_cast<uint64_t>(static_cast<uint8_t>(word));
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = ((word >> 8) & 0x7f);
      *output++ = ((word >> 16) & 0x7f);
      *output++ = bits::extractBits<uint64_t>(word, 0x0000007f7f000000ULL);
      *output++ = ((word >> 40) & 0x7f);
      carryover = 0ULL;
      carryoverBits = 0;
      break;
    }
    case 9ULL: {
      const uint64_t firstValue =
          bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = ((word >> 16) & 0x7f);
      *output++ = bits::extractBits<uint64_t>(word, 0x0000007f7f000000ULL);
      *output++ = ((word >> 40) & 0x7f);
      carryover = 0ULL;
      carryoverBits = 0;
      break;
    }
    case 10ULL: {
      const uint64_t firstValue =
          static_cast<uint64_t>(static_cast<uint8_t>(word));
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = bits::extractBits<uint64_t>(word, 0x00000000007f7f00ULL);
      *output++ = bits::extractBits<uint64_t>(word, 0x0000007f7f000000ULL);
      *output++ = ((word >> 40) & 0x7f);
      carryover = 0ULL;
      carryoverBits = 0;
      break;
    }
    case 11ULL: {
      const uint64_t firstValue =
          bits::extractBits<uint64_t>(word, 0x00000000007f7f7fULL);
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = bits::extractBits<uint64_t>(word, 0x0000007f7f000000ULL);
      *output++ = ((word >> 40) & 0x7f);
      carryover = 0ULL;
      carryoverBits = 0;
      break;
    }
    case 12ULL: {
      const uint64_t firstValue =
          static_cast<uint64_t>(static_cast<uint8_t>(word));
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = ((word >> 8) & 0x7f);
      *output++ = bits::extractBits<uint64_t>(word, 0x0000007f7f7f0000ULL);
      *output++ = ((word >> 40) & 0x7f);
      carryover = 0ULL;
      carryoverBits = 0;
      break;
    }
    case 13ULL: {
      const uint64_t firstValue =
          bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = bits::extractBits<uint64_t>(word, 0x0000007f7f7f0000ULL);
      *output++ = ((word >> 40) & 0x7f);
      carryover = 0ULL;
      carryoverBits = 0;
      break;
    }
    case 14ULL: {
      const uint64_t firstValue =
          static_cast<uint64_t>(static_cast<uint8_t>(word));
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = bits::extractBits<uint64_t>(word, 0x0000007f7f7f7f00ULL);
      *output++ = ((word >> 40) & 0x7f);
      carryover = 0ULL;
      carryoverBits = 0;
      break;
    }
    case 15ULL: {
      const uint64_t firstValue =
          bits::extractBits<uint64_t>(word, 0x0000007f7f7f7f7fULL);
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = ((word >> 40) & 0x7f);
      carryover = 0ULL;
      carryoverBits = 0;
      break;
    }
    case 16ULL: {
      unpack4x1(word >> 0, output);
      applyCarryover(carryoverBits, carryover, output - 4);
      *output++ = bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
      carryover = 0ULL;
      carryoverBits = 0;
      break;
    }
    case 17ULL: {
      const uint64_t firstValue =
          bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = ((word >> 16) & 0x7f);
      *output++ = ((word >> 24) & 0x7f);
      *output++ = bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
      carryover = 0ULL;
      carryoverBits = 0;
      break;
    }
    case 18ULL: {
      const uint64_t firstValue =
          static_cast<uint64_t>(static_cast<uint8_t>(word));
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = bits::extractBits<uint64_t>(word, 0x00000000007f7f00ULL);
      *output++ = ((word >> 24) & 0x7f);
      *output++ = bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
      carryover = 0ULL;
      carryoverBits = 0;
      break;
    }
    case 19ULL: {
      const uint64_t firstValue =
          bits::extractBits<uint64_t>(word, 0x00000000007f7f7fULL);
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = ((word >> 24) & 0x7f);
      *output++ = bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
      carryover = 0ULL;
      carryoverBits = 0;
      break;
    }
    case 20ULL: {
      const uint64_t firstValue =
          static_cast<uint64_t>(static_cast<uint8_t>(word));
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = ((word >> 8) & 0x7f);
      *output++ = bits::extractBits<uint64_t>(word, 0x000000007f7f0000ULL);
      *output++ = bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
      carryover = 0ULL;
      carryoverBits = 0;
      break;
    }
    case 21ULL: {
      const uint64_t firstValue =
          bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = bits::extractBits<uint64_t>(word, 0x000000007f7f0000ULL);
      *output++ = bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
      carryover = 0ULL;
      carryoverBits = 0;
      break;
    }
    case 22ULL: {
      const uint64_t firstValue =
          static_cast<uint64_t>(static_cast<uint8_t>(word));
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = bits::extractBits<uint64_t>(word, 0x000000007f7f7f00ULL);
      *output++ = bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
      carryover = 0ULL;
      carryoverBits = 0;
      break;
    }
    case 23ULL: {
      const uint64_t firstValue =
          bits::extractBits<uint64_t>(word, 0x000000007f7f7f7fULL);
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
      carryover = 0ULL;
      carryoverBits = 0;
      break;
    }
    case 24ULL: {
      const uint64_t firstValue =
          static_cast<uint64_t>(static_cast<uint8_t>(word));
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = ((word >> 8) & 0x7f);
      *output++ = ((word >> 16) & 0x7f);
      *output++ = bits::extractBits<uint64_t>(word, 0x00007f7f7f000000ULL);
      carryover = 0ULL;
      carryoverBits = 0;
      break;
    }
    case 25ULL: {
      const uint64_t firstValue =
          bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = ((word >> 16) & 0x7f);
      *output++ = bits::extractBits<uint64_t>(word, 0x00007f7f7f000000ULL);
      carryover = 0ULL;
      carryoverBits = 0;
      break;
    }
    case 26ULL: {
      const uint64_t firstValue =
          static_cast<uint64_t>(static_cast<uint8_t>(word));
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = bits::extractBits<uint64_t>(word, 0x00000000007f7f00ULL);
      *output++ = bits::extractBits<uint64_t>(word, 0x00007f7f7f000000ULL);
      carryover = 0ULL;
      carryoverBits = 0;
      break;
    }
    case 27ULL: {
      const uint64_t firstValue =
          bits::extractBits<uint64_t>(word, 0x00000000007f7f7fULL);
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = bits::extractBits<uint64_t>(word, 0x00007f7f7f000000ULL);
      carryover = 0ULL;
      carryoverBits = 0;
      break;
    }
    case 28ULL: {
      const uint64_t firstValue =
          static_cast<uint64_t>(static_cast<uint8_t>(word));
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = ((word >> 8) & 0x7f);
      *output++ = bits::extractBits<uint64_t>(word, 0x00007f7f7f7f0000ULL);
      carryover = 0ULL;
      carryoverBits = 0;
      break;
    }
    case 29ULL: {
      const uint64_t firstValue =
          bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = bits::extractBits<uint64_t>(word, 0x00007f7f7f7f0000ULL);
      carryover = 0ULL;
      carryoverBits = 0;
      break;
    }
    case 30ULL: {
      const uint64_t firstValue =
          static_cast<uint64_t>(static_cast<uint8_t>(word));
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = bits::extractBits<uint64_t>(word, 0x00007f7f7f7f7f00ULL);
      carryover = 0ULL;
      carryoverBits = 0;
      break;
    }
    case 31ULL: {
      const uint64_t firstValue =
          bits::extractBits<uint64_t>(word, 0x00007f7f7f7f7f7fULL);
      *output++ = (firstValue << carryoverBits) | carryover;
      carryover = 0ULL;
      carryoverBits = 0;
      break;
    }
    case 32ULL: {
      unpack4x1(word >> 0, output);
      applyCarryover(carryoverBits, carryover, output - 4);
      *output++ = ((word >> 32) & 0x7f);
      carryover = ((word >> 40) & 0x7f);
      carryoverBits = 7;
      break;
    }
    case 33ULL: {
      const uint64_t firstValue =
          bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = ((word >> 16) & 0x7f);
      *output++ = ((word >> 24) & 0x7f);
      *output++ = ((word >> 32) & 0x7f);
      carryover = ((word >> 40) & 0x7f);
      carryoverBits = 7;
      break;
    }
    case 34ULL: {
      const uint64_t firstValue =
          static_cast<uint64_t>(static_cast<uint8_t>(word));
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = bits::extractBits<uint64_t>(word, 0x00000000007f7f00ULL);
      *output++ = ((word >> 24) & 0x7f);
      *output++ = ((word >> 32) & 0x7f);
      carryover = ((word >> 40) & 0x7f);
      carryoverBits = 7;
      break;
    }
    case 35ULL: {
      const uint64_t firstValue =
          bits::extractBits<uint64_t>(word, 0x00000000007f7f7fULL);
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = ((word >> 24) & 0x7f);
      *output++ = ((word >> 32) & 0x7f);
      carryover = ((word >> 40) & 0x7f);
      carryoverBits = 7;
      break;
    }
    case 36ULL: {
      const uint64_t firstValue =
          static_cast<uint64_t>(static_cast<uint8_t>(word));
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = ((word >> 8) & 0x7f);
      *output++ = bits::extractBits<uint64_t>(word, 0x000000007f7f0000ULL);
      *output++ = ((word >> 32) & 0x7f);
      carryover = ((word >> 40) & 0x7f);
      carryoverBits = 7;
      break;
    }
    case 37ULL: {
      const uint64_t firstValue =
          bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = bits::extractBits<uint64_t>(word, 0x000000007f7f0000ULL);
      *output++ = ((word >> 32) & 0x7f);
      carryover = ((word >> 40) & 0x7f);
      carryoverBits = 7;
      break;
    }
    case 38ULL: {
      const uint64_t firstValue =
          static_cast<uint64_t>(static_cast<uint8_t>(word));
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = bits::extractBits<uint64_t>(word, 0x000000007f7f7f00ULL);
      *output++ = ((word >> 32) & 0x7f);
      carryover = ((word >> 40) & 0x7f);
      carryoverBits = 7;
      break;
    }
    case 39ULL: {
      const uint64_t firstValue =
          bits::extractBits<uint64_t>(word, 0x000000007f7f7f7fULL);
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = ((word >> 32) & 0x7f);
      carryover = ((word >> 40) & 0x7f);
      carryoverBits = 7;
      break;
    }
    case 40ULL: {
      const uint64_t firstValue =
          static_cast<uint64_t>(static_cast<uint8_t>(word));
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = ((word >> 8) & 0x7f);
      *output++ = ((word >> 16) & 0x7f);
      *output++ = bits::extractBits<uint64_t>(word, 0x0000007f7f000000ULL);
      carryover = ((word >> 40) & 0x7f);
      carryoverBits = 7;
      break;
    }
    case 41ULL: {
      const uint64_t firstValue =
          bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = ((word >> 16) & 0x7f);
      *output++ = bits::extractBits<uint64_t>(word, 0x0000007f7f000000ULL);
      carryover = ((word >> 40) & 0x7f);
      carryoverBits = 7;
      break;
    }
    case 42ULL: {
      const uint64_t firstValue =
          static_cast<uint64_t>(static_cast<uint8_t>(word));
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = bits::extractBits<uint64_t>(word, 0x00000000007f7f00ULL);
      *output++ = bits::extractBits<uint64_t>(word, 0x0000007f7f000000ULL);
      carryover = ((word >> 40) & 0x7f);
      carryoverBits = 7;
      break;
    }
    case 43ULL: {
      const uint64_t firstValue =
          bits::extractBits<uint64_t>(word, 0x00000000007f7f7fULL);
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = bits::extractBits<uint64_t>(word, 0x0000007f7f000000ULL);
      carryover = ((word >> 40) & 0x7f);
      carryoverBits = 7;
      break;
    }
    case 44ULL: {
      const uint64_t firstValue =
          static_cast<uint64_t>(static_cast<uint8_t>(word));
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = ((word >> 8) & 0x7f);
      *output++ = bits::extractBits<uint64_t>(word, 0x0000007f7f7f0000ULL);
      carryover = ((word >> 40) & 0x7f);
      carryoverBits = 7;
      break;
    }
    case 45ULL: {
      const uint64_t firstValue =
          bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = bits::extractBits<uint64_t>(word, 0x0000007f7f7f0000ULL);
      carryover = ((word >> 40) & 0x7f);
      carryoverBits = 7;
      break;
    }
    case 46ULL: {
      const uint64_t firstValue =
          static_cast<uint64_t>(static_cast<uint8_t>(word));
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = bits::extractBits<uint64_t>(word, 0x0000007f7f7f7f00ULL);
      carryover = ((word >> 40) & 0x7f);
      carryoverBits = 7;
      break;
    }
    case 47ULL: {
      const uint64_t firstValue =
          bits::extractBits<uint64_t>(word, 0x0000007f7f7f7f7fULL);
      *output++ = (firstValue << carryoverBits) | carryover;
      carryover = ((word >> 40) & 0x7f);
      carryoverBits = 7;
      break;
    }
    case 48ULL: {
      unpack4x1(word >> 0, output);
      applyCarryover(carryoverBits, carryover, output - 4);
      carryover = bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
      carryoverBits = 14;
      break;
    }
    case 49ULL: {
      const uint64_t firstValue =
          bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = ((word >> 16) & 0x7f);
      *output++ = ((word >> 24) & 0x7f);
      carryover = bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
      carryoverBits = 14;
      break;
    }
    case 50ULL: {
      const uint64_t firstValue =
          static_cast<uint64_t>(static_cast<uint8_t>(word));
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = bits::extractBits<uint64_t>(word, 0x00000000007f7f00ULL);
      *output++ = ((word >> 24) & 0x7f);
      carryover = bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
      carryoverBits = 14;
      break;
    }
    case 51ULL: {
      const uint64_t firstValue =
          bits::extractBits<uint64_t>(word, 0x00000000007f7f7fULL);
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = ((word >> 24) & 0x7f);
      carryover = bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
      carryoverBits = 14;
      break;
    }
    case 52ULL: {
      const uint64_t firstValue =
          static_cast<uint64_t>(static_cast<uint8_t>(word));
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = ((word >> 8) & 0x7f);
      *output++ = bits::extractBits<uint64_t>(word, 0x000000007f7f0000ULL);
      carryover = bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
      carryoverBits = 14;
      break;
    }
    case 53ULL: {
      const uint64_t firstValue =
          bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = bits::extractBits<uint64_t>(word, 0x000000007f7f0000ULL);
      carryover = bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
      carryoverBits = 14;
      break;
    }
    case 54ULL: {
      const uint64_t firstValue =
          static_cast<uint64_t>(static_cast<uint8_t>(word));
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = bits::extractBits<uint64_t>(word, 0x000000007f7f7f00ULL);
      carryover = bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
      carryoverBits = 14;
      break;
    }
    case 55ULL: {
      const uint64_t firstValue =
          bits::extractBits<uint64_t>(word, 0x000000007f7f7f7fULL);
      *output++ = (firstValue << carryoverBits) | carryover;
      carryover = bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
      carryoverBits = 14;
      break;
    }
    case 56ULL: {
      const uint64_t firstValue =
          static_cast<uint64_t>(static_cast<uint8_t>(word));
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = ((word >> 8) & 0x7f);
      *output++ = ((word >> 16) & 0x7f);
      carryover = bits::extractBits<uint64_t>(word, 0x00007f7f7f000000ULL);
      carryoverBits = 21;
      break;
    }
    case 57ULL: {
      const uint64_t firstValue =
          bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = ((word >> 16) & 0x7f);
      carryover = bits::extractBits<uint64_t>(word, 0x00007f7f7f000000ULL);
      carryoverBits = 21;
      break;
    }
    case 58ULL: {
      const uint64_t firstValue =
          static_cast<uint64_t>(static_cast<uint8_t>(word));
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = bits::extractBits<uint64_t>(word, 0x00000000007f7f00ULL);
      carryover = bits::extractBits<uint64_t>(word, 0x00007f7f7f000000ULL);
      carryoverBits = 21;
      break;
    }
    case 59ULL: {
      const uint64_t firstValue =
          bits::extractBits<uint64_t>(word, 0x00000000007f7f7fULL);
      *output++ = (firstValue << carryoverBits) | carryover;
      carryover = bits::extractBits<uint64_t>(word, 0x00007f7f7f000000ULL);
      carryoverBits = 21;
      break;
    }
    case 60ULL: {
      const uint64_t firstValue =
          static_cast<uint64_t>(static_cast<uint8_t>(word));
      *output++ = (firstValue << carryoverBits) | carryover;
      *output++ = ((word >> 8) & 0x7f);
      carryover = bits::extractBits<uint64_t>(word, 0x00007f7f7f7f0000ULL);
      carryoverBits = 28;
      break;
    }
    case 61ULL: {
      const uint64_t firstValue =
          bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
      *output++ = (firstValue << carryoverBits) | carryover;
      carryover = bits::extractBits<uint64_t>(word, 0x00007f7f7f7f0000ULL);
      carryoverBits = 28;
      break;
    }
    case 62ULL: {
      const uint64_t firstValue =
          static_cast<uint64_t>(static_cast<uint8_t>(word));
      *output++ = (firstValue << carryoverBits) | carryover;
      carryover = bits::extractBits<uint64_t>(word, 0x00007f7f7f7f7f00ULL);
      carryoverBits = 35;
      break;
    }
    case 63ULL: {
      carryover |= bits::extractBits<uint64_t>(word, 0x00007f7f7f7f7f7fULL)
          << carryoverBits;
      carryoverBits += 42;
      break;
    }
    default: {
      throw std::logic_error(
          "It should be impossible for control bits to be > 63.");
    }
  }
}

template <bool isSigned>
template <typename T>
void IntDecoder<isSigned>::bulkRead(uint64_t size, T* result) {
  if (!useVInts) {
    bulkReadFixed(size, result);
    return;
  }
  constexpr uint64_t mask = 0x8080808080808080;
  constexpr int32_t maskSize = 6;
  uint64_t carryover = 0;
  int32_t carryoverBits = 0;
  auto output = result;
  const char* pos = bufferStart;
  auto end = result + size;
  if (pos) {
    // Decrement only if non-null to avoid asan error.
    pos -= maskSize;
  }
  while (output < end) {
    while (end >= output + 8 && bufferEnd - pos >= 8 + maskSize) {
      pos += maskSize;
      const auto word = folly::loadUnaligned<uint64_t>(pos);
      const uint64_t controlBits = bits::extractBits<uint64_t>(word, mask);
      varintSwitch(word, controlBits, pos, output, carryover, carryoverBits);
    }
    if (pos) {
      pos += maskSize;
    }
    bufferStart = pos;
    if (output < end) {
      *output++ = (readVuLong() << carryoverBits) | carryover;
      carryover = 0;
      carryoverBits = 0;
      while (output < end) {
        *output++ = readVuLong();
        if (output + 8 <= end && bufferEnd - bufferStart > 8 + maskSize) {
          // Go back to fast loop after refilling the buffer.
          pos = bufferStart - maskSize;
          break;
        }
      }
    }
  }
  if (isSigned) {
    bulkZigzagDecode(size, result);
  }
}

template <bool isSigned>
template <typename T>
void IntDecoder<isSigned>::bulkReadRows(
    RowSet rows,
    T* result,
    int32_t initialRow) {
  if (!useVInts) {
    bulkReadRowsFixed(rows, initialRow, result);
    return;
  }
  constexpr uint64_t mask = 0x8080808080808080;
  constexpr int32_t maskSize = 6;
  uint64_t carryover = 0;
  int32_t carryoverBits = 0;
  auto output = result;
  const char* pos = bufferStart;
  int32_t nextRowIndex = 0;
  int32_t nextRow = rows[0];
  int32_t row = initialRow;
  int32_t endRow = rows.back() + 1;
  int32_t endRowIndex = rows.size();
  if (pos) {
    // Decrement only if non-null to avoid asan error.
    pos -= maskSize;
  }
  while (nextRowIndex < rows.size()) {
    while (row + 8 <= endRow && bufferEnd - pos >= 8 + maskSize) {
      pos += maskSize;
      const auto word = folly::loadUnaligned<uint64_t>(pos);
      if (nextRow >= row + 8) {
        row += __builtin_popcountll(~word & mask);
        pos += 8 - maskSize;
        continue;
      }
      const uint64_t controlBits = bits::extractBits<uint64_t>(word, mask);
      int32_t numEnds = __builtin_popcount(controlBits ^ 0xff);
      if (row != nextRow) {
        if (nextRow > row + numEnds) {
          row += numEnds;
          pos += 8 - maskSize;
          continue;
        }
      }
      if (nextRowIndex + numEnds < rows.size() &&
          rows[nextRowIndex + numEnds] == row + numEnds) {
        // A word worth of contiguous rows.
        auto orgOutput = output;
        // Reading 6 - 8 bytes of contiguous values.
        varintSwitch(word, controlBits, pos, output, carryover, carryoverBits);
        int numDone = output - orgOutput;
        row += numDone;
        nextRowIndex += numDone;
        nextRow = rows[nextRowIndex];
      } else {
        // switch with conditional store of results.

        switch (controlBits & 63) {
          case 0ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  static_cast<uint64_t>(static_cast<uint8_t>(word));
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 8) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 16) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 24) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 32) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 40) & 0x7f);
            }
            carryover = 0ULL;
            carryoverBits = 0;
            break;
          }
          case 1ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 16) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 24) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 32) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 40) & 0x7f);
            }
            carryover = 0ULL;
            carryoverBits = 0;
            break;
          }
          case 2ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  static_cast<uint64_t>(static_cast<uint8_t>(word));
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x00000000007f7f00ULL);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 24) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 32) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 40) & 0x7f);
            }
            carryover = 0ULL;
            carryoverBits = 0;
            break;
          }
          case 3ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  bits::extractBits<uint64_t>(word, 0x00000000007f7f7fULL);
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 24) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 32) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 40) & 0x7f);
            }
            carryover = 0ULL;
            carryoverBits = 0;
            break;
          }
          case 4ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  static_cast<uint64_t>(static_cast<uint8_t>(word));
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 8) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x000000007f7f0000ULL);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 32) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 40) & 0x7f);
            }
            carryover = 0ULL;
            carryoverBits = 0;
            break;
          }
          case 5ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x000000007f7f0000ULL);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 32) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 40) & 0x7f);
            }
            carryover = 0ULL;
            carryoverBits = 0;
            break;
          }
          case 6ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  static_cast<uint64_t>(static_cast<uint8_t>(word));
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x000000007f7f7f00ULL);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 32) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 40) & 0x7f);
            }
            carryover = 0ULL;
            carryoverBits = 0;
            break;
          }
          case 7ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  bits::extractBits<uint64_t>(word, 0x000000007f7f7f7fULL);
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 32) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 40) & 0x7f);
            }
            carryover = 0ULL;
            carryoverBits = 0;
            break;
          }
          case 8ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  static_cast<uint64_t>(static_cast<uint8_t>(word));
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 8) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 16) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x0000007f7f000000ULL);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 40) & 0x7f);
            }
            carryover = 0ULL;
            carryoverBits = 0;
            break;
          }
          case 9ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 16) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x0000007f7f000000ULL);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 40) & 0x7f);
            }
            carryover = 0ULL;
            carryoverBits = 0;
            break;
          }
          case 10ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  static_cast<uint64_t>(static_cast<uint8_t>(word));
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x00000000007f7f00ULL);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x0000007f7f000000ULL);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 40) & 0x7f);
            }
            carryover = 0ULL;
            carryoverBits = 0;
            break;
          }
          case 11ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  bits::extractBits<uint64_t>(word, 0x00000000007f7f7fULL);
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x0000007f7f000000ULL);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 40) & 0x7f);
            }
            carryover = 0ULL;
            carryoverBits = 0;
            break;
          }
          case 12ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  static_cast<uint64_t>(static_cast<uint8_t>(word));
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 8) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x0000007f7f7f0000ULL);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 40) & 0x7f);
            }
            carryover = 0ULL;
            carryoverBits = 0;
            break;
          }
          case 13ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x0000007f7f7f0000ULL);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 40) & 0x7f);
            }
            carryover = 0ULL;
            carryoverBits = 0;
            break;
          }
          case 14ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  static_cast<uint64_t>(static_cast<uint8_t>(word));
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x0000007f7f7f7f00ULL);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 40) & 0x7f);
            }
            carryover = 0ULL;
            carryoverBits = 0;
            break;
          }
          case 15ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  bits::extractBits<uint64_t>(word, 0x0000007f7f7f7f7fULL);
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 40) & 0x7f);
            }
            carryover = 0ULL;
            carryoverBits = 0;
            break;
          }
          case 16ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  static_cast<uint64_t>(static_cast<uint8_t>(word));
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 8) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 16) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 24) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
            }
            carryover = 0ULL;
            carryoverBits = 0;
            break;
          }
          case 17ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 16) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 24) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
            }
            carryover = 0ULL;
            carryoverBits = 0;
            break;
          }
          case 18ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  static_cast<uint64_t>(static_cast<uint8_t>(word));
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x00000000007f7f00ULL);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 24) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
            }
            carryover = 0ULL;
            carryoverBits = 0;
            break;
          }
          case 19ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  bits::extractBits<uint64_t>(word, 0x00000000007f7f7fULL);
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 24) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
            }
            carryover = 0ULL;
            carryoverBits = 0;
            break;
          }
          case 20ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  static_cast<uint64_t>(static_cast<uint8_t>(word));
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 8) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x000000007f7f0000ULL);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
            }
            carryover = 0ULL;
            carryoverBits = 0;
            break;
          }
          case 21ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x000000007f7f0000ULL);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
            }
            carryover = 0ULL;
            carryoverBits = 0;
            break;
          }
          case 22ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  static_cast<uint64_t>(static_cast<uint8_t>(word));
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x000000007f7f7f00ULL);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
            }
            carryover = 0ULL;
            carryoverBits = 0;
            break;
          }
          case 23ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  bits::extractBits<uint64_t>(word, 0x000000007f7f7f7fULL);
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
            }
            carryover = 0ULL;
            carryoverBits = 0;
            break;
          }
          case 24ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  static_cast<uint64_t>(static_cast<uint8_t>(word));
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 8) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 16) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x00007f7f7f000000ULL);
            }
            carryover = 0ULL;
            carryoverBits = 0;
            break;
          }
          case 25ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 16) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x00007f7f7f000000ULL);
            }
            carryover = 0ULL;
            carryoverBits = 0;
            break;
          }
          case 26ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  static_cast<uint64_t>(static_cast<uint8_t>(word));
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x00000000007f7f00ULL);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x00007f7f7f000000ULL);
            }
            carryover = 0ULL;
            carryoverBits = 0;
            break;
          }
          case 27ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  bits::extractBits<uint64_t>(word, 0x00000000007f7f7fULL);
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x00007f7f7f000000ULL);
            }
            carryover = 0ULL;
            carryoverBits = 0;
            break;
          }
          case 28ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  static_cast<uint64_t>(static_cast<uint8_t>(word));
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 8) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x00007f7f7f7f0000ULL);
            }
            carryover = 0ULL;
            carryoverBits = 0;
            break;
          }
          case 29ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x00007f7f7f7f0000ULL);
            }
            carryover = 0ULL;
            carryoverBits = 0;
            break;
          }
          case 30ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  static_cast<uint64_t>(static_cast<uint8_t>(word));
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x00007f7f7f7f7f00ULL);
            }
            carryover = 0ULL;
            carryoverBits = 0;
            break;
          }
          case 31ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  bits::extractBits<uint64_t>(word, 0x00007f7f7f7f7f7fULL);
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            carryover = 0ULL;
            carryoverBits = 0;
            break;
          }
          case 32ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  static_cast<uint64_t>(static_cast<uint8_t>(word));
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 8) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 16) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 24) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 32) & 0x7f);
            }
            if (row == nextRow) {
              carryover = ((word >> 40) & 0x7f);
              carryoverBits = 7;
            }
            break;
          }
          case 33ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 16) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 24) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 32) & 0x7f);
            }
            if (row == nextRow) {
              carryover = ((word >> 40) & 0x7f);
              carryoverBits = 7;
            }
            break;
          }
          case 34ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  static_cast<uint64_t>(static_cast<uint8_t>(word));
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x00000000007f7f00ULL);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 24) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 32) & 0x7f);
            }
            if (row == nextRow) {
              carryover = ((word >> 40) & 0x7f);
              carryoverBits = 7;
            }
            break;
          }
          case 35ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  bits::extractBits<uint64_t>(word, 0x00000000007f7f7fULL);
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 24) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 32) & 0x7f);
            }
            if (row == nextRow) {
              carryover = ((word >> 40) & 0x7f);
              carryoverBits = 7;
            }
            break;
          }
          case 36ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  static_cast<uint64_t>(static_cast<uint8_t>(word));
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 8) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x000000007f7f0000ULL);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 32) & 0x7f);
            }
            if (row == nextRow) {
              carryover = ((word >> 40) & 0x7f);
              carryoverBits = 7;
            }
            break;
          }
          case 37ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x000000007f7f0000ULL);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 32) & 0x7f);
            }
            if (row == nextRow) {
              carryover = ((word >> 40) & 0x7f);
              carryoverBits = 7;
            }
            break;
          }
          case 38ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  static_cast<uint64_t>(static_cast<uint8_t>(word));
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x000000007f7f7f00ULL);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 32) & 0x7f);
            }
            if (row == nextRow) {
              carryover = ((word >> 40) & 0x7f);
              carryoverBits = 7;
            }
            break;
          }
          case 39ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  bits::extractBits<uint64_t>(word, 0x000000007f7f7f7fULL);
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 32) & 0x7f);
            }
            if (row == nextRow) {
              carryover = ((word >> 40) & 0x7f);
              carryoverBits = 7;
            }
            break;
          }
          case 40ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  static_cast<uint64_t>(static_cast<uint8_t>(word));
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 8) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 16) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x0000007f7f000000ULL);
            }
            if (row == nextRow) {
              carryover = ((word >> 40) & 0x7f);
              carryoverBits = 7;
            }
            break;
          }
          case 41ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 16) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x0000007f7f000000ULL);
            }
            if (row == nextRow) {
              carryover = ((word >> 40) & 0x7f);
              carryoverBits = 7;
            }
            break;
          }
          case 42ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  static_cast<uint64_t>(static_cast<uint8_t>(word));
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x00000000007f7f00ULL);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x0000007f7f000000ULL);
            }
            if (row == nextRow) {
              carryover = ((word >> 40) & 0x7f);
              carryoverBits = 7;
            }
            break;
          }
          case 43ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  bits::extractBits<uint64_t>(word, 0x00000000007f7f7fULL);
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x0000007f7f000000ULL);
            }
            if (row == nextRow) {
              carryover = ((word >> 40) & 0x7f);
              carryoverBits = 7;
            }
            break;
          }
          case 44ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  static_cast<uint64_t>(static_cast<uint8_t>(word));
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 8) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x0000007f7f7f0000ULL);
            }
            if (row == nextRow) {
              carryover = ((word >> 40) & 0x7f);
              carryoverBits = 7;
            }
            break;
          }
          case 45ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x0000007f7f7f0000ULL);
            }
            if (row == nextRow) {
              carryover = ((word >> 40) & 0x7f);
              carryoverBits = 7;
            }
            break;
          }
          case 46ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  static_cast<uint64_t>(static_cast<uint8_t>(word));
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x0000007f7f7f7f00ULL);
            }
            if (row == nextRow) {
              carryover = ((word >> 40) & 0x7f);
              carryoverBits = 7;
            }
            break;
          }
          case 47ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  bits::extractBits<uint64_t>(word, 0x0000007f7f7f7f7fULL);
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (row == nextRow) {
              carryover = ((word >> 40) & 0x7f);
              carryoverBits = 7;
            }
            break;
          }
          case 48ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  static_cast<uint64_t>(static_cast<uint8_t>(word));
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 8) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 16) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 24) & 0x7f);
            }
            if (row == nextRow) {
              carryover =
                  bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
              carryoverBits = 14;
            }
            break;
          }
          case 49ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 16) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 24) & 0x7f);
            }
            if (row == nextRow) {
              carryover =
                  bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
              carryoverBits = 14;
            }
            break;
          }
          case 50ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  static_cast<uint64_t>(static_cast<uint8_t>(word));
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x00000000007f7f00ULL);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 24) & 0x7f);
            }
            if (row == nextRow) {
              carryover =
                  bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
              carryoverBits = 14;
            }
            break;
          }
          case 51ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  bits::extractBits<uint64_t>(word, 0x00000000007f7f7fULL);
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 24) & 0x7f);
            }
            if (row == nextRow) {
              carryover =
                  bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
              carryoverBits = 14;
            }
            break;
          }
          case 52ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  static_cast<uint64_t>(static_cast<uint8_t>(word));
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 8) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x000000007f7f0000ULL);
            }
            if (row == nextRow) {
              carryover =
                  bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
              carryoverBits = 14;
            }
            break;
          }
          case 53ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x000000007f7f0000ULL);
            }
            if (row == nextRow) {
              carryover =
                  bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
              carryoverBits = 14;
            }
            break;
          }
          case 54ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  static_cast<uint64_t>(static_cast<uint8_t>(word));
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x000000007f7f7f00ULL);
            }
            if (row == nextRow) {
              carryover =
                  bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
              carryoverBits = 14;
            }
            break;
          }
          case 55ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  bits::extractBits<uint64_t>(word, 0x000000007f7f7f7fULL);
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (row == nextRow) {
              carryover =
                  bits::extractBits<uint64_t>(word, 0x00007f7f00000000ULL);
              carryoverBits = 14;
            }
            break;
          }
          case 56ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  static_cast<uint64_t>(static_cast<uint8_t>(word));
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 8) & 0x7f);
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 16) & 0x7f);
            }
            if (row == nextRow) {
              carryover =
                  bits::extractBits<uint64_t>(word, 0x00007f7f7f000000ULL);
              carryoverBits = 21;
            }
            break;
          }
          case 57ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 16) & 0x7f);
            }
            if (row == nextRow) {
              carryover =
                  bits::extractBits<uint64_t>(word, 0x00007f7f7f000000ULL);
              carryoverBits = 21;
            }
            break;
          }
          case 58ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  static_cast<uint64_t>(static_cast<uint8_t>(word));
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ =
                  bits::extractBits<uint64_t>(word, 0x00000000007f7f00ULL);
            }
            if (row == nextRow) {
              carryover =
                  bits::extractBits<uint64_t>(word, 0x00007f7f7f000000ULL);
              carryoverBits = 21;
            }
            break;
          }
          case 59ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  bits::extractBits<uint64_t>(word, 0x00000000007f7f7fULL);
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (row == nextRow) {
              carryover =
                  bits::extractBits<uint64_t>(word, 0x00007f7f7f000000ULL);
              carryoverBits = 21;
            }
            break;
          }
          case 60ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  static_cast<uint64_t>(static_cast<uint8_t>(word));
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              *output++ = ((word >> 8) & 0x7f);
            }
            if (row == nextRow) {
              carryover =
                  bits::extractBits<uint64_t>(word, 0x00007f7f7f7f0000ULL);
              carryoverBits = 28;
            }
            break;
          }
          case 61ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  bits::extractBits<uint64_t>(word, 0x0000000000007f7fULL);
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (row == nextRow) {
              carryover =
                  bits::extractBits<uint64_t>(word, 0x00007f7f7f7f0000ULL);
              carryoverBits = 28;
            }
            break;
          }
          case 62ULL: {
            if (isEnabled(row, nextRow, nextRowIndex, rows.data())) {
              const uint64_t firstValue =
                  static_cast<uint64_t>(static_cast<uint8_t>(word));
              *output++ = (firstValue << carryoverBits) | carryover;
              carryover = 0;
              carryoverBits = 0;
            }
            if (row == nextRow) {
              carryover =
                  bits::extractBits<uint64_t>(word, 0x00007f7f7f7f7f00ULL);
              carryoverBits = 35;
            }
            break;
          }
          case 63ULL: {
            if (row == nextRow) {
              carryover |=
                  bits::extractBits<uint64_t>(word, 0x00007f7f7f7f7f7fULL)
                  << carryoverBits;
              carryoverBits += 42;
            }
            break;
          }

          default:
            throw std::logic_error(
                "It should be impossible for control bits to be > 63.");
        }
      }
    }
    if (pos) {
      pos += maskSize;
    }
    bufferStart = pos;
    DCHECK(!carryover || row == nextRow);
    while (nextRowIndex < endRowIndex) {
      skipVarints(nextRow - row);
      *output++ = (readVuLong() << carryoverBits) | carryover;
      carryover = 0;
      carryoverBits = 0;
      if (++nextRowIndex >= endRowIndex) {
        break;
      }
      row = nextRow + 1;
      nextRow = rows[nextRowIndex];
      if (endRow - row >= 8 && bufferEnd - bufferStart > 8 + maskSize) {
        // Go back to fast loop after refilling the buffer.
        pos = bufferStart - maskSize;
        break;
      }
    }
  }
  if (isSigned) {
    bulkZigzagDecode(rows.size(), result);
  }
}

template void IntDecoder<true>::bulkRead(uint64_t size, uint64_t* result);

template void IntDecoder<false>::bulkRead(uint64_t size, uint64_t* result);

template void IntDecoder<true>::bulkRead(uint64_t size, int64_t* result);

template void IntDecoder<false>::bulkRead(uint64_t size, int64_t* result);

template void IntDecoder<true>::bulkRead(uint64_t size, int32_t* result);

template void IntDecoder<false>::bulkRead(uint64_t size, int32_t* result);

template void IntDecoder<true>::bulkRead(uint64_t size, int16_t* result);

template void IntDecoder<false>::bulkRead(uint64_t size, int16_t* result);

template void IntDecoder<true>::bulkReadRows(
    RowSet rows,
    uint64_t* result,
    int32_t initialRow);

template void IntDecoder<true>::bulkReadRows(
    RowSet rows,
    int64_t* result,
    int32_t initialRow);

template void IntDecoder<false>::bulkReadRows(
    RowSet rows,
    int64_t* result,
    int32_t initialRow);

template void IntDecoder<false>::bulkReadRows(
    RowSet rows,
    uint64_t* result,
    int32_t initialRow);

template void IntDecoder<true>::bulkReadRows(
    RowSet rows,
    int32_t* result,
    int32_t initialRow);

template void IntDecoder<false>::bulkReadRows(
    RowSet rows,
    int32_t* result,
    int32_t initialRow);

template void IntDecoder<true>::bulkReadRows(
    RowSet rows,
    int16_t* result,
    int32_t initialRow);

template void IntDecoder<false>::bulkReadRows(
    RowSet rows,
    int16_t* result,
    int32_t initialRow);

#if XSIMD_WITH_AVX2
// Bit unpacking using BMI2 and AVX2.
typedef int32_t __m256si __attribute__((__vector_size__(32), __may_alias__));

typedef int32_t __m256si_u
    __attribute__((__vector_size__(32), __may_alias__, __aligned__(1)));

namespace {

template <int8_t i>
auto as4x64(__m256i x) {
  return _mm256_cvtepu32_epi64(_mm256_extracti128_si256(x, i));
}

template <typename T>
void store8Ints(__m256i eightInts, int32_t i, T* FOLLY_NONNULL result) {
  if (sizeof(T) == 4) {
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(result + i), eightInts);
  } else {
    _mm256_storeu_si256(
        reinterpret_cast<__m256i*>(result + i), as4x64<0>(eightInts));
    _mm256_storeu_si256(
        reinterpret_cast<__m256i*>(result + i + 4), as4x64<1>(eightInts));
  }
}

template <typename T>
inline T* addBytes(T* pointer, int32_t bytes) {
  return reinterpret_cast<T*>(reinterpret_cast<uint64_t>(pointer) + bytes);
}

template <typename T>
inline __m256i as256i(T x) {
  return reinterpret_cast<__m256i>(x);
}

template <typename T>
inline __m256si as8x32(T x) {
  return reinterpret_cast<__m256si>(x);
}

template <uint8_t width, typename T>
FOLLY_ALWAYS_INLINE __m256i gather8Sparse(
    const uint64_t* bits,
    int32_t bitOffset,
    const int32_t* rows,
    int32_t i,
    __m256si masks,
    T* result) {
  constexpr __m256si kMultipliers = {256, 128, 64, 32, 16, 8, 4, 2};

  auto indices =
      *reinterpret_cast<const __m256si_u*>(rows + i) * width + bitOffset;
  __m256si multipliers;
  if (width % 8 != 0) {
    multipliers = (__m256si)_mm256_permutevar8x32_epi32(
        as256i(kMultipliers), as256i(indices & 7));
  }
  auto byteIndices = indices >> 3;
  auto data = as8x32(_mm256_i32gather_epi32(
      reinterpret_cast<const int*>(bits), as256i(byteIndices), 1));
  if (width % 8 != 0) {
    data = (data * multipliers) >> 8;
  }
  return as256i(data & masks);
}

template <uint8_t width, typename T>
int32_t decode1To24(
    const uint64_t* bits,
    int32_t bitOffset,
    const int* rows,
    int32_t numRows,
    T* result) {
  constexpr uint64_t kMask = bits::lowMask(width);
  constexpr uint64_t kMask2 = kMask | (kMask << 8);
  constexpr uint64_t kMask4 = kMask2 | (kMask2 << 16);
  constexpr uint64_t kDepMask8 = kMask4 | (kMask4 << 32);
  constexpr uint64_t kMask16 = kMask | (kMask << 16);
  constexpr uint64_t kDepMask16 = kMask16 | (kMask16 << 32);
  int32_t i = 0;
  const auto masks = as8x32(_mm256_set1_epi32(kMask));
  for (; i + 8 <= numRows; i += 8) {
    auto row = rows[i];
    auto endRow = rows[i + 7];
    __m256i eightInts;
    if (width <= 16 && endRow - row == 7) {
      // Special cases for 8 contiguous values with <= 16 bits.
      if (width <= 8) {
        uint64_t eightBytes;
        if (width == 8) {
          if (!bitOffset) {
            eightBytes = *addBytes(bits, row);
          } else {
            eightBytes =
                bits::detail::loadBits<uint64_t>(bits, bitOffset + 8 * row, 64);
          }
        } else {
          auto bit = row * width + bitOffset;
          auto byte = bit >> 3;
          auto shift = bit & 7;
          uint64_t word = *addBytes(bits, byte) >> shift;
          eightBytes = _pdep_u64(word, kDepMask8);
        }
        eightInts = _mm256_cvtepu8_epi32(
            _mm_loadl_epi64(reinterpret_cast<const __m128i*>(&eightBytes)));
      } else {
        // Use pdep to shift 2 words of bit packed data with width
        // 9-16. For widts <= 14 four bit packed fields can always be
        // loaded with a single uint64_t load. For 15 and 16 bits this
        // depends on the start bit position. For either case we fill
        // an array of 2x64 bits and widen that to a 8x32 word.
        uint64_t words[2];
        if (width <= 14) {
          auto bit = row * width + bitOffset;
          auto byte = bit >> 3;
          auto shift = bit & 7;
          uint64_t word = *addBytes(bits, byte) >> shift;
          words[0] = _pdep_u64(word, kDepMask16);
          bit += 4 * width;
          byte = bit >> 3;
          shift = bit & 7;
          word = *addBytes(bits, byte) >> shift;
          words[1] = _pdep_u64(word, kDepMask16);
        } else {
          words[0] = bits::detail::loadBits<uint64_t>(
              bits, bitOffset + width * row, 64);
          words[1] = bits::detail::loadBits<uint64_t>(
              bits, bitOffset + width * (row + 4), 64);
          if (width == 15) {
            words[0] = _pdep_u64(words[0], kDepMask16);
            words[1] = _pdep_u64(words[1], kDepMask16);
          }
        }
        eightInts = _mm256_cvtepu16_epi32(
            _mm_load_si128(reinterpret_cast<const __m128i*>(&words)));
      }
    } else {
      eightInts = gather8Sparse<width>(bits, bitOffset, rows, i, masks, result);
    }
    store8Ints(eightInts, i, result);
  }
  return i;
}

} // namespace
#endif

#define WIDTH_CASE(width)                                                      \
  case width:                                                                  \
    i = decode1To24<width>(bits, bitOffset, rows.data(), numSafeRows, result); \
    break;

template <bool isSigned>
template <typename T>
// static
void IntDecoder<isSigned>::decodeBitsLE(
    const uint64_t* bits,
    int32_t bitOffset,
    RowSet rows,
    int32_t rowBias,
    uint8_t bitWidth,
    const char* bufferEnd,
    T* result) {
  uint64_t mask = bits::lowMask(bitWidth);
  if (bitWidth == 0) {
    // A column of dictionary indices can be 0 bits wide if all indices are 0.
    memset(result, 0, rows.size() * sizeof(T));
    return;
  }
  // We subtract rowBias * bitWidth bits from the starting position.
  bitOffset -= rowBias * bitWidth;
  if (bitOffset < 0) {
    // Decrement the pointer by enough bytes to have a non-negative bitOffset.
    auto bytes = bits::roundUp(-bitOffset, 8) / 8;
    bitOffset += bytes * 8;
    bits = reinterpret_cast<const uint64_t*>(
        reinterpret_cast<const char*>(bits) - bytes);
  }
  auto numRows = rows.size();
  if (bitWidth > 56) {
    for (auto i = 0; i < numRows; ++i) {
      auto bit = bitOffset + (rows[i]) * bitWidth;
      result[i] = bits::detail::loadBits<T>(bits, bit, bitWidth) & mask;
    }
    return;
  }
  auto FOLLY_NONNULL lastSafe = bufferEnd - sizeof(uint64_t);
  int32_t numSafeRows = numRows;
  bool anyUnsafe = false;
  if (bufferEnd) {
    const char* endByte = reinterpret_cast<const char*>(bits) +
        bits::roundUp(bitOffset + (rows.back() + 1) * bitWidth, 8) / 8;
    // redzone is the number of bytes at the end of the accessed range that
    // could overflow the buffer if accessed 64 its wide.
    int64_t redZone =
        sizeof(uint64_t) - static_cast<int64_t>(bufferEnd - endByte);
    if (redZone > 0) {
      anyUnsafe = true;
      auto numRed = (redZone + 1) * 8 / bitWidth;
      int32_t lastSafeIndex = rows.back() - numRed;
      --numSafeRows;
      for (; numSafeRows >= 1; --numSafeRows) {
        if (rows[numSafeRows - 1] < lastSafeIndex) {
          break;
        }
      }
    }
  }
  int32_t i = 0;

#if XSIMD_WITH_AVX2
  // Use AVX2 for specific widths.
  switch (bitWidth) {
    WIDTH_CASE(1);
    WIDTH_CASE(2);
    WIDTH_CASE(3);
    WIDTH_CASE(4);
    WIDTH_CASE(5);
    WIDTH_CASE(6);
    WIDTH_CASE(7);
    WIDTH_CASE(8);
    WIDTH_CASE(9);
    WIDTH_CASE(10);
    WIDTH_CASE(11);
    WIDTH_CASE(12);
    WIDTH_CASE(13);
    WIDTH_CASE(14);
    WIDTH_CASE(15);
    WIDTH_CASE(16);
    WIDTH_CASE(17);
    WIDTH_CASE(18);
    WIDTH_CASE(19);
    WIDTH_CASE(20);
    WIDTH_CASE(21);
    WIDTH_CASE(22);
    WIDTH_CASE(23);
    WIDTH_CASE(24);
    default:
      break;
  }
#endif

  for (; i < numSafeRows; ++i) {
    auto bit = bitOffset + (rows[i]) * bitWidth;
    auto byte = bit / 8;
    auto shift = bit & 7;
    result[i] = (*reinterpret_cast<const uint64_t*>(
                     reinterpret_cast<const char*>(bits) + byte) >>
                 shift) &
        mask;
  }
  if (anyUnsafe) {
    auto lastSafeWord = bufferEnd - sizeof(uint64_t);
    VELOX_DCHECK(lastSafeWord);
    for (auto i = numSafeRows; i < numRows; ++i) {
      auto bit = bitOffset + (rows[i]) * bitWidth;
      auto byte = bit / 8;
      auto shift = bit & 7;
      result[i] = IntDecoder<isSigned>::safeLoadBits(
                      reinterpret_cast<const char*>(bits) + byte,
                      shift,
                      bitWidth,
                      lastSafeWord) &
          mask;
    }
  }
}

template void IntDecoder<false>::decodeBitsLE(
    const uint64_t* bits,
    int32_t bitOffset,
    RowSet rows,
    int32_t rowBias,
    uint8_t bitWidth,
    const char* bufferEnd,
    int32_t* result);

template void IntDecoder<false>::decodeBitsLE(
    const uint64_t* bits,
    int32_t bitOffset,
    RowSet rows,
    int32_t rowBias,
    uint8_t bitWidth,
    const char* bufferEnd,
    int64_t* result);

template void IntDecoder<true>::decodeBitsLE(
    const uint64_t* bits,
    int32_t bitOffset,
    RowSet rows,
    int32_t rowBias,
    uint8_t bitWidth,
    const char* bufferEnd,
    int32_t* result);

template void IntDecoder<true>::decodeBitsLE(
    const uint64_t* bits,
    int32_t bitOffset,
    RowSet rows,
    int32_t rowBias,
    uint8_t bitWidth,
    const char* bufferEnd,
    int64_t* result);

template void IntDecoder<true>::decodeBitsLE(
    const uint64_t* bits,
    int32_t bitOffset,
    RowSet rows,
    int32_t rowBias,
    uint8_t bitWidth,
    const char* bufferEnd,
    int128_t* result);

template void IntDecoder<false>::decodeBitsLE(
    const uint64_t* bits,
    int32_t bitOffset,
    RowSet rows,
    int32_t rowBias,
    uint8_t bitWidth,
    const char* bufferEnd,
    int128_t* result);

template void IntDecoder<false>::decodeBitsLE(
    const uint64_t* bits,
    int32_t bitOffset,
    RowSet rows,
    int32_t rowBias,
    uint8_t bitWidth,
    const char* bufferEnd,
    int16_t* result);

template void IntDecoder<true>::decodeBitsLE(
    const uint64_t* bits,
    int32_t bitOffset,
    RowSet rows,
    int32_t rowBias,
    uint8_t bitWidth,
    const char* bufferEnd,
    int16_t* result);

#ifdef CODEGEN_BULK_VARINTS
// Codegen for vint bulk decode. This is to document how varintSwitch
// and similar functions were made and how to regenerate modified
// versions of these. This is not intended to be compiled in regular
// builds and exists as documentation only.

std::string codegenVarintMask(int end_byte, int len) {
  CHECK_GE(end_byte + 1, len);
  std::string s = "0x0000000000000000ULL";
  int offset = 5 + end_byte * 2;
  for (int i = 0; i < len; ++i) {
    *(s.end() - offset) = '7';
    *(s.end() - offset + 1) = 'f';
    offset -= 2;
  }
  return s;
}

std::string codegenExtract(int end_byte, int len) {
  if (len > 1) {
    return fmt::format(
        " bits::extractBits<uint64_t>(word, {})",
        codegenVarintMask(end_byte, len));
  }
  if (len == 1 && end_byte == 0) {
    return "static_cast<uint64_t>(static_cast<uint8_t>(word))";
  }
  int sourceByte = end_byte - len + 1;
  std::string s = fmt::format("((word >> {}) & 0x7f)", sourceByte * 8);
  int targetByte = 1;
  for (sourceByte = sourceByte + 1; sourceByte <= end_byte; ++sourceByte) {
    s += fmt::format(
        " | ((word >> {}) & {})",
        sourceByte * 8 - targetByte * 7,
        0x7f << (targetByte * 7));
    ++targetByte;
  }
  return s;
}

std::string startConditional(bool sparse) {
  if (sparse) {
    return "\nif (isEnabled(row, nextRow, nextRowIndex, rows.data())) {";
  }
  return "";
}

std::string startTrailingConditional(bool sparse) {
  if (sparse) {
    return "\nif (row == nextRow) {";
  }
  return "";
}

std::string endConditional(bool sparse) {
  if (sparse) {
    return "\n}";
  }
  return "";
}

std::string advanceCount(bool sparse, int32_t num_varints) {
  if (sparse) {
    fmt::format("\n        row += {};", num_varints);
  }
  return "";
  // return fmt::format("\n        n -= {};", num_varints);
}

std::string codegenControlWordCase(
    uint64_t control_bits,
    int mask_len,
    bool sparse = false) {
  CHECK(control_bits < (1 << mask_len));
  std::string s = fmt::format("      case {}ULL: {{", control_bits);
  int last_zero = -1;
  int num_varints = 0;
  bool postscript_generated = false;
  bool carryover_used = false;
  for (int next_bit = 0; next_bit < mask_len; ++next_bit) {
    // A zero control bit means we detected the end of a varint, so
    // we can construct a mask of the bottom 7 bits starting at the end
    // of the next_bit byte and going back (next_bit - last_zero) bytes.
    if ((control_bits & (1ULL << next_bit)) == 0) {
      if (!sparse && next_bit <= 4 && last_zero == next_bit - 1 &&
          ((control_bits >> next_bit) & 0xf) == 0) {
        // There are up to 4 consecutive single byte vints.
        s += fmt::format("\nunpack4x1(word >> {}, output);", next_bit * 8);
        if (!carryover_used) {
          s += fmt::format(
              "\napplyCarryover(carryoverBits, carryover, output - 4);");
          carryover_used = true;
        }
        if (next_bit >= 3) {
          // Can be we wrote extra if byte 6 or 7 was not a single byte vint.
          s += fmt::format(
              "\nclipTrailingStores({}, controlBits, pos, output);", next_bit);
          postscript_generated = true;
        }
        num_varints += 4;
        next_bit = std::min(next_bit + 3, mask_len - 1);
      } else {
        if (carryover_used) {
          s += startConditional(sparse);
          s += fmt::format(
              "\n        *output++ =  {};",
              codegenExtract(next_bit, next_bit - last_zero));
          s += endConditional(sparse);
        } else {
          s += startConditional(sparse);
          s += fmt::format(
              "\n        const uint64_t firstValue = "
              " {};",
              codegenExtract(next_bit, next_bit - last_zero));
          s +=
              "\n        *output++ = (firstValue << carryoverBits) |             carryover;         ";
          if (sparse) {
            s += "\ncarryover = 0;\ncarryoverBits = 0;";
          }
          s += endConditional(sparse);
          carryover_used = true;
        }
        ++num_varints;
      }
      last_zero = next_bit;
    }
  }
  // Ending on a complete varint, not completing any varint, and completing
  // at least 1 varint but not ending on one are all distinct cases.
  if (last_zero == -1) {
    s += startTrailingConditional(sparse);
    s += fmt::format(
        "\n        carryover |= "
        "bits::extractBits<uint64_t>(word, {}) << carryoverBits;",
        codegenVarintMask(mask_len - 1, mask_len));
    s += fmt::format("\n        carryoverBits += {};", 7 * mask_len);
    s += endConditional(sparse);
  } else if (last_zero >= mask_len - 1) {
    s += "\n        carryover = 0ULL;";
    s += "\n        carryoverBits = 0;";
    if (!postscript_generated) {
      s += advanceCount(sparse, num_varints);
    }
  } else {
    s += startTrailingConditional(sparse);
    s += fmt::format(
        "\n        carryover = {};",
        codegenExtract(mask_len - 1, mask_len - 1 - last_zero));
    s += fmt::format(
        "\n        carryoverBits = {};", 7 * (mask_len - 1 - last_zero));
    s += endConditional(sparse);
    if (!postscript_generated) {
      s += advanceCount(sparse, num_varints);
    }
  }
  s += "\n        break;";
  s += "\n      }";

  return s;
}

std::string codegenSwitch(int maskLen, bool sparse) {
  std::stringstream out;
  for (auto i = 0; i < 1 << maskLen; ++i) {
    out << codegenControlWordCase(i, maskLen, sparse);
  }
  return out.str();
}

void printVarintSwitch(int32_t mask_len, bool sparse) {
  std::cout << codegenSwitch(mask_len, sparse) << std::endl;
}

#endif

} // namespace facebook::velox::dwio::common
