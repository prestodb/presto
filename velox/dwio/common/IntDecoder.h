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

#include <folly/Likely.h>
#include <folly/Range.h>
#include <folly/Varint.h>
#include "velox/common/base/Nulls.h"
#include "velox/common/encode/Coding.h"
#include "velox/dwio/common/IntCodecCommon.h"
#include "velox/dwio/common/SeekableInputStream.h"
#include "velox/dwio/common/StreamUtil.h"
#include "velox/dwio/common/exception/Exception.h"

namespace facebook::velox::dwio::common {

template <bool isSigned>
class IntDecoder {
 public:
  static constexpr int32_t kMinDenseBatch = 8;
  static constexpr bool kIsSigned = isSigned;

  IntDecoder(
      std::unique_ptr<dwio::common::SeekableInputStream> input,
      bool useVInts,
      uint32_t numBytes,
      bool bigEndian = false)
      : inputStream(std::move(input)),
        bufferStart(nullptr),
        bufferEnd(bufferStart),
        useVInts(useVInts),
        numBytes(numBytes),
        bigEndian(bigEndian) {}

  // Constructs for use in Parquet /Alphawhere the buffer is always preloaded.
  IntDecoder(const char* start, const char* end)
      : bufferStart(start), bufferEnd(end), useVInts(false), numBytes(0) {}

  virtual ~IntDecoder() = default;

  /**
   * Seek to a specific row group.  Should not read the underlying input stream
   * to avoid decoding same data multiple times.
   */
  virtual void seekToRowGroup(
      dwio::common::PositionProvider& positionProvider) = 0;

  /**
   * Seek over a given number of values.  Does not decode the underlying input
   * stream.
   */
  void skip(uint64_t numValues) {
    pendingSkip += numValues;
  }

  /**
   * Read a number of values into the batch.  Should call skipPending() in the
   * beginning.
   *
   * @param data the array to read into
   * @param numValues the number of values to read
   * @param nulls If the pointer is null, all values are read. If the
   *    pointer is not null, positions that are true are skipped.
   */
  virtual void
  next(int64_t* data, uint64_t numValues, const uint64_t* nulls) = 0;

  virtual void next(int32_t* data, uint64_t numValues, const uint64_t* nulls) {
    if (numValues <= 4) {
      int64_t temp[4];
      next(temp, numValues, nulls);
      for (int32_t i = 0; i < numValues; ++i) {
        data[i] = temp[i];
      }
    } else {
      std::vector<int64_t> temp(numValues);
      next(temp.data(), numValues, nulls);
      for (int32_t i = 0; i < numValues; ++i) {
        data[i] = temp[i];
      }
    }
  }

  virtual void
  nextInts(int32_t* data, uint64_t numValues, const uint64_t* nulls) {
    narrow(data, numValues, nulls);
  }

  virtual void
  nextShorts(int16_t* data, uint64_t numValues, const uint64_t* nulls) {
    narrow(data, numValues, nulls);
  }

  virtual void nextLengths(int32_t* /*values*/, int32_t /*numValues*/) {
    VELOX_FAIL("A length decoder should be a RLEv1");
  }

  /**
   * Load RowIndex values for the stream being read.
   * @return updated start index after this stream's index values.
   */
  size_t loadIndices(size_t startIndex) const {
    return inputStream->positionSize() + startIndex + 1;
  }

  // Reads 'size' consecutive T' and stores then in 'result'.
  template <typename T>
  void bulkRead(uint64_t size, T* result);

  // Reads data at positions 'rows' to 'result'. 'initialRow' is the
  // row number of the first unread element of 'this'. if rows is {10}
  // and 'initialRow' is 9, then this skips one element and reads the
  // next element into 'result'.
  template <typename T>
  void bulkReadRows(RowSet rows, T* result, int32_t initialRow = 0);

 protected:
  // Actually skip the pending entries.
  virtual void skipPending() = 0;

  template <bool kHasNulls>
  inline void skip(int32_t numValues, int32_t current, const uint64_t* nulls) {
    if constexpr (kHasNulls) {
      numValues = bits::countNonNulls(nulls, current, current + numValues);
    }
    pendingSkip += numValues;
    if (pendingSkip > 0) {
      skipPending();
    }
  }

  void skipLongs(uint64_t numValues);

  template <typename T>
  void bulkReadFixed(uint64_t size, T* result);

  template <typename T>
  void bulkReadRowsFixed(RowSet rows, int32_t initialRow, T* result);

  template <typename T>
  T readInt();

  template <typename T>
  T readVInt();

  signed char readByte();
  uint64_t readVuLong();
  int64_t readVsLong();
  int64_t readLongLE();

  template <typename cppType>
  cppType readLittleEndianFromBigEndian();

 private:
  uint64_t skipVarintsInBuffer(uint64_t items);
  void skipVarints(uint64_t items);
  int128_t readVsHugeInt();
  uint128_t readVuHugeInt();

  // note: there is opportunity for performance gains here by avoiding
  //       this by directly supporting deserialization into the correct
  //       target data type
  template <typename T>
  void
  narrow(T* const data, const uint64_t numValues, const uint64_t* const nulls) {
    DWIO_ENSURE_LE(numBytes, sizeof(T))
    std::array<int64_t, 64> buf;
    uint64_t remain = numValues;
    T* dataPtr = data;
    const uint64_t* nullsPtr = nulls;
    while (remain != 0) {
      uint64_t num = std::min(remain, static_cast<uint64_t>(buf.size()));
      next(buf.data(), num, nullsPtr);
      for (uint64_t i = 0; i < num; ++i) {
        *(dataPtr++) = (T)buf[i];
      }
      remain -= num;
      if (remain != 0 && nullsPtr) {
        DWIO_ENSURE(num % 64 == 0);
        nullsPtr += num / 64;
      }
    }
  }

 protected:
  const std::unique_ptr<dwio::common::SeekableInputStream> inputStream;
  const char* bufferStart;
  const char* bufferEnd;
  const bool useVInts;
  const uint32_t numBytes;
  bool bigEndian;
  int64_t pendingSkip = 0;
};

template <bool isSigned>
FOLLY_ALWAYS_INLINE signed char IntDecoder<isSigned>::readByte() {
  VELOX_DCHECK_EQ(pendingSkip, 0);
  if (UNLIKELY(bufferStart == bufferEnd)) {
    int32_t bufferLength;
    const void* bufferPointer;
    DWIO_ENSURE(
        inputStream->Next(&bufferPointer, &bufferLength),
        "bad read in readByte, ",
        inputStream->getName());
    bufferStart = static_cast<const char*>(bufferPointer);
    bufferEnd = bufferStart + bufferLength;
  }
  return *(bufferStart++);
}

template <bool isSigned>
FOLLY_ALWAYS_INLINE uint64_t IntDecoder<isSigned>::readVuLong() {
  VELOX_DCHECK_EQ(pendingSkip, 0);
  if (LIKELY(bufferEnd - bufferStart >= folly::kMaxVarintLength64)) {
    const char* p = bufferStart;
    uint64_t val;
    do {
      int64_t b;
      b = *p++;
      val = (b & 0x7f);
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      val |= (b & 0x7f) << 7;
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      val |= (b & 0x7f) << 14;
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      val |= (b & 0x7f) << 21;
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      val |= (b & 0x7f) << 28;
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      val |= (b & 0x7f) << 35;
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      val |= (b & 0x7f) << 42;
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      val |= (b & 0x7f) << 49;
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      val |= (b & 0x7f) << 56;
      if (UNLIKELY(b >= 0)) {
        break;
      }
      b = *p++;
      val |= (b & 0x01) << 63;
      if (LIKELY(b >= 0)) {
        break;
      } else {
        DWIO_RAISE(fmt::format(
            "Invalid encoding: likely corrupt data.  bytes remaining: {} , useVInts: {}, numBytes: {}, Input Stream Name: {}, byte: {}, val: {}",
            bufferEnd - bufferStart,
            useVInts,
            numBytes,
            inputStream->getName(),
            b,
            val));
      }
    } while (false);
    bufferStart = p;
    return val;
  } else {
    int64_t result = 0;
    int64_t offset = 0;
    signed char ch;
    do {
      ch = readByte();
      result |= (ch & BASE_128_MASK) << offset;
      offset += 7;
    } while (ch < 0);
    return result;
  }
}

template <bool isSigned>
FOLLY_ALWAYS_INLINE int64_t IntDecoder<isSigned>::readVsLong() {
  return ZigZag::decode<uint64_t>(readVuLong());
}

template <bool isSigned>
inline int64_t IntDecoder<isSigned>::readLongLE() {
  VELOX_DCHECK_EQ(pendingSkip, 0);
  int64_t result = 0;
  if (bufferStart && bufferStart + sizeof(int64_t) <= bufferEnd) {
    bufferStart += numBytes;
    if (numBytes == 8) {
      return *reinterpret_cast<const int64_t*>(bufferStart - 8);
    }
    if (numBytes == 4) {
      if (isSigned) {
        return *reinterpret_cast<const int32_t*>(bufferStart - 4);
      }
      return *reinterpret_cast<const uint32_t*>(bufferStart - 4);
    }
    if (isSigned) {
      return *reinterpret_cast<const int16_t*>(bufferStart - 2);
    }
    return *reinterpret_cast<const uint16_t*>(bufferStart - 2);
  }
  char b;
  int64_t offset = 0;
  for (uint32_t i = 0; i < numBytes; ++i) {
    b = readByte();
    result |= (b & BASE_256_MASK) << offset;
    offset += 8;
  }

  if (isSigned && numBytes < 8) {
    if (numBytes == 2) {
      return static_cast<int16_t>(result);
    }
    if (numBytes == 4) {
      return static_cast<int32_t>(result);
    }
    DCHECK(false) << "Bad width for signed fixed width: " << numBytes;
  }
  return result;
}

template <bool isSigned>
template <typename cppType>
inline cppType IntDecoder<isSigned>::readLittleEndianFromBigEndian() {
  VELOX_DCHECK_EQ(pendingSkip, 0);
  cppType bigEndianValue = 0;
  // Input is in Big Endian layout of size numBytes.
  if (bufferStart && bufferStart + sizeof(int64_t) <= bufferEnd) {
    bufferStart += numBytes;
    auto valueOffset = bufferStart - numBytes;
    // Use first byte to initialize bigEndianValue.
    bigEndianValue =
        *(reinterpret_cast<const int8_t*>(valueOffset)) >= 0 ? 0 : -1;
    // Copy numBytes input to the bigEndianValue.
    memcpy(
        reinterpret_cast<char*>(&bigEndianValue) + (sizeof(cppType) - numBytes),
        reinterpret_cast<const char*>(valueOffset),
        numBytes);
    // Convert bigEndianValue to little endian value and return.
    if constexpr (sizeof(cppType) == 16) {
      return bits::builtin_bswap128(bigEndianValue);
    } else {
      return __builtin_bswap64(bigEndianValue);
    }
  }
  char b;
  cppType offset = 0;
  cppType numBytesBigEndian = 0;
  // Read numBytes input into numBytesBigEndian.
  for (uint32_t i = 0; i < numBytes; ++i) {
    b = readByte();
    if constexpr (sizeof(cppType) == 16) {
      numBytesBigEndian |= (b & INT128_BASE_256_MASK) << offset;
    } else {
      numBytesBigEndian |= (b & BASE_256_MASK) << offset;
    }
    offset += 8;
  }
  // Use first byte to initialize bigEndianValue.
  bigEndianValue =
      (reinterpret_cast<const int8_t*>(&numBytesBigEndian)[0]) >= 0 ? 0 : -1;
  // Copy numBytes input to the bigEndianValue.
  memcpy(
      reinterpret_cast<char*>(&bigEndianValue) + (sizeof(cppType) - numBytes),
      reinterpret_cast<const char*>(&numBytesBigEndian),
      numBytes);
  // Convert bigEndianValue to little endian value and return.
  if constexpr (sizeof(cppType) == 16) {
    return bits::builtin_bswap128(bigEndianValue);
  } else {
    return __builtin_bswap64(bigEndianValue);
  }
}

template <bool isSigned>
inline int128_t IntDecoder<isSigned>::readVsHugeInt() {
  return ZigZag::decode<uint128_t>(readVuHugeInt());
}

template <bool isSigned>
inline uint128_t IntDecoder<isSigned>::readVuHugeInt() {
  VELOX_DCHECK_EQ(pendingSkip, 0);
  uint128_t value = 0;
  uint128_t work;
  uint32_t offset = 0;
  signed char ch;
  while (true) {
    ch = readByte();
    work = ch & 0x7f;
    work <<= offset;
    value |= work;
    offset += 7;
    if (!(ch & 0x80)) {
      break;
    }
  }
  return value;
}

template <bool isSigned>
template <typename T>
inline T IntDecoder<isSigned>::readInt() {
  if (useVInts) {
    return readVInt<T>();
  }
  if (bigEndian) {
    return readLittleEndianFromBigEndian<T>();
  } else {
    if constexpr (std::is_same_v<T, int128_t>) {
      VELOX_NYI();
    }
    return readLongLE();
  }
}

template <bool isSigned>
template <typename T>
inline T IntDecoder<isSigned>::readVInt() {
  if constexpr (isSigned) {
    if constexpr (std::is_same_v<T, int128_t>) {
      return readVsHugeInt();
    } else {
      return readVsLong();
    }
  } else {
    if constexpr (std::is_same_v<T, int128_t>) {
      return readVuHugeInt();
    } else {
      return readVuLong();
    }
  }
}

template <>
template <>
inline void IntDecoder<false>::bulkRead(uint64_t /*size*/, double* /*result*/) {
  VELOX_UNREACHABLE();
}

template <>
template <>
inline void IntDecoder<false>::bulkReadRows(
    RowSet /*rows*/,
    double* /*result*/,
    int32_t /*initialRow*/) {
  VELOX_UNREACHABLE();
}

template <>
template <>
inline void IntDecoder<true>::bulkRead(uint64_t /*size*/, double* /*result*/) {
  VELOX_UNREACHABLE();
}

template <>
template <>
inline void IntDecoder<true>::bulkReadRows(
    RowSet /*rows*/,
    double* /*result*/,
    int32_t /*initialRow*/) {
  VELOX_UNREACHABLE();
}

template <>
template <>
inline void IntDecoder<false>::bulkRead(uint64_t /*size*/, float* /*result*/) {
  VELOX_UNREACHABLE();
}

template <>
template <>
inline void IntDecoder<false>::bulkReadRows(
    RowSet /*rows*/,
    float* /*result*/,
    int32_t /*initialRow*/) {
  VELOX_UNREACHABLE();
}

template <>
template <>
inline void IntDecoder<true>::bulkRead(uint64_t /*size*/, float* /*result*/) {
  VELOX_UNREACHABLE();
}

template <>
template <>
inline void IntDecoder<true>::bulkReadRows(
    RowSet /*rows*/,
    float* /*result*/,
    int32_t /*initialRow*/) {
  VELOX_UNREACHABLE();
}

template <>
template <>
inline void IntDecoder<false>::bulkRead(
    uint64_t /*size*/,
    int128_t* /*result*/) {
  VELOX_UNREACHABLE();
}

template <>
template <>
inline void IntDecoder<false>::bulkReadRows(
    RowSet /*rows*/,
    int128_t* /*result*/,
    int32_t /*initialRow*/) {
  VELOX_UNREACHABLE();
}

template <>
template <>
inline void IntDecoder<true>::bulkRead(
    uint64_t /*size*/,
    int128_t* /*result*/) {
  VELOX_UNREACHABLE();
}

template <>
template <>
inline void IntDecoder<true>::bulkReadRows(
    RowSet /*rows*/,
    int128_t* /*result*/,
    int32_t /*initialRow*/) {
  VELOX_UNREACHABLE();
}

} // namespace facebook::velox::dwio::common
