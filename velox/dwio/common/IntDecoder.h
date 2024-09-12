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
      : inputStream_(std::move(input)),
        bufferStart_(nullptr),
        bufferEnd_(bufferStart_),
        useVInts_(useVInts),
        numBytes_(numBytes),
        bigEndian_(bigEndian) {}

  /// Constructs for use in Parquet /Alphawhere the buffer is always preloaded.
  IntDecoder(const char* start, const char* end)
      : bufferStart_(start), bufferEnd_(end), useVInts_(false), numBytes_(0) {}

  virtual ~IntDecoder() = default;

  /// Seeks to a specific row group.  Should not read the underlying input
  /// stream to avoid decoding same data multiple times.
  virtual void seekToRowGroup(
      dwio::common::PositionProvider& positionProvider) = 0;

  /// Seeks over a given number of values.  Does not decode the underlying input
  /// stream.
  void skip(uint64_t numValues) {
    pendingSkip_ += numValues;
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
    return inputStream_->positionSize() + startIndex + 1;
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
    pendingSkip_ += numValues;
    if (pendingSkip_ > 0) {
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

  // NOTE: there is opportunity for performance gains here by avoiding this by
  // directly supporting deserialization into the correct target data type
  template <typename T>
  void narrow(T* data, uint64_t numValues, const uint64_t* nulls) {
    VELOX_CHECK_LE(numBytes_, sizeof(T));
    std::array<int64_t, 64> buf;
    uint64_t remain = numValues;
    T* dataPtr = data;
    const uint64_t* nullsPtr = nulls;
    while (remain > 0) {
      const uint64_t num = std::min(remain, static_cast<uint64_t>(buf.size()));
      next(buf.data(), num, nullsPtr);
      for (uint64_t i = 0; i < num; ++i) {
        *(dataPtr++) = (T)buf[i];
      }
      remain -= num;
      if (remain != 0 && nullsPtr) {
        VELOX_CHECK_EQ(num % 64, 0);
        nullsPtr += num / 64;
      }
    }
  }

 protected:
  const std::unique_ptr<dwio::common::SeekableInputStream> inputStream_;
  const char* bufferStart_;
  const char* bufferEnd_;
  const bool useVInts_;
  const uint32_t numBytes_;
  const bool bigEndian_;
  int64_t pendingSkip_{0};
};

template <bool isSigned>
FOLLY_ALWAYS_INLINE signed char IntDecoder<isSigned>::readByte() {
  VELOX_DCHECK_EQ(pendingSkip_, 0);

  if (UNLIKELY(bufferStart_ == bufferEnd_)) {
    int32_t bufferLength;
    const void* bufferPointer;
    const bool ret = inputStream_->Next(&bufferPointer, &bufferLength);
    VELOX_CHECK(ret, "bad read in readByte, ", inputStream_->getName());
    bufferStart_ = static_cast<const char*>(bufferPointer);
    bufferEnd_ = bufferStart_ + bufferLength;
  }
  return *(bufferStart_++);
}

template <bool isSigned>
FOLLY_ALWAYS_INLINE uint64_t IntDecoder<isSigned>::readVuLong() {
  VELOX_DCHECK_EQ(pendingSkip_, 0);

  if (LIKELY(bufferEnd_ - bufferStart_ >= folly::kMaxVarintLength64)) {
    const char* p = bufferStart_;
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
        VELOX_FAIL(
            "Invalid encoding: likely corrupt data.  bytes remaining: {} , useVInts: {}, numBytes: {}, Input Stream Name: {}, byte: {}, val: {}",
            bufferEnd_ - bufferStart_,
            useVInts_,
            numBytes_,
            inputStream_->getName(),
            b,
            val);
      }
    } while (false);

    bufferStart_ = p;
    return val;
  }

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

template <bool isSigned>
FOLLY_ALWAYS_INLINE int64_t IntDecoder<isSigned>::readVsLong() {
  return ZigZag::decode<uint64_t>(readVuLong());
}

template <bool isSigned>
inline int64_t IntDecoder<isSigned>::readLongLE() {
  VELOX_DCHECK_EQ(pendingSkip_, 0);
  int64_t result = 0;
  if (bufferStart_ && bufferStart_ + sizeof(int64_t) <= bufferEnd_) {
    bufferStart_ += numBytes_;
    if (numBytes_ == 8) {
      return *reinterpret_cast<const int64_t*>(bufferStart_ - 8);
    }
    if (numBytes_ == 4) {
      if (isSigned) {
        return *reinterpret_cast<const int32_t*>(bufferStart_ - 4);
      }
      return *reinterpret_cast<const uint32_t*>(bufferStart_ - 4);
    }
    if (isSigned) {
      return *reinterpret_cast<const int16_t*>(bufferStart_ - 2);
    }
    return *reinterpret_cast<const uint16_t*>(bufferStart_ - 2);
  }

  char b;
  int64_t offset = 0;
  for (uint32_t i = 0; i < numBytes_; ++i) {
    b = readByte();
    result |= (b & BASE_256_MASK) << offset;
    offset += 8;
  }

  if (isSigned && numBytes_ < 8) {
    if (numBytes_ == 2) {
      return static_cast<int16_t>(result);
    }
    if (numBytes_ == 4) {
      return static_cast<int32_t>(result);
    }
    VELOX_DCHECK(false, "Bad width for signed fixed width: {}", numBytes_);
  }
  return result;
}

template <bool isSigned>
template <typename cppType>
inline cppType IntDecoder<isSigned>::readLittleEndianFromBigEndian() {
  VELOX_DCHECK_EQ(pendingSkip_, 0);

  cppType bigEndianValue = 0;
  // Input is in Big Endian layout of size numBytes.
  if (bufferStart_ && (bufferStart_ + sizeof(int64_t) <= bufferEnd_)) {
    bufferStart_ += numBytes_;
    const auto valueOffset = bufferStart_ - numBytes_;
    // Use first byte to initialize bigEndianValue.
    bigEndianValue =
        *(reinterpret_cast<const int8_t*>(valueOffset)) >= 0 ? 0 : -1;
    // Copy numBytes input to the bigEndianValue.
    ::memcpy(
        reinterpret_cast<char*>(&bigEndianValue) +
            (sizeof(cppType) - numBytes_),
        reinterpret_cast<const char*>(valueOffset),
        numBytes_);
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
  for (uint32_t i = 0; i < numBytes_; ++i) {
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
  ::memcpy(
      reinterpret_cast<char*>(&bigEndianValue) + (sizeof(cppType) - numBytes_),
      reinterpret_cast<const char*>(&numBytesBigEndian),
      numBytes_);
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
  VELOX_DCHECK_EQ(pendingSkip_, 0);

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
  if (useVInts_) {
    return readVInt<T>();
  }
  if (bigEndian_) {
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
