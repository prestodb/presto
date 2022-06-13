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
#include "velox/common/encode/Coding.h"
#include "velox/dwio/common/SeekableInputStream.h"
#include "velox/dwio/common/exception/Exception.h"
#include "velox/dwio/dwrf/common/IntCodecCommon.h"
#include "velox/dwio/dwrf/common/StreamUtil.h"

namespace facebook::velox::dwrf {

template <bool isSigned>
class IntDecoder {
 public:
  static constexpr int32_t kMinDenseBatch = 8;

  IntDecoder(
      std::unique_ptr<dwio::common::SeekableInputStream> input,
      bool useVInts,
      uint32_t numBytes)
      : inputStream(std::move(input)),
        bufferStart(nullptr),
        bufferEnd(bufferStart),
        useVInts(useVInts),
        numBytes(numBytes) {}

  virtual ~IntDecoder() = default;

  /**
   * Seek to a specific row group.
   */
  virtual void seekToRowGroup(
      dwio::common::PositionProvider& positionProvider) = 0;

  /**
   * Seek over a given number of values.
   */
  virtual void skip(uint64_t numValues) = 0;

  /**
   * Read a number of values into the batch.
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
  size_t loadIndices(size_t startIndex) {
    return inputStream->positionSize() + startIndex + 1;
  }

  /**
   * Create an RLE decoder.
   * @param input the input stream to read from
   * @param version version of RLE decoding to do
   * @param pool memory pool to use for allocation
   */
  static std::unique_ptr<IntDecoder<isSigned>> createRle(
      std::unique_ptr<dwio::common::SeekableInputStream> input,
      RleVersion version,
      memory::MemoryPool& pool,
      bool useVInts,
      uint32_t numBytes);

  /**
   * Create a direct decoder
   */
  static std::unique_ptr<IntDecoder<isSigned>> createDirect(
      std::unique_ptr<dwio::common::SeekableInputStream> input,
      bool useVInts,
      uint32_t numBytes);

  void skipLongs(uint64_t numValues);

  // Optimized variant of skipLongs using popcnt. Used on selective
  // path only pending validation.
  void skipLongsFast(uint64_t numValues);

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
  template <typename T>
  void bulkReadFixed(uint64_t size, T* result);

  template <typename T>
  void bulkReadRowsFixed(RowSet rows, int32_t initialRow, T* result);

  signed char readByte();

  int64_t readLong();
  uint64_t readVuLong();
  int64_t readVsLong();
  int64_t readLongLE();

  // Applies 'visitor to 'numRows' consecutive values.
  template <typename Visitor>
  void readDense(int32_t numRows, Visitor& visitor) {
    auto data = visitor.mutableValues(numRows);
    bulkRead(numRows, data);
    visitor.processN(data, numRows);
  }

 private:
  uint64_t skipVarintsInBuffer(uint64_t items);
  void skipVarints(uint64_t items);

 protected:
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

  const std::unique_ptr<dwio::common::SeekableInputStream> inputStream;
  const char* bufferStart;
  const char* bufferEnd;
  const bool useVInts;
  const uint32_t numBytes;
};

template <bool isSigned>
FOLLY_ALWAYS_INLINE signed char IntDecoder<isSigned>::readByte() {
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
  return ZigZag::decode(readVuLong());
}

template <bool isSigned>
inline int64_t IntDecoder<isSigned>::readLongLE() {
  int64_t result = 0;
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
inline int64_t IntDecoder<isSigned>::readLong() {
  if (useVInts) {
    if constexpr (isSigned) {
      return readVsLong();
    } else {
      return static_cast<int64_t>(readVuLong());
    }
  } else {
    return readLongLE();
  }
}

} // namespace facebook::velox::dwrf
