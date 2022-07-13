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

#include <folly/Varint.h>
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/GTestMacros.h"
#include "velox/common/base/Nulls.h"
#include "velox/common/encode/Coding.h"
#include "velox/dwio/common/IntCodecCommon.h"
#include "velox/dwio/dwrf/common/Common.h"
#include "velox/dwio/dwrf/common/OutputStream.h"
#include "velox/dwio/dwrf/common/Range.h"

namespace facebook::velox::dwrf {

template <bool isSigned>
class IntEncoder {
 public:
  IntEncoder(
      std::unique_ptr<BufferedOutputStream> output,
      bool useVInts,
      uint32_t numBytes)
      : output_{std::move(output)},
        useVInts_{useVInts},
        numBytes_{numBytes},
        buffer_{nullptr},
        bufferPosition_{0},
        bufferLength_{0} {}

  virtual ~IntEncoder() = default;

  /**
   * Encode the next batch of values.
   * @param data the array to read from
   * @param numValues the number of values to write
   * @param nulls If the pointer is null, all values are read. If the
   *    pointer is not null, positions that are true are skipped.
   */
  virtual uint64_t
  add(const int64_t* data, const Ranges& ranges, const uint64_t* nulls) {
    return addImpl(data, ranges, nulls);
  }

  // We need both signed and unsigned interface to allow cast to int64_t
  // directly. In int dictionary encoding, DATA stream is defined as unsigned,
  // but may contain both unsigned (for dictionary offsets) and signed (for
  // original values), having both interfaces allows us to avoid casting signed
  // to unsigned then to int64_t.
  virtual uint64_t
  add(const int32_t* data, const Ranges& ranges, const uint64_t* nulls) {
    return addImpl(data, ranges, nulls);
  }

  virtual uint64_t
  add(const uint32_t* data, const Ranges& ranges, const uint64_t* nulls) {
    return addImpl(data, ranges, nulls);
  }

  virtual uint64_t
  add(const int16_t* data, const Ranges& ranges, const uint64_t* nulls) {
    return addImpl(data, ranges, nulls);
  }

  virtual uint64_t
  add(const uint16_t* data, const Ranges& ranges, const uint64_t* nulls) {
    return addImpl(data, ranges, nulls);
  }

  /**
   * writeValue lets the caller add one non null value at a time.
   * This method is significantly slower, when compared to "add"
   * due to condition checking for each write. But beneficial when
   * the caller has a stream to transform and caller does not want to
   * allocate extra memory, for e.g. {@see dwrf::TimestampColumnWriter}.
   *
   * This method is intentionally implemented only for int64 instead of
   * templates, as the only use case for this now is int64_t.
   * @param value value to be written.
   */
  virtual void writeValue(const int64_t value) {
    if (!useVInts_) {
      writeLongLE(value);
    } else {
      if constexpr (isSigned) {
        writeVslong(value);
      } else {
        writeVulong(value);
      }
    }
  }

  /**
   * Get size of buffer used so far.
   */
  uint64_t getBufferSize() const {
    return output_->size();
  }

  /**
   * Flushing underlying BufferedOutputStream
   */
  virtual uint64_t flush();

  /**
   * record current position for a specific stride, negative stride index
   * signals to append to the latest stride.
   * @param recorder use the recorder to record current positions to backfill
   *                 the index entry for a specific stride.
   * @param strideIndex the index of the stride to backfill
   */
  virtual void recordPosition(
      PositionRecorder& recorder,
      int32_t strideIndex = -1) const {
    output_->recordPosition(
        recorder, bufferLength_, bufferPosition_, strideIndex);
  }

  /**
   * Create an RLE encoder.
   * @param output the output stream to write to
   * @param isSigned true if the number sequence is signed
   * @param version version of RLE decoding to do
   * @param pool memory pool to use for allocation
   */
  static std::unique_ptr<IntEncoder<isSigned>> createRle(
      RleVersion version,
      std::unique_ptr<BufferedOutputStream> output,
      bool useVInts,
      uint32_t numBytes);

  /**
   * Create a direct encoder
   */
  static std::unique_ptr<IntEncoder<isSigned>> createDirect(
      std::unique_ptr<BufferedOutputStream> output,
      bool useVInts,
      uint32_t numBytes);

 protected:
  std::unique_ptr<BufferedOutputStream> output_;
  const bool useVInts_;
  const uint32_t numBytes_;

  char* buffer_;
  int32_t bufferPosition_;
  int32_t bufferLength_;

  FOLLY_ALWAYS_INLINE void writeByte(char c);
  FOLLY_ALWAYS_INLINE void writeVulong(int64_t val);
  FOLLY_ALWAYS_INLINE void writeVslong(int64_t val) {
    writeVulong(ZigZag::encode(val));
  }
  FOLLY_ALWAYS_INLINE void writeLongLE(int64_t val);

 private:
  template <typename T>
  uint64_t addImpl(const T* data, const Ranges& ranges, const uint64_t* nulls);

  FOLLY_ALWAYS_INLINE void writeBuffer(char* start, char* end) {
    int32_t valsToWrite = end - start;
    while (valsToWrite) {
      if (bufferPosition_ == bufferLength_) {
        DWIO_ENSURE(
            output_->Next(reinterpret_cast<void**>(&buffer_), &bufferLength_),
            "failed to allocate");
        bufferPosition_ = 0;
      }
      int32_t numVals = std::min(valsToWrite, bufferLength_ - bufferPosition_);
      memcpy(buffer_ + bufferPosition_, start, numVals);
      bufferPosition_ += numVals;
      start += numVals;
      valsToWrite -= numVals;
    }
  }

  template <int32_t size>
  FOLLY_ALWAYS_INLINE static int32_t writeVarint(uint64_t value, char* buffer);
  FOLLY_ALWAYS_INLINE static int32_t write64Varint(
      uint64_t value,
      char* buffer);

  FOLLY_ALWAYS_INLINE int32_t writeVulong(int64_t value, char* buffer);
  FOLLY_ALWAYS_INLINE int32_t writeVslong(int64_t value, char* buffer);
  FOLLY_ALWAYS_INLINE int32_t writeLongLE(int64_t value, char* buffer);

  VELOX_FRIEND_TEST(TestIntEncoder, TestVarIntEncoder);
};

#define WRITE_INTS(FUNC)                                       \
  uint64_t count = 0;                                          \
  char buffer[1024];                                           \
  char* writeLoc = buffer;                                     \
  char* endBuf = buffer + 1024;                                \
  if (nulls) {                                                 \
    for (const auto [start, end] : ranges.getRanges()) {       \
      for (auto pos = start; pos < end; ++pos) {               \
        if (!bits::isBitNull(nulls, pos)) {                    \
          writeLoc += FUNC(data[pos], writeLoc);               \
          if (writeLoc + folly::kMaxVarintLength64 > endBuf) { \
            writeBuffer(buffer, writeLoc);                     \
            writeLoc = buffer;                                 \
          }                                                    \
          count++;                                             \
        }                                                      \
      }                                                        \
    }                                                          \
  } else {                                                     \
    for (const auto [start, end] : ranges.getRanges()) {       \
      for (auto pos = start; pos < end; ++pos) {               \
        writeLoc += FUNC(data[pos], writeLoc);                 \
        if (writeLoc + folly::kMaxVarintLength64 > endBuf) {   \
          writeBuffer(buffer, writeLoc);                       \
          writeLoc = buffer;                                   \
        }                                                      \
      }                                                        \
      count += (end - start);                                  \
    }                                                          \
  }                                                            \
  writeBuffer(buffer, writeLoc);                               \
  return count;

template <bool isSigned>
template <typename T>
uint64_t IntEncoder<isSigned>::addImpl(
    const T* data,
    const Ranges& ranges,
    const uint64_t* nulls) {
  if (!useVInts_) {
    WRITE_INTS(writeLongLE);
  } else {
    if constexpr (isSigned) {
      WRITE_INTS(writeVslong);
    } else {
      WRITE_INTS(writeVulong);
    }
  }
}

#undef WRITE_INTS
template <bool isSigned>
void IntEncoder<isSigned>::writeByte(char c) {
  if (UNLIKELY(bufferPosition_ == bufferLength_)) {
    DWIO_ENSURE(
        output_->Next(reinterpret_cast<void**>(&buffer_), &bufferLength_),
        "failed to allocate");
    bufferPosition_ = 0;
  }
  buffer_[bufferPosition_++] = c;
}

template <bool isSigned>
template <int32_t size>
/* static */ int32_t IntEncoder<isSigned>::writeVarint(
    uint64_t value,
    char* buffer) {
  for (int32_t i = 1; i < size; i++) {
    *buffer = static_cast<char>(0x80 | (value & dwio::common::BASE_128_MASK));
    value >>= 7;
    buffer++;
  }
  DCHECK(value < 128);
  *buffer = static_cast<char>(value);
  return size;
}

template <bool isSigned>
/* static */ int32_t IntEncoder<isSigned>::write64Varint(
    uint64_t value,
    char* buffer) {
  int32_t leadingZeros = __builtin_clzll(value | 1);
  DCHECK(leadingZeros <= 63);
  switch (leadingZeros) {
    case 0:
      return writeVarint<10>(value, buffer);
    case 1 ... 7:
      return writeVarint<9>(value, buffer);
    case 8 ... 14:
      return writeVarint<8>(value, buffer);
    case 15 ... 21:
      return writeVarint<7>(value, buffer);
    case 22 ... 28:
      return writeVarint<6>(value, buffer);
    case 29 ... 35:
      return writeVarint<5>(value, buffer);
    case 36 ... 42:
      return writeVarint<4>(value, buffer);
    case 43 ... 49:
      return writeVarint<3>(value, buffer);
    case 50 ... 56:
      return writeVarint<2>(value, buffer);
    case 57 ... 63:
      return writeVarint<1>(value, buffer);
  }
  DWIO_RAISE(folly::sformat(
      "Unexpected leading zeros {} for value {}", leadingZeros, value));
}

template <bool isSigned>
int32_t IntEncoder<isSigned>::writeVslong(int64_t value, char* buffer) {
  return write64Varint(ZigZag::encode(value), buffer);
}

template <bool isSigned>
int32_t IntEncoder<isSigned>::writeVulong(int64_t val, char* buffer) {
  return write64Varint(static_cast<uint64_t>(val), buffer);
}

template <bool isSigned>
int32_t IntEncoder<isSigned>::writeLongLE(int64_t val, char* buffer) {
  for (int32_t i = 0; i < numBytes_; i++) {
    *buffer = val & dwio::common::BASE_256_MASK;
    val >>= 8;
    buffer++;
  }
  return numBytes_;
}

// TODO: writeVuLong should ideally be using uint64_t as input.
// but RLE encoder, works only with int64_t and that needs
// to be fixed.
template <bool isSigned>
void IntEncoder<isSigned>::writeVulong(int64_t val) {
  while (true) {
    if ((val & ~0x7f) == 0) {
      writeByte(static_cast<char>(val));
      return;
    } else {
      writeByte(static_cast<char>(0x80 | (val & dwio::common::BASE_128_MASK)));
      // cast val to unsigned so as to force 0-fill right shift
      val = (static_cast<uint64_t>(val) >> 7);
    }
  }
}

template <bool isSigned>
void IntEncoder<isSigned>::writeLongLE(int64_t val) {
  for (int32_t i = 0; i < numBytes_; i++) {
    writeByte(static_cast<char>(val & dwio::common::BASE_256_MASK));
    val >>= 8;
  }
}

} // namespace facebook::velox::dwrf
