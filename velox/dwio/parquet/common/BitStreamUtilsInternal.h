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

// From Apache Impala (incubating) as of 2016-01-29

// Adapted from Apache Arrow.

#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>

#include "velox/common/base/Exceptions.h"

#include "arrow/util/bit_util.h"
#include "arrow/util/bpacking.h"

namespace facebook::velox::parquet {

/// Utility class to write bit/byte streams.  This class can write data to
/// either be bit packed or byte aligned (and a single stream that has a mix of
/// both). This class does not allocate memory.
class BitWriter {
 public:
  /// buffer: buffer to write bits to.  Buffer should be preallocated with
  /// 'bufferLen' bytes.
  BitWriter(uint8_t* buffer, int bufferLen)
      : buffer_(buffer), maxBytes_(bufferLen) {
    Clear();
  }

  void Clear() {
    bufferedValues_ = 0;
    byteOffset_ = 0;
    bitOffset_ = 0;
  }

  /// The number of current bytes written, including the current byte (i.e. may
  /// include a fraction of a byte). Includes buffered values.
  int bytesWritten() const {
    return byteOffset_ +
        static_cast<int>(::arrow::bit_util::BytesForBits(bitOffset_));
  }
  uint8_t* buffer() const {
    return buffer_;
  }
  int bufferLen() const {
    return maxBytes_;
  }

  /// Writes a value to bufferedValues_, flushing to buffer_ if necessary. This
  /// is bit packed.  Returns false if there was not enough space. numBits must
  /// be <= 32.
  bool PutValue(uint64_t v, int numBits);

  /// Writes v to the next aligned byte using numBytes. If T is larger than
  /// numBytes, the extra high-order bytes will be ignored. Returns false if
  /// there was not enough space.
  /// Assume the v is stored in buffer_ as a little-endian format
  template <typename T>
  bool PutAligned(T v, int numBytes);

  /// Write a Vlq encoded int to the buffer.  Returns false if there was not
  /// enough room.  The value is written byte aligned. For more details on vlq:
  /// en.wikipedia.org/wiki/Variable-length_quantity
  bool PutVlqInt(uint32_t v);

  // Writes an int zigzag encoded.
  bool PutZigZagVlqInt(int32_t v);

  /// Write a Vlq encoded int64 to the buffer.  Returns false if there was not
  /// enough room.  The value is written byte aligned. For more details on vlq:
  /// en.wikipedia.org/wiki/Variable-length_quantity
  bool PutVlqInt(uint64_t v);

  // Writes an int64 zigzag encoded.
  bool PutZigZagVlqInt(int64_t v);

  /// Get a pointer to the next aligned byte and advance the underlying buffer
  /// by numBytes.
  /// Returns NULL if there was not enough space.
  uint8_t* GetNextBytePtr(int numBytes = 1);

  /// Flushes all buffered values to the buffer. Call this when done writing to
  /// the buffer.  If 'align' is true, bufferedValues_ is reset and any future
  /// writes will be written to the next byte boundary.
  void Flush(bool align = false);

 private:
  uint8_t* buffer_;
  int maxBytes_;

  /// Bit-packed values are initially written to this variable before being
  /// memcpy'd to buffer_. This is faster than writing values byte by byte
  /// directly to buffer_.
  uint64_t bufferedValues_;

  int byteOffset_; // Offset in buffer_
  int bitOffset_; // Offset in bufferedValues_
};

namespace detail {

inline uint64_t ReadLittleEndianWord(
    const uint8_t* buffer,
    int bytesRemaining) {
  uint64_t leValue = 0;
  if (FOLLY_LIKELY(bytesRemaining >= 8)) {
    memcpy(&leValue, buffer, 8);
  } else {
    memcpy(&leValue, buffer, bytesRemaining);
  }
  return ::arrow::bit_util::FromLittleEndian(leValue);
}

} // namespace detail

/// Utility class to read bit/byte stream.  This class can read bits or bytes
/// that are either byte aligned or not.  It also has utilities to read multiple
/// bytes in one read (e.g. encoded int).
class BitReader {
 public:
  BitReader() = default;

  /// 'buffer' is the buffer to read from.  The buffer's length is 'bufferLen'.
  BitReader(const uint8_t* buffer, int bufferLen) : BitReader() {
    Reset(buffer, bufferLen);
  }

  void Reset(const uint8_t* buffer, int bufferLen) {
    buffer_ = buffer;
    maxBytes_ = bufferLen;
    byteOffset_ = 0;
    bitOffset_ = 0;
    bufferedValues_ = detail::ReadLittleEndianWord(
        buffer_ + byteOffset_, maxBytes_ - byteOffset_);
  }

  /// Gets the next value from the buffer.  Returns true if 'v' could be read or
  /// false if there are not enough bytes left.
  template <typename T>
  bool GetValue(int numBits, T* v);

  /// Get a number of values from the buffer. Return the number of values
  /// actually read.
  template <typename T>
  int GetBatch(int numBits, T* v, int batchSize);

  /// Reads a 'numBytes'-sized value from the buffer and stores it in 'v'. T
  /// needs to be a little-endian native type and big enough to store
  /// 'numBytes'. The value is assumed to be byte-aligned so the stream will
  /// be advanced to the start of the next byte before 'v' is read. Returns
  /// false if there are not enough bytes left.
  /// Assume the v was stored in buffer_ as a little-endian format
  template <typename T>
  bool GetAligned(int numBytes, T* v);

  /// Advances the stream by a number of bits. Returns true if succeed or false
  /// if there are not enough bits left.
  bool Advance(int64_t numBits);

  /// Reads a vlq encoded int from the stream.  The encoded int must start at
  /// the beginning of a byte. Return false if there were not enough bytes in
  /// the buffer.
  bool GetVlqInt(uint32_t* v);

  // Reads a zigzag encoded int `into` v.
  bool GetZigZagVlqInt(int32_t* v);

  /// Reads a vlq encoded int64 from the stream.  The encoded int must start at
  /// the beginning of a byte. Return false if there were not enough bytes in
  /// the buffer.
  bool GetVlqInt(uint64_t* v);

  // Reads a zigzag encoded int64 `into` v.
  bool GetZigZagVlqInt(int64_t* v);

  /// Returns the number of bytes left in the stream, not including the current
  /// byte (i.e., there may be an additional fraction of a byte).
  int bytesLeft() const {
    return maxBytes_ -
        (byteOffset_ +
         static_cast<int>(::arrow::bit_util::BytesForBits(bitOffset_)));
  }

  /// Maximum byte length of a vlq encoded int
  static constexpr int kMaxVlqByteLength = 5;

  /// Maximum byte length of a vlq encoded int64
  static constexpr int kMaxVlqByteLengthForInt64 = 10;

 private:
  const uint8_t* buffer_;
  int maxBytes_;

  /// Bytes are memcpy'd from buffer_ and values are read from this variable.
  /// This is faster than reading values byte by byte directly from buffer_.
  uint64_t bufferedValues_;

  int byteOffset_; // Offset in buffer_
  int bitOffset_; // Offset in bufferedValues_
};

inline bool BitWriter::PutValue(uint64_t v, int numBits) {
  VELOX_DCHECK_LE(numBits, 64);
  if (numBits < 64) {
    VELOX_DCHECK_EQ(v >> numBits, 0, "v = {}, numBits = {}", v, numBits);
  }

  if (FOLLY_UNLIKELY(byteOffset_ * 8 + bitOffset_ + numBits > maxBytes_ * 8))
    return false;

  bufferedValues_ |= v << bitOffset_;
  bitOffset_ += numBits;

  if (FOLLY_UNLIKELY(bitOffset_ >= 64)) {
    // Flush bufferedValues_ and write out bits of v that did not fit
    bufferedValues_ = folly::Endian::little(bufferedValues_);
    memcpy(buffer_ + byteOffset_, &bufferedValues_, 8);
    bufferedValues_ = 0;
    byteOffset_ += 8;
    bitOffset_ -= 64;
    bufferedValues_ =
        (numBits - bitOffset_ == 64) ? 0 : (v >> (numBits - bitOffset_));
  }
  VELOX_DCHECK_LT(bitOffset_, 64);
  return true;
}

inline void BitWriter::Flush(bool align) {
  int numBytes = static_cast<int>(::arrow::bit_util::BytesForBits(bitOffset_));
  VELOX_DCHECK_LE(byteOffset_ + numBytes, maxBytes_);
  auto bufferedValues = folly::Endian::little(bufferedValues_);
  memcpy(buffer_ + byteOffset_, &bufferedValues, numBytes);

  if (align) {
    bufferedValues_ = 0;
    byteOffset_ += numBytes;
    bitOffset_ = 0;
  }
}

inline uint8_t* BitWriter::GetNextBytePtr(int numBytes) {
  Flush(/* align */ true);
  VELOX_DCHECK_LE(byteOffset_, maxBytes_);
  if (byteOffset_ + numBytes > maxBytes_)
    return NULL;
  uint8_t* ptr = buffer_ + byteOffset_;
  byteOffset_ += numBytes;
  return ptr;
}

template <typename T>
inline bool BitWriter::PutAligned(T val, int numBytes) {
  uint8_t* ptr = GetNextBytePtr(numBytes);
  if (ptr == NULL)
    return false;
  val = folly::Endian::little(val);
  memcpy(ptr, &val, numBytes);
  return true;
}

namespace detail {

template <typename T>
inline void GetValue_(
    int numBits,
    T* v,
    int maxBytes,
    const uint8_t* buffer,
    int* bitOffset,
    int* byteOffset,
    uint64_t* bufferedValues) {
  *v = static_cast<T>(
      ::arrow::bit_util::TrailingBits(*bufferedValues, *bitOffset + numBits) >>
      *bitOffset);
  *bitOffset += numBits;
  if (*bitOffset >= 64) {
    *byteOffset += 8;
    *bitOffset -= 64;

    *bufferedValues = detail::ReadLittleEndianWord(
        buffer + *byteOffset, maxBytes - *byteOffset);
    // Read bits of v that crossed into new bufferedValues_
    if (FOLLY_LIKELY(numBits - *bitOffset < static_cast<int>(8 * sizeof(T)))) {
      // if shift exponent(numBits - *bitOffset) is not less than sizeof(T),
      // *v will not change and the following code may cause a runtime error
      // that the shift exponent is too large
      *v = *v |
          static_cast<T>(
              ::arrow::bit_util::TrailingBits(*bufferedValues, *bitOffset)
              << (numBits - *bitOffset));
    }
    VELOX_DCHECK_LE(*bitOffset, 64);
  }
}

} // namespace detail

template <typename T>
inline bool BitReader::GetValue(int numBits, T* v) {
  return GetBatch(numBits, v, 1) == 1;
}

template <typename T>
inline int BitReader::GetBatch(int numBits, T* v, int batchSize) {
  VELOX_DCHECK(buffer_ != NULL);
  VELOX_DCHECK_LE(
      numBits, static_cast<int>(sizeof(T) * 8), "numBits: {}", numBits);

  int bitOffset = bitOffset_;
  int byteOffset = byteOffset_;
  uint64_t bufferedValues = bufferedValues_;
  int maxBytes = maxBytes_;
  const uint8_t* buffer = buffer_;

  const int64_t neededBits = numBits * static_cast<int64_t>(batchSize);
  constexpr uint64_t kBitsPerByte = 8;
  const int64_t remainingBits =
      static_cast<int64_t>(maxBytes - byteOffset) * kBitsPerByte - bitOffset;
  if (remainingBits < neededBits) {
    batchSize = static_cast<int>(remainingBits / numBits);
  }

  int i = 0;
  if (FOLLY_UNLIKELY(bitOffset != 0)) {
    for (; i < batchSize && bitOffset != 0; ++i) {
      detail::GetValue_(
          numBits,
          &v[i],
          maxBytes,
          buffer,
          &bitOffset,
          &byteOffset,
          &bufferedValues);
    }
  }

  if (sizeof(T) == 4) {
    int numUnpacked = ::arrow::internal::unpack32(
        reinterpret_cast<const uint32_t*>(buffer + byteOffset),
        reinterpret_cast<uint32_t*>(v + i),
        batchSize - i,
        numBits);
    i += numUnpacked;
    byteOffset += numUnpacked * numBits / 8;
  } else if (sizeof(T) == 8 && numBits > 32) {
    // Use unpack64 only if numBits is larger than 32
    // TODO (ARROW-13677): improve the performance of internal::unpack64
    // and remove the restriction of numBits
    int numUnpacked = ::arrow::internal::unpack64(
        buffer + byteOffset,
        reinterpret_cast<uint64_t*>(v + i),
        batchSize - i,
        numBits);
    i += numUnpacked;
    byteOffset += numUnpacked * numBits / 8;
  } else {
    // TODO: revisit this limit if necessary
    VELOX_DCHECK_LE(numBits, 32);
    const int bufferSize = 1024;
    uint32_t unpackBuffer[bufferSize];
    while (i < batchSize) {
      int unpack_size = std::min(bufferSize, batchSize - i);
      int numUnpacked = ::arrow::internal::unpack32(
          reinterpret_cast<const uint32_t*>(buffer + byteOffset),
          unpackBuffer,
          unpack_size,
          numBits);
      if (numUnpacked == 0) {
        break;
      }
      for (int k = 0; k < numUnpacked; ++k) {
        v[i + k] = static_cast<T>(unpackBuffer[k]);
      }
      i += numUnpacked;
      byteOffset += numUnpacked * numBits / 8;
    }
  }

  bufferedValues =
      detail::ReadLittleEndianWord(buffer + byteOffset, maxBytes - byteOffset);

  for (; i < batchSize; ++i) {
    detail::GetValue_(
        numBits,
        &v[i],
        maxBytes,
        buffer,
        &bitOffset,
        &byteOffset,
        &bufferedValues);
  }

  bitOffset_ = bitOffset;
  byteOffset_ = byteOffset;
  bufferedValues_ = bufferedValues;

  return batchSize;
}

template <typename T>
inline bool BitReader::GetAligned(int numBytes, T* v) {
  if (FOLLY_UNLIKELY(numBytes > static_cast<int>(sizeof(T)))) {
    return false;
  }

  int bytesRead = static_cast<int>(::arrow::bit_util::BytesForBits(bitOffset_));
  if (FOLLY_UNLIKELY(byteOffset_ + bytesRead + numBytes > maxBytes_)) {
    return false;
  }

  // Advance byteOffset to next unread byte and read numBytes
  byteOffset_ += bytesRead;
  if constexpr (std::is_same_v<T, bool>) {
    // ARROW-18031: if we're trying to get an aligned bool, just check
    // the LSB of the next byte and move on. If we memcpy + FromLittleEndian
    // as usual, we have potential undefined behavior for bools if the value
    // isn't 0 or 1
    *v = *(buffer_ + byteOffset_) & 1;
  } else {
    memcpy(v, buffer_ + byteOffset_, numBytes);
    *v = ::arrow::bit_util::FromLittleEndian(*v);
  }
  byteOffset_ += numBytes;

  bitOffset_ = 0;
  bufferedValues_ = detail::ReadLittleEndianWord(
      buffer_ + byteOffset_, maxBytes_ - byteOffset_);
  return true;
}

inline bool BitReader::Advance(int64_t numBits) {
  int64_t bitsRequired = bitOffset_ + numBits;
  int64_t bytesRequired = ::arrow::bit_util::BytesForBits(bitsRequired);
  if (FOLLY_UNLIKELY(bytesRequired > maxBytes_ - byteOffset_)) {
    return false;
  }
  byteOffset_ += static_cast<int>(bitsRequired >> 3);
  bitOffset_ = static_cast<int>(bitsRequired & 7);
  bufferedValues_ = detail::ReadLittleEndianWord(
      buffer_ + byteOffset_, maxBytes_ - byteOffset_);
  return true;
}

inline bool BitWriter::PutVlqInt(uint32_t v) {
  bool result = true;
  while ((v & 0xFFFFFF80UL) != 0UL) {
    result &= PutAligned<uint8_t>(static_cast<uint8_t>((v & 0x7F) | 0x80), 1);
    v >>= 7;
  }
  result &= PutAligned<uint8_t>(static_cast<uint8_t>(v & 0x7F), 1);
  return result;
}

inline bool BitReader::GetVlqInt(uint32_t* v) {
  uint32_t tmp = 0;

  for (int i = 0; i < kMaxVlqByteLength; i++) {
    uint8_t byte = 0;
    if (FOLLY_UNLIKELY(!GetAligned<uint8_t>(1, &byte))) {
      return false;
    }
    tmp |= static_cast<uint32_t>(byte & 0x7F) << (7 * i);

    if ((byte & 0x80) == 0) {
      *v = tmp;
      return true;
    }
  }

  return false;
}

inline bool BitWriter::PutZigZagVlqInt(int32_t v) {
  uint32_t copyValue = ::arrow::util::SafeCopy<uint32_t>(v);
  copyValue = (copyValue << 1) ^ static_cast<uint32_t>(v >> 31);
  return PutVlqInt(copyValue);
}

inline bool BitReader::GetZigZagVlqInt(int32_t* v) {
  uint32_t u;
  if (!GetVlqInt(&u))
    return false;
  u = (u >> 1) ^ (~(u & 1) + 1);
  *v = ::arrow::util::SafeCopy<int32_t>(u);
  return true;
}

inline bool BitWriter::PutVlqInt(uint64_t v) {
  bool result = true;
  while ((v & 0xFFFFFFFFFFFFFF80ULL) != 0ULL) {
    result &= PutAligned<uint8_t>(static_cast<uint8_t>((v & 0x7F) | 0x80), 1);
    v >>= 7;
  }
  result &= PutAligned<uint8_t>(static_cast<uint8_t>(v & 0x7F), 1);
  return result;
}

inline bool BitReader::GetVlqInt(uint64_t* v) {
  uint64_t tmp = 0;

  for (int i = 0; i < kMaxVlqByteLengthForInt64; i++) {
    uint8_t byte = 0;
    if (FOLLY_UNLIKELY(!GetAligned<uint8_t>(1, &byte))) {
      return false;
    }
    tmp |= static_cast<uint64_t>(byte & 0x7F) << (7 * i);

    if ((byte & 0x80) == 0) {
      *v = tmp;
      return true;
    }
  }

  return false;
}

inline bool BitWriter::PutZigZagVlqInt(int64_t v) {
  uint64_t copyValue = ::arrow::util::SafeCopy<uint64_t>(v);
  copyValue = (copyValue << 1) ^ static_cast<uint64_t>(v >> 63);
  return PutVlqInt(copyValue);
}

inline bool BitReader::GetZigZagVlqInt(int64_t* v) {
  uint64_t u;
  if (!GetVlqInt(&u))
    return false;
  u = (u >> 1) ^ (~(u & 1) + 1);
  *v = ::arrow::util::SafeCopy<int64_t>(u);
  return true;
}

} // namespace facebook::velox::parquet
