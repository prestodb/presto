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

#include <memory>
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Nulls.h"
#include "velox/dwio/common/IntCodecCommon.h"
#include "velox/dwio/common/Range.h"
#include "velox/dwio/common/SeekableInputStream.h"
#include "velox/dwio/dwrf/common/Common.h"
#include "velox/dwio/dwrf/common/OutputStream.h"
#include "velox/dwio/dwrf/common/wrap/dwrf-proto-wrapper.h"
#include "velox/vector/TypeAliases.h"

namespace facebook::velox::dwrf {

class ByteRleEncoder {
 public:
  virtual ~ByteRleEncoder() = default;

  /**
   * Encode the next batch of values
   * @param data to be encoded
   * @param ranges positions to be encoded
   * @param nulls If the pointer is null, all values are added. If the
   *    pointer is not null, positions that are true are skipped.
   */
  virtual uint64_t add(
      const char* data,
      const common::Ranges& ranges,
      const uint64_t* nulls) = 0;

  virtual uint64_t add(
      const std::function<char(vector_size_t)>& valueAt,
      const common::Ranges& ranges,
      const std::function<bool(vector_size_t)>& isNullAt) = 0;

  /**
   * Encode the next batch of values
   * @param data to be encoded as a bit mask
   * @param ranges positions to be encoded
   * @param nulls If the pointer is null, all values are added. If the
   *    pointer is not null, positions that are true are skipped.
   * @param invert If true, adds inverse of the bit.
   */
  virtual uint64_t addBits(
      const uint64_t* data,
      const common::Ranges& ranges,
      const uint64_t* nulls,
      bool invert) = 0;

  virtual uint64_t addBits(
      const std::function<bool(vector_size_t)>& valueAt,
      const common::Ranges& ranges,
      const std::function<bool(vector_size_t)>& isNullAt,
      bool invert) = 0;

  /**
   * Get size of buffer used so far.
   */
  virtual uint64_t getBufferSize() const = 0;

  /**
   * Flushing underlying output stream
   */
  virtual uint64_t flush() = 0;

  /**
   * record current position for a specific stride, negative stride index
   * signals to append to the latest stride.
   * @param recorder use the recorder to record current positions to backfill
   *                 the index entry for a specific stride.
   * @param strideIndex the index of the stride to backfill
   */
  virtual void recordPosition(
      PositionRecorder& recorder,
      int32_t strideIndex = -1) const = 0;
};

class ByteRleDecoder {
 public:
  ByteRleDecoder(
      std::unique_ptr<dwio::common::SeekableInputStream> input,
      EncodingKey ek)
      : inputStream{std::move(input)},
        remainingValues{0},
        value{0},
        bufferStart{nullptr},
        bufferEnd{nullptr},
        repeating{false},
        encodingKey_{ek} {}

  virtual ~ByteRleDecoder() = default;

  /**
   * Seek to a specific row group.
   */
  virtual void seekToRowGroup(dwio::common::PositionProvider& positionProvider);

  /**
   * Seek over a given number of values.
   */
  virtual void skip(uint64_t numValues);

  /**
   * Read a number of values into the batch.
   * @param data the array to read into
   * @param numValues the number of values to read
   * @param nulls If the pointer is null, all values are read. If the
   *    pointer is not null, positions that are true are skipped.
   */
  virtual void next(char* data, uint64_t numValues, const uint64_t* nulls);

  /**
   * Load the RowIndex values for the stream this is reading.
   */
  virtual size_t loadIndices(size_t startIndex) {
    return inputStream->positionSize() + startIndex + 1;
  }

  void skipBytes(size_t bytes);

  template <bool hasNulls>
  inline void skip(int32_t numValues, int32_t current, const uint64_t* nulls) {
    if (hasNulls) {
      numValues = bits::countNonNulls(nulls, current, current + numValues);
    }
    while (numValues > 0) {
      if (remainingValues == 0) {
        readHeader();
      }
      uint64_t count = std::min<int32_t>(numValues, remainingValues);
      remainingValues -= count;
      numValues -= count;
      if (!repeating) {
        skipBytes(count);
      }
    }
  }

  template <bool hasNulls, typename Visitor>
  void readWithVisitor(const uint64_t* nulls, Visitor visitor) {
    int32_t current = visitor.start();
    skip<hasNulls>(current, 0, nulls);
    int32_t toSkip;
    bool atEnd = false;
    const bool allowNulls = hasNulls && visitor.allowNulls();
    for (;;) {
      if (hasNulls && allowNulls && bits::isBitNull(nulls, current)) {
        toSkip = visitor.processNull(atEnd);
      } else {
        if (hasNulls && !allowNulls) {
          toSkip = visitor.checkAndSkipNulls(nulls, current, atEnd);
          if (!Visitor::dense) {
            skip<false>(toSkip, current, nullptr);
          }
          if (atEnd) {
            return;
          }
        }
        // We are at a non-null value on a row to visit.
        if (!remainingValues) {
          readHeader();
        }
        if (repeating) {
          toSkip = visitor.process(value, atEnd);
        } else {
          value = readByte();
          toSkip = visitor.process(value, atEnd);
        }
        --remainingValues;
      }
      ++current;
      if (toSkip) {
        skip<hasNulls>(toSkip, current, nulls);
        current += toSkip;
      }
      if (atEnd) {
        return;
      }
    }
  }

 protected:
  void nextBuffer();

  inline signed char readByte() {
    if (bufferStart == bufferEnd) {
      nextBuffer();
    }
    return *(bufferStart++);
  }

  inline void readHeader() {
    signed char ch = readByte();
    if (ch < 0) {
      remainingValues = static_cast<size_t>(-ch);
      repeating = false;
    } else {
      remainingValues = static_cast<size_t>(ch) + RLE_MINIMUM_REPEAT;
      repeating = true;
      value = readByte();
    }
  }

  std::unique_ptr<dwio::common::SeekableInputStream> inputStream;
  size_t remainingValues;
  char value;
  const char* bufferStart;
  const char* bufferEnd;
  bool repeating;
  EncodingKey encodingKey_;
};

/**
 * Create a byte RLE encoder.
 * @param output the output stream to write to
 */
std::unique_ptr<ByteRleEncoder> createByteRleEncoder(
    std::unique_ptr<BufferedOutputStream> output);

/**
 * Create a boolean RLE encoder.
 * @param output the output stream to write to
 */
std::unique_ptr<ByteRleEncoder> createBooleanRleEncoder(
    std::unique_ptr<BufferedOutputStream> output);

/**
 * Create a byte RLE decoder.
 * @param input the input stream to read from
 */
std::unique_ptr<ByteRleDecoder> createByteRleDecoder(
    std::unique_ptr<dwio::common::SeekableInputStream> input,
    const EncodingKey& ek);

class BooleanRleDecoder : public ByteRleDecoder {
 public:
  BooleanRleDecoder(
      std::unique_ptr<dwio::common::SeekableInputStream> input,
      const EncodingKey& ek)
      : ByteRleDecoder{std::move(input), ek},
        remainingBits{0},
        reversedLastByte{0} {}

  ~BooleanRleDecoder() override = default;

  void seekToRowGroup(
      dwio::common::PositionProvider& positionProvider) override;

  void skip(uint64_t numValues) override;

  void next(char* data, uint64_t numValues, const uint64_t* nulls) override;

  size_t loadIndices(size_t startIndex) override {
    return ByteRleDecoder::loadIndices(startIndex) + 1;
  }

  // Advances 'dataPosition' by 'numValue' non-nulls, where 'current'
  // is the position in 'nulls'.
  template <bool hasNulls>
  void skip(
      int32_t numValues,
      int32_t current,
      const uint64_t* nulls,
      int32_t& dataPosition) {
    if (hasNulls) {
      numValues = bits::countNonNulls(nulls, current, current + numValues);
    }
    dataPosition += numValues;
  }

  template <bool hasNulls, typename Visitor>
  void readWithVisitor(const uint64_t* nulls, Visitor visitor) {
    int32_t end = visitor.rowAt(visitor.numRows() - 1) + 1;
    int32_t totalNulls = 0;
    // Reads all the non-null bits between 0 and last row into 'bits',
    // then applies visitors. This is more streamlined and less
    // error-prone than directly dealing with the encoding.
    if (hasNulls) {
      totalNulls = bits::countNulls(nulls, 0, end);
    }
    bits.resize(bits::nwords(end - totalNulls));
    if (end > totalNulls) {
      next(reinterpret_cast<char*>(bits.data()), end - totalNulls, nullptr);
    }
    int32_t dataPosition = 0;
    int32_t current = visitor.start();
    skip<hasNulls>(current, 0, nulls, dataPosition);
    bool atEnd = false;
    const bool allowNulls = hasNulls && visitor.allowNulls();
    for (;;) {
      int32_t toSkip;
      if (hasNulls) {
        if (!allowNulls) {
          toSkip = visitor.checkAndSkipNulls(nulls, current, atEnd);
          if (!Visitor::dense) {
            skip<false>(toSkip, current, nullptr, dataPosition);
          }
          if (atEnd) {
            return;
          }
        } else {
          if (bits::isBitNull(nulls, current)) {
            toSkip = visitor.processNull(atEnd);
            goto skip;
          }
        }
      }
      toSkip =
          visitor.process(bits::isBitSet(bits.data(), dataPosition), atEnd);
      ++dataPosition;
    skip:
      ++current;
      if (toSkip) {
        skip<hasNulls>(toSkip, current, nulls, dataPosition);
        current += toSkip;
      }
      if (atEnd) {
        return;
      }
    }
  }

 protected:
  size_t remainingBits;
  uint8_t reversedLastByte;
  char buffer;
  std::vector<uint64_t> bits;
};

/**
 * Create a boolean RLE decoder.
 *
 * Unlike the other RLE decoders, the boolean decoder sets the data to 0
 * if the value is masked by notNull. This is required for the notNull stream
 * processing to properly apply multiple masks from nested types.
 * @param input the input stream to read from
 */
std::unique_ptr<BooleanRleDecoder> createBooleanRleDecoder(
    std::unique_ptr<dwio::common::SeekableInputStream> input,
    const EncodingKey& ek);

} // namespace facebook::velox::dwrf
