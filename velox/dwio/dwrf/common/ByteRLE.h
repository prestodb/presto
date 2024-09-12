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

#include <glog/logging.h>
#include <memory>
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Nulls.h"
#include "velox/dwio/common/IntCodecCommon.h"
#include "velox/dwio/common/OutputStream.h"
#include "velox/dwio/common/Range.h"
#include "velox/dwio/common/SeekableInputStream.h"
#include "velox/dwio/dwrf/common/Common.h"
#include "velox/dwio/dwrf/common/wrap/dwrf-proto-wrapper.h"
#include "velox/vector/TypeAliases.h"

namespace facebook::velox::dwrf {

using dwio::common::BufferedOutputStream;
using dwio::common::PositionRecorder;

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
      : inputStream_{std::move(input)},
        encodingKey_{ek},
        remainingValues_{0},
        value_{0},
        bufferStart_{nullptr},
        bufferEnd_{nullptr},
        repeating_{false} {}

  virtual ~ByteRleDecoder() = default;

  /**
   * Seek to a specific row group.  Should not read the underlying input stream
   * to avoid decoding same data multiple times.
   */
  virtual void seekToRowGroup(dwio::common::PositionProvider& positionProvider);

  /**
   * Seek over a given number of values.  Does not decode the underlying input
   * stream.
   */
  void skip(uint64_t numValues) {
    pendingSkip_ += numValues;
  }

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
    return inputStream_->positionSize() + startIndex + 1;
  }

  void skipBytes(size_t bytes);

  template <bool hasNulls, typename Visitor>
  void readWithVisitor(const uint64_t* nulls, Visitor visitor) {
    skipPending();
    int32_t current = visitor.start();
    skip<hasNulls>(current, 0, nulls);
    int32_t toSkip{0};
    bool atEnd{false};
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
        if (!remainingValues_) {
          readHeader();
        }
        if (repeating_) {
          toSkip = visitor.process(value_, atEnd);
        } else {
          value_ = readByte();
          toSkip = visitor.process(value_, atEnd);
        }
        --remainingValues_;
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
    if (bufferStart_ == bufferEnd_) {
      nextBuffer();
    }
    return *(bufferStart_++);
  }

  inline void readHeader() {
    const signed char ch = readByte();
    if (ch < 0) {
      remainingValues_ = static_cast<size_t>(-ch);
      repeating_ = false;
    } else {
      remainingValues_ = static_cast<size_t>(ch) + RLE_MINIMUM_REPEAT;
      repeating_ = true;
      value_ = readByte();
    }
  }

  virtual void skipPending() {
    auto numValues = pendingSkip_;
    pendingSkip_ = 0;
    while (numValues > 0) {
      if (remainingValues_ == 0) {
        readHeader();
      }
      const auto count = std::min<int64_t>(numValues, remainingValues_);
      remainingValues_ -= count;
      numValues -= count;
      if (!repeating_) {
        skipBytes(count);
      }
    }
  }

  const std::unique_ptr<dwio::common::SeekableInputStream> inputStream_;
  const EncodingKey encodingKey_;
  size_t remainingValues_;
  char value_;
  const char* bufferStart_;
  const char* bufferEnd_;
  bool repeating_;
  int64_t pendingSkip_{0};

 private:
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
    const EncodingKey& encodingKey);

class BooleanRleDecoder : public ByteRleDecoder {
 public:
  BooleanRleDecoder(
      std::unique_ptr<dwio::common::SeekableInputStream> input,
      const EncodingKey& encodingKey)
      : ByteRleDecoder{std::move(input), encodingKey},
        remainingBits_{0},
        reversedLastByte_{0} {}

  ~BooleanRleDecoder() override = default;

  void seekToRowGroup(
      dwio::common::PositionProvider& positionProvider) override;

  void skip(uint64_t numValues) {
    pendingSkip_ += numValues;
  }

  void next(char* data, uint64_t numValues, const uint64_t* nulls) override;

  size_t loadIndices(size_t startIndex) override {
    return ByteRleDecoder::loadIndices(startIndex) + 1;
  }

  template <bool hasNulls, typename Visitor>
  void readWithVisitor(const uint64_t* nulls, Visitor visitor) {
    skipPending();
    int32_t end = visitor.rowAt(visitor.numRows() - 1) + 1;
    int32_t totalNulls = 0;
    // Reads all the non-null bits between 0 and last row into 'bits',
    // then applies visitors. This is more streamlined and less
    // error-prone than directly dealing with the encoding.
    if (hasNulls) {
      totalNulls = bits::countNulls(nulls, 0, end);
    }
    bits_.resize(bits::nwords(end - totalNulls));
    if (end > totalNulls) {
      next(reinterpret_cast<char*>(bits_.data()), end - totalNulls, nullptr);
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
          visitor.process(bits::isBitSet(bits_.data(), dataPosition), atEnd);
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

 private:
  // Advances 'dataPosition' by 'numValue' non-nulls, where 'current'
  // is the position in 'nulls'.
  template <bool hasNulls>
  static void skip(
      int32_t numValues,
      int32_t current,
      const uint64_t* nulls,
      int32_t& dataPosition) {
    if (hasNulls) {
      numValues = bits::countNonNulls(nulls, current, current + numValues);
    }
    dataPosition += numValues;
  }

  void skipPending() override;

 protected:
  size_t remainingBits_;
  uint8_t reversedLastByte_;
  char buffer_;
  std::vector<uint64_t> bits_;
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
