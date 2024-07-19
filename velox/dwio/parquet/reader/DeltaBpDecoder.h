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

#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::velox::parquet {

// DeltaBpDecoder is adapted from Apache Arrow:
// https://github.com/apache/arrow/blob/apache-arrow-12.0.0/cpp/src/parquet/encoding.cc#LL2357C18-L2586C3
class DeltaBpDecoder {
 public:
  explicit DeltaBpDecoder(const char* start) : bufferStart_(start) {
    initHeader();
  }

  void skip(uint64_t numValues) {
    skip<false>(numValues, 0, nullptr);
  }

  template <bool hasNulls>
  inline void skip(int32_t numValues, int32_t current, const uint64_t* nulls) {
    if (hasNulls) {
      numValues = bits::countNonNulls(nulls, current, current + numValues);
    }
    for (int32_t i = 0; i < numValues; ++i) {
      readLong();
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
        toSkip = visitor.process(readLong(), atEnd);
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

  const char* bufferStart() {
    return bufferStart_;
  }

  int64_t validValuesCount() {
    return static_cast<int64_t>(totalValuesRemaining_);
  }

 private:
  bool getVlqInt(uint64_t& v) {
    uint64_t tmp = 0;
    for (int i = 0; i < folly::kMaxVarintLength64; i++) {
      uint8_t byte = *(bufferStart_++);
      tmp |= static_cast<uint64_t>(byte & 0x7F) << (7 * i);
      if ((byte & 0x80) == 0) {
        v = tmp;
        return true;
      }
    }
    return false;
  }

  bool getZigZagVlqInt(int64_t& v) {
    uint64_t u;
    if (!getVlqInt(u)) {
      return false;
    }
    v = (u >> 1) ^ (~(u & 1) + 1);
    return true;
  }

  void initHeader() {
    if (!getVlqInt(valuesPerBlock_) || !getVlqInt(miniBlocksPerBlock_) ||
        !getVlqInt(totalValueCount_) || !getZigZagVlqInt(lastValue_)) {
      VELOX_FAIL("initHeader EOF");
    }

    VELOX_CHECK_GT(valuesPerBlock_, 0, "cannot have zero value per block");
    VELOX_CHECK_EQ(
        valuesPerBlock_ % 128,
        0,
        "the number of values in a block must be multiple of 128, but it's " +
            std::to_string(valuesPerBlock_));
    VELOX_CHECK_GT(
        miniBlocksPerBlock_, 0, "cannot have zero miniblock per block");
    valuesPerMiniBlock_ = valuesPerBlock_ / miniBlocksPerBlock_;
    VELOX_CHECK_GT(
        valuesPerMiniBlock_, 0, "cannot have zero value per miniblock");
    VELOX_CHECK_EQ(
        valuesPerMiniBlock_ % 32,
        0,
        "the number of values in a miniblock must be multiple of 32, but it's " +
            std::to_string(valuesPerMiniBlock_));

    totalValuesRemaining_ = totalValueCount_;
    deltaBitWidths_.resize(miniBlocksPerBlock_);
    firstBlockInitialized_ = false;
    valuesRemainingCurrentMiniBlock_ = 0;
  }

  void initBlock() {
    VELOX_DCHECK_GT(totalValuesRemaining_, 0, "initBlock called at EOF");

    if (!getZigZagVlqInt(minDelta_)) {
      VELOX_FAIL("initBlock EOF")
    }

    // read the bitwidth of each miniblock
    for (uint32_t i = 0; i < miniBlocksPerBlock_; ++i) {
      deltaBitWidths_[i] = *(bufferStart_++);
      // Note that non-conformant bitwidth entries are allowed by the Parquet
      // spec for extraneous miniblocks in the last block (GH-14923), so we
      // check the bitwidths when actually using them (see initMiniBlock()).
    }

    miniBlockIdx_ = 0;
    firstBlockInitialized_ = true;
    initMiniBlock(deltaBitWidths_[0]);
  }

  void initMiniBlock(int32_t bitWidth) {
    VELOX_DCHECK_LE(
        bitWidth,
        kMaxDeltaBitWidth,
        "delta bit width larger than integer bit width");
    deltaBitWidth_ = bitWidth;
    valuesRemainingCurrentMiniBlock_ = valuesPerMiniBlock_;
  }

  int64_t readLong() {
    int64_t value = 0;
    if (valuesRemainingCurrentMiniBlock_ == 0) {
      if (!firstBlockInitialized_) {
        value = lastValue_;
        // When block is uninitialized we have two different possibilities:
        // 1. totalValueCount_ == 1, which means that the page may have only
        // one value (encoded in the header), and we should not initialize
        // any block.
        // 2. totalValueCount_ != 1, which means we should initialize the
        // incoming block for subsequent reads.
        if (totalValueCount_ != 1) {
          initBlock();
        }
        totalValuesRemaining_--;
        return value;
      } else {
        ++miniBlockIdx_;
        if (miniBlockIdx_ < miniBlocksPerBlock_) {
          initMiniBlock(deltaBitWidths_[miniBlockIdx_]);
        } else {
          initBlock();
        }
      }
    }

    uint64_t consumedBits =
        (valuesPerMiniBlock_ - valuesRemainingCurrentMiniBlock_) *
        deltaBitWidth_;
    bits::copyBits(
        reinterpret_cast<const uint64_t*>(bufferStart_),
        consumedBits,
        reinterpret_cast<uint64_t*>(&value),
        0,
        deltaBitWidth_);
    // Addition between minDelta_, packed int and lastValue_ should be treated
    // as unsigned addition. Overflow is as expected.
    value = static_cast<uint64_t>(minDelta_) + static_cast<uint64_t>(value) +
        static_cast<uint64_t>(lastValue_);
    lastValue_ = value;
    valuesRemainingCurrentMiniBlock_--;
    totalValuesRemaining_--;

    if (valuesRemainingCurrentMiniBlock_ == 0 || totalValuesRemaining_ == 0) {
      bufferStart_ += bits::nbytes(deltaBitWidth_ * valuesPerMiniBlock_);
    }
    return value;
  }

  static constexpr int kMaxDeltaBitWidth =
      static_cast<int>(sizeof(int64_t) * 8);

  const char* bufferStart_;

  uint64_t valuesPerBlock_;
  uint64_t miniBlocksPerBlock_;
  uint64_t valuesPerMiniBlock_;
  uint64_t totalValueCount_;

  uint64_t totalValuesRemaining_;
  // Remaining values in current mini block. If the current block is the last
  // mini block, valuesRemainingCurrentMiniBlock_ may greater than
  // totalValuesRemaining_.
  uint64_t valuesRemainingCurrentMiniBlock_;

  // If the page doesn't contain any block, `firstBlockInitialized_` will
  // always be false. Otherwise, it will be true when first block initialized.
  bool firstBlockInitialized_;
  int64_t minDelta_;
  uint64_t miniBlockIdx_;
  std::vector<uint8_t> deltaBitWidths_;
  uint64_t deltaBitWidth_;

  int64_t lastValue_;
};

} // namespace facebook::velox::parquet
