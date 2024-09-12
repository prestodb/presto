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

#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/Adaptor.h"
#include "velox/dwio/common/DataBuffer.h"
#include "velox/dwio/common/IntDecoder.h"
#include "velox/dwio/common/exception/Exception.h"

#include <vector>

namespace facebook::velox::dwrf {

template <bool isSigned>
class RleDecoderV2 : public dwio::common::IntDecoder<isSigned> {
 public:
  enum EncodingType {
    SHORT_REPEAT = 0,
    DIRECT = 1,
    PATCHED_BASE = 2,
    DELTA = 3
  };

  RleDecoderV2(
      std::unique_ptr<dwio::common::SeekableInputStream> input,
      memory::MemoryPool& pool);

  /**
   * Seek to a specific row group.
   */
  void seekToRowGroup(dwio::common::PositionProvider&) override;

  void skipPending() override;

  /**
   * Read a number of values into the batch.
   */
  void next(int64_t* data, uint64_t numValues, const uint64_t* nulls) override;

  void nextLengths(int32_t* const data, const int32_t numValues) override {
    skipPending();
    for (int i = 0; i < numValues; ++i) {
      data[i] = readValue();
    }
  }

  template <bool hasNulls, typename Visitor>
  void readWithVisitor(const uint64_t* nulls, Visitor visitor) {
    skipPending();
    int32_t current = visitor.start();
    this->template skip<hasNulls>(current, 0, nulls);

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
            this->template skip<false>(toSkip, current, nullptr);
          }
          if (atEnd) {
            return;
          }
        }

        // We are at a non-null value on a row to visit.
        auto value = readValue();
        toSkip = visitor.process(value, atEnd);
      }

      ++current;
      if (toSkip) {
        this->template skip<hasNulls>(toSkip, current, nulls);
        current += toSkip;
      }
      if (atEnd) {
        return;
      }
    }
  }

 private:
  // Used by PATCHED_BASE
  void adjustGapAndPatch() {
    curGap_ = static_cast<uint64_t>(unpackedPatch_[patchIdx_]) >> patchBitSize_;
    curPatch_ = unpackedPatch_[patchIdx_] & patchMask_;
    actualGap_ = 0;

    // special case: gap is >255 then patch value will be 0.
    // if gap is <=255 then patch value cannot be 0
    while (curGap_ == 255 && curPatch_ == 0) {
      actualGap_ += 255;
      ++patchIdx_;
      curGap_ =
          static_cast<uint64_t>(unpackedPatch_[patchIdx_]) >> patchBitSize_;
      curPatch_ = unpackedPatch_[patchIdx_] & patchMask_;
    }
    // add the left over gap
    actualGap_ += curGap_;
  }

  void resetReadLongs() {
    bitsLeft_ = 0;
    curByte_ = 0;
  }

  void resetRun() {
    resetReadLongs();
    bitSize_ = 0;
    firstByte_ = readByte();
    type_ = static_cast<EncodingType>((firstByte_ >> 6) & 0x03);
  }

  unsigned char readByte() {
    if (dwio::common::IntDecoder<isSigned>::bufferStart_ ==
        dwio::common::IntDecoder<isSigned>::bufferEnd_) {
      int32_t bufferLength;
      const void* bufferPointer;
      const bool ret = dwio::common::IntDecoder<isSigned>::inputStream_->Next(
          &bufferPointer, &bufferLength);
      VELOX_CHECK(
          ret,
          "bad read in RleDecoderV2::readByte, ",
          dwio::common::IntDecoder<isSigned>::inputStream_->getName());
      dwio::common::IntDecoder<isSigned>::bufferStart_ =
          static_cast<const char*>(bufferPointer);
      dwio::common::IntDecoder<isSigned>::bufferEnd_ =
          dwio::common::IntDecoder<isSigned>::bufferStart_ + bufferLength;
    }

    unsigned char result = static_cast<unsigned char>(
        *dwio::common::IntDecoder<isSigned>::bufferStart_++);
    return result;
  }

  int64_t readLongBE(uint64_t bsz);
  uint64_t readLongs(
      int64_t* data,
      uint64_t offset,
      uint64_t len,
      uint64_t fb,
      const uint64_t* nulls = nullptr) {
    uint64_t ret = 0;

    // TODO: unroll to improve performance
    for (uint64_t i = offset; i < (offset + len); i++) {
      // skip null positions
      if (nulls && bits::isBitNull(nulls, i)) {
        continue;
      }
      uint64_t result = 0;
      uint64_t bitsLeftToRead = fb;
      while (bitsLeftToRead > bitsLeft_) {
        result <<= bitsLeft_;
        result |= curByte_ & ((1 << bitsLeft_) - 1);
        bitsLeftToRead -= bitsLeft_;
        curByte_ = readByte();
        bitsLeft_ = 8;
      }

      // handle the left over bits
      if (bitsLeftToRead > 0) {
        result <<= bitsLeftToRead;
        bitsLeft_ -= static_cast<uint32_t>(bitsLeftToRead);
        result |= (curByte_ >> bitsLeft_) & ((1 << bitsLeftToRead) - 1);
      }
      data[i] = static_cast<int64_t>(result);
      ++ret;
    }

    return ret;
  }

  uint64_t nextShortRepeats(
      int64_t* data,
      uint64_t offset,
      uint64_t numValues,
      const uint64_t* nulls);
  uint64_t nextDirect(
      int64_t* data,
      uint64_t offset,
      uint64_t numValues,
      const uint64_t* nulls);
  uint64_t nextPatched(
      int64_t* data,
      uint64_t offset,
      uint64_t numValues,
      const uint64_t* nulls);
  uint64_t nextDelta(
      int64_t* data,
      uint64_t offset,
      uint64_t numValues,
      const uint64_t* nulls);

  int64_t readValue();

  void doNext(
      int64_t* const data,
      const uint64_t numValues,
      const uint64_t* const nulls);

  unsigned char firstByte_;
  uint64_t runLength_;
  uint64_t runRead_;
  int64_t deltaBase_; // Used by DELTA
  uint64_t byteSize_; // Used by SHORT_REPEAT and PATCHED_BASE
  int64_t firstValue_; // Used by SHORT_REPEAT and DELTA
  int64_t prevValue_; // Used by DELTA
  uint32_t bitSize_; // Used by DIRECT, PATCHED_BASE and DELTA
  uint32_t bitsLeft_; // Used by anything that uses readLongs
  uint32_t curByte_; // Used by anything that uses readLongs
  uint32_t patchBitSize_; // Used by PATCHED_BASE
  uint64_t unpackedIdx_; // Used by PATCHED_BASE
  uint64_t patchIdx_; // Used by PATCHED_BASE
  int64_t base_; // Used by PATCHED_BASE
  uint64_t curGap_; // Used by PATCHED_BASE
  int64_t curPatch_; // Used by PATCHED_BASE
  int64_t patchMask_; // Used by PATCHED_BASE
  int64_t actualGap_; // Used by PATCHED_BASE
  EncodingType type_;
  dwio::common::DataBuffer<int64_t> unpacked_; // Used by PATCHED_BASE
  dwio::common::DataBuffer<int64_t> unpackedPatch_; // Used by PATCHED_BASE
};

} // namespace facebook::velox::dwrf
