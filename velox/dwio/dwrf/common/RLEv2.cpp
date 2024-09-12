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

#include "velox/dwio/dwrf/common/RLEv2.h"
#include "velox/dwio/common/SeekableInputStream.h"
#include "velox/dwio/dwrf/common/Common.h"

namespace facebook::velox::dwrf {

using memory::MemoryPool;

struct FixedBitSizes {
  enum FBS {
    ONE = 0,
    TWO,
    THREE,
    FOUR,
    FIVE,
    SIX,
    SEVEN,
    EIGHT,
    NINE,
    TEN,
    ELEVEN,
    TWELVE,
    THIRTEEN,
    FOURTEEN,
    FIFTEEN,
    SIXTEEN,
    SEVENTEEN,
    EIGHTEEN,
    NINETEEN,
    TWENTY,
    TWENTYONE,
    TWENTYTWO,
    TWENTYTHREE,
    TWENTYFOUR,
    TWENTYSIX,
    TWENTYEIGHT,
    THIRTY,
    THIRTYTWO,
    FORTY,
    FORTYEIGHT,
    FIFTYSIX,
    SIXTYFOUR
  };
};

inline uint32_t decodeBitWidth(uint32_t n) {
  if (n <= FixedBitSizes::TWENTYFOUR) {
    return n + 1;
  } else if (n == FixedBitSizes::TWENTYSIX) {
    return 26;
  } else if (n == FixedBitSizes::TWENTYEIGHT) {
    return 28;
  } else if (n == FixedBitSizes::THIRTY) {
    return 30;
  } else if (n == FixedBitSizes::THIRTYTWO) {
    return 32;
  } else if (n == FixedBitSizes::FORTY) {
    return 40;
  } else if (n == FixedBitSizes::FORTYEIGHT) {
    return 48;
  } else if (n == FixedBitSizes::FIFTYSIX) {
    return 56;
  } else {
    return 64;
  }
}

inline uint32_t getClosestFixedBits(uint32_t n) {
  if (n == 0) {
    return 1;
  }

  if (n >= 1 && n <= 24) {
    return n;
  } else if (n > 24 && n <= 26) {
    return 26;
  } else if (n > 26 && n <= 28) {
    return 28;
  } else if (n > 28 && n <= 30) {
    return 30;
  } else if (n > 30 && n <= 32) {
    return 32;
  } else if (n > 32 && n <= 40) {
    return 40;
  } else if (n > 40 && n <= 48) {
    return 48;
  } else if (n > 48 && n <= 56) {
    return 56;
  } else {
    return 64;
  }
}

template <bool isSigned>
int64_t RleDecoderV2<isSigned>::readLongBE(uint64_t bsz) {
  int64_t ret = 0, val;
  uint64_t n = bsz;
  while (n > 0) {
    --n;
    val = readByte();
    ret |= (val << (n * 8));
  }
  return ret;
}

template <bool isSigned>
RleDecoderV2<isSigned>::RleDecoderV2(
    std::unique_ptr<dwio::common::SeekableInputStream> input,
    MemoryPool& pool)
    : dwio::common::IntDecoder<isSigned>{std::move(input), false, 0},
      firstByte_(0),
      runLength_(0),
      runRead_(0),
      deltaBase_(0),
      byteSize_(0),
      firstValue_(0),
      prevValue_(0),
      bitSize_(0),
      bitsLeft_(0),
      curByte_(0),
      patchBitSize_(0),
      unpackedIdx_(0),
      patchIdx_(0),
      base_(0),
      curGap_(0),
      curPatch_(0),
      patchMask_(0),
      actualGap_(0),
      unpacked_(pool, 0),
      unpackedPatch_(pool, 0) {}

template RleDecoderV2<true>::RleDecoderV2(
    std::unique_ptr<dwio::common::SeekableInputStream> input,
    MemoryPool& pool);
template RleDecoderV2<false>::RleDecoderV2(
    std::unique_ptr<dwio::common::SeekableInputStream> input,
    MemoryPool& pool);

template <bool isSigned>
void RleDecoderV2<isSigned>::seekToRowGroup(
    dwio::common::PositionProvider& location) {
  // move the input stream
  dwio::common::IntDecoder<isSigned>::inputStream_->seekToPosition(location);
  // clear state
  dwio::common::IntDecoder<isSigned>::bufferStart_ = nullptr;
  dwio::common::IntDecoder<isSigned>::bufferEnd_ = nullptr;
  runRead_ = 0;
  runLength_ = 0;
  // skip ahead the given number of records
  this->pendingSkip_ = location.next();
}

template void RleDecoderV2<true>::seekToRowGroup(
    dwio::common::PositionProvider& location);
template void RleDecoderV2<false>::seekToRowGroup(
    dwio::common::PositionProvider& location);

template <bool isSigned>
void RleDecoderV2<isSigned>::skipPending() {
  // simple for now, until perf tests indicate something encoding specific is
  // needed
  constexpr int64_t N = 64;
  int64_t dummy[N];
  auto numValues = this->pendingSkip_;
  this->pendingSkip_ = 0;
  while (numValues) {
    uint64_t nRead = std::min(N, numValues);
    doNext(dummy, nRead, nullptr);
    numValues -= nRead;
  }
}

template void RleDecoderV2<true>::skipPending();
template void RleDecoderV2<false>::skipPending();

template <bool isSigned>
void RleDecoderV2<isSigned>::next(
    int64_t* const data,
    const uint64_t numValues,
    const uint64_t* const nulls) {
  skipPending();
  doNext(data, numValues, nulls);
}

template <bool isSigned>
void RleDecoderV2<isSigned>::doNext(
    int64_t* const data,
    const uint64_t numValues,
    const uint64_t* const nulls) {
  uint64_t nRead = 0;

  while (nRead < numValues) {
    // Skip any nulls before attempting to read first byte.
    while (nulls && bits::isBitNull(nulls, nRead)) {
      if (++nRead == numValues) {
        return; // ended with null values
      }
    }

    if (runRead_ == runLength_) {
      resetRun();
    }

    uint64_t offset = nRead, length = numValues - nRead;

    switch (type_) {
      case SHORT_REPEAT:
        nRead += nextShortRepeats(data, offset, length, nulls);
        break;
      case DIRECT:
        nRead += nextDirect(data, offset, length, nulls);
        break;
      case PATCHED_BASE:
        nRead += nextPatched(data, offset, length, nulls);
        break;
      case DELTA:
        nRead += nextDelta(data, offset, length, nulls);
        break;
      default:
        VELOX_FAIL("unknown encoding: {}", static_cast<int>(type_));
    }
  }
}

template void RleDecoderV2<true>::doNext(
    int64_t* const data,
    const uint64_t numValues,
    const uint64_t* const nulls);
template void RleDecoderV2<false>::doNext(
    int64_t* const data,
    const uint64_t numValues,
    const uint64_t* const nulls);

template <bool isSigned>
uint64_t RleDecoderV2<isSigned>::nextShortRepeats(
    int64_t* data,
    uint64_t offset,
    uint64_t numValues,
    const uint64_t* nulls) {
  if (runRead_ == runLength_) {
    // extract the number of fixed bytes
    byteSize_ = (firstByte_ >> 3) & 0x07;
    byteSize_ += 1;

    runLength_ = firstByte_ & 0x07;
    // run lengths values are stored only after MIN_REPEAT value is met
    runLength_ += RLE_MINIMUM_REPEAT;
    runRead_ = 0;

    // read the repeated value which is store using fixed bytes
    firstValue_ = readLongBE(byteSize_);

    if (isSigned) {
      firstValue_ =
          ZigZag::decode<uint64_t>(static_cast<uint64_t>(firstValue_));
    }
  }

  uint64_t nRead = std::min(runLength_ - runRead_, numValues);

  if (nulls) {
    for (uint64_t pos = offset; pos < offset + nRead; ++pos) {
      if (!bits::isBitNull(nulls, pos)) {
        data[pos] = firstValue_;
        ++runRead_;
      }
    }
  } else {
    for (uint64_t pos = offset; pos < offset + nRead; ++pos) {
      data[pos] = firstValue_;
      ++runRead_;
    }
  }

  return nRead;
}

template uint64_t RleDecoderV2<true>::nextShortRepeats(
    int64_t* data,
    uint64_t offset,
    uint64_t numValues,
    const uint64_t* nulls);
template uint64_t RleDecoderV2<false>::nextShortRepeats(
    int64_t* data,
    uint64_t offset,
    uint64_t numValues,
    const uint64_t* nulls);

template <bool isSigned>
uint64_t RleDecoderV2<isSigned>::nextDirect(
    int64_t* data,
    uint64_t offset,
    uint64_t numValues,
    const uint64_t* nulls) {
  if (runRead_ == runLength_) {
    // extract the number of fixed bits
    unsigned char fbo = (firstByte_ >> 1) & 0x1f;
    bitSize_ = decodeBitWidth(fbo);

    // extract the run length
    runLength_ = static_cast<uint64_t>(firstByte_ & 0x01) << 8;
    runLength_ |= readByte();
    // runs are one off
    runLength_ += 1;
    runRead_ = 0;
  }

  uint64_t nRead = std::min(runLength_ - runRead_, numValues);

  runRead_ += readLongs(data, offset, nRead, bitSize_, nulls);

  if (isSigned) {
    if (nulls) {
      for (uint64_t pos = offset; pos < offset + nRead; ++pos) {
        if (!bits::isBitNull(nulls, pos)) {
          data[pos] =
              ZigZag::decode<uint64_t>(static_cast<uint64_t>(data[pos]));
        }
      }
    } else {
      for (uint64_t pos = offset; pos < offset + nRead; ++pos) {
        data[pos] = ZigZag::decode<uint64_t>(static_cast<uint64_t>(data[pos]));
      }
    }
  }

  return nRead;
}

template uint64_t RleDecoderV2<true>::nextDirect(
    int64_t* const data,
    uint64_t offset,
    uint64_t numValues,
    const uint64_t* const nulls);
template uint64_t RleDecoderV2<false>::nextDirect(
    int64_t* const data,
    uint64_t offset,
    uint64_t numValues,
    const uint64_t* const nulls);

template <bool isSigned>
uint64_t RleDecoderV2<isSigned>::nextPatched(
    int64_t* const data,
    uint64_t offset,
    uint64_t numValues,
    const uint64_t* const nulls) {
  if (runRead_ == runLength_) {
    // extract the number of fixed bits
    unsigned char fbo = (firstByte_ >> 1) & 0x1f;
    bitSize_ = decodeBitWidth(fbo);

    // extract the run length
    runLength_ = static_cast<uint64_t>(firstByte_ & 0x01) << 8;
    runLength_ |= readByte();
    // runs are one off
    runLength_ += 1;
    runRead_ = 0;

    // extract the number of bytes occupied by base
    uint64_t thirdByte = readByte();
    byteSize_ = (thirdByte >> 5) & 0x07;
    // base width is one off
    byteSize_ += 1;

    // extract patch width
    uint32_t pwo = thirdByte & 0x1f;
    patchBitSize_ = decodeBitWidth(pwo);

    // read fourth byte and extract patch gap width
    uint64_t fourthByte = readByte();
    uint32_t pgw = (fourthByte >> 5) & 0x07;
    // patch gap width is one off
    pgw += 1;

    // extract the length of the patch list
    size_t pl = fourthByte & 0x1f;
    VELOX_CHECK_NE(
        pl,
        0,
        "Corrupt PATCHED_BASE encoded data (pl==0)! ",
        dwio::common::IntDecoder<isSigned>::inputStream_->getName());

    // read the next base width number of bytes to extract base value
    base_ = readLongBE(byteSize_);
    int64_t mask = (static_cast<int64_t>(1) << ((byteSize_ * 8) - 1));
    // if mask of base value is 1 then base is negative value else positive
    if ((base_ & mask) != 0) {
      base_ = base_ & ~mask;
      base_ = -base_;
    }

    // TODO: something more efficient than resize
    unpacked_.resize(runLength_);
    unpackedIdx_ = 0;
    readLongs(unpacked_.data(), 0, runLength_, bitSize_);
    // any remaining bits are thrown out
    resetReadLongs();

    // TODO: something more efficient than resize
    unpackedPatch_.resize(pl);
    patchIdx_ = 0;
    // TODO: Skip corrupt?
    //    if ((patchBitSize + pgw) > 64 && !skipCorrupt) {
    VELOX_CHECK_LE(
        (patchBitSize_ + pgw),
        64,
        "Corrupt PATCHED_BASE encoded data (patchBitSize + pgw > 64)! ",
        dwio::common::IntDecoder<isSigned>::inputStream_->getName());
    uint32_t cfb = getClosestFixedBits(patchBitSize_ + pgw);
    readLongs(unpackedPatch_.data(), 0, pl, cfb);
    // any remaining bits are thrown out
    resetReadLongs();

    // apply the patch directly when decoding the packed data
    patchMask_ = ((static_cast<int64_t>(1) << patchBitSize_) - 1);

    adjustGapAndPatch();
  }

  uint64_t nRead = std::min(runLength_ - runRead_, numValues);

  for (uint64_t pos = offset; pos < offset + nRead; ++pos) {
    // skip null positions
    if (nulls && bits::isBitNull(nulls, pos)) {
      continue;
    }
    if (static_cast<int64_t>(unpackedIdx_) != actualGap_) {
      // no patching required. add base to unpacked value to get final value
      data[pos] = base_ + unpacked_[unpackedIdx_];
    } else {
      // extract the patch value
      int64_t patchedVal = unpacked_[unpackedIdx_] | (curPatch_ << bitSize_);

      // add base to patched value
      data[pos] = base_ + patchedVal;

      // increment the patch to point to next entry in patch list
      ++patchIdx_;

      if (patchIdx_ < unpackedPatch_.size()) {
        adjustGapAndPatch();

        // next gap is relative to the current gap
        actualGap_ += unpackedIdx_;
      }
    }

    ++runRead_;
    ++unpackedIdx_;
  }

  return nRead;
}

template uint64_t RleDecoderV2<true>::nextPatched(
    int64_t* const data,
    uint64_t offset,
    uint64_t numValues,
    const uint64_t* const nulls);

template uint64_t RleDecoderV2<false>::nextPatched(
    int64_t* const data,
    uint64_t offset,
    uint64_t numValues,
    const uint64_t* const nulls);

template <bool isSigned>
uint64_t RleDecoderV2<isSigned>::nextDelta(
    int64_t* const data,
    uint64_t offset,
    uint64_t numValues,
    const uint64_t* const nulls) {
  if (runRead_ == runLength_) {
    // extract the number of fixed bits
    unsigned char fbo = (firstByte_ >> 1) & 0x1f;
    if (fbo != 0) {
      bitSize_ = decodeBitWidth(fbo);
    } else {
      bitSize_ = 0;
    }

    // extract the run length
    runLength_ = static_cast<uint64_t>(firstByte_ & 0x01) << 8;
    runLength_ |= readByte();
    ++runLength_; // account for first value
    runRead_ = deltaBase_ = 0;

    // read the first value stored as vint
    if constexpr (isSigned) {
      firstValue_ = dwio::common::IntDecoder<isSigned>::readVsLong();
    } else {
      firstValue_ = static_cast<int64_t>(
          dwio::common::IntDecoder<isSigned>::readVuLong());
    }

    prevValue_ = firstValue_;

    // read the fixed delta value stored as vint (deltas can be negative even
    // if all number are positive)
    deltaBase_ = dwio::common::IntDecoder<isSigned>::readVsLong();
  }

  uint64_t nRead = std::min(runLength_ - runRead_, numValues);

  uint64_t pos = offset;
  for (; pos < offset + nRead; ++pos) {
    // skip null positions
    if (!nulls || !bits::isBitNull(nulls, pos)) {
      break;
    }
  }
  if (runRead_ == 0 && pos < offset + nRead) {
    data[pos++] = firstValue_;
    ++runRead_;
  }

  if (bitSize_ == 0) {
    // add fixed deltas to adjacent values
    for (; pos < offset + nRead; ++pos) {
      // skip null positions
      if (nulls && bits::isBitNull(nulls, pos)) {
        continue;
      }
      prevValue_ = data[pos] = prevValue_ + deltaBase_;
      ++runRead_;
    }
  } else {
    for (; pos < offset + nRead; ++pos) {
      // skip null positions
      if (!nulls || !bits::isBitNull(nulls, pos)) {
        break;
      }
    }
    if (runRead_ < 2 && pos < offset + nRead) {
      // add delta base and first value
      prevValue_ = data[pos++] = firstValue_ + deltaBase_;
      ++runRead_;
    }

    // write the unpacked values, add it to previous value and store final
    // value to result buffer. if the delta base value is negative then it
    // is a decreasing sequence else an increasing sequence
    uint64_t remaining = (offset + nRead) - pos;
    runRead_ += readLongs(data, pos, remaining, bitSize_, nulls);

    if (deltaBase_ < 0) {
      for (; pos < offset + nRead; ++pos) {
        // skip null positions
        if (nulls && bits::isBitNull(nulls, pos)) {
          continue;
        }
        prevValue_ = data[pos] = prevValue_ - data[pos];
      }
    } else {
      for (; pos < offset + nRead; ++pos) {
        // skip null positions
        if (nulls && bits::isBitNull(nulls, pos)) {
          continue;
        }
        prevValue_ = data[pos] = prevValue_ + data[pos];
      }
    }
  }
  return nRead;
}

template uint64_t RleDecoderV2<true>::nextDelta(
    int64_t* const data,
    uint64_t offset,
    uint64_t numValues,
    const uint64_t* const nulls);

template uint64_t RleDecoderV2<false>::nextDelta(
    int64_t* const data,
    uint64_t offset,
    uint64_t numValues,
    const uint64_t* const nulls);

template <bool isSigned>
int64_t RleDecoderV2<isSigned>::readValue() {
  if (runRead_ == runLength_) {
    resetRun();
  }

  uint64_t nRead = 0;
  int64_t value = 0;
  switch (type_) {
    case SHORT_REPEAT:
      nRead = nextShortRepeats(&value, 0, 1, nullptr);
      break;
    case DIRECT:
      nRead = nextDirect(&value, 0, 1, nullptr);
      break;
    case PATCHED_BASE:
      nRead = nextPatched(&value, 0, 1, nullptr);
      break;
    case DELTA:
      nRead = nextDelta(&value, 0, 1, nullptr);
      break;
    default:
      VELOX_FAIL("unknown encoding: {}", static_cast<int>(type_));
  }
  VELOX_CHECK_EQ(nRead, (uint64_t)1);
  return value;
}

template int64_t RleDecoderV2<true>::readValue();

template int64_t RleDecoderV2<false>::readValue();

} // namespace facebook::velox::dwrf
