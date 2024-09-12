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

#include "velox/dwio/dwrf/common/RLEv1.h"

#include <algorithm>

namespace facebook::velox::dwrf {

template <bool isSigned>
void RleEncoderV1<isSigned>::writeValues() {
  if (numLiterals_ == 0) {
    return;
  }

  if (repeat_) {
    IntEncoder<isSigned>::writeByte(
        static_cast<char>(numLiterals_ - RLE_MINIMUM_REPEAT));
    IntEncoder<isSigned>::writeByte(static_cast<char>(delta_));
    if (!IntEncoder<isSigned>::useVInts_) {
      IntEncoder<isSigned>::writeLongLE(literals_[0]);
    } else {
      if constexpr (isSigned) {
        IntEncoder<isSigned>::writeVslong(literals_[0]);
      } else {
        IntEncoder<isSigned>::writeVulong(literals_[0]);
      }
    }
  } else {
    IntEncoder<isSigned>::writeByte(static_cast<char>(-numLiterals_));
    if (!IntEncoder<isSigned>::useVInts_) {
      for (int32_t i = 0; i < numLiterals_; ++i) {
        IntEncoder<isSigned>::writeLongLE(literals_[i]);
      }
    } else {
      if constexpr (isSigned) {
        for (int32_t i = 0; i < numLiterals_; ++i) {
          IntEncoder<isSigned>::writeVslong(literals_[i]);
        }
      } else {
        for (int32_t i = 0; i < numLiterals_; ++i) {
          IntEncoder<isSigned>::writeVulong(literals_[i]);
        }
      }
    }
  }

  repeat_ = false;
  numLiterals_ = 0;
  tailRunLength_ = 0;
}

template void RleEncoderV1<true>::writeValues();
template void RleEncoderV1<false>::writeValues();

template <bool isSigned>
void RleDecoderV1<isSigned>::seekToRowGroup(
    dwio::common::PositionProvider& location) {
  // Move the input stream.
  dwio::common::IntDecoder<isSigned>::inputStream_->seekToPosition(location);
  // Force a re-read from the stream.
  dwio::common::IntDecoder<isSigned>::bufferEnd_ =
      dwio::common::IntDecoder<isSigned>::bufferStart_;
  // Force reading a new header.
  remainingValues_ = 0;
  // Skip ahead the given number of records.
  this->pendingSkip_ = location.next();
}

template void RleDecoderV1<true>::seekToRowGroup(
    dwio::common::PositionProvider& location);
template void RleDecoderV1<false>::seekToRowGroup(
    dwio::common::PositionProvider& location);

template <bool isSigned>
void RleDecoderV1<isSigned>::skipPending() {
  uint64_t numValues = this->pendingSkip_;
  this->pendingSkip_ = 0;
  while (numValues > 0) {
    if (remainingValues_ == 0) {
      readHeader();
    }
    const uint64_t count = std::min(numValues, remainingValues_);
    remainingValues_ -= count;
    numValues -= count;
    if (repeating_) {
      value_ += delta_ * static_cast<int64_t>(count);
    } else {
      dwio::common::IntDecoder<isSigned>::skipLongs(count);
    }
  }
}

template void RleDecoderV1<true>::skipPending();
template void RleDecoderV1<false>::skipPending();

template <bool isSigned>
void RleDecoderV1<isSigned>::next(
    int64_t* data,
    const uint64_t numValues,
    const uint64_t* const nulls) {
  skipPending();

  uint64_t position = 0;
  // skipNulls()
  if (nulls) {
    // Skip over null values.
    while (position < numValues && bits::isBitNull(nulls, position)) {
      ++position;
    }
  }
  while (position < numValues) {
    // If we are out of values, read more.
    if (remainingValues_ == 0) {
      readHeader();
    }
    // How many do we read out of this block?
    const uint64_t count = std::min(numValues - position, remainingValues_);
    uint64_t consumed = 0;
    if (repeating_) {
      if (nulls) {
        for (uint64_t i = 0; i < count; ++i) {
          if (!bits::isBitNull(nulls, position + i)) {
            data[position + i] =
                value_ + static_cast<int64_t>(consumed) * delta_;
            ++consumed;
          }
        }
      } else {
        for (uint64_t i = 0; i < count; ++i) {
          data[position + i] = value_ + static_cast<int64_t>(i) * delta_;
        }
        consumed = count;
      }
      value_ += static_cast<int64_t>(consumed) * delta_;
    } else {
      int64_t* next = data + position;
      int64_t* const end = next + count;
      if (nulls) {
        int32_t index = position;
        if (!dwio::common::IntDecoder<isSigned>::useVInts_) {
          while (next != end) {
            if (LIKELY(!bits::isBitNull(nulls, index++))) {
              *(next++) = dwio::common::IntDecoder<isSigned>::readLongLE();
              ++consumed;
            } else {
              *(next++) = 0;
            }
          }
        } else if constexpr (isSigned) {
          while (next != end) {
            if (LIKELY(!bits::isBitNull(nulls, index++))) {
              *(next++) = dwio::common::IntDecoder<isSigned>::readVsLong();
              ++consumed;
            } else {
              *(next++) = 0;
            }
          }
        } else {
          while (next != end) {
            if (LIKELY(!bits::isBitNull(nulls, index++))) {
              *(next++) = static_cast<int64_t>(
                  dwio::common::IntDecoder<isSigned>::readVuLong());
              ++consumed;
            } else {
              *(next++) = 0;
            }
          }
        }
      } else {
        if (!dwio::common::IntDecoder<isSigned>::useVInts_) {
          while (next != end) {
            *(next++) = dwio::common::IntDecoder<isSigned>::readLongLE();
          }
        } else if constexpr (isSigned) {
          while (next != end) {
            *(next++) = dwio::common::IntDecoder<isSigned>::readVsLong();
          }
        } else {
          while (next != end) {
            *(next++) = static_cast<int64_t>(
                dwio::common::IntDecoder<isSigned>::readVuLong());
          }
        }
        consumed = count;
      }
    }

    remainingValues_ -= consumed;
    position += count;

    // skipNulls().
    if (nulls) {
      // Skip over null values.
      while (position < numValues && bits::isBitNull(nulls, position)) {
        ++position;
      }
    }
  }
}

template void RleDecoderV1<true>::next(
    int64_t* const data,
    const uint64_t numValues,
    const uint64_t* nulls);
template void RleDecoderV1<false>::next(
    int64_t* const data,
    const uint64_t numValues,
    const uint64_t* nulls);

template <bool isSigned>
void RleDecoderV1<isSigned>::nextLengths(int32_t* data, int32_t numValues) {
  skipPending();

  uint32_t position = 0;
  while (position < numValues) {
    // If we are out of values, read more.
    if (remainingValues_ == 0) {
      readHeader();
    }

    // How many do we read out of this block?
    const int32_t count =
        std::min<int32_t>(numValues - position, remainingValues_);
    uint64_t consumed = 0;
    if (repeating_) {
      for (uint32_t i = 0; i < count; ++i) {
        data[position + i] = value_ + i * delta_;
      }
      consumed = count;
      value_ += static_cast<int64_t>(consumed) * delta_;
    } else {
      dwio::common::IntDecoder<isSigned>::bulkRead(count, data + position);
      consumed = count;
    }
    remainingValues_ -= consumed;
    position += count;
  }
}

template void RleDecoderV1<false>::nextLengths(
    int32_t* const data,
    const int32_t numValues);
template void RleDecoderV1<true>::nextLengths(
    int32_t* const data,
    const int32_t numValues);

} // namespace facebook::velox::dwrf
