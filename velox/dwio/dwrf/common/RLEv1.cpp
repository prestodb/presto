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
  if (numLiterals != 0) {
    if (repeat) {
      IntEncoder<isSigned>::writeByte(
          static_cast<char>(numLiterals - RLE_MINIMUM_REPEAT));
      IntEncoder<isSigned>::writeByte(static_cast<char>(delta));
      if (!IntEncoder<isSigned>::useVInts_) {
        IntEncoder<isSigned>::writeLongLE(literals[0]);
      } else {
        if constexpr (isSigned) {
          IntEncoder<isSigned>::writeVslong(literals[0]);
        } else {
          IntEncoder<isSigned>::writeVulong(literals[0]);
        }
      }
    } else {
      IntEncoder<isSigned>::writeByte(static_cast<char>(-numLiterals));
      if (!IntEncoder<isSigned>::useVInts_) {
        for (int32_t i = 0; i < numLiterals; ++i) {
          IntEncoder<isSigned>::writeLongLE(literals[i]);
        }
      } else {
        if constexpr (isSigned) {
          for (int32_t i = 0; i < numLiterals; ++i) {
            IntEncoder<isSigned>::writeVslong(literals[i]);
          }
        } else {
          for (int32_t i = 0; i < numLiterals; ++i) {
            IntEncoder<isSigned>::writeVulong(literals[i]);
          }
        }
      }
    }
    repeat = false;
    numLiterals = 0;
    tailRunLength = 0;
  }
}

template void RleEncoderV1<true>::writeValues();
template void RleEncoderV1<false>::writeValues();

template <bool isSigned>
void RleDecoderV1<isSigned>::seekToRowGroup(
    dwio::common::PositionProvider& location) {
  // move the input stream
  IntDecoder<isSigned>::inputStream->seekToPosition(location);
  // force a re-read from the stream
  IntDecoder<isSigned>::bufferEnd = IntDecoder<isSigned>::bufferStart;
  // force reading a new header
  remainingValues = 0;
  // skip ahead the given number of records
  skip(location.next());
}

template void RleDecoderV1<true>::seekToRowGroup(
    dwio::common::PositionProvider& location);
template void RleDecoderV1<false>::seekToRowGroup(
    dwio::common::PositionProvider& location);

template <bool isSigned>
void RleDecoderV1<isSigned>::skip(uint64_t numValues) {
  while (numValues > 0) {
    if (remainingValues == 0) {
      readHeader();
    }
    uint64_t count = std::min(numValues, remainingValues);
    remainingValues -= count;
    numValues -= count;
    if (repeating) {
      value += delta * static_cast<int64_t>(count);
    } else {
      IntDecoder<isSigned>::skipLongs(count);
    }
  }
}

template void RleDecoderV1<true>::skip(uint64_t numValues);
template void RleDecoderV1<false>::skip(uint64_t numValues);

template <bool isSigned>
void RleDecoderV1<isSigned>::next(
    int64_t* const data,
    const uint64_t numValues,
    const uint64_t* const nulls) {
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
    if (remainingValues == 0) {
      readHeader();
    }
    // How many do we read out of this block?
    uint64_t count = std::min(numValues - position, remainingValues);
    uint64_t consumed = 0;
    if (repeating) {
      if (nulls) {
        for (uint64_t i = 0; i < count; ++i) {
          if (!bits::isBitNull(nulls, position + i)) {
            data[position + i] = value + static_cast<int64_t>(consumed) * delta;
            consumed += 1;
          }
        }
      } else {
        for (uint64_t i = 0; i < count; ++i) {
          data[position + i] = value + static_cast<int64_t>(i) * delta;
        }
        consumed = count;
      }
      value += static_cast<int64_t>(consumed) * delta;
    } else {
      int64_t* datap = data + position;
      int64_t* const datapEnd = datap + count;
      if (nulls) {
        int32_t idx = position;
        if (!IntDecoder<isSigned>::useVInts) {
          while (datap != datapEnd) {
            if (LIKELY(!bits::isBitNull(nulls, idx++))) {
              *(datap++) = IntDecoder<isSigned>::readLongLE();
              ++consumed;
            } else {
              *(datap++) = 0;
            }
          }
        } else if constexpr (isSigned) {
          while (datap != datapEnd) {
            if (LIKELY(!bits::isBitNull(nulls, idx++))) {
              *(datap++) = IntDecoder<isSigned>::readVsLong();
              ++consumed;
            } else {
              *(datap++) = 0;
            }
          }
        } else {
          while (datap != datapEnd) {
            if (LIKELY(!bits::isBitNull(nulls, idx++))) {
              *(datap++) =
                  static_cast<int64_t>(IntDecoder<isSigned>::readVuLong());
              ++consumed;
            } else {
              *(datap++) = 0;
            }
          }
        }
      } else {
        if (!IntDecoder<isSigned>::useVInts) {
          while (datap != datapEnd) {
            *(datap++) = IntDecoder<isSigned>::readLongLE();
          }
        } else if constexpr (isSigned) {
          while (datap != datapEnd) {
            *(datap++) = IntDecoder<isSigned>::readVsLong();
          }
        } else {
          while (datap != datapEnd) {
            *(datap++) =
                static_cast<int64_t>(IntDecoder<isSigned>::readVuLong());
          }
        }
        consumed = count;
      }
    }
    remainingValues -= consumed;
    position += count;

    // skipNulls()
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
    const uint64_t* const nulls);
template void RleDecoderV1<false>::next(
    int64_t* const data,
    const uint64_t numValues,
    const uint64_t* const nulls);

template <bool isSigned>
void RleDecoderV1<isSigned>::nextLengths(
    int32_t* const data,
    const int32_t numValues) {
  uint32_t position = 0;
  while (position < numValues) {
    // If we are out of values, read more.
    if (remainingValues == 0) {
      readHeader();
    }
    // How many do we read out of this block?
    int32_t count = std::min<int32_t>(numValues - position, remainingValues);
    uint64_t consumed = 0;
    if (repeating) {
      for (uint32_t i = 0; i < count; ++i) {
        data[position + i] = value + i * delta;
      }
      consumed = count;
      value += static_cast<int64_t>(consumed) * delta;
    } else {
      IntDecoder<isSigned>::bulkRead(count, data + position);
      consumed = count;
    }
    remainingValues -= consumed;
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
