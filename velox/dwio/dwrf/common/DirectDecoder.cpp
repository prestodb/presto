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

#include "velox/dwio/dwrf/common/DirectDecoder.h"
#include "velox/common/base/BitUtil.h"

namespace facebook::velox::dwrf {

template <bool isSigned>
void DirectDecoder<isSigned>::seekToRowGroup(PositionProvider& location) {
  // move the input stream
  IntDecoder<isSigned>::inputStream->seekToPosition(location);
  // force a re-read from the stream
  IntDecoder<isSigned>::bufferEnd = IntDecoder<isSigned>::bufferStart;
}

template void DirectDecoder<true>::seekToRowGroup(PositionProvider& location);
template void DirectDecoder<false>::seekToRowGroup(PositionProvider& location);

template <bool isSigned>
void DirectDecoder<isSigned>::skip(uint64_t numValues) {
  IntDecoder<isSigned>::skipLongs(numValues);
}

template void DirectDecoder<true>::skip(uint64_t numValues);
template void DirectDecoder<false>::skip(uint64_t numValues);

template <bool isSigned>
void DirectDecoder<isSigned>::next(
    int64_t* const data,
    const uint64_t numValues,
    const uint64_t* nulls) {
  uint64_t position = 0;
  // skipNulls()
  if (nulls) {
    // Skip over null values.
    while (position < numValues && bits::isBitNull(nulls, position)) {
      ++position;
    }
  }

  // this is gross and very not DRY, but helps avoid branching
  if (position < numValues) {
    if (nulls) {
      if (!IntDecoder<isSigned>::useVInts) {
        for (uint64_t i = position; i < numValues; ++i) {
          if (!bits::isBitNull(nulls, i)) {
            data[i] = IntDecoder<isSigned>::readLongLE();
          }
        }
      } else if constexpr (isSigned) {
        for (uint64_t i = position; i < numValues; ++i) {
          if (!bits::isBitNull(nulls, i)) {
            data[i] = IntDecoder<isSigned>::readVsLong();
          }
        }
      } else {
        for (uint64_t i = position; i < numValues; ++i) {
          if (!bits::isBitNull(nulls, i)) {
            data[i] = static_cast<int64_t>(IntDecoder<isSigned>::readVuLong());
          }
        }
      }
    } else {
      if (!IntDecoder<isSigned>::useVInts) {
        for (uint64_t i = position; i < numValues; ++i) {
          data[i] = IntDecoder<isSigned>::readLongLE();
        }
      } else if constexpr (isSigned) {
        for (uint64_t i = position; i < numValues; ++i) {
          data[i] = IntDecoder<isSigned>::readVsLong();
        }
      } else {
        for (uint64_t i = position; i < numValues; ++i) {
          data[i] = static_cast<int64_t>(IntDecoder<isSigned>::readVuLong());
        }
      }
    }
  }
}

template void DirectDecoder<true>::next(
    int64_t* const data,
    const uint64_t numValues,
    const uint64_t* nulls);

template void DirectDecoder<false>::next(
    int64_t* const data,
    const uint64_t numValues,
    const uint64_t* nulls);

} // namespace facebook::velox::dwrf
