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

#include "velox/dwio/common/DirectDecoder.h"
#include "velox/common/base/BitUtil.h"
#include "velox/dwio/common/SeekableInputStream.h"

namespace facebook::velox::dwio::common {

template <bool isSigned>
void DirectDecoder<isSigned>::seekToRowGroup(
    dwio::common::PositionProvider& location) {
  // Moves the input stream.
  IntDecoder<isSigned>::inputStream_->seekToPosition(location);
  // Forces a re-read from the stream.
  IntDecoder<isSigned>::bufferEnd_ = IntDecoder<isSigned>::bufferStart_;
  this->pendingSkip_ = 0;
}

template void DirectDecoder<true>::seekToRowGroup(
    dwio::common::PositionProvider& location);
template void DirectDecoder<false>::seekToRowGroup(
    dwio::common::PositionProvider& location);

template <bool isSigned>
template <typename T>
void DirectDecoder<isSigned>::nextValues(
    T* data,
    uint64_t numValues,
    const uint64_t* nulls) {
  skipPending();
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
      if (!IntDecoder<isSigned>::useVInts_) {
        if constexpr (std::is_same_v<T, int128_t>) {
          VELOX_NYI();
        }
        for (uint64_t i = position; i < numValues; ++i) {
          if (!bits::isBitNull(nulls, i)) {
            data[i] = IntDecoder<isSigned>::readLongLE();
          }
        }
      } else {
        for (uint64_t i = position; i < numValues; ++i) {
          if (!bits::isBitNull(nulls, i)) {
            data[i] = IntDecoder<isSigned>::template readVInt<T>();
          }
        }
      }
    } else {
      if (!IntDecoder<isSigned>::useVInts_) {
        if constexpr (std::is_same_v<T, int128_t>) {
          VELOX_NYI();
        }
        for (uint64_t i = position; i < numValues; ++i) {
          data[i] = IntDecoder<isSigned>::readLongLE();
        }
      } else {
        for (uint64_t i = position; i < numValues; ++i) {
          data[i] = IntDecoder<isSigned>::template readVInt<T>();
        }
      }
    }
  }
}

template void DirectDecoder<true>::nextValues(
    int64_t* data,
    uint64_t numValues,
    const uint64_t* nulls);

template void DirectDecoder<true>::nextValues(
    int128_t* data,
    uint64_t numValues,
    const uint64_t* nulls);

template void DirectDecoder<false>::nextValues(
    int64_t* data,
    uint64_t numValues,
    const uint64_t* nulls);

template void DirectDecoder<false>::nextValues(
    int128_t* data,
    uint64_t numValues,
    const uint64_t* nulls);

} // namespace facebook::velox::dwio::common
