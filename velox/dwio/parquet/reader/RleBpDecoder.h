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
#include "velox/dwio/common/DecoderUtil.h"
#include "velox/type/Filter.h"
#include "velox/vector/LazyVector.h"

#include <folly/Varint.h>

namespace facebook::velox::parquet {

class RleBpDecoder {
 public:
  RleBpDecoder(
      const char* FOLLY_NONNULL start,
      const char* FOLLY_NONNULL end,
      uint8_t bitWidth)
      : bufferStart_(start),
        bufferEnd_(end),
        bitWidth_(bitWidth),
        byteWidth_(bits::roundUp(bitWidth, 8) / 8),
        bitMask_(bits::lowMask(bitWidth)),
        lastSafeWord_(end - sizeof(uint64_t)) {}

  void skip(uint64_t numValues);

  /// Copies 'numValues' bits from the encoding into 'buffer',
  /// little-endian. If 'allOnes' is non-nullptr, this function may
  /// check if all the bits are ones, as in a RLE run of all ones and
  /// not copy them into 'buffer' but instead may set '*allOnes' to
  /// true. If allOnes is non-nullptr and not all bits are ones, then
  /// '*allOnes' is set to false and the bits are copied to 'buffer'.
  void readBits(
      int32_t numValues,
      uint64_t* FOLLY_NONNULL buffer,
      bool* FOLLY_NULLABLE allOnes = nullptr);

 protected:
  void readHeader();

  const char* FOLLY_NULLABLE bufferStart_;
  const char* FOLLY_NULLABLE bufferEnd_;
  const int8_t bitWidth_;
  const int8_t byteWidth_;
  const uint64_t bitMask_;
  const char* FOLLY_NONNULL const lastSafeWord_;
  uint64_t remainingValues_{0};
  int64_t value_;
  int8_t bitOffset_{0};
  bool repeating_;
};

} // namespace facebook::velox::parquet
