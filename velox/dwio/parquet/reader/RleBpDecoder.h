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
#include "velox/dwio/common/BitPackDecoder.h"
#include "velox/dwio/common/DecoderUtil.h"

namespace facebook::velox::parquet {

class RleBpDecoder {
 public:
  RleBpDecoder(const char* start, const char* end, uint8_t bitWidth)
      : bufferStart_(start),
        bufferEnd_(end),
        bitWidth_(bitWidth),
        byteWidth_(bits::divRoundUp(bitWidth, 8)),
        bitMask_(bits::lowMask(bitWidth)),
        lastSafeWord_(end - sizeof(uint64_t)) {}

  void skip(uint64_t numValues);

  /// Decode @param numValues number of values and copy the decoded values into
  /// @param outputBuffer
  template <typename T>
  void next(T* FOLLY_NONNULL& outputBuffer, uint64_t numValues) {
    while (numValues > 0) {
      if (numRemainingUnpackedValues_ > 0) {
        auto numValuesToRead =
            std::min<uint64_t>(numValues, numRemainingUnpackedValues_);
        copyRemainingUnpackedValues(outputBuffer, numValuesToRead);

        numValues -= numValuesToRead;
      } else {
        if (remainingValues_ == 0) {
          readHeader();
        }

        auto numValuesToRead = std::min<uint32_t>(numValues, remainingValues_);
        if (repeating_) {
          std::fill(outputBuffer, outputBuffer + numValuesToRead, value_);
          outputBuffer += numValuesToRead;
          remainingValues_ -= numValuesToRead;
        } else {
          remainingUnpackedValuesOffset_ = 0;
          // The parquet standard requires the bit packed values are always a
          // multiple of 8. So we read a multiple of 8 values each time
          dwio::common::unpack<T>(
              reinterpret_cast<const uint8_t*&>(bufferStart_),
              bufferEnd_ - bufferStart_,
              numValuesToRead & 0xfffffff8,
              bitWidth_,
              reinterpret_cast<T * FOLLY_NONNULL&>(outputBuffer));
          remainingValues_ -= (numValuesToRead & 0xfffffff8);

          // Unpack the next 8 values to remainingUnpackedValues_ if necessary
          if ((numValuesToRead & 7) != 0) {
            T* output = reinterpret_cast<T*>(remainingUnpackedValues_);
            dwio::common::unpack<T>(
                reinterpret_cast<const uint8_t*&>(bufferStart_),
                bufferEnd_ - bufferStart_,
                8,
                bitWidth_,
                output);
            numRemainingUnpackedValues_ = 8;
            remainingUnpackedValuesOffset_ = 0;

            copyRemainingUnpackedValues(outputBuffer, numValuesToRead & 7);
            remainingValues_ -= 8;
          }
        }

        numValues -= numValuesToRead;
      }
    }
  }

  /// Copies 'numValues' bits from the encoding into 'buffer',
  /// little-endian. If 'allOnes' is non-nullptr, this function may
  /// check if all the bits are ones, as in a RLE run of all ones and
  /// not copy them into 'buffer' but instead may set '*allOnes' to
  /// true. If allOnes is non-nullptr and not all bits are ones, then
  /// '*allOnes' is set to false and the bits are copied to 'buffer'.
  void
  readBits(int32_t numValues, uint64_t* outputBuffer, bool* allOnes = nullptr);

 protected:
  void readHeader();

  template <typename T>
  inline void copyRemainingUnpackedValues(
      T* FOLLY_NONNULL& outputBuffer,
      int8_t numValues) {
    VELOX_CHECK_LE(numValues, numRemainingUnpackedValues_);

    std::memcpy(
        outputBuffer,
        reinterpret_cast<T*>(remainingUnpackedValues_) +
            remainingUnpackedValuesOffset_,
        numValues);

    outputBuffer += numValues;
    numRemainingUnpackedValues_ -= numValues;
    remainingUnpackedValuesOffset_ += numValues;
  }

  const char* bufferStart_;
  const char* bufferEnd_;
  const int8_t bitWidth_;
  const int8_t byteWidth_;
  const uint64_t bitMask_;
  const char* const lastSafeWord_;
  uint64_t remainingValues_{0};
  int64_t value_;
  int8_t bitOffset_{0};
  bool repeating_;

  uint64_t remainingUnpackedValues_[8];
  int8_t remainingUnpackedValuesOffset_{0};
  int8_t numRemainingUnpackedValues_{0};
};

} // namespace facebook::velox::parquet
