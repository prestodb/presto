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

namespace facebook::velox::parquet {

class StringDecoder {
 public:
  StringDecoder(const char* start, const char* end, int fixedLength = -1)
      : bufferStart_(start),
        bufferEnd_(end),
        lastSafeWord_(end - simd::kPadding),
        fixedLength_(fixedLength) {}

  void skip(uint64_t numValues) {
    skip<false>(numValues, 0, nullptr);
  }

  template <bool hasNulls>
  inline void skip(int32_t numValues, int32_t current, const uint64_t* nulls) {
    if (hasNulls) {
      numValues = bits::countNonNulls(nulls, current, current + numValues);
    }
    for (auto i = 0; i < numValues; ++i) {
      bufferStart_ += lengthAt(bufferStart_) + sizeof(int32_t);
    }
  }

  template <bool hasNulls, typename Visitor>
  void readWithVisitor(const uint64_t* nulls, Visitor visitor) {
    int32_t current = visitor.start();
    int32_t numValues = 0;
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
            if constexpr (Visitor::kHasHook) {
              visitor.setNumValues(
                  Visitor::kHasFilter ? numValues : visitor.numRows());
            }
            return;
          }
        }

        // We are at a non-null value on a row to visit.
        toSkip = visitor.process(
            fixedLength_ > 0 ? readFixedString() : readString(), atEnd);
      }
      ++current;
      ++numValues;
      if (toSkip) {
        skip<hasNulls>(toSkip, current, nulls);
        current += toSkip;
      }
      if (atEnd) {
        if constexpr (Visitor::kHasHook) {
          visitor.setNumValues(
              Visitor::kHasFilter ? numValues : visitor.numRows());
        }
        return;
      }
    }
  }

 private:
  int32_t lengthAt(const char* buffer) {
    return *reinterpret_cast<const int32_t*>(buffer);
  }

  folly::StringPiece readString() {
    auto length = lengthAt(bufferStart_);
    bufferStart_ += length + sizeof(int32_t);
    return folly::StringPiece(bufferStart_ - length, length);
  }

  folly::StringPiece readFixedString() {
    bufferStart_ += fixedLength_;
    return folly::StringPiece(bufferStart_ - fixedLength_, fixedLength_);
  }

  const char* bufferStart_;
  const char* bufferEnd_;
  const char* const lastSafeWord_;
  const int fixedLength_;
};

} // namespace facebook::velox::parquet
