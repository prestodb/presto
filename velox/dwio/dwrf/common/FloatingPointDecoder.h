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

#include "velox/dwio/dwrf/common/DecoderUtil.h"
#include "velox/dwio/dwrf/common/StreamUtil.h"

namespace facebook::velox::dwrf {

struct DropValues;

template <typename TData, typename TRequested>
class FloatingPointDecoder {
 public:
  FloatingPointDecoder(std::unique_ptr<SeekableInputStream>&& input)
      : input_(std::move(input)) {}

  TData readValue() {
    if (bufferEnd_ - bufferStart_ >= sizeof(TData)) {
      TData value = *reinterpret_cast<const TData*>(bufferStart_);
      bufferStart_ += sizeof(TData);
      return value;
    }
    TData temp;
    readBytes(sizeof(temp), input_.get(), &temp, bufferStart_, bufferEnd_);
    return temp;
  }

  void seekToRowGroup(PositionProvider& positionProvider) {
    input_->seekToPosition(positionProvider);
    bufferStart_ = bufferEnd_;
  }

  void skip(uint64_t numValues) {
    skipBytes(
        numValues * sizeof(TData), input_.get(), bufferStart_, bufferEnd_);
  }

  template <bool hasNulls>
  inline void skip(int32_t numValues, int32_t current, const uint64_t* nulls) {
    if (hasNulls) {
      numValues = bits::countNonNulls(nulls, current, current + numValues);
    }
    skipBytes(
        numValues * sizeof(TData), input_.get(), bufferStart_, bufferEnd_);
  }

  template <bool hasNulls, typename Visitor>
  void readWithVisitor(const uint64_t* nulls, Visitor visitor) {
    if (std::is_same<TData, TRequested>::value &&
        useFastPath<Visitor, hasNulls>(visitor)) {
      fastPath<hasNulls>(nulls, visitor);
      return;
    }
    int32_t current = visitor.start();
    skip<hasNulls>(current, 0, nulls);
    bool atEnd = false;
    bool allowNulls = hasNulls && visitor.allowNulls();
    for (;;) {
      int32_t toSkip;
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
        toSkip = visitor.process(static_cast<TRequested>(readValue()), atEnd);
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

 private:
  template <bool hasNulls, typename Visitor>
  void fastPath(const uint64_t* nulls, Visitor& visitor) {
    constexpr bool hasFilter =
        !std::is_same<typename Visitor::FilterType, common::AlwaysTrue>::value;
    constexpr bool filterOnly =
        std::is_same<typename Visitor::Extract, DropValues>::value;
    constexpr bool hasHook =
        !std::is_same<typename Visitor::HookType, NoHook>::value;
    int32_t numValues = 0;
    auto rows = visitor.rows();
    auto numRows = visitor.numRows();
    if (hasNulls) {
      raw_vector<int32_t>* innerVector = nullptr;
      auto outerVector = &visitor.outerNonNullRows();
      int32_t tailSkip = 0;
      if (Visitor::dense) {
        nonNullRowsFromDense(nulls, numRows, *outerVector);
        if (outerVector->empty()) {
          visitor.setAllNull(hasFilter ? 0 : numRows);
          return;
        }
      } else {
        innerVector = &visitor.innerNonNullRows();
        auto anyNulls = nonNullRowsFromSparse < hasFilter,
             !hasFilter &&
            !hasHook >
                (nulls,
                 folly::Range<const int32_t*>(rows, numRows),
                 *innerVector,
                 *outerVector,
                 (hasFilter || hasHook) ? nullptr : visitor.rawNulls(numRows),
                 tailSkip);
        if (anyNulls) {
          visitor.setHasNulls();
        }
        if (innerVector->empty()) {
          skip<false>(tailSkip, 0, nullptr);
          visitor.setAllNull(hasFilter ? 0 : numRows);
          return;
        }
      }
      fixedWidthScan<TData, filterOnly, true>(
          innerVector ? folly::Range<const int32_t*>(*innerVector)
                      : folly::Range<const int32_t*>(rows, outerVector->size()),
          outerVector->data(),
          visitor.rawValues(numRows),
          hasFilter ? visitor.outputRows(numRows) : nullptr,
          numValues,
          *input_,
          bufferStart_,
          bufferEnd_,
          visitor.filter(),
          visitor.hook());
      skip<false>(tailSkip, 0, nullptr);
    } else {
      fixedWidthScan<TData, filterOnly, false>(
          folly::Range<const int32_t*>(rows, numRows),
          hasHook ? velox::iota(numRows, visitor.innerNonNullRows()) : nullptr,
          visitor.rawValues(numRows),
          hasFilter ? visitor.outputRows(numRows) : nullptr,
          numValues,
          *input_,
          bufferStart_,
          bufferEnd_,
          visitor.filter(),
          visitor.hook());
    }
    visitor.setNumValues(hasFilter ? numValues : numRows);
  }

  std::unique_ptr<SeekableInputStream> input_;
  const char* bufferStart_ = nullptr;
  const char* bufferEnd_ = nullptr;
};

} // namespace facebook::velox::dwrf
