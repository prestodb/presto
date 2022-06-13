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

#include "velox/common/base/Nulls.h"
#include "velox/dwio/dwrf/common/DecoderUtil.h"
#include "velox/dwio/dwrf/common/IntDecoder.h"

namespace facebook::velox::dwrf {

struct DropValues;

template <bool isSigned>
class DirectDecoder : public IntDecoder<isSigned> {
 public:
  DirectDecoder(
      std::unique_ptr<dwio::common::SeekableInputStream> input,
      bool useVInts,
      uint32_t numBytes)
      : IntDecoder<isSigned>{std::move(input), useVInts, numBytes} {}

  void seekToRowGroup(dwio::common::PositionProvider&) override;

  void skip(uint64_t numValues) override;

  void next(int64_t* data, uint64_t numValues, const uint64_t* nulls) override;

  template <bool hasNulls>
  inline void skip(int32_t numValues, int32_t current, const uint64_t* nulls) {
    if (!numValues) {
      return;
    }
    if (hasNulls) {
      numValues = bits::countNonNulls(nulls, current, current + numValues);
    }
    IntDecoder<isSigned>::skipLongsFast(numValues);
  }

  template <bool hasNulls, typename Visitor>
  void readWithVisitor(const uint64_t* nulls, Visitor visitor) {
    if (useFastPath<Visitor, hasNulls>(visitor)) {
      fastPath<hasNulls>(nulls, visitor);
      return;
    }
    int32_t current = visitor.start();
    skip<hasNulls>(current, 0, nulls);
    const bool allowNulls = hasNulls && visitor.allowNulls();
    for (;;) {
      bool atEnd = false;
      int32_t toSkip;
      if (hasNulls) {
        if (!allowNulls) {
          toSkip = visitor.checkAndSkipNulls(nulls, current, atEnd);
          if (!Visitor::dense) {
            skip<false>(toSkip, current, nullptr);
          }
          if (atEnd) {
            return;
          }
        } else {
          if (bits::isBitNull(nulls, current)) {
            toSkip = visitor.processNull(atEnd);
            goto skip;
          }
        }
      }
      toSkip = visitor.process(IntDecoder<isSigned>::readLong(), atEnd);
    skip:
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
  using super = IntDecoder<isSigned>;

  template <bool hasNulls, typename Visitor>
  void fastPath(const uint64_t* nulls, Visitor& visitor) {
    using T = typename Visitor::DataType;
    constexpr bool hasFilter =
        !std::is_same<typename Visitor::FilterType, common::AlwaysTrue>::value;
    constexpr bool filterOnly =
        std::is_same<typename Visitor::Extract, DropValues>::value;
    constexpr bool hasHook =
        !std::is_same<typename Visitor::HookType, NoHook>::value;

    int32_t numValues = 0;
    auto rows = visitor.rows();
    auto numRows = visitor.numRows();
    auto numNonNull = numRows;
    auto rowsAsRange = folly::Range<const int32_t*>(rows, numRows);
    auto data = visitor.rawValues(numRows);
    if (hasNulls) {
      int32_t tailSkip = 0;
      raw_vector<int32_t>* innerVector = nullptr;
      auto outerVector = &visitor.outerNonNullRows();
      if (Visitor::dense) {
        nonNullRowsFromDense(nulls, numRows, *outerVector);
        numNonNull = outerVector->size();
        if (!numNonNull) {
          visitor.setAllNull(hasFilter ? 0 : numRows);
          return;
        }
      } else {
        innerVector = &visitor.innerNonNullRows();
        auto anyNulls = nonNullRowsFromSparse < hasFilter,
             !hasFilter &&
            !hasHook >
                (nulls,
                 rowsAsRange,
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
      if (super::useVInts) {
        if (Visitor::dense) {
          super::bulkRead(numNonNull, data);
        } else {
          super::bulkReadRows(*innerVector, data);
        }
        skip<false>(tailSkip, 0, nullptr);
        auto dataRows = innerVector
            ? folly::Range<const int*>(innerVector->data(), innerVector->size())
            : folly::Range<const int32_t*>(rows, outerVector->size());
        processFixedWidthRun<T, filterOnly, true, Visitor::dense>(
            dataRows,
            0,
            dataRows.size(),
            outerVector->data(),
            data,
            hasFilter ? visitor.outputRows(numRows) : nullptr,
            numValues,
            visitor.filter(),
            visitor.hook());
      } else {
        fixedWidthScan<T, filterOnly, true>(
            innerVector
                ? folly::Range<const int32_t*>(*innerVector)
                : folly::Range<const int32_t*>(rows, outerVector->size()),
            outerVector->data(),
            visitor.rawValues(numRows),
            hasFilter ? visitor.outputRows(numRows) : nullptr,
            numValues,
            *super::inputStream,
            super::bufferStart,
            super::bufferEnd,
            visitor.filter(),
            visitor.hook());
        skip<false>(tailSkip, 0, nullptr);
      }
    } else {
      if (super::useVInts) {
        if (Visitor::dense) {
          super::bulkRead(numRows, visitor.rawValues(numRows));
        } else {
          super::bulkReadRows(
              folly::Range<const int32_t*>(rows, numRows),
              visitor.rawValues(numRows));
        }
        processFixedWidthRun<T, filterOnly, false, Visitor::dense>(
            rowsAsRange,
            0,
            rowsAsRange.size(),
            hasHook ? velox::iota(numRows, visitor.innerNonNullRows())
                    : nullptr,
            visitor.rawValues(numRows),
            hasFilter ? visitor.outputRows(numRows) : nullptr,
            numValues,
            visitor.filter(),
            visitor.hook());
      } else {
        fixedWidthScan<T, filterOnly, false>(
            rowsAsRange,
            hasHook ? velox::iota(numRows, visitor.innerNonNullRows())
                    : nullptr,
            visitor.rawValues(numRows),
            visitor.outputRows(numRows),
            numValues,
            *super::inputStream,
            super::bufferStart,
            super::bufferEnd,
            visitor.filter(),
            visitor.hook());
      }
    }
    visitor.setNumValues(hasFilter ? numValues : numRows);
  }
};

} // namespace facebook::velox::dwrf
