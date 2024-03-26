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

#include "velox/common/base/GTestMacros.h"
#include "velox/common/base/Nulls.h"
#include "velox/dwio/common/BitPackDecoder.h"
#include "velox/dwio/common/DecoderUtil.h"
#include "velox/dwio/common/TypeUtil.h"
#include "velox/dwio/parquet/reader/RleBpDecoder.h"

namespace facebook::velox::parquet {

// This class will be used for dictionary Ids or other data that is RLE/BP
// encoded.
class RleBpDataDecoder : public facebook::velox::parquet::RleBpDecoder {
 public:
  using super = facebook::velox::parquet::RleBpDecoder;

  RleBpDataDecoder(const char* start, const char* end, uint8_t bitWidth)
      : super::RleBpDecoder{start, end, bitWidth} {}

  template <bool hasNulls>
  inline void skip(int32_t numValues, int32_t current, const uint64_t* nulls) {
    if (hasNulls) {
      numValues = bits::countNonNulls(nulls, current, current + numValues);
    }

    super::skip(numValues);
  }

  void skip(uint64_t numValues) {
    super::skip(numValues);
  }

  template <bool hasNulls, typename Visitor>
  void readWithVisitor(const uint64_t* nulls, Visitor visitor) {
    if (dwio::common::useFastPath<Visitor, hasNulls>(visitor)) {
      fastPath<hasNulls>(nulls, visitor);
      return;
    }
    int32_t current = visitor.start();
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
            return;
          }
        }

        // We are at a non-null value on a row to visit.
        if (!remainingValues_) {
          readHeader();
        }
        if (repeating_) {
          toSkip = visitor.process(value_, atEnd);
        } else {
          value_ = readBitField();
          toSkip = visitor.process(value_, atEnd);
        }
        --remainingValues_;
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
        !std::is_same_v<typename Visitor::FilterType, common::AlwaysTrue>;
    constexpr bool hasHook =
        !std::is_same_v<typename Visitor::HookType, dwio::common::NoHook>;
    auto rows = visitor.rows();
    auto numRows = visitor.numRows();
    auto rowsAsRange = folly::Range<const int32_t*>(rows, numRows);
    if (hasNulls) {
      raw_vector<int32_t>* innerVector = nullptr;
      auto outerVector = &visitor.outerNonNullRows();
      if (Visitor::dense || rowsAsRange.back() == rowsAsRange.size() - 1) {
        dwio::common::nonNullRowsFromDense(nulls, numRows, *outerVector);
        if (outerVector->empty()) {
          visitor.setAllNull(hasFilter ? 0 : numRows);
          return;
        }
        bulkScan<hasFilter, hasHook, true>(
            folly::Range<const int32_t*>(rows, outerVector->size()),
            outerVector->data(),
            visitor);
      } else {
        innerVector = &visitor.innerNonNullRows();
        int32_t tailSkip = -1;
        auto anyNulls = dwio::common::nonNullRowsFromSparse < hasFilter,
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
        bulkScan<hasFilter, hasHook, true>(
            *innerVector, outerVector->data(), visitor);
        skip<false>(tailSkip, 0, nullptr);
      }
    } else {
      bulkScan<hasFilter, hasHook, false>(rowsAsRange, nullptr, visitor);
    }
  }

  template <bool hasFilter, bool hasHook, bool scatter, typename Visitor>
  void processRun(
      const int32_t* rows,
      int32_t rowIndex,
      int32_t currentRow,
      int32_t numRows,
      const int32_t* scatterRows,
      int32_t* filterHits,
      typename Visitor::DataType* values,
      int32_t& numValues,
      Visitor& visitor) {
    auto numBits = bitOffset_ +
        (rows[rowIndex + numRows - 1] + 1 - currentRow) * bitWidth_;

    using TValues = typename std::remove_reference<decltype(values[0])>::type;
    using TIndex = typename std::make_signed_t<
        typename dwio::common::make_index<TValues>::type>;
    facebook::velox::dwio::common::unpack(
        reinterpret_cast<const uint64_t*>(super::bufferStart_),
        bitOffset_,
        folly::Range<const int32_t*>(rows + rowIndex, numRows),
        currentRow,
        bitWidth_,
        super::bufferEnd_,
        reinterpret_cast<TIndex*>(values) + numValues);
    super::bufferStart_ += numBits >> 3;
    bitOffset_ = numBits & 7;
    visitor.template processRun<hasFilter, hasHook, scatter>(
        values + numValues,
        numRows,
        scatterRows,
        filterHits,
        values,
        numValues);
  }

  // Returns 1. how many of 'rows' are in the current run 2. the
  // distance in rows from the current row to the first row after the
  // last in rows that falls in the current run.
  template <bool dense>
  std::pair<int32_t, std::int32_t> findNumInRun(
      const int32_t* rows,
      int32_t rowIndex,
      int32_t numRows,
      int32_t currentRow) {
    DCHECK_LT(rowIndex, numRows);
    if (dense) {
      auto left = std::min<int32_t>(remainingValues_, numRows - rowIndex);
      return std::make_pair(left, left);
    }
    if (rows[rowIndex] - currentRow >= remainingValues_) {
      return std::make_pair(0, 0);
    }
    if (rows[numRows - 1] - currentRow < remainingValues_) {
      return std::pair(numRows - rowIndex, rows[numRows - 1] - currentRow + 1);
    }
    auto range = folly::Range<const int32_t*>(
        rows + rowIndex,
        std::min<int32_t>(remainingValues_, numRows - rowIndex));
    auto endOfRun = currentRow + remainingValues_;
    auto bound = std::lower_bound(range.begin(), range.end(), endOfRun);
    return std::make_pair(bound - range.begin(), bound[-1] - currentRow + 1);
  }

  template <bool hasFilter, bool hasHook, bool scatter, typename Visitor>
  void bulkScan(
      folly::Range<const int32_t*> nonNullRows,
      const int32_t* scatterRows,
      Visitor& visitor) {
    auto numAllRows = visitor.numRows();
    visitor.setRows(nonNullRows);
    auto rows = visitor.rows();
    auto numRows = visitor.numRows();
    auto rowIndex = 0;
    int32_t currentRow = 0;
    auto values = visitor.rawValues(numRows);
    auto filterHits = hasFilter ? visitor.outputRows(numRows) : nullptr;
    int32_t numValues = 0;
    for (;;) {
      if (remainingValues_) {
        auto [numInRun, numAdvanced] =
            findNumInRun<Visitor::dense>(rows, rowIndex, numRows, currentRow);
        if (!numInRun) {
          // We are not at end and the next row of interest is after this run.
          VELOX_CHECK(!numAdvanced, "Would advance past end of RLEv1 run");
        } else if (repeating_) {
          visitor.template processRle<hasFilter, hasHook, scatter>(
              value_,
              0,
              numInRun,
              currentRow,
              scatterRows,
              filterHits,
              values,
              numValues);
        } else {
          processRun<hasFilter, hasHook, scatter>(
              rows,
              rowIndex,
              currentRow,
              numInRun,
              scatterRows,
              filterHits,
              values,
              numValues,
              visitor);
        }
        remainingValues_ -= numAdvanced;
        currentRow += numAdvanced;
        rowIndex += numInRun;
        if (visitor.atEnd()) {
          visitor.setNumValues(hasFilter ? numValues : numAllRows);
          return;
        }
        if (remainingValues_) {
          currentRow += remainingValues_;
          skip<false>(remainingValues_, -1, nullptr);
        }
      }
      readHeader();
    }
  }

  // Loads a bit field from 'ptr' + bitOffset for up to 'bitWidth' bits. makes
  // sure not to access bytes past lastSafeWord + 7.
  static inline uint64_t safeLoadBits(
      const char* ptr,
      int32_t bitOffset,
      uint8_t bitWidth,
      const char* lastSafeWord) {
    VELOX_DCHECK_GE(7, bitOffset);
    VELOX_DCHECK_GE(56, bitWidth);
    if (ptr < lastSafeWord) {
      return *reinterpret_cast<const uint64_t*>(ptr) >> bitOffset;
    }
    int32_t byteWidth = bits::roundUp(bitOffset + bitWidth, 8) / 8;
    return bits::loadPartialWord(
               reinterpret_cast<const uint8_t*>(ptr), byteWidth) >>
        bitOffset;
  }

  // Reads one value of 'bitWithd_' bits and advances the position.
  int64_t readBitField() {
    auto value =
        safeLoadBits(
            super::bufferStart_, bitOffset_, bitWidth_, lastSafeWord_) &
        bitMask_;
    bitOffset_ += bitWidth_;
    super::bufferStart_ += bitOffset_ >> 3;
    bitOffset_ &= 7;
    return value;
  }
};

} // namespace facebook::velox::parquet
