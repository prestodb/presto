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
#include "velox/dwio/common/Adaptor.h"
#include "velox/dwio/common/DecoderUtil.h"
#include "velox/dwio/common/IntDecoder.h"
#include "velox/dwio/dwrf/common/IntEncoder.h"

#include <memory>

namespace facebook::velox::dwrf {

using dwio::common::BufferedOutputStream;
using dwio::common::PositionRecorder;

template <bool isSigned>
class RleEncoderV1 : public IntEncoder<isSigned> {
 public:
  RleEncoderV1(
      std::unique_ptr<BufferedOutputStream> outStream,
      bool useVInts,
      uint32_t numBytes)
      : IntEncoder<isSigned>{std::move(outStream), useVInts, numBytes},
        numLiterals_{0},
        delta_{0},
        repeat_{false},
        tailRunLength_{0},
        overflow_{false} {}

  /// For 64 bit Integers, only signed type is supported. writeVuLong only
  /// supports int64_t and it needs to support uint64_t before this method
  /// can support uint64_t overload.
  uint64_t add(
      const int64_t* data,
      const common::Ranges& ranges,
      const uint64_t* nulls) override {
    return addImpl(data, ranges, nulls);
  }

  uint64_t add(
      const int32_t* data,
      const common::Ranges& ranges,
      const uint64_t* nulls) override {
    return addImpl(data, ranges, nulls);
  }

  uint64_t add(
      const uint32_t* data,
      const common::Ranges& ranges,
      const uint64_t* nulls) override {
    return addImpl(data, ranges, nulls);
  }

  uint64_t add(
      const int16_t* data,
      const common::Ranges& ranges,
      const uint64_t* nulls) override {
    return addImpl(data, ranges, nulls);
  }

  uint64_t add(
      const uint16_t* data,
      const common::Ranges& ranges,
      const uint64_t* nulls) override {
    return addImpl(data, ranges, nulls);
  }

  void writeValue(int64_t value) override {
    write(value);
  }

  uint64_t flush() override {
    writeValues();
    return IntEncoder<isSigned>::flush();
  }

  void recordPosition(PositionRecorder& recorder, int32_t strideIndex = -1)
      const override {
    IntEncoder<isSigned>::recordPosition(recorder, strideIndex);
    recorder.add(static_cast<uint64_t>(numLiterals_), strideIndex);
  }

 private:
  constexpr static int32_t MAX_DELTA = 127;
  constexpr static int32_t MIN_DELTA = -128;

  template <typename T>
  void write(T value) {
    if (numLiterals_ == 0) {
      // Starting new sequence of run or literals.
      literals_[numLiterals_++] = value;
      tailRunLength_ = 1;
      return;
    }

    if (repeat_) {
      if (isRunRepeating(value)) {
        ++numLiterals_;
        if (numLiterals_ == RLE_MAXIMUM_REPEAT) {
          writeValues();
        }
      } else {
        writeValues();
        literals_[numLiterals_++] = value;
        tailRunLength_ = 1;
      }
      return;
    }

    if (tailRunLength_ == 1) {
      computeDeltaAndTailRunLength(value);
    } else if (isRunStarting(value)) {
      ++tailRunLength_;
    } else {
      computeDeltaAndTailRunLength(value);
    }

    if (tailRunLength_ == RLE_MINIMUM_REPEAT) {
      if (numLiterals_ + 1 == RLE_MINIMUM_REPEAT) {
        ++numLiterals_;
      } else {
        numLiterals_ -= (RLE_MINIMUM_REPEAT - 1);
        const int64_t base = literals_[numLiterals_];
        writeValues();
        literals_[0] = base;
        numLiterals_ = RLE_MINIMUM_REPEAT;
      }
      // Set repeat, so that next call call to write can be special cased.
      repeat_ = true;
    } else {
      literals_[numLiterals_++] = value;
      if (numLiterals_ == RLE_MAX_LITERAL_SIZE) {
        writeValues();
      }
    }
  }

  void writeValues();

  template <typename T>
  uint64_t
  addImpl(const T* data, const common::Ranges& ranges, const uint64_t* nulls);

  template <typename Integer>
  FOLLY_ALWAYS_INLINE bool isRunRepeating(const Integer& value) {
    if constexpr (sizeof(Integer) == sizeof(delta_)) {
      int64_t nextRunValue;
      overflow_ = __builtin_add_overflow(
          literals_[0], delta_ * numLiterals_, &nextRunValue);
      return value == nextRunValue && !overflow_;
    } else {
      return value == literals_[0] + delta_ * numLiterals_;
    }
  }

  template <typename Integer>
  FOLLY_ALWAYS_INLINE bool isRunStarting(const Integer& value) {
    if constexpr (sizeof(Integer) == sizeof(delta_)) {
      int64_t nextRunValue;
      overflow_ = __builtin_add_overflow(
          literals_[numLiterals_ - 1], delta_, &nextRunValue);
      return value == nextRunValue && !overflow_;
    } else {
      return value == literals_[numLiterals_ - 1] + delta_;
    }
  }

  template <typename Integer>
  FOLLY_ALWAYS_INLINE void computeDeltaAndTailRunLength(const Integer& value) {
    if constexpr (sizeof(Integer) == sizeof(delta_)) {
      overflow_ =
          __builtin_sub_overflow(value, literals_[numLiterals_ - 1], &delta_);
      if (UNLIKELY(overflow_)) {
        tailRunLength_ = 1;
        return;
      }
    } else {
      delta_ = value - literals_[numLiterals_ - 1];
    }

    if (delta_ < MIN_DELTA || delta_ > MAX_DELTA) {
      tailRunLength_ = 1;
    } else {
      tailRunLength_ = 2;
    }
  }

  std::array<int64_t, RLE_MAX_LITERAL_SIZE> literals_;
  int32_t numLiterals_;
  int64_t delta_;
  bool repeat_;
  int32_t tailRunLength_;
  bool overflow_;

  VELOX_FRIEND_TEST(RleEncoderV1Test, encodeMinAndMax);
  VELOX_FRIEND_TEST(RleEncoderV1Test, encodeMinAndMaxint32);
};

template <bool isSigned>
template <typename T>
uint64_t RleEncoderV1<isSigned>::addImpl(
    const T* data,
    const common::Ranges& ranges,
    const uint64_t* nulls) {
  uint64_t count = 0;
  if (nulls) {
    for (auto& pos : ranges) {
      if (!bits::isBitNull(nulls, pos)) {
        write(data[pos]);
        ++count;
      }
    }
  } else {
    for (auto& pos : ranges) {
      write(data[pos]);
      ++count;
    }
  }
  return count;
}

template <bool isSigned>
class RleDecoderV1 : public dwio::common::IntDecoder<isSigned> {
 public:
  using super = dwio::common::IntDecoder<isSigned>;

  RleDecoderV1(
      std::unique_ptr<dwio::common::SeekableInputStream> input,
      bool useVInts,
      uint32_t numBytes)
      : dwio::common::IntDecoder<
            isSigned>{std::move(input), useVInts, numBytes},
        remainingValues_(0),
        value_(0),
        delta_(0),
        repeating_(false) {}

  void seekToRowGroup(
      dwio::common::PositionProvider& positionProvider) override;

  void next(int64_t* data, uint64_t numValues, const uint64_t* nulls) override;

  void nextLengths(int32_t* data, int32_t numValues) override;

  template <bool hasNulls, typename Visitor>
  void readWithVisitor(const uint64_t* nulls, Visitor visitor) {
    skipPending();
    if (dwio::common::useFastPath<Visitor, hasNulls>(visitor)) {
      fastPath<hasNulls>(nulls, visitor);
      return;
    }

    int32_t current = visitor.start();
    this->template skip<hasNulls>(current, 0, nulls);
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
            this->template skip<false>(toSkip, current, nullptr);
          }
          if (atEnd) {
            return;
          }
        }

        // We are at a non-null value on a row to visit.
        if (remainingValues_ == 0) {
          readHeader();
        }
        if (repeating_) {
          toSkip = visitor.process(value_, atEnd);
          value_ += delta_;
        } else {
          value_ =
              dwio::common::IntDecoder<isSigned>::template readInt<int64_t>();
          toSkip = visitor.process(value_, atEnd);
        }
        --remainingValues_;
      }
      ++current;
      if (toSkip > 0) {
        this->template skip<hasNulls>(toSkip, current, nulls);
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
      if (Visitor::dense) {
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
          this->template skip<false>(tailSkip, 0, nullptr);
          visitor.setAllNull(hasFilter ? 0 : numRows);
          return;
        }
        bulkScan<hasFilter, hasHook, true>(
            *innerVector, outerVector->data(), visitor);
        this->template skip<false>(tailSkip, 0, nullptr);
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
    if (Visitor::dense) {
      super::bulkRead(numRows, values + numValues);
    } else {
      super::bulkReadRows(
          folly::Range<const int32_t*>(rows + rowIndex, numRows),
          values + numValues,
          currentRow);
    }
    visitor.template processRun<hasFilter, hasHook, scatter>(
        values + numValues,
        numRows,
        scatterRows,
        filterHits,
        values,
        numValues);
  }

  /// Returns 1. how many of 'rows' are in the current run; 2. the distance in
  /// rows from the current row to the first row after the last in rows that
  /// falls in the current run.
  template <bool dense>
  std::pair<int32_t, std::int32_t> findNumInRun(
      const int32_t* rows,
      int32_t rowIndex,
      int32_t numRows,
      int32_t currentRow) {
    VELOX_DCHECK_LT(rowIndex, numRows);
    if (dense) {
      const auto left = std::min<int32_t>(remainingValues_, numRows - rowIndex);
      return std::make_pair(left, left);
    }

    if (rows[rowIndex] - currentRow >= remainingValues_) {
      return std::make_pair(0, 0);
    }

    if (rows[numRows - 1] - currentRow < remainingValues_) {
      return std::pair(numRows - rowIndex, rows[numRows - 1] - currentRow + 1);
    }

    const auto range = folly::Range<const int32_t*>(
        rows + rowIndex,
        std::min<int32_t>(remainingValues_, numRows - rowIndex));
    const auto endOfRun = currentRow + remainingValues_;
    const auto bound = std::lower_bound(range.begin(), range.end(), endOfRun);
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
      if (remainingValues_ > 0) {
        auto [numInRun, numAdvanced] =
            findNumInRun<Visitor::dense>(rows, rowIndex, numRows, currentRow);
        if (!numInRun) {
          // We are not at end and the next row of interest is after this run.
          VELOX_CHECK(!numAdvanced, "Would advance past end of RLEv1 run");
        } else if (repeating_) {
          visitor.template processRle<hasFilter, hasHook, scatter>(
              value_,
              delta_,
              numInRun,
              currentRow,
              scatterRows,
              filterHits,
              values,
              numValues);
          value_ += numAdvanced * delta_;
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
        if (remainingValues_ > 0) {
          currentRow += remainingValues_;
          this->template skip<false>(remainingValues_, -1, nullptr);
        }
      }
      readHeader();
    }
  }

  inline void readHeader() {
    const signed char ch = dwio::common::IntDecoder<isSigned>::readByte();
    if (ch < 0) {
      remainingValues_ = static_cast<uint64_t>(-ch);
      repeating_ = false;
    } else {
      remainingValues_ = static_cast<uint64_t>(ch) + RLE_MINIMUM_REPEAT;
      repeating_ = true;
      delta_ = dwio::common::IntDecoder<isSigned>::readByte();
      value_ = dwio::common::IntDecoder<isSigned>::template readInt<int64_t>();
    }
  }

  void skipPending() override;

  uint64_t remainingValues_;
  int64_t value_;
  int64_t delta_;
  bool repeating_;
};

} // namespace facebook::velox::dwrf
