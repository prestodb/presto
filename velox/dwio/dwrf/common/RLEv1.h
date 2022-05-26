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
#include "velox/dwio/dwrf/common/Adaptor.h"
#include "velox/dwio/dwrf/common/DecoderUtil.h"
#include "velox/dwio/dwrf/common/IntDecoder.h"
#include "velox/dwio/dwrf/common/IntEncoder.h"

#include <memory>

namespace facebook::velox::dwrf {

template <bool isSigned>
class RleEncoderV1 : public IntEncoder<isSigned> {
 public:
  RleEncoderV1(
      std::unique_ptr<BufferedOutputStream> outStream,
      bool useVInts,
      uint32_t numBytes)
      : IntEncoder<isSigned>{std::move(outStream), useVInts, numBytes},
        numLiterals{0},
        delta{0},
        repeat{false},
        tailRunLength{0},
        isOverflow{false} {}

  // For 64 bit Integers, only signed type is supported. writeVuLong only
  // supports int64_t and it needs to support uint64_t before this method
  // can support uint64_t overload.
  uint64_t add(const int64_t* data, const Ranges& ranges, const uint64_t* nulls)
      override {
    return addImpl(data, ranges, nulls);
  }

  uint64_t add(const int32_t* data, const Ranges& ranges, const uint64_t* nulls)
      override {
    return addImpl(data, ranges, nulls);
  }

  uint64_t add(
      const uint32_t* data,
      const Ranges& ranges,
      const uint64_t* nulls) override {
    return addImpl(data, ranges, nulls);
  }

  uint64_t add(const int16_t* data, const Ranges& ranges, const uint64_t* nulls)
      override {
    return addImpl(data, ranges, nulls);
  }

  uint64_t add(
      const uint16_t* data,
      const Ranges& ranges,
      const uint64_t* nulls) override {
    return addImpl(data, ranges, nulls);
  }

  void writeValue(const int64_t value) override {
    write(value);
  }

  uint64_t flush() override {
    writeValues();
    return IntEncoder<isSigned>::flush();
  }

  void recordPosition(PositionRecorder& recorder, int32_t strideIndex = -1)
      const override {
    IntEncoder<isSigned>::recordPosition(recorder, strideIndex);
    recorder.add(static_cast<uint64_t>(numLiterals), strideIndex);
  }

 private:
  constexpr static int32_t MAX_DELTA = 127;
  constexpr static int32_t MIN_DELTA = -128;

  std::array<int64_t, RLE_MAX_LITERAL_SIZE> literals;
  int32_t numLiterals;
  int64_t delta;
  bool repeat;
  int32_t tailRunLength;
  bool isOverflow;

  template <typename T>
  void write(T value) {
    if (numLiterals == 0) {
      // Starting new sequence of run or literals.
      literals[numLiterals++] = value;
      tailRunLength = 1;
      return;
    }

    if (repeat) {
      if (isRunRepeating(value)) {
        numLiterals += 1;
        if (numLiterals == RLE_MAXIMUM_REPEAT) {
          writeValues();
        }
      } else {
        writeValues();
        literals[numLiterals++] = value;
        tailRunLength = 1;
      }
      return;
    }

    if (tailRunLength == 1) {
      computeDeltaAndTailRunLength(value);
    } else if (isRunStarting(value)) {
      tailRunLength += 1;
    } else {
      computeDeltaAndTailRunLength(value);
    }
    if (tailRunLength == RLE_MINIMUM_REPEAT) {
      if (numLiterals + 1 == RLE_MINIMUM_REPEAT) {
        numLiterals += 1;
      } else {
        numLiterals -= (RLE_MINIMUM_REPEAT - 1);
        int64_t base = literals[numLiterals];
        writeValues();
        literals[0] = base;
        numLiterals = RLE_MINIMUM_REPEAT;
      }
      // set repeat, so that next call call to write can be special cased.
      repeat = true;
    } else {
      literals[numLiterals++] = value;
      if (numLiterals == RLE_MAX_LITERAL_SIZE) {
        writeValues();
      }
    }
  }

  void writeValues();

  template <typename T>
  uint64_t addImpl(const T* data, const Ranges& ranges, const uint64_t* nulls);

  template <typename Integer>
  FOLLY_ALWAYS_INLINE bool isRunRepeating(const Integer& value) {
    if constexpr (sizeof(Integer) == sizeof(delta)) {
      int64_t nextRunValue;
      isOverflow = __builtin_add_overflow(
          literals[0], delta * numLiterals, &nextRunValue);
      return value == nextRunValue && !isOverflow;
    } else {
      return value == literals[0] + delta * numLiterals;
    }
  }

  template <typename Integer>
  FOLLY_ALWAYS_INLINE bool isRunStarting(const Integer& value) {
    if constexpr (sizeof(Integer) == sizeof(delta)) {
      int64_t nextRunValue;
      isOverflow = __builtin_add_overflow(
          literals[numLiterals - 1], delta, &nextRunValue);
      return value == nextRunValue && !isOverflow;
    } else {
      return value == literals[numLiterals - 1] + delta;
    }
  }

  template <typename Integer>
  FOLLY_ALWAYS_INLINE void computeDeltaAndTailRunLength(const Integer& value) {
    if constexpr (sizeof(Integer) == sizeof(delta)) {
      isOverflow =
          __builtin_sub_overflow(value, literals[numLiterals - 1], &delta);
      if (UNLIKELY(isOverflow)) {
        tailRunLength = 1;
        return;
      }
    } else {
      delta = value - literals[numLiterals - 1];
    }

    if (delta < MIN_DELTA || delta > MAX_DELTA) {
      tailRunLength = 1;
    } else {
      tailRunLength = 2;
    }
  }

  VELOX_FRIEND_TEST(RleEncoderV1Test, encodeMinAndMax);
  VELOX_FRIEND_TEST(RleEncoderV1Test, encodeMinAndMaxint32);
};

template <bool isSigned>
template <typename T>
uint64_t RleEncoderV1<isSigned>::addImpl(
    const T* data,
    const Ranges& ranges,
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

struct DropValues;

template <bool isSigned>
class RleDecoderV1 : public IntDecoder<isSigned> {
 public:
  using super = IntDecoder<isSigned>;

  RleDecoderV1(
      std::unique_ptr<SeekableInputStream> input,
      bool useVInts,
      uint32_t numBytes)
      : IntDecoder<isSigned>{std::move(input), useVInts, numBytes},
        remainingValues(0),
        value(0),
        delta(0),
        repeating(false) {}

  void seekToRowGroup(PositionProvider& positionProvider) override;

  void skip(uint64_t numValues) override;

  void next(int64_t* data, uint64_t numValues, const uint64_t* nulls) override;

  void nextLengths(int32_t* data, int32_t numValues) override;

  template <bool hasNulls>
  inline void skip(int32_t numValues, int32_t current, const uint64_t* nulls) {
    if (hasNulls) {
      numValues = bits::countNonNulls(nulls, current, current + numValues);
    }
    while (numValues > 0) {
      if (remainingValues == 0) {
        readHeader();
      }
      uint64_t count = std::min<int>(numValues, remainingValues);
      remainingValues -= count;
      numValues -= count;
      if (repeating) {
        value += delta * static_cast<int64_t>(count);
      } else {
        IntDecoder<isSigned>::skipLongsFast(count);
      }
    }
  }

  template <bool hasNulls, typename Visitor>
  void readWithVisitor(const uint64_t* nulls, Visitor visitor) {
    if (useFastPath<Visitor, hasNulls>(visitor)) {
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
        if (!remainingValues) {
          readHeader();
        }
        if (repeating) {
          toSkip = visitor.process(value, atEnd);
          value += delta;
        } else {
          value = IntDecoder<isSigned>::readLong();
          toSkip = visitor.process(value, atEnd);
        }
        --remainingValues;
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
    constexpr bool hasHook =
        !std::is_same<typename Visitor::HookType, NoHook>::value;
    auto rows = visitor.rows();
    auto numRows = visitor.numRows();
    auto rowsAsRange = folly::Range<const int32_t*>(rows, numRows);
    if (hasNulls) {
      raw_vector<int32_t>* innerVector = nullptr;
      auto outerVector = &visitor.outerNonNullRows();
      if (Visitor::dense) {
        nonNullRowsFromDense(nulls, numRows, *outerVector);
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
      auto left = std::min<int32_t>(remainingValues, numRows - rowIndex);
      return std::make_pair(left, left);
    }
    if (rows[rowIndex] - currentRow >= remainingValues) {
      return std::make_pair(0, 0);
    }
    if (rows[numRows - 1] - currentRow < remainingValues) {
      return std::pair(numRows - rowIndex, rows[numRows - 1] - currentRow + 1);
    }
    auto range = folly::Range<const int32_t*>(
        rows + rowIndex,
        std::min<int32_t>(remainingValues, numRows - rowIndex));
    auto endOfRun = currentRow + remainingValues;
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
      if (remainingValues) {
        auto [numInRun, numAdvanced] =
            findNumInRun<Visitor::dense>(rows, rowIndex, numRows, currentRow);
        if (!numInRun) {
          // We are not at end and the next row of interest is after this run.
          VELOX_CHECK(!numAdvanced, "Would advance past end of RLEv1 run");
        } else if (repeating) {
          visitor.template processRle<hasFilter, hasHook, scatter>(
              value,
              delta,
              numInRun,
              currentRow,
              scatterRows,
              filterHits,
              values,
              numValues);
          value += numAdvanced * delta;
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
        remainingValues -= numAdvanced;
        currentRow += numAdvanced;
        rowIndex += numInRun;
        if (visitor.atEnd()) {
          visitor.setNumValues(hasFilter ? numValues : numAllRows);
          return;
        }
        if (remainingValues) {
          currentRow += remainingValues;
          skip<false>(remainingValues, -1, nullptr);
        }
      }
      readHeader();
    }
  }

  inline void readHeader() {
    signed char ch = IntDecoder<isSigned>::readByte();
    if (ch < 0) {
      remainingValues = static_cast<uint64_t>(-ch);
      repeating = false;
    } else {
      remainingValues = static_cast<uint64_t>(ch) + RLE_MINIMUM_REPEAT;
      repeating = true;
      delta = IntDecoder<isSigned>::readByte();
      value = IntDecoder<isSigned>::readLong();
    }
  }

  uint64_t remainingValues;
  int64_t value;
  int64_t delta;
  bool repeating;
};

} // namespace facebook::velox::dwrf
