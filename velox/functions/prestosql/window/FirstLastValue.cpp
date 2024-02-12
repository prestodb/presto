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

#include "velox/common/base/Exceptions.h"
#include "velox/exec/WindowFunction.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::window::prestosql {

namespace {

enum class ValueType {
  kFirst,
  kLast,
};

template <ValueType TValue>
class FirstLastValueFunction : public exec::WindowFunction {
 public:
  explicit FirstLastValueFunction(
      const std::vector<exec::WindowFunctionArg>& args,
      const TypePtr& resultType,
      bool ignoreNulls,
      velox::memory::MemoryPool* pool)
      : WindowFunction(resultType, pool, nullptr), ignoreNulls_(ignoreNulls) {
    VELOX_CHECK_NULL(args[0].constantValue);
    valueIndex_ = args[0].index.value();

    nulls_ = allocateNulls(0, pool_);
  }

  void resetPartition(const exec::WindowPartition* partition) override {
    partition_ = partition;
  }

  void apply(
      const BufferPtr& /*peerGroupStarts*/,
      const BufferPtr& /*peerGroupEnds*/,
      const BufferPtr& frameStarts,
      const BufferPtr& frameEnds,
      const SelectivityVector& validRows,
      int32_t resultOffset,
      const VectorPtr& result) override {
    auto numRows = frameStarts->size() / sizeof(vector_size_t);
    rowNumbers_.resize(numRows);
    if (validRows.hasSelections()) {
      if (ignoreNulls_) {
        setRowNumbersIgnoreNulls(validRows, frameStarts, frameEnds);
      } else {
        setRowNumbersRespectNulls(validRows, frameStarts, frameEnds);
      }
    }

    setRowNumbersForEmptyFrames(validRows);
    auto rowNumbersRange = folly::Range(rowNumbers_.data(), numRows);
    partition_->extractColumn(
        valueIndex_, rowNumbersRange, resultOffset, result);
  }

 private:
  void setRowNumbersForEmptyFrames(const SelectivityVector& validRows) {
    if (validRows.isAllSelected()) {
      return;
    }
    // Rows with empty (not-valid) frames have nullptr in the result.
    // So mark rowNumber to copy as kNullRow for it.
    invalidRows_.resizeFill(validRows.size(), true);
    invalidRows_.deselect(validRows);
    invalidRows_.applyToSelected([&](auto i) { rowNumbers_[i] = kNullRow; });
  }

  void setRowNumbersRespectNulls(
      const SelectivityVector& validRows,
      const BufferPtr& frameStarts,
      const BufferPtr& frameEnds) {
    if constexpr (TValue == ValueType::kFirst) {
      auto rawFrameStarts = frameStarts->as<vector_size_t>();
      validRows.applyToSelected(
          [&](auto i) { rowNumbers_[i] = rawFrameStarts[i]; });
    } else {
      auto rawFrameEnds = frameEnds->as<vector_size_t>();
      validRows.applyToSelected(
          [&](auto i) { rowNumbers_[i] = rawFrameEnds[i]; });
    }
  }

  void setRowNumbersIgnoreNulls(
      const SelectivityVector& validRows,
      const BufferPtr& frameStarts,
      const BufferPtr& frameEnds) {
    auto extractNullsResult = partition_->extractNulls(
        valueIndex_, validRows, frameStarts, frameEnds, &nulls_);
    if (!extractNullsResult.has_value()) {
      // There are no nulls in the column. Continue the processing with the
      // function that respects nulls since it is more efficient.
      return setRowNumbersRespectNulls(validRows, frameStarts, frameEnds);
    }

    auto leastFrame = extractNullsResult->first;
    auto frameSize = extractNullsResult->second;

    // first(last)Value functions return the first(last) non-null values for the
    // frame. Negate the bits in nulls_ so that nonNull bits are set instead.
    bits::negate(nulls_->asMutable<char>(), frameSize);
    auto rawNonNulls = nulls_->as<uint64_t>();

    auto rawFrameStarts = frameStarts->as<vector_size_t>();
    auto rawFrameEnds = frameEnds->as<vector_size_t>();
    validRows.applyToSelected([&](auto i) {
      auto frameStart = rawFrameStarts[i];
      auto frameEnd = rawFrameEnds[i];
      // bits::findFirst(Last)Bit returns -1 if a set bit is not found.
      // The function returns null for this case. -1 correctly maps to
      // kNullRow as expected for rowNumbers_ extraction.
      if constexpr (TValue == ValueType::kFirst) {
        auto position = bits::findFirstBit(
            rawNonNulls, frameStart - leastFrame, frameEnd - leastFrame + 1);
        rowNumbers_[i] = (position == -1) ? -1 : position + leastFrame;
      } else {
        auto position = bits::findLastBit(
            rawNonNulls, frameStart - leastFrame, frameEnd - leastFrame + 1);
        rowNumbers_[i] = (position == -1) ? -1 : position + leastFrame;
      }
    });
  }

  const bool ignoreNulls_;

  // Index of the first_value / last_value argument column in the input row
  // vector. This is used to retrieve column values from the partition data.
  column_index_t valueIndex_;

  const exec::WindowPartition* partition_;

  // The first_value, last_value functions directly write from the input column
  // to the resultVector using the extractColumn API specifying the rowNumber
  // mapping to copy between the 2 vectors. This variable is used for the
  // rowNumber vector across getOutput calls.
  std::vector<vector_size_t> rowNumbers_;

  // Used to extract nulls positions for the input value column if ignoreNulls
  // is set.
  BufferPtr nulls_;

  // Member variable re-used for setting null for empty frames.
  SelectivityVector invalidRows_;
};
} // namespace

template <ValueType TValue>
void registerFirstLastInternal(const std::string& name) {
  // T -> T
  std::vector<exec::FunctionSignaturePtr> signatures{
      exec::FunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("T")
          .argumentType("T")
          .build(),
  };

  exec::registerWindowFunction(
      name,
      std::move(signatures),
      [](const std::vector<exec::WindowFunctionArg>& args,
         const TypePtr& resultType,
         bool ignoreNulls,
         velox::memory::MemoryPool* pool,
         HashStringAllocator* /*stringAllocator*/,
         const velox::core::QueryConfig& /*queryConfig*/)
          -> std::unique_ptr<exec::WindowFunction> {
        return std::make_unique<FirstLastValueFunction<TValue>>(
            args, resultType, ignoreNulls, pool);
      });
}

void registerFirstValue(const std::string& name) {
  registerFirstLastInternal<ValueType::kFirst>(name);
}
void registerLastValue(const std::string& name) {
  registerFirstLastInternal<ValueType::kLast>(name);
}
} // namespace facebook::velox::window::prestosql
