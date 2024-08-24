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

namespace facebook::velox::functions::window {

namespace {
class NthValueFunction : public exec::WindowFunction {
 public:
  explicit NthValueFunction(
      const std::vector<exec::WindowFunctionArg>& args,
      const TypePtr& resultType,
      bool ignoreNulls,
      velox::memory::MemoryPool* pool)
      : WindowFunction(resultType, pool, nullptr), ignoreNulls_(ignoreNulls) {
    VELOX_CHECK_EQ(args.size(), 2);
    VELOX_CHECK_NULL(args[0].constantValue);
    auto offsetType = args[1].type;
    VELOX_USER_CHECK(
        (offsetType->isInteger() || offsetType->isBigint()),
        "Invalid offset type: {}",
        offsetType->toString());
    valueIndex_ = args[0].index.value();
    if (args[1].constantValue) {
      if (args[1].constantValue->isNullAt(0)) {
        isConstantOffsetNull_ = true;
        return;
      }
      if (offsetType->isInteger()) {
        constantOffset_ =
            args[1]
                .constantValue->template as<ConstantVector<int32_t>>()
                ->valueAt(0);
      } else {
        constantOffset_ =
            args[1]
                .constantValue->template as<ConstantVector<int64_t>>()
                ->valueAt(0);
      }
      VELOX_USER_CHECK_GE(
          constantOffset_.value(), 1, "Offset must be at least 1");
    } else {
      offsetIndex_ = args[1].index.value();
      if (offsetType->isInteger()) {
        offsets_ = BaseVector::create<FlatVector<int32_t>>(INTEGER(), 0, pool);
      } else {
        offsets_ = BaseVector::create<FlatVector<int64_t>>(BIGINT(), 0, pool);
      }
    }

    nulls_ = allocateNulls(0, pool);
  }

  void resetPartition(const exec::WindowPartition* partition) override {
    partition_ = partition;
    partitionOffset_ = 0;
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

    if (isConstantOffsetNull_) {
      std::fill(rowNumbers_.begin(), rowNumbers_.end(), kNullRow);
    } else {
      if (validRows.hasSelections()) {
        setRowNumbers(validRows, frameStarts, frameEnds, numRows);
      }
    }

    setRowNumbersForEmptyFrames(validRows);

    auto rowNumbersRange = folly::Range(rowNumbers_.data(), numRows);
    partition_->extractColumn(
        valueIndex_, rowNumbersRange, resultOffset, result);

    partitionOffset_ += numRows;
  }

 private:
  // The below functions build the rowNumbers for column extraction.
  // The rowNumbers map for each output row, as per nth_value function
  // semantics, the rowNumber (relative to the start of the partition) from
  // which the input value should be copied.
  // A rowNumber of kNullRow is for nullptr in the result.
  void setRowNumbers(
      const SelectivityVector& validRows,
      const BufferPtr& frameStarts,
      const BufferPtr& frameEnds,
      vector_size_t numRows) {
    auto rawFrameStarts = frameStarts->as<vector_size_t>();
    auto rawFrameEnds = frameEnds->as<vector_size_t>();
    bool ignoreNullsForBlock = false;
    vector_size_t leastFrame = 0;
    if (ignoreNulls_) {
      auto extractNullsResult = partition_->extractNulls(
          valueIndex_, validRows, frameStarts, frameEnds, &nulls_);
      // Perform ignoreNulls processing only if there are null values in the
      // current output block. Otherwise continue processing with the logic
      // without as it is more efficient.
      ignoreNullsForBlock = extractNullsResult.has_value();
      leastFrame = ignoreNullsForBlock ? extractNullsResult->first : leastFrame;
    }

    if (constantOffset_.has_value()) {
      setRowNumbersForConstantOffset(
          ignoreNullsForBlock,
          validRows,
          rawFrameStarts,
          rawFrameEnds,
          leastFrame);
    } else {
      setRowNumbersForColumnOffset(
          ignoreNullsForBlock,
          numRows,
          validRows,
          rawFrameStarts,
          rawFrameEnds,
          leastFrame);
    }
  }

  void setRowNumbersForConstantOffset(
      bool ignoreNulls,
      const SelectivityVector& validRows,
      const vector_size_t* frameStarts,
      const vector_size_t* frameEnds,
      vector_size_t leastFrame) {
    auto constantOffsetValue = constantOffset_.value();
    if (ignoreNulls) {
      auto rawNulls = nulls_->as<uint64_t>();
      validRows.applyToSelected([&](auto i) {
        setRowNumberIgnoreNulls(
            i,
            rawNulls,
            leastFrame,
            frameStarts,
            frameEnds,
            constantOffsetValue);
      });
    } else {
      validRows.applyToSelected([&](auto i) {
        setRowNumber(i, frameStarts, frameEnds, constantOffsetValue);
      });
    }
  }

  template <bool ignoreNulls, typename T>
  void setRowNumbersApplyLoop(
      const SelectivityVector& validRows,
      const vector_size_t* frameStarts,
      const vector_size_t* frameEnds,
      vector_size_t leastFrame = 0) {
    auto rawNulls = nulls_->as<uint64_t>();
    auto offsetsVector = offsets_->as<FlatVector<T>>();
    validRows.applyToSelected([&](auto i) {
      if (offsetsVector->isNullAt(i)) {
        rowNumbers_[i] = kNullRow;
      } else {
        T offset = offsetsVector->valueAt(i);
        VELOX_USER_CHECK_GE(offset, 1, "Offset must be at least 1");
        if constexpr (ignoreNulls) {
          setRowNumberIgnoreNulls(
              i, rawNulls, leastFrame, frameStarts, frameEnds, offset);
        } else {
          setRowNumber(i, frameStarts, frameEnds, offset);
        }
      }
    });
  }

  void setRowNumbersForColumnOffset(
      bool ignoreNulls,
      vector_size_t numRows,
      const SelectivityVector& validRows,
      const vector_size_t* frameStarts,
      const vector_size_t* frameEnds,
      vector_size_t leastFrame) {
    offsets_->resize(numRows);
    partition_->extractColumn(
        offsetIndex_, partitionOffset_, numRows, 0, offsets_);

    if (ignoreNulls) {
      if (offsets_->type()->isInteger()) {
        setRowNumbersApplyLoop<true, int32_t>(
            validRows, frameStarts, frameEnds, leastFrame);
      } else {
        setRowNumbersApplyLoop<true, int64_t>(
            validRows, frameStarts, frameEnds, leastFrame);
      }
    } else {
      if (offsets_->type()->isInteger()) {
        setRowNumbersApplyLoop<false, int32_t>(
            validRows, frameStarts, frameEnds);
      } else {
        setRowNumbersApplyLoop<false, int64_t>(
            validRows, frameStarts, frameEnds);
      }
    }
  }

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

  template <typename T>
  inline void setRowNumber(
      vector_size_t i,
      const vector_size_t* frameStarts,
      const vector_size_t* frameEnds,
      T offset) {
    auto frameStart = frameStarts[i];
    auto frameEnd = frameEnds[i];
    auto rowNumber = frameStart + offset - 1;
    rowNumbers_[i] = rowNumber <= frameEnd ? rowNumber : kNullRow;
  }

  template <typename T>
  inline void setRowNumberIgnoreNulls(
      vector_size_t i,
      const uint64_t* rawNulls,
      vector_size_t leastFrame,
      const vector_size_t* frameStarts,
      const vector_size_t* frameEnds,
      T offset) {
    auto frameStart = frameStarts[i];
    auto frameEnd = frameEnds[i];
    T nonNullCount = 0;
    for (auto j = frameStart; j <= frameEnd; j++) {
      if (!bits::isBitSet(rawNulls, j - leastFrame)) {
        ++nonNullCount;
        if (nonNullCount == offset) {
          rowNumbers_[i] = j;
          return;
        }
      }
    }
    rowNumbers_[i] = kNullRow;
  }

  const bool ignoreNulls_;

  // These are the argument indices of the nth_value value and offset columns
  // in the input row vector. These are needed to retrieve column values
  // from the partition data.
  column_index_t valueIndex_;
  column_index_t offsetIndex_;

  const exec::WindowPartition* partition_;

  // These fields are set if the offset argument is a constant value.
  std::optional<int64_t> constantOffset_;
  bool isConstantOffsetNull_ = false;

  // This vector is used to extract values of the offset argument column
  // (if not a constant offset value).
  VectorPtr offsets_ = nullptr;

  // This offset tracks how far along the partition rows have been output.
  // This can be used to optimize reading offset column values corresponding
  // to the present row set in getOutput.
  vector_size_t partitionOffset_;

  // Used to read the null positions of the value column for ignoreNulls.
  BufferPtr nulls_;

  // The function directly writes from the input column to the
  // resultVector using the extractColumn API specifying the rowNumber mapping
  // to copy between the 2 vectors. This variable is used for the rowNumber
  // vector across getOutput calls.
  std::vector<vector_size_t> rowNumbers_;

  // Member variable re-used for setting null for empty frames.
  SelectivityVector invalidRows_;
};
} // namespace

void registerNthValue(const std::string& name, TypeKind offsetTypeKind) {
  std::vector<exec::FunctionSignaturePtr> signatures{
      exec::FunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("T")
          .argumentType("T")
          .argumentType(mapTypeKindToName(offsetTypeKind))
          .build(),
  };

  exec::registerWindowFunction(
      name,
      std::move(signatures),
      exec::WindowFunction::Metadata::defaultMetadata(),
      [name](
          const std::vector<exec::WindowFunctionArg>& args,
          const TypePtr& resultType,
          bool ignoreNulls,
          velox::memory::MemoryPool* pool,
          HashStringAllocator* /*stringAllocator*/,
          const core::QueryConfig& /*queryConfig*/)
          -> std::unique_ptr<exec::WindowFunction> {
        return std::make_unique<NthValueFunction>(
            args, resultType, ignoreNulls, pool);
      });
}

void registerNthValueInteger(const std::string& name) {
  registerNthValue(name, TypeKind::INTEGER);
}

void registerNthValueBigint(const std::string& name) {
  registerNthValue(name, TypeKind::BIGINT);
}
} // namespace facebook::velox::functions::window
