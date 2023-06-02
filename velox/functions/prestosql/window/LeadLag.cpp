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

namespace facebook::velox::window::prestosql {

namespace {

template <bool isLag>
class LeadLagFunction : public exec::WindowFunction {
 public:
  explicit LeadLagFunction(
      const std::vector<exec::WindowFunctionArg>& args,
      const TypePtr& resultType,
      velox::memory::MemoryPool* pool)
      : WindowFunction(resultType, pool, nullptr) {
    valueIndex_ = args[0].index.value();

    initializeOffset(args);
    initializeDefaultValue(args);
  }

  void resetPartition(const exec::WindowPartition* partition) override {
    partition_ = partition;
    partitionOffset_ = 0;
  }

  void apply(
      const BufferPtr& /*peerGroupStarts*/,
      const BufferPtr& /*peerGroupEnds*/,
      const BufferPtr& frameStarts,
      const BufferPtr& /*frameEnds*/,
      const SelectivityVector& /*validRows*/,
      int32_t resultOffset,
      const VectorPtr& result) override {
    const auto numRows = frameStarts->size() / sizeof(vector_size_t);

    rowNumbers_.resize(numRows);

    if (constantOffset_.has_value() || isConstantOffsetNull_) {
      setRowNumbersForConstantOffset();
    } else {
      setRowNumbers(numRows);
    }

    auto rowNumbersRange = folly::Range(rowNumbers_.data(), numRows);
    partition_->extractColumn(
        valueIndex_, rowNumbersRange, resultOffset, result);

    setDefaultValue(result, resultOffset);

    partitionOffset_ += numRows;
  }

 private:
  void initializeOffset(const std::vector<exec::WindowFunctionArg>& args) {
    if (args.size() == 1) {
      constantOffset_ = 1;
      return;
    }

    const auto& offsetArg = args[1];
    if (auto constantOffset = offsetArg.constantValue) {
      if (constantOffset->isNullAt(0)) {
        isConstantOffsetNull_ = true;
      } else {
        constantOffset_ =
            constantOffset->as<ConstantVector<int64_t>>()->valueAt(0);
        VELOX_USER_CHECK_GE(
            constantOffset_.value(), 0, "Offset must be at least 0");
      }
    } else {
      offsetIndex_ = offsetArg.index.value();
      offsets_ = BaseVector::create<FlatVector<int64_t>>(BIGINT(), 0, pool());
    }
  }

  void initializeDefaultValue(
      const std::vector<exec::WindowFunctionArg>& args) {
    if (args.size() <= 2) {
      return;
    }

    const auto& defaultValueArg = args[2];
    if (defaultValueArg.constantValue) {
      // Null default value is equivalent to no default value.
      if (!defaultValueArg.constantValue->isNullAt(0)) {
        constantDefaultValue_ = defaultValueArg.constantValue;
      }
    } else {
      defaultValueIndex_ = defaultValueArg.index.value();
      defaultValues_ = BaseVector::create(defaultValueArg.type, 0, pool());
    }
  }

  void setRowNumbersForConstantOffset();

  void setRowNumbers(vector_size_t numRows) {
    offsets_->resize(numRows);
    partition_->extractColumn(
        offsetIndex_, partitionOffset_, numRows, 0, offsets_);

    const auto maxRowNumber = partition_->numRows() - 1;

    for (auto i = 0; i < numRows; ++i) {
      if (offsets_->isNullAt(i)) {
        rowNumbers_[i] = kNullRow;
      } else {
        vector_size_t offset = offsets_->valueAt(i);
        VELOX_USER_CHECK_GE(offset, 0, "Offset must be at least 0");

        if constexpr (isLag) {
          auto rowNumber = partitionOffset_ + i - offset;
          rowNumbers_[i] = rowNumber >= 0 ? rowNumber : kNullRow;
        } else {
          auto rowNumber = partitionOffset_ + i + offset;
          rowNumbers_[i] = rowNumber <= maxRowNumber ? rowNumber : kNullRow;
        }
      }
    }
  }

  void setDefaultValue(const VectorPtr& result, int32_t resultOffset) {
    if (!constantDefaultValue_ && !defaultValueIndex_) {
      return;
    }

    // Copy default values into 'result' for rows with invalid offsets or empty
    // frames.

    if (constantDefaultValue_) {
      for (auto i = 0; i < rowNumbers_.size(); ++i) {
        if (rowNumbers_[i] == kNullRow) {
          result->copy(constantDefaultValue_.get(), resultOffset + i, 0, 1);
        }
      }
    } else {
      std::vector<vector_size_t> defaultValueRowNumbers;
      defaultValueRowNumbers.reserve(rowNumbers_.size());
      for (auto i = 0; i < rowNumbers_.size(); ++i) {
        if (rowNumbers_[i] == kNullRow) {
          defaultValueRowNumbers.push_back(partitionOffset_ + i);
        }
      }

      if (defaultValueRowNumbers.empty()) {
        return;
      }

      partition_->extractColumn(
          defaultValueIndex_.value(),
          folly::Range(
              defaultValueRowNumbers.data(), defaultValueRowNumbers.size()),
          0,
          defaultValues_);

      for (auto i = 0; i < defaultValueRowNumbers.size(); ++i) {
        result->copy(
            defaultValues_.get(),
            resultOffset + defaultValueRowNumbers[i] - partitionOffset_,
            i,
            1);
      }
    }
  }

  // Index of the 'value' argument.
  column_index_t valueIndex_;

  // Index of the 'offset' argument if offset is not constant.
  column_index_t offsetIndex_;

  // Value of the 'offset' if constant.
  std::optional<int64_t> constantOffset_;
  bool isConstantOffsetNull_ = false;

  // Index of the 'default_value' argument if default value is specified and not
  // constant.
  std::optional<column_index_t> defaultValueIndex_;

  // Constant 'default_value' or null if default value is not constant.
  VectorPtr constantDefaultValue_;

  const exec::WindowPartition* partition_;

  // Reusable vector of offsets if these are not constant.
  FlatVectorPtr<int64_t> offsets_;

  // Reusable vector of default values if these are not cosntant.
  VectorPtr defaultValues_;

  // This offset tracks how far along the partition rows have been output.
  // This can be used to optimize reading offset column values corresponding
  // to the present row set in getOutput.
  vector_size_t partitionOffset_;

  // The Lag function directly writes from the input column to the
  // resultVector using the extractColumn API specifying the rowNumber mapping
  // to copy between the 2 vectors. This variable is used for the rowNumber
  // vector across getOutput calls.
  std::vector<vector_size_t> rowNumbers_;
};

template <>
void LeadLagFunction<true>::setRowNumbersForConstantOffset() {
  if (isConstantOffsetNull_) {
    std::fill(rowNumbers_.begin(), rowNumbers_.end(), kNullRow);
    return;
  }

  auto constantOffsetValue = constantOffset_.value();

  // Figure out how many rows at the start should be NULL.
  vector_size_t nullCnt = 0;
  if (constantOffsetValue > partitionOffset_) {
    nullCnt = std::min<vector_size_t>(
        constantOffsetValue - partitionOffset_, rowNumbers_.size());
    if (nullCnt) {
      std::fill(rowNumbers_.begin(), rowNumbers_.begin() + nullCnt, kNullRow);
    }
  }

  // Populate sequential values for non-NULL rows.
  std::iota(
      rowNumbers_.begin() + nullCnt,
      rowNumbers_.end(),
      partitionOffset_ + nullCnt - constantOffsetValue);
}

template <>
void LeadLagFunction<false>::setRowNumbersForConstantOffset() {
  if (isConstantOffsetNull_) {
    std::fill(rowNumbers_.begin(), rowNumbers_.end(), kNullRow);
    return;
  }

  auto constantOffsetValue = constantOffset_.value();

  // Figure out how many rows at the end should be NULL.
  vector_size_t nonNullCnt = std::max<vector_size_t>(
      0,
      std::min<vector_size_t>(
          rowNumbers_.size(),
          partition_->numRows() - partitionOffset_ - constantOffsetValue));
  if (nonNullCnt < rowNumbers_.size()) {
    std::fill(rowNumbers_.begin() + nonNullCnt, rowNumbers_.end(), kNullRow);
  }

  // Populate sequential values for non-NULL rows.
  if (nonNullCnt > 0) {
    std::iota(
        rowNumbers_.begin(),
        rowNumbers_.begin() + nonNullCnt,
        partitionOffset_ + constantOffsetValue);
  }
}

std::vector<exec::FunctionSignaturePtr> signatures() {
  return {
      // T -> T.
      exec::FunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("T")
          .argumentType("T")
          .build(),
      // (T, bigint) -> T.
      exec::FunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("T")
          .argumentType("T")
          .argumentType("bigint")
          .build(),
      // (T, bigint, T) -> T.
      exec::FunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("T")
          .argumentType("T")
          .argumentType("bigint")
          .argumentType("T")
          .build(),
  };
}

} // namespace

void registerLag(const std::string& name) {
  exec::registerWindowFunction(
      name,
      signatures(),
      [name](
          const std::vector<exec::WindowFunctionArg>& args,
          const TypePtr& resultType,
          velox::memory::MemoryPool* pool,
          HashStringAllocator* /*stringAllocator*/)
          -> std::unique_ptr<exec::WindowFunction> {
        return std::make_unique<LeadLagFunction<true>>(args, resultType, pool);
      });
}

void registerLead(const std::string& name) {
  exec::registerWindowFunction(
      name,
      signatures(),
      [name](
          const std::vector<exec::WindowFunctionArg>& args,
          const TypePtr& resultType,
          velox::memory::MemoryPool* pool,
          HashStringAllocator* /*stringAllocator*/)
          -> std::unique_ptr<exec::WindowFunction> {
        return std::make_unique<LeadLagFunction<false>>(args, resultType, pool);
      });
}
} // namespace facebook::velox::window::prestosql
