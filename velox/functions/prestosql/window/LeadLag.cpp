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
      bool ignoreNulls,
      velox::memory::MemoryPool* pool)
      : WindowFunction(resultType, pool, nullptr), ignoreNulls_(ignoreNulls) {
    valueIndex_ = args[0].index.value();

    initializeOffset(args);
    initializeDefaultValue(args);

    nulls_ = allocateNulls(0, pool);
  }

  void resetPartition(const exec::WindowPartition* partition) override {
    partition_ = partition;
    partitionOffset_ = 0;
    ignoreNullsForPartition_ = false;

    if (ignoreNulls_) {
      auto partitionSize = partition_->numRows();
      AlignedBuffer::reallocate<bool>(&nulls_, partitionSize);

      partition_->extractNulls(valueIndex_, 0, partitionSize, nulls_);
      // There are null bits so the special ignoreNulls processing is required
      // for this partition.
      ignoreNullsForPartition_ =
          bits::countBits(nulls_->as<uint64_t>(), 0, partitionSize) > 0;
    }
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
      if (ignoreNullsForPartition_) {
        setRowNumbers<true>(numRows);
      } else {
        setRowNumbers<false>(numRows);
      }
    }

    auto rowNumbersRange = folly::Range(rowNumbers_.data(), numRows);
    partition_->extractColumn(
        valueIndex_, rowNumbersRange, resultOffset, result);

    setDefaultValue(result, resultOffset);

    partitionOffset_ += numRows;
  }

 private:
  // Lead/Lag return default value (using kDefaultValueRow) if offsets for
  // target rowNumbers are outside the partition. If offset is null, then the
  // functions return null (using kNullRow) .
  //
  // kDefaultValueRow needs to be a negative number so that
  // WindowPartition::extractColumn calls skip this row. It is set to -2 to
  // distinguish it from kNullRow which is -1.
  static constexpr vector_size_t kDefaultValueRow = -2;

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
        if (constantOffset_.value() == 0) {
          isConstantOffsetZero_ = true;
        }
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

  void setRowNumbersForConstantOffset(vector_size_t offset);

  void setRowNumbersForConstantOffset() {
    // Set row number to kNullRow for NULL offset.
    if (isConstantOffsetNull_) {
      std::fill(rowNumbers_.begin(), rowNumbers_.end(), kNullRow);
      return;
    }
    // If the offset is 0 then it means always return the current row.
    if (isConstantOffsetZero_) {
      std::iota(rowNumbers_.begin(), rowNumbers_.end(), partitionOffset_);
      return;
    }

    auto constantOffsetValue = constantOffset_.value();
    // Set row number to kDefaultValueRow for out of range offset.
    if (constantOffsetValue > partition_->numRows()) {
      std::fill(rowNumbers_.begin(), rowNumbers_.end(), kDefaultValueRow);
      return;
    }

    setRowNumbersForConstantOffset(constantOffsetValue);
  }

  template <bool ignoreNulls>
  void setRowNumbers(vector_size_t numRows) {
    offsets_->resize(numRows);
    partition_->extractColumn(
        offsetIndex_, partitionOffset_, numRows, 0, offsets_);

    const auto maxRowNumber = partition_->numRows() - 1;
    auto* rawNulls = nulls_->as<uint64_t>();
    for (auto i = 0; i < numRows; ++i) {
      // Set row number to kNullRow for NULL offset.
      if (offsets_->isNullAt(i)) {
        rowNumbers_[i] = kNullRow;
      } else {
        auto offset = offsets_->valueAt(i);
        VELOX_USER_CHECK_GE(offset, 0, "Offset must be at least 0");
        // Set rowNumber to kDefaultValueRow for out of range offset.
        if (offset > partition_->numRows()) {
          rowNumbers_[i] = kDefaultValueRow;
          continue;
        }
        // If the offset is 0 then it means always return the current row.
        if (offset == 0) {
          rowNumbers_[i] = partitionOffset_ + i;
          continue;
        }

        if constexpr (isLag) {
          if constexpr (ignoreNulls) {
            rowNumbers_[i] = rowNumberIgnoreNull(
                rawNulls, offset, partitionOffset_ + i - 1, -1, -1);
          } else {
            // Set rowNumber to kDefaultValueRow for out of range offset.
            auto rowNumber = partitionOffset_ + i - offset;
            rowNumbers_[i] = rowNumber >= 0 ? rowNumber : kDefaultValueRow;
          }
        } else {
          if constexpr (ignoreNulls) {
            rowNumbers_[i] = rowNumberIgnoreNull(
                rawNulls,
                offset,
                partitionOffset_ + i + 1,
                partition_->numRows(),
                1);
          } else {
            // Set rowNumber to kDefaultValueRow for out of range offset.
            auto rowNumber = partitionOffset_ + i + offset;
            rowNumbers_[i] =
                rowNumber <= maxRowNumber ? rowNumber : kDefaultValueRow;
          }
        }
      }
    }
  }

  // This method assumes the input offset > 0
  vector_size_t rowNumberIgnoreNull(
      const uint64_t* rawNulls,
      vector_size_t offset,
      vector_size_t start,
      vector_size_t end,
      vector_size_t step) {
    auto nonNullCount = 0;
    for (auto j = start; j != end; j += step) {
      if (!bits::isBitSet(rawNulls, j)) {
        nonNullCount++;
        if (nonNullCount == offset) {
          return j;
        }
      }
    }

    return kDefaultValueRow;
  }

  void setDefaultValue(const VectorPtr& result, int32_t resultOffset) {
    // Default value is not specified, just return.
    if (!constantDefaultValue_ && !defaultValueIndex_) {
      return;
    }

    // Copy default values into 'result' for rows with invalid offsets or empty
    // frames.
    if (constantDefaultValue_) {
      for (auto i = 0; i < rowNumbers_.size(); ++i) {
        if (rowNumbers_[i] == kDefaultValueRow) {
          result->copy(constantDefaultValue_.get(), resultOffset + i, 0, 1);
        }
      }
    } else {
      std::vector<vector_size_t> defaultValueRowNumbers;
      defaultValueRowNumbers.reserve(rowNumbers_.size());
      for (auto i = 0; i < rowNumbers_.size(); ++i) {
        if (rowNumbers_[i] == kDefaultValueRow) {
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

  const bool ignoreNulls_;

  // Certain partitions may not have null values. So ignore nulls processing can
  // be skipped for them. Used for tracking this at the partition level.
  bool ignoreNullsForPartition_;

  // Index of the 'value' argument.
  column_index_t valueIndex_;

  // Index of the 'offset' argument if offset is not constant.
  column_index_t offsetIndex_;

  // Value of the 'offset' if constant.
  std::optional<int64_t> constantOffset_;
  bool isConstantOffsetNull_ = false;
  bool isConstantOffsetZero_ = false;

  // Index of the 'default_value' argument if default value is specified and not
  // constant.
  std::optional<column_index_t> defaultValueIndex_;

  // Constant 'default_value' or null if default value is not constant.
  VectorPtr constantDefaultValue_;

  const exec::WindowPartition* partition_;

  // Reusable vector of offsets if these are not constant.
  FlatVectorPtr<int64_t> offsets_;

  // Reusable vector of default values if these are not constant.
  VectorPtr defaultValues_;

  // Null positions buffer to use for ignoreNulls.
  BufferPtr nulls_;

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
void LeadLagFunction<true>::setRowNumbersForConstantOffset(
    vector_size_t offset) {
  // Figure out how many rows at the start is out of range.
  vector_size_t nullCnt = 0;
  if (offset > partitionOffset_) {
    nullCnt =
        std::min<vector_size_t>(offset - partitionOffset_, rowNumbers_.size());
    if (nullCnt) {
      std::fill(
          rowNumbers_.begin(), rowNumbers_.begin() + nullCnt, kDefaultValueRow);
    }
  }

  if (ignoreNullsForPartition_) {
    auto rawNulls = nulls_->as<uint64_t>();
    for (auto i = nullCnt; i < rowNumbers_.size(); i++) {
      rowNumbers_[i] = rowNumberIgnoreNull(
          rawNulls, offset, partitionOffset_ + i - 1, -1, -1);
    }
  } else {
    // Populate sequential values for non-NULL rows.
    std::iota(
        rowNumbers_.begin() + nullCnt,
        rowNumbers_.end(),
        partitionOffset_ + nullCnt - offset);
  }
}

template <>
void LeadLagFunction<false>::setRowNumbersForConstantOffset(
    vector_size_t offset) {
  // Figure out how many rows at the end is out of range.
  vector_size_t nonNullCnt = std::max<vector_size_t>(
      0,
      std::min<vector_size_t>(
          rowNumbers_.size(),
          partition_->numRows() - partitionOffset_ - offset));
  if (nonNullCnt < rowNumbers_.size()) {
    std::fill(
        rowNumbers_.begin() + nonNullCnt, rowNumbers_.end(), kDefaultValueRow);
  }

  // Populate sequential values for non-NULL rows.
  if (nonNullCnt > 0) {
    if (ignoreNullsForPartition_) {
      auto rawNulls = nulls_->as<uint64_t>();
      for (auto i = 0; i < nonNullCnt; i++) {
        rowNumbers_[i] = rowNumberIgnoreNull(
            rawNulls,
            offset,
            partitionOffset_ + i + 1,
            partition_->numRows(),
            1);
      }
    } else {
      std::iota(
          rowNumbers_.begin(),
          rowNumbers_.begin() + nonNullCnt,
          partitionOffset_ + offset);
    }
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
      exec::WindowFunction::Metadata::defaultMetadata(),
      [name](
          const std::vector<exec::WindowFunctionArg>& args,
          const TypePtr& resultType,
          bool ignoreNulls,
          velox::memory::MemoryPool* pool,
          HashStringAllocator* /*stringAllocator*/,
          const velox::core::QueryConfig& /*queryConfig*/)
          -> std::unique_ptr<exec::WindowFunction> {
        return std::make_unique<LeadLagFunction<true>>(
            args, resultType, ignoreNulls, pool);
      });
}

void registerLead(const std::string& name) {
  exec::registerWindowFunction(
      name,
      signatures(),
      exec::WindowFunction::Metadata::defaultMetadata(),
      [name](
          const std::vector<exec::WindowFunctionArg>& args,
          const TypePtr& resultType,
          bool ignoreNulls,
          velox::memory::MemoryPool* pool,
          HashStringAllocator* /*stringAllocator*/,
          const velox::core::QueryConfig& /*queryConfig*/)
          -> std::unique_ptr<exec::WindowFunction> {
        return std::make_unique<LeadLagFunction<false>>(
            args, resultType, ignoreNulls, pool);
      });
}
} // namespace facebook::velox::window::prestosql
