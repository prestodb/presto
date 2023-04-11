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
      velox::memory::MemoryPool* pool)
      : WindowFunction(resultType, pool, nullptr) {
    VELOX_CHECK_NULL(args[0].constantValue);
    argIndex_ = args[0].index.value();
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

    if constexpr (TValue == ValueType::kFirst) {
      auto rawFrameStarts = frameStarts->as<vector_size_t>();
      validRows.applyToSelected(
          [&](auto i) { rowNumbers_[i] = rawFrameStarts[i]; });
    } else {
      auto rawFrameEnds = frameEnds->as<vector_size_t>();
      validRows.applyToSelected(
          [&](auto i) { rowNumbers_[i] = rawFrameEnds[i]; });
    }
    setRowNumbersForEmptyFrames(validRows);

    auto rowNumbersRange = folly::Range(rowNumbers_.data(), numRows);
    partition_->extractColumn(argIndex_, rowNumbersRange, resultOffset, result);
  }

 private:
  void setRowNumbersForEmptyFrames(const SelectivityVector& validRows) {
    if (validRows.isAllSelected()) {
      return;
    }
    // Rows with empty (not-valid) frames have nullptr in the result.
    // So mark rowNumber to copy as -1 for it.
    invalidRows_.resizeFill(validRows.size(), true);
    invalidRows_.deselect(validRows);
    invalidRows_.applyToSelected([&](auto i) { rowNumbers_[i] = -1; });
  }

  // Index of the first_value / last_value argument column in the input row
  // vector. This is used to retrieve column values from the partition data.
  column_index_t argIndex_;

  const exec::WindowPartition* partition_;

  // The first_value, last_value functions directly write from the input column
  // to the resultVector using the extractColumn API specifying the rowNumber
  // mapping to copy between the 2 vectors. This variable is used for the
  // rowNumber vector across getOutput calls.
  std::vector<vector_size_t> rowNumbers_;

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
         velox::memory::MemoryPool* pool,
         HashStringAllocator* /*stringAllocator*/)
          -> std::unique_ptr<exec::WindowFunction> {
        return std::make_unique<FirstLastValueFunction<TValue>>(
            args, resultType, pool);
      });
}

void registerFirstValue(const std::string& name) {
  registerFirstLastInternal<ValueType::kFirst>(name);
}
void registerLastValue(const std::string& name) {
  registerFirstLastInternal<ValueType::kLast>(name);
}
} // namespace facebook::velox::window::prestosql
