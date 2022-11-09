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
#include "velox/exec/Aggregate.h"
#include "velox/exec/WindowFunction.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::window {

namespace {

// A generic way to compute any aggregation used as a window function.
// Creates an Aggregate function object for the window function invocation.
// At each row, computes the aggregation across all rows from the frameStart
// to frameEnd boundaries at that row using singleGroup.
class AggregateWindowFunction : public exec::WindowFunction {
 public:
  AggregateWindowFunction(
      const std::string& name,
      const std::vector<exec::WindowFunctionArg>& args,
      const TypePtr& resultType,
      velox::memory::MemoryPool* pool,
      HashStringAllocator* stringAllocator)
      : WindowFunction(resultType, pool, stringAllocator) {
    argTypes_.reserve(args.size());
    argIndices_.reserve(args.size());
    argVectors_.reserve(args.size());
    for (const auto& arg : args) {
      argTypes_.push_back(arg.type);
      // TODO : Enhance code to handle constant arguments.
      VELOX_CHECK_NULL(
          arg.constantValue,
          "Constant arguments are not supported in aggregate window functions.");
      VELOX_CHECK(arg.index.has_value());
      argIndices_.push_back(arg.index.value());
      argVectors_.push_back(BaseVector::create(arg.type, 0, pool_));
    }
    // Create an Aggregate function object to do result computation. Window
    // function usage only requires single group aggregation for calculating
    // the function value for each row.
    aggregate_ = exec::Aggregate::create(
        name, core::AggregationNode::Step::kSingle, argTypes_, resultType);
    aggregate_->setAllocator(stringAllocator_);

    // Aggregate initialization.
    // Row layout is:
    //  - null flags - one bit per aggregate.
    //  - uint32_t row size,
    //  - fixed-width accumulators - one per aggregate
    //
    // Here we always make space for a row size since we only have one
    // row and no RowContainer. We also have a single aggregate here, so there
    // is only one null bit.
    static const int32_t kNullOffset = 0;
    static const int32_t kRowSizeOffset = bits::nbytes(1);
    singleGroupRowSize_ = kRowSizeOffset + sizeof(int32_t);
    // Accumulator offset must be aligned by their alignment size.
    singleGroupRowSize_ = bits::roundUp(
        singleGroupRowSize_, aggregate_->accumulatorAlignmentSize());
    aggregate_->setOffsets(
        singleGroupRowSize_,
        exec::RowContainer::nullByte(kNullOffset),
        exec::RowContainer::nullMask(kNullOffset),
        /* needed for out of line allocations */ kRowSizeOffset);
    singleGroupRowSize_ += aggregate_->accumulatorFixedWidthSize();

    // Construct the single row in the MemoryPool.
    singleGroupRowBufferPtr_ =
        AlignedBuffer::allocate<char>(singleGroupRowSize_, pool_);
    rawSingleGroupRow_ = singleGroupRowBufferPtr_->asMutable<char>();

    // Constructing a vector of a single result value used for copying from
    // the aggregate to the final result.
    aggregateResultVector_ = BaseVector::create(resultType, 1, pool_);
  }

  ~AggregateWindowFunction() {
    // Needed to delete any out-of-line storage for the accumulator in the
    // group row.
    std::vector<char*> singleGroupRowVector = {rawSingleGroupRow_};
    aggregate_->destroy(folly::Range(singleGroupRowVector.data(), 1));
  }

  void resetPartition(const exec::WindowPartition* partition) override {
    partition_ = partition;
  }

  void apply(
      const BufferPtr& /*peerGroupStarts*/,
      const BufferPtr& /*peerGroupEnds*/,
      const BufferPtr& frameStarts,
      const BufferPtr& frameEnds,
      vector_size_t resultOffset,
      const VectorPtr& result) override {
    int numRows = frameStarts->size() / sizeof(vector_size_t);
    auto rawFrameStarts = frameStarts->as<vector_size_t>();
    auto rawFrameEnds = frameEnds->as<vector_size_t>();

    FrameMetadata frameMetadata =
        fillArgVectors(rawFrameStarts, rawFrameEnds, numRows);
    if (frameMetadata.fixedFirstRow && frameMetadata.nonDecreasingFrameEnd) {
      fixedStartAggregation(
          numRows,
          frameMetadata.firstRow,
          frameMetadata.lastRow,
          rawFrameEnds,
          resultOffset,
          result);
    } else {
      simpleAggregation(
          numRows,
          frameMetadata.firstRow,
          frameMetadata.lastRow,
          rawFrameStarts,
          rawFrameEnds,
          resultOffset,
          result);
    }
  }

 private:
  struct FrameMetadata {
    // Min frame start row required for aggregation.
    vector_size_t firstRow;
    // Max frame end required for the aggregation.
    vector_size_t lastRow;
    // Set to indicate if all rows require the same start row.
    bool fixedFirstRow;
    // Indicates if the frame end values are non-decreasing. If they are
    // then we can perform optimizations to do incremental aggregation.
    bool nonDecreasingFrameEnd;
  };

  FrameMetadata fillArgVectors(
      const vector_size_t* rawFrameStarts,
      const vector_size_t* rawFrameEnds,
      vector_size_t numRows) {
    // Argument values are needed to compute the aggregate. The aggregate
    // computation for each row needs the values from its frameStart to frameEnd
    // indices. So for each apply() we read arguments from the least
    // frameStart to the highest frameEnd value in the row block.
    vector_size_t firstRow = rawFrameStarts[0];
    vector_size_t lastRow = rawFrameEnds[0];

    bool nonDecreasingFrameEnd = true;
    bool fixedFirstRow = true;
    for (int i = 1; i < numRows; i++) {
      firstRow = std::min(firstRow, rawFrameStarts[i]);
      lastRow = std::max(lastRow, rawFrameEnds[i]);
      fixedFirstRow &= rawFrameStarts[i] == firstRow;
      nonDecreasingFrameEnd &= rawFrameEnds[i] >= rawFrameEnds[i - 1];
    }

    vector_size_t numFrameRows = lastRow + 1 - firstRow;
    for (int i = 0; i < argIndices_.size(); i++) {
      argVectors_[i]->resize(numRows);
      partition_->extractColumn(
          argIndices_[i], firstRow, numFrameRows, 0, argVectors_[i]);
    }

    return {firstRow, lastRow, fixedFirstRow, nonDecreasingFrameEnd};
  }

  void computeAggregate(
      SelectivityVector rows,
      vector_size_t startFrame,
      vector_size_t endFrame) {
    rows.clearAll();
    rows.setValidRange(startFrame, endFrame, true);
    rows.updateBounds();

    aggregate_->addSingleGroupRawInput(
        rawSingleGroupRow_, rows, argVectors_, false);
    aggregate_->extractValues(&rawSingleGroupRow_, 1, &aggregateResultVector_);
  }

  void fixedStartAggregation(
      vector_size_t numRows,
      vector_size_t startFrame,
      vector_size_t endFrame,
      const vector_size_t* rawFrameEnds,
      vector_size_t resultOffset,
      const VectorPtr& result) {
    SelectivityVector rows;
    rows.resize(endFrame + 1 - startFrame);

    auto singleGroup = std::vector<vector_size_t>{0};
    // Initialize the aggregate at the beginning of the output block.
    aggregate_->clear();
    aggregate_->initializeNewGroups(&rawSingleGroupRow_, singleGroup);
    auto prevFrameEnd = 0;

    // This is a simple optimization for frames that have a fixed startFrame
    // and increasing frameEnd values. In that case, we can
    // incrementally aggregate over the new rows seen in the frame between
    // the previous and current row.
    for (int i = 0; i < numRows; i++) {
      auto currentFrameEnd = rawFrameEnds[i] - startFrame + 1;
      if (currentFrameEnd > prevFrameEnd) {
        computeAggregate(rows, prevFrameEnd, currentFrameEnd);
      }

      result->copy(aggregateResultVector_.get(), resultOffset + i, 0, 1);
      prevFrameEnd = currentFrameEnd;
    }
  }

  void simpleAggregation(
      vector_size_t numRows,
      vector_size_t minFrame,
      vector_size_t maxFrame,
      const vector_size_t* frameStartsVector,
      const vector_size_t* frameEndsVector,
      vector_size_t resultOffset,
      const VectorPtr& result) {
    SelectivityVector rows;
    rows.resize(maxFrame + 1 - minFrame);
    auto singleGroup = std::vector<vector_size_t>{0};
    for (int i = 0; i < numRows; i++) {
      // This is a very naive algorithm.
      // It evaluates the entire aggregation for each row by iterating over
      // input rows from frameStart to frameEnd in the SelectivityVector.
      // TODO : Try to re-use previous computations by advancing and retracting
      // the aggregation based on the frame changes with each row. This would
      // require adding new APIs to the Aggregate framework.
      aggregate_->clear();
      aggregate_->initializeNewGroups(&rawSingleGroupRow_, singleGroup);

      auto frameStartIndex = frameStartsVector[i] - minFrame;
      auto frameEndIndex = frameEndsVector[i] - minFrame + 1;
      computeAggregate(rows, frameStartIndex, frameEndIndex);
      result->copy(aggregateResultVector_.get(), resultOffset + i, 0, 1);
    }
  }

  // Aggregate function object required for this window function evaluation.
  std::unique_ptr<exec::Aggregate> aggregate_;

  // Current WindowPartition used for accessing rows in the apply method.
  const exec::WindowPartition* partition_;

  // Args information : their types, column indexes in inputs and vectors
  // used to populate values to pass to the aggregate function.
  std::vector<TypePtr> argTypes_;
  std::vector<column_index_t> argIndices_;
  std::vector<VectorPtr> argVectors_;

  // This is a single aggregate row needed by the aggregate function for its
  // computation. These values are for the row and its various components.
  BufferPtr singleGroupRowBufferPtr_;
  char* rawSingleGroupRow_;
  vector_size_t singleGroupRowSize_;

  // Used for per-row aggregate computations.
  // This vector is used to copy from the aggregate to the result.
  VectorPtr aggregateResultVector_;
};

} // namespace

void registerAggregateWindowFunction(const std::string& name) {
  auto aggregateFunctionSignatures = exec::getAggregateFunctionSignatures(name);
  if (aggregateFunctionSignatures.has_value()) {
    // This copy is needed to obtain a vector of the base FunctionSignaturePtr
    // from the AggregateFunctionSignaturePtr type of
    // aggregateFunctionSignatures variable.
    std::vector<exec::FunctionSignaturePtr> signatures(
        aggregateFunctionSignatures.value().begin(),
        aggregateFunctionSignatures.value().end());

    exec::registerWindowFunction(
        name,
        std::move(signatures),
        [name](
            const std::vector<exec::WindowFunctionArg>& args,
            const TypePtr& resultType,
            velox::memory::MemoryPool* pool,
            HashStringAllocator* stringAllocator)
            -> std::unique_ptr<exec::WindowFunction> {
          return std::make_unique<AggregateWindowFunction>(
              name, args, resultType, pool, stringAllocator);
        });
  }
}
} // namespace facebook::velox::window
