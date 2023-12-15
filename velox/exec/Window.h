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

#include "velox/exec/Operator.h"
#include "velox/exec/RowContainer.h"
#include "velox/exec/WindowBuild.h"
#include "velox/exec/WindowFunction.h"
#include "velox/exec/WindowPartition.h"

namespace facebook::velox::exec {

/// This is a very simple in-Memory implementation of a Window Operator
/// to compute window functions.
///
/// This operator uses a very naive algorithm that sorts all the input
/// data with a combination of the (partition_by keys + order_by keys)
/// to obtain a full ordering of the input. We can easily identify
/// partitions while traversing this sorted data in order.
/// It is also sorted in the order required for the WindowFunction
/// to process it.
///
/// We will revise this algorithm in the future using a HashTable based
/// approach pending some profiling results.
class Window : public Operator {
 public:
  Window(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::WindowNode>& windowNode);

  /// Initialize the window functions from 'windowNode_' once by driver operator
  /// initialization. 'windowNode_' is reset after this call.
  void initialize() override;

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  bool needsInput() const override {
    return !noMoreInput_ && windowBuild_->needsInput();
  }

  void noMoreInput() override;

  BlockingReason isBlocked(ContinueFuture* /* unused */) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return noMoreInput_ && numRows_ == numProcessedRows_;
  }

  void reclaim(uint64_t targetBytes, memory::MemoryReclaimer::Stats& stats)
      override;

 private:
  // Used for k preceding/following frames. Index is the column index if k is a
  // column. value is used to read column values from the column index when k
  // is a column. The field constant stores constant k values.
  struct FrameChannelArg {
    column_index_t index;
    VectorPtr value;
    std::optional<int64_t> constant;
  };

  // Structure for the window frame for each function.
  struct WindowFrame {
    const core::WindowNode::WindowType type;
    const core::WindowNode::BoundType startType;
    const core::WindowNode::BoundType endType;
    // Set only when startType is BoundType::kPreceding or kFollowing.
    const std::optional<FrameChannelArg> start;
    // Set only when endType is BoundType::kPreceding or kFollowing.
    const std::optional<FrameChannelArg> end;
  };

  // Creates WindowFunction and frame objects for this operator.
  void createWindowFunctions();

  // Converts WindowNode::Frame to Window::WindowFrame.
  WindowFrame createWindowFrame(
      const std::shared_ptr<const core::WindowNode>& windowNode,
      const core::WindowNode::Frame& frame,
      const RowTypePtr& inputType);

  // Creates the buffers for peer and frame row
  // indices to send in window function apply invocations.
  void createPeerAndFrameBuffers();

  // Compute the peer and frame buffers for rows between
  // startRow and endRow in the current partition.
  void computePeerAndFrameBuffers(vector_size_t startRow, vector_size_t endRow);

  // Updates all the state for the next partition.
  void callResetPartition();

  // Computes the result vector for a subset of the current
  // partition rows starting from startRow to endRow. A single partition
  // could span multiple output blocks and a single output block could
  // also have multiple partitions in it. So resultOffset is the
  // offset in the result vector corresponding to the current range of
  // partition rows.
  void callApplyForPartitionRows(
      vector_size_t startRow,
      vector_size_t endRow,
      vector_size_t resultOffset,
      const RowVectorPtr& result);

  // Gets the input columns of the current window partition
  // between startRow and endRow in result at resultOffset.
  void getInputColumns(
      vector_size_t startRow,
      vector_size_t endRow,
      vector_size_t resultOffset,
      const RowVectorPtr& result);

  // Computes the result vector for a single output block. The result
  // consists of all the input columns followed by the results of the
  // window function.
  // @return The number of rows processed in the loop.
  vector_size_t callApplyLoop(
      vector_size_t numOutputRows,
      const RowVectorPtr& result);

  // Update frame bounds for kPreceding, kFollowing row frames.
  void updateKRowsFrameBounds(
      bool isKPreceding,
      const FrameChannelArg& frameArg,
      vector_size_t startRow,
      vector_size_t numRows,
      vector_size_t* rawFrameBounds);

  void updateFrameBounds(
      const WindowFrame& windowFrame,
      const bool isStartBound,
      const vector_size_t startRow,
      const vector_size_t numRows,
      const vector_size_t* rawPeerStarts,
      const vector_size_t* rawPeerEnds,
      vector_size_t* rawFrameBounds);

  const vector_size_t numInputColumns_;

  // WindowBuild is used to store input rows and return WindowPartitions
  // for the processing.
  std::unique_ptr<WindowBuild> windowBuild_;

  // The cached window plan node used for window function initialization. It is
  // reset after the initialization.
  std::shared_ptr<const core::WindowNode> windowNode_;

  // Used to access window partition rows and columns by the window
  // operator and functions. This structure is owned by the WindowBuild.
  std::unique_ptr<WindowPartition> currentPartition_;

  // HashStringAllocator required by functions that allocate out of line
  // buffers.
  HashStringAllocator stringAllocator_;

  // Vector of WindowFunction objects required by this operator.
  // WindowFunction is the base API implemented by all the window functions.
  // The functions are ordered by their positions in the output columns.
  std::vector<std::unique_ptr<exec::WindowFunction>> windowFunctions_;

  // Vector of WindowFrames corresponding to each windowFunction above.
  // It represents the frame spec for the function computation.
  std::vector<WindowFrame> windowFrames_;

  // The following 4 Buffers are used to pass peer and frame start and
  // end values to the WindowFunction::apply method. These
  // buffers can be allocated once and reused across all the getOutput
  // calls.
  // Only a single peer start and peer end buffer is needed across all
  // functions (as the peer values are based on the ORDER BY clause).
  BufferPtr peerStartBuffer_;
  BufferPtr peerEndBuffer_;
  // A separate BufferPtr is required for the frame indexes of each
  // function. Each function has its own frame clause and style. So we
  // have as many buffers as the number of functions.
  std::vector<BufferPtr> frameStartBuffers_;
  std::vector<BufferPtr> frameEndBuffers_;

  // Frame types for kPreceding or kFollowing could result in empty
  // frames if the frameStart > frameEnds, or frameEnds < firstPartitionRow
  // or frameStarts > lastPartitionRow. Such frames usually evaluate to NULL
  // in the window function.
  // This SelectivityVector captures the valid (non-empty) frames in the
  // buffer being worked on. The window function can use this to compute
  // output values.
  // There is one SelectivityVector per window function.
  std::vector<SelectivityVector> validFrames_;

  // Number of input rows.
  vector_size_t numRows_ = 0;

  // Number of rows that be fit into an output block.
  vector_size_t numRowsPerOutput_;

  // Number of rows output from the WindowOperator so far. The rows
  // are output in the same order of the pointers in sortedRows. This
  // value is updated as the WindowFunction::apply() function is
  // called on the partition blocks.
  vector_size_t numProcessedRows_ = 0;

  // Tracks how far along the partition rows have been output.
  vector_size_t partitionOffset_ = 0;

  // When traversing input partition rows, the peers are the rows
  // with the same values for the ORDER BY clause. These rows
  // are equal in some ways and affect the results of ranking functions.
  // Since all rows between the peerStartRow_ and peerEndRow_ have the same
  // values for peerStartRow_ and peerEndRow_, we needn't compute
  // them for each row independently. Since these rows might
  // cross getOutput boundaries and be called in subsequent calls to
  // computePeerBuffers they are saved here.
  vector_size_t peerStartRow_ = 0;
  vector_size_t peerEndRow_ = 0;
};

} // namespace facebook::velox::exec
