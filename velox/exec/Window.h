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

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  bool needsInput() const override {
    return !noMoreInput_;
  }

  void noMoreInput() override;

  BlockingReason isBlocked(ContinueFuture* /* unused */) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return finished_;
  }

 private:
  // Structure for the window frame for each function.
  struct WindowFrame {
    const core::WindowNode::WindowType type;
    const core::WindowNode::BoundType startType;
    const core::WindowNode::BoundType endType;
    const std::optional<column_index_t> startChannel;
    const std::optional<column_index_t> endChannel;
  };

  // Helper function to create WindowFunction and frame objects
  // for this operator.
  void createWindowFunctions(
      const std::shared_ptr<const core::WindowNode>& windowNode,
      const RowTypePtr& inputType);

  // Helper function to create the buffers for peer and frame
  // row indices to send in window function apply invocations.
  void createPeerAndFrameBuffers();

  // Function to compute the partitionStartRows_ structure.
  // partitionStartRows_ is vector of the starting rows index
  // of each partition in the data. This is an auxiliary
  // structure that helps simplify the window function computations.
  void computePartitionStartRows();

  // This function is invoked after receiving all the input data.
  // The input data needs to be separated into partitions and
  // ordered within it (as that is the order in which the rows
  // will be output for the partition).
  // This function achieves this by ordering the input rows by
  // (partition keys + order by keys). Doing so orders all rows
  // of a partition adjacent to each other and sorted by the
  // ORDER BY clause.
  void sortPartitions();

  // Helper function to find window frame start and end values for
  // currentRow of ith window function (in windowFunctions_ list).
  std::pair<vector_size_t, vector_size_t> findFrameEndPoints(
      vector_size_t i,
      vector_size_t partitionStartRow,
      vector_size_t partitionEndRow,
      vector_size_t peerStartRow,
      vector_size_t peerEndRow,
      vector_size_t currentRow);

  // Helper function to call WindowFunction::resetPartition() for
  // all WindowFunctions.
  void callResetPartition(vector_size_t partitionNumber);

  // Helper method to call WindowFunction::apply to all the rows
  // of a partition between startRow and endRow. The outputs
  // will be written to the vectors in windowFunctionOutputs
  // starting at offset resultIndex.
  void callApplyForPartitionRows(
      vector_size_t startRow,
      vector_size_t endRow,
      const std::vector<VectorPtr>& result,
      vector_size_t resultOffset);

  // Helper function to compare the rows at lhs and rhs pointers
  // using the keyInfo in keys. This can be used to compare the
  // rows for partitionKeys, orderByKeys or a combination of both.
  inline bool compareRowsWithKeys(
      const char* lhs,
      const char* rhs,
      const std::vector<std::pair<column_index_t, core::SortOrder>>& keys);

  // Function to compute window function values for the current output
  // buffer. The buffer has numOutputRows number of rows. windowOutputs
  // has the vectors for window function columns.
  void callApplyLoop(
      vector_size_t numOutputRows,
      const std::vector<VectorPtr>& windowOutputs);

  bool finished_ = false;
  const vector_size_t outputBatchSizeInBytes_;
  const vector_size_t numInputColumns_;

  // The Window operator needs to see all the input rows before starting
  // any function computation. As the Window operators gets input rows
  // we store the rows in the RowContainer (data_).
  std::unique_ptr<RowContainer> data_;
  // The decodedInputVectors_ are reused across addInput() calls to decode
  // the partition and sort keys for the above RowContainer.
  std::vector<DecodedVector> decodedInputVectors_;

  // The below 3 vectors represent the ChannelIndex of the partition keys,
  // the order by keys and the concatenation of the 2. These keyInfo are
  // used for sorting by those key combinations during the processing.
  // partitionKeyInfo_ is used to separate partitions in the rows.
  // sortKeyInfo_ is used to identify peer rows in a partition.
  // allKeyInfo_ is a combination of (partitionKeyInfo_ and sortKeyInfo_).
  // It is used to perform a full sorting of the input rows to be able to
  // separate partitions and sort the rows in it. The rows are output in
  // this order by the operator.
  std::vector<std::pair<column_index_t, core::SortOrder>> partitionKeyInfo_;
  std::vector<std::pair<column_index_t, core::SortOrder>> sortKeyInfo_;
  std::vector<std::pair<column_index_t, core::SortOrder>> allKeyInfo_;

  // Vector of WindowFunction objects required by this operator.
  // WindowFunction is the base API implemented by all the window functions.
  // The functions are ordered by their positions in the output columns.
  std::vector<std::unique_ptr<exec::WindowFunction>> windowFunctions_;
  // Vector of WindowFrames corresponding to each windowFunction above.
  // It represents the frame spec for the function computation.
  std::vector<WindowFrame> windowFrames_;

  // This SelectivityVector is used across addInput calls for decoding.
  SelectivityVector inputRows_;
  // Number of input rows.
  vector_size_t numRows_ = 0;

  // Vector of pointers to each input row in the data_ RowContainer.
  // The rows are sorted by partitionKeys + sortKeys. This total
  // ordering can be used to split partitions (with the correct
  // order by) for the processing.
  std::vector<char*> sortedRows_;

  // Window partition object used to provide per-partition
  // data to the window function.
  std::unique_ptr<WindowPartition> windowPartition_;

  // Number of rows that be fit into an output block.
  vector_size_t numRowsPerOutput_;

  // This is a vector that gives the index of the start row
  // (in sortedRows_) of each partition in the RowContainer data_.
  // This auxiliary structure helps demarcate partitions in
  // getOutput calls.
  std::vector<vector_size_t> partitionStartRows_;

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

  // Number of rows output from the WindowOperator so far. The rows
  // are output in the same order of the pointers in sortedRows. This
  // value is updated as the WindowFunction::apply() function is
  // called on the partition blocks.
  vector_size_t numProcessedRows_ = 0;
  // Current partition being output. The partition might be
  // output across multiple getOutput() calls so this needs to
  // be tracked in the operator.
  vector_size_t currentPartition_;

  // When traversing input partition rows, the peers are the rows
  // with the same values for the ORDER BY clause. These rows
  // are equal in some ways and affect the results of ranking functions.
  // Since all rows between the peerStartRow_ and peerEndRow_ have the same
  // values for peerStartRow_ and peerEndRow_, we needn't compute
  // them for each row independently. Since these rows might
  // cross getOutput boundaries they are saved in the operator.
  vector_size_t peerStartRow_ = 0;
  vector_size_t peerEndRow_ = 0;
};

} // namespace facebook::velox::exec
