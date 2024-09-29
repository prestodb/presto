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

#include <folly/Random.h>
#include "velox/exec/Operator.h"
#include "velox/exec/OutputBufferManager.h"
#include "velox/vector/VectorStream.h"

namespace facebook::velox::exec {

namespace detail {
class Destination {
 public:
  /// @param recordEnqueued Should be called to record each call to
  /// OutputBufferManager::enqueue. Takes number of bytes and rows.
  Destination(
      const std::string& taskId,
      int destination,
      memory::MemoryPool* pool,
      bool eagerFlush,
      std::function<void(uint64_t bytes, uint64_t rows)> recordEnqueued)
      : taskId_(taskId),
        destination_(destination),
        pool_(pool),
        eagerFlush_(eagerFlush),
        recordEnqueued_(std::move(recordEnqueued)) {
    setTargetSizePct();
  }

  /// Resets the destination before starting a new batch.
  void beginBatch() {
    rows_.clear();
    rowIdx_ = 0;
  }

  void addRow(vector_size_t row) {
    rows_.push_back(row);
  }

  void addRows(const IndexRange& rows) {
    for (auto i = 0; i < rows.size; ++i) {
      rows_.push_back(rows.begin + i);
    }
  }

  /// Serializes row from 'output' till either 'maxBytes' have been serialized
  /// or
  BlockingReason advance(
      uint64_t maxBytes,
      const std::vector<vector_size_t>& sizes,
      const RowVectorPtr& output,
      OutputBufferManager& bufferManager,
      const std::function<void()>& bufferReleaseFn,
      bool* atEnd,
      ContinueFuture* future,
      Scratch& scratch);

  BlockingReason flush(
      OutputBufferManager& bufferManager,
      const std::function<void()>& bufferReleaseFn,
      ContinueFuture* future);

  bool isFinished() const {
    return finished_;
  }

  void setFinished() {
    finished_ = true;
  }

  uint64_t serializedBytes() const {
    return bytesInCurrent_;
  }

  /// Adds stats from 'this' to runtime stats of 'op'.
  void updateStats(Operator* op);

 private:
  // Sets the next target size for flushing. This is called at the
  // start of each batch of output for the destination. The effect is
  // to make different destinations ready at slightly different times
  // so that for an even distribution of output we avoid a bursty
  // traffic pattern where all consumers contend for the network at
  // the same time. This is done for each batch so that the average
  // batch size for each converges.
  void setTargetSizePct() {
    // Flush at 70 to 120% of target row or byte count.
    targetSizePct_ = 70 + (folly::Random::rand32(rng_) % 50);
    targetNumRows_ = (10'000 * targetSizePct_) / 100;
  }

  const std::string taskId_;
  const int destination_;
  memory::MemoryPool* const pool_;
  const bool eagerFlush_;
  const std::function<void(uint64_t bytes, uint64_t rows)> recordEnqueued_;

  // Bytes serialized in 'current_'
  uint64_t bytesInCurrent_{0};
  // Number of rows serialized in 'current_'
  vector_size_t rowsInCurrent_{0};
  raw_vector<vector_size_t> rows_;

  // First index of 'rows_' that is not appended to 'current_'.
  vector_size_t rowIdx_{0};

  // The current stream where the input is serialized to. This is cleared on
  // every flush() call.
  std::unique_ptr<VectorStreamGroup> current_;
  bool finished_{false};

  // Flush accumulated data to buffer manager after reaching this
  // percentage of target bytes or rows. This will make data for
  // different destinations ready at different times to flatten a
  // burst of traffic.
  int32_t targetSizePct_;

  // Number of rows to accumulate before flushing.
  int32_t targetNumRows_;

  // Generator for varying target batch size. Randomly seeded at construction.
  folly::Random::DefaultGenerator rng_;
};
} // namespace detail

/// In a distributed query engine data needs to be shuffled between workers so
/// that each worker only has to process a fraction of the total data. Because
/// rows are usually not pre-ordered based on the hash of the partition key for
/// an operation (for example join columns, or group by columns), repartitioning
/// is needed to send the rows to the right workers. PartitionedOutput operator
/// is responsible for this process: it takes a stream of data that is not
/// partitioned, and divides the stream into a series of output data ready to be
/// sent to other workers. This operator is also capable of re-ordering and
/// dropping columns from its input.
class PartitionedOutput : public Operator {
 public:
  /// Minimum flush size for non-final flush. 60KB + overhead fits a
  /// network MTU of 64K.
  static constexpr uint64_t kMinDestinationSize = 60 * 1024;

  PartitionedOutput(
      int32_t operatorId,
      DriverCtx* ctx,
      const std::shared_ptr<const core::PartitionedOutputNode>& planNode,
      bool eagerFlush);

  void addInput(RowVectorPtr input) override;

  /// Always returns nullptr. The action is to further process
  /// unprocessed input. If all input has been processed, 'this' is in
  /// a non-blocked state, otherwise blocked.
  RowVectorPtr getOutput() override;

  /// always true but the caller will check isBlocked before adding input, hence
  /// the blocked state does not accumulate input.
  bool needsInput() const override {
    return true;
  }

  BlockingReason isBlocked(ContinueFuture* future) override {
    if (blockingReason_ != BlockingReason::kNotBlocked) {
      *future = std::move(future_);
      blockingReason_ = BlockingReason::kNotBlocked;
      return BlockingReason::kWaitForConsumer;
    }
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override;

  void close() override {
    destinations_.clear();
  }

  static void testingSetMinCompressionRatio(float ratio) {
    minCompressionRatio_ = ratio;
  }

  static float minCompressionRatio() {
    return minCompressionRatio_;
  }

 private:
  void initializeInput(RowVectorPtr input);

  void initializeDestinations();

  void initializeSizeBuffers();

  void estimateRowSizes();

  // Collect all rows with null keys into nullRows_.
  void collectNullRows();

  // If compression in serde is enabled, this is the minimum compression that
  // must be achieved before starting to skip compression. Used for testing.
  inline static float minCompressionRatio_ = 0.8;

  const std::vector<column_index_t> keyChannels_;
  const int numDestinations_;
  const bool replicateNullsAndAny_;
  std::unique_ptr<core::PartitionFunction> partitionFunction_;
  // Empty if column order in the output is exactly the same as in input.
  const std::vector<column_index_t> outputChannels_;
  const std::weak_ptr<exec::OutputBufferManager> bufferManager_;
  const std::function<void()> bufferReleaseFn_;
  const int64_t maxBufferedBytes_;
  const bool eagerFlush_;

  BlockingReason blockingReason_{BlockingReason::kNotBlocked};
  ContinueFuture future_;
  bool finished_{false};
  // Contains pointers to 'rowSize_' elements. 'sizePointers_[i]' contains a
  // pointer to 'rowSize_[i]'.
  std::vector<vector_size_t*> sizePointers_;
  // The estimated row size for each row. Index maps back to 'output_' index
  std::vector<vector_size_t> rowSize_;
  std::vector<std::unique_ptr<detail::Destination>> destinations_;
  bool replicatedAny_{false};
  RowVectorPtr output_;

  // Reusable memory.
  SelectivityVector rows_;
  SelectivityVector nullRows_;
  std::vector<uint32_t> partitions_;
  std::vector<DecodedVector> decodedVectors_;
  Scratch scratch_;
};

} // namespace facebook::velox::exec
