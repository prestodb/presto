/*
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
#include "velox/exec/PartitionedOutputBufferManager.h"
#include "velox/exec/VectorHasher.h"

namespace facebook::velox::exec {

class PartitionedOutput;

class Destination {
 public:
  Destination(
      const std::string& taskId,
      int destination,
      memory::MappedMemory* memory)
      : taskId_(taskId), destination_(destination), memory_(memory) {}

  // Resets the destination before starting a new batch.
  void beginBatch() {
    rows_.clear();
    row_ = 0;
  }

  void addRow(vector_size_t row) {
    rows_.push_back(IndexRange{row, 1});
  }

  void addRows(const IndexRange& rows) {
    rows_.push_back(rows);
  }

  BlockingReason advance(
      uint64_t maxBytes,
      const std::vector<vector_size_t>& sizes,
      const RowVectorPtr& input,
      PartitionedOutputBufferManager& bufferManager,
      bool* atEnd,
      ContinueFuture* future);

  BlockingReason flush(
      PartitionedOutputBufferManager& bufferManager,
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

 private:
  void
  serialize(const RowVectorPtr& input, vector_size_t begin, vector_size_t end);

  const std::string taskId_;
  const int destination_;
  memory::MappedMemory* const memory_;
  uint64_t bytesInCurrent_{0};
  std::vector<IndexRange> rows_;

  // First row of 'rows_' that is not appended to 'current_'
  vector_size_t row_{0};
  std::unique_ptr<VectorStreamGroup> current_;
  bool finished_{false};
};

// In a distributed query engine data needs to be shuffled between workers so
// that each worker only has to process a fraction of the total data. Because
// rows are usually not pre-ordered based on the hash of the partition key for
// an operation (for example join columns, or group by columns), repartitioning
// is needed to send the rows to the right workers. PartitionedOutput operator
// is responsible for this process: it takes a stream of data that is not
// partitioned, and divides the stream into a series of output data ready to be
// sent to other workers. This operator is also capable of re-ordering and
// dropping columns from its input.
class PartitionedOutput : public Operator {
 public:
  PartitionedOutput(
      int32_t operatorId,
      DriverCtx* ctx,
      const std::shared_ptr<const core::PartitionedOutputNode>& planNode)
      : Operator(
            ctx,
            planNode->outputType(),
            operatorId,
            planNode->id(),
            "PartitionedOutput"),
        keyChannels_(toChannels(planNode->inputType(), planNode->keys())),
        numDestinations_(planNode->numPartitions()),
        outputChannels_(calculateOutputChannels(
            planNode->inputType(),
            planNode->outputType())),
        future_(false),
        bufferManager_(PartitionedOutputBufferManager::getInstance(
            operatorCtx_->task()->queryCtx()->host())) {
    VELOX_CHECK(numDestinations_ > 1 || keyChannels_.empty());
    VELOX_CHECK_GT(outputType_->size(), 0);
  }

  void addInput(RowVectorPtr input) override;

  // Always returns nullptr. The action is to further process
  // unprocessed input. If all input has been processed, 'this' is in
  // a non-blocked state, otherwise blocked.
  RowVectorPtr getOutput() override;

  // always true but the caller will check isBlocked before adding input, hence
  // the blocked state does not accumulate input.
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

  void close() override {
    destinations_.clear();
  }

 private:
  static constexpr uint64_t kMaxDestinationSize = 1024 * 1024; // 1MB
  static constexpr uint64_t kMinDestinationSize = 16 * 1024; // 16 KB

  const std::vector<ChannelIndex> keyChannels_;
  const int numDestinations_;
  // Empty if column order in the output is exactly the same as in input.
  const std::vector<ChannelIndex> outputChannels_;
  BlockingReason blockingReason_{BlockingReason::kNotBlocked};
  ContinueFuture future_;
  bool isFinished_{false};
  std::vector<std::unique_ptr<VectorHasher>> hashers_;
  // top-level row numbers used as input to
  // VectorStreamGroup::estimateSerializedSize member variable is used to avoid
  // re-allocating memory
  std::vector<IndexRange> topLevelRanges_;
  std::vector<vector_size_t*> sizePointers_;
  std::vector<vector_size_t> rowSize_;
  std::vector<uint64_t> hashes_;
  std::vector<std::unique_ptr<Destination>> destinations_;
  SelectivityVector allRows_;
  std::weak_ptr<exec::PartitionedOutputBufferManager> bufferManager_;
};

} // namespace facebook::velox::exec
