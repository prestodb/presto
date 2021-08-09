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

#include "velox/exec/PartitionedOutput.h"
#include "velox/exec/PartitionedOutputBufferManager.h"
#include "velox/expression/Expr.h"

namespace facebook::velox::exec {

BlockingReason Destination::advance(
    uint64_t maxBytes,
    const std::vector<vector_size_t>& sizes,
    const RowVectorPtr& input,
    PartitionedOutputBufferManager& bufferManager,
    bool* atEnd,
    ContinueFuture* future) {
  if (row_ >= rows_.size()) {
    *atEnd = true;
    return BlockingReason::kNotBlocked;
  }
  if (bytesInCurrent_ >= maxBytes) {
    return flush(bufferManager, future);
  }
  auto firstRow = row_;
  for (; row_ < rows_.size(); ++row_) {
    // TODO Add support for serializing partial ranges if
    //  the full range is too big
    for (vector_size_t i = 0; i < rows_[row_].size; i++) {
      bytesInCurrent_ += sizes[rows_[row_].begin + i];
    }
    if (bytesInCurrent_ >= maxBytes) {
      serialize(input, firstRow, row_ + 1);
      if (row_ == rows_.size() - 1) {
        *atEnd = true;
      }
      ++row_;
      return flush(bufferManager, future);
    }
  }
  serialize(input, firstRow, row_);
  *atEnd = true;
  return BlockingReason::kNotBlocked;
}

void Destination::serialize(
    const RowVectorPtr& input,
    vector_size_t begin,
    vector_size_t end) {
  if (!current_) {
    current_ = std::make_unique<VectorStreamGroup>(memory_);
    auto rowType = std::dynamic_pointer_cast<const RowType>(input->type());
    vector_size_t numRows = 0;
    for (vector_size_t i = begin; i < end; i++) {
      numRows += rows_[i].size;
    }
    current_->createStreamTree(rowType, numRows);
  }
  current_->append(input, folly::Range(&rows_[begin], end - begin));
}

BlockingReason Destination::flush(
    PartitionedOutputBufferManager& bufferManager,
    ContinueFuture* future) {
  if (!current_) {
    return BlockingReason::kNotBlocked;
  }
  bytesInCurrent_ = 0;
  return bufferManager.enqueue(
      taskId_, destination_, std::move(current_), future);
}

void PartitionedOutput::addInput(RowVectorPtr input) {
  // TODO Report outputBytes as bytes after serialization
  stats_.outputBytes += input->retainedSize();
  stats_.outputPositions += input->size();

  if (outputChannels_.empty()) {
    input_ = input;
  } else {
    std::vector<VectorPtr> outputColumns;
    outputColumns.reserve(outputChannels_.size());
    for (auto& i : outputChannels_) {
      outputColumns.push_back(input->childAt(i));
    }

    input_ = std::make_shared<RowVector>(
        input->pool(),
        outputType_,
        input->nulls(),
        input->size(),
        outputColumns,
        input->getNullCount());
  }
  if (destinations_.empty()) {
    auto memory = operatorCtx_->mappedMemory();
    auto taskId = operatorCtx_->taskId();
    for (int i = 0; i < numDestinations_; ++i) {
      destinations_.push_back(std::make_unique<Destination>(taskId, i, memory));
    }
    for (auto channel : keyChannels_) {
      hashers_.push_back(
          VectorHasher::create(input_->childAt(channel)->type(), channel));
    }
  }
  auto numInput = input_->size();
  if (numInput > topLevelRanges_.size()) {
    vector_size_t numOld = topLevelRanges_.size();
    topLevelRanges_.resize(numInput);
    for (auto i = numOld; i < numInput; ++i) {
      topLevelRanges_[i] = IndexRange{i, 1};
    }
    hashes_.resize(numInput);
    rowSize_.resize(numInput);
    sizePointers_.resize(numInput);
    // Set all the size pointers since 'rowSize_' may have been reallocated.
    for (vector_size_t i = 0; i < numInput; ++i) {
      sizePointers_[i] = &rowSize_[i];
    }
  }
  if (!keyChannels_.empty()) {
    allRows_.resize(numInput);
    allRows_.setAll();
    for (ChannelIndex i = 0; i < keyChannels_.size(); ++i) {
      hashers_[i]->hash(
          *input_->childAt(keyChannels_[i]), allRows_, i > 0, &hashes_);
    }
  }
  std::fill(rowSize_.begin(), rowSize_.end(), 0);
  for (int i = 0; i < input_->childrenSize(); ++i) {
    VectorStreamGroup::estimateSerializedSize(
        input_->childAt(i),
        folly::Range(topLevelRanges_.data(), numInput),
        sizePointers_.data());
  }
  for (auto& destination : destinations_) {
    destination->beginBatch();
  }
  if (numDestinations_ == 1) {
    destinations_[0]->addRows(IndexRange{0, numInput});
  } else {
    for (vector_size_t i = 0; i < numInput; ++i) {
      destinations_[hashes_[i] % numDestinations_]->addRow(i);
    }
  }
}

RowVectorPtr PartitionedOutput::getOutput() {
  if (isFinished_) {
    return nullptr;
  }

  blockingReason_ = BlockingReason::kNotBlocked;
  Destination* blockedDestination = nullptr;
  auto bufferManager = bufferManager_.lock();
  VELOX_CHECK_NOT_NULL(
      bufferManager, "PartitionedOutputBufferManager was already destructed");

  bool workLeft;
  do {
    workLeft = false;
    for (auto& destination : destinations_) {
      bool atEnd = false;
      blockingReason_ = destination->advance(
          kMaxDestinationSize,
          rowSize_,
          input_,
          *bufferManager,
          &atEnd,
          &future_);
      if (blockingReason_ != BlockingReason::kNotBlocked) {
        blockedDestination = destination.get();
        workLeft = false;
        // We stop on first blocked. Adding data to unflushed targets
        // would be possible but could allocate memory. We wait for
        // free space in the outgoing queue.
        break;
      }
      if (!atEnd) {
        workLeft = true;
      }
    }
  } while (workLeft);
  if (blockedDestination) {
    // If we are going off-thread, we may as well make the output in
    // progress for other destinations available, unless it is too
    // small to be worth transfer.
    for (auto& destination : destinations_) {
      if (destination.get() == blockedDestination ||
          destination->serializedBytes() < kMinDestinationSize) {
        continue;
      }
      destination->flush(*bufferManager, nullptr);
    }
    return nullptr;
  }
  // All of 'input_' is written into the destinations. We are finishing, hence
  // move all the destinations to the output queue. This will not grow memory
  // and hence does not need blocking.
  if (isFinishing_) {
    for (auto& destination : destinations_) {
      if (destination->isFinished()) {
        continue;
      }
      destination->flush(*bufferManager, nullptr);
      destination->setFinished();
    }

    bufferManager->noMoreData(operatorCtx_->task()->taskId());
    isFinished_ = true;
  }
  // The input is fully processed, drop the reference to allow reuse.
  input_ = nullptr;
  return nullptr;
}

} // namespace facebook::velox::exec
