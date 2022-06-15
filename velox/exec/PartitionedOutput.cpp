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

#include "velox/exec/PartitionedOutput.h"
#include "velox/exec/PartitionedOutputBufferManager.h"

namespace facebook::velox::exec {

BlockingReason Destination::advance(
    uint64_t maxBytes,
    const std::vector<vector_size_t>& sizes,
    const RowVectorPtr& output,
    PartitionedOutputBufferManager& bufferManager,
    bool* atEnd,
    ContinueFuture* future) {
  if (row_ >= rows_.size()) {
    *atEnd = true;
    return BlockingReason::kNotBlocked;
  }
  uint32_t adjustedMaxBytes = std::max(
      PartitionedOutput::kMinDestinationSize,
      (maxBytes * targetSizePct_) / 100);
  if (bytesInCurrent_ >= adjustedMaxBytes) {
    return flush(bufferManager, future);
  }
  auto firstRow = row_;
  for (; row_ < rows_.size(); ++row_) {
    // TODO Add support for serializing partial ranges if
    //  the full range is too big
    for (vector_size_t i = 0; i < rows_[row_].size; i++) {
      bytesInCurrent_ += sizes[rows_[row_].begin + i];
    }
    if (bytesInCurrent_ >= adjustedMaxBytes ||
        row_ - firstRow >= targetNumRows_) {
      serialize(output, firstRow, row_ + 1);
      if (row_ == rows_.size() - 1) {
        *atEnd = true;
      }
      ++row_;
      return flush(bufferManager, future);
    }
  }
  serialize(output, firstRow, row_);
  *atEnd = true;
  return BlockingReason::kNotBlocked;
}

void Destination::serialize(
    const RowVectorPtr& output,
    vector_size_t begin,
    vector_size_t end) {
  if (!current_) {
    current_ = std::make_unique<VectorStreamGroup>(memory_);
    auto rowType = std::dynamic_pointer_cast<const RowType>(output->type());
    vector_size_t numRows = 0;
    for (vector_size_t i = begin; i < end; i++) {
      numRows += rows_[i].size;
    }
    current_->createStreamTree(rowType, numRows);
  }
  current_->append(output, folly::Range(&rows_[begin], end - begin));
}

BlockingReason Destination::flush(
    PartitionedOutputBufferManager& bufferManager,
    ContinueFuture* future) {
  if (!current_) {
    return BlockingReason::kNotBlocked;
  }
  // Upper limit of message size with no columns.
  constexpr int32_t kMinMessageSize = 128;
  auto listener = bufferManager.newListener();
  IOBufOutputStream stream(
      *current_->mappedMemory(),
      listener.get(),
      std::max<int64_t>(kMinMessageSize, current_->size()));
  current_->flush(&stream);
  current_.reset();
  bytesInCurrent_ = 0;
  setTargetSizePct();

  return bufferManager.enqueue(
      taskId_,
      destination_,
      std::make_unique<SerializedPage>(stream.getIOBuf()),
      future);
}

void PartitionedOutput::initializeInput(RowVectorPtr input) {
  input_ = std::move(input);
  if (outputChannels_.empty()) {
    output_ = input_;
  } else {
    std::vector<VectorPtr> outputColumns;
    outputColumns.reserve(outputChannels_.size());
    for (auto& i : outputChannels_) {
      outputColumns.push_back(input_->childAt(i));
    }

    output_ = std::make_shared<RowVector>(
        input_->pool(),
        outputType_,
        input_->nulls(),
        input_->size(),
        outputColumns,
        input_->getNullCount());
  }
}

void PartitionedOutput::initializeDestinations() {
  if (destinations_.empty()) {
    auto taskId = operatorCtx_->taskId();
    for (int i = 0; i < numDestinations_; ++i) {
      destinations_.push_back(
          std::make_unique<Destination>(taskId, i, mappedMemory_));
    }
  }
}

void PartitionedOutput::initializeSizeBuffers() {
  auto numInput = input_->size();
  if (numInput > topLevelRanges_.size()) {
    vector_size_t numOld = topLevelRanges_.size();
    topLevelRanges_.resize(numInput);
    for (auto i = numOld; i < numInput; ++i) {
      topLevelRanges_[i] = IndexRange{i, 1};
    }
    rowSize_.resize(numInput);
    sizePointers_.resize(numInput);
    // Set all the size pointers since 'rowSize_' may have been reallocated.
    for (vector_size_t i = 0; i < numInput; ++i) {
      sizePointers_[i] = &rowSize_[i];
    }
  }
}

void PartitionedOutput::estimateRowSizes() {
  auto numInput = input_->size();
  std::fill(rowSize_.begin(), rowSize_.end(), 0);
  for (int i = 0; i < output_->childrenSize(); ++i) {
    VectorStreamGroup::estimateSerializedSize(
        output_->childAt(i),
        folly::Range(topLevelRanges_.data(), numInput),
        sizePointers_.data());
  }
}

void PartitionedOutput::addInput(RowVectorPtr input) {
  // TODO Report outputBytes as bytes after serialization
  stats_.outputBytes += input->retainedSize();
  stats_.outputPositions += input->size();

  initializeInput(std::move(input));

  initializeDestinations();

  initializeSizeBuffers();

  estimateRowSizes();

  for (auto& destination : destinations_) {
    destination->beginBatch();
  }

  auto numInput = input_->size();
  if (numDestinations_ == 1) {
    destinations_[0]->addRows(IndexRange{0, numInput});
  } else {
    partitionFunction_->partition(*input_, partitions_);
    if (replicateNullsAndAny_) {
      collectNullRows();

      vector_size_t start = 0;
      if (!replicatedAny_) {
        for (auto& destination : destinations_) {
          destination->addRow(0);
        }
        replicatedAny_ = true;
        // Make sure not to replicate first row twice.
        start = 1;
      }
      for (auto i = start; i < numInput; ++i) {
        if (nullRows_.isValid(i)) {
          for (auto& destination : destinations_) {
            destination->addRow(i);
          }
        } else {
          destinations_[partitions_[i]]->addRow(i);
        }
      }
    } else {
      for (vector_size_t i = 0; i < numInput; ++i) {
        destinations_[partitions_[i]]->addRow(i);
      }
    }
  }
}

void PartitionedOutput::collectNullRows() {
  auto size = input_->size();
  rows_.resize(size);
  rows_.setAll();

  nullRows_.resize(size);
  nullRows_.clearAll();

  for (auto i : keyChannels_) {
    auto& keyVector = input_->childAt(i);
    if (keyVector->mayHaveNulls()) {
      auto* rawNulls = keyVector->flatRawNulls(rows_);
      bits::orWithNegatedBits(
          nullRows_.asMutableRange().bits(), rawNulls, 0, size);
    }
  }
  nullRows_.updateBounds();
}

RowVectorPtr PartitionedOutput::getOutput() {
  if (finished_) {
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
          maxBufferedBytes_ / destinations_.size(),
          rowSize_,
          output_,
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
  // All of 'output_' is written into the destinations. We are finishing, hence
  // move all the destinations to the output queue. This will not grow memory
  // and hence does not need blocking.
  if (noMoreInput_) {
    for (auto& destination : destinations_) {
      if (destination->isFinished()) {
        continue;
      }
      destination->flush(*bufferManager, nullptr);
      destination->setFinished();
    }

    bufferManager->noMoreData(operatorCtx_->task()->taskId());
    finished_ = true;
  }
  // The input is fully processed, drop the reference to allow reuse.
  input_ = nullptr;
  output_ = nullptr;
  return nullptr;
}

bool PartitionedOutput::isFinished() {
  return finished_;
}

} // namespace facebook::velox::exec
