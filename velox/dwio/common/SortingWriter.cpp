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

#include "velox/dwio/common/SortingWriter.h"

namespace facebook::velox::dwio::common {

SortingWriter::SortingWriter(
    std::unique_ptr<Writer> writer,
    std::unique_ptr<exec::SortBuffer> sortBuffer,
    vector_size_t maxOutputRowsConfig,
    uint64_t maxOutputBytesConfig)
    : outputWriter_(std::move(writer)),
      maxOutputRowsConfig_(maxOutputRowsConfig),
      maxOutputBytesConfig_(maxOutputBytesConfig),
      sortPool_(sortBuffer->pool()),
      canReclaim_(sortBuffer->canSpill()),
      sortBuffer_(std::move(sortBuffer)) {
  VELOX_CHECK_GT(maxOutputRowsConfig_, 0);
  VELOX_CHECK_GT(maxOutputBytesConfig_, 0);
  if (sortPool_->parent()->reclaimer() != nullptr) {
    sortPool_->setReclaimer(MemoryReclaimer::create(this));
  }
  setState(State::kRunning);
}

SortingWriter::~SortingWriter() {
  sortPool_->release();
}

void SortingWriter::write(const VectorPtr& data) {
  checkRunning();
  sortBuffer_->addInput(data);
}

void SortingWriter::flush() {
  checkRunning();
  outputWriter_->flush();
}

void SortingWriter::close() {
  setState(State::kClosed);

  sortBuffer_->noMoreInput();
  const auto maxOutputBatchRows = outputBatchRows();
  RowVectorPtr output = sortBuffer_->getOutput(maxOutputBatchRows);
  while (output != nullptr) {
    outputWriter_->write(output);
    output = sortBuffer_->getOutput(maxOutputBatchRows);
  }

  sortBuffer_.reset();
  sortPool_->release();
  outputWriter_->close();
}

void SortingWriter::abort() {
  setState(State::kAborted);

  sortBuffer_.reset();
  sortPool_->release();
  outputWriter_->abort();
}

bool SortingWriter::canReclaim() const {
  return canReclaim_;
}

uint64_t SortingWriter::reclaim(
    uint64_t targetBytes,
    memory::MemoryReclaimer::Stats& stats) {
  if (!canReclaim_) {
    return 0;
  }

  if (!isRunning()) {
    LOG(WARNING) << "Can't reclaim from a not running hive sort writer pool: "
                 << sortPool_->name() << ", state: " << state()
                 << "used memory: " << succinctBytes(sortPool_->usedBytes())
                 << ", reserved memory: "
                 << succinctBytes(sortPool_->reservedBytes());
    ++stats.numNonReclaimableAttempts;
    return 0;
  }
  VELOX_CHECK_NOT_NULL(sortBuffer_);

  return memory::MemoryReclaimer::run(
      [&]() {
        int64_t reclaimedBytes{0};
        {
          memory::ScopedReclaimedBytesRecorder recorder(
              sortPool_, &reclaimedBytes);
          sortBuffer_->spill();
          sortPool_->release();
        }
        return reclaimedBytes;
      },
      stats);
}

vector_size_t SortingWriter::outputBatchRows() {
  vector_size_t estimatedMaxOutputRows =
      std::numeric_limits<vector_size_t>::max();
  if (sortBuffer_->estimateOutputRowSize().has_value() &&
      sortBuffer_->estimateOutputRowSize().value() != 0) {
    const uint64_t maxOutputRows =
        maxOutputBytesConfig_ / sortBuffer_->estimateOutputRowSize().value();
    if (UNLIKELY(maxOutputRows > std::numeric_limits<vector_size_t>::max())) {
      return maxOutputRowsConfig_;
    }

    estimatedMaxOutputRows = maxOutputRows;
  }
  return std::min(estimatedMaxOutputRows, maxOutputRowsConfig_);
}

std::unique_ptr<memory::MemoryReclaimer> SortingWriter::MemoryReclaimer::create(
    SortingWriter* writer) {
  return std::unique_ptr<memory::MemoryReclaimer>(new MemoryReclaimer(writer));
}

bool SortingWriter::MemoryReclaimer::reclaimableBytes(
    const memory::MemoryPool& pool,
    uint64_t& reclaimableBytes) const {
  VELOX_CHECK_EQ(pool.name(), writer_->sortPool_->name());

  reclaimableBytes = 0;
  if (!writer_->canReclaim()) {
    return false;
  }
  reclaimableBytes = pool.usedBytes();
  return true;
}

uint64_t SortingWriter::MemoryReclaimer::reclaim(
    memory::MemoryPool* pool,
    uint64_t targetBytes,
    uint64_t /*unused*/,
    memory::MemoryReclaimer::Stats& stats) {
  VELOX_CHECK_EQ(pool->name(), writer_->sortPool_->name());

  return writer_->reclaim(targetBytes, stats);
}
} // namespace facebook::velox::dwio::common
