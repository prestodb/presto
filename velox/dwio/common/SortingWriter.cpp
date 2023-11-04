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
    std::unique_ptr<exec::SortBuffer> sortBuffer)
    : outputWriter_(std::move(writer)),
      sortPool_(sortBuffer->pool()),
      canReclaim_(sortBuffer->canSpill()),
      sortBuffer_(std::move(sortBuffer)) {
  if (sortPool_->parent()->reclaimer() != nullptr) {
    sortPool_->setReclaimer(MemoryReclaimer::create(this));
  }
}

void SortingWriter::write(const VectorPtr& data) {
  sortBuffer_->addInput(data);
}

void SortingWriter::flush() {
  outputWriter_->flush();
}

void SortingWriter::close() {
  if (setClose()) {
    return;
  }

  sortBuffer_->noMoreInput();
  RowVectorPtr output = sortBuffer_->getOutput();
  while (output != nullptr) {
    outputWriter_->write(output);
    output = sortBuffer_->getOutput();
  }
  sortBuffer_.reset();
  sortPool_->release();
  outputWriter_->close();
}

void SortingWriter::abort() {
  if (setClose()) {
    return;
  }
  sortBuffer_.reset();
  sortPool_->release();
  outputWriter_->abort();
}

bool SortingWriter::setClose() {
  const bool closed = closed_;
  closed_ = true;
  return closed;
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

  if (closed_) {
    LOG(WARNING) << "Can't reclaim from a closed or aborted hive sort writer: "
                 << sortPool_->name() << ", used memory: "
                 << succinctBytes(sortPool_->currentBytes())
                 << ", reserved memory: "
                 << succinctBytes(sortPool_->reservedBytes());
    ++stats.numNonReclaimableAttempts;
    return 0;
  }
  VELOX_CHECK_NOT_NULL(sortBuffer_);

  sortBuffer_->spill(0, 0);
  sortPool_->release();
  return sortPool_->shrink(targetBytes);
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
  reclaimableBytes = pool.currentBytes();
  return true;
}

uint64_t SortingWriter::MemoryReclaimer::reclaim(
    memory::MemoryPool* pool,
    uint64_t targetBytes,
    memory::MemoryReclaimer::Stats& stats) {
  VELOX_CHECK_EQ(pool->name(), writer_->sortPool_->name());

  return writer_->reclaim(targetBytes, stats);
}
} // namespace facebook::velox::dwio::common
