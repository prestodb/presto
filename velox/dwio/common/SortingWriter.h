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

#include "velox/dwio/common/Writer.h"
#include "velox/exec/MemoryReclaimer.h"
#include "velox/exec/SortBuffer.h"

namespace facebook::velox::dwio::common {

/// Sorting Writer object is used to write sorted data into a single file.
class SortingWriter : public Writer {
 public:
  SortingWriter(
      std::unique_ptr<Writer> writer,
      std::unique_ptr<exec::SortBuffer> sortBuffer,
      vector_size_t maxOutputRowsConfig,
      uint64_t maxOutputBytesConfig);

  ~SortingWriter() override;

  void write(const VectorPtr& data) override;

  /// No action because we need to accumulate all data and sort before data can
  /// be flushed
  void flush() override;

  void close() override;

  void abort() override;

 private:
  class MemoryReclaimer : public exec::MemoryReclaimer {
   public:
    static std::unique_ptr<memory::MemoryReclaimer> create(
        SortingWriter* writer);

    bool reclaimableBytes(
        const memory::MemoryPool& pool,
        uint64_t& reclaimableBytes) const override;

    uint64_t reclaim(
        memory::MemoryPool* pool,
        uint64_t targetBytes,
        uint64_t maxWaitMs,
        memory::MemoryReclaimer::Stats& stats) override;

   private:
    explicit MemoryReclaimer(SortingWriter* writer)
        : exec::MemoryReclaimer(),
          writer_(writer),
          canReclaim_(writer_->sortBuffer_->canSpill()) {}

    SortingWriter* const writer_;
    const bool canReclaim_;
  };

  bool canReclaim() const;

  uint64_t reclaim(uint64_t targetBytes, memory::MemoryReclaimer::Stats& stats);

  vector_size_t outputBatchRows();

  const std::unique_ptr<Writer> outputWriter_;
  const vector_size_t maxOutputRowsConfig_;
  const uint64_t maxOutputBytesConfig_;
  memory::MemoryPool* const sortPool_;
  const bool canReclaim_;

  std::unique_ptr<exec::SortBuffer> sortBuffer_;
};

} // namespace facebook::velox::dwio::common
