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

#include "velox/dwio/dwrf/writer/ColumnWriter.h"
#include "velox/dwio/dwrf/writer/WriterShared.h"

namespace facebook::velox::dwrf {

struct WriterOptions : public WriterOptionsShared {
  std::function<std::unique_ptr<ColumnWriter>(
      WriterContext& context,
      const velox::dwio::common::TypeWithId& type)>
      columnWriterFactory;
};

class Writer : public WriterShared {
 public:
  Writer(
      const WriterOptions& options,
      std::unique_ptr<dwio::common::DataSink> sink,
      memory::MemoryPool& pool)
      : WriterShared{options, std::move(sink), pool} {
    if (!options.columnWriterFactory) {
      writer_ = BaseColumnWriter::create(getContext(), *schema_);
    } else {
      writer_ = options.columnWriterFactory(getContext(), *schema_);
    }
  }

  ~Writer() override = default;

  // Write columnar batch
  void write(const VectorPtr& slice);

  void setMemoryUsageTracker(
      const std::shared_ptr<velox::memory::MemoryUsageTracker>& tracker) {
    getContext()
        .getMemoryPool(velox::dwrf::MemoryUsageCategory::DICTIONARY)
        .setMemoryUsageTracker(tracker);
    getContext()
        .getMemoryPool(velox::dwrf::MemoryUsageCategory::GENERAL)
        .setMemoryUsageTracker(tracker);
    getContext()
        .getMemoryPool(velox::dwrf::MemoryUsageCategory::OUTPUT_STREAM)
        .setMemoryUsageTracker(tracker);
  }

 protected:
  void flushImpl(std::function<proto::ColumnEncoding&(uint32_t)>
                     encodingFactory) override {
    writer_->flush(encodingFactory);
  }

  void createIndexEntryImpl() override {
    writer_->createIndexEntry();
  }

  void writeFileStatsImpl(
      std::function<proto::ColumnStatistics&(uint32_t)> statsFactory) override {
    writer_->writeFileStats(statsFactory);
  }

  bool abandonLowValueDictionaries() {
    return writer_->tryAbandonDictionaries(false);
  }

  void abandonDictionariesImpl() override {
    writer_->tryAbandonDictionaries(true);
  }

  void resetImpl() override {
    writer_->reset();
  }

 private:
  std::unique_ptr<ColumnWriter> writer_;

  friend class E2EEncryptionTest;
  friend class WriterTestHelper;
};

} // namespace facebook::velox::dwrf
