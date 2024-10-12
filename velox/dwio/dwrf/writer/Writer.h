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

#include <iterator>
#include <limits>

#include "velox/dwio/common/Writer.h"
#include "velox/dwio/common/WriterFactory.h"
#include "velox/dwio/dwrf/common/Encryption.h"
#include "velox/dwio/dwrf/writer/ColumnWriter.h"
#include "velox/dwio/dwrf/writer/FlushPolicy.h"
#include "velox/dwio/dwrf/writer/LayoutPlanner.h"
#include "velox/dwio/dwrf/writer/WriterBase.h"
#include "velox/exec/MemoryReclaimer.h"

namespace facebook::velox::dwrf {

struct WriterOptions : public dwio::common::WriterOptions {
  std::shared_ptr<const Config> config = std::make_shared<Config>();
  /// Changes the interface to stream list and encoding iter.
  std::function<std::unique_ptr<LayoutPlanner>(const dwio::common::TypeWithId&)>
      layoutPlannerFactory;
  std::shared_ptr<encryption::EncryptionSpecification> encryptionSpec;
  std::shared_ptr<dwio::common::encryption::EncrypterFactory> encrypterFactory;
  int64_t memoryBudget = std::numeric_limits<int64_t>::max();
  std::function<std::unique_ptr<ColumnWriter>(
      WriterContext& context,
      const velox::dwio::common::TypeWithId& type)>
      columnWriterFactory;
  const tz::TimeZone* sessionTimezone{nullptr};
  bool adjustTimestampToTimezone{false};
};

class Writer : public dwio::common::Writer {
 public:
  Writer(
      const WriterOptions& options,
      std::unique_ptr<dwio::common::FileSink> sink,
      memory::MemoryPool& parentPool)
      : Writer{
            std::move(sink),
            options,
            parentPool.addAggregateChild(fmt::format(
                "{}.dwrf_{}",
                parentPool.name(),
                folly::to<std::string>(folly::Random::rand64())))} {}

  Writer(
      std::unique_ptr<dwio::common::FileSink> sink,
      const WriterOptions& options,
      std::shared_ptr<memory::MemoryPool> pool);

  Writer(
      std::unique_ptr<dwio::common::FileSink> sink,
      const WriterOptions& options);

  ~Writer() override = default;

  virtual void write(const VectorPtr& input) override;

  // Forces the writer to flush, does not close the writer.
  virtual void flush() override;

  virtual bool finish() override {
    return true;
  }

  virtual void close() override;

  virtual void abort() override;

  void setLowMemoryMode();

  uint64_t flushTimeMemoryUsageEstimate(
      const WriterContext& context,
      size_t nextWriteSize) const;

  bool overMemoryBudget(const WriterContext& context, size_t numRows) const;

  /// Writer will flush to make more memory if the incoming input would make
  /// it exceed memory budget with the default flush policy. Other policies
  /// can intentionally throw and expect the application to retry.
  ///
  /// The current approach is to assume that the customer passes in slices of
  /// similar sizes, perhaps even bounded by a configurable amount. We then
  /// compute the soft_cap = hard_budget - expected_increment_per_slice, and
  /// compare that against a dynamically determined flush_overhead +
  /// current_total_usage and try to flush preemptively after writing each
  /// slice/stride to bring the current memory usage below the soft_cap again.
  ///
  /// Using less memory than the soft_cap ensures being able to
  /// write a new slice/stride, unless the slice/stride is drastically bigger
  /// than the previous ones.
  bool shouldFlush(const WriterContext& context, size_t nextWriteRows);

  /// Low memory allows for the writer to write the same data with a lower
  /// memory budget. Currently this method is only called locally to switch
  /// encoding if we couldn't meet flush criteria without exceeding memory
  /// budget.
  ///
  /// NOTE: switching encoding is not a good mitigation for immediate memory
  /// pressure because the switch consumes even more memory than a flush.
  void enterLowMemoryMode();

  void abandonDictionaries() {
    writer_->tryAbandonDictionaries(true);
  }

  void addUserMetadata(const std::string& key, const std::string& value) {
    writerBase_->addUserMetadata(key, value);
  }

  WriterContext& getContext() const {
    return writerBase_->getContext();
  }

  const proto::Footer& getFooter() const {
    return writerBase_->getFooter();
  }

  WriterSink& getSink() {
    return writerBase_->getSink();
  }

  /// True if we can reclaim memory from this writer by memory arbitration.
  bool canReclaim() const;

  tsan_atomic<bool>& testingNonReclaimableSection() {
    return *nonReclaimableSection_;
  }

 protected:
  std::shared_ptr<WriterBase> writerBase_;

 private:
  class MemoryReclaimer : public exec::MemoryReclaimer {
   public:
    static std::unique_ptr<memory::MemoryReclaimer> create(Writer* writer);

    bool reclaimableBytes(
        const memory::MemoryPool& pool,
        uint64_t& reclaimableBytes) const override;

    uint64_t reclaim(
        memory::MemoryPool* pool,
        uint64_t targetBytes,
        uint64_t maxWaitMs,
        memory::MemoryReclaimer::Stats& stats) override;

   private:
    explicit MemoryReclaimer(Writer* writer) : writer_(writer) {
      VELOX_CHECK_NOT_NULL(writer_);
    }

    Writer* const writer_;
  };

  // Sets the memory reclaimers for all the memory pools used by this writer.
  void setMemoryReclaimers(const std::shared_ptr<memory::MemoryPool>& pool);

  // Invoked to ensure sufficient memory to process the given size of input by
  // reserving memory from each of the leaf memory pool. This only applies if we
  // support memory reclaim on this writer. The memory reservation might trigger
  // stripe flush by memory arbitration if the query root memory pool doesn't
  // enough memory capacity.
  void ensureWriteFits(size_t appendBytes, size_t appendRows);

  // Similar to 'ensureWriteFits' above to ensure sufficient memory to flush
  // the buffered stripe data to disk.
  void ensureStripeFlushFits();

  // Grows a memory pool size by the specified ratio.
  bool maybeReserveMemory(
      MemoryUsageCategory memoryUsageCategory,
      double estimatedMemoryGrowthRatio);

  // Releases the unused memory reservations after we flush a stripe. Returns
  // the total number of released bytes.
  int64_t releaseMemory();

  // Create a new stripe. No-op if there is no data written.
  void flushInternal(bool close = false);

  void flushStripe(bool close);

  void createRowIndexEntry() {
    writer_->createIndexEntry();
    writerBase_->getContext().resetIndexRowCount();
  }

  const std::shared_ptr<const dwio::common::TypeWithId> schema_;
  const common::SpillConfig* const spillConfig_;
  // If not null, used by memory arbitration to track if this file writer is
  // under memory reclaimable section or not.
  tsan_atomic<bool>* const nonReclaimableSection_{nullptr};

  std::unique_ptr<DWRFFlushPolicy> flushPolicy_;
  std::unique_ptr<LayoutPlanner> layoutPlanner_;
  std::unique_ptr<ColumnWriter> writer_;
};

class DwrfWriterFactory : public dwio::common::WriterFactory {
 public:
  DwrfWriterFactory() : WriterFactory(dwio::common::FileFormat::DWRF) {}

  std::unique_ptr<dwio::common::Writer> createWriter(
      std::unique_ptr<dwio::common::FileSink> sink,
      const std::shared_ptr<dwio::common::WriterOptions>& options) override;

  std::unique_ptr<dwio::common::WriterOptions> createWriterOptions() override;
};

} // namespace facebook::velox::dwrf
