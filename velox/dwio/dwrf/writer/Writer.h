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

#include "velox/dwio/dwrf/common/Encryption.h"
#include "velox/dwio/dwrf/writer/ColumnWriter.h"
#include "velox/dwio/dwrf/writer/FlushPolicy.h"
#include "velox/dwio/dwrf/writer/LayoutPlanner.h"
#include "velox/dwio/dwrf/writer/WriterBase.h"

namespace facebook::velox::dwrf {

struct WriterOptions {
  std::shared_ptr<const Config> config = std::make_shared<Config>();
  std::shared_ptr<const Type> schema;
  // The default factory allows the writer to construct the default flush
  // policy with the configs in its ctor.
  std::function<std::unique_ptr<DWRFFlushPolicy>()> flushPolicyFactory;
  // Change the interface to stream list and encoding iter.
  std::function<std::unique_ptr<LayoutPlanner>(const dwio::common::TypeWithId&)>
      layoutPlannerFactory;
  std::shared_ptr<encryption::EncryptionSpecification> encryptionSpec;
  std::shared_ptr<dwio::common::encryption::EncrypterFactory> encrypterFactory;
  int64_t memoryBudget = std::numeric_limits<int64_t>::max();
  std::function<std::unique_ptr<ColumnWriter>(
      WriterContext& context,
      const velox::dwio::common::TypeWithId& type)>
      columnWriterFactory;
};

class Writer : public WriterBase {
 public:
  Writer(
      const WriterOptions& options,
      std::unique_ptr<dwio::common::DataSink> sink,
      std::shared_ptr<memory::MemoryPool> pool)
      : WriterBase{std::move(sink)},
        schema_{dwio::common::TypeWithId::create(options.schema)} {
    auto handler =
        (options.encryptionSpec ? encryption::EncryptionHandler::create(
                                      schema_,
                                      *options.encryptionSpec,
                                      options.encrypterFactory.get())
                                : nullptr);
    initContext(options.config, std::move(pool), std::move(handler));
    auto& context = getContext();
    context.buildPhysicalSizeAggregators(*schema_);
    if (!options.flushPolicyFactory) {
      flushPolicy_ = std::make_unique<DefaultFlushPolicy>(
          context.stripeSizeFlushThreshold,
          context.dictionarySizeFlushThreshold);
    } else {
      flushPolicy_ = options.flushPolicyFactory();
    }

    if (options.layoutPlannerFactory) {
      layoutPlanner_ = options.layoutPlannerFactory(*schema_);
    } else {
      layoutPlanner_ = std::make_unique<LayoutPlanner>(*schema_);
    }

    if (!options.columnWriterFactory) {
      writer_ = BaseColumnWriter::create(getContext(), *schema_);
    } else {
      writer_ = options.columnWriterFactory(getContext(), *schema_);
    }
  }

  Writer(
      const WriterOptions& options,
      std::unique_ptr<dwio::common::DataSink> sink,
      memory::MemoryPool& parentPool)
      : Writer{
            options,
            std::move(sink),
            parentPool.addAggregateChild(fmt::format(
                "writer_node_{}",
                folly::to<std::string>(folly::Random::rand64())))} {}

  ~Writer() override = default;

  void write(const VectorPtr& slice);

  // Forces the writer to flush, does not close the writer.
  void flush();

  void close() override;

  void setLowMemoryMode();

  uint64_t flushTimeMemoryUsageEstimate(
      const WriterContext& context,
      size_t nextWriteSize) const;

  // protected:
  bool overMemoryBudget(const WriterContext& context, size_t writeLength) const;

  bool shouldFlush(const WriterContext& context, size_t nextWriteLength);

  void enterLowMemoryMode();

  void abandonDictionaries() {
    writer_->tryAbandonDictionaries(true);
  }

 private:
  // Create a new stripe. No-op if there is no data written.
  void flushInternal(bool close = false);

  void flushStripe(bool close);

  void createRowIndexEntry() {
    writer_->createIndexEntry();
    getContext().indexRowCount = 0;
  }

  const std::shared_ptr<const dwio::common::TypeWithId> schema_;
  std::unique_ptr<DWRFFlushPolicy> flushPolicy_;
  std::unique_ptr<LayoutPlanner> layoutPlanner_;
  std::unique_ptr<ColumnWriter> writer_;

  friend class WriterTestHelper;
};

} // namespace facebook::velox::dwrf
