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

#include <limits>

#include "velox/dwio/dwrf/writer/FlushPolicy.h"
#include "velox/dwio/dwrf/writer/WriterBase.h"

namespace facebook::velox::dwrf {

struct WriterOptionsShared {
  std::shared_ptr<const Config> config = std::make_shared<Config>();
  std::shared_ptr<const Type> schema;
  std::function<bool(bool, const WriterContext&)> flushPolicy;
  std::shared_ptr<encryption::EncryptionSpecification> encryptionSpec;
  std::shared_ptr<dwio::common::encryption::EncrypterFactory> encrypterFactory;
  int64_t memoryBudget = std::numeric_limits<int64_t>::max();
};

class WriterShared : public WriterBase {
 public:
  WriterShared(
      WriterOptionsShared& options,
      std::unique_ptr<dwio::common::DataSink> sink,
      memory::MemoryPool& parentPool)
      : WriterBase{std::move(sink)},
        schema_{dwio::common::TypeWithId::create(options.schema)},
        flushPolicy_{options.flushPolicy} {
    auto handler =
        (options.encryptionSpec ? encryption::EncryptionHandler::create(
                                      schema_,
                                      *options.encryptionSpec,
                                      options.encrypterFactory.get())
                                : nullptr);
    initContext(
        options.config,
        parentPool.addScopedChild(
            fmt::format(
                "writer_node_{}",
                folly::to<std::string>(folly::Random::rand64())),
            std::min(options.memoryBudget, parentPool.getCap())),
        std::move(handler));
    if (!flushPolicy_) {
      auto& context = getContext();
      flushPolicy_ = DefaultFlushPolicy(
          context.stripeSizeFlushThreshold,
          context.dictionarySizeFlushThreshold);
    }
  }

  ~WriterShared() override = default;

  void close() override;

  void setLowMemoryMode();

  uint64_t flushTimeMemoryUsageEstimate(
      const WriterContext& context,
      size_t nextWriteSize) const;

 protected:
  bool overMemoryBudget(const WriterContext& context, size_t writeLength) const;

  bool shouldFlush(const WriterContext& context, size_t nextWriteLength);

  // Is it safe to call this after first flush?
  void enterLowMemoryMode();

  // Create a new stripe. No-op if there is no data written.
  void flush(bool close = false);

  void flushStripe(bool close);

  void createRowIndexEntry() {
    createIndexEntryImpl();
    getContext().indexRowCount = 0;
  }

  // Actual implementation that depends on column writer
  virtual void flushImpl(
      std::function<proto::ColumnEncoding&(uint32_t)> encodingFactory) = 0;

  virtual void createIndexEntryImpl() = 0;

  virtual void writeFileStatsImpl(
      std::function<proto::ColumnStatistics&(uint32_t)> statsFactory) = 0;

  virtual void abandonDictionariesImpl() = 0;

  virtual void resetImpl() = 0;

  const std::shared_ptr<const dwio::common::TypeWithId> schema_;
  std::function<bool(bool, const WriterContext&)> flushPolicy_;
};

} // namespace facebook::velox::dwrf
