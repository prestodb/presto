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

#include "velox/common/base/GTestMacros.h"
#include "velox/dwio/dwrf/common/Encryption.h"
#include "velox/dwio/dwrf/common/wrap/dwrf-proto-wrapper.h"
#include "velox/dwio/dwrf/proto/dwrf_proto.pb.h"
#include "velox/dwio/dwrf/writer/FlushPolicy.h"
#include "velox/dwio/dwrf/writer/LayoutPlanner.h"
#include "velox/dwio/dwrf/writer/WriterBase.h"

namespace facebook::velox::dwrf {

class EncodingIter {
 public:
  using value_type = const proto::ColumnEncoding;
  using reference = const proto::ColumnEncoding&;
  using pointer = const proto::ColumnEncoding*;
  using iterator_category = std::forward_iterator_tag;
  using difference_type = int64_t;

  static EncodingIter begin(
      const proto::StripeFooter& footer,
      const std::vector<proto::StripeEncryptionGroup>& encryptionGroups);

  static EncodingIter end(
      const proto::StripeFooter& footer,
      const std::vector<proto::StripeEncryptionGroup>& encryptionGroups);

  EncodingIter& operator++();
  EncodingIter operator++(int);
  bool operator==(const EncodingIter& other) const;
  bool operator!=(const EncodingIter& other) const;
  reference operator*() const;
  pointer operator->() const;

 private:
  EncodingIter(
      const proto::StripeFooter& footer,
      const std::vector<proto::StripeEncryptionGroup>& encryptionGroups,
      int32_t encryptionGroupIndex,
      google::protobuf::RepeatedPtrField<proto::ColumnEncoding>::const_iterator
          current,
      google::protobuf::RepeatedPtrField<proto::ColumnEncoding>::const_iterator
          currentEnd);

  void next();

  VELOX_FRIEND_TEST(TestEncodingIter, Ctor);
  VELOX_FRIEND_TEST(TestEncodingIter, EncodingIterBeginAndEnd);
  bool emptyEncryptionGroups() const;

  const proto::StripeFooter& footer_;
  const std::vector<proto::StripeEncryptionGroup>& encryptionGroups_;
  int32_t encryptionGroupIndex_{-1};
  google::protobuf::RepeatedPtrField<proto::ColumnEncoding>::const_iterator
      current_;
  google::protobuf::RepeatedPtrField<proto::ColumnEncoding>::const_iterator
      currentEnd_;
};

class EncodingContainer {
 public:
  virtual ~EncodingContainer() = default;
  virtual EncodingIter begin() const = 0;
  virtual EncodingIter end() const = 0;
};

class EncodingManager : public EncodingContainer {
 public:
  explicit EncodingManager(
      const encryption::EncryptionHandler& encryptionHandler);
  virtual ~EncodingManager() override = default;

  proto::ColumnEncoding& addEncodingToFooter(uint32_t nodeId);
  proto::Stream* addStreamToFooter(uint32_t nodeId, uint32_t& currentIndex);
  std::string* addEncryptionGroupToFooter();
  proto::StripeEncryptionGroup getEncryptionGroup(uint32_t i);
  const proto::StripeFooter& getFooter() const;

  EncodingIter begin() const override;
  EncodingIter end() const override;

 private:
  void initEncryptionGroups();

  const encryption::EncryptionHandler& encryptionHandler_;
  proto::StripeFooter footer_;
  std::vector<proto::StripeEncryptionGroup> encryptionGroups_;
};

struct WriterOptionsShared {
  std::shared_ptr<const Config> config = std::make_shared<Config>();
  std::shared_ptr<const Type> schema;
  // The default factory allows the writer to construct the default flush
  // policy with the configs in its ctor.
  std::function<std::unique_ptr<DWRFFlushPolicy>()> flushPolicyFactory;
  // Change the interface to stream list and encoding iter.
  std::function<
      std::unique_ptr<LayoutPlanner>(StreamList, const EncodingContainer&)>
      layoutPlannerFactory;
  std::shared_ptr<encryption::EncryptionSpecification> encryptionSpec;
  std::shared_ptr<dwio::common::encryption::EncrypterFactory> encrypterFactory;
  int64_t memoryBudget = std::numeric_limits<int64_t>::max();
};

class WriterShared : public WriterBase {
 public:
  WriterShared(
      const WriterOptionsShared& options,
      std::unique_ptr<dwio::common::DataSink> sink,
      memory::MemoryPool& parentPool)
      : WriterBase{std::move(sink)},
        schema_{dwio::common::TypeWithId::create(options.schema)},
        layoutPlannerFactory_{options.layoutPlannerFactory} {
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
    if (!options.flushPolicyFactory) {
      auto& context = getContext();
      flushPolicy_ = std::make_unique<DefaultFlushPolicy>(
          context.stripeSizeFlushThreshold,
          context.dictionarySizeFlushThreshold);
    } else {
      flushPolicy_ = options.flushPolicyFactory();
    }

    if (!layoutPlannerFactory_) {
      layoutPlannerFactory_ = [](StreamList streams,
                                 const EncodingContainer& /* unused */) {
        return std::make_unique<LayoutPlanner>(std::move(streams));
      };
    }
  }

  ~WriterShared() override = default;

  // Forces the writer to flush, does not close the writer.
  void flush();

  void close() override;

  void setLowMemoryMode();

  uint64_t flushTimeMemoryUsageEstimate(
      const WriterContext& context,
      size_t nextWriteSize) const;

 protected:
  bool overMemoryBudget(const WriterContext& context, size_t writeLength) const;

  bool shouldFlush(const WriterContext& context, size_t nextWriteLength);

  void enterLowMemoryMode();

  // Create a new stripe. No-op if there is no data written.
  void flushInternal(bool close = false);

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
  std::unique_ptr<DWRFFlushPolicy> flushPolicy_;
  std::function<
      std::unique_ptr<LayoutPlanner>(StreamList, const EncodingContainer&)>
      layoutPlannerFactory_;
};

} // namespace facebook::velox::dwrf
