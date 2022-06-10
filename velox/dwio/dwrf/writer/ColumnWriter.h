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

#include "velox/common/base/GTestMacros.h"
#include "velox/dwio/dwrf/common/ByteRLE.h"
#include "velox/dwio/dwrf/common/Common.h"
#include "velox/dwio/dwrf/common/IntEncoder.h"
#include "velox/dwio/dwrf/common/OutputStream.h"
#include "velox/dwio/dwrf/writer/IndexBuilder.h"
#include "velox/dwio/dwrf/writer/StatisticsBuilder.h"
#include "velox/dwio/dwrf/writer/WriterContext.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/DecodedVector.h"

namespace facebook::velox::dwrf {

constexpr uint64_t NULL_SIZE = 1;

class ColumnWriter {
 public:
  virtual ~ColumnWriter() = default;

  virtual uint64_t write(const VectorPtr& slice, const Ranges& ranges) = 0;

  virtual void createIndexEntry() = 0;

  virtual void reset() = 0;

  virtual void flush(
      std::function<proto::ColumnEncoding&(uint32_t)> encodingFactory,
      std::function<void(proto::ColumnEncoding&)> encodingOverride =
          [](auto& /* e */) {}) = 0;

  virtual uint64_t writeFileStats(
      std::function<proto::ColumnStatistics&(uint32_t)> statsFactory) const = 0;

  virtual bool tryAbandonDictionaries(bool force) = 0;

 protected:
  ColumnWriter(
      WriterContext& context,
      const uint32_t id,
      const uint32_t sequence)
      : context_{context}, id_{id}, sequence_{sequence} {}

  virtual void setEncoding(proto::ColumnEncoding& encoding) const {
    encoding.set_kind(proto::ColumnEncoding_Kind::ColumnEncoding_Kind_DIRECT);
    encoding.set_dictionarysize(0);
    encoding.set_node(id_);
    encoding.set_sequence(sequence_);
  }

  std::unique_ptr<BufferedOutputStream> newStream(StreamKind kind) {
    return context_.newStream(DwrfStreamIdentifier{id_, sequence_, 0, kind});
  }

  WriterContext& context_;
  const uint32_t id_;
  const uint32_t sequence_;
};

class BaseColumnWriter : public ColumnWriter {
 public:
  ~BaseColumnWriter() override = default;

  void createIndexEntry() override {
    hasNull_ = hasNull_ || indexStatsBuilder_->hasNull().value();
    fileStatsBuilder_->merge(*indexStatsBuilder_);
    indexBuilder_->addEntry(*indexStatsBuilder_);
    indexStatsBuilder_->reset();
    recordPosition();
    for (auto& child : children_) {
      child->createIndexEntry();
    }
  }

  void reset() override {
    hasNull_ = false;
    recordPosition();
    for (auto& child : children_) {
      child->reset();
    }
  }

  void flush(
      std::function<proto::ColumnEncoding&(uint32_t)> encodingFactory,
      std::function<void(proto::ColumnEncoding&)> encodingOverride =
          [](auto& /* e */) {}) override {
    if (!isRoot()) {
      present_->flush();

      // remove present stream if doesn't have null
      if (!hasNull_) {
        suppressStream(StreamKind::StreamKind_PRESENT);
        indexBuilder_->removePresentStreamPositions(
            context_.isStreamPaged(id_));
      }
    }

    auto& encoding = encodingFactory(id_);
    setEncoding(encoding);
    encodingOverride(encoding);
    indexBuilder_->flush();
  }

  uint64_t writeFileStats(std::function<proto::ColumnStatistics&(uint32_t)>
                              statsFactory) const override {
    auto& stats = statsFactory(id_);
    fileStatsBuilder_->toProto(stats);
    uint64_t size = context_.getNodeSize(id_);
    for (auto& child : children_) {
      size += child->writeFileStats(statsFactory);
    }
    stats.set_size(size);
    return size;
  }

  // Determine whether dictionary is the right encoding to use when writing
  // the first stripe. We will continue using the same decision for all
  // subsequent stripes.
  // Returns true if an encoding change is performed, false otherwise.
  bool tryAbandonDictionaries(bool force) override {
    bool result = false;
    for (auto& child : children_) {
      result |= child->tryAbandonDictionaries(force);
    }
    return result;
  }

  const velox::dwio::common::TypeWithId& getType() const {
    return type_;
  }

  static std::unique_ptr<BaseColumnWriter> create(
      WriterContext& context,
      const dwio::common::TypeWithId& type,
      const uint32_t sequence = 0,
      std::function<void(IndexBuilder&)> onRecordPosition = nullptr);

 protected:
  BaseColumnWriter(
      WriterContext& context,
      const dwio::common::TypeWithId& type,
      const uint32_t sequence,
      std::function<void(IndexBuilder&)> onRecordPosition)
      : ColumnWriter{context, type.id, sequence},
        type_{type},
        indexBuilder_{context_.newIndexBuilder(
            newStream(StreamKind::StreamKind_ROW_INDEX))},
        onRecordPosition_{std::move(onRecordPosition)} {
    if (!isRoot()) {
      present_ =
          createBooleanRleEncoder(newStream(StreamKind::StreamKind_PRESENT));
    }
    auto options = StatisticsBuilderOptions::fromConfig(context.getConfigs());
    indexStatsBuilder_ = StatisticsBuilder::create(type.type->kind(), options);
    fileStatsBuilder_ = StatisticsBuilder::create(type.type->kind(), options);
  }

  uint64_t writeNulls(const VectorPtr& slice, const Ranges& ranges) {
    if (UNLIKELY(ranges.size() == 0)) {
      return 0;
    }
    auto nulls = slice->rawNulls();
    if (!slice->mayHaveNulls()) {
      present_->add(nullptr, ranges, nullptr);
    } else {
      present_->addBits(nulls, ranges, nullptr, false);
    }
    return 0;
  }

  // Function used only for the cases dealing with Dictionary vectors
  uint64_t writeNulls(const DecodedVector& decoded, const Ranges& ranges) {
    if (UNLIKELY(ranges.size() == 0)) {
      return 0;
    }
    if (!decoded.mayHaveNulls()) {
      present_->add(nullptr, ranges, nullptr);
    } else {
      present_->addBits(
          [&decoded](vector_size_t pos) { return decoded.isNullAt(pos); },
          ranges,
          nullptr,
          true);
    }
    return 0;
  }

  virtual void recordPosition() {
    if (onRecordPosition_) {
      onRecordPosition_(*indexBuilder_);
    }
    if (!isRoot()) {
      indexBuilder_->capturePresentStreamOffset();
      present_->recordPosition(*indexBuilder_);
    }
  }

  bool isRoot() const {
    return id_ == 0;
  }

  void suppressStream(StreamKind kind) {
    context_.suppressStream(DwrfStreamIdentifier{id_, sequence_, 0, kind});
  }

  template <typename T>
  T getConfig(const Config::Entry<T>& config) const {
    return context_.getConfig(config);
  }

  memory::MemoryPool& getMemoryPool(const MemoryUsageCategory& category) const {
    return context_.getMemoryPool(category);
  }

  bool isIndexEnabled() const {
    return context_.isIndexEnabled;
  }

  virtual bool useDictionaryEncoding() const {
    return (sequence_ == 0 ||
            !context_.getConfig(Config::MAP_FLAT_DISABLE_DICT_ENCODING)) &&
        !context_.isLowMemoryMode();
  }

  WriterContext::LocalDecodedVector decode(
      const VectorPtr& slice,
      const Ranges& ranges);

  const dwio::common::TypeWithId& type_;
  std::vector<std::unique_ptr<BaseColumnWriter>> children_;
  std::unique_ptr<IndexBuilder> indexBuilder_;
  std::unique_ptr<StatisticsBuilder> indexStatsBuilder_;
  std::unique_ptr<StatisticsBuilder> fileStatsBuilder_;

  std::unique_ptr<ByteRleEncoder> present_;
  bool hasNull_ = false;
  // callback used to inject the logic that captures positions for flat map
  // in_map stream
  const std::function<void(IndexBuilder&)> onRecordPosition_;

  VELOX_FRIEND_TEST(ColumnWriterTests, LowMemoryModeConfig);
  friend class ValueStatisticsBuilder;
};

} // namespace facebook::velox::dwrf
