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

namespace facebook::velox::dwrf {

// This class aggregates statistics of all value writers.
// Since a map value column can be complex (it is a tree of nodes), we need to
// aggregae every level of the tree, across all value writers.
class ValueStatisticsBuilder {
 public:
  ValueStatisticsBuilder(
      WriterContext& context,
      uint32_t id,
      std::unique_ptr<StatisticsBuilder> statisticsBuilder,
      std::vector<std::unique_ptr<ValueStatisticsBuilder>> children)
      : context_{context},
        id_{id},
        statisticsBuilder_{std::move(statisticsBuilder)},
        children_{std::move(children)} {}

  static std::unique_ptr<const ValueStatisticsBuilder> create(
      WriterContext& context,
      const dwio::common::TypeWithId& root) {
    auto options = StatisticsBuilderOptions::fromConfig(context.getConfigs());
    return create_(context, root, options);
  }

  void merge(const BaseColumnWriter& writer) const {
    statisticsBuilder_->merge(*writer.indexStatsBuilder_);
    DWIO_ENSURE(
        children_.size() == writer.children_.size(),
        "Value statistics writer children mismatch");
    for (int32_t i = 0; i < children_.size(); ++i) {
      children_[i]->merge(*writer.children_[i]);
    }
  }

  uint64_t writeFileStats(
      std::function<proto::ColumnStatistics&(uint32_t)> statsFactory) const {
    auto& stats = statsFactory(id_);
    statisticsBuilder_->toProto(stats);
    uint64_t size = context_.getNodeSize(id_);
    for (int32_t i = 0; i < children_.size(); ++i) {
      size += children_[i]->writeFileStats(statsFactory);
    }
    stats.set_size(size);
    return size;
  }

 private:
  static std::unique_ptr<ValueStatisticsBuilder> create_(
      WriterContext& context,
      const dwio::common::TypeWithId& type,
      const StatisticsBuilderOptions& options) {
    auto builder = StatisticsBuilder::create(*type.type, options);

    std::vector<std::unique_ptr<ValueStatisticsBuilder>> children{};
    for (size_t i = 0; i < type.size(); ++i) {
      children.push_back(create_(context, *type.childAt(i), options));
    }

    return std::make_unique<ValueStatisticsBuilder>(
        context, type.id, std::move(builder), std::move(children));
  }

  WriterContext& context_;
  const uint32_t id_;
  const std::unique_ptr<StatisticsBuilder> statisticsBuilder_;
  const std::vector<std::unique_ptr<ValueStatisticsBuilder>> children_;
};

// ValueWriter is used to write flat-map value columns.
// It holds a column writer to write the values and an in-map encoder to
// indicate if a value exists in the map (to distinguish null values from
// not-in-map values)
class ValueWriter {
 public:
  ValueWriter(
      const uint32_t sequence,
      const proto::KeyInfo& keyInfo,
      WriterContext& context,
      const dwio::common::TypeWithId& type,
      uint32_t inMapSize)
      : sequence_{sequence},
        keyInfo_{keyInfo},
        inMap_{createBooleanRleEncoder(context.newStream(
            {type.id, sequence, 0, StreamKind::StreamKind_IN_MAP}))},
        columnWriter_{BaseColumnWriter::create(
            context,
            type,
            sequence,
            [this](auto& indexBuilder) {
              inMap_->recordPosition(indexBuilder);
            })},
        inMapBuffer_{
            context.getMemoryPool(MemoryUsageCategory::GENERAL),
            inMapSize},
        ranges_{},
        collectMapStats_{context.getConfig(Config::MAP_STATISTICS)} {}

  void addOffset(uint64_t offset, uint64_t inMapIndex) {
    if (UNLIKELY(inMapBuffer_[inMapIndex])) {
      DWIO_RAISE("Duplicate key in map");
    }

    ranges_.add(offset, offset + 1);
    inMapBuffer_[inMapIndex] = 1;
  }

  uint64_t writeBuffers(const VectorPtr& values, uint32_t mapCount) {
    if (mapCount) {
      inMap_->add(inMapBuffer_.data(), Ranges::of(0, mapCount), nullptr);
    }

    if (values) {
      return columnWriter_->write(values, ranges_);
    }
    return 0;
  }

  // used for struct encoding writer
  uint64_t writeBuffers(
      const VectorPtr& values,
      const Ranges& nonNullRanges,
      const BufferPtr& inMapBuffer /* all 1 */) {
    if (nonNullRanges.size()) {
      inMap_->add(
          inMapBuffer->as<char>(),
          Ranges::of(0, nonNullRanges.size()),
          nullptr);
    }

    if (values) {
      return columnWriter_->write(values, nonNullRanges);
    }
    return 0;
  }

  void backfill(uint32_t count) {
    if (count == 0) {
      return;
    }

    inMapBuffer_.reserve(count);
    std::memset(inMapBuffer_.data(), 0, count);
    inMap_->add(inMapBuffer_.data(), Ranges::of(0, count), nullptr);
  }

  uint32_t getSequence() const {
    return sequence_;
  }

  void createIndexEntry(
      const ValueStatisticsBuilder& valueStatsBuilder,
      MapStatisticsBuilder& mapStatsBuilder) {
    if (collectMapStats_) {
      mapStatsBuilder.addValues(keyInfo_, *columnWriter_->indexStatsBuilder_);
    }
    valueStatsBuilder.merge(*columnWriter_);
    columnWriter_->createIndexEntry();
  }

  void flush(std::function<proto::ColumnEncoding&(uint32_t)> encodingFactory) {
    inMap_->flush();
    columnWriter_->flush(encodingFactory, [&](auto& encoding) {
      *encoding.mutable_key() = keyInfo_;
    });
  }

  void reset() {
    columnWriter_->reset();
  }

  void resizeBuffers(size_t inMap) {
    inMapBuffer_.reserve(inMap);
    std::memset(inMapBuffer_.data(), 0, inMap);
    ranges_.clear();
  }

 private:
  uint32_t sequence_;
  const proto::KeyInfo keyInfo_;
  std::unique_ptr<ByteRleEncoder> inMap_;
  std::unique_ptr<BaseColumnWriter> columnWriter_;
  dwio::common::DataBuffer<char> inMapBuffer_;
  Ranges ranges_;
  const bool collectMapStats_;
};

namespace {

template <TypeKind K>
struct TypeInfo {};

template <>
struct TypeInfo<TypeKind::TINYINT> {
  using StatisticsBuilder = IntegerStatisticsBuilder;
};

template <>
struct TypeInfo<TypeKind::SMALLINT> {
  using StatisticsBuilder = IntegerStatisticsBuilder;
};

template <>
struct TypeInfo<TypeKind::INTEGER> {
  using StatisticsBuilder = IntegerStatisticsBuilder;
};

template <>
struct TypeInfo<TypeKind::BIGINT> {
  using StatisticsBuilder = IntegerStatisticsBuilder;
};

template <>
struct TypeInfo<TypeKind::VARCHAR> {
  using StatisticsBuilder = StringStatisticsBuilder;
};

template <>
struct TypeInfo<TypeKind::VARBINARY> {
  using StatisticsBuilder = BinaryStatisticsBuilder;
};

} // namespace

template <TypeKind K>
class FlatMapColumnWriter : public BaseColumnWriter {
 public:
  FlatMapColumnWriter(
      WriterContext& context,
      const dwio::common::TypeWithId& type,
      const uint32_t sequence);

  uint64_t write(const VectorPtr& slice, const Ranges& ranges) override;

  void flush(
      std::function<proto::ColumnEncoding&(uint32_t)> encodingFactory,
      std::function<void(proto::ColumnEncoding&)> encodingOverride) override;

  void createIndexEntry() override;

  void reset() override;

  uint64_t writeFileStats(std::function<proto::ColumnStatistics&(uint32_t)>
                              statsFactory) const override;

 private:
  using KeyType = typename TypeTraits<K>::NativeType;

  void setEncoding(proto::ColumnEncoding& encoding) const override;

  ValueWriter& getValueWriter(KeyType key, uint32_t inMapSize);

  // write() calls writeMap() or writeRow() depending on input type
  uint64_t writeMap(const VectorPtr& slice, const Ranges& ranges);
  uint64_t writeRow(const VectorPtr& slice, const Ranges& ranges);

  void clearNodes();

  // Map of value writers for each key in the dictionary. Needs referential
  // stability because a member variable is captured by reference by lambda
  // function passed to another class.
  folly::F14NodeMap<KeyType, ValueWriter, folly::Hash> valueWriters_;

  // Captures row count for each completed stride in current stripe
  std::vector<size_t> rowsInStrides_;

  // Captures current row count for current (incomplete) stride
  size_t rowsInCurrentStride_{0};

  // Remember key and value types. Needed for constructing value writers
  const dwio::common::TypeWithId& keyType_;
  const dwio::common::TypeWithId& valueType_;

  std::unique_ptr<typename TypeInfo<K>::StatisticsBuilder> keyFileStatsBuilder_;
  std::unique_ptr<const ValueStatisticsBuilder> valueFileStatsBuilder_;
  const uint32_t maxKeyCount_;

  // Stores column keys as string in case of StringView pointers
  std::vector<std::string> stringKeys_;

  // Stores column keys if writing with RowVector input
  std::vector<KeyType> structKeys_;
};

template <>
class FlatMapColumnWriter<TypeKind::INVALID> {
 public:
  static std::unique_ptr<BaseColumnWriter> create(
      WriterContext& context,
      const dwio::common::TypeWithId& type,
      const uint32_t sequence) {
    DWIO_ENSURE_EQ(type.size(), 2, "Map should have exactly two children");

    auto kind = type.childAt(0)->type->kind();
    switch (kind) {
      case TypeKind::TINYINT:
        return std::make_unique<FlatMapColumnWriter<TypeKind::TINYINT>>(
            context, type, sequence);
      case TypeKind::SMALLINT:
        return std::make_unique<FlatMapColumnWriter<TypeKind::SMALLINT>>(
            context, type, sequence);
      case TypeKind::INTEGER:
        return std::make_unique<FlatMapColumnWriter<TypeKind::INTEGER>>(
            context, type, sequence);
      case TypeKind::BIGINT:
        return std::make_unique<FlatMapColumnWriter<TypeKind::BIGINT>>(
            context, type, sequence);
      case TypeKind::VARBINARY:
        return std::make_unique<FlatMapColumnWriter<TypeKind::VARBINARY>>(
            context, type, sequence);
      case TypeKind::VARCHAR:
        return std::make_unique<FlatMapColumnWriter<TypeKind::VARCHAR>>(
            context, type, sequence);
      default:
        DWIO_RAISE("Not supported key type: ", kind);
    }
  }
};

} // namespace facebook::velox::dwrf
