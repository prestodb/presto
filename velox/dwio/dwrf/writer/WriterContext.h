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

#include <limits>
#include "velox/common/base/GTestMacros.h"
#include "velox/common/time/CpuWallTimer.h"
#include "velox/dwio/dwrf/common/Common.h"
#include "velox/dwio/dwrf/common/Compression.h"
#include "velox/dwio/dwrf/common/Config.h"
#include "velox/dwio/dwrf/common/EncoderUtil.h"
#include "velox/dwio/dwrf/writer/IndexBuilder.h"
#include "velox/dwio/dwrf/writer/IntegerDictionaryEncoder.h"
#include "velox/dwio/dwrf/writer/PhysicalSizeAggregator.h"
#include "velox/dwio/dwrf/writer/RatioTracker.h"
#include "velox/vector/DecodedVector.h"

namespace facebook::velox::dwrf {
using dwio::common::BufferedOutputStream;
using dwio::common::DataBufferHolder;
using dwio::common::compression::CompressionBufferPool;

enum class MemoryUsageCategory { DICTIONARY, OUTPUT_STREAM, GENERAL };

class WriterContext : public CompressionBufferPool {
 public:
  WriterContext(
      const std::shared_ptr<const Config>& config,
      std::shared_ptr<memory::MemoryPool> pool,
      const dwio::common::MetricsLogPtr& metricLogger =
          dwio::common::MetricsLog::voidLog(),
      std::unique_ptr<encryption::EncryptionHandler> handler = nullptr);

  ~WriterContext() override;

  bool hasStream(const DwrfStreamIdentifier& stream) const {
    return streams_.find(stream) != streams_.end();
  }

  const DataBufferHolder& getStream(const DwrfStreamIdentifier& stream) const {
    return streams_.at(stream);
  }

  void addBuffer(
      const DwrfStreamIdentifier& stream,
      folly::StringPiece buffer) {
    streams_.at(stream).take(buffer);
  }

  size_t getStreamCount() const {
    return streams_.size();
  }

  // Stream content is not compressed until flush (stripe flush or full buffer),
  // so accounting for the memory usage can be inflated even aside from the
  // capacity vs actual usage problem. However, this is ok as an upperbound for
  // flush policy evaluation and would be more accurate after flush.
  std::unique_ptr<BufferedOutputStream> newStream(
      const DwrfStreamIdentifier& stream) {
    VELOX_CHECK(
        !hasStream(stream), "Stream already exists: {}", stream.toString());

    streams_.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(stream),
        std::forward_as_tuple(
            getMemoryPool(MemoryUsageCategory::OUTPUT_STREAM),
            compressionBlockSize(),
            getConfig(Config::COMPRESSION_BLOCK_SIZE_MIN),
            getConfig(Config::COMPRESSION_BLOCK_SIZE_EXTEND_RATIO)));
    auto& holder = streams_.at(stream);
    auto encrypter = handler_->isEncrypted(stream.encodingKey().node())
        ? std::addressof(
              handler_->getEncryptionProvider(stream.encodingKey().node()))
        : nullptr;
    return newStream(compression_, holder, encrypter);
  }

  std::unique_ptr<DataBufferHolder> newDataBufferHolder(
      dwio::common::FileSink* sink = nullptr) {
    return std::make_unique<DataBufferHolder>(
        getMemoryPool(MemoryUsageCategory::OUTPUT_STREAM),
        compressionBlockSize(),
        getConfig(Config::COMPRESSION_BLOCK_SIZE_MIN),
        getConfig(Config::COMPRESSION_BLOCK_SIZE_EXTEND_RATIO),
        sink);
  }

  std::unique_ptr<BufferedOutputStream> newStream(
      common::CompressionKind kind,
      DataBufferHolder& holder,
      const dwio::common::encryption::Encrypter* encrypter = nullptr) {
    return createCompressor(kind, *this, holder, *config_, encrypter);
  }

  template <typename T>
  IntegerDictionaryEncoder<T>& getIntDictionaryEncoder(
      const EncodingKey& encodingKey,
      velox::memory::MemoryPool& dictionaryPool,
      velox::memory::MemoryPool& generalPool) {
    auto result = dictEncoders_.find(encodingKey);
    if (result == dictEncoders_.end()) {
      auto emplaceResult = dictEncoders_.emplace(
          encodingKey,
          std::make_unique<IntegerDictionaryEncoder<T>>(
              dictionaryPool,
              generalPool,
              getConfig(Config::DICTIONARY_SORT_KEYS),
              createDirectEncoder</* isSigned */ true>(
                  newStream(
                      {encodingKey.node(),
                       encodingKey.sequence(),
                       0,
                       StreamKind::StreamKind_DICTIONARY_DATA}),
                  getConfig(Config::USE_VINTS),
                  sizeof(T))));
      result = emplaceResult.first;
    } else {
      result->second->bumpRefCount();
    }
    return static_cast<IntegerDictionaryEncoder<T>&>(*result->second);
  }

  std::unique_ptr<IndexBuilder> newIndexBuilder(
      std::unique_ptr<BufferedOutputStream> stream) const {
    return indexBuilderFactory_
        ? indexBuilderFactory_(std::move(stream))
        : std::make_unique<IndexBuilder>(std::move(stream));
  }

  void suppressStream(const DwrfStreamIdentifier& stream) {
    VELOX_CHECK(hasStream(stream));
    auto& collector = streams_.at(stream);
    collector.suppress();
  }

  bool isStreamPaged(uint32_t nodeId) const {
    return (compression_ != common::CompressionKind::CompressionKind_NONE) ||
        handler_->isEncrypted(nodeId);
  }

  void nextStripe() {
    fileRowCount_ += stripeRowCount_;
    rowsPerStripe_.push_back(stripeRowCount_);
    stripeRowCount_ = 0;
    indexRowCount_ = 0;
    fileRawSize_ += stripeRawSize_;
    stripeRawSize_ = 0;
    ++stripeIndex_;

    for (auto& pair : streams_) {
      pair.second.reset();
    }
  }

  void incRowCount(uint64_t count) {
    stripeRowCount_ += count;
    if (indexEnabled_) {
      indexRowCount_ += count;
    }
  }

  void incRawSize(uint64_t size) {
    stripeRawSize_ += size;
  }

  memory::MemoryPool& getMemoryPool(const MemoryUsageCategory& category);

  int64_t getMemoryUsage(const MemoryUsageCategory& category) const;

  int64_t getTotalMemoryUsage() const;

  int64_t getMemoryBudget() const {
    return pool_->maxCapacity();
  }

  /// Returns the available memory reservations aggregated from all the memory
  /// pools.
  int64_t availableMemoryReservation() const;

  /// Releases unused memory reservation aggregated from all the memory pools.
  void releaseMemoryReservation();

  const encryption::EncryptionHandler& getEncryptionHandler() const {
    return *handler_;
  }

  template <typename T>
  T getConfig(const Config::Entry<T>& config) const {
    return config_->get(config);
  }

  const Config& getConfigs() const {
    return *config_;
  }

  void iterateUnSuppressedStreams(
      std::function<void(
          std::pair<const DwrfStreamIdentifier, DataBufferHolder>&)> callback) {
    for (auto& pair : streams_) {
      if (!pair.second.isSuppressed()) {
        callback(pair);
      }
    }
  }

  // Used by FlatMapColumnWriter to remove previously registered
  // dictionary encoders. This logic exists due to how FlatMapColumnWriter
  // cleans up its value writer streams upon reset().
  void removeAllIntDictionaryEncodersOnNode(
      std::function<bool(uint32_t)> predicate) {
    auto iter = dictEncoders_.begin();
    while (iter != dictEncoders_.end()) {
      if (predicate(iter->first.node())) {
        iter = dictEncoders_.erase(iter);
      } else {
        ++iter;
      }
    }
  }

  virtual void removeStreams(
      std::function<bool(const DwrfStreamIdentifier&)> predicate) {
    auto it = streams_.begin();
    while (it != streams_.end()) {
      if (predicate(it->first)) {
        it = streams_.erase(it);
        continue;
      }
      ++it;
    }
  }

  void initBuffer();

  std::unique_ptr<dwio::common::DataBuffer<char>> getBuffer(
      uint64_t size) override {
    VELOX_CHECK_NOT_NULL(compressionBuffer_);
    VELOX_CHECK_GE(compressionBuffer_->size(), size);
    return std::move(compressionBuffer_);
  }

  void returnBuffer(
      std::unique_ptr<dwio::common::DataBuffer<char>> buffer) override {
    VELOX_CHECK_NOT_NULL(buffer);
    VELOX_CHECK_NULL(compressionBuffer_);
    compressionBuffer_ = std::move(buffer);
  }

  void incrementNodeSize(uint32_t node, uint64_t size) {
    nodeSize_[node] += size;
  }

  uint64_t getNodeSize(uint32_t node) {
    if (nodeSize_.count(node) > 0) {
      return nodeSize_[node];
    }
    return 0;
  }

  void recordCompressionRatio(uint64_t compressedSize) {
    compressionRatioTracker_.takeSample(stripeRawSize_, compressedSize);
  }

  void recordFlushOverhead(uint64_t flushOverhead) {
    flushOverheadRatioTracker_.takeSample(
        stripeRawSize_ + getMemoryUsage(MemoryUsageCategory::DICTIONARY),
        flushOverhead);
  }

  void recordAverageRowSize() {
    rowSizeTracker_.takeSample(stripeRowCount(), stripeRawSize());
  }

  float getCompressionRatio() const {
    return compressionRatioTracker_.getEstimatedRatio();
  }

  float getFlushOverheadRatio() const {
    return flushOverheadRatioTracker_.getEstimatedRatio();
  }

  float getAverageRowSize() const {
    return rowSizeTracker_.getEstimatedRatio();
  }

  /// This is parity with bbio. Doesn't seem like we do anything special when
  /// estimated compression ratio is larger than 1.0f. In fact, given how
  /// compression works, we should cap the ratio used for estimate at 1.0f.
  /// Estimates prior to first flush can be quite inaccurate depending on
  /// encoding, so we rely on a tuned compression ratio initial guess unless we
  /// want to produce estimates at ColumnWriter level.
  ///
  /// TODO: expose config for initial guess?
  int64_t getEstimatedStripeSize(size_t dataRawSize) const {
    return ceil(compressionRatioTracker_.getEstimatedRatio() * dataRawSize);
  }

  int64_t getEstimatedOutputStreamSize() const {
    return (int64_t)std::ceil(
        (getMemoryUsage(MemoryUsageCategory::OUTPUT_STREAM) +
         getMemoryUsage(MemoryUsageCategory::DICTIONARY)) /
        getConfig(Config::COMPRESSION_BLOCK_SIZE_EXTEND_RATIO));
  }

  /// The additional memory usage of writers during flush typically comes from
  /// flushing remaining data to output buffer, or all of it in the case of
  /// dictionary encoding. In either case, the maximal memory consumption is
  /// O(k * raw data size). The actual coefficient k can differ
  /// from encoding to encoding, and thus should be schema aware.
  size_t getEstimatedFlushOverhead(size_t dataRawSize) const {
    return ceil(flushOverheadRatioTracker_.getEstimatedRatio() * dataRawSize);
  }

  /// We currently use previous stripe raw size as the proxy for the expected
  /// stripe raw size. For the first stripe, we are more conservative about
  /// flush overhead memory unless we know otherwise, e.g. perhaps from
  /// encoding DB work.
  /// This can be fitted linearly just like flush overhead or perhaps
  /// figured out from the schema.
  /// This can be simplified with Slice::estimateMemory().
  size_t estimateNextWriteSize(size_t numRows) const {
    // This is 0 for first slice. We are assuming reasonable input for now.
    return folly::to<size_t>(ceil(getAverageRowSize() * numRows));
  }

  bool checkLowMemoryMode() const {
    return checkLowMemoryMode_;
  }

  void setLowMemoryMode() {
    lowMemoryMode_ = true;
  }

  bool isLowMemoryMode() const {
    return lowMemoryMode_;
  }

  PhysicalSizeAggregator& getPhysicalSizeAggregator(uint32_t node) {
    return *physicalSizeAggregators_.at(node);
  }

  void recordPhysicalSize(const DwrfStreamIdentifier& streamId, uint64_t size) {
    auto& agg = getPhysicalSizeAggregator(streamId.encodingKey().node());
    agg.recordSize(streamId, size);
  }

  bool indexEnabled() const {
    return indexEnabled_;
  }

  uint32_t indexStride() const {
    return indexStride_;
  }

  common::CompressionKind compression() const {
    return compression_;
  }

  uint64_t compressionBlockSize() const {
    return compressionBlockSize_;
  }

  bool shareFlatMapDictionaries() const {
    return shareFlatMapDictionaries_;
  }

  uint64_t stripeSizeFlushThreshold() const {
    return stripeSizeFlushThreshold_;
  }

  uint64_t dictionarySizeFlushThreshold() const {
    return dictionarySizeFlushThreshold_;
  }

  bool streamSizeAboveThresholdCheckEnabled() const {
    return streamSizeAboveThresholdCheckEnabled_;
  }

  uint64_t rawDataSizePerBatch() const {
    return rawDataSizePerBatch_;
  }

  const dwio::common::MetricsLogPtr& metricLogger() const {
    return metricLogger_;
  }

  uint32_t stripeIndex() const {
    return stripeIndex_;
  }

  void testingIncStripeIndex() {
    ++stripeIndex_;
  }

  uint64_t fileRowCount() const {
    return fileRowCount_;
  }

  void incFileRowCount(uint64_t increment) {
    fileRowCount_ += increment;
  }

  uint64_t stripeRowCount() const {
    return stripeRowCount_;
  }

  void setStripeRowCount(uint64_t stripeRowCount) {
    stripeRowCount_ = stripeRowCount;
  }

  uint32_t indexRowCount() const {
    return indexRowCount_;
  }

  void resetIndexRowCount() {
    indexRowCount_ = 0;
  }

  uint64_t fileRawSize() const {
    return fileRawSize_;
  }

  void incFileRawSize(uint64_t increment) {
    fileRawSize_ += increment;
  }

  uint64_t stripeRawSize() const {
    return stripeRawSize_;
  }

  void setStripeRawSize(uint64_t stripeRawSize) {
    stripeRawSize_ = stripeRawSize;
  }

  const std::vector<uint64_t>& rowsPerStripe() const {
    return rowsPerStripe_;
  }

  CpuWallTiming& flushTiming() {
    return flushTiming_;
  }

  const CpuWallTiming& flushTiming() const {
    return flushTiming_;
  }

  void buildPhysicalSizeAggregators(
      const velox::dwio::common::TypeWithId& type,
      PhysicalSizeAggregator* parent = nullptr) {
    switch (type.type()->kind()) {
      case TypeKind::ROW: {
        physicalSizeAggregators_.emplace(
            type.id(), std::make_unique<PhysicalSizeAggregator>(parent));
        auto* current = physicalSizeAggregators_.at(type.id()).get();
        for (const auto& child : type.getChildren()) {
          buildPhysicalSizeAggregators(*child, current);
        }
        break;
      }
      case TypeKind::MAP: {
        // MapPhysicalSizeAggregator is only required for flatmaps, but it will
        // behave just fine as a regular PhysicalSizeAggregator.
        physicalSizeAggregators_.emplace(
            type.id(), std::make_unique<MapPhysicalSizeAggregator>(parent));
        auto* current = physicalSizeAggregators_.at(type.id()).get();
        buildPhysicalSizeAggregators(*type.childAt(0), current);
        buildPhysicalSizeAggregators(*type.childAt(1), current);
        break;
      }
      case TypeKind::ARRAY: {
        physicalSizeAggregators_.emplace(
            type.id(), std::make_unique<PhysicalSizeAggregator>(parent));
        auto* current = physicalSizeAggregators_.at(type.id()).get();
        buildPhysicalSizeAggregators(*type.childAt(0), current);
        break;
      }
      case TypeKind::BOOLEAN:
        [[fallthrough]];
      case TypeKind::TINYINT:
        [[fallthrough]];
      case TypeKind::SMALLINT:
        [[fallthrough]];
      case TypeKind::INTEGER:
        [[fallthrough]];
      case TypeKind::BIGINT:
        [[fallthrough]];
      case TypeKind::HUGEINT:
        [[fallthrough]];
      case TypeKind::REAL:
        [[fallthrough]];
      case TypeKind::DOUBLE:
        [[fallthrough]];
      case TypeKind::VARCHAR:
        [[fallthrough]];
      case TypeKind::VARBINARY:
        [[fallthrough]];
      case TypeKind::TIMESTAMP:
        physicalSizeAggregators_.emplace(
            type.id(), std::make_unique<PhysicalSizeAggregator>(parent));
        break;
      case TypeKind::UNKNOWN:
        [[fallthrough]];
      case TypeKind::FUNCTION:
        [[fallthrough]];
      case TypeKind::OPAQUE:
        [[fallthrough]];
      case TypeKind::INVALID:
        [[fallthrough]];
      default:
        VELOX_FAIL(
            "Unexpected type kind {} encountered when building "
            "physical size aggregator for node {}.",
            type.type()->toString(),
            type.id());
    }
  }

  class LocalDecodedVector {
   public:
    explicit LocalDecodedVector(WriterContext& context)
        : context_(context), vector_(context_.getDecodedVector()) {}

    LocalDecodedVector(LocalDecodedVector&& other) noexcept
        : context_{other.context_}, vector_{std::move(other.vector_)} {}

    LocalDecodedVector& operator=(LocalDecodedVector&& other) = delete;

    ~LocalDecodedVector() {
      if (vector_) {
        context_.releaseDecodedVector(std::move(vector_));
      }
    }

    DecodedVector& get() {
      return *vector_;
    }

   private:
    WriterContext& context_;
    std::unique_ptr<velox::DecodedVector> vector_;
  };

  LocalDecodedVector getLocalDecodedVector() {
    return LocalDecodedVector{*this};
  }

  SelectivityVector& getSharedSelectivityVector(velox::vector_size_t size) {
    if (FOLLY_UNLIKELY(selectivityVector_ == nullptr)) {
      selectivityVector_ = std::make_unique<velox::SelectivityVector>(size);
    } else {
      selectivityVector_->resize(size);
    }
    return *selectivityVector_;
  }

  void abort();

  dwio::common::DataBuffer<char>* testingCompressionBuffer() const {
    return compressionBuffer_.get();
  }

 private:
  void validateConfigs() const;

  std::unique_ptr<velox::DecodedVector> getDecodedVector() {
    if (decodedVectorPool_.empty()) {
      return std::make_unique<velox::DecodedVector>();
    }
    auto vector = std::move(decodedVectorPool_.back());
    decodedVectorPool_.pop_back();
    return vector;
  }

  void releaseDecodedVector(std::unique_ptr<velox::DecodedVector>&& vector) {
    decodedVectorPool_.push_back(std::move(vector));
  }

  const std::shared_ptr<const Config> config_;
  const std::shared_ptr<memory::MemoryPool> pool_;
  const std::shared_ptr<memory::MemoryPool> dictionaryPool_;
  const std::shared_ptr<memory::MemoryPool> outputStreamPool_;
  const std::shared_ptr<memory::MemoryPool> generalPool_;
  // config
  const bool indexEnabled_;
  const uint32_t indexStride_;
  const common::CompressionKind compression_;
  const uint64_t compressionBlockSize_;
  const bool shareFlatMapDictionaries_;
  const uint64_t stripeSizeFlushThreshold_;
  const uint64_t dictionarySizeFlushThreshold_;
  const bool streamSizeAboveThresholdCheckEnabled_;
  const uint64_t rawDataSizePerBatch_;
  const dwio::common::MetricsLogPtr metricLogger_;

  // Map needs referential stability because reference to map value is stored by
  // another class.
  folly::F14NodeMap<
      DwrfStreamIdentifier,
      DataBufferHolder,
      dwio::common::StreamIdentifierHash>
      streams_;
  folly::F14NodeMap<uint32_t, std::unique_ptr<PhysicalSizeAggregator>>
      physicalSizeAggregators_;
  folly::F14FastMap<
      EncodingKey,
      std::unique_ptr<AbstractIntegerDictionaryEncoder>,
      EncodingKeyHash>
      dictEncoders_;
  std::function<std::unique_ptr<IndexBuilder>(
      std::unique_ptr<BufferedOutputStream>)>
      indexBuilderFactory_;
  std::unique_ptr<dwio::common::DataBuffer<char>> compressionBuffer_;
  // A pool of reusable DecodedVectors.
  std::vector<std::unique_ptr<velox::DecodedVector>> decodedVectorPool_;
  // Reusable SelectivityVector
  std::unique_ptr<velox::SelectivityVector> selectivityVector_;

  std::unique_ptr<encryption::EncryptionHandler> handler_;
  folly::F14FastMap<uint32_t, uint64_t> nodeSize_;
  CompressionRatioTracker compressionRatioTracker_;
  FlushOverheadRatioTracker flushOverheadRatioTracker_;
  // This might not be the best idea if client actually sends batches
  // of similar sizes. We will find out through production traffic.
  AverageRowSizeTracker rowSizeTracker_;
  bool checkLowMemoryMode_;
  bool lowMemoryMode_{false};

  /// stats
  uint32_t stripeIndex_{0};
  uint64_t fileRowCount_{0};
  uint64_t stripeRowCount_{0};
  uint32_t indexRowCount_{0};
  std::vector<uint64_t> rowsPerStripe_;

  uint64_t fileRawSize_{0};
  uint64_t stripeRawSize_{0};

  CpuWallTiming flushTiming_{};

  friend class IntegerColumnWriterDirectEncodingIndexTest;
  friend class StringColumnWriterDictionaryEncodingIndexTest;
  friend class StringColumnWriterDirectEncodingIndexTest;
  // TODO: remove once writer code is consolidated
  template <typename TestType>
  friend class WriterEncodingIndexTest2;

  VELOX_FRIEND_TEST(WriterContextTest, GetIntDictionaryEncoder);
  VELOX_FRIEND_TEST(WriterContextTest, RemoveIntDictionaryEncoderForNode);
};

} // namespace facebook::velox::dwrf
