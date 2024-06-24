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

#include "velox/common/base/BitSet.h"
#include "velox/dwio/common/ColumnSelector.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/SeekableInputStream.h"
#include "velox/dwio/dwrf/common/Common.h"
#include "velox/dwio/dwrf/reader/StreamLabels.h"
#include "velox/dwio/dwrf/reader/StripeDictionaryCache.h"
#include "velox/dwio/dwrf/reader/StripeReaderBase.h"

namespace facebook::velox::dwrf {

class StrideIndexProvider {
 public:
  virtual ~StrideIndexProvider() = default;
  virtual uint64_t getStrideIndex() const = 0;
};

/**
 * StreamInformation Implementation
 */
class StreamInformationImpl : public StreamInformation {
 private:
  DwrfStreamIdentifier streamId_;
  uint64_t offset_;
  uint64_t length_;
  bool useVInts_;

 public:
  static const StreamInformationImpl& getNotFound() {
    static const StreamInformationImpl NOT_FOUND;
    return NOT_FOUND;
  }

  StreamInformationImpl() : streamId_{DwrfStreamIdentifier::getInvalid()} {}
  StreamInformationImpl(uint64_t offset, const proto::Stream& stream)
      : streamId_(stream),
        offset_(offset),
        length_(stream.length()),
        useVInts_(stream.usevints()) {
    // PASS
  }

  ~StreamInformationImpl() override = default;

  StreamKind getKind() const override {
    return streamId_.kind();
  }

  uint32_t getNode() const override {
    return streamId_.encodingKey().node();
  }

  uint32_t getSequence() const override {
    return streamId_.encodingKey().sequence();
  }

  uint64_t getOffset() const override {
    return offset_;
  }

  uint64_t getLength() const override {
    return length_;
  }

  bool getUseVInts() const override {
    return useVInts_;
  }

  bool valid() const override {
    return streamId_.encodingKey().valid();
  }
};

class StripeStreams {
 public:
  virtual ~StripeStreams() = default;

  /**
   * Get the DwrfFormat for the stream
   * @return DwrfFormat
   */
  virtual DwrfFormat format() const = 0;

  /**
   * get column selector for current stripe reading session
   * @return column selector will hold column projection info
   */
  virtual const dwio::common::ColumnSelector& getColumnSelector() const = 0;

  // Get row reader options
  virtual const dwio::common::RowReaderOptions& getRowReaderOptions() const = 0;

  /**
   * Get the encoding for the given column for this stripe.
   */
  virtual const proto::ColumnEncoding& getEncoding(
      const EncodingKey&) const = 0;

  /**
   * Get the stream for the given column/kind in this stripe.
   * @param streamId stream identifier object
   * @param throwIfNotFound fail if a stream is required and not found
   * @return the new stream
   */
  virtual std::unique_ptr<dwio::common::SeekableInputStream> getStream(
      const DwrfStreamIdentifier& si,
      std::string_view label,
      bool throwIfNotFound) const = 0;

  /// Get the integer dictionary data for the given node and sequence.
  ///
  /// 'elementWidth' is the width of the data type of the column.
  /// 'dictionaryWidth' is *the width at which this is stored  in the reader.
  /// The non - selective path stores this always as int64, the selective path
  /// stores this at column width.
  virtual std::function<BufferPtr()> getIntDictionaryInitializerForNode(
      const EncodingKey& ek,
      uint64_t elementWidth,
      const StreamLabels& streamLabels,
      uint64_t dictionaryWidth = sizeof(int64_t)) = 0;

  virtual std::shared_ptr<StripeDictionaryCache> getStripeDictionaryCache() = 0;

  /**
   * visit all streams of given node and execute visitor logic
   * return number of streams visited
   */
  virtual uint32_t visitStreamsOfNode(
      uint32_t node,
      std::function<void(const StreamInformation&)> visitor) const = 0;

  /**
   * Get the value of useVInts for the given column in this stripe.
   * Defaults to true.
   * @param streamId stream identifier
   */
  virtual bool getUseVInts(const DwrfStreamIdentifier& streamId) const = 0;

  /**
   * Get the memory pool for this reader.
   */
  virtual memory::MemoryPool& getMemoryPool() const = 0;

  /**
   * Get stride index provider which is used by string dictionary reader to
   * get the row index stride index where next() happens
   */
  virtual const StrideIndexProvider& getStrideIndexProvider() const = 0;

  virtual int64_t stripeRows() const = 0;

  // Number of rows per row group. Last row group may have fewer rows.
  virtual uint32_t rowsPerRowGroup() const = 0;
};

class StripeStreamsBase : public StripeStreams {
 public:
  explicit StripeStreamsBase(velox::memory::MemoryPool* pool)
      : pool_{pool},
        stripeDictionaryCache_{std::make_shared<StripeDictionaryCache>(pool_)} {
  }
  virtual ~StripeStreamsBase() override = default;

  memory::MemoryPool& getMemoryPool() const override {
    return *pool_;
  }

  // For now just return DWRF, will refine when ORC has better support
  virtual DwrfFormat format() const override {
    return DwrfFormat::kDwrf;
  }

  std::function<BufferPtr()> getIntDictionaryInitializerForNode(
      const EncodingKey& ek,
      uint64_t elementWidth,
      const StreamLabels& streamLabels,
      uint64_t dictionaryWidth = sizeof(int64_t)) override;

  std::shared_ptr<StripeDictionaryCache> getStripeDictionaryCache() override {
    return stripeDictionaryCache_;
  }

 protected:
  memory::MemoryPool* pool_;
  std::shared_ptr<StripeDictionaryCache> stripeDictionaryCache_;
};

struct StripeReadState {
  std::shared_ptr<ReaderBase> readerBase;
  std::unique_ptr<const StripeMetadata> stripeMetadata;

  StripeReadState(
      std::shared_ptr<ReaderBase> readerBase,
      std::unique_ptr<const StripeMetadata> stripeMetadata)
      : readerBase{std::move(readerBase)},
        stripeMetadata{std::move(stripeMetadata)} {}
};

/**
 * StripeStream Implementation
 */
class StripeStreamsImpl : public StripeStreamsBase {
 public:
  static constexpr int64_t kUnknownStripeRows = -1;

  StripeStreamsImpl(
      std::shared_ptr<StripeReadState> readState,
      const dwio::common::ColumnSelector* selector,
      std::shared_ptr<BitSet> projectedNodes,
      const dwio::common::RowReaderOptions& opts,
      uint64_t stripeStart,
      int64_t stripeNumberOfRows,
      const StrideIndexProvider& provider,
      uint32_t stripeIndex)
      : StripeStreamsBase{&readState->readerBase->getMemoryPool()},
        readState_(std::move(readState)),
        selector_{selector},
        opts_{opts},
        projectedNodes_{std::move(projectedNodes)},
        stripeStart_{stripeStart},
        stripeNumberOfRows_{stripeNumberOfRows},
        provider_(provider),
        stripeIndex_{stripeIndex},
        readPlanLoaded_{false} {
    loadStreams();
  }

  ~StripeStreamsImpl() override = default;

  DwrfFormat format() const override {
    return readState_->readerBase->format();
  }

  const dwio::common::ColumnSelector& getColumnSelector() const override {
    return *selector_;
  }

  const dwio::common::RowReaderOptions& getRowReaderOptions() const override {
    return opts_;
  }

  const proto::ColumnEncoding& getEncoding(
      const EncodingKey& ek) const override {
    auto index = encodings_.find(ek);
    if (index != encodings_.end()) {
      return readState_->stripeMetadata->footer->encoding(index->second);
    }
    auto enc = decryptedEncodings_.find(ek);
    DWIO_ENSURE(
        enc != decryptedEncodings_.end(),
        "encoding not found: ",
        ek.toString());
    return enc->second;
  }

  // load data into buffer according to read plan
  void loadReadPlan();

  std::unique_ptr<dwio::common::SeekableInputStream> getCompressedStream(
      const DwrfStreamIdentifier& si,
      std::string_view label) const;

  uint64_t getStreamOffset(const DwrfStreamIdentifier& si) const {
    return getStreamInfo(si).getOffset() + stripeStart_;
  }

  uint64_t getStreamLength(const DwrfStreamIdentifier& si) const {
    return getStreamInfo(si).getLength();
  }

  folly::F14FastMap<uint32_t, std::vector<uint32_t>> getEncodingKeys() const;

  folly::F14FastMap<uint32_t, std::vector<DwrfStreamIdentifier>>
  getStreamIdentifiers() const;

  std::unique_ptr<dwio::common::SeekableInputStream> getStream(
      const DwrfStreamIdentifier& si,
      std::string_view label,
      bool throwIfNotFound) const override;

  uint32_t visitStreamsOfNode(
      uint32_t node,
      std::function<void(const StreamInformation&)> visitor) const override;

  bool getUseVInts(const DwrfStreamIdentifier& si) const override;

  const StrideIndexProvider& getStrideIndexProvider() const override {
    return provider_;
  }

  int64_t stripeRows() const override {
    VELOX_CHECK_NE(stripeNumberOfRows_, kUnknownStripeRows);
    return stripeNumberOfRows_;
  }

  uint32_t rowsPerRowGroup() const override {
    return readState_->readerBase->getFooter().rowIndexStride();
  }

 private:
  const StreamInformation& getStreamInfo(
      const DwrfStreamIdentifier& si,
      const bool throwIfNotFound = true) const {
    auto index = streams_.find(si);
    if (index == streams_.end()) {
      VELOX_CHECK(!throwIfNotFound, "stream info not found: ", si.toString());
      return StreamInformationImpl::getNotFound();
    }

    return index->second;
  }

  std::unique_ptr<dwio::common::SeekableInputStream> getIndexStreamFromCache(
      const StreamInformation& info) const;

  const dwio::common::encryption::Decrypter* getDecrypter(
      uint32_t nodeId) const {
    auto& handler = *readState_->stripeMetadata->handler;
    return handler.isEncrypted(nodeId)
        ? std::addressof(handler.getEncryptionProvider(nodeId))
        : nullptr;
  }

  void loadStreams();

  const std::shared_ptr<StripeReadState> readState_;
  const dwio::common::ColumnSelector* const selector_;
  const dwio::common::RowReaderOptions& opts_;
  // When selector_ is null, this needs to be passed in constructor; otherwise
  // leave it as null and it will be populated from selector_.
  std::shared_ptr<BitSet> projectedNodes_;
  const uint64_t stripeStart_;
  const int64_t stripeNumberOfRows_;
  const StrideIndexProvider& provider_;
  const uint32_t stripeIndex_;

  bool readPlanLoaded_;

  // map of stream id -> stream information
  folly::F14FastMap<
      DwrfStreamIdentifier,
      StreamInformationImpl,
      dwio::common::StreamIdentifierHash>
      streams_;
  folly::F14FastMap<EncodingKey, uint32_t, EncodingKeyHash> encodings_;
  folly::F14FastMap<EncodingKey, proto::ColumnEncoding, EncodingKeyHash>
      decryptedEncodings_;
};

/**
 * StripeInformation Implementation
 */
class StripeInformationImpl : public StripeInformation {
  uint64_t offset;
  uint64_t indexLength;
  uint64_t dataLength;
  uint64_t footerLength;
  uint64_t numRows;

 public:
  StripeInformationImpl(
      uint64_t _offset,
      uint64_t _indexLength,
      uint64_t _dataLength,
      uint64_t _footerLength,
      uint64_t _numRows)
      : offset(_offset),
        indexLength(_indexLength),
        dataLength(_dataLength),
        footerLength(_footerLength),
        numRows(_numRows) {}

  uint64_t getOffset() const override {
    return offset;
  }

  uint64_t getLength() const override {
    return indexLength + dataLength + footerLength;
  }
  uint64_t getIndexLength() const override {
    return indexLength;
  }

  uint64_t getDataLength() const override {
    return dataLength;
  }

  uint64_t getFooterLength() const override {
    return footerLength;
  }

  uint64_t getNumberOfRows() const override {
    return numRows;
  }
};

} // namespace facebook::velox::dwrf
