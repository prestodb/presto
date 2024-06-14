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

#include "velox/common/base/RandomUtil.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/SeekableInputStream.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/dwio/dwrf/common/Compression.h"
#include "velox/dwio/dwrf/common/Decryption.h"
#include "velox/dwio/dwrf/common/FileMetadata.h"
#include "velox/dwio/dwrf/common/Statistics.h"
#include "velox/dwio/dwrf/reader/StripeMetadataCache.h"
#include "velox/dwio/dwrf/utils/ProtoUtils.h"

namespace facebook::velox::dwrf {

class ReaderBase;

class FooterStatisticsImpl : public dwio::common::Statistics {
 private:
  std::vector<std::unique_ptr<dwio::common::ColumnStatistics>> colStats_;

  FooterStatisticsImpl(const FooterStatisticsImpl&) = delete;
  FooterStatisticsImpl(FooterStatisticsImpl&&) = delete;
  FooterStatisticsImpl& operator=(const FooterStatisticsImpl&) = delete;
  FooterStatisticsImpl& operator=(FooterStatisticsImpl&&) = delete;

 public:
  FooterStatisticsImpl(
      const ReaderBase& reader,
      const StatsContext& statsContext);

  virtual ~FooterStatisticsImpl() override = default;

  virtual const dwio::common::ColumnStatistics& getColumnStatistics(
      uint32_t columnId) const override {
    return *colStats_.at(columnId);
  }

  uint32_t getNumberOfColumns() const override {
    return static_cast<uint32_t>(colStats_.size());
  }
};

class ReaderBase {
 public:
  // create reader base from buffered input
  ReaderBase(
      memory::MemoryPool& pool,
      std::unique_ptr<dwio::common::BufferedInput> input,
      std::shared_ptr<dwio::common::encryption::DecrypterFactory>
          decryptorFactory = nullptr,
      uint64_t footerEstimatedSize =
          dwio::common::ReaderOptions::kDefaultFooterEstimatedSize,
      uint64_t filePreloadThreshold =
          dwio::common::ReaderOptions::kDefaultFilePreloadThreshold,
      dwio::common::FileFormat fileFormat = dwio::common::FileFormat::DWRF,
      bool fileColumnNamesReadAsLowerCase = false,
      std::shared_ptr<random::RandomSkipTracker> randomSkip = nullptr,
      std::shared_ptr<velox::common::ScanSpec> scanSpec = nullptr);

  ReaderBase(
      memory::MemoryPool& pool,
      std::unique_ptr<dwio::common::BufferedInput> input,
      dwio::common::FileFormat fileFormat);

  // create reader base from metadata
  ReaderBase(
      memory::MemoryPool& pool,
      std::unique_ptr<dwio::common::BufferedInput> input,
      std::unique_ptr<PostScript> ps,
      const proto::Footer* footer,
      std::unique_ptr<StripeMetadataCache> cache,
      std::unique_ptr<encryption::DecryptionHandler> handler = nullptr)
      : pool_{pool},
        postScript_{std::move(ps)},
        footer_{std::make_unique<FooterWrapper>(footer)},
        cache_{std::move(cache)},
        handler_{std::move(handler)},
        input_{std::move(input)},
        schema_{
            std::dynamic_pointer_cast<const RowType>(convertType(*footer_))},
        fileLength_{0},
        psLength_{0} {
    DWIO_ENSURE_NOT_NULL(schema_, "invalid schema");
    if (!handler_) {
      handler_ = encryption::DecryptionHandler::create(*footer);
    }
  }

  // for testing
  explicit ReaderBase(memory::MemoryPool& pool) : pool_{pool} {}

  virtual ~ReaderBase() = default;

  memory::MemoryPool& getMemoryPool() const {
    return pool_;
  }

  const PostScript& getPostScript() const {
    return *postScript_;
  }

  const FooterWrapper& getFooter() const {
    return *footer_;
  }

  const RowTypePtr& getSchema() const {
    return schema_;
  }

  void setSchema(RowTypePtr newSchema) {
    schema_ = std::move(newSchema);
  }

  const std::shared_ptr<const dwio::common::TypeWithId>& getSchemaWithId()
      const {
    if (!schemaWithId_) {
      if (scanSpec_) {
        schemaWithId_ = dwio::common::TypeWithId::create(schema_, *scanSpec_);
      } else {
        schemaWithId_ = dwio::common::TypeWithId::create(schema_);
      }
    }
    return schemaWithId_;
  }

  dwio::common::BufferedInput& getBufferedInput() const {
    return *input_;
  }

  const std::unique_ptr<StripeMetadataCache>& getMetadataCache() const {
    return cache_;
  }

  const encryption::DecryptionHandler& getDecryptionHandler() const {
    return *handler_;
  }

  uint64_t getFooterEstimatedSize() const {
    return footerEstimatedSize_;
  }

  uint64_t getFileLength() const {
    return fileLength_;
  }

  std::vector<uint64_t> getRowsPerStripe() const;

  uint64_t getPostScriptLength() const {
    return psLength_;
  }

  uint64_t getCompressionBlockSize() const {
    return postScript_->hasCompressionBlockSize()
        ? postScript_->compressionBlockSize()
        : common::DEFAULT_COMPRESSION_BLOCK_SIZE;
  }

  common::CompressionKind getCompressionKind() const {
    return postScript_->hasCompressionBlockSize()
        ? postScript_->compression()
        : common::CompressionKind::CompressionKind_NONE;
  }

  WriterVersion getWriterVersion() const {
    if (!postScript_->hasWriterVersion()) {
      return WriterVersion::ORIGINAL;
    }
    auto version = postScript_->writerVersion();
    return version <= WriterVersion_CURRENT
        ? static_cast<WriterVersion>(version)
        : WriterVersion::FUTURE;
  }

  const std::string& getWriterName() const {
    for (int32_t index = 0; index < footer_->metadataSize(); ++index) {
      auto entry = footer_->metadata(index);
      if (entry.name() == WRITER_NAME_KEY) {
        return entry.value();
      }
    }

    static const std::string kEmpty;
    return kEmpty;
  }

  std::unique_ptr<dwio::common::Statistics> getStatistics() const;

  std::unique_ptr<dwio::common::ColumnStatistics> getColumnStatistics(
      uint32_t index) const;

  std::unique_ptr<dwio::common::SeekableInputStream> createDecompressedStream(
      std::unique_ptr<dwio::common::SeekableInputStream> compressed,
      const std::string& streamDebugInfo,
      const dwio::common::encryption::Decrypter* decrypter = nullptr) const {
    return createDecompressor(
        getCompressionKind(),
        std::move(compressed),
        getCompressionBlockSize(),
        pool_,
        streamDebugInfo,
        decrypter);
  }

  template <typename T>
  std::unique_ptr<T> readProtoFromString(
      const std::string& data,
      const dwio::common::encryption::Decrypter* decrypter = nullptr) const {
    auto compressed = std::make_unique<dwio::common::SeekableArrayInputStream>(
        data.data(), data.size());
    return ProtoUtils::readProto<T>(createDecompressedStream(
        std::move(compressed), "Protobuf Metadata", decrypter));
  }

  google::protobuf::Arena* arena() const {
    return arena_.get();
  }

  DwrfFormat format() const {
    return postScript_->format();
  }

  const std::shared_ptr<random::RandomSkipTracker>& randomSkip() const {
    return randomSkip_;
  }

 private:
  static std::shared_ptr<const Type> convertType(
      const FooterWrapper& footer,
      uint32_t index = 0,
      bool fileColumnNamesReadAsLowerCase = false);

  memory::MemoryPool& pool_;
  std::unique_ptr<google::protobuf::Arena> arena_;
  std::unique_ptr<PostScript> postScript_;
  std::unique_ptr<FooterWrapper> footer_ = nullptr;
  std::unique_ptr<StripeMetadataCache> cache_;
  // Keeps factory alive for possibly async prefetch.
  std::shared_ptr<dwio::common::encryption::DecrypterFactory> decryptorFactory_;
  std::unique_ptr<encryption::DecryptionHandler> handler_;
  const uint64_t footerEstimatedSize_{
      dwio::common::ReaderOptions::kDefaultFooterEstimatedSize};
  const uint64_t filePreloadThreshold_{
      dwio::common::ReaderOptions::kDefaultFilePreloadThreshold};

  std::unique_ptr<dwio::common::BufferedInput> input_;
  const std::shared_ptr<random::RandomSkipTracker> randomSkip_;
  const std::shared_ptr<velox::common::ScanSpec> scanSpec_;
  RowTypePtr schema_;
  // Lazily populated
  mutable std::shared_ptr<const dwio::common::TypeWithId> schemaWithId_;
  uint64_t fileLength_;
  uint64_t psLength_;
};

} // namespace facebook::velox::dwrf
