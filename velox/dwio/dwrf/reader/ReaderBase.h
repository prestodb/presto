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
  /// Creates reader base from buffered input.
  ReaderBase(
      const dwio::common::ReaderOptions& options,
      std::unique_ptr<dwio::common::BufferedInput> input);

  /// Creates reader base from buffered input.
  /// It is kept here for backward compatibility with Meta's internal usage.
  ReaderBase(
      memory::MemoryPool& pool,
      std::unique_ptr<dwio::common::BufferedInput> input,
      dwio::common::FileFormat fileFormat = dwio::common::FileFormat::DWRF);

  /// Creates reader base from metadata.
  ReaderBase(
      memory::MemoryPool& pool,
      std::unique_ptr<dwio::common::BufferedInput> input,
      std::unique_ptr<PostScript> ps,
      const proto::Footer* footer,
      std::unique_ptr<StripeMetadataCache> cache,
      std::unique_ptr<encryption::DecryptionHandler> handler = nullptr)
      : postScript_{std::move(ps)},
        footer_{std::make_unique<FooterWrapper>(footer)},
        cache_{std::move(cache)},
        handler_{std::move(handler)},
        options_{dwio::common::ReaderOptions(&pool)},
        input_{std::move(input)},
        fileLength_{0},
        schema_{
            std::dynamic_pointer_cast<const RowType>(convertType(*footer_))},
        psLength_{0} {
    VELOX_CHECK_NOT_NULL(schema_, "invalid schema");
    if (!handler_) {
      handler_ = encryption::DecryptionHandler::create(*footer);
    }
  }

  // for testing
  explicit ReaderBase(const dwio::common::ReaderOptions& options)
      : options_{options}, fileLength_{0} {}

  virtual ~ReaderBase() = default;

  const dwio::common::ReaderOptions& readerOptions() const {
    return options_;
  }

  memory::MemoryPool& memoryPool() const {
    return options_.memoryPool();
  }

  const PostScript& postScript() const {
    return *postScript_;
  }

  const FooterWrapper& footer() const {
    return *footer_;
  }

  const RowTypePtr& schema() const {
    return schema_;
  }

  dwio::common::FileFormat fileFormat() const {
    if (options_.fileFormat() == dwio::common::FileFormat::ORC) {
      return dwio::common::FileFormat::ORC;
    }

    return dwio::common::FileFormat::DWRF;
  }

  void setSchema(RowTypePtr newSchema) {
    schema_ = std::move(newSchema);
  }

  const std::shared_ptr<const dwio::common::TypeWithId>& schemaWithId() const {
    if (!schemaWithId_) {
      if (options_.scanSpec()) {
        schemaWithId_ =
            dwio::common::TypeWithId::create(schema_, *options_.scanSpec());
      } else {
        schemaWithId_ = dwio::common::TypeWithId::create(schema_);
      }
    }
    return schemaWithId_;
  }

  dwio::common::BufferedInput& bufferedInput() const {
    return *input_;
  }

  const std::unique_ptr<StripeMetadataCache>& metadataCache() const {
    return cache_;
  }

  const encryption::DecryptionHandler& decryptionHandler() const {
    return *handler_;
  }

  uint64_t footerEstimatedSize() const {
    return options_.footerEstimatedSize();
  }

  uint64_t fileLength() const {
    return fileLength_;
  }

  std::vector<uint64_t> rowsPerStripe() const;

  uint64_t postScriptLength() const {
    return psLength_;
  }

  uint64_t compressionBlockSize() const {
    return postScript_->hasCompressionBlockSize()
        ? postScript_->compressionBlockSize()
        : common::DEFAULT_COMPRESSION_BLOCK_SIZE;
  }

  common::CompressionKind compressionKind() const {
    return postScript_->hasCompressionBlockSize()
        ? postScript_->compression()
        : common::CompressionKind::CompressionKind_NONE;
  }

  WriterVersion writerVersion() const {
    if (!postScript_->hasWriterVersion()) {
      return WriterVersion::ORIGINAL;
    }
    auto version = postScript_->writerVersion();
    return version <= WriterVersion_CURRENT
        ? static_cast<WriterVersion>(version)
        : WriterVersion::FUTURE;
  }

  const std::string& writerName() const {
    for (int32_t index = 0; index < footer_->metadataSize(); ++index) {
      auto entry = footer_->metadata(index);
      if (entry.name() == WRITER_NAME_KEY) {
        return entry.value();
      }
    }

    static const std::string kEmpty;
    return kEmpty;
  }

  std::unique_ptr<dwio::common::Statistics> statistics() const;

  std::unique_ptr<dwio::common::ColumnStatistics> columnStatistics(
      uint32_t index) const;

  std::unique_ptr<dwio::common::SeekableInputStream> createDecompressedStream(
      std::unique_ptr<dwio::common::SeekableInputStream> compressed,
      const std::string& streamDebugInfo,
      const dwio::common::encryption::Decrypter* decrypter = nullptr) const {
    return createDecompressor(
        compressionKind(),
        std::move(compressed),
        compressionBlockSize(),
        options_.memoryPool(),
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
    return options_.randomSkip();
  }

 private:
  static std::shared_ptr<const Type> convertType(
      const FooterWrapper& footer,
      uint32_t index = 0,
      bool fileColumnNamesReadAsLowerCase = false);

  static dwio::common::ReaderOptions createReaderOptions(
      memory::MemoryPool& pool,
      dwio::common::FileFormat fileFormat) {
    dwio::common::ReaderOptions options(&pool);
    options.setFileFormat(fileFormat);
    return options;
  }

  std::unique_ptr<google::protobuf::Arena> arena_;
  std::unique_ptr<PostScript> postScript_;
  std::unique_ptr<FooterWrapper> footer_ = nullptr;
  std::unique_ptr<StripeMetadataCache> cache_;
  std::unique_ptr<encryption::DecryptionHandler> handler_;

  const dwio::common::ReaderOptions options_;
  const std::unique_ptr<dwio::common::BufferedInput> input_;
  const uint64_t fileLength_;

  RowTypePtr schema_;
  // Lazily populated
  mutable std::shared_ptr<const dwio::common::TypeWithId> schemaWithId_;
  uint64_t psLength_;
};

} // namespace facebook::velox::dwrf
