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

#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/SeekableInputStream.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/dwio/dwrf/common/Compression.h"
#include "velox/dwio/dwrf/common/Statistics.h"
#include "velox/dwio/dwrf/reader/StripeMetadataCache.h"
#include "velox/dwio/dwrf/utils/ProtoUtils.h"

namespace facebook::velox::dwrf {

constexpr uint64_t DIRECTORY_SIZE_GUESS = 1024 * 1024;
constexpr uint64_t FILE_PRELOAD_THRESHOLD = 1024 * 1024 * 8;

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
  static constexpr uint64_t kDefaultFileNum =
      std::numeric_limits<uint64_t>::max();

 public:
  // create reader base from input stream
  ReaderBase(
      memory::MemoryPool& pool,
      std::unique_ptr<dwio::common::InputStream> stream,
      std::shared_ptr<dwio::common::encryption::DecrypterFactory>
          decryptorFactory = nullptr,
      std::shared_ptr<dwio::common::BufferedInputFactory> bufferedInputFactory =
          nullptr,
      uint64_t fileNum = kDefaultFileNum,
      dwio::common::FileFormat fileFormat = dwio::common::FileFormat::DWRF);

  ReaderBase(
      memory::MemoryPool& pool,
      std::unique_ptr<dwio::common::InputStream> stream,
      dwio::common::FileFormat fileFormat);

  // create reader base from metadata
  ReaderBase(
      memory::MemoryPool& pool,
      std::unique_ptr<dwio::common::InputStream> stream,
      std::unique_ptr<PostScript> ps,
      proto::Footer* footer,
      std::unique_ptr<StripeMetadataCache> cache,
      std::unique_ptr<encryption::DecryptionHandler> handler = nullptr)
      : pool_{pool},
        stream_{std::move(stream)},
        postScript_{std::move(ps)},
        footer_{footer},
        cache_{std::move(cache)},
        handler_{std::move(handler)},
        input_{
            stream_
                ? std::make_unique<dwio::common::BufferedInput>(*stream_, pool_)
                : nullptr},
        schema_{
            std::dynamic_pointer_cast<const RowType>(convertType(*footer_))},
        fileLength_{0},
        psLength_{0} {
    DWIO_ENSURE(footer_->GetArena());
    DWIO_ENSURE_NOT_NULL(schema_, "invalid schema");
    if (!handler_) {
      handler_ = encryption::DecryptionHandler::create(*footer_);
    }
  }

  // for testing
  explicit ReaderBase(memory::MemoryPool& pool) : pool_{pool} {}

  virtual ~ReaderBase() = default;

  memory::MemoryPool& getMemoryPool() const {
    return pool_;
  }

  dwio::common::InputStream& getStream() const {
    return *stream_;
  }

  uint64_t getFileNum() const {
    return fileNum_;
  }

  const PostScript& getPostScript() const {
    return *postScript_;
  }

  const proto::Footer& getFooter() const {
    return *footer_;
  }

  const std::shared_ptr<const RowType>& getSchema() const {
    return schema_;
  }

  const std::shared_ptr<const dwio::common::TypeWithId>& getSchemaWithId()
      const {
    if (!schemaWithId_) {
      schemaWithId_ = dwio::common::TypeWithId::create(schema_);
    }
    return schemaWithId_;
  }

  dwio::common::BufferedInput& getBufferedInput() const {
    return *input_;
  }

  const dwio::common::BufferedInputFactory& bufferedInputFactory() const {
    return bufferedInputFactory_
        ? *bufferedInputFactory_
        : *dwio::common::BufferedInputFactory::baseFactory();
  }

  const std::unique_ptr<StripeMetadataCache>& getMetadataCache() const {
    return cache_;
  }

  const encryption::DecryptionHandler& getDecryptionHandler() const {
    return *handler_;
  }

  uint64_t getFileLength() const {
    return fileLength_;
  }

  std::vector<uint64_t> getRowsPerStripe() const;

  uint64_t getPostScriptLength() const {
    return psLength_;
  }

  uint64_t getCompressionBlockSize() const {
    return postScript_->compressionBlockSize();
  }

  dwio::common::CompressionKind getCompressionKind() const {
    return postScript_->compression();
  }

  WriterVersion getWriterVersion() const {
    auto version = postScript_->writerVersion();
    return version <= WriterVersion_CURRENT
        ? static_cast<WriterVersion>(version)
        : WriterVersion::FUTURE;
  }

  const std::string& getWriterName() const {
    for (auto& entry : footer_->metadata()) {
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

  dwio::common::FileFormat getFileFormat() const {
    return postScript_->fileFormat();
  }

 private:
  static std::shared_ptr<const Type> convertType(
      const proto::Footer& footer,
      uint32_t index = 0);

  memory::MemoryPool& pool_;
  std::unique_ptr<dwio::common::InputStream> stream_;
  std::unique_ptr<google::protobuf::Arena> arena_;
  std::unique_ptr<PostScript> postScript_;

  proto::Footer* footer_ = nullptr;
  uint64_t fileNum_;
  std::unique_ptr<StripeMetadataCache> cache_;
  // Keeps factory alive for possibly async prefetch.
  std::shared_ptr<dwio::common::encryption::DecrypterFactory> decryptorFactory_;
  std::unique_ptr<encryption::DecryptionHandler> handler_;
  std::shared_ptr<dwio::common::BufferedInputFactory> bufferedInputFactory_;

  std::unique_ptr<dwio::common::BufferedInput> input_;
  RowTypePtr schema_;
  // Lazily populated
  mutable std::shared_ptr<const dwio::common::TypeWithId> schemaWithId_;
  uint64_t fileLength_;
  uint64_t psLength_;
};

} // namespace facebook::velox::dwrf
