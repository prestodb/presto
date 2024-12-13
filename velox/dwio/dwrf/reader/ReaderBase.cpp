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

#include "velox/dwio/dwrf/reader/ReaderBase.h"

#include <fmt/format.h>

#include "velox/common/process/TraceContext.h"
#include "velox/dwio/common/Mutation.h"
#include "velox/dwio/common/exception/Exception.h"

namespace facebook::velox::dwrf {

using dwio::common::ColumnStatistics;
using dwio::common::FileFormat;
using dwio::common::LogType;
using dwio::common::Statistics;
using dwio::common::encryption::DecrypterFactory;
using encryption::DecryptionHandler;
using memory::MemoryPool;

FooterStatisticsImpl::FooterStatisticsImpl(
    const ReaderBase& reader,
    const StatsContext& statsContext) {
  auto& footer = reader.footer();
  auto& handler = reader.decryptionHandler();
  colStats_.resize(footer.statisticsSize());
  // fill in the encrypted stats
  if (handler.isEncrypted()) {
    auto& encryption = footer.encryption();
    for (uint32_t groupIndex = 0;
         groupIndex < encryption.encryptiongroups_size();
         ++groupIndex) {
      auto& group = encryption.encryptiongroups(groupIndex);
      auto& decrypter = handler.getEncryptionProviderByIndex(groupIndex);

      // it's possible user doesn't have access to all the encryption groups. In
      // such cases, avoid decrypting stats
      if (!decrypter.isKeyLoaded()) {
        continue;
      }

      for (uint32_t nodeIndex = 0; nodeIndex < group.nodes_size();
           ++nodeIndex) {
        auto node = group.nodes(nodeIndex);
        auto stats = reader.readProtoFromString<proto::FileStatistics>(
            group.statistics(nodeIndex), &decrypter);
        for (uint32_t statsIndex = 0; statsIndex < stats->statistics_size();
             ++statsIndex) {
          colStats_[node + statsIndex] = buildColumnStatisticsFromProto(
              ColumnStatisticsWrapper(&stats->statistics(statsIndex)),
              statsContext);
        }
      }
    }
  }
  // fill in unencrypted stats if not found in encryption groups
  for (int32_t i = 0; i < footer.statisticsSize(); i++) {
    if (!colStats_[i]) {
      colStats_[i] =
          buildColumnStatisticsFromProto(footer.statistics(i), statsContext);
    }
  }
}

ReaderBase::ReaderBase(
    MemoryPool& pool,
    std::unique_ptr<dwio::common::BufferedInput> input,
    FileFormat fileFormat)
    : ReaderBase(createReaderOptions(pool, fileFormat), std::move(input)) {}

namespace {

template <typename T>
std::unique_ptr<PostScript> parsePostScript(const char* input, int size) {
  auto impl = std::make_unique<T>();
  VELOX_CHECK(impl->ParseFromArray(input, size));
  return std::make_unique<PostScript>(std::move(impl));
}

template <typename T>
std::unique_ptr<FooterWrapper> parseFooter(
    dwio::common::SeekableInputStream* input,
    google::protobuf::Arena* arena) {
  auto* impl = google::protobuf::Arena::CreateMessage<T>(arena);
  VELOX_CHECK(impl->ParseFromZeroCopyStream(input));
  return std::make_unique<FooterWrapper>(impl);
}

} // namespace

ReaderBase::ReaderBase(
    const dwio::common::ReaderOptions& options,
    std::unique_ptr<dwio::common::BufferedInput> input)
    : options_{options},
      input_(std::move(input)),
      fileLength_(input_->getReadFile()->size()),
      arena_(std::make_unique<google::protobuf::Arena>()) {
  process::TraceContext trace("ReaderBase::ReaderBase");
  // TODO: make a config
  DWIO_ENSURE(fileLength_ > 0, "ORC file is empty");
  VELOX_CHECK_GE(fileLength_, 4, "File size too small");

  const auto preloadFile = fileLength_ <= options_.filePreloadThreshold();
  const int64_t footerBufSize =
      std::min(fileLength_, options_.footerEstimatedSize());
  const uint64_t readSize = preloadFile ? fileLength_ : footerBufSize;
  if (input_->supportSyncLoad()) {
    input_->enqueue({fileLength_ - readSize, readSize, "footer"});
    input_->load(preloadFile ? LogType::FILE : LogType::FOOTER);
  }

  // TODO: read footer from spectrum
  auto footerBuffer =
      AlignedBuffer::allocate<char>(footerBufSize, &options_.memoryPool());
  auto* rawFooterBuffer = footerBuffer->asMutable<char>();
  input_->read(fileLength_ - footerBufSize, footerBufSize, LogType::FOOTER)
      ->readFully(rawFooterBuffer, footerBufSize);
  int32_t footerOffset = footerBufSize - 1;
  psLength_ = static_cast<uint8_t>(rawFooterBuffer[footerOffset]);
  VELOX_CHECK_LE(
      psLength_ + 4, // 1 byte for post script len, 3 byte "ORC" header.
      fileLength_,
      "Corrupted file, Post script size is invalid");

  VELOX_CHECK_GE(footerOffset, psLength_);
  footerOffset -= psLength_;
  if (fileFormat() == FileFormat::DWRF) {
    postScript_ = parsePostScript<proto::PostScript>(
        rawFooterBuffer + footerOffset, psLength_);
  } else {
    postScript_ = parsePostScript<proto::orc::PostScript>(
        rawFooterBuffer + footerOffset, psLength_);
  }

  const uint64_t footerSize = postScript_->footerLength();
  const uint64_t cacheSize =
      postScript_->hasCacheSize() ? postScript_->cacheSize() : 0;
  const uint64_t tailSize = 1 + psLength_ + footerSize + cacheSize;
  footerBufferOverread_ =
      std::max<int>(0, footerBufSize - static_cast<int64_t>(tailSize));

  // There are cases in warehouse, where RC/text files are stored
  // in ORC partition. This causes the Reader to SIGSEGV. The following
  // checks catches most of the corrupted files (but not all).
  VELOX_CHECK_LT(
      footerSize, fileLength_, "Corrupted file, footer size is invalid");
  VELOX_CHECK_LT(
      cacheSize, fileLength_, "Corrupted file, cache size is invalid");
  VELOX_CHECK_LE(tailSize, fileLength_, "Corrupted file, tail size is invalid");

  VELOX_CHECK(
      (format() == DwrfFormat::kDwrf)
          ? proto::CompressionKind_IsValid(postScript_->compression())
          : proto::orc::CompressionKind_IsValid(postScript_->compression()),
      "Corrupted File, invalid compression kind ",
      postScript_->compression());

  if (input_->supportSyncLoad() && (tailSize > readSize)) {
    input_->enqueue({fileLength_ - tailSize, tailSize, "footer"});
    input_->load(LogType::FOOTER);
  }

  BufferPtr fullFooterBuffer;
  char* footerStart;
  if (footerOffset >= footerSize) {
    footerOffset -= footerSize;
    footerStart = rawFooterBuffer + footerOffset;
  } else {
    fullFooterBuffer =
        AlignedBuffer::allocate<char>(footerSize, &options_.memoryPool());
    footerStart = fullFooterBuffer->asMutable<char>();
    auto remainingBytes = footerSize - footerOffset;
    input_
        ->read(
            fileLength_ - footerSize - psLength_ - 1,
            remainingBytes,
            LogType::FOOTER)
        ->readFully(footerStart, remainingBytes);
    ::memcpy(footerStart + remainingBytes, rawFooterBuffer, footerOffset);
    footerOffset = 0;
  }
  auto decompressed = createDecompressedStream(
      std::make_unique<dwio::common::SeekableArrayInputStream>(
          footerStart, footerSize),
      "File Footer");
  if (fileFormat() == FileFormat::DWRF) {
    footer_ = parseFooter<proto::Footer>(decompressed.get(), arena_.get());
  } else {
    footer_ = parseFooter<proto::orc::Footer>(decompressed.get(), arena_.get());
  }

  stripeMetadataCacheBuffer_ = footerBuffer;
  stripeMetadataCacheBufferSize_ = footerOffset;

  schema_ = std::dynamic_pointer_cast<const RowType>(
      convertType(*footer_, 0, options_.fileColumnNamesReadAsLowerCase()));
  VELOX_CHECK_NOT_NULL(schema_, "invalid schema");

  // initialize file decrypter
  handler_ =
      DecryptionHandler::create(*footer_, options_.decrypterFactory().get());
}

void ReaderBase::loadCache() {
  if (!stripeMetadataCacheBuffer_) {
    // NOTE: we only expect call this once as stripeMetadataCacheBuffer_ is
    // reset on the first call.
    return;
  }
  const uint64_t footerSize = postScript_->footerLength();
  const uint64_t cacheSize =
      postScript_->hasCacheSize() ? postScript_->cacheSize() : 0;
  const uint64_t tailSize = 1 + psLength_ + footerSize + cacheSize;
  if (cacheSize > 0) {
    VELOX_CHECK_EQ(format(), DwrfFormat::kDwrf);
    const uint64_t cacheOffset = fileLength_ - tailSize;
    if (input_->shouldPrefetchStripes()) {
      cache_ = std::make_unique<StripeMetadataCache>(
          postScript_->cacheMode(),
          *footer_,
          input_->read(cacheOffset, cacheSize, LogType::FOOTER));
      input_->load(LogType::FOOTER);
    } else {
      auto cacheBuffer = std::make_shared<dwio::common::DataBuffer<char>>(
          options_.memoryPool(), cacheSize);
      auto* target = cacheBuffer->data();
      auto* source = stripeMetadataCacheBuffer_->as<char>();
      auto copySize = cacheSize;
      if (cacheSize > stripeMetadataCacheBufferSize_) {
        auto remainingBytes = cacheSize - stripeMetadataCacheBufferSize_;
        auto stream =
            input_->read(cacheOffset, remainingBytes, LogType::FOOTER);
        stream->readFully(target, remainingBytes);
        target += remainingBytes;
        copySize -= remainingBytes;
      } else {
        source += stripeMetadataCacheBufferSize_ - cacheSize;
      }
      ::memcpy(target, source, copySize);
      cache_ = std::make_unique<StripeMetadataCache>(
          postScript_->cacheMode(), *footer_, std::move(cacheBuffer));
    }
  }
  if (!cache_ && input_->shouldPrefetchStripes()) {
    const auto numStripes = footer().stripesSize();
    for (auto i = 0; i < numStripes; i++) {
      const auto stripe = footer().stripes(i);
      input_->enqueue(
          {stripe.offset() + stripe.indexLength() + stripe.dataLength(),
           stripe.footerLength(),
           "stripe_footer"});
    }
    if (numStripes > 0) {
      input_->load(LogType::FOOTER);
    }
  }
  // Release the memory as we no longer need it.
  stripeMetadataCacheBuffer_.reset();
}

std::vector<uint64_t> ReaderBase::rowsPerStripe() const {
  std::vector<uint64_t> rowsPerStripe;
  auto numStripes = footer().stripesSize();
  rowsPerStripe.reserve(numStripes);
  for (auto i = 0; i < numStripes; i++) {
    rowsPerStripe.push_back(footer().stripes(i).numberOfRows());
  }
  return rowsPerStripe;
}

std::unique_ptr<Statistics> ReaderBase::statistics() const {
  StatsContext statsContext(writerName(), writerVersion());
  return std::make_unique<FooterStatisticsImpl>(*this, statsContext);
}

std::unique_ptr<ColumnStatistics> ReaderBase::columnStatistics(
    uint32_t index) const {
  VELOX_CHECK_LT(
      index,
      static_cast<uint32_t>(footer_->statisticsSize()),
      "column index out of range");
  StatsContext statsContext(writerVersion());
  if (!handler_->isEncrypted(index)) {
    auto stats = footer_->statistics(index);
    return buildColumnStatisticsFromProto(stats, statsContext);
  }

  auto root = handler_->getEncryptionRoot(index);
  auto groupIndex = handler_->getEncryptionGroupIndex(index);
  auto& group = footer_->encryption().encryptiongroups(groupIndex);
  auto& decrypter = handler_->getEncryptionProviderByIndex(groupIndex);

  // if key is not loaded, return plaintext stats
  if (!decrypter.isKeyLoaded()) {
    auto stats = footer_->statistics(index);
    return buildColumnStatisticsFromProto(stats, statsContext);
  }

  // find the right offset inside the group
  uint32_t nodeIndex = 0;
  for (; nodeIndex < group.nodes_size(); ++nodeIndex) {
    if (group.nodes(nodeIndex) == root) {
      break;
    }
  }

  DWIO_ENSURE_LT(nodeIndex, group.nodes_size());
  auto stats = readProtoFromString<proto::FileStatistics>(
      group.statistics(nodeIndex), &decrypter);
  return buildColumnStatisticsFromProto(
      ColumnStatisticsWrapper(&stats->statistics(index - root)), statsContext);
}

std::shared_ptr<const Type> ReaderBase::convertType(
    const FooterWrapper& footer,
    uint32_t index,
    bool fileColumnNamesReadAsLowerCase) {
  VELOX_CHECK_LT(
      index,
      folly::to<uint32_t>(footer.typesSize()),
      "Corrupted file, invalid types");
  const auto type = footer.types(index);
  switch (type.kind()) {
    case TypeKind::BOOLEAN:
      return BOOLEAN();
    case TypeKind::TINYINT:
      return TINYINT();
    case TypeKind::SMALLINT:
      return SMALLINT();
    case TypeKind::INTEGER:
      return INTEGER();
    case TypeKind::BIGINT:
      if (type.format() == DwrfFormat::kOrc &&
          type.getOrcPtr()->kind() == proto::orc::Type_Kind_DECIMAL) {
        return DECIMAL(
            type.getOrcPtr()->precision(), type.getOrcPtr()->scale());
      }
      return BIGINT();
    case TypeKind::HUGEINT:
      if (type.format() == DwrfFormat::kOrc &&
          type.getOrcPtr()->kind() == proto::orc::Type_Kind_DECIMAL) {
        return DECIMAL(
            type.getOrcPtr()->precision(), type.getOrcPtr()->scale());
      }
      return HUGEINT();
    case TypeKind::REAL:
      return REAL();
    case TypeKind::DOUBLE:
      return DOUBLE();
    case TypeKind::VARCHAR:
      return VARCHAR();
    case TypeKind::VARBINARY:
      return VARBINARY();
    case TypeKind::TIMESTAMP:
      return TIMESTAMP();
    case TypeKind::ARRAY:
      return ARRAY(convertType(
          footer, type.subtypes(0), fileColumnNamesReadAsLowerCase));
    case TypeKind::MAP:
      return MAP(
          convertType(footer, type.subtypes(0), fileColumnNamesReadAsLowerCase),
          convertType(
              footer, type.subtypes(1), fileColumnNamesReadAsLowerCase));
    case TypeKind::ROW: {
      std::vector<std::shared_ptr<const Type>> types;
      types.reserve(type.subtypesSize());
      std::vector<std::string> names;
      names.reserve(type.subtypesSize());
      for (int32_t i = 0; i < type.subtypesSize(); ++i) {
        auto childType = convertType(
            footer, type.subtypes(i), fileColumnNamesReadAsLowerCase);
        auto childName = type.fieldNames(i);
        if (fileColumnNamesReadAsLowerCase) {
          folly::toLowerAscii(childName);
        }
        names.push_back(std::move(childName));
        types.push_back(std::move(childType));
      }

      // NOTE: There are empty dwrf files in data warehouse that has empty
      // struct as the root type. So the assumption that struct has at least one
      // child doesn't hold.
      return ROW(std::move(names), std::move(types));
    }
    default:
      DWIO_RAISE("Unknown type kind");
  }
}

} // namespace facebook::velox::dwrf
