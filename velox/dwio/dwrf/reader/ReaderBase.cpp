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

#include "velox/dwio/common/exception/Exception.h"

namespace facebook::velox::dwrf {

using dwio::common::ColumnStatistics;
using dwio::common::InputStream;
using dwio::common::LogType;
using dwio::common::Statistics;
using dwio::common::encryption::DecrypterFactory;
using encryption::DecryptionHandler;
using memory::MemoryPool;

FooterStatisticsImpl::FooterStatisticsImpl(
    const ReaderBase& reader,
    const StatsContext& statsContext) {
  auto& footer = reader.getFooter();
  auto& handler = reader.getDecryptionHandler();
  colStats_.resize(footer.statistics_size());
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
              stats->statistics(statsIndex), statsContext);
        }
      }
    }
  }
  // fill in unencrypted stats if not found in encryption groups
  for (int32_t i = 0; i < footer.statistics_size(); i++) {
    if (!colStats_[i]) {
      colStats_[i] =
          buildColumnStatisticsFromProto(footer.statistics(i), statsContext);
    }
  }
}

namespace {

// Key for the DataCache for the 'tail' of the file, i.e. the data we need to
// read when opening a file.
std::string TailKey(uint64_t filenum) {
  return fmt::format("{}_tail", filenum);
}

} // namespace

ReaderBase::ReaderBase(
    MemoryPool& pool,
    std::unique_ptr<InputStream> stream,
    std::shared_ptr<DecrypterFactory> decryptorFactory,
    std::shared_ptr<BufferedInputFactory> bufferedInputFactory,
    std::shared_ptr<dwio::common::DataCacheConfig> dataCacheConfig)
    : pool_{pool},
      stream_{std::move(stream)},
      arena_(std::make_unique<google::protobuf::Arena>()),
      decryptorFactory_(decryptorFactory),
      bufferedInputFactory_(
          bufferedInputFactory ? bufferedInputFactory
                               : BufferedInputFactory::baseFactoryShared()),
      dataCacheConfig_(dataCacheConfig) {
  input_ = bufferedInputFactory_->create(*stream_, pool, dataCacheConfig.get());

  // We may have cached the tail before, in which case we can skip the read.
  if (dataCacheConfig && dataCacheConfig->cache) {
    const std::string tailKey = TailKey(dataCacheConfig->filenum);
    std::string tail;
    if (dataCacheConfig->cache->get(tailKey, &tail)) {
      psLength_ = tail.data()[tail.size() - 1] & 0xff;
      postScript_ = ProtoUtils::readProto<proto::PostScript>(
          std::make_unique<SeekableArrayInputStream>(
              tail.data() + tail.size() - 1 - psLength_, psLength_));
      footer_ =
          google::protobuf::Arena::CreateMessage<proto::Footer>(arena_.get());
      ProtoUtils::readProtoInto(
          createDecompressedStream(
              std::make_unique<SeekableArrayInputStream>(
                  tail.data() + tail.size() - 1 - psLength_ -
                      postScript_->footerlength(),
                  postScript_->footerlength()),
              "File Footer"),
          footer_);
      schema_ = std::dynamic_pointer_cast<const RowType>(convertType(*footer_));
      DWIO_ENSURE_NOT_NULL(schema_, "invalid schema");
      if (postScript_->cachesize() > 0) {
        auto cacheBuffer = std::make_shared<dwio::common::DataBuffer<char>>(
            pool, postScript_->cachesize());
        memcpy(cacheBuffer->data(), tail.data(), postScript_->cachesize());
        cache_ = std::make_unique<StripeMetadataCache>(
            *postScript_, *footer_, std::move(cacheBuffer));
      }
      handler_ = DecryptionHandler::create(*footer_, decryptorFactory_.get());
      return;
    }
  }

  // read last bytes into buffer to get PostScript
  // If file is small, load the entire file.
  // TODO: make a config
  fileLength_ = stream_->getLength();
  DWIO_ENSURE(fileLength_ > 0, "ORC file is empty");

  auto preloadFile = fileLength_ <= FILE_PRELOAD_THRESHOLD;
  uint64_t readSize =
      preloadFile ? fileLength_ : std::min(fileLength_, DIRECTORY_SIZE_GUESS);
  DWIO_ENSURE_GE(readSize, 4, "File size too small");

  input_->enqueue({fileLength_ - readSize, readSize});
  input_->load(preloadFile ? LogType::FILE : LogType::FOOTER);

  // TODO: read footer from spectrum
  {
    const void* buf;
    int32_t ignored;
    auto lastByteStream = input_->read(fileLength_ - 1, 1, LogType::FOOTER);
    DWIO_ENSURE(lastByteStream->Next(&buf, &ignored), "failed to read");
    // Make sure 'lastByteStream' is live while dereferencing 'buf'.
    psLength_ = *static_cast<const char*>(buf) & 0xff;
  }
  DWIO_ENSURE_LE(
      psLength_ + 4, // 1 byte for post script len, 3 byte "ORC" header.
      fileLength_,
      "Corrupted file, Post script size is invalid");

  postScript_ = ProtoUtils::readProto<proto::PostScript>(
      input_->read(fileLength_ - psLength_ - 1, psLength_, LogType::FOOTER));

  uint64_t footerSize = postScript_->footerlength();
  uint64_t cacheSize = postScript_->cachesize();
  uint64_t tailSize = 1 + psLength_ + footerSize + cacheSize;

  // There are cases in warehouse, where RC/text files are stored
  // in ORC partition. This causes the Reader to SIGSEGV. The following
  // checks catches most of the corrupted files (but not all).
  DWIO_ENSURE_LT(
      footerSize, fileLength_, "Corrupted file, footer size is invalid");
  DWIO_ENSURE_LT(
      cacheSize, fileLength_, "Corrupted file, cache size is invalid");
  DWIO_ENSURE_LE(tailSize, fileLength_, "Corrupted file, tail size is invalid");

  if (postScript_->has_compression()) {
    DWIO_ENSURE(
        proto::CompressionKind_IsValid(postScript_->compression()),
        "Corrupted File, invalid compression kind ",
        postScript_->compression());
  }

  if (tailSize > readSize) {
    input_->enqueue({fileLength_ - tailSize, tailSize});
    input_->load(LogType::FOOTER);
  }

  auto footerStream = input_->read(
      fileLength_ - psLength_ - footerSize - 1, footerSize, LogType::FOOTER);
  footer_ = google::protobuf::Arena::CreateMessage<proto::Footer>(arena_.get());
  ProtoUtils::readProtoInto<proto::Footer>(
      createDecompressedStream(std::move(footerStream), "File Footer"),
      footer_);

  schema_ = std::dynamic_pointer_cast<const RowType>(convertType(*footer_));
  DWIO_ENSURE_NOT_NULL(schema_, "invalid schema");

  // load stripe index/footer cache
  if (cacheSize > 0) {
    auto cacheBuffer =
        std::make_shared<dwio::common::DataBuffer<char>>(pool, cacheSize);
    input_->read(fileLength_ - tailSize, cacheSize, LogType::FOOTER)
        ->readFully(cacheBuffer->data(), cacheSize);
    cache_ = std::make_unique<StripeMetadataCache>(
        *postScript_, *footer_, std::move(cacheBuffer));
  }

  // Insert the tail in the data cache so we can skip the disk read next time.
  if (dataCacheConfig && dataCacheConfig->cache) {
    std::unique_ptr<char[]> tail(new char[tailSize]);
    input_->read(fileLength_ - tailSize, tailSize, LogType::FOOTER)
        ->readFully(tail.get(), tailSize);
    const std::string tailKey = TailKey(dataCacheConfig->filenum);
    dataCacheConfig->cache->put(tailKey, {tail.get(), tailSize});
  }
  if (input_->shouldPrefetchStripes()) {
    auto numStripes = getFooter().stripes_size();
    for (auto i = 0; i < numStripes; i++) {
      const auto& stripe = getFooter().stripes(i);
      input_->enqueue(
          {stripe.offset() + stripe.indexlength() + stripe.datalength(),
           stripe.footerlength()});
    }
    if (numStripes) {
      input_->load(LogType::FOOTER);
    }
  }
  // initialize file decrypter
  handler_ = DecryptionHandler::create(*footer_, decryptorFactory_.get());
}

std::vector<uint64_t> ReaderBase::getRowsPerStripe() const {
  std::vector<uint64_t> rowsPerStripe;
  auto numStripes = getFooter().stripes_size();
  rowsPerStripe.reserve(numStripes);
  for (auto i = 0; i < numStripes; i++) {
    rowsPerStripe.push_back(getFooter().stripes(i).numberofrows());
  }
  return rowsPerStripe;
}

std::unique_ptr<Statistics> ReaderBase::getStatistics() const {
  StatsContext statsContext(getWriterName(), getWriterVersion());
  return std::make_unique<FooterStatisticsImpl>(*this, statsContext);
}

std::unique_ptr<ColumnStatistics> ReaderBase::getColumnStatistics(
    uint32_t index) const {
  DWIO_ENSURE_LT(
      index,
      static_cast<uint32_t>(footer_->statistics_size()),
      "column index out of range");
  StatsContext statsContext(getWriterVersion());
  if (!handler_->isEncrypted(index)) {
    auto& stats = footer_->statistics(index);
    return buildColumnStatisticsFromProto(stats, statsContext);
  }

  auto root = handler_->getEncryptionRoot(index);
  auto groupIndex = handler_->getEncryptionGroupIndex(index);
  auto& group = footer_->encryption().encryptiongroups(groupIndex);
  auto& decrypter = handler_->getEncryptionProviderByIndex(groupIndex);

  // if key is not loaded, return plaintext stats
  if (!decrypter.isKeyLoaded()) {
    auto& stats = footer_->statistics(index);
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
      stats->statistics(index - root), statsContext);
}

std::shared_ptr<const Type> ReaderBase::convertType(
    const proto::Footer& footer,
    uint32_t index) {
  DWIO_ENSURE_LT(
      index,
      folly::to<uint32_t>(footer.types_size()),
      "Corrupted file, invalid types");
  const auto& type = footer.types(index);
  switch (static_cast<int64_t>(type.kind())) {
    case proto::Type_Kind_BOOLEAN:
    case proto::Type_Kind_BYTE:
    case proto::Type_Kind_SHORT:
    case proto::Type_Kind_INT:
    case proto::Type_Kind_LONG:
    case proto::Type_Kind_FLOAT:
    case proto::Type_Kind_DOUBLE:
    case proto::Type_Kind_STRING:
    case proto::Type_Kind_BINARY:
    case proto::Type_Kind_TIMESTAMP:
      return createScalarType(static_cast<TypeKind>(type.kind()));
    case proto::Type_Kind_LIST:
      return ARRAY(convertType(footer, type.subtypes(0)));
    case proto::Type_Kind_MAP:
      return MAP(
          convertType(footer, type.subtypes(0)),
          convertType(footer, type.subtypes(1)));
    case proto::Type_Kind_UNION: {
      DWIO_RAISE("Union type is deprecated!");
    }

    case proto::Type_Kind_STRUCT: {
      std::vector<std::shared_ptr<const Type>> tl;
      tl.reserve(type.subtypes_size());
      std::vector<std::string> names;
      names.reserve(type.subtypes_size());
      for (int32_t i = 0; i < type.subtypes_size(); ++i) {
        auto child = convertType(footer, type.subtypes(i));
        names.push_back(type.fieldnames(i));
        tl.push_back(std::move(child));
      }

      // NOTE: There are empty dwrf files in data warehouse that has empty
      // struct as the root type. So the assumption that struct has at least one
      // child doesn't hold.
      return ROW(std::move(names), std::move(tl));
    }
    default:
      DWIO_RAISE("Unknown type kind");
  }
}

} // namespace facebook::velox::dwrf
