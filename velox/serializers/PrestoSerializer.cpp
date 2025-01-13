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
#include "velox/serializers/PrestoSerializer.h"

#include <optional>

#include <folly/lang/Bits.h>

#include "velox/common/base/Crc.h"
#include "velox/common/memory/ByteStream.h"
#include "velox/serializers/PrestoBatchVectorSerializer.h"
#include "velox/serializers/PrestoHeader.h"
#include "velox/serializers/PrestoIterativeVectorSerializer.h"
#include "velox/serializers/PrestoSerializerDeserializationUtils.h"
#include "velox/serializers/PrestoSerializerEstimationUtils.h"
#include "velox/serializers/PrestoSerializerSerializationUtils.h"
#include "velox/serializers/PrestoVectorLexer.h"
#include "velox/serializers/VectorStream.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::serializer::presto {

using SerdeOpts = PrestoVectorSerde::PrestoOptions;

namespace {
int64_t computeChecksum(
    ByteInputStream* source,
    int codecMarker,
    int numRows,
    int32_t uncompressedSize,
    int32_t compressedSize) {
  const auto offset = source->tellp();
  const bool compressed = codecMarker & detail::kCompressedBitMask;
  if (compressed) {
    VELOX_CHECK_LT(compressedSize, uncompressedSize);
  }
  const int32_t dataSize = compressed ? compressedSize : uncompressedSize;
  bits::Crc32 crc32;
  if (FOLLY_UNLIKELY(source->remainingSize() < dataSize)) {
    VELOX_FAIL(
        "Tried to read {} bytes, larger than what's remained in source {} "
        "bytes. Source details: {}",
        dataSize,
        source->remainingSize(),
        source->toString());
  }
  auto remainingBytes = dataSize;
  while (remainingBytes > 0) {
    auto data = source->nextView(remainingBytes);
    if (FOLLY_UNLIKELY(data.size() == 0)) {
      VELOX_FAIL(
          "Reading 0 bytes from source. Source details: {}",
          source->toString());
    }
    crc32.process_bytes(data.data(), data.size());
    remainingBytes -= data.size();
  }

  crc32.process_bytes(&codecMarker, 1);
  crc32.process_bytes(&numRows, 4);
  crc32.process_bytes(&uncompressedSize, 4);
  auto checksum = crc32.checksum();

  source->seekp(offset);

  return checksum;
}

PrestoVectorSerde::PrestoOptions toPrestoOptions(
    const VectorSerde::Options* options) {
  if (options == nullptr) {
    return PrestoVectorSerde::PrestoOptions();
  }
  auto prestoOptions =
      dynamic_cast<const PrestoVectorSerde::PrestoOptions*>(options);
  VELOX_CHECK_NOT_NULL(
      prestoOptions, "Serde options are not Presto-compatible");
  return *prestoOptions;
}
} // namespace

void PrestoVectorSerde::estimateSerializedSize(
    const BaseVector* vector,
    const folly::Range<const IndexRange*>& ranges,
    vector_size_t** sizes,
    Scratch& scratch) {
  detail::estimateSerializedSizeInt(
      vector->loadedVector(), ranges, sizes, scratch);
}

void PrestoVectorSerde::estimateSerializedSize(
    const BaseVector* vector,
    const folly::Range<const vector_size_t*>& rows,
    vector_size_t** sizes,
    Scratch& scratch) {
  detail::estimateSerializedSizeInt(
      vector->loadedVector(), rows, sizes, scratch);
}

std::unique_ptr<IterativeVectorSerializer>
PrestoVectorSerde::createIterativeSerializer(
    RowTypePtr type,
    int32_t numRows,
    StreamArena* streamArena,
    const Options* options) {
  const auto prestoOptions = toPrestoOptions(options);
  return std::make_unique<detail::PrestoIterativeVectorSerializer>(
      type, numRows, streamArena, prestoOptions);
}

std::unique_ptr<BatchVectorSerializer> PrestoVectorSerde::createBatchSerializer(
    memory::MemoryPool* pool,
    const Options* options) {
  const auto prestoOptions = toPrestoOptions(options);
  return std::make_unique<detail::PrestoBatchVectorSerializer>(
      pool, prestoOptions);
}

void PrestoVectorSerde::deserialize(
    ByteInputStream* source,
    velox::memory::MemoryPool* pool,
    RowTypePtr type,
    RowVectorPtr* result,
    vector_size_t resultOffset,
    const Options* options) {
  const auto prestoOptions = toPrestoOptions(options);
  const auto codec =
      common::compressionKindToCodec(prestoOptions.compressionKind);
  auto maybeHeader = detail::PrestoHeader::read(source);
  VELOX_CHECK(
      maybeHeader.hasValue(),
      fmt::format(
          "PrestoPage header is invalid: {}", maybeHeader.error().message()));
  auto const header = std::move(maybeHeader.value());

  int64_t actualCheckSum = 0;
  if (detail::isChecksumBitSet(header.pageCodecMarker)) {
    actualCheckSum = computeChecksum(
        source,
        header.pageCodecMarker,
        header.numRows,
        header.uncompressedSize,
        header.compressedSize);
  }

  VELOX_CHECK_EQ(
      header.checksum, actualCheckSum, "Received corrupted serialized page.");

  if (resultOffset > 0) {
    VELOX_CHECK_NOT_NULL(*result);
    VELOX_CHECK_EQ(result->use_count(), 1);
    (*result)->resize(resultOffset + header.numRows);
  } else if (*result && result->use_count() == 1) {
    VELOX_CHECK(
        *(*result)->type() == *type,
        "Unexpected type: {} vs. {}",
        (*result)->type()->toString(),
        type->toString());
    (*result)->prepareForReuse();
    (*result)->resize(header.numRows);
  } else {
    *result = BaseVector::create<RowVector>(type, header.numRows, pool);
  }

  VELOX_CHECK_EQ(
      header.checksum, actualCheckSum, "Received corrupted serialized page.");

  if (!detail::isCompressedBitSet(header.pageCodecMarker)) {
    detail::readTopColumns(
        *source, type, pool, *result, resultOffset, prestoOptions);
  } else {
    auto compressBuf = folly::IOBuf::create(header.compressedSize);
    source->readBytes(compressBuf->writableData(), header.compressedSize);
    compressBuf->append(header.compressedSize);

    // Process chained uncompressed results IOBufs.
    auto uncompress =
        codec->uncompress(compressBuf.get(), header.uncompressedSize);
    auto uncompressedSource = std::make_unique<BufferInputStream>(
        byteRangesFromIOBuf(uncompress.get()));
    detail::readTopColumns(
        *uncompressedSource, type, pool, *result, resultOffset, prestoOptions);
  }
}

void PrestoVectorSerde::deserializeSingleColumn(
    ByteInputStream* source,
    velox::memory::MemoryPool* pool,
    TypePtr type,
    VectorPtr* result,
    const Options* options) {
  const auto prestoOptions = toPrestoOptions(options);
  VELOX_CHECK_EQ(
      prestoOptions.compressionKind,
      common::CompressionKind::CompressionKind_NONE);
  if (*result && result->use_count() == 1) {
    VELOX_CHECK(
        *(*result)->type() == *type,
        "Unexpected type: {} vs. {}",
        (*result)->type()->toString(),
        type->toString());
    (*result)->prepareForReuse();
  } else {
    *result = BaseVector::create(type, 0, pool);
  }

  auto rowType = ROW({"c0"}, {type});
  auto row = std::make_shared<RowVector>(
      pool, rowType, BufferPtr(nullptr), 0, std::vector<VectorPtr>{*result});
  detail::readTopColumns(*source, rowType, pool, row, 0, prestoOptions, true);
  *result = row->childAt(0);
}

void PrestoVectorSerde::serializeSingleColumn(
    const VectorPtr& vector,
    const Options* opts,
    memory::MemoryPool* pool,
    std::ostream* output) {
  const auto prestoOptions = toPrestoOptions(opts);
  VELOX_USER_CHECK_EQ(
      prestoOptions.compressionKind,
      common::CompressionKind::CompressionKind_NONE);
  VELOX_USER_CHECK_EQ(prestoOptions.nullsFirst, false);

  const IndexRange range{0, vector->size()};
  const auto arena = std::make_unique<StreamArena>(pool);
  auto stream = std::make_unique<detail::VectorStream>(
      vector->type(),
      std::nullopt,
      std::nullopt,
      arena.get(),
      vector->size(),
      prestoOptions);
  Scratch scratch;
  serializeColumn(vector, folly::Range(&range, 1), stream.get(), scratch);

  PrestoOutputStreamListener listener;
  OStreamOutputStream outputStream(output, &listener);
  stream->flush(&outputStream);
}

// static
void PrestoVectorSerde::registerVectorSerde() {
  detail::initBitsToMapOnce();
  velox::registerVectorSerde(std::make_unique<PrestoVectorSerde>());
}

// static
void PrestoVectorSerde::registerNamedVectorSerde() {
  detail::initBitsToMapOnce();
  velox::registerNamedVectorSerde(
      VectorSerde::Kind::kPresto, std::make_unique<PrestoVectorSerde>());
}

/* static */ Status PrestoVectorSerde::lex(
    std::string_view source,
    std::vector<Token>& out,
    const Options* options) {
  const auto prestoOptions = toPrestoOptions(options);

  VELOX_RETURN_IF(
      prestoOptions.useLosslessTimestamp,
      Status::Invalid(
          "Lossless timestamps are not supported, because they cannot be decoded without the Schema"));
  VELOX_RETURN_IF(
      prestoOptions.compressionKind !=
          common::CompressionKind::CompressionKind_NONE,
      Status::Invalid("Compression is not supported"));
  VELOX_RETURN_IF(
      prestoOptions.nullsFirst,
      Status::Invalid(
          "Nulls first encoding is not currently supported, but support can be added if needed"));

  return detail::PrestoVectorLexer(source).lex(out);
}

} // namespace facebook::velox::serializer::presto
