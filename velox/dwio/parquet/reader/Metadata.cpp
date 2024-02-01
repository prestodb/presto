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

#include "velox/dwio/parquet/reader/Metadata.h"

#include "velox/dwio/parquet/reader/Statistics.h"

namespace facebook::velox::parquet {

common::CompressionKind thriftCodecToCompressionKind(
    thrift::CompressionCodec::type codec) {
  switch (codec) {
    case thrift::CompressionCodec::UNCOMPRESSED:
      return common::CompressionKind::CompressionKind_NONE;
      break;
    case thrift::CompressionCodec::SNAPPY:
      return common::CompressionKind::CompressionKind_SNAPPY;
      break;
    case thrift::CompressionCodec::GZIP:
      return common::CompressionKind::CompressionKind_GZIP;
      break;
    case thrift::CompressionCodec::LZO:
      return common::CompressionKind::CompressionKind_LZO;
      break;
    case thrift::CompressionCodec::LZ4:
      return common::CompressionKind::CompressionKind_LZ4;
      break;
    case thrift::CompressionCodec::ZSTD:
      return common::CompressionKind::CompressionKind_ZSTD;
      break;
    case thrift::CompressionCodec::LZ4_RAW:
      return common::CompressionKind::CompressionKind_LZ4;
    default:
      VELOX_UNSUPPORTED(
          "Unsupported compression type: " +
          facebook::velox::parquet::thrift::to_string(codec));
      break;
  }
}

ColumnChunkMetaDataPtr::ColumnChunkMetaDataPtr(const void* metadata)
    : ptr_(metadata) {}

ColumnChunkMetaDataPtr::~ColumnChunkMetaDataPtr() = default;

FOLLY_ALWAYS_INLINE const thrift::ColumnChunk* thriftColumnChunkPtr(
    const void* metadata) {
  return reinterpret_cast<const thrift::ColumnChunk*>(metadata);
}

int64_t ColumnChunkMetaDataPtr::numValues() const {
  return thriftColumnChunkPtr(ptr_)->meta_data.num_values;
}

bool ColumnChunkMetaDataPtr::hasMetadata() const {
  return thriftColumnChunkPtr(ptr_)->__isset.meta_data;
}

bool ColumnChunkMetaDataPtr::hasStatistics() const {
  return hasMetadata() &&
      thriftColumnChunkPtr(ptr_)->meta_data.__isset.statistics;
}

bool ColumnChunkMetaDataPtr::hasDictionaryPageOffset() const {
  return hasMetadata() &&
      thriftColumnChunkPtr(ptr_)->meta_data.__isset.dictionary_page_offset;
}

std::unique_ptr<dwio::common::ColumnStatistics>
ColumnChunkMetaDataPtr::getColumnStatistics(
    const TypePtr type,
    int64_t numRows) {
  VELOX_CHECK(hasStatistics());
  return buildColumnStatisticsFromThrift(
      thriftColumnChunkPtr(ptr_)->meta_data.statistics, *type, numRows);
};

int64_t ColumnChunkMetaDataPtr::dataPageOffset() const {
  return thriftColumnChunkPtr(ptr_)->meta_data.data_page_offset;
}

int64_t ColumnChunkMetaDataPtr::dictionaryPageOffset() const {
  VELOX_CHECK(hasDictionaryPageOffset());
  return thriftColumnChunkPtr(ptr_)->meta_data.dictionary_page_offset;
}

common::CompressionKind ColumnChunkMetaDataPtr::compression() const {
  return thriftCodecToCompressionKind(
      thriftColumnChunkPtr(ptr_)->meta_data.codec);
}

int64_t ColumnChunkMetaDataPtr::totalCompressedSize() const {
  return thriftColumnChunkPtr(ptr_)->meta_data.total_compressed_size;
}

int64_t ColumnChunkMetaDataPtr::totalUncompressedSize() const {
  return thriftColumnChunkPtr(ptr_)->meta_data.total_uncompressed_size;
}

FOLLY_ALWAYS_INLINE const thrift::RowGroup* thriftRowGroupPtr(
    const void* metadata) {
  return reinterpret_cast<const thrift::RowGroup*>(metadata);
}

RowGroupMetaDataPtr::RowGroupMetaDataPtr(const void* metadata)
    : ptr_(metadata) {}

RowGroupMetaDataPtr::~RowGroupMetaDataPtr() = default;

int RowGroupMetaDataPtr::numColumns() const {
  return thriftRowGroupPtr(ptr_)->columns.size();
}

int64_t RowGroupMetaDataPtr::numRows() const {
  return thriftRowGroupPtr(ptr_)->num_rows;
}

int64_t RowGroupMetaDataPtr::totalByteSize() const {
  return thriftRowGroupPtr(ptr_)->total_byte_size;
}

bool RowGroupMetaDataPtr::hasFileOffset() const {
  return thriftRowGroupPtr(ptr_)->__isset.file_offset;
}

int64_t RowGroupMetaDataPtr::fileOffset() const {
  return thriftRowGroupPtr(ptr_)->file_offset;
}

bool RowGroupMetaDataPtr::hasTotalCompressedSize() const {
  return thriftRowGroupPtr(ptr_)->__isset.total_compressed_size;
}

int64_t RowGroupMetaDataPtr::totalCompressedSize() const {
  return thriftRowGroupPtr(ptr_)->total_compressed_size;
}

ColumnChunkMetaDataPtr RowGroupMetaDataPtr::columnChunk(int i) const {
  return ColumnChunkMetaDataPtr(
      reinterpret_cast<const void*>(&thriftRowGroupPtr(ptr_)->columns[i]));
}

FOLLY_ALWAYS_INLINE const thrift::FileMetaData* thriftFileMetaDataPtr(
    const void* metadata) {
  return reinterpret_cast<const thrift::FileMetaData*>(metadata);
}

FileMetaDataPtr::FileMetaDataPtr(const void* metadata) : ptr_(metadata) {}

FileMetaDataPtr::~FileMetaDataPtr() = default;

RowGroupMetaDataPtr FileMetaDataPtr::rowGroup(int i) const {
  return RowGroupMetaDataPtr(reinterpret_cast<const void*>(
      &thriftFileMetaDataPtr(ptr_)->row_groups[i]));
}

int64_t FileMetaDataPtr::numRows() const {
  return thriftFileMetaDataPtr(ptr_)->num_rows;
}

int FileMetaDataPtr::numRowGroups() const {
  return thriftFileMetaDataPtr(ptr_)->row_groups.size();
}

} // namespace facebook::velox::parquet
