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

#include "velox/dwio/parquet/reader/ParquetData.h"

#include "velox/dwio/common/BufferedInput.h"

namespace facebook::velox::parquet {

std::unique_ptr<dwio::common::FormatData> ParquetParams::toFormatData(
    const std::shared_ptr<const dwio::common::TypeWithId>& type,
    const common::ScanSpec& /*scanSpec*/) {
  return std::make_unique<ParquetData>(type, metaData_, pool());
}

void ParquetData::filterRowGroups(
    const common::ScanSpec& scanSpec,
    uint64_t /*rowsPerRowGroup*/,
    const dwio::common::StatsContext& /*writerContext*/,
    FilterRowGroupsResult& result) {
  result.totalCount =
      std::max<int>(result.totalCount, fileMetaDataPtr_.numRowGroups());
  auto nwords = bits::nwords(result.totalCount);
  if (result.filterResult.size() < nwords) {
    result.filterResult.resize(nwords);
  }
  auto metadataFiltersStartIndex = result.metadataFilterResults.size();
  for (int i = 0; i < scanSpec.numMetadataFilters(); ++i) {
    result.metadataFilterResults.emplace_back(
        scanSpec.metadataFilterNodeAt(i), std::vector<uint64_t>(nwords));
  }
  for (auto i = 0; i < fileMetaDataPtr_.numRowGroups(); ++i) {
    if (scanSpec.filter() && !rowGroupMatches(i, scanSpec.filter())) {
      bits::setBit(result.filterResult.data(), i);
      continue;
    }
    for (int j = 0; j < scanSpec.numMetadataFilters(); ++j) {
      auto* metadataFilter = scanSpec.metadataFilterAt(j);
      if (!rowGroupMatches(i, metadataFilter)) {
        bits::setBit(
            result.metadataFilterResults[metadataFiltersStartIndex + j]
                .second.data(),
            i);
      }
    }
  }
}

bool ParquetData::rowGroupMatches(
    uint32_t rowGroupId,
    common::Filter* FOLLY_NULLABLE filter) {
  auto column = type_->column();
  auto type = type_->type();
  auto rowGroup = fileMetaDataPtr_.rowGroup(rowGroupId);
  assert(rowGroup.numColumns() != 0);

  if (!filter) {
    return true;
  }

  auto columnChunk = rowGroup.columnChunk(column);
  if (columnChunk.hasStatistics()) {
    auto columnStats =
        columnChunk.getColumnStatistics(type, rowGroup.numRows());
    return testFilter(filter, columnStats.get(), rowGroup.numRows(), type);
  }
  return true;
}

void ParquetData::enqueueRowGroup(
    uint32_t index,
    dwio::common::BufferedInput& input) {
  auto chunk = fileMetaDataPtr_.rowGroup(index).columnChunk(type_->column());
  streams_.resize(fileMetaDataPtr_.numRowGroups());
  VELOX_CHECK(
      chunk.hasMetadata(),
      "ColumnMetaData does not exist for schema Id ",
      type_->column());
  ;

  uint64_t chunkReadOffset = chunk.dataPageOffset();
  if (chunk.hasDictionaryPageOffset() && chunk.dictionaryPageOffset() >= 4) {
    // this assumes the data pages follow the dict pages directly.
    chunkReadOffset = chunk.dictionaryPageOffset();
  }
  VELOX_CHECK_GE(chunkReadOffset, 0);

  uint64_t readSize =
      (chunk.compression() == common::CompressionKind::CompressionKind_NONE)
      ? chunk.totalUncompressedSize()
      : chunk.totalCompressedSize();

  auto id = dwio::common::StreamIdentifier(type_->column());
  streams_[index] = input.enqueue({chunkReadOffset, readSize}, &id);
}

dwio::common::PositionProvider ParquetData::seekToRowGroup(uint32_t index) {
  static std::vector<uint64_t> empty;
  VELOX_CHECK_LT(index, streams_.size());
  VELOX_CHECK(streams_[index], "Stream not enqueued for column");
  auto metadata = fileMetaDataPtr_.rowGroup(index).columnChunk(type_->column());
  reader_ = std::make_unique<PageReader>(
      std::move(streams_[index]),
      pool_,
      type_,
      metadata.compression(),
      metadata.totalCompressedSize());
  return dwio::common::PositionProvider(empty);
}

std::pair<int64_t, int64_t> ParquetData::getRowGroupRegion(
    uint32_t index) const {
  auto rowGroup = fileMetaDataPtr_.rowGroup(index);

  VELOX_CHECK_GT(rowGroup.numColumns(), 0);
  auto fileOffset = rowGroup.hasFileOffset() ? rowGroup.fileOffset()
      : rowGroup.columnChunk(0).hasDictionaryPageOffset()
      ? rowGroup.columnChunk(0).dictionaryPageOffset()
      : rowGroup.columnChunk(0).dataPageOffset();
  VELOX_CHECK_GT(fileOffset, 0);

  auto length = rowGroup.hasTotalCompressedSize()
      ? rowGroup.totalCompressedSize()
      : rowGroup.totalByteSize();

  return {fileOffset, length};
}

} // namespace facebook::velox::parquet
