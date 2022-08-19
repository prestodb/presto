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
#include "velox/dwio/parquet/reader/Statistics.h"

namespace facebook::velox::parquet {

using thrift::RowGroup;

std::unique_ptr<dwio::common::FormatData> ParquetParams::toFormatData(
    const std::shared_ptr<const dwio::common::TypeWithId>& type,
    const common::ScanSpec& /*scanSpec*/) {
  return std::make_unique<ParquetData>(type, metaData_.row_groups, pool());
}

std::vector<uint32_t> ParquetData::filterRowGroups(
    const common::ScanSpec& scanSpec,
    uint64_t /*rowsPerRowGroup*/,
    const dwio::common::StatsContext& /*writerInfo*/) {
  if (!scanSpec.filter()) {
    return {};
  }
  std::vector<uint32_t> toSkip;
  for (auto i = 0; i < rowGroups_.size(); ++i) {
    if (!filterMatches(rowGroups_[i], *scanSpec.filter())) {
      toSkip.push_back(i);
    }
  }
  return toSkip;
}

bool ParquetData::filterMatches(
    const RowGroup& rowGroup,
    common::Filter& filter) {
  auto column = type_->column;
  auto type = type_->type;
  assert(!rowGroup.columns.empty());
  if (rowGroup.columns[column].__isset.meta_data &&
      rowGroup.columns[column].meta_data.__isset.statistics) {
    auto columnStats = buildColumnStatisticsFromThrift(
        rowGroup.columns[column].meta_data.statistics,
        *type,
        rowGroup.num_rows);
    if (!testFilter(&filter, columnStats.get(), rowGroup.num_rows, type)) {
      return false;
    }
  }
  return true;
}

void ParquetData::enqueueRowGroup(
    uint32_t index,
    dwio::common::BufferedInput& input) {
  auto& chunk = rowGroups_[index].columns[type_->column];
  streams_.resize(rowGroups_.size());
  VELOX_CHECK(
      chunk.__isset.meta_data,
      "ColumnMetaData does not exist for schema Id ",
      type_->column);
  VELOX_CHECK(
      chunk.__isset.meta_data,
      "ColumnMetaData does not exist for schema Id ",
      type_->column);
  auto& metaData = chunk.meta_data;

  uint64_t chunkReadOffset = metaData.data_page_offset;
  if (metaData.__isset.dictionary_page_offset &&
      metaData.dictionary_page_offset >= 4) {
    // this assumes the data pages follow the dict pages directly.
    chunkReadOffset = metaData.dictionary_page_offset;
  }
  VELOX_CHECK_GE(chunkReadOffset, 0);

  uint64_t readSize = (metaData.codec == thrift::CompressionCodec::UNCOMPRESSED)
      ? metaData.total_uncompressed_size
      : metaData.total_compressed_size;

  auto id = dwio::common::StreamIdentifier(type_->column);
  streams_[index] = input.enqueue({chunkReadOffset, readSize}, &id);
}

dwio::common::PositionProvider ParquetData::seekToRowGroup(uint32_t index) {
  static std::vector<uint64_t> empty;
  VELOX_CHECK_LT(index, streams_.size());
  VELOX_CHECK(streams_[index], "Stream not enqueued for column");
  auto& metadata = rowGroups_[index].columns[type_->column].meta_data;
  reader_ = std::make_unique<PageReader>(
      std::move(streams_[index]),
      pool_,
      type_,
      metadata.codec,
      metadata.total_compressed_size);
  return dwio::common::PositionProvider(empty);
}

} // namespace facebook::velox::parquet
