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

#include "velox/dwio/common/Options.h"

#include <cudf/io/parquet.hpp>
#include <cudf/io/types.hpp>
#include <cudf/types.hpp>

#include <optional>

namespace facebook::velox::cudf_velox::connector::hive {

using namespace cudf::io;

/**
 * @brief Struct to 1:1 correspond with cudf::io::chunked_parquet_reader_options
 * except sink_info and a few others which are provided to the CudfHiveDataSink
 * from elsewhere.
 */
struct CudfHiveWriterOptions
    : public facebook::velox::dwio::common::WriterOptions {
  // Specify the level of statistics in the output file
  statistics_freq statsLevel = statistics_freq::STATISTICS_ROWGROUP;

  // CudfHive writer can write INT96 or TIMESTAMP_MICROS. Defaults to
  // TIMESTAMPMICROS. If true then overrides any per-column setting in
  // Metadata.
  bool writeTimestampsAsInt96 = false;

  // CudfHive writer can write timestamps as UTC
  // Defaults to true because libcudf timestamps are implicitly UTC
  bool writeTimestampsAsUTC = true;

  // Whether to write ARROW schema
  bool writeArrowSchema = false;

  // Maximum size of each row group (unless smaller than a single page)
  size_t rowGroupSizeBytes = default_row_group_size_bytes;

  // Maximum number of rows in row group (unless smaller than a single page)
  cudf::size_type rowGroupSizeRows = default_row_group_size_rows;

  // Maximum size of each page (uncompressed) - Velox uses 1KB (2 x cudf limit)
  size_t maxPageSizeBytes = 2 * default_max_page_size_bytes;

  // Maximum number of rows in a page
  cudf::size_type maxPageSizeRows = default_max_page_size_rows;

  // Maximum size of min or max values in column index
  int32_t columnIndexTruncateLength = default_column_index_truncate_length;

  // When to use dictionary encoding for data
  dictionary_policy dictionaryPolicy = dictionary_policy::ADAPTIVE;

  // Maximum size of column chunk dictionary (in bytes)
  size_t maxDictionarySize = default_max_dictionary_size;

  // Maximum number of rows in a page fragment
  std::optional<cudf::size_type> maxPageFragmentSize;

  // Optional compression statistics
  std::shared_ptr<writer_compression_statistics> compressionStats;

  // Write V2 page headers?
  bool v2PageHeaders = false;

  // Encoding to use for columns
  column_encoding encoding = column_encoding::PLAIN;

  // Sorting columns
  std::vector<sorting_column> sortingColumns;
};

} // namespace facebook::velox::cudf_velox::connector::hive
