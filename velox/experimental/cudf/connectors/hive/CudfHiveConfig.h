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

#include "velox/common/config/Config.h"

#include <cudf/types.hpp>

#include <optional>

namespace facebook::velox::config {
class ConfigBase;
}

namespace facebook::velox::cudf_velox::connector::hive {

class CudfHiveConfig {
 public:
  // Reader config options

  // Number of rows to skip from the start; CudfHive stores the number of rows
  // as int64_t
  static constexpr const char* kSkipRows = "parquet.reader.skip-rows";

  // Number of rows to read; `nullopt` is all
  static constexpr const char* kNumRows = "parquet.reader.num-rows";

  // This isn't a typo; parquet connector and session config names are different
  // ('-' vs '_').
  static constexpr const char* kMaxChunkReadLimit =
      "parquet.reader.chunk-read-limit";
  static constexpr const char* kMaxChunkReadLimitSession =
      "parquet.reader.chunk_read_limit";

  static constexpr const char* kMaxPassReadLimit =
      "parquet.reader.pass-read-limit";
  static constexpr const char* kMaxPassReadLimitSession =
      "parquet.reader.pass_read_limit";

  // Whether to store string data as categorical type
  static constexpr const char* kConvertStringsToCategories =
      "parquet.reader.convert-strings-to-categories";
  static constexpr const char* kConvertStringsToCategoriesSession =
      "parquet.reader.convert_strings_to_categories";

  // Whether to use PANDAS metadata to load columns
  static constexpr const char* kUsePandasMetadata =
      "parquet.reader.use-pandas-metadata";
  static constexpr const char* kUsePandasMetadataSession =
      "parquet.reader.use_pandas_metadata";

  // Whether to read and use ARROW schema
  static constexpr const char* kUseArrowSchema =
      "parquet.reader.use-arrow-schema";
  static constexpr const char* kUseArrowSchemaSession =
      "parquet.reader.use_arrow_schema";

  // Whether to allow reading matching select columns from mismatched CudfHive
  // files.
  static constexpr const char* kAllowMismatchedCudfHiveSchemas =
      "parquet.reader.allow-mismatched-parquet-schemas";
  static constexpr const char* kAllowMismatchedCudfHiveSchemasSession =
      "parquet.reader.allow_mismatched_parquet_schemas";

  // Cast timestamp columns to a specific type
  static constexpr const char* kTimestampType = "parquet.reader.timestamp-type";
  static constexpr const char* kTimestampTypeSession =
      "parquet.reader.timestamp_type";

  // Writer config options

  /// Whether new data can be inserted into a CudfHive file
  /// Cudf-Velox currently does not support appending data to existing files.
  static constexpr const char* kImmutableFiles = "parquet.immutable-files";

  /// Sort Writer will exit finish() method after this many milliseconds even if
  /// it has not completed its work yet. Zero means no time limit.
  static constexpr const char* kSortWriterFinishTimeSliceLimitMs =
      "sort-writer_finish_time_slice_limit_ms";
  static constexpr const char* kSortWriterFinishTimeSliceLimitMsSession =
      "sort_writer_finish_time_slice_limit_ms";

  static constexpr const char* kWriteTimestampsAsUTC =
      "parquet.writer.write-timestamps-as-utc";
  static constexpr const char* kWriteTimestampsAsUTCSession =
      "parquet.writer.write_timestamps_as_utc";

  static constexpr const char* kWriteArrowSchema =
      "parquet.writer.write-arrow-schema";
  static constexpr const char* kWriteArrowSchemaSession =
      "parquet.writer.write_arrow_schema";

  static constexpr const char* kWritev2PageHeaders =
      "parquet.writer.write-v2-page-headers";
  static constexpr const char* kWritev2PageHeadersSession =
      "parquet.writer.write_v2_page_headers";

  CudfHiveConfig(std::shared_ptr<const config::ConfigBase> config) {
    VELOX_CHECK_NOT_NULL(
        config, "Config is null for CudfHiveConfig initialization");
    config_ = std::move(config);
  }

  const std::shared_ptr<const config::ConfigBase>& config() const {
    return config_;
  }

  uint64_t sortWriterFinishTimeSliceLimitMs(
      const config::ConfigBase* session) const;

  std::size_t maxChunkReadLimit() const;
  std::size_t maxChunkReadLimitSession(const config::ConfigBase* session) const;

  std::size_t maxPassReadLimit() const;
  std::size_t maxPassReadLimitSession(const config::ConfigBase* session) const;

  int64_t skipRows() const;
  std::optional<cudf::size_type> numRows() const;

  bool isConvertStringsToCategories() const;
  bool isConvertStringsToCategoriesSession(
      const config::ConfigBase* session) const;

  bool isUsePandasMetadata() const;
  bool isUsePandasMetadataSession(const config::ConfigBase* session) const;

  bool isUseArrowSchema() const;
  bool isUseArrowSchemaSession(const config::ConfigBase* session) const;

  bool isAllowMismatchedCudfHiveSchemas() const;
  bool isAllowMismatchedCudfHiveSchemasSession(
      const config::ConfigBase* session) const;

  cudf::data_type timestampType() const;
  cudf::data_type timestampTypeSession(const config::ConfigBase* session) const;

  bool immutableFiles() const;

  bool writeTimestampsAsUTC() const;
  bool writeTimestampsAsUTCSession(const config::ConfigBase* session) const;

  bool writeArrowSchema() const;
  bool writeArrowSchemaSession(const config::ConfigBase* session) const;

  bool writev2PageHeaders() const;
  bool writev2PageHeadersSession(const config::ConfigBase* session) const;

 private:
  std::shared_ptr<const config::ConfigBase> config_;
};
} // namespace facebook::velox::cudf_velox::connector::hive
