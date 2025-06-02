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

#include "velox/experimental/cudf/connectors/parquet/ParquetConfig.h"

#include "velox/common/base/Exceptions.h"
#include "velox/common/config/Config.h"

#include <cudf/types.hpp>

#include <optional>

namespace facebook::velox::cudf_velox::connector::parquet {

int64_t ParquetConfig::skipRows() const {
  return config_->get<int64_t>(kSkipRows, 0);
}

std::optional<cudf::size_type> ParquetConfig::numRows() const {
  auto numRows = config_->get<cudf::size_type>(kNumRows);
  return numRows.has_value()
      ? std::make_optional<cudf::size_type>(numRows.value())
      : std::nullopt;
}

std::size_t ParquetConfig::maxChunkReadLimit() const {
  // chunk read limit = 0 means no limit
  return config_->get<std::size_t>(kMaxChunkReadLimit, 0);
}

std::size_t ParquetConfig::maxChunkReadLimitSession(
    const config::ConfigBase* session) const {
  // pass read limit = 0 means no limit
  return session->get<std::size_t>(
      kMaxChunkReadLimitSession,
      config_->get<std::size_t>(kMaxChunkReadLimit, 0));
}

std::size_t ParquetConfig::maxPassReadLimit() const {
  // pass read limit = 0 means no limit
  return config_->get<std::size_t>(kMaxPassReadLimit, 0);
}

std::size_t ParquetConfig::maxPassReadLimitSession(
    const config::ConfigBase* session) const {
  // pass read limit = 0 means no limit
  return session->get<std::size_t>(
      kMaxPassReadLimitSession,
      config_->get<std::size_t>(kMaxPassReadLimit, 0));
}

bool ParquetConfig::isConvertStringsToCategories() const {
  return config_->get<bool>(kConvertStringsToCategories, false);
}

bool ParquetConfig::isConvertStringsToCategoriesSession(
    const config::ConfigBase* session) const {
  return session->get<bool>(
      kConvertStringsToCategoriesSession,
      config_->get<bool>(kConvertStringsToCategories, false));
}

bool ParquetConfig::isUsePandasMetadata() const {
  return config_->get<bool>(kUsePandasMetadata, true);
}

bool ParquetConfig::isUsePandasMetadataSession(
    const config::ConfigBase* session) const {
  return session->get<bool>(
      kUsePandasMetadataSession, config_->get<bool>(kUsePandasMetadata, true));
}

bool ParquetConfig::isUseArrowSchema() const {
  return config_->get<bool>(kUseArrowSchema, true);
}

bool ParquetConfig::isUseArrowSchemaSession(
    const config::ConfigBase* session) const {
  return session->get<bool>(
      kUseArrowSchemaSession, config_->get<bool>(kUseArrowSchema, true));
}

bool ParquetConfig::isAllowMismatchedParquetSchemas() const {
  return config_->get<bool>(kAllowMismatchedParquetSchemas, false);
}

bool ParquetConfig::isAllowMismatchedParquetSchemasSession(
    const config::ConfigBase* session) const {
  return session->get<bool>(
      kAllowMismatchedParquetSchemasSession,
      config_->get<bool>(kAllowMismatchedParquetSchemas, false));
}

cudf::data_type ParquetConfig::timestampType() const {
  const auto unit = config_->get<cudf::type_id>(
      kTimestampType, cudf::type_id::TIMESTAMP_MILLISECONDS /*milli*/);
  VELOX_CHECK(
      unit == cudf::type_id::TIMESTAMP_DAYS /*days*/ ||
          unit == cudf::type_id::TIMESTAMP_SECONDS /*seconds*/ ||
          unit == cudf::type_id::TIMESTAMP_MILLISECONDS /*milli*/ ||
          unit == cudf::type_id::TIMESTAMP_MICROSECONDS /*micro*/ ||
          unit == cudf::type_id::TIMESTAMP_NANOSECONDS /*nano*/,
      "Invalid timestamp unit.");
  return cudf::data_type(cudf::type_id{unit});
}

cudf::data_type ParquetConfig::timestampTypeSession(
    const config::ConfigBase* session) const {
  const auto unit = session->get<cudf::type_id>(
      kTimestampTypeSession,
      config_->get<cudf::type_id>(
          kTimestampType, cudf::type_id::TIMESTAMP_MILLISECONDS /*milli*/));
  VELOX_CHECK(
      unit == cudf::type_id::TIMESTAMP_DAYS /*days*/ ||
          unit == cudf::type_id::TIMESTAMP_SECONDS /*seconds*/ ||
          unit == cudf::type_id::TIMESTAMP_MILLISECONDS /*milli*/ ||
          unit == cudf::type_id::TIMESTAMP_MICROSECONDS /*micro*/ ||
          unit == cudf::type_id::TIMESTAMP_NANOSECONDS /*nano*/,
      "Invalid timestamp unit.");
  return cudf::data_type(cudf::type_id{unit});
}

bool ParquetConfig::immutableFiles() const {
  return config_->get<bool>(kImmutableFiles, false);
}

uint64_t ParquetConfig::sortWriterFinishTimeSliceLimitMs(
    const config::ConfigBase* session) const {
  return session->get<uint64_t>(
      kSortWriterFinishTimeSliceLimitMsSession,
      config_->get<uint64_t>(kSortWriterFinishTimeSliceLimitMs, 5'000));
}

bool ParquetConfig::writeTimestampsAsUTC() const {
  return config_->get<bool>(kWriteTimestampsAsUTC, true);
}

bool ParquetConfig::writeTimestampsAsUTCSession(
    const config::ConfigBase* session) const {
  return session->get<bool>(
      kWriteTimestampsAsUTCSession,
      config_->get<bool>(kWriteTimestampsAsUTC, true));
}

bool ParquetConfig::writeArrowSchema() const {
  return config_->get<bool>(kWriteArrowSchema, false);
}

bool ParquetConfig::writeArrowSchemaSession(
    const config::ConfigBase* session) const {
  return session->get<bool>(
      kWriteArrowSchemaSession, config_->get<bool>(kWriteArrowSchema, false));
}

bool ParquetConfig::writev2PageHeaders() const {
  return config_->get<bool>(kWritev2PageHeaders, false);
}

bool ParquetConfig::writev2PageHeadersSession(
    const config::ConfigBase* session) const {
  return session->get<bool>(
      kWritev2PageHeadersSession,
      config_->get<bool>(kWritev2PageHeaders, false));
}

} // namespace facebook::velox::cudf_velox::connector::parquet
