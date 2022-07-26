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

#include "velox/dwio/parquet/RegisterParquetReader.h"

#include "velox/dwio/parquet/duckdb_reader/ParquetReader.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"

namespace facebook::velox::parquet {

void registerParquetReaderFactory(ParquetReaderType parquetReaderType) {
  switch (parquetReaderType) {
    case ParquetReaderType::DUCKDB:
      dwio::common::registerReaderFactory(
          std::make_shared<duckdb_reader::ParquetReaderFactory>());
      break;
    case ParquetReaderType::NATIVE:
      dwio::common::registerReaderFactory(
          std::make_shared<ParquetReaderFactory>());
      break;
    default:
      VELOX_UNSUPPORTED(
          "Velox does not support ParquetReaderType ", parquetReaderType);
  }
}

void registerParquetReaderFactory() {
  registerParquetReaderFactory(ParquetReaderType::DUCKDB);
}

void unregisterParquetReaderFactory() {
  dwio::common::unregisterReaderFactory(dwio::common::FileFormat::PARQUET);
}

} // namespace facebook::velox::parquet
