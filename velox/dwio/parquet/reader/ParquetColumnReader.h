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

#include "velox/dwio/parquet/reader/ParquetData.h"

namespace facebook::velox::parquet {

/// Parquet stores integers of 8, 16, 32 bits with 32 bits and 64 as 64 bits.
inline int32_t parquetSizeOfIntKind(TypeKind kind) {
  switch (kind) {
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::DATE:
      return 4;
    case TypeKind::BIGINT:
    case TypeKind::SHORT_DECIMAL:
      return 8;
    default:
      VELOX_FAIL("Not an integer TypeKind");
  }
}

/// Wrapper for static functions for Parquet columns.
class ParquetColumnReader {
 public:
  /// Builds a reader tree producing 'dataType'. The metadata is in 'params'.
  /// The filters and pruning are in 'scanSpec'.
  static std::unique_ptr<dwio::common::SelectiveColumnReader> build(
      const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
      ParquetParams& params,
      common::ScanSpec& scanSpec);
};
} // namespace facebook::velox::parquet
