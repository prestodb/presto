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

#include "velox/dwio/common/SelectiveStructColumnReader.h"
#include "velox/dwio/parquet/reader/ParquetColumnReader.h"

namespace facebook::velox::parquet {

class StructColumnReader : public dwio::common::SelectiveStructColumnReader {
 public:
  StructColumnReader(
      const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
      ParquetParams& params,
      common::ScanSpec& scanSpec);

  void seekToRowGroup(uint32_t index) override;

  /// Creates the streams for 'rowGroup in 'input'. Does not load yet.
  void enqueueRowGroup(uint32_t index, dwio::common::BufferedInput& input);

  // No-op in Parquet. All readers switch row groups at the same time, there is
  // no on-demand skipping to a new row group.
  void advanceFieldReader(
      dwio::common::SelectiveColumnReader* FOLLY_NONNULL /*reader*/,
      vector_size_t /*offset*/) override {}

 private:
  bool filterMatches(const thrift::RowGroup& rowGroup);
};

} // namespace facebook::velox::parquet
