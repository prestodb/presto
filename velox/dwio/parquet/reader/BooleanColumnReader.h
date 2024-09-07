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

#include "velox/dwio/common/SelectiveByteRleColumnReader.h"
#include "velox/dwio/parquet/reader/ParquetData.h"

namespace facebook::velox::parquet {

class BooleanColumnReader : public dwio::common::SelectiveByteRleColumnReader {
 public:
  using ValueType = bool;
  BooleanColumnReader(
      const TypePtr& requestedType,
      std::shared_ptr<const dwio::common::TypeWithId> fileType,
      ParquetParams& params,
      common::ScanSpec& scanSpec)
      : SelectiveByteRleColumnReader(
            requestedType,
            std::move(fileType),
            params,
            scanSpec) {}

  void seekToRowGroup(uint32_t index) override {
    SelectiveByteRleColumnReader::seekToRowGroup(index);
    scanState().clear();
    readOffset_ = 0;
    formatData_->as<ParquetData>().seekToRowGroup(index);
  }

  uint64_t skip(uint64_t numValues) override {
    formatData_->as<ParquetData>().skip(numValues);
    return numValues;
  }

  void read(
      vector_size_t offset,
      const RowSet& rows,
      const uint64_t* incomingNulls) override {
    readCommon<BooleanColumnReader, true>(offset, rows, incomingNulls);
    readOffset_ += rows.back() + 1;
  }

  template <typename ColumnVisitor>
  void readWithVisitor(const RowSet& /*rows*/, ColumnVisitor visitor) {
    formatData_->as<ParquetData>().readWithVisitor(visitor);
  }
};

} // namespace facebook::velox::parquet
