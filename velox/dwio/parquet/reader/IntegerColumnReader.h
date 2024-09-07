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

#include "velox/dwio/common/SelectiveIntegerColumnReader.h"

namespace facebook::velox::parquet {

class IntegerColumnReader : public dwio::common::SelectiveIntegerColumnReader {
 public:
  IntegerColumnReader(
      const TypePtr& requestedType,
      std::shared_ptr<const dwio::common::TypeWithId> fileType,
      ParquetParams& params,
      common::ScanSpec& scanSpec)
      : SelectiveIntegerColumnReader(
            requestedType,
            params,
            scanSpec,
            std::move(fileType)) {}

  bool hasBulkPath() const override {
    return !formatData_->as<ParquetData>().isDeltaBinaryPacked() &&
        !this->fileType().type()->isLongDecimal() &&
        ((this->fileType().type()->isShortDecimal())
             ? formatData_->as<ParquetData>().hasDictionary()
             : true);
  }

  void seekToRowGroup(uint32_t index) override {
    SelectiveIntegerColumnReader::seekToRowGroup(index);
    scanState().clear();
    readOffset_ = 0;
    formatData_->as<ParquetData>().seekToRowGroup(index);
  }

  uint64_t skip(uint64_t numValues) override {
    formatData_->as<ParquetData>().skip(numValues);
    return numValues;
  }

  void getValues(const RowSet& rows, VectorPtr* result) override {
    auto& fileType = static_cast<const ParquetTypeWithId&>(*fileType_);
    auto logicalType = fileType.logicalType_;
    if (logicalType.has_value() && logicalType.value().__isset.INTEGER &&
        !logicalType.value().INTEGER.isSigned) {
      getUnsignedIntValues(rows, requestedType_, result);
    } else {
      getIntValues(rows, requestedType_, result);
    }
  }

  void read(
      vector_size_t offset,
      const RowSet& rows,
      const uint64_t* /*incomingNulls*/) override {
    auto& data = formatData_->as<ParquetData>();
    VELOX_WIDTH_DISPATCH(
        parquetSizeOfIntKind(fileType_->type()->kind()),
        prepareRead,
        offset,
        rows,
        nullptr);
    readCommon<IntegerColumnReader, true>(rows);
    readOffset_ += rows.back() + 1;
  }

  template <typename ColumnVisitor>
  void readWithVisitor(const RowSet& rows, ColumnVisitor visitor) {
    formatData_->as<ParquetData>().readWithVisitor(visitor);
  }
};

} // namespace facebook::velox::parquet
