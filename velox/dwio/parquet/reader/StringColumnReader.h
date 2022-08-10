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

#include "velox/dwio/common/SelectiveColumnReaderInternal.h"
#include "velox/dwio/parquet/reader/ParquetData.h"

namespace facebook::velox::parquet {

class StringColumnReader : public dwio::common::SelectiveColumnReader {
 public:
  using ValueType = StringView;
  StringColumnReader(
      const std::shared_ptr<const dwio::common::TypeWithId>& nodeType,
      ParquetParams& params,
      common::ScanSpec& scanSpec);

  bool hasBulkPath() const override {
    //  Non-dictionary encodings do not have fast path.
    return scanState_.dictionary.values != nullptr;
  }

  void seekToRowGroup(uint32_t index) override {
    scanState().clear();
    readOffset_ = 0;
    formatData_->as<ParquetData>().seekToRowGroup(index);
  }

  uint64_t skip(uint64_t numValues) override;

  void read(vector_size_t offset, RowSet rows, const uint64_t* incomingNulls)
      override;

  void getValues(RowSet rows, VectorPtr* result) override;

  void dedictionarize() override;

 private:
  template <bool hasNulls>
  void skipInDecode(int32_t numValues, int32_t current, const uint64_t* nulls);

  folly::StringPiece readValue(int32_t length);

  template <bool hasNulls, typename Visitor>
  void decode(const uint64_t* nulls, Visitor visitor);

  template <typename TVisitor>
  void readWithVisitor(RowSet rows, TVisitor visitor);

  template <typename TFilter, bool isDense, typename ExtractValues>
  void readHelper(common::Filter* filter, RowSet rows, ExtractValues values);

  template <bool isDense, typename ExtractValues>
  void processFilter(
      common::Filter* filter,
      RowSet rows,
      ExtractValues extractValues);
};

} // namespace facebook::velox::parquet
