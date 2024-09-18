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

#include "velox/dwio/parquet/reader/StringColumnReader.h"

#include "velox/dwio/common/SelectiveColumnReaderInternal.h"

namespace facebook::velox::parquet {

StringColumnReader::StringColumnReader(
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    ParquetParams& params,
    common::ScanSpec& scanSpec)
    : SelectiveColumnReader(fileType->type(), fileType, params, scanSpec) {}

uint64_t StringColumnReader::skip(uint64_t numValues) {
  formatData_->skip(numValues);
  return numValues;
}

void StringColumnReader::read(
    vector_size_t offset,
    const RowSet& rows,
    const uint64_t* incomingNulls) {
  prepareRead<folly::StringPiece>(offset, rows, incomingNulls);
  dwio::common::StringColumnReadWithVisitorHelper<true, false>(
      *this, rows)([&](auto visitor) {
    formatData_->as<ParquetData>().readWithVisitor(visitor);
  });
  readOffset_ += rows.back() + 1;
}

void StringColumnReader::getValues(const RowSet& rows, VectorPtr* result) {
  if (scanState_.dictionary.values) {
    auto dictionaryValues =
        formatData_->as<ParquetData>().dictionaryValues(fileType_->type());
    compactScalarValues<int32_t, int32_t>(rows, false);

    *result = std::make_shared<DictionaryVector<StringView>>(
        memoryPool_, resultNulls(), numValues_, dictionaryValues, values_);
    return;
  }
  rawStringBuffer_ = nullptr;
  rawStringSize_ = 0;
  rawStringUsed_ = 0;
  getFlatValues<StringView, StringView>(rows, result, fileType_->type());
}

void StringColumnReader::dedictionarize() {
  if (scanSpec_->keepValues()) {
    auto dict = formatData_->as<ParquetData>()
                    .dictionaryValues(fileType_->type())
                    ->as<FlatVector<StringView>>();
    auto valuesCapacity = values_->capacity();
    auto indices = values_->as<vector_size_t>();
    // 'values_' is sized for the batch worth of StringViews. It is filled with
    // 32 bit indices by dictionary scan.
    VELOX_CHECK_GE(valuesCapacity, numValues_ * sizeof(StringView));
    stringBuffers_.clear();
    rawStringBuffer_ = nullptr;
    rawStringSize_ = 0;
    rawStringUsed_ = 0;
    auto numValues = numValues_;
    // Convert indices to values in place. Loop from end to beginning
    // so as not to overwrite integer indices with longer StringViews.
    for (auto i = numValues - 1; i >= 0; --i) {
      if (anyNulls_ && bits::isBitNull(rawResultNulls_, i)) {
        reinterpret_cast<StringView*>(rawValues_)[i] = StringView();
        continue;
      }
      auto& view = dict->valueAt(indices[i]);
      numValues_ = i;
      addStringValue(folly::StringPiece(view.data(), view.size()));
    }
    numValues_ = numValues;
  }
  scanState_.clear();
  formatData_->as<ParquetData>().clearDictionary();
}
} // namespace facebook::velox::parquet
