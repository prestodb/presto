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
#include "velox/dwio/common/BufferUtil.h"

namespace facebook::velox::parquet {

StringColumnReader::StringColumnReader(
    const std::shared_ptr<const dwio::common::TypeWithId>& nodeType,
    ParquetParams& params,
    common::ScanSpec& scanSpec)
    : SelectiveColumnReader(nodeType, params, scanSpec, nodeType->type) {}

uint64_t StringColumnReader::skip(uint64_t numValues) {
  formatData_->skip(numValues);
  return numValues;
}

template <typename TFilter, bool isDense, typename ExtractValues>
void StringColumnReader::readHelper(
    common::Filter* filter,
    RowSet rows,
    ExtractValues extractValues) {
  formatData_->as<ParquetData>().readWithVisitor(
      dwio::common::
          ColumnVisitor<folly::StringPiece, TFilter, ExtractValues, isDense>(
              *reinterpret_cast<TFilter*>(filter), this, rows, extractValues));
  readOffset_ += rows.back() + 1;
}

template <bool isDense, typename ExtractValues>
void StringColumnReader::processFilter(
    common::Filter* filter,
    RowSet rows,
    ExtractValues extractValues) {
  switch (filter ? filter->kind() : common::FilterKind::kAlwaysTrue) {
    case common::FilterKind::kAlwaysTrue:
      readHelper<common::AlwaysTrue, isDense>(filter, rows, extractValues);
      break;
    case common::FilterKind::kIsNull:
      filterNulls<StringView>(
          rows,
          true,
          !std::is_same<decltype(extractValues), dwio::common::DropValues>::
              value);
      break;
    case common::FilterKind::kIsNotNull:
      if (std::is_same<decltype(extractValues), dwio::common::DropValues>::
              value) {
        filterNulls<StringView>(rows, false, false);
      } else {
        readHelper<common::IsNotNull, isDense>(filter, rows, extractValues);
      }
      break;
    case common::FilterKind::kBytesRange:
      readHelper<common::BytesRange, isDense>(filter, rows, extractValues);
      break;
    case common::FilterKind::kNegatedBytesRange:
      readHelper<common::NegatedBytesRange, isDense>(
          filter, rows, extractValues);
      break;
    case common::FilterKind::kBytesValues:
      readHelper<common::BytesValues, isDense>(filter, rows, extractValues);
      break;
    case common::FilterKind::kNegatedBytesValues:
      readHelper<common::NegatedBytesValues, isDense>(
          filter, rows, extractValues);
      break;
    default:
      readHelper<common::Filter, isDense>(filter, rows, extractValues);
      break;
  }
}

void StringColumnReader::read(
    vector_size_t offset,
    RowSet rows,
    const uint64_t* incomingNulls) {
  prepareRead<folly::StringPiece>(offset, rows, incomingNulls);
  bool isDense = rows.back() == rows.size() - 1;
  if (scanSpec_->keepValues()) {
    if (scanSpec_->valueHook()) {
      if (isDense) {
        readHelper<common::AlwaysTrue, true>(
            &dwio::common::alwaysTrue(),
            rows,
            dwio::common::ExtractToGenericHook(scanSpec_->valueHook()));
      } else {
        readHelper<common::AlwaysTrue, false>(
            &dwio::common::alwaysTrue(),
            rows,
            dwio::common::ExtractToGenericHook(scanSpec_->valueHook()));
      }
      return;
    }
    if (isDense) {
      processFilter<true>(
          scanSpec_->filter(), rows, dwio::common::ExtractToReader(this));
    } else {
      processFilter<false>(
          scanSpec_->filter(), rows, dwio::common::ExtractToReader(this));
    }
  } else {
    if (isDense) {
      processFilter<true>(
          scanSpec_->filter(), rows, dwio::common::DropValues());
    } else {
      processFilter<false>(
          scanSpec_->filter(), rows, dwio::common::DropValues());
    }
  }
}

void StringColumnReader::getValues(RowSet rows, VectorPtr* result) {
  if (scanState_.dictionary.values) {
    auto dictionaryValues = formatData_->as<ParquetData>().dictionaryValues();
    compactScalarValues<int32_t, int32_t>(rows, false);

    *result = std::make_shared<DictionaryVector<StringView>>(
        &memoryPool_,
        !anyNulls_               ? nullptr
            : returnReaderNulls_ ? nullsInReadRange_
                                 : resultNulls_,
        numValues_,
        dictionaryValues,
        values_);
    return;
  }
  rawStringBuffer_ = nullptr;
  rawStringSize_ = 0;
  rawStringUsed_ = 0;
  getFlatValues<StringView, StringView>(rows, result, type_);
}

void StringColumnReader::dedictionarize() {
  if (scanSpec_->keepValues()) {
    auto dict = formatData_->as<ParquetData>()
                    .dictionaryValues()
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
