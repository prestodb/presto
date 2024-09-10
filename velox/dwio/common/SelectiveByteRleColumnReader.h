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

namespace facebook::velox::dwio::common {

class SelectiveByteRleColumnReader : public SelectiveColumnReader {
 public:
  SelectiveByteRleColumnReader(
      const TypePtr& requestedType,
      std::shared_ptr<const dwio::common::TypeWithId> fileType,
      dwio::common::FormatParams& params,
      velox::common::ScanSpec& scanSpec)
      : SelectiveColumnReader(
            requestedType,
            std::move(fileType),
            params,
            scanSpec) {}

  bool hasBulkPath() const override {
    return false;
  }

  void getValues(const RowSet& rows, VectorPtr* result) override;

  template <
      typename Reader,
      bool isDense,
      bool kEncodingHasNulls,
      typename ExtractValues>
  void processFilter(
      velox::common::Filter* filter,
      ExtractValues extractValues,
      const RowSet& rows);

  template <typename Reader, bool isDense>
  void processValueHook(const RowSet& rows, ValueHook* hook);

  template <
      typename Reader,
      typename TFilter,
      bool isDense,
      typename ExtractValues>
  void readHelper(
      velox::common::Filter* filter,
      const RowSet& rows,
      ExtractValues extractValues);

  template <typename Reader, bool kEncodingHasNulls>
  void readCommon(
      vector_size_t offset,
      const RowSet& rows,
      const uint64_t* incomingNulls);
};

template <
    typename Reader,
    typename TFilter,
    bool isDense,
    typename ExtractValues>
void SelectiveByteRleColumnReader::readHelper(
    velox::common::Filter* filter,
    const RowSet& rows,
    ExtractValues extractValues) {
  reinterpret_cast<Reader*>(this)->readWithVisitor(
      rows,
      ColumnVisitor<int8_t, TFilter, ExtractValues, isDense>(
          *reinterpret_cast<TFilter*>(filter), this, rows, extractValues));
}

template <
    typename Reader,
    bool isDense,
    bool kEncodingHasNulls,
    typename ExtractValues>
void SelectiveByteRleColumnReader::processFilter(
    velox::common::Filter* filter,
    ExtractValues extractValues,
    const RowSet& rows) {
  using velox::common::FilterKind;
  switch (filter ? filter->kind() : FilterKind::kAlwaysTrue) {
    case FilterKind::kAlwaysTrue:
      readHelper<Reader, velox::common::AlwaysTrue, isDense>(
          filter, rows, extractValues);
      break;
    case FilterKind::kIsNull:
      if constexpr (kEncodingHasNulls) {
        filterNulls<int8_t>(
            rows,
            true,
            !std::is_same_v<decltype(extractValues), dwio::common::DropValues>);
      } else {
        readHelper<Reader, velox::common::IsNull, isDense>(
            filter, rows, extractValues);
      }
      break;
    case FilterKind::kIsNotNull:
      if constexpr (
          kEncodingHasNulls &&
          std::is_same_v<decltype(extractValues), dwio::common::DropValues>) {
        filterNulls<int8_t>(rows, false, false);
      } else {
        readHelper<Reader, velox::common::IsNotNull, isDense>(
            filter, rows, extractValues);
      }
      break;
    case FilterKind::kBigintRange:
      readHelper<Reader, velox::common::BigintRange, isDense>(
          filter, rows, extractValues);
      break;
    case FilterKind::kNegatedBigintRange:
      readHelper<Reader, velox::common::NegatedBigintRange, isDense>(
          filter, rows, extractValues);
      break;
    case FilterKind::kBigintValuesUsingBitmask:
      readHelper<Reader, velox::common::BigintValuesUsingBitmask, isDense>(
          filter, rows, extractValues);
      break;
    case FilterKind::kNegatedBigintValuesUsingBitmask:
      readHelper<
          Reader,
          velox::common::NegatedBigintValuesUsingBitmask,
          isDense>(filter, rows, extractValues);
      break;
    default:
      readHelper<Reader, velox::common::Filter, isDense>(
          filter, rows, extractValues);
      break;
  }
}

template <typename Reader, bool isDense>
void SelectiveByteRleColumnReader::processValueHook(
    const RowSet& rows,
    ValueHook* hook) {
  using namespace facebook::velox::aggregate;
  switch (hook->kind()) {
    case aggregate::AggregationHook::kBigintSum:
      readHelper<Reader, velox::common::AlwaysTrue, isDense>(
          &alwaysTrue(), rows, ExtractToHook<SumHook<int64_t>>(hook));
      break;
    default:
      readHelper<Reader, velox::common::AlwaysTrue, isDense>(
          &alwaysTrue(), rows, ExtractToGenericHook(hook));
  }
}

template <typename Reader, bool kEncodingHasNulls>
void SelectiveByteRleColumnReader::readCommon(
    vector_size_t offset,
    const RowSet& rows,
    const uint64_t* incomingNulls) {
  prepareRead<int8_t>(offset, rows, incomingNulls);
  const bool isDense = rows.back() == rows.size() - 1;
  velox::common::Filter* filter =
      scanSpec_->filter() ? scanSpec_->filter() : &dwio::common::alwaysTrue();
  if (scanSpec_->keepValues()) {
    if (scanSpec_->valueHook()) {
      if (isDense) {
        processValueHook<Reader, true>(rows, scanSpec_->valueHook());
      } else {
        processValueHook<Reader, false>(rows, scanSpec_->valueHook());
      }
      return;
    }
    if (isDense) {
      processFilter<Reader, true, kEncodingHasNulls>(
          filter, dwio::common::ExtractToReader(this), rows);
    } else {
      processFilter<Reader, false, kEncodingHasNulls>(
          filter, dwio::common::ExtractToReader(this), rows);
    }
  } else {
    if (isDense) {
      processFilter<Reader, true, kEncodingHasNulls>(
          filter, dwio::common::DropValues(), rows);
    } else {
      processFilter<Reader, false, kEncodingHasNulls>(
          filter, dwio::common::DropValues(), rows);
    }
  }
}

} // namespace facebook::velox::dwio::common
