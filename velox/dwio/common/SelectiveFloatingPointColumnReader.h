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

template <typename TData, typename TRequested>
class SelectiveFloatingPointColumnReader : public SelectiveColumnReader {
 public:
  using ValueType = TRequested;
  SelectiveFloatingPointColumnReader(
      std::shared_ptr<const dwio::common::TypeWithId> nodeType,
      FormatParams& params,
      velox::common::ScanSpec& scanSpec)
      : SelectiveColumnReader(
            std::move(nodeType),
            params,
            scanSpec,
            CppToType<TData>::create()) {}

  // Offers fast path only if data and result widths match.
  bool hasBulkPath() const override {
    return std::is_same<TData, TRequested>::value;
  }

  template <typename Reader>
  void
  readCommon(vector_size_t offset, RowSet rows, const uint64_t* incomingNulls);

  void getValues(RowSet rows, VectorPtr* result) override {
    getFlatValues<TRequested, TRequested>(rows, result);
  }

 protected:
  template <
      typename Reader,
      typename TFilter,
      bool isDense,
      typename ExtractValues>
  void
  readHelper(velox::common::Filter* filter, RowSet rows, ExtractValues values);

  template <typename Reader, bool isDense, typename ExtractValues>
  void processFilter(
      velox::common::Filter* filter,
      RowSet rows,
      ExtractValues extractValues);

  template <typename Reader, bool isDense>
  void processValueHook(RowSet rows, ValueHook* hook);
};

template <typename TData, typename TRequested>
template <
    typename Reader,
    typename TFilter,
    bool isDense,
    typename ExtractValues>
void SelectiveFloatingPointColumnReader<TData, TRequested>::readHelper(
    velox::common::Filter* filter,
    RowSet rows,
    ExtractValues extractValues) {
  reinterpret_cast<Reader*>(this)->readWithVisitor(
      rows,
      ColumnVisitor<TRequested, TFilter, ExtractValues, isDense>(
          *reinterpret_cast<TFilter*>(filter), this, rows, extractValues));
}

template <typename TData, typename TRequested>
template <typename Reader, bool isDense, typename ExtractValues>
void SelectiveFloatingPointColumnReader<TData, TRequested>::processFilter(
    velox::common::Filter* filter,
    RowSet rows,
    ExtractValues extractValues) {
  switch (filter ? filter->kind() : velox::common::FilterKind::kAlwaysTrue) {
    case velox::common::FilterKind::kAlwaysTrue:
      readHelper<Reader, velox::common::AlwaysTrue, isDense>(
          filter, rows, extractValues);
      break;
    case velox::common::FilterKind::kIsNull:
      filterNulls<TRequested>(
          rows,
          true,
          !std::is_same<decltype(extractValues), DropValues>::value);
      break;
    case velox::common::FilterKind::kIsNotNull:
      if (std::is_same<decltype(extractValues), DropValues>::value) {
        filterNulls<TRequested>(rows, false, false);
      } else {
        readHelper<Reader, velox::common::IsNotNull, isDense>(
            filter, rows, extractValues);
      }
      break;
    case velox::common::FilterKind::kDoubleRange:
    case velox::common::FilterKind::kFloatRange:
      readHelper<Reader, velox::common::FloatingPointRange<TData>, isDense>(
          filter, rows, extractValues);
      break;
    default:
      readHelper<Reader, velox::common::Filter, isDense>(
          filter, rows, extractValues);
      break;
  }
}

template <typename TData, typename TRequested>
template <typename Reader, bool isDense>
void SelectiveFloatingPointColumnReader<TData, TRequested>::processValueHook(
    RowSet rows,
    ValueHook* hook) {
  switch (hook->kind()) {
    case aggregate::AggregationHook::kSumFloatToDouble:
      readHelper<Reader, velox::common::AlwaysTrue, isDense>(
          &alwaysTrue(),
          rows,
          ExtractToHook<aggregate::SumHook<float, double>>(hook));
      break;
    case aggregate::AggregationHook::kSumDoubleToDouble:
      readHelper<Reader, velox::common::AlwaysTrue, isDense>(
          &alwaysTrue(),
          rows,
          ExtractToHook<aggregate::SumHook<double, double>>(hook));
      break;
    case aggregate::AggregationHook::kFloatMax:
    case aggregate::AggregationHook::kDoubleMax:
      readHelper<Reader, velox::common::AlwaysTrue, isDense>(
          &alwaysTrue(),
          rows,
          ExtractToHook<aggregate::MinMaxHook<TRequested, false>>(hook));
      break;
    case aggregate::AggregationHook::kFloatMin:
    case aggregate::AggregationHook::kDoubleMin:
      readHelper<Reader, velox::common::AlwaysTrue, isDense>(
          &alwaysTrue(),
          rows,
          ExtractToHook<aggregate::MinMaxHook<TRequested, true>>(hook));
      break;
    default:
      readHelper<Reader, velox::common::AlwaysTrue, isDense>(
          &alwaysTrue(), rows, ExtractToGenericHook(hook));
  }
}

template <typename TData, typename TRequested>
template <typename Reader>
void SelectiveFloatingPointColumnReader<TData, TRequested>::readCommon(
    vector_size_t offset,
    RowSet rows,
    const uint64_t* incomingNulls) {
  prepareRead<TRequested>(offset, rows, incomingNulls);
  bool isDense = rows.back() == rows.size() - 1;
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
      processFilter<Reader, true>(
          scanSpec_->filter(), rows, ExtractToReader(this));
    } else {
      processFilter<Reader, false>(
          scanSpec_->filter(), rows, ExtractToReader(this));
    }
  } else {
    if (isDense) {
      processFilter<Reader, true>(scanSpec_->filter(), rows, DropValues());
    } else {
      processFilter<Reader, false>(scanSpec_->filter(), rows, DropValues());
    }
  }
}

} // namespace facebook::velox::dwio::common
