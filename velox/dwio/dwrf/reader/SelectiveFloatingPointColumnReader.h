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

#include "velox/dwio/dwrf/reader/SelectiveColumnReaderInternal.h"

namespace facebook::velox::dwrf {

template <typename TData, typename TRequested>
class SelectiveFloatingPointColumnReader : public SelectiveColumnReader {
 public:
  using ValueType = TRequested;
  SelectiveFloatingPointColumnReader(
      std::shared_ptr<const dwio::common::TypeWithId> nodeType,
      DwrfParams& params,
      common::ScanSpec& scanSpec);

  // Offers fast path only if data and result widths match.
  bool hasBulkPath() const override {
    return std::is_same<TData, TRequested>::value;
  }

  void seekToRowGroup(uint32_t index) override {
    auto positionsProvider = formatData_->seekToRowGroup(index);
    decoder_.seekToRowGroup(positionsProvider);
    VELOX_CHECK(!positionsProvider.hasNext());
  }

  uint64_t skip(uint64_t numValues) override;
  void read(vector_size_t offset, RowSet rows, const uint64_t* incomingNulls)
      override;

  void getValues(RowSet rows, VectorPtr* result) override {
    getFlatValues<TRequested, TRequested>(rows, result);
  }

 private:
  template <typename TVisitor>
  void readWithVisitor(RowSet rows, TVisitor visitor);

  template <typename TFilter, bool isDense, typename ExtractValues>
  void readHelper(common::Filter* filter, RowSet rows, ExtractValues values);

  template <bool isDense, typename ExtractValues>
  void processFilter(
      common::Filter* filter,
      RowSet rows,
      ExtractValues extractValues);

  template <bool isDense>
  void processValueHook(RowSet rows, ValueHook* hook);

  FloatingPointDecoder<TData, TRequested> decoder_;
};

template <typename TData, typename TRequested>
SelectiveFloatingPointColumnReader<TData, TRequested>::
    SelectiveFloatingPointColumnReader(
        std::shared_ptr<const dwio::common::TypeWithId> requestedType,
        DwrfParams& params,
        common::ScanSpec& scanSpec)
    : SelectiveColumnReader(
          std::move(requestedType),
          params,
          scanSpec,
          CppToType<TData>::create()),
      decoder_(params.stripeStreams().getStream(
          EncodingKey{nodeType_->id, params.flatMapContext().sequence}.forKind(
              proto::Stream_Kind_DATA),
          true)) {}

template <typename TData, typename TRequested>
uint64_t SelectiveFloatingPointColumnReader<TData, TRequested>::skip(
    uint64_t numValues) {
  numValues = formatData_->skipNulls(numValues);
  decoder_.skip(numValues);
  return numValues;
}

template <typename TData, typename TRequested>
template <typename TVisitor>
void SelectiveFloatingPointColumnReader<TData, TRequested>::readWithVisitor(
    RowSet rows,
    TVisitor visitor) {
  vector_size_t numRows = rows.back() + 1;
  if (nullsInReadRange_) {
    decoder_.template readWithVisitor<true, TVisitor>(
        SelectiveColumnReader::nullsInReadRange_->as<uint64_t>(), visitor);
  } else {
    decoder_.template readWithVisitor<false, TVisitor>(nullptr, visitor);
  }
  readOffset_ += numRows;
}

template <typename TData, typename TRequested>
template <typename TFilter, bool isDense, typename ExtractValues>
void SelectiveFloatingPointColumnReader<TData, TRequested>::readHelper(
    common::Filter* filter,
    RowSet rows,
    ExtractValues extractValues) {
  readWithVisitor(
      rows,
      ColumnVisitor<TRequested, TFilter, ExtractValues, isDense>(
          *reinterpret_cast<TFilter*>(filter), this, rows, extractValues));
}

template <typename TData, typename TRequested>
template <bool isDense, typename ExtractValues>
void SelectiveFloatingPointColumnReader<TData, TRequested>::processFilter(
    common::Filter* filter,
    RowSet rows,
    ExtractValues extractValues) {
  switch (filter ? filter->kind() : common::FilterKind::kAlwaysTrue) {
    case common::FilterKind::kAlwaysTrue:
      readHelper<common::AlwaysTrue, isDense>(filter, rows, extractValues);
      break;
    case common::FilterKind::kIsNull:
      filterNulls<TRequested>(
          rows,
          true,
          !std::is_same<decltype(extractValues), DropValues>::value);
      break;
    case common::FilterKind::kIsNotNull:
      if (std::is_same<decltype(extractValues), DropValues>::value) {
        filterNulls<TRequested>(rows, false, false);
      } else {
        readHelper<common::IsNotNull, isDense>(filter, rows, extractValues);
      }
      break;
    case common::FilterKind::kDoubleRange:
    case common::FilterKind::kFloatRange:
      readHelper<common::FloatingPointRange<TData>, isDense>(
          filter, rows, extractValues);
      break;
    default:
      readHelper<common::Filter, isDense>(filter, rows, extractValues);
      break;
  }
}

template <typename TData, typename TRequested>
template <bool isDense>
void SelectiveFloatingPointColumnReader<TData, TRequested>::processValueHook(
    RowSet rows,
    ValueHook* hook) {
  switch (hook->kind()) {
    case aggregate::AggregationHook::kSumFloatToDouble:
      readHelper<common::AlwaysTrue, isDense>(
          &alwaysTrue(),
          rows,
          ExtractToHook<aggregate::SumHook<float, double>>(hook));
      break;
    case aggregate::AggregationHook::kSumDoubleToDouble:
      readHelper<common::AlwaysTrue, isDense>(
          &alwaysTrue(),
          rows,
          ExtractToHook<aggregate::SumHook<double, double>>(hook));
      break;
    case aggregate::AggregationHook::kFloatMax:
    case aggregate::AggregationHook::kDoubleMax:
      readHelper<common::AlwaysTrue, isDense>(
          &alwaysTrue(),
          rows,
          ExtractToHook<aggregate::MinMaxHook<TRequested, false>>(hook));
      break;
    case aggregate::AggregationHook::kFloatMin:
    case aggregate::AggregationHook::kDoubleMin:
      readHelper<common::AlwaysTrue, isDense>(
          &alwaysTrue(),
          rows,
          ExtractToHook<aggregate::MinMaxHook<TRequested, true>>(hook));
      break;
    default:
      readHelper<common::AlwaysTrue, isDense>(
          &alwaysTrue(), rows, ExtractToGenericHook(hook));
  }
}

template <typename TData, typename TRequested>
void SelectiveFloatingPointColumnReader<TData, TRequested>::read(
    vector_size_t offset,
    RowSet rows,
    const uint64_t* incomingNulls) {
  prepareRead<TRequested>(offset, rows, incomingNulls);
  bool isDense = rows.back() == rows.size() - 1;
  if (scanSpec_->keepValues()) {
    if (scanSpec_->valueHook()) {
      if (isDense) {
        processValueHook<true>(rows, scanSpec_->valueHook());
      } else {
        processValueHook<false>(rows, scanSpec_->valueHook());
      }
      return;
    }
    if (isDense) {
      processFilter<true>(scanSpec_->filter(), rows, ExtractToReader(this));
    } else {
      processFilter<false>(scanSpec_->filter(), rows, ExtractToReader(this));
    }
  } else {
    if (isDense) {
      processFilter<true>(scanSpec_->filter(), rows, DropValues());
    } else {
      processFilter<false>(scanSpec_->filter(), rows, DropValues());
    }
  }
}

} // namespace facebook::velox::dwrf
