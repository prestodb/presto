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

class SelectiveIntegerDictionaryColumnReader : public SelectiveColumnReader {
 public:
  using ValueType = int64_t;

  SelectiveIntegerDictionaryColumnReader(
      std::shared_ptr<const dwio::common::TypeWithId> requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
      StripeStreams& stripe,
      common::ScanSpec* scanSpec,
      uint32_t numBytes);

  void seekToRowGroup(uint32_t index) override {
    ensureRowGroupIndex();

    auto positions = toPositions(index_->entry(index));
    PositionProvider positionsProvider(positions);

    if (notNullDecoder_) {
      notNullDecoder_->seekToRowGroup(positionsProvider);
    }

    if (inDictionaryReader_) {
      inDictionaryReader_->seekToRowGroup(positionsProvider);
    }

    dataReader_->seekToRowGroup(positionsProvider);

    VELOX_CHECK(!positionsProvider.hasNext());
  }

  uint64_t skip(uint64_t numValues) override;

  void read(vector_size_t offset, RowSet rows, const uint64_t* incomingNulls)
      override;

  void getValues(RowSet rows, VectorPtr* result) override {
    getIntValues(rows, nodeType_->type.get(), result);
  }

 private:
  template <typename ColumnVisitor>
  void readWithVisitor(RowSet rows, ColumnVisitor visitor);

  template <bool isDense, typename ExtractValues>
  void processFilter(
      common::Filter* filter,
      ExtractValues extractValues,
      RowSet rows);

  template <bool isDence>
  void processValueHook(RowSet rows, ValueHook* hook);

  template <typename TFilter, bool isDense, typename ExtractValues>
  void
  readHelper(common::Filter* filter, RowSet rows, ExtractValues extractValues);

  void ensureInitialized();

  std::unique_ptr<ByteRleDecoder> inDictionaryReader_;
  std::unique_ptr<IntDecoder</* isSigned = */ false>> dataReader_;
  std::unique_ptr<IntDecoder</* isSigned = */ true>> dictReader_;
  std::function<BufferPtr()> dictInit_;
  RleVersion rleVersion_;
  bool initialized_{false};
};

template <typename ColumnVisitor>
void SelectiveIntegerDictionaryColumnReader::readWithVisitor(
    RowSet rows,
    ColumnVisitor visitor) {
  vector_size_t numRows = rows.back() + 1;
  VELOX_CHECK_EQ(rleVersion_, RleVersion_1);
  auto reader = reinterpret_cast<RleDecoderV1<false>*>(dataReader_.get());
  if (nullsInReadRange_) {
    reader->readWithVisitor<true>(nullsInReadRange_->as<uint64_t>(), visitor);
  } else {
    reader->readWithVisitor<false>(nullptr, visitor);
  }
  readOffset_ += numRows;
}

template <typename TFilter, bool isDense, typename ExtractValues>
void SelectiveIntegerDictionaryColumnReader::readHelper(
    common::Filter* filter,
    RowSet rows,
    ExtractValues extractValues) {
  switch (valueSize_) {
    case 2:
      readWithVisitor(
          rows,
          DictionaryColumnVisitor<int16_t, TFilter, ExtractValues, isDense>(
              *reinterpret_cast<TFilter*>(filter), this, rows, extractValues));
      break;
    case 4:
      readWithVisitor(
          rows,
          DictionaryColumnVisitor<int32_t, TFilter, ExtractValues, isDense>(
              *reinterpret_cast<TFilter*>(filter), this, rows, extractValues));
      break;

    case 8:
      readWithVisitor(
          rows,
          DictionaryColumnVisitor<int64_t, TFilter, ExtractValues, isDense>(
              *reinterpret_cast<TFilter*>(filter), this, rows, extractValues));
      break;

    default:
      VELOX_FAIL("Unsupported valueSize_ {}", valueSize_);
  }
}

template <bool isDense, typename ExtractValues>
void SelectiveIntegerDictionaryColumnReader::processFilter(
    common::Filter* filter,
    ExtractValues extractValues,
    RowSet rows) {
  switch (filter ? filter->kind() : common::FilterKind::kAlwaysTrue) {
    case common::FilterKind::kAlwaysTrue:
      readHelper<common::AlwaysTrue, isDense>(filter, rows, extractValues);
      break;
    case common::FilterKind::kIsNull:
      filterNulls<int64_t>(
          rows,
          true,
          !std::is_same<decltype(extractValues), DropValues>::value);
      break;
    case common::FilterKind::kIsNotNull:
      if (std::is_same<decltype(extractValues), DropValues>::value) {
        filterNulls<int64_t>(rows, false, false);
      } else {
        readHelper<common::IsNotNull, isDense>(filter, rows, extractValues);
      }
      break;
    case common::FilterKind::kBigintRange:
      readHelper<common::BigintRange, isDense>(filter, rows, extractValues);
      break;
    case common::FilterKind::kBigintValuesUsingHashTable:
      readHelper<common::BigintValuesUsingHashTable, isDense>(
          filter, rows, extractValues);
      break;
    case common::FilterKind::kBigintValuesUsingBitmask:
      readHelper<common::BigintValuesUsingBitmask, isDense>(
          filter, rows, extractValues);
      break;
    default:
      readHelper<common::Filter, isDense>(filter, rows, extractValues);
      break;
  }
}

template <bool isDense>
void SelectiveIntegerDictionaryColumnReader::processValueHook(
    RowSet rows,
    ValueHook* hook) {
  switch (hook->kind()) {
    case aggregate::AggregationHook::kSumBigintToBigint:
      readHelper<common::AlwaysTrue, isDense>(
          &alwaysTrue(),
          rows,
          ExtractToHook<aggregate::SumHook<int64_t, int64_t>>(hook));
      break;
    default:
      readHelper<common::AlwaysTrue, isDense>(
          &alwaysTrue(), rows, ExtractToGenericHook(hook));
  }
}

} // namespace facebook::velox::dwrf
