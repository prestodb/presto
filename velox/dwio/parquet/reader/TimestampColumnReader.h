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

#include "velox/dwio/parquet/reader/IntegerColumnReader.h"
#include "velox/dwio/parquet/reader/ParquetColumnReader.h"

namespace facebook::velox::parquet {
namespace {

// Range filter for Parquet Int96 Timestamp.
class ParquetInt96TimestampRange final : public common::TimestampRange {
 public:
  // @param lower Lower end of the range, inclusive.
  // @param upper Upper end of the range, inclusive.
  // @param nullAllowed Null values are passing the filter if true.
  ParquetInt96TimestampRange(
      const Timestamp& lower,
      const Timestamp& upper,
      bool nullAllowed)
      : TimestampRange(lower, upper, nullAllowed) {}

  // Int96 is read as int128_t value and converted to Timestamp by extracting
  // days and nanos.
  bool testInt128(const int128_t& value) const final {
    const int32_t days = static_cast<int32_t>(value >> 64);
    const uint64_t nanos = value & ((((1ULL << 63) - 1ULL) << 1) + 1);
    const auto ts = Timestamp::fromDaysAndNanos(days, nanos);
    return ts >= this->lower() && ts <= this->upper();
  }
};

} // namespace

class TimestampColumnReader : public IntegerColumnReader {
 public:
  TimestampColumnReader(
      const TypePtr& requestedType,
      std::shared_ptr<const dwio::common::TypeWithId> fileType,
      ParquetParams& params,
      common::ScanSpec& scanSpec)
      : IntegerColumnReader(requestedType, fileType, params, scanSpec),
        timestampPrecision_(params.timestampPrecision()) {}

  bool hasBulkPath() const override {
    return false;
  }

  void getValues(const RowSet& rows, VectorPtr* result) override {
    getFlatValues<Timestamp, Timestamp>(rows, result, requestedType_);
    if (allNull_) {
      return;
    }

    // Adjust timestamp nanos to the requested precision.
    VectorPtr resultVector = *result;
    auto rawValues =
        resultVector->asUnchecked<FlatVector<Timestamp>>()->mutableRawValues();
    for (auto i = 0; i < numValues_; ++i) {
      if (resultVector->isNullAt(i)) {
        continue;
      }

      // Convert int128_t to Timestamp by extracting days and nanos.
      const int128_t encoded = reinterpret_cast<int128_t&>(rawValues[i]);
      const int32_t days = static_cast<int32_t>(encoded >> 64);
      uint64_t nanos = encoded & ((((1ULL << 63) - 1ULL) << 1) + 1);
      const auto timestamp = Timestamp::fromDaysAndNanos(days, nanos);

      nanos = timestamp.getNanos();
      switch (timestampPrecision_) {
        case TimestampPrecision::kMilliseconds:
          nanos = nanos / 1'000'000 * 1'000'000;
          break;
        case TimestampPrecision::kMicroseconds:
          nanos = nanos / 1'000 * 1'000;
          break;
        case TimestampPrecision::kNanoseconds:
          break;
      }
      rawValues[i] = Timestamp(timestamp.getSeconds(), nanos);
    }
  }

  template <
      typename Reader,
      typename TFilter,
      bool isDense,
      typename ExtractValues>
  void readHelper(
      velox::common::Filter* filter,
      const RowSet& rows,
      ExtractValues extractValues) {
    if (auto* range = dynamic_cast<common::TimestampRange*>(filter)) {
      // Convert TimestampRange to ParquetInt96TimestampRange.
      ParquetInt96TimestampRange newRange = ParquetInt96TimestampRange(
          range->lower(), range->upper(), range->nullAllowed());
      this->readWithVisitor(
          rows,
          dwio::common::ColumnVisitor<
              int128_t,
              ParquetInt96TimestampRange,
              ExtractValues,
              isDense>(newRange, this, rows, extractValues));
    } else {
      this->readWithVisitor(
          rows,
          dwio::common::
              ColumnVisitor<int128_t, TFilter, ExtractValues, isDense>(
                  *reinterpret_cast<TFilter*>(filter),
                  this,
                  rows,
                  extractValues));
    }
    return;
  }

  void read(
      int64_t offset,
      const RowSet& rows,
      const uint64_t* /*incomingNulls*/) override {
    // Use int128_t as a workaround. Timestamp in Velox is of 16-byte length.
    prepareRead<int128_t>(offset, rows, nullptr);
    readCommon<TimestampColumnReader, true>(rows);
    readOffset_ += rows.back() + 1;
  }

 private:
  // The requested precision can be specified from HiveConfig to read timestamp
  // from Parquet.
  TimestampPrecision timestampPrecision_;
};

} // namespace facebook::velox::parquet
