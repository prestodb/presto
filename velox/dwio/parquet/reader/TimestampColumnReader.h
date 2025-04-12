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
#include "velox/dwio/parquet/thrift/ParquetThriftTypes.h"

namespace facebook::velox::parquet {
namespace {

Timestamp toInt64Timestamp(int64_t value, TimestampPrecision filePrecision) {
  switch (filePrecision) {
    case TimestampPrecision::kMilliseconds:
      return Timestamp::fromMillis(value);
    case TimestampPrecision::kMicroseconds:
      return Timestamp::fromMicros(value);
    case TimestampPrecision::kNanoseconds:
      return Timestamp::fromNanos(value);
    default:
      VELOX_UNREACHABLE();
  }
}

Timestamp toInt96Timestamp(const int128_t& value) {
  // Convert int128_t to Int96 Timestamp by extracting days and nanos.
  const int32_t days = static_cast<int32_t>(value >> 64);
  const uint64_t nanos = value & ((((1ULL << 63) - 1ULL) << 1) + 1);
  return Timestamp::fromDaysAndNanos(days, nanos);
}

// Range filter for Parquet Timestamp.
template <typename T>
class ParquetTimestampRange final : public common::TimestampRange {
 public:
  // Use int128_t for Int96
  static_assert(std::is_same_v<T, int64_t> || std::is_same_v<T, int128_t>);

  // @param lower Lower end of the range, inclusive.
  // @param upper Upper end of the range, inclusive.
  // @param nullAllowed Null values are passing the filter if true.
  // @param timestampUnit Unit of the Int64 Timestamp.
  ParquetTimestampRange(
      const Timestamp& lower,
      const Timestamp& upper,
      bool nullAllowed,
      TimestampPrecision filePrecision)
      : TimestampRange(lower, upper, nullAllowed),
        filePrecision_(filePrecision) {}

  bool testInt128(const int128_t& value) const final {
    Timestamp ts;
    if constexpr (std::is_same_v<T, int64_t>) {
      ts = toInt64Timestamp(value, filePrecision_);
    } else if constexpr (std::is_same_v<T, int128_t>) {
      ts = toInt96Timestamp(value);
    }
    return ts >= this->lower() && ts <= this->upper();
  }

 private:
  // Only used when T is int64_t.
  const TimestampPrecision filePrecision_;
};

} // namespace

template <typename T>
class TimestampColumnReader : public IntegerColumnReader {
 public:
  // Use int128_t for Int96
  static_assert(std::is_same_v<T, int64_t> || std::is_same_v<T, int128_t>);

  TimestampColumnReader(
      const TypePtr& requestedType,
      std::shared_ptr<const dwio::common::TypeWithId> fileType,
      ParquetParams& params,
      common::ScanSpec& scanSpec)
      : IntegerColumnReader(requestedType, fileType, params, scanSpec),
        requestedPrecision_(params.timestampPrecision()) {
    if constexpr (std::is_same_v<T, int64_t>) {
      const auto typeWithId =
          std::static_pointer_cast<const ParquetTypeWithId>(fileType_);
      if (auto logicalType = typeWithId->logicalType_) {
        VELOX_CHECK(logicalType->__isset.TIMESTAMP);
        const auto unit = logicalType->TIMESTAMP.unit;
        if (unit.__isset.MILLIS) {
          filePrecision_ = TimestampPrecision::kMilliseconds;
        } else if (unit.__isset.MICROS) {
          filePrecision_ = TimestampPrecision::kMicroseconds;
        } else if (unit.__isset.NANOS) {
          filePrecision_ = TimestampPrecision::kNanoseconds;
        } else {
          VELOX_UNREACHABLE();
        }
      } else if (auto convertedType = typeWithId->convertedType_) {
        if (convertedType == thrift::ConvertedType::type::TIMESTAMP_MILLIS) {
          filePrecision_ = TimestampPrecision::kMilliseconds;
        } else if (
            convertedType == thrift::ConvertedType::type::TIMESTAMP_MICROS) {
          filePrecision_ = TimestampPrecision::kMicroseconds;
        } else {
          VELOX_UNREACHABLE();
        }
      } else {
        VELOX_NYI("Logical type and converted type are not provided.");
      }
      if (filePrecision_ != requestedPrecision_) {
        needsConversion_ = true;
      }
    }
  }

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

      const int128_t encoded = reinterpret_cast<int128_t&>(rawValues[i]);
      if constexpr (std::is_same_v<T, int64_t>) {
        rawValues[i] = toInt64Timestamp(encoded, filePrecision_);
        if (needsConversion_) {
          rawValues[i] = rawValues[i].toPrecision(requestedPrecision_);
        }
      } else if constexpr (std::is_same_v<T, int128_t>) {
        rawValues[i] =
            toInt96Timestamp(encoded).toPrecision(requestedPrecision_);
      }
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
      ParquetTimestampRange<T> newRange{
          range->lower(), range->upper(), range->nullAllowed(), filePrecision_};
      this->readWithVisitor(
          rows,
          dwio::common::ColumnVisitor<
              int128_t,
              common::TimestampRange,
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
  const TimestampPrecision requestedPrecision_;

  // The precision of int64_t timestamp in Parquet. Only set when T is int64_t.
  TimestampPrecision filePrecision_;

  // Whether Int64 Timestamp needs to be converted to the requested precision.
  bool needsConversion_ = false;
};

} // namespace facebook::velox::parquet
