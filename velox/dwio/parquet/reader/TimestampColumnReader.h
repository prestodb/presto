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

  void getValues(RowSet rows, VectorPtr* result) override {
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
      const auto timestamp = rawValues[i];
      uint64_t nanos = timestamp.getNanos();
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

  void read(
      vector_size_t offset,
      RowSet rows,
      const uint64_t* /*incomingNulls*/) override {
    auto& data = formatData_->as<ParquetData>();
    // Use int128_t as a workaroud. Timestamp in Velox is of 16-byte length.
    prepareRead<int128_t>(offset, rows, nullptr);
    readCommon<IntegerColumnReader, true>(rows);
    readOffset_ += rows.back() + 1;
  }

 private:
  // The requested precision can be specified from HiveConfig to read timestamp
  // from Parquet.
  TimestampPrecision timestampPrecision_;
};

} // namespace facebook::velox::parquet
