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
#include "velox/dwio/dwrf/reader/DwrfData.h"

namespace facebook::velox::dwrf {
class SelectiveTimestampColumnReader
    : public dwio::common::SelectiveColumnReader {
 public:
  // The readers produce int64_t, the vector is Timestamps.
  using ValueType = int64_t;

  SelectiveTimestampColumnReader(
      const std::shared_ptr<const dwio::common::TypeWithId>& nodeType,
      DwrfParams& params,
      common::ScanSpec& scanSpec);

  void seekToRowGroup(uint32_t index) override;
  uint64_t skip(uint64_t numValues) override;

  void read(vector_size_t offset, RowSet rows, const uint64_t* incomingNulls)
      override;

  void getValues(RowSet rows, VectorPtr* result) override;

 private:
  template <bool dense>
  void readHelper(RowSet rows);

  std::unique_ptr<dwio::common::IntDecoder</*isSigned*/ true>> seconds_;
  std::unique_ptr<dwio::common::IntDecoder</*isSigned*/ false>> nano_;

  // Values from copied from 'seconds_'. Nanos are in 'values_'.
  BufferPtr secondsValues_;
};

} // namespace facebook::velox::dwrf
