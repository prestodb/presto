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

#include "velox/dwio/common/BufferUtil.h"
#include "velox/dwio/common/SelectiveColumnReaderInternal.h"
#include "velox/dwio/dwrf/common/DecoderUtil.h"
#include "velox/dwio/dwrf/reader/DwrfData.h"

namespace facebook::velox::dwrf {

using namespace dwio::common;

template <typename DataT>
class SelectiveDecimalColumnReader : public SelectiveColumnReader {
 public:
  SelectiveDecimalColumnReader(
      const std::shared_ptr<const TypeWithId>& fileType,
      DwrfParams& params,
      common::ScanSpec& scanSpec);

  bool hasBulkPath() const override {
    // Only ORC uses RLEv2 encoding. Currently, ORC decimal data does not
    // support fastpath reads. When reading RLEv2-encoded decimal data
    // with null, the query will fail.
    return version_ != velox::dwrf::RleVersion_2;
  }

  void seekToRowGroup(uint32_t index) override;
  uint64_t skip(uint64_t numValues) override;

  void read(vector_size_t offset, const RowSet& rows, const uint64_t* nulls)
      override;

  void getValues(const RowSet& rows, VectorPtr* result) override;

 private:
  template <bool kDense>
  void readHelper(RowSet rows);

  std::unique_ptr<IntDecoder<true>> valueDecoder_;
  std::unique_ptr<IntDecoder<true>> scaleDecoder_;

  BufferPtr scaleBuffer_;
  RleVersion version_;
  int32_t scale_ = 0;
};

} // namespace facebook::velox::dwrf
