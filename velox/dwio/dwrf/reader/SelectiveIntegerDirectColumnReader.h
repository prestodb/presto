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

#include "velox/dwio/common/SelectiveIntegerColumnReader.h"
#include "velox/dwio/dwrf/common/DecoderUtil.h"
#include "velox/dwio/dwrf/reader/DwrfData.h"

namespace facebook::velox::dwrf {

class SelectiveIntegerDirectColumnReader
    : public dwio::common::SelectiveIntegerColumnReader {
 public:
  using ValueType = int64_t;

  SelectiveIntegerDirectColumnReader(
      std::shared_ptr<const dwio::common::TypeWithId> requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
      DwrfParams& params,
      uint32_t numBytes,
      common::ScanSpec& scanSpec)
      : SelectiveIntegerColumnReader(
            std::move(requestedType),
            params,
            scanSpec,
            dataType->type) {
    EncodingKey encodingKey{nodeType_->id, params.flatMapContext().sequence};
    auto data = encodingKey.forKind(proto::Stream_Kind_DATA);
    auto& stripe = params.stripeStreams();
    bool dataVInts = stripe.getUseVInts(data);
    auto decoder = createDirectDecoder</*isSigned*/ true>(
        stripe.getStream(data, true), dataVInts, numBytes);
    auto rawDecoder = decoder.release();
    auto directDecoder =
        dynamic_cast<dwio::common::DirectDecoder<true>*>(rawDecoder);
    ints.reset(directDecoder);
  }

  bool hasBulkPath() const override {
    return true;
  }

  void seekToRowGroup(uint32_t index) override {
    SelectiveColumnReader::seekToRowGroup(index);
    auto positionsProvider = formatData_->seekToRowGroup(index);
    ints->seekToRowGroup(positionsProvider);

    VELOX_CHECK(!positionsProvider.hasNext());
  }

  uint64_t skip(uint64_t numValues) override;

  void read(vector_size_t offset, RowSet rows, const uint64_t* incomingNulls)
      override;

  template <typename ColumnVisitor>
  void readWithVisitor(RowSet rows, ColumnVisitor visitor);

 private:
  std::unique_ptr<dwio::common::DirectDecoder</*isSigned*/ true>> ints;
};

template <typename ColumnVisitor>
void SelectiveIntegerDirectColumnReader::readWithVisitor(
    RowSet rows,
    ColumnVisitor visitor) {
  vector_size_t numRows = rows.back() + 1;
  if (nullsInReadRange_) {
    ints->readWithVisitor<true>(nullsInReadRange_->as<uint64_t>(), visitor);
  } else {
    ints->readWithVisitor<false>(nullptr, visitor);
  }
  readOffset_ += numRows;
}
} // namespace facebook::velox::dwrf
