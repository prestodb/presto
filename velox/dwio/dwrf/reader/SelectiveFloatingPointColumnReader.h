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

#include "velox/dwio/common/SelectiveFloatingPointColumnReader.h"
#include "velox/dwio/dwrf/common/FloatingPointDecoder.h"

namespace facebook::velox::dwrf {

template <typename TData, typename TRequested>
class SelectiveFloatingPointColumnReader
    : public dwio::common::
          SelectiveFloatingPointColumnReader<TData, TRequested> {
 public:
  using ValueType = TRequested;

  using root = dwio::common::SelectiveColumnReader;
  using base =
      dwio::common::SelectiveFloatingPointColumnReader<TData, TRequested>;

  SelectiveFloatingPointColumnReader(
      std::shared_ptr<const dwio::common::TypeWithId> nodeType,
      DwrfParams& params,
      common::ScanSpec& scanSpec);

  void seekToRowGroup(uint32_t index) override {
    auto positionsProvider = root::formatData_->seekToRowGroup(index);
    decoder_.seekToRowGroup(positionsProvider);
    VELOX_CHECK(!positionsProvider.hasNext());
  }

  uint64_t skip(uint64_t numValues) override;

  void read(vector_size_t offset, RowSet rows, const uint64_t* incomingNulls)
      override {
    using T = SelectiveFloatingPointColumnReader<TData, TRequested>;
    base::template readCommon<T>(offset, rows, incomingNulls);
  }

  template <typename TVisitor>
  void readWithVisitor(RowSet rows, TVisitor visitor);

  FloatingPointDecoder<TData, TRequested> decoder_;
};

template <typename TData, typename TRequested>
SelectiveFloatingPointColumnReader<TData, TRequested>::
    SelectiveFloatingPointColumnReader(
        std::shared_ptr<const dwio::common::TypeWithId> requestedType,
        DwrfParams& params,
        common::ScanSpec& scanSpec)
    : dwio::common::SelectiveFloatingPointColumnReader<TData, TRequested>(
          std::move(requestedType),
          params,
          scanSpec),
      decoder_(params.stripeStreams().getStream(
          EncodingKey{root::nodeType_->id, params.flatMapContext().sequence}
              .forKind(proto::Stream_Kind_DATA),
          true)) {}

template <typename TData, typename TRequested>
uint64_t SelectiveFloatingPointColumnReader<TData, TRequested>::skip(
    uint64_t numValues) {
  numValues = root::formatData_->skipNulls(numValues);
  decoder_.skip(numValues);
  return numValues;
}

template <typename TData, typename TRequested>
template <typename TVisitor>
void SelectiveFloatingPointColumnReader<TData, TRequested>::readWithVisitor(
    RowSet rows,
    TVisitor visitor) {
  vector_size_t numRows = rows.back() + 1;
  if (root::nullsInReadRange_) {
    decoder_.template readWithVisitor<true, TVisitor>(
        root::nullsInReadRange_->as<uint64_t>(), visitor);
  } else {
    decoder_.template readWithVisitor<false, TVisitor>(nullptr, visitor);
  }
  root::readOffset_ += numRows;
}

} // namespace facebook::velox::dwrf
