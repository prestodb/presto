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

template <typename TFile, typename TRequested>
class SelectiveFloatingPointColumnReader
    : public dwio::common::
          SelectiveFloatingPointColumnReader<TFile, TRequested> {
 public:
  using ValueType = TRequested;

  using base =
      dwio::common::SelectiveFloatingPointColumnReader<TFile, TRequested>;

  SelectiveFloatingPointColumnReader(
      const TypePtr& requestedType,
      std::shared_ptr<const dwio::common::TypeWithId> fileType,
      DwrfParams& params,
      common::ScanSpec& scanSpec);

  void seekToRowGroup(uint32_t index) override {
    base::seekToRowGroup(index);
    auto positionsProvider = this->formatData_->seekToRowGroup(index);
    decoder_.seekToRowGroup(positionsProvider);
    VELOX_CHECK(!positionsProvider.hasNext());
  }

  uint64_t skip(uint64_t numValues) override;

  void read(vector_size_t offset, RowSet rows, const uint64_t* incomingNulls)
      override {
    using T = SelectiveFloatingPointColumnReader<TFile, TRequested>;
    this->template readCommon<T, true>(offset, rows, incomingNulls);
    this->readOffset_ += rows.back() + 1;
  }

  template <typename TVisitor>
  void readWithVisitor(RowSet rows, TVisitor visitor);

  FloatingPointDecoder<TFile, TRequested> decoder_;
};

template <typename TFile, typename TRequested>
SelectiveFloatingPointColumnReader<TFile, TRequested>::
    SelectiveFloatingPointColumnReader(
        const TypePtr& requestedType,
        std::shared_ptr<const dwio::common::TypeWithId> fileType,
        DwrfParams& params,
        common::ScanSpec& scanSpec)
    : dwio::common::SelectiveFloatingPointColumnReader<TFile, TRequested>(
          requestedType,
          std::move(fileType),
          params,
          scanSpec),
      decoder_(params.stripeStreams().getStream(
          EncodingKey{this->fileType_->id(), params.flatMapContext().sequence}
              .forKind(proto::Stream_Kind_DATA),
          params.streamLabels().label(),
          true)) {}

template <typename TFile, typename TRequested>
uint64_t SelectiveFloatingPointColumnReader<TFile, TRequested>::skip(
    uint64_t numValues) {
  numValues = this->formatData_->skipNulls(numValues);
  decoder_.skip(numValues);
  return numValues;
}

template <typename TFile, typename TRequested>
template <typename TVisitor>
void SelectiveFloatingPointColumnReader<TFile, TRequested>::readWithVisitor(
    RowSet rows,
    TVisitor visitor) {
  if (this->nullsInReadRange_) {
    decoder_.template readWithVisitor<true, TVisitor>(
        this->nullsInReadRange_->template as<uint64_t>(), visitor);
  } else {
    decoder_.template readWithVisitor<false, TVisitor>(nullptr, visitor);
  }
}

} // namespace facebook::velox::dwrf
