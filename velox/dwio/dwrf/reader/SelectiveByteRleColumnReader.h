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

#include "velox/dwio/common/SelectiveByteRleColumnReader.h"

namespace facebook::velox::dwrf {

class SelectiveByteRleColumnReader
    : public dwio::common::SelectiveByteRleColumnReader {
 public:
  using ValueType = int8_t;

  SelectiveByteRleColumnReader(
      const TypePtr& requestedType,
      std::shared_ptr<const dwio::common::TypeWithId> fileType,
      DwrfParams& params,
      common::ScanSpec& scanSpec,
      bool isBool)
      : dwio::common::SelectiveByteRleColumnReader(
            requestedType,
            std::move(fileType),
            params,
            scanSpec) {
    const EncodingKey encodingKey{
        fileType_->id(), params.flatMapContext().sequence};
    auto& stripe = params.stripeStreams();
    if (isBool) {
      boolRle_ = createBooleanRleDecoder(
          stripe.getStream(
              encodingKey.forKind(proto::Stream_Kind_DATA),
              params.streamLabels().label(),
              true),
          encodingKey);
    } else {
      byteRle_ = createByteRleDecoder(
          stripe.getStream(
              encodingKey.forKind(proto::Stream_Kind_DATA),
              params.streamLabels().label(),
              true),
          encodingKey);
    }
  }

  void seekToRowGroup(uint32_t index) override {
    dwio::common::SelectiveByteRleColumnReader::seekToRowGroup(index);
    auto positionsProvider = formatData_->seekToRowGroup(index);
    if (boolRle_) {
      boolRle_->seekToRowGroup(positionsProvider);
    } else {
      byteRle_->seekToRowGroup(positionsProvider);
    }

    VELOX_CHECK(!positionsProvider.hasNext());
  }

  uint64_t skip(uint64_t numValues) override {
    numValues = formatData_->skipNulls(numValues);
    if (byteRle_) {
      byteRle_->skip(numValues);
    } else {
      boolRle_->skip(numValues);
    }
    return numValues;
  }

  void read(
      vector_size_t offset,
      const RowSet& rows,
      const uint64_t* incomingNulls) override {
    readCommon<SelectiveByteRleColumnReader, true>(offset, rows, incomingNulls);
    readOffset_ += rows.back() + 1;
  }

  template <typename ColumnVisitor>
  void readWithVisitor(const RowSet& rows, ColumnVisitor visitor);

  std::unique_ptr<ByteRleDecoder> byteRle_;
  std::unique_ptr<BooleanRleDecoder> boolRle_;
};

template <typename ColumnVisitor>
void SelectiveByteRleColumnReader::readWithVisitor(
    const RowSet& rows,
    ColumnVisitor visitor) {
  if (boolRle_) {
    if (nullsInReadRange_) {
      boolRle_->readWithVisitor<true>(
          nullsInReadRange_->as<uint64_t>(), visitor);
    } else {
      boolRle_->readWithVisitor<false>(nullptr, visitor);
    }
  } else {
    if (nullsInReadRange_) {
      byteRle_->readWithVisitor<true>(
          nullsInReadRange_->as<uint64_t>(), visitor);
    } else {
      byteRle_->readWithVisitor<false>(nullptr, visitor);
    }
  }
}

} // namespace facebook::velox::dwrf
