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
      const TypePtr& requestedType,
      std::shared_ptr<const dwio::common::TypeWithId> fileType,
      DwrfParams& params,
      uint32_t numBytes,
      common::ScanSpec& scanSpec)
      : SelectiveIntegerColumnReader(
            requestedType,
            params,
            scanSpec,
            std::move(fileType)) {
    const EncodingKey encodingKey{
        fileType_->id(), params.flatMapContext().sequence};
    const auto si = encodingKey.forKind(proto::Stream_Kind_DATA);
    const auto& stripe = params.stripeStreams();
    const bool dataVInts = stripe.getUseVInts(si);

    format_ = stripe.format();
    version_ = convertRleVersion(stripe.getEncoding(encodingKey).kind());
    if (format_ == velox::dwrf::DwrfFormat::kDwrf) {
      intDecoder_ = createDirectDecoder</*isSigned=*/true>(
          stripe.getStream(si, params.streamLabels().label(), true),
          dataVInts,
          numBytes);
    } else if (format_ == velox::dwrf::DwrfFormat::kOrc) {
      version_ = convertRleVersion(stripe.getEncoding(encodingKey).kind());
      intDecoder_ = createRleDecoder</*isSigned*/ true>(
          stripe.getStream(si, params.streamLabels().label(), true),
          version_,
          params.pool(),
          dataVInts,
          numBytes);
    } else {
      VELOX_FAIL("invalid stripe format: {}", format_);
    }
  }

  bool hasBulkPath() const override {
    // Only ORC uses RLEv2 encoding. Currently, ORC integer data does not
    // support fastpath reads. When reading RLEv2-encoded integer data
    // with null, the query will fail.
    return version_ != velox::dwrf::RleVersion_2;
  }

  void seekToRowGroup(uint32_t index) override {
    dwio::common::SelectiveIntegerColumnReader::seekToRowGroup(index);
    auto positionsProvider = formatData_->seekToRowGroup(index);
    intDecoder_->seekToRowGroup(positionsProvider);

    VELOX_CHECK(!positionsProvider.hasNext());
  }

  uint64_t skip(uint64_t numValues) override;

  void read(
      vector_size_t offset,
      const RowSet& rows,
      const uint64_t* incomingNulls) override;

  template <typename ColumnVisitor>
  void readWithVisitor(const RowSet& rows, ColumnVisitor visitor);

 private:
  dwrf::DwrfFormat format_;
  RleVersion version_;
  std::unique_ptr<dwio::common::IntDecoder<true>> intDecoder_;
};

template <typename ColumnVisitor>
void SelectiveIntegerDirectColumnReader::readWithVisitor(
    const RowSet& rows,
    ColumnVisitor visitor) {
  if (format_ == velox::dwrf::DwrfFormat::kDwrf) {
    decodeWithVisitor<dwio::common::DirectDecoder<true>>(
        intDecoder_.get(), visitor);
  } else {
    // orc format does not use int128
    if constexpr (!std::is_same_v<typename ColumnVisitor::DataType, int128_t>) {
      velox::dwio::common::DirectRleColumnVisitor<
          typename ColumnVisitor::DataType,
          typename ColumnVisitor::FilterType,
          typename ColumnVisitor::Extract,
          ColumnVisitor::dense>
          drVisitor(
              visitor.filter(),
              &visitor.reader(),
              rows,
              visitor.extractValues());
      if (version_ == velox::dwrf::RleVersion_1) {
        decodeWithVisitor<velox::dwrf::RleDecoderV1<true>>(
            intDecoder_.get(), drVisitor);
      } else {
        VELOX_CHECK(version_ == velox::dwrf::RleVersion_2);
        decodeWithVisitor<velox::dwrf::RleDecoderV2<true>>(
            intDecoder_.get(), drVisitor);
      }
    } else {
      VELOX_UNREACHABLE(
          "SelectiveIntegerDirectColumnReader::readWithVisitor get int128_t");
    }
  }
}

} // namespace facebook::velox::dwrf
