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
    EncodingKey encodingKey{fileType_->id(), params.flatMapContext().sequence};
    auto data = encodingKey.forKind(proto::Stream_Kind_DATA);
    auto& stripe = params.stripeStreams();
    bool dataVInts = stripe.getUseVInts(data);

    format = stripe.format();
    if (format == velox::dwrf::DwrfFormat::kDwrf) {
      ints = createDirectDecoder</*isSigned*/ true>(
          stripe.getStream(data, params.streamLabels().label(), true),
          dataVInts,
          numBytes);
    } else if (format == velox::dwrf::DwrfFormat::kOrc) {
      version = convertRleVersion(stripe.getEncoding(encodingKey).kind());
      ints = createRleDecoder</*isSigned*/ true>(
          stripe.getStream(data, params.streamLabels().label(), true),
          version,
          params.pool(),
          dataVInts,
          numBytes);
    } else {
      VELOX_FAIL("invalid stripe format");
    }
  }

  bool hasBulkPath() const override {
    return true;
  }

  void seekToRowGroup(uint32_t index) override {
    dwio::common::SelectiveIntegerColumnReader::seekToRowGroup(index);
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
  dwrf::DwrfFormat format;
  RleVersion version;
  std::unique_ptr<dwio::common::IntDecoder<true>> ints;
};

template <typename ColumnVisitor>
void SelectiveIntegerDirectColumnReader::readWithVisitor(
    RowSet rows,
    ColumnVisitor visitor) {
  if (format == velox::dwrf::DwrfFormat::kDwrf) {
    decodeWithVisitor<dwio::common::DirectDecoder<true>>(ints.get(), visitor);
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
      if (version == velox::dwrf::RleVersion_1) {
        decodeWithVisitor<velox::dwrf::RleDecoderV1<true>>(
            ints.get(), drVisitor);
      } else {
        VELOX_CHECK(version == velox::dwrf::RleVersion_2);
        decodeWithVisitor<velox::dwrf::RleDecoderV2<true>>(
            ints.get(), drVisitor);
      }
    } else {
      VELOX_UNREACHABLE(
          "SelectiveIntegerDirectColumnReader::readWithVisitor get int128_t");
    }
  }
}

} // namespace facebook::velox::dwrf
