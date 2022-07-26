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

#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/ColumnSelector.h"
#include "velox/dwio/common/FormatData.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/dwio/dwrf/common/ByteRLE.h"
#include "velox/dwio/dwrf/common/Compression.h"
#include "velox/dwio/dwrf/common/RLEv1.h"
#include "velox/dwio/dwrf/common/wrap/dwrf-proto-wrapper.h"
#include "velox/dwio/dwrf/reader/EncodingContext.h"
#include "velox/dwio/dwrf/reader/StripeStream.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::dwrf {

// DWRF specific functions shared between all readers.
class DwrfData : public dwio::common::FormatData {
 public:
  DwrfData(
      std::shared_ptr<const dwio::common::TypeWithId> nodeType,
      StripeStreams& stripe,
      FlatMapContext flatMapContext);

  void readNulls(
      vector_size_t numValues,
      const uint64_t* FOLLY_NULLABLE incomingNulls,
      BufferPtr& nulls) override;

  uint64_t skipNulls(uint64_t numValues) override;

  uint64_t skip(uint64_t numValues) override {
    return skipNulls(numValues);
  }

  std::vector<uint32_t> filterRowGroups(
      const common::ScanSpec& scanSpec,
      uint64_t rowsPerRowGroup,
      const dwio::common::StatsContext& context) override;

  bool hasNulls() const override {
    return notNullDecoder_ != nullptr;
  }

  auto* FOLLY_NULLABLE notNullDecoder() const {
    return notNullDecoder_.get();
  }

  const FlatMapContext& flatMapContext() {
    return flatMapContext_;
  }

  // seeks possible flat map in map streams and nulls to the row group
  // and returns a PositionsProvider for the other streams.
  dwio::common::PositionProvider seekToRowGroup(uint32_t index) override;

  uint32_t rowsPerRowGroup() const {
    return rowsPerRowGroup_;
  }

  // Decodes the entry from row group index for 'this' in the stripe,
  // if not already decoded. Throws if no index.
  void ensureRowGroupIndex();

  auto& index() const {
    return *index_;
  }

 private:
  static std::vector<uint64_t> toPositionsInner(
      const proto::RowIndexEntry& entry) {
    return std::vector<uint64_t>(
        entry.positions().begin(), entry.positions().end());
  }

  memory::MemoryPool& memoryPool_;
  const std::shared_ptr<const dwio::common::TypeWithId> nodeType_;
  FlatMapContext flatMapContext_;
  std::unique_ptr<ByteRleDecoder> notNullDecoder_;
  std::unique_ptr<dwio::common::SeekableInputStream> indexStream_;
  std::unique_ptr<proto::RowIndex> index_;
  // Number of rows in a row group. Last row group may have fewer rows.
  uint32_t rowsPerRowGroup_;

  // Storage for positions backing a PositionProvider returned from
  // seekToRowGroup().
  std::vector<uint64_t> tempPositions_;
};

// DWRF specific initialization.
class DwrfParams : public dwio::common::FormatParams {
 public:
  DwrfParams(
      StripeStreams& stripeStreams,
      FlatMapContext context = FlatMapContext::nonFlatMapContext())
      : FormatParams(stripeStreams.getMemoryPool()),
        stripeStreams_(stripeStreams),
        flatMapContext_(context) {}

  std::unique_ptr<dwio::common::FormatData> toFormatData(
      const std::shared_ptr<const dwio::common::TypeWithId>& type,
      const common::ScanSpec& /*scanSpec*/) override {
    return std::make_unique<DwrfData>(type, stripeStreams_, flatMapContext_);
  }

  StripeStreams& stripeStreams() {
    return stripeStreams_;
  }

  FlatMapContext flatMapContext() const {
    return flatMapContext_;
  }

 private:
  StripeStreams& stripeStreams_;
  FlatMapContext flatMapContext_;
};

inline RleVersion convertRleVersion(proto::ColumnEncoding_Kind kind) {
  switch (static_cast<int64_t>(kind)) {
    case proto::ColumnEncoding_Kind_DIRECT:
    case proto::ColumnEncoding_Kind_DICTIONARY:
      return RleVersion_1;
    default:
      DWIO_RAISE("Unknown encoding in convertRleVersion");
  }
}

} // namespace facebook::velox::dwrf
