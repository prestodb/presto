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

#include "velox/dwio/dwrf/reader/DwrfData.h"

namespace facebook::velox::dwrf {

DwrfData::DwrfData(
    std::shared_ptr<const dwio::common::TypeWithId> nodeType,
    StripeStreams& stripe,
    FlatMapContext flatMapContext)
    : memoryPool_(stripe.getMemoryPool()),
      nodeType_(std::move(nodeType)),
      flatMapContext_(std::move(flatMapContext)),
      rowsPerRowGroup_{stripe.rowsPerRowGroup()} {
  EncodingKey encodingKey{nodeType_->id, flatMapContext_.sequence};
  std::unique_ptr<dwio::common::SeekableInputStream> stream =
      stripe.getStream(encodingKey.forKind(proto::Stream_Kind_PRESENT), false);
  if (stream) {
    notNullDecoder_ = createBooleanRleDecoder(std::move(stream), encodingKey);
  }

  // We always initialize indexStream_ because indices are needed as
  // soon as there is a single filter that can trigger row group skips
  // anywhere in the reader tree. This is not known at construct time
  // because the first filter can come from a hash join or other run
  // time pushdown.
  indexStream_ = stripe.getStream(
      encodingKey.forKind(proto::Stream_Kind_ROW_INDEX), false);
}

uint64_t DwrfData::skipNulls(uint64_t numValues, bool /*nullsOnly*/) {
  if (notNullDecoder_) {
    // page through the values that we want to skip
    // and count how many are non-null
    constexpr uint64_t kBufferBytes = 1024;
    std::array<char, kBufferBytes> buffer;
    constexpr auto kBitCount = kBufferBytes * 8;
    uint64_t remaining = numValues;
    while (remaining > 0) {
      uint64_t chunkSize = std::min(remaining, kBitCount);
      notNullDecoder_->next(buffer.data(), chunkSize, nullptr);
      remaining -= chunkSize;
      numValues -= bits::countNulls(
          reinterpret_cast<uint64_t*>(buffer.data()), 0, chunkSize);
    }
  }
  return numValues;
}

void DwrfData::ensureRowGroupIndex() {
  VELOX_CHECK(index_ || indexStream_, "Reader needs to have an index stream");
  if (indexStream_) {
    index_ = ProtoUtils::readProto<proto::RowIndex>(std::move(indexStream_));
  }
}

dwio::common::PositionProvider DwrfData::seekToRowGroup(uint32_t index) {
  ensureRowGroupIndex();
  tempPositions_ = toPositionsInner(index_->entry(index));
  dwio::common::PositionProvider positionProvider(tempPositions_);
  if (flatMapContext_.inMapDecoder) {
    flatMapContext_.inMapDecoder->seekToRowGroup(positionProvider);
  }

  if (notNullDecoder_) {
    notNullDecoder_->seekToRowGroup(positionProvider);
  }

  return positionProvider;
}

void DwrfData::readNulls(
    vector_size_t numValues,
    const uint64_t* FOLLY_NULLABLE incomingNulls,
    BufferPtr& nulls,
    bool /*nullsOnly*/) {
  if (!notNullDecoder_ && !incomingNulls) {
    nulls = nullptr;
    return;
  }
  auto numBytes = bits::nbytes(numValues);
  if (!nulls || nulls->capacity() < numBytes + simd::kPadding) {
    nulls =
        AlignedBuffer::allocate<char>(numBytes + simd::kPadding, &memoryPool_);
  }
  nulls->setSize(numBytes);
  auto* nullsPtr = nulls->asMutable<uint64_t>();
  if (!notNullDecoder_) {
    memcpy(nullsPtr, incomingNulls, numBytes);
    return;
  }
  memset(nullsPtr, bits::kNotNullByte, numBytes);
  notNullDecoder_->next(
      reinterpret_cast<char*>(nullsPtr), numValues, incomingNulls);
}

std::vector<uint32_t> DwrfData::filterRowGroups(
    const common::ScanSpec& scanSpec,
    uint64_t rowGroupSize,
    const dwio::common::StatsContext& context) {
  if ((!index_ && !indexStream_) || !scanSpec.filter()) {
    return {};
  }

  ensureRowGroupIndex();
  auto filter = scanSpec.filter();

  std::vector<uint32_t> stridesToSkip;
  auto dwrfContext = reinterpret_cast<const StatsContext*>(&context);
  for (auto i = 0; i < index_->entry_size(); i++) {
    const auto& entry = index_->entry(i);
    auto columnStats =
        buildColumnStatisticsFromProto(entry.statistics(), *dwrfContext);
    if (!testFilter(filter, columnStats.get(), rowGroupSize, nodeType_->type)) {
      VLOG(1) << "Drop stride " << i << " on " << scanSpec.toString();
      stridesToSkip.push_back(i); // Skipping stride based on column stats.
    }
  }
  return stridesToSkip;
}

} // namespace facebook::velox::dwrf
