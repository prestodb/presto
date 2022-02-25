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

#include "velox/dwio/dwrf/reader/SelectiveColumnReaderInternal.h"

namespace facebook::velox::dwrf {

class SelectiveStringDictionaryColumnReader : public SelectiveColumnReader {
 public:
  using ValueType = int32_t;

  SelectiveStringDictionaryColumnReader(
      const std::shared_ptr<const dwio::common::TypeWithId>& nodeType,
      StripeStreams& stripe,
      common::ScanSpec* scanSpec,
      FlatMapContext flatMapContext);

  void resetFilterCaches() override {
    // 'filterCache_' could be empty before first read.
    if (!filterCache_.empty()) {
      simd::memset(
          filterCache_.data(), FilterResult::kUnknown, dictionaryCount_);
    }
  }

  void seekToRowGroup(uint32_t index) override {
    ensureRowGroupIndex();

    auto positions = toPositions(index_->entry(index));
    PositionProvider positionsProvider(positions);

    if (flatMapContext_.inMapDecoder) {
      flatMapContext_.inMapDecoder->seekToRowGroup(positionsProvider);
    }

    if (notNullDecoder_) {
      notNullDecoder_->seekToRowGroup(positionsProvider);
    }

    if (strideDictStream_) {
      strideDictStream_->seekToRowGroup(positionsProvider);
      strideDictLengthDecoder_->seekToRowGroup(positionsProvider);
      // skip row group dictionary size
      positionsProvider.next();
    }

    dictIndex_->seekToRowGroup(positionsProvider);

    if (inDictionaryReader_) {
      inDictionaryReader_->seekToRowGroup(positionsProvider);
    }

    VELOX_CHECK(!positionsProvider.hasNext());
  }

  uint64_t skip(uint64_t numValues) override;

  void read(vector_size_t offset, RowSet rows, const uint64_t* incomingNulls)
      override;

  void getValues(RowSet rows, VectorPtr* result) override;

 private:
  void loadStrideDictionary();
  void makeDictionaryBaseVector();

  template <typename TVisitor>
  void readWithVisitor(RowSet rows, TVisitor visitor);

  template <typename TFilter, bool isDense, typename ExtractValues>
  void readHelper(common::Filter* filter, RowSet rows, ExtractValues values);

  template <bool isDense, typename ExtractValues>
  void processFilter(
      common::Filter* filter,
      RowSet rows,
      ExtractValues extractValues);

  BufferPtr loadDictionary(
      uint64_t count,
      SeekableInputStream& data,
      IntDecoder</*isSigned*/ false>& lengthDecoder,
      BufferPtr& offsets);

  void ensureInitialized();

  BufferPtr dictionaryBlob_;
  BufferPtr dictionaryOffset_;
  BufferPtr inDict_;
  BufferPtr strideDict_;
  BufferPtr strideDictOffset_;
  std::unique_ptr<IntDecoder</*isSigned*/ false>> dictIndex_;
  std::unique_ptr<ByteRleDecoder> inDictionaryReader_;
  std::unique_ptr<SeekableInputStream> strideDictStream_;
  std::unique_ptr<IntDecoder</*isSigned*/ false>> strideDictLengthDecoder_;

  FlatVectorPtr<StringView> dictionaryValues_;

  uint64_t dictionaryCount_;
  uint64_t strideDictCount_{0};
  int64_t lastStrideIndex_;
  size_t positionOffset_;
  size_t strideDictSizeOffset_;

  const StrideIndexProvider& provider_;

  // lazy load the dictionary
  std::unique_ptr<IntDecoder</*isSigned*/ false>> lengthDecoder_;
  std::unique_ptr<SeekableInputStream> blobStream_;
  raw_vector<uint8_t> filterCache_;
  bool initialized_{false};
};

template <typename TVisitor>
void SelectiveStringDictionaryColumnReader::readWithVisitor(
    RowSet rows,
    TVisitor visitor) {
  vector_size_t numRows = rows.back() + 1;
  auto decoder = dynamic_cast<RleDecoderV1<false>*>(dictIndex_.get());
  VELOX_CHECK(decoder, "Only RLEv1 is supported");
  if (nullsInReadRange_) {
    decoder->readWithVisitor<true, TVisitor>(
        nullsInReadRange_->as<uint64_t>(), visitor);
  } else {
    decoder->readWithVisitor<false, TVisitor>(nullptr, visitor);
  }
  readOffset_ += numRows;
}

template <typename TFilter, bool isDense, typename ExtractValues>
void SelectiveStringDictionaryColumnReader::readHelper(
    common::Filter* filter,
    RowSet rows,
    ExtractValues values) {
  readWithVisitor(
      rows,
      StringDictionaryColumnVisitor<TFilter, ExtractValues, isDense>(
          *reinterpret_cast<TFilter*>(filter),
          this,
          rows,
          values,
          (strideDict_ && inDict_) ? inDict_->as<uint64_t>() : nullptr,
          filterCache_.empty() ? nullptr : filterCache_.data(),
          dictionaryBlob_->as<char>(),
          dictionaryOffset_->as<uint64_t>(),
          dictionaryCount_,
          strideDict_ ? strideDict_->as<char>() : nullptr,
          strideDictOffset_ ? strideDictOffset_->as<uint64_t>() : nullptr));
}

template <bool isDense, typename ExtractValues>
void SelectiveStringDictionaryColumnReader::processFilter(
    common::Filter* filter,
    RowSet rows,
    ExtractValues extractValues) {
  switch (filter ? filter->kind() : common::FilterKind::kAlwaysTrue) {
    case common::FilterKind::kAlwaysTrue:
      readHelper<common::AlwaysTrue, isDense>(filter, rows, extractValues);
      break;
    case common::FilterKind::kIsNull:
      filterNulls<int32_t>(
          rows,
          true,
          !std::is_same<decltype(extractValues), DropValues>::value);
      break;
    case common::FilterKind::kIsNotNull:
      if (std::is_same<decltype(extractValues), DropValues>::value) {
        filterNulls<int32_t>(rows, false, false);
      } else {
        readHelper<common::IsNotNull, isDense>(filter, rows, extractValues);
      }
      break;
    case common::FilterKind::kBytesRange:
      readHelper<common::BytesRange, isDense>(filter, rows, extractValues);
      break;
    case common::FilterKind::kBytesValues:
      readHelper<common::BytesValues, isDense>(filter, rows, extractValues);
      break;
    default:
      readHelper<common::Filter, isDense>(filter, rows, extractValues);
      break;
  }
}

} // namespace facebook::velox::dwrf
