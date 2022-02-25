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

#include "velox/dwio/dwrf/reader/SelectiveStringDictionaryColumnReader.h"

namespace facebook::velox::dwrf {

using namespace dwio::common;

SelectiveStringDictionaryColumnReader::SelectiveStringDictionaryColumnReader(
    const std::shared_ptr<const TypeWithId>& nodeType,
    StripeStreams& stripe,
    common::ScanSpec* scanSpec,
    FlatMapContext flatMapContext)
    : SelectiveColumnReader(
          nodeType,
          stripe,
          scanSpec,
          nodeType->type,
          std::move(flatMapContext)),
      lastStrideIndex_(-1),
      provider_(stripe.getStrideIndexProvider()) {
  EncodingKey encodingKey{nodeType_->id, flatMapContext_.sequence};
  RleVersion rleVersion =
      convertRleVersion(stripe.getEncoding(encodingKey).kind());
  dictionaryCount_ = stripe.getEncoding(encodingKey).dictionarysize();

  const auto dataId = encodingKey.forKind(proto::Stream_Kind_DATA);
  bool dictVInts = stripe.getUseVInts(dataId);
  dictIndex_ = IntDecoder</*isSigned*/ false>::createRle(
      stripe.getStream(dataId, true),
      rleVersion,
      memoryPool_,
      dictVInts,
      INT_BYTE_SIZE);

  const auto lenId = encodingKey.forKind(proto::Stream_Kind_LENGTH);
  bool lenVInts = stripe.getUseVInts(lenId);
  lengthDecoder_ = IntDecoder</*isSigned*/ false>::createRle(
      stripe.getStream(lenId, false),
      rleVersion,
      memoryPool_,
      lenVInts,
      INT_BYTE_SIZE);

  blobStream_ = stripe.getStream(
      encodingKey.forKind(proto::Stream_Kind_DICTIONARY_DATA), false);

  // handle in dictionary stream
  std::unique_ptr<SeekableInputStream> inDictStream = stripe.getStream(
      encodingKey.forKind(proto::Stream_Kind_IN_DICTIONARY), false);
  if (inDictStream) {
    DWIO_ENSURE_NOT_NULL(indexStream_, "String index is missing");

    inDictionaryReader_ =
        createBooleanRleDecoder(std::move(inDictStream), encodingKey);

    // stride dictionary only exists if in dictionary exists
    strideDictStream_ = stripe.getStream(
        encodingKey.forKind(proto::Stream_Kind_STRIDE_DICTIONARY), true);
    DWIO_ENSURE_NOT_NULL(strideDictStream_, "Stride dictionary is missing");

    const auto strideDictLenId =
        encodingKey.forKind(proto::Stream_Kind_STRIDE_DICTIONARY_LENGTH);
    bool strideLenVInt = stripe.getUseVInts(strideDictLenId);
    strideDictLengthDecoder_ = IntDecoder</*isSigned*/ false>::createRle(
        stripe.getStream(strideDictLenId, true),
        rleVersion,
        memoryPool_,
        strideLenVInt,
        INT_BYTE_SIZE);
  }
}

uint64_t SelectiveStringDictionaryColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  dictIndex_->skip(numValues);
  if (inDictionaryReader_) {
    inDictionaryReader_->skip(numValues);
  }
  return numValues;
}

BufferPtr SelectiveStringDictionaryColumnReader::loadDictionary(
    uint64_t count,
    SeekableInputStream& data,
    IntDecoder</*isSigned*/ false>& lengthDecoder,
    BufferPtr& offsets) {
  // read lengths from length reader
  auto* offsetsPtr = offsets->asMutable<int64_t>();
  offsetsPtr[0] = 0;
  lengthDecoder.next(offsetsPtr + 1, count, nullptr);

  // set up array that keeps offset of start positions of individual entries
  // in the dictionary
  for (uint64_t i = 1; i < count + 1; ++i) {
    offsetsPtr[i] += offsetsPtr[i - 1];
  }

  // read bytes from underlying string
  int64_t blobSize = offsetsPtr[count];
  BufferPtr dictionary = AlignedBuffer::allocate<char>(blobSize, &memoryPool_);
  data.readFully(dictionary->asMutable<char>(), blobSize);
  return dictionary;
}

void SelectiveStringDictionaryColumnReader::loadStrideDictionary() {
  auto nextStride = provider_.getStrideIndex();
  if (nextStride == lastStrideIndex_) {
    return;
  }

  // get stride dictionary size and load it if needed
  auto& positions = index_->entry(nextStride).positions();
  strideDictCount_ = positions.Get(strideDictSizeOffset_);
  if (strideDictCount_ > 0) {
    // seek stride dictionary related streams
    std::vector<uint64_t> pos(
        positions.begin() + positionOffset_, positions.end());
    PositionProvider pp(pos);
    strideDictStream_->seekToRowGroup(pp);
    strideDictLengthDecoder_->seekToRowGroup(pp);

    ensureCapacity<int64_t>(
        strideDictOffset_, strideDictCount_ + 1, &memoryPool_);
    strideDict_ = loadDictionary(
        strideDictCount_,
        *strideDictStream_,
        *strideDictLengthDecoder_,
        strideDictOffset_);
  } else {
    strideDict_.reset();
  }

  lastStrideIndex_ = nextStride;

  dictionaryValues_.reset();
  filterCache_.resize(dictionaryCount_ + strideDictCount_);
  simd::memset(
      filterCache_.data(),
      FilterResult::kUnknown,
      dictionaryCount_ + strideDictCount_);
}

void SelectiveStringDictionaryColumnReader::makeDictionaryBaseVector() {
  const auto* dictionaryBlob_Ptr = dictionaryBlob_->as<char>();
  const auto* dictionaryOffset_sPtr = dictionaryOffset_->as<int64_t>();
  if (strideDictCount_) {
    // TODO Reuse memory
    BufferPtr values = AlignedBuffer::allocate<StringView>(
        dictionaryCount_ + strideDictCount_, &memoryPool_);
    auto* valuesPtr = values->asMutable<StringView>();
    for (size_t i = 0; i < dictionaryCount_; i++) {
      valuesPtr[i] = StringView(
          dictionaryBlob_Ptr + dictionaryOffset_sPtr[i],
          dictionaryOffset_sPtr[i + 1] - dictionaryOffset_sPtr[i]);
    }

    const auto* strideDictPtr = strideDict_->as<char>();
    const auto* strideDictOffset_Ptr = strideDictOffset_->as<int64_t>();
    for (size_t i = 0; i < strideDictCount_; i++) {
      valuesPtr[dictionaryCount_ + i] = StringView(
          strideDictPtr + strideDictOffset_Ptr[i],
          strideDictOffset_Ptr[i + 1] - strideDictOffset_Ptr[i]);
    }

    dictionaryValues_ = std::make_shared<FlatVector<StringView>>(
        &memoryPool_,
        type_,
        BufferPtr(nullptr), // TODO nulls
        dictionaryCount_ + strideDictCount_ /*length*/,
        values,
        std::vector<BufferPtr>{dictionaryBlob_, strideDict_});
  } else {
    // TODO Reuse memory
    BufferPtr values =
        AlignedBuffer::allocate<StringView>(dictionaryCount_, &memoryPool_);
    auto* valuesPtr = values->asMutable<StringView>();
    for (size_t i = 0; i < dictionaryCount_; i++) {
      valuesPtr[i] = StringView(
          dictionaryBlob_Ptr + dictionaryOffset_sPtr[i],
          dictionaryOffset_sPtr[i + 1] - dictionaryOffset_sPtr[i]);
    }

    dictionaryValues_ = std::make_shared<FlatVector<StringView>>(
        &memoryPool_,
        type_,
        BufferPtr(nullptr), // TODO nulls
        dictionaryCount_ /*length*/,
        values,
        std::vector<BufferPtr>{dictionaryBlob_});
  }
}

void SelectiveStringDictionaryColumnReader::read(
    vector_size_t offset,
    RowSet rows,
    const uint64_t* incomingNulls) {
  static std::array<char, 1> EMPTY_DICT;

  prepareRead<int32_t>(offset, rows, incomingNulls);
  bool isDense = rows.back() == rows.size() - 1;
  const auto* nullsPtr =
      nullsInReadRange_ ? nullsInReadRange_->as<uint64_t>() : nullptr;
  // lazy loading dictionary data when first hit
  ensureInitialized();

  if (inDictionaryReader_) {
    auto end = rows.back() + 1;
    bool isBulk = useBulkPath();
    int32_t numFlags = (isBulk && nullsInReadRange_)
        ? bits::countNonNulls(nullsInReadRange_->as<uint64_t>(), 0, end)
        : end;
    ensureCapacity<uint64_t>(inDict_, bits::nwords(numFlags), &memoryPool_);
    inDictionaryReader_->next(
        inDict_->asMutable<char>(), numFlags, isBulk ? nullptr : nullsPtr);
    loadStrideDictionary();
    if (strideDict_) {
      DWIO_ENSURE_NOT_NULL(strideDictOffset_);

      // It's possible strideDictBlob is nullptr when stride dictionary only
      // contains empty string. In that case, we need to make sure
      // strideDictBlob points to some valid address, and the last entry of
      // strideDictOffset_ have value 0.
      auto strideDictBlob = strideDict_->as<char>();
      if (!strideDictBlob) {
        strideDictBlob = EMPTY_DICT.data();
        DWIO_ENSURE_EQ(strideDictOffset_->as<int64_t>()[strideDictCount_], 0);
      }
    }
  }
  if (scanSpec_->keepValues()) {
    if (scanSpec_->valueHook()) {
      if (isDense) {
        readHelper<common::AlwaysTrue, true>(
            &alwaysTrue(),
            rows,
            ExtractStringDictionaryToGenericHook(
                scanSpec_->valueHook(),
                rows,
                (strideDict_ && inDict_) ? inDict_->as<uint64_t>() : nullptr,
                dictionaryBlob_->as<char>(),
                dictionaryOffset_->as<uint64_t>(),
                dictionaryCount_,
                strideDict_ ? strideDict_->as<char>() : nullptr,
                strideDictOffset_ ? strideDictOffset_->as<uint64_t>()
                                  : nullptr));
      } else {
        readHelper<common::AlwaysTrue, false>(
            &alwaysTrue(),
            rows,
            ExtractStringDictionaryToGenericHook(
                scanSpec_->valueHook(),
                rows,
                (strideDict_ && inDict_) ? inDict_->as<uint64_t>() : nullptr,
                dictionaryBlob_->as<char>(),
                dictionaryOffset_->as<uint64_t>(),
                dictionaryCount_,
                strideDict_ ? strideDict_->as<char>() : nullptr,
                strideDictOffset_ ? strideDictOffset_->as<uint64_t>()
                                  : nullptr));
      }
      return;
    }
    if (isDense) {
      processFilter<true>(scanSpec_->filter(), rows, ExtractToReader(this));
    } else {
      processFilter<false>(scanSpec_->filter(), rows, ExtractToReader(this));
    }
  } else {
    if (isDense) {
      processFilter<true>(scanSpec_->filter(), rows, DropValues());
    } else {
      processFilter<false>(scanSpec_->filter(), rows, DropValues());
    }
  }
}

void SelectiveStringDictionaryColumnReader::getValues(
    RowSet rows,
    VectorPtr* result) {
  if (!dictionaryValues_) {
    makeDictionaryBaseVector();
  }
  compactScalarValues<int32_t, int32_t>(rows, false);

  *result = std::make_shared<DictionaryVector<StringView>>(
      &memoryPool_,
      !anyNulls_               ? nullptr
          : returnReaderNulls_ ? nullsInReadRange_
                               : resultNulls_,
      numValues_,
      dictionaryValues_,
      TypeKind::INTEGER,
      values_);

  if (scanSpec_->makeFlat()) {
    BaseVector::ensureWritable(
        SelectivityVector::empty(), (*result)->type(), &memoryPool_, result);
  }
}

void SelectiveStringDictionaryColumnReader::ensureInitialized() {
  if (LIKELY(initialized_)) {
    return;
  }

  Timer timer;

  ensureCapacity<int64_t>(
      dictionaryOffset_, dictionaryCount_ + 1, &memoryPool_);
  dictionaryBlob_ = loadDictionary(
      dictionaryCount_, *blobStream_, *lengthDecoder_, dictionaryOffset_);
  dictionaryValues_.reset();
  filterCache_.resize(dictionaryCount_);
  simd::memset(filterCache_.data(), FilterResult::kUnknown, dictionaryCount_);

  // handle in dictionary stream
  if (inDictionaryReader_) {
    ensureRowGroupIndex();
    // load stride dictionary offsets
    auto indexStartOffset = flatMapContext_.inMapDecoder
        ? flatMapContext_.inMapDecoder->loadIndices(*index_, 0)
        : 0;
    positionOffset_ = notNullDecoder_
        ? notNullDecoder_->loadIndices(*index_, indexStartOffset)
        : indexStartOffset;
    size_t offset = strideDictStream_->loadIndices(*index_, positionOffset_);
    strideDictSizeOffset_ =
        strideDictLengthDecoder_->loadIndices(*index_, offset);
  }
  initialized_ = true;
  initTimeClocks_ = timer.elapsedClocks();
}

} // namespace facebook::velox::dwrf
