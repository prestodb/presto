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
  scanState_.dictionary.numValues =
      stripe.getEncoding(encodingKey).dictionarysize();

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

void SelectiveStringDictionaryColumnReader::loadDictionary(
    SeekableInputStream& data,
    IntDecoder</*isSigned*/ false>& lengthDecoder,
    DictionaryValues& values) {
  // read lengths from length reader
  detail::ensureCapacity<StringView>(
      values.values, values.numValues, &memoryPool_);
  // The lengths are read in the low addresses of the string views array.
  int64_t* int64Values = values.values->asMutable<int64_t>();
  lengthDecoder.next(int64Values, values.numValues, nullptr);
  int64_t stringsBytes = 0;
  for (auto i = 0; i < values.numValues; ++i) {
    stringsBytes += int64Values[i];
  }
  // read bytes from underlying string
  values.strings = AlignedBuffer::allocate<char>(stringsBytes, &memoryPool_);
  data.readFully(values.strings->asMutable<char>(), stringsBytes);
  // fill the values with StringViews over the strings. 'strings' will
  // exist even if 'stringsBytes' is 0, which can happen if the only
  // content of the dictionary is the empty string.
  auto views = values.values->asMutable<StringView>();
  auto strings = values.strings->as<char>();
  // Write the StringViews from end to begin so as not to overwrite
  // the lengths at the start of values.
  auto offset = stringsBytes;
  for (int32_t i = values.numValues - 1; i >= 0; --i) {
    offset -= int64Values[i];
    views[i] = StringView(strings + offset, int64Values[i]);
  }
}

void SelectiveStringDictionaryColumnReader::loadStrideDictionary() {
  auto nextStride = provider_.getStrideIndex();
  if (nextStride == lastStrideIndex_) {
    return;
  }

  // get stride dictionary size and load it if needed
  auto& positions = index_->entry(nextStride).positions();
  scanState_.dictionary2.numValues = positions.Get(strideDictSizeOffset_);
  if (scanState_.dictionary2.numValues > 0) {
    // seek stride dictionary related streams
    std::vector<uint64_t> pos(
        positions.begin() + positionOffset_, positions.end());
    PositionProvider pp(pos);
    strideDictStream_->seekToRowGroup(pp);
    strideDictLengthDecoder_->seekToRowGroup(pp);

    loadDictionary(
        *strideDictStream_, *strideDictLengthDecoder_, scanState_.dictionary2);
    scanState_.updateRawState();
  }
  lastStrideIndex_ = nextStride;
  dictionaryValues_ = nullptr;

  scanState_.filterCache.resize(
      scanState_.dictionary.numValues + scanState_.dictionary2.numValues);
  simd::memset(
      scanState_.filterCache.data() + scanState_.dictionary.numValues,
      FilterResult::kUnknown,
      scanState_.dictionary2.numValues);
}

void SelectiveStringDictionaryColumnReader::makeDictionaryBaseVector() {
  if (scanState_.dictionary2.numValues) {
    BufferPtr values = AlignedBuffer::allocate<StringView>(
        scanState_.dictionary.numValues + scanState_.dictionary2.numValues,
        &memoryPool_);
    auto* valuesPtr = values->asMutable<StringView>();
    memcpy(
        valuesPtr,
        scanState_.dictionary.values->as<char>(),
        scanState_.dictionary.numValues * sizeof(StringView));
    memcpy(
        valuesPtr + scanState_.dictionary.numValues,
        scanState_.dictionary2.values->as<char>(),
        scanState_.dictionary2.numValues * sizeof(StringView));

    dictionaryValues_ = std::make_shared<FlatVector<StringView>>(
        &memoryPool_,
        type_,
        BufferPtr(nullptr), // TODO nulls
        scanState_.dictionary.numValues +
            scanState_.dictionary2.numValues, // length
        values,
        std::vector<BufferPtr>{
            scanState_.dictionary.strings, scanState_.dictionary2.strings});
  } else {
    dictionaryValues_ = std::make_shared<FlatVector<StringView>>(
        &memoryPool_,
        type_,
        BufferPtr(nullptr), // TODO nulls
        scanState_.dictionary.numValues /*length*/,
        scanState_.dictionary.values,
        std::vector<BufferPtr>{scanState_.dictionary.strings});
  }
}

void SelectiveStringDictionaryColumnReader::read(
    vector_size_t offset,
    RowSet rows,
    const uint64_t* incomingNulls) {
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
    detail::ensureCapacity<uint64_t>(
        scanState_.inDictionary, bits::nwords(numFlags), &memoryPool_);
    // The in dict buffer may have changed. If no change in
    // dictionary, the raw state will not be updated elsewhere.
    scanState_.rawState.inDictionary = scanState_.inDictionary->as<uint64_t>();

    inDictionaryReader_->next(
        scanState_.inDictionary->asMutable<char>(),
        numFlags,
        isBulk ? nullptr : nullsPtr);
    loadStrideDictionary();
  }

  if (scanSpec_->keepValues()) {
    if (scanSpec_->valueHook()) {
      if (isDense) {
        readHelper<common::AlwaysTrue, true>(
            &alwaysTrue(),
            rows,
            ExtractStringDictionaryToGenericHook(
                scanSpec_->valueHook(), rows, scanState_.rawState));
      } else {
        readHelper<common::AlwaysTrue, false>(
            &alwaysTrue(),
            rows,
            ExtractStringDictionaryToGenericHook(
                scanSpec_->valueHook(), rows, scanState_.rawState));
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

  loadDictionary(*blobStream_, *lengthDecoder_, scanState_.dictionary);

  scanState_.filterCache.resize(scanState_.dictionary.numValues);
  simd::memset(
      scanState_.filterCache.data(),
      FilterResult::kUnknown,
      scanState_.dictionary.numValues);

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
  scanState_.updateRawState();
  initialized_ = true;
  initTimeClocks_ = timer.elapsedClocks();
}

} // namespace facebook::velox::dwrf
