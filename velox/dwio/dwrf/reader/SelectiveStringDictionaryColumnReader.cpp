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
#include "velox/dwio/common/BufferUtil.h"
#include "velox/dwio/dwrf/common/DecoderUtil.h"

namespace facebook::velox::dwrf {

using namespace dwio::common;

SelectiveStringDictionaryColumnReader::SelectiveStringDictionaryColumnReader(
    const std::shared_ptr<const TypeWithId>& fileType,
    DwrfParams& params,
    common::ScanSpec& scanSpec)
    : SelectiveColumnReader(fileType->type(), fileType, params, scanSpec),
      lastStrideIndex_(-1),
      provider_(params.stripeStreams().getStrideIndexProvider()),
      statistics_(params.runtimeStatistics()) {
  auto& stripe = params.stripeStreams();
  EncodingKey encodingKey{fileType_->id(), params.flatMapContext().sequence};
  version_ = convertRleVersion(stripe.getEncoding(encodingKey).kind());
  scanState_.dictionary.numValues =
      stripe.getEncoding(encodingKey).dictionarysize();

  const auto dataId = encodingKey.forKind(proto::Stream_Kind_DATA);
  bool dictVInts = stripe.getUseVInts(dataId);
  dictIndex_ = createRleDecoder</*isSigned*/ false>(
      stripe.getStream(dataId, params.streamLabels().label(), true),
      version_,
      *memoryPool_,
      dictVInts,
      dwio::common::INT_BYTE_SIZE);

  const auto lenId = encodingKey.forKind(proto::Stream_Kind_LENGTH);
  bool lenVInts = stripe.getUseVInts(lenId);
  lengthDecoder_ = createRleDecoder</*isSigned*/ false>(
      stripe.getStream(lenId, params.streamLabels().label(), false),
      version_,
      *memoryPool_,
      lenVInts,
      dwio::common::INT_BYTE_SIZE);

  blobStream_ = stripe.getStream(
      encodingKey.forKind(proto::Stream_Kind_DICTIONARY_DATA),
      params.streamLabels().label(),
      false);

  // handle in dictionary stream
  std::unique_ptr<SeekableInputStream> inDictStream = stripe.getStream(
      encodingKey.forKind(proto::Stream_Kind_IN_DICTIONARY),
      params.streamLabels().label(),
      false);
  if (inDictStream) {
    inDictionaryReader_ =
        createBooleanRleDecoder(std::move(inDictStream), encodingKey);

    // stride dictionary only exists if in dictionary exists
    strideDictStream_ = stripe.getStream(
        encodingKey.forKind(proto::Stream_Kind_STRIDE_DICTIONARY),
        params.streamLabels().label(),
        true);
    VELOX_CHECK_NOT_NULL(strideDictStream_, "Stride dictionary is missing");

    const auto strideDictLenId =
        encodingKey.forKind(proto::Stream_Kind_STRIDE_DICTIONARY_LENGTH);
    bool strideLenVInt = stripe.getUseVInts(strideDictLenId);
    strideDictLengthDecoder_ = createRleDecoder</*isSigned*/ false>(
        stripe.getStream(strideDictLenId, params.streamLabels().label(), true),
        version_,
        *memoryPool_,
        strideLenVInt,
        dwio::common::INT_BYTE_SIZE);
  }
  scanState_.updateRawState();
}

uint64_t SelectiveStringDictionaryColumnReader::skip(uint64_t numValues) {
  numValues = SelectiveColumnReader::skip(numValues);
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
  dwio::common::ensureCapacity<StringView>(
      values.values, values.numValues, memoryPool_);
  // The lengths are read in the low addresses of the string views array.
  auto* lengths = values.values->asMutable<int32_t>();
  lengthDecoder.nextLengths(lengths, values.numValues);
  int64_t stringsBytes = 0;
  for (auto i = 0; i < values.numValues; ++i) {
    stringsBytes += lengths[i];
  }
  // read bytes from underlying string
  values.strings = AlignedBuffer::allocate<char>(stringsBytes, memoryPool_);
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
    offset -= lengths[i];
    views[i] = StringView(strings + offset, lengths[i]);
  }
}

void SelectiveStringDictionaryColumnReader::loadStrideDictionary() {
  auto nextStride = provider_.getStrideIndex();
  if (nextStride == lastStrideIndex_) {
    return;
  }

  // get stride dictionary size and load it if needed
  auto& positions =
      formatData_->as<DwrfData>().index().entry(nextStride).positions();
  scanState_.dictionary2.numValues = positions.Get(strideDictSizeOffset_);
  if (scanState_.dictionary2.numValues > 0) {
    // seek stride dictionary related streams
    std::vector<uint64_t> pos(
        positions.begin() + positionOffset_, positions.end());
    PositionProvider pp(pos);
    strideDictStream_->seekToPosition(pp);
    strideDictLengthDecoder_->seekToRowGroup(pp);

    loadDictionary(
        *strideDictStream_, *strideDictLengthDecoder_, scanState_.dictionary2);
  }
  lastStrideIndex_ = nextStride;
  dictionaryValues_ = nullptr;

  if (DictionaryValues::hasFilter(scanSpec_->filter())) {
    scanState_.filterCache.resize(
        scanState_.dictionary.numValues + scanState_.dictionary2.numValues);
    simd::memset(
        scanState_.filterCache.data() + scanState_.dictionary.numValues,
        FilterResult::kUnknown,
        scanState_.dictionary2.numValues);
  }
  scanState_.updateRawState();
}

void SelectiveStringDictionaryColumnReader::makeDictionaryBaseVector() {
  if (scanState_.dictionary2.numValues) {
    BufferPtr values = AlignedBuffer::allocate<StringView>(
        scanState_.dictionary.numValues + scanState_.dictionary2.numValues,
        memoryPool_);
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
        memoryPool_,
        fileType_->type(),
        BufferPtr(nullptr), // TODO nulls
        scanState_.dictionary.numValues +
            scanState_.dictionary2.numValues, // length
        values,
        std::vector<BufferPtr>{
            scanState_.dictionary.strings, scanState_.dictionary2.strings});
  } else {
    dictionaryValues_ = std::make_shared<FlatVector<StringView>>(
        memoryPool_,
        fileType_->type(),
        BufferPtr(nullptr), // TODO nulls
        scanState_.dictionary.numValues /*length*/,
        scanState_.dictionary.values,
        std::vector<BufferPtr>{scanState_.dictionary.strings});
  }
}

void SelectiveStringDictionaryColumnReader::read(
    vector_size_t offset,
    const RowSet& rows,
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
    dwio::common::ensureCapacity<uint64_t>(
        scanState_.inDictionary, bits::nwords(numFlags), memoryPool_);
    // The in dict buffer may have changed. If no change in
    // dictionary, the raw state will not be updated elsewhere.
    scanState_.rawState.inDictionary = scanState_.inDictionary->as<uint64_t>();

    inDictionaryReader_->next(
        scanState_.inDictionary->asMutable<char>(),
        numFlags,
        isBulk ? nullptr : nullsPtr);
    loadStrideDictionary();
  }

  dwio::common::StringColumnReadWithVisitorHelper<true, true>(
      *this, rows)([&](auto visitor) {
    readWithVisitor(visitor.toStringDictionaryColumnVisitor());
  });

  readOffset_ += rows.back() + 1;
  numRowsScanned_ = readOffset_ - offset;
}

void SelectiveStringDictionaryColumnReader::makeFlat(VectorPtr* result) {
  auto* indices = reinterpret_cast<const vector_size_t*>(rawValues_);
  auto values = AlignedBuffer::allocate<StringView>(numValues_, memoryPool_);
  auto* stringViews = values->asMutable<StringView>();
  std::vector<BufferPtr> stringBuffers;
  auto* stripeDict = scanState_.dictionary.values->as<StringView>();
  stringBuffers.push_back(scanState_.dictionary.strings);
  const StringView* strideDict = nullptr;
  if (scanState_.dictionary2.numValues > 0) {
    strideDict = scanState_.dictionary2.values->as<StringView>();
    stringBuffers.push_back(scanState_.dictionary2.strings);
  }
  auto nulls = resultNulls();
  auto* rawNulls = nulls ? nulls->as<uint64_t>() : nullptr;
  for (vector_size_t i = 0; i < numValues_; ++i) {
    if (rawNulls && bits::isBitNull(rawNulls, i)) {
      stringViews[i] = {};
      continue;
    }
    auto j = indices[i];
    if (j < scanState_.dictionary.numValues) {
      stringViews[i] = stripeDict[j];
    } else {
      stringViews[i] = strideDict[j - scanState_.dictionary.numValues];
    }
  }
  *result = std::make_shared<FlatVector<StringView>>(
      memoryPool_,
      requestedType(),
      std::move(nulls),
      numValues_,
      std::move(values),
      std::move(stringBuffers));
  statistics_.flattenStringDictionaryValues += numValues_;
}

void SelectiveStringDictionaryColumnReader::getValues(
    const RowSet& rows,
    VectorPtr* result) {
  compactScalarValues<int32_t, int32_t>(rows, false);
  VELOX_CHECK_GT(numRowsScanned_, 0);
  double selectivity = 1.0 * rows.size() / numRowsScanned_;
  auto& dwrfData = formatData_->as<DwrfData>();
  auto flatSize = selectivity *
      (scanState_.dictionary2.numValues > 0 ? dwrfData.rowsPerRowGroup().value()
                                            : dwrfData.stripeRows());
  flatSize = std::max<double>(flatSize, rows.size());
  auto dictSize =
      scanState_.dictionary.numValues + scanState_.dictionary2.numValues;
  if (scanSpec_->makeFlat() || (!dictionaryValues_ && flatSize < dictSize)) {
    makeFlat(result);
    return;
  }
  if (!dictionaryValues_) {
    makeDictionaryBaseVector();
  }
  *result = std::make_shared<DictionaryVector<StringView>>(
      memoryPool_, resultNulls(), numValues_, dictionaryValues_, values_);
}

void SelectiveStringDictionaryColumnReader::ensureInitialized() {
  if (LIKELY(initialized_)) {
    return;
  }

  ClockTimer timer{initTimeClocks_};

  loadDictionary(*blobStream_, *lengthDecoder_, scanState_.dictionary);

  if (DictionaryValues::hasFilter(scanSpec_->filter())) {
    scanState_.filterCache.resize(scanState_.dictionary.numValues);
    simd::memset(
        scanState_.filterCache.data(),
        FilterResult::kUnknown,
        scanState_.dictionary.numValues);
  }

  // handle in dictionary stream
  if (inDictionaryReader_) {
    auto& dwrfData = formatData_->as<DwrfData>();
    dwrfData.ensureRowGroupIndex();
    // load stride dictionary offsets
    auto indexStartOffset = dwrfData.flatMapContext().inMapDecoder
        ? dwrfData.flatMapContext().inMapDecoder->loadIndices(0)
        : 0;
    positionOffset_ = dwrfData.notNullDecoder()
        ? dwrfData.notNullDecoder()->loadIndices(indexStartOffset)
        : indexStartOffset;
    size_t offset = strideDictStream_->positionSize() + positionOffset_;
    strideDictSizeOffset_ = strideDictLengthDecoder_->loadIndices(offset);
  }
  scanState_.updateRawState();
  initialized_ = true;
}

} // namespace facebook::velox::dwrf
