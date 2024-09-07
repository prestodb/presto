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

#include "velox/dwio/dwrf/reader/SelectiveIntegerDictionaryColumnReader.h"
#include "velox/dwio/common/BufferUtil.h"
#include "velox/dwio/dwrf/common/DecoderUtil.h"

namespace facebook::velox::dwrf {
using namespace dwio::common;

SelectiveIntegerDictionaryColumnReader::SelectiveIntegerDictionaryColumnReader(
    const TypePtr& requestedType,
    std::shared_ptr<const TypeWithId> fileType,
    DwrfParams& params,
    common::ScanSpec& scanSpec,
    uint32_t numBytes)
    : SelectiveIntegerColumnReader(
          requestedType,
          params,
          scanSpec,
          std::move(fileType)) {
  const EncodingKey encodingKey{
      fileType_->id(), params.flatMapContext().sequence};
  auto& stripe = params.stripeStreams();
  const auto encoding = stripe.getEncoding(encodingKey);
  scanState_.dictionary.numValues = encoding.dictionarysize();
  rleVersion_ = convertRleVersion(encoding.kind());
  const auto si = encodingKey.forKind(proto::Stream_Kind_DATA);
  const bool dataVInts = stripe.getUseVInts(si);
  dataReader_ = createRleDecoder</*isSigned=*/false>(
      stripe.getStream(si, params.streamLabels().label(), true),
      rleVersion_,
      *memoryPool_,
      dataVInts,
      numBytes);

  // Makes a lazy dictionary initializer
  dictInit_ = stripe.getIntDictionaryInitializerForNode(
      encodingKey, numBytes, params.streamLabels(), numBytes);

  auto inDictStream = stripe.getStream(
      encodingKey.forKind(proto::Stream_Kind_IN_DICTIONARY),
      params.streamLabels().label(),
      false);
  if (inDictStream) {
    inDictionaryReader_ =
        createBooleanRleDecoder(std::move(inDictStream), encodingKey);
  }
  scanState_.updateRawState();
}

uint64_t SelectiveIntegerDictionaryColumnReader::skip(uint64_t numValues) {
  numValues = SelectiveColumnReader::skip(numValues);
  dataReader_->skip(numValues);
  if (inDictionaryReader_) {
    inDictionaryReader_->skip(numValues);
  }
  return numValues;
}

void SelectiveIntegerDictionaryColumnReader::read(
    vector_size_t offset,
    const RowSet& rows,
    const uint64_t* incomingNulls) {
  VELOX_WIDTH_DISPATCH(
      sizeOfIntKind(fileType_->type()->kind()),
      prepareRead,
      offset,
      rows,
      incomingNulls);
  const auto end = rows.back() + 1;
  const auto* rawNulls =
      nullsInReadRange_ ? nullsInReadRange_->as<uint64_t>() : nullptr;

  // Read the stream of booleans indicating whether a given data entry is an
  // offset or a literal value.
  if (inDictionaryReader_) {
    const bool isBulk = useBulkPath();
    const int32_t numFlags = (isBulk && nullsInReadRange_)
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
        isBulk ? nullptr : rawNulls);
  }

  // lazy load dictionary only when it's needed
  ensureInitialized();
  readCommon<SelectiveIntegerDictionaryColumnReader, true>(rows);

  readOffset_ += rows.back() + 1;
}

void SelectiveIntegerDictionaryColumnReader::ensureInitialized() {
  if (LIKELY(initialized_)) {
    return;
  }

  ClockTimer timer{initTimeClocks_};
  scanState_.dictionary.values = dictInit_();
  if (DictionaryValues::hasFilter(scanSpec_->filter())) {
    // Make sure there is a cache even for an empty dictionary because of asan
    // failure when preparing a gather with all lanes masked out.
    scanState_.filterCache.resize(
        std::max<int32_t>(1, scanState_.dictionary.numValues));
    simd::memset(
        scanState_.filterCache.data(),
        FilterResult::kUnknown,
        scanState_.filterCache.size());
  }
  scanState_.updateRawState();
  initialized_ = true;
}

} // namespace facebook::velox::dwrf
