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

#include "velox/dwio/dwrf/reader/SelectiveRepeatedColumnReader.h"
#include "velox/dwio/dwrf/reader/SelectiveDwrfReader.h"

namespace facebook::velox::dwrf {
namespace {
std::unique_ptr<dwio::common::IntDecoder</*isSigned*/ false>> makeLengthDecoder(
    const dwio::common::TypeWithId& fileType,
    DwrfParams& params,
    memory::MemoryPool& pool) {
  EncodingKey encodingKey{fileType.id(), params.flatMapContext().sequence};
  auto& stripe = params.stripeStreams();
  const auto rleVersion =
      convertRleVersion(stripe.getEncoding(encodingKey).kind());
  const auto lenId = encodingKey.forKind(proto::Stream_Kind_LENGTH);
  const bool lenVints = stripe.getUseVInts(lenId);
  return createRleDecoder</*isSigned=*/false>(
      stripe.getStream(lenId, params.streamLabels().label(), true),
      rleVersion,
      pool,
      lenVints,
      dwio::common::INT_BYTE_SIZE);
}
} // namespace

FlatMapContext flatMapContextFromEncodingKey(const EncodingKey& encodingKey) {
  return FlatMapContext{
      .sequence = encodingKey.sequence(),
      .inMapDecoder = nullptr,
      .keySelectionCallback = nullptr};
}

SelectiveListColumnReader::SelectiveListColumnReader(
    const TypePtr& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    DwrfParams& params,
    common::ScanSpec& scanSpec)
    : dwio::common::SelectiveListColumnReader(
          requestedType,
          fileType,
          params,
          scanSpec),
      length_(makeLengthDecoder(*fileType_, params, *memoryPool_)) {
  VELOX_CHECK_EQ(fileType_->id(), fileType->id(), "working on the same node");
  EncodingKey encodingKey{fileType_->id(), params.flatMapContext().sequence};
  auto& stripe = params.stripeStreams();
  // count the number of selected sub-columns
  auto& childType = requestedType_->childAt(0);
  if (scanSpec_->children().empty()) {
    scanSpec.getOrCreateChild(common::ScanSpec::kArrayElementsFieldName);
  }
  scanSpec_->children()[0]->setProjectOut(true);
  scanSpec_->children()[0]->setExtractValues(true);

  auto childParams = DwrfParams(
      stripe,
      params.streamLabels(),
      params.runtimeStatistics(),
      flatMapContextFromEncodingKey(encodingKey));
  child_ = SelectiveDwrfReader::build(
      childType, fileType_->childAt(0), childParams, *scanSpec_->children()[0]);
  children_ = {child_.get()};
}

SelectiveMapColumnReader::SelectiveMapColumnReader(
    const TypePtr& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    DwrfParams& params,
    common::ScanSpec& scanSpec)
    : dwio::common::SelectiveMapColumnReader(
          requestedType,
          fileType,
          params,
          scanSpec),
      length_(makeLengthDecoder(*fileType_, params, *memoryPool_)) {
  VELOX_CHECK_EQ(fileType_->id(), fileType->id(), "working on the same node");
  const EncodingKey encodingKey{
      fileType_->id(), params.flatMapContext().sequence};
  auto& stripe = params.stripeStreams();
  if (scanSpec_->children().empty()) {
    scanSpec_->getOrCreateChild(common::ScanSpec::kMapKeysFieldName);
    scanSpec_->getOrCreateChild(common::ScanSpec::kMapValuesFieldName);
  }
  scanSpec_->children()[0]->setProjectOut(true);
  scanSpec_->children()[0]->setExtractValues(true);
  scanSpec_->children()[1]->setProjectOut(true);
  scanSpec_->children()[1]->setExtractValues(true);

  auto& keyType = requestedType_->childAt(0);
  auto keyParams = DwrfParams(
      stripe,
      params.streamLabels(),
      params.runtimeStatistics(),
      flatMapContextFromEncodingKey(encodingKey));
  keyReader_ = SelectiveDwrfReader::build(
      keyType,
      fileType_->childAt(0),
      keyParams,
      *scanSpec_->children()[0].get());

  auto& valueType = requestedType_->childAt(1);
  auto elementParams = DwrfParams(
      stripe,
      params.streamLabels(),
      params.runtimeStatistics(),
      flatMapContextFromEncodingKey(encodingKey));
  elementReader_ = SelectiveDwrfReader::build(
      valueType,
      fileType_->childAt(1),
      elementParams,
      *scanSpec_->children()[1]);
  children_ = {keyReader_.get(), elementReader_.get()};
}

} // namespace facebook::velox::dwrf
