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

#include "velox/dwio/dwrf/reader/SelectiveMapColumnReader.h"
#include "velox/dwio/common/BufferUtil.h"
#include "velox/dwio/dwrf/reader/SelectiveDwrfReader.h"

namespace facebook::velox::dwrf {

using namespace facebook::velox::dwio::common;

SelectiveMapColumnReader::SelectiveMapColumnReader(
    const TypePtr& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    DwrfParams& params,
    common::ScanSpec& scanSpec)
    : facebook::velox::dwio::common::SelectiveMapColumnReader(
          requestedType,
          fileType,
          params,
          scanSpec) {
  EncodingKey encodingKey{fileType_->id(), params.flatMapContext().sequence};
  auto& stripe = params.stripeStreams();
  formatData_->as<DwrfData>().makeLengthDecoder(
      encodingKey, stripe, params.streamLabels().label());

  const auto& children = *scanSpec.children();
  VELOX_CHECK_EQ(children.size(), 2);

  // Nested map/list children are never flat-map sequences of the parent.
  FlatMapContext nonFlat = FlatMapContext::nonFlatMapContext();
  DwrfParams keyParams(params, nonFlat);
  keyReader_ = SelectiveDwrfReader::build(
      requestedType->childAt(0),
      fileType->childAt(0),
      keyParams,
      *children[0]);

  DwrfParams elementParams(params, nonFlat);
  elementReader_ = SelectiveDwrfReader::build(
      requestedType->childAt(1),
      fileType->childAt(1),
      elementParams,
      *children[1]);

  children_ = {keyReader_.get(), elementReader_.get()};
}

void SelectiveMapColumnReader::readLengths(
    int32_t* lengths,
    int32_t numLengths,
    const uint64_t* nulls) {
  // Length stream has one entry per non-null map only. Null maps (and
  // empty-present ranges) must be passed through so the RLE decoder does not
  // consume past the stream (failOnEof in RleDecoderV2).
  if (numLengths == 0) {
    return;
  }
  formatData_->readLengths(lengths, numLengths, nulls);
}

} // namespace facebook::velox::dwrf
