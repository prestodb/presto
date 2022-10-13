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

#include "velox/dwio/dwrf/reader/SelectiveStructColumnReader.h"
#include "velox/dwio/common/ColumnLoader.h"
#include "velox/dwio/dwrf/reader/SelectiveDwrfReader.h"

namespace facebook::velox::dwrf {

using namespace dwio::common;

SelectiveStructColumnReader::SelectiveStructColumnReader(
    const std::shared_ptr<const TypeWithId>& requestedType,
    const std::shared_ptr<const TypeWithId>& dataType,
    DwrfParams& params,
    common::ScanSpec& scanSpec)
    : dwio::common::SelectiveStructColumnReader(
          requestedType,
          dataType,
          params,
          scanSpec),
      rowsPerRowGroup_(formatData_->rowsPerRowGroup().value()) {
  EncodingKey encodingKey{nodeType_->id, params.flatMapContext().sequence};
  DWIO_ENSURE_EQ(encodingKey.node, dataType->id, "working on the same node");
  auto& stripe = params.stripeStreams();
  auto encoding = static_cast<int64_t>(stripe.getEncoding(encodingKey).kind());
  DWIO_ENSURE_EQ(
      encoding,
      proto::ColumnEncoding_Kind_DIRECT,
      "Unknown encoding for StructColumnReader");

  const auto& cs = stripe.getColumnSelector();
  auto& childSpecs = scanSpec.children();
  for (auto i = 0; i < childSpecs.size(); ++i) {
    auto childSpec = childSpecs[i].get();
    if (childSpec->isConstant()) {
      continue;
    }
    auto childDataType = nodeType_->childByName(childSpec->fieldName());
    auto childRequestedType =
        requestedType_->childByName(childSpec->fieldName());
    auto childParams =
        DwrfParams(stripe, FlatMapContext{encodingKey.sequence, nullptr});
    VELOX_CHECK(cs.shouldReadNode(childDataType->id));
    children_.push_back(SelectiveDwrfReader::build(
        childRequestedType, childDataType, childParams, *childSpec));
    childSpec->setSubscript(children_.size() - 1);
  }
}

void SelectiveStructColumnReader::seekTo(
    vector_size_t offset,
    bool readsNullsOnly) {
  if (offset == readOffset_) {
    return;
  }
  if (readOffset_ < offset) {
    if (numParentNulls_) {
      VELOX_CHECK_LE(
          parentNullsRecordedTo_,
          offset,
          "Must not seek to before parentNullsRecordedTo_");
    }
    auto distance = offset - readOffset_ - numParentNulls_;
    auto numNonNulls = formatData_->skipNulls(distance);
    // We inform children how many nulls there were between original position
    // and destination. The children will seek this many less. The
    // nulls include the nulls found here as well as the enclosing
    // level nulls reported to this by parents.
    for (auto& child : children_) {
      if (child) {
        child->addSkippedParentNulls(
            readOffset_, offset, numParentNulls_ + distance - numNonNulls);
      }
    }
    numParentNulls_ = 0;
    parentNullsRecordedTo_ = 0;
    readOffset_ = offset;
  } else {
    VELOX_FAIL("Seeking backward on a ColumnReader");
  }
}

} // namespace facebook::velox::dwrf
