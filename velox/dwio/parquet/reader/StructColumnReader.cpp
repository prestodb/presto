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

#include "velox/dwio/parquet/reader/StructColumnReader.h"

namespace facebook::velox::parquet {

StructColumnReader::StructColumnReader(
    const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
    ParquetParams& params,
    common::ScanSpec& scanSpec)
    : dwio::common::SelectiveStructColumnReader(
          dataType,
          dataType,
          params,
          scanSpec) {
  auto& childSpecs = scanSpec_->children();
  for (auto i = 0; i < childSpecs.size(); ++i) {
    if (childSpecs[i]->isConstant()) {
      continue;
    }
    auto childDataType = nodeType_->childByName(childSpecs[i]->fieldName());

    children_.push_back(
        ParquetColumnReader::build(childDataType, params, *childSpecs[i]));
    childSpecs[i]->setSubscript(children_.size() - 1);
  }
}

void StructColumnReader::enqueueRowGroup(
    uint32_t index,
    dwio::common::BufferedInput& input) {
  for (auto& child : children_) {
    if (auto structChild = dynamic_cast<StructColumnReader*>(child.get())) {
      structChild->enqueueRowGroup(index, input);
    } else {
      child->formatData().as<ParquetData>().enqueueRowGroup(index, input);
    }
  }
}

void StructColumnReader::seekToRowGroup(uint32_t index) {
  readOffset_ = 0;
  for (auto& child : children_) {
    child->seekToRowGroup(index);
  }
}

bool StructColumnReader::filterMatches(const thrift::RowGroup& /*rowGroup*/) {
  return true;
}

} // namespace facebook::velox::parquet
