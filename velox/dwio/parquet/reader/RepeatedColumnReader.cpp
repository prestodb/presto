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

#include "velox/dwio/parquet/reader/RepeatedColumnReader.h"
#include "velox/dwio/parquet/reader/ParquetColumnReader.h"
#include "velox/dwio/parquet/reader/StructColumnReader.h"

namespace facebook::velox::parquet {

PageReader* readLeafRepDefs(
    dwio::common::SelectiveColumnReader* FOLLY_NONNULL reader,
    int32_t numTop) {
  auto children = reader->children();
  if (children.empty()) {
    auto pageReader = reader->formatData().as<ParquetData>().reader();
    pageReader->decodeRepDefs(numTop);
    return pageReader;
  }
  PageReader* pageReader;
  for (auto i = 0; i < children.size(); ++i) {
    auto child = children[i];
    if (i == 0) {
      pageReader = readLeafRepDefs(child, numTop);

      auto& type =
          *reinterpret_cast<const ParquetTypeWithId*>(&reader->nodeType());
      if (auto structChild = dynamic_cast<StructColumnReader*>(reader)) {
        VELOX_NYI();
      }
      auto list = dynamic_cast<ListColumnReader*>(reader);
      VELOX_CHECK(list);
      list->setLengthsFromRepDefs(*pageReader);
    } else {
      readLeafRepDefs(child, numTop);
    }
  }
  return pageReader;
}

void enqueueChildren(
    dwio::common::SelectiveColumnReader* reader,
    uint32_t index,
    dwio::common::BufferedInput& input) {
  auto children = reader->children();
  if (children.empty()) {
    reader->formatData().as<ParquetData>().enqueueRowGroup(index, input);
    return;
  }
  for (auto* child : children) {
    enqueueChildren(child, index, input);
  }
}

void ensureRepDefs(
    dwio::common::SelectiveColumnReader& reader,
    int32_t numTop) {
  auto& nodeType =
      *reinterpret_cast<const ParquetTypeWithId*>(&reader.nodeType());
  if (nodeType.maxDefine_ <= 1)
    readLeafRepDefs(&reader, numTop);
}

ListColumnReader::ListColumnReader(
    std::shared_ptr<const dwio::common::TypeWithId> requestedType,
    ParquetParams& params,
    common::ScanSpec& scanSpec)
    : dwio::common::SelectiveListColumnReader(
          requestedType,
          requestedType,
          params,
          scanSpec) {
  auto& childType = requestedType->childAt(0);
  child_ =
      ParquetColumnReader::build(childType, params, *scanSpec.children()[0]);
  reinterpret_cast<const ParquetTypeWithId*>(requestedType.get())
      ->makeLevelInfo(levelInfo_);
}

void ListColumnReader::enqueueRowGroup(
    uint32_t index,
    dwio::common::BufferedInput& input) {
  enqueueChildren(this, index, input);
}

void ListColumnReader::seekToRowGroup(uint32_t index) {
  SelectiveColumnReader::seekToRowGroup(index);
  readOffset_ = 0;
  childTargetReadOffset_ = 0;
  child_->seekToRowGroup(index);
}

void ListColumnReader::setLengthsFromRepDefs(PageReader& pageReader) {
  auto repDefRange = pageReader.repDefRange();
  int32_t numRepDefs = repDefRange.second - repDefRange.first;
  BufferPtr lengths = std::move(lengths_.lengths());
  dwio::common::ensureCapacity<int32_t>(lengths, numRepDefs, &memoryPool_);
  memset(lengths->asMutable<uint64_t>(), 0, lengths->size());
  dwio::common::ensureCapacity<uint64_t>(
      nullsInReadRange_, bits::nwords(numRepDefs), &memoryPool_);
  auto numLists = pageReader.getLengthsAndNulls(
      LevelMode::kList,
      levelInfo_,
      repDefRange.first,
      repDefRange.second,
      numRepDefs,
      lengths->asMutable<int32_t>(),
      nullsInReadRange()->asMutable<uint64_t>(),
      0);
  lengths->setSize(numLists * sizeof(int32_t));
  formatData_->as<ParquetData>().setNulls(nullsInReadRange(), numLists);
  setLengths(std::move(lengths));
}
void ListColumnReader::read(
    vector_size_t offset,
    RowSet rows,
    const uint64_t* incomingNulls) {
  // The topmost list reader reads the repdefs for the left subtree.
  ensureRepDefs(*this, offset + rows.back() + 1 - readOffset_);
  SelectiveListColumnReader::read(offset, rows, incomingNulls);
}

} // namespace facebook::velox::parquet
