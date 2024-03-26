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

class ParquetTypeWithId;

namespace {
PageReader* readLeafRepDefs(
    dwio::common::SelectiveColumnReader* reader,
    int32_t numTop,
    bool mustRead) {
  auto children = reader->children();
  if (children.empty()) {
    if (!mustRead) {
      return nullptr;
    }
    auto pageReader = reader->formatData().as<ParquetData>().reader();
    pageReader->decodeRepDefs(numTop);
    return pageReader;
  }
  PageReader* pageReader = nullptr;
  auto& type = *reinterpret_cast<const ParquetTypeWithId*>(&reader->fileType());
  if (type.type()->kind() == TypeKind::ARRAY) {
    pageReader = readLeafRepDefs(children[0], numTop, true);
    auto list = dynamic_cast<ListColumnReader*>(reader);
    assert(list);
    list->setLengthsFromRepDefs(*pageReader);
    return pageReader;
  }
  if (type.type()->kind() == TypeKind::MAP) {
    pageReader = readLeafRepDefs(children[0], numTop, true);
    readLeafRepDefs(children[1], numTop, false);
    auto map = dynamic_cast<MapColumnReader*>(reader);
    assert(map);
    map->setLengthsFromRepDefs(*pageReader);
    return pageReader;
  }
  if (auto structReader = dynamic_cast<StructColumnReader*>(reader)) {
    pageReader = readLeafRepDefs(structReader->childForRepDefs(), numTop, true);
    assert(pageReader);
    structReader->setNullsFromRepDefs(*pageReader);
    for (auto i = 0; i < children.size(); ++i) {
      auto child = children[i];
      if (child != structReader->childForRepDefs()) {
        readLeafRepDefs(child, numTop, false);
      }
    }
  }
  return pageReader;
}

void skipUnreadLengthsAndNulls(dwio::common::SelectiveColumnReader& reader) {
  auto children = reader.children();
  if (children.empty()) {
    return;
  }
  if (reader.fileType().type()->kind() == TypeKind::ARRAY) {
    reinterpret_cast<ListColumnReader*>(&reader)->skipUnreadLengths();
  } else if (reader.fileType().type()->kind() == TypeKind::ROW) {
    reinterpret_cast<StructColumnReader*>(&reader)->seekToEndOfPresetNulls();
  } else if (reader.fileType().type()->kind() == TypeKind::MAP) {
    reinterpret_cast<MapColumnReader*>(&reader)->skipUnreadLengths();
  } else {
    VELOX_UNREACHABLE();
  }
}

void enqueueChildren(
    dwio::common::SelectiveColumnReader* reader,
    uint32_t index,
    dwio::common::BufferedInput& input) {
  auto children = reader->children();
  if (children.empty()) {
    return reader->formatData().as<ParquetData>().enqueueRowGroup(index, input);
  }
  for (auto* child : children) {
    enqueueChildren(child, index, input);
  }
}
} // namespace

void ensureRepDefs(
    dwio::common::SelectiveColumnReader& reader,
    int32_t numTop) {
  auto& fileType =
      *reinterpret_cast<const ParquetTypeWithId*>(&reader.fileType());
  // Check that this is a direct child of the root struct.
  if (fileType.parent() && !fileType.parent()->parent()) {
    skipUnreadLengthsAndNulls(reader);
    readLeafRepDefs(&reader, numTop, true);
  }
}

MapColumnReader::MapColumnReader(
    const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    ParquetParams& params,
    common::ScanSpec& scanSpec)
    : dwio::common::SelectiveMapColumnReader(
          requestedType,
          fileType,
          params,
          scanSpec) {
  DWIO_ENSURE_EQ(fileType_->id(), fileType->id(), "working on the same node");
  auto& keyChildType = requestedType->childAt(0);
  auto& elementChildType = requestedType->childAt(1);
  keyReader_ = ParquetColumnReader::build(
      keyChildType, fileType_->childAt(0), params, *scanSpec.children()[0]);
  elementReader_ = ParquetColumnReader::build(
      elementChildType, fileType_->childAt(1), params, *scanSpec.children()[1]);
  reinterpret_cast<const ParquetTypeWithId*>(fileType.get())
      ->makeLevelInfo(levelInfo_);
  children_ = {keyReader_.get(), elementReader_.get()};
}

void MapColumnReader::enqueueRowGroup(
    uint32_t index,
    dwio::common::BufferedInput& input) {
  enqueueChildren(this, index, input);
}

void MapColumnReader::seekToRowGroup(uint32_t index) {
  SelectiveMapColumnReader::seekToRowGroup(index);
  readOffset_ = 0;
  childTargetReadOffset_ = 0;
  BufferPtr noBuffer;
  formatData_->as<ParquetData>().setNulls(noBuffer, 0);
  lengths_.setLengths(nullptr);
  keyReader_->seekToRowGroup(index);
  elementReader_->seekToRowGroup(index);
}

void MapColumnReader::skipUnreadLengths() {
  auto& previousLengths = lengths_.lengths();
  if (previousLengths) {
    auto numPreviousLengths =
        (previousLengths->size() / sizeof(vector_size_t)) -
        lengths_.nextLengthIndex();
    if (numPreviousLengths) {
      skip(numPreviousLengths);
    }
  }
}

void MapColumnReader::setLengthsFromRepDefs(PageReader& pageReader) {
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

void MapColumnReader::read(
    vector_size_t offset,
    RowSet rows,
    const uint64_t* incomingNulls) {
  // The topmost list reader reads the repdefs for the left subtree.
  ensureRepDefs(*this, offset + rows.back() + 1 - readOffset_);
  if (offset > readOffset_) {
    // There is no page reader on this level so cannot call skipNullsOnly on it.
    if (fileType().parent() && !fileType().parent()->parent()) {
      skip(offset - readOffset_);
    }
    readOffset_ = offset;
  }
  SelectiveMapColumnReader::read(offset, rows, incomingNulls);

  // The child should be at the end of the range provided to this
  // read() so that it can receive new repdefs for the next set of top
  // level rows. The end of the range is not the end of unused lengths
  // because all lengths maty have been used but the last one might
  // have been 0.  If the last list was 0 and the previous one was not
  // in 'rows' we will be at the end of the last non-zero list in
  // 'rows', which is not the end of the lengths. ORC can seek to this
  // point on next read, Parquet needs to seek here because new
  // repdefs will be scanned and new lengths provided, overwriting the
  // previous ones before the next read().
  keyReader_->seekTo(childTargetReadOffset_, false);
  elementReader_->seekTo(childTargetReadOffset_, false);
}

void MapColumnReader::filterRowGroups(
    uint64_t rowGroupSize,
    const dwio::common::StatsContext& context,
    dwio::common::FormatData::FilterRowGroupsResult& result) const {
  keyReader_->filterRowGroups(rowGroupSize, context, result);
  elementReader_->filterRowGroups(rowGroupSize, context, result);
}

ListColumnReader::ListColumnReader(
    const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    ParquetParams& params,
    common::ScanSpec& scanSpec)
    : dwio::common::SelectiveListColumnReader(
          requestedType,
          fileType,
          params,
          scanSpec) {
  auto& childType = requestedType->childAt(0);
  child_ = ParquetColumnReader::build(
      childType, fileType_->childAt(0), params, *scanSpec.children()[0]);
  reinterpret_cast<const ParquetTypeWithId*>(fileType.get())
      ->makeLevelInfo(levelInfo_);
  children_ = {child_.get()};
}

void ListColumnReader::enqueueRowGroup(
    uint32_t index,
    dwio::common::BufferedInput& input) {
  enqueueChildren(this, index, input);
}

void ListColumnReader::seekToRowGroup(uint32_t index) {
  SelectiveListColumnReader::seekToRowGroup(index);
  readOffset_ = 0;
  childTargetReadOffset_ = 0;
  BufferPtr noBuffer;
  formatData_->as<ParquetData>().setNulls(noBuffer, 0);
  lengths_.setLengths(nullptr);
  child_->seekToRowGroup(index);
}

void ListColumnReader::skipUnreadLengths() {
  auto& previousLengths = lengths_.lengths();
  if (previousLengths) {
    auto numPreviousLengths =
        (previousLengths->size() / sizeof(vector_size_t)) -
        lengths_.nextLengthIndex();
    if (numPreviousLengths) {
      skip(numPreviousLengths);
    }
  }
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
  if (offset > readOffset_) {
    // There is no page reader on this level so cannot call skipNullsOnly on it.
    if (fileType().parent() && !fileType().parent()->parent()) {
      skip(offset - readOffset_);
    }
    readOffset_ = offset;
  }
  SelectiveListColumnReader::read(offset, rows, incomingNulls);

  // The child should be at the end of the range provided to this
  // read() so that it can receive new repdefs for the next set of top
  // level rows. The end of the range is not the end of unused lengths
  // because all lengths maty have been used but the last one might
  // have been 0.  If the last list was 0 and the previous one was not
  // in 'rows' we will be at the end of the last non-zero list in
  // 'rows', which is not the end of the lengths. ORC can seek to this
  // point on next read, Parquet needs to seek here because new
  // repdefs will be scanned and new lengths provided, overwriting the
  // previous ones before the next read().
  child_->seekTo(childTargetReadOffset_, false);
}

void ListColumnReader::filterRowGroups(
    uint64_t rowGroupSize,
    const dwio::common::StatsContext& context,
    dwio::common::FormatData::FilterRowGroupsResult& result) const {
  child_->filterRowGroups(rowGroupSize, context, result);
}

} // namespace facebook::velox::parquet
