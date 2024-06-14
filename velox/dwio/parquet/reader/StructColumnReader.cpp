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

#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/parquet/reader/ParquetColumnReader.h"
#include "velox/dwio/parquet/reader/RepeatedColumnReader.h"

namespace facebook::velox::common {
class ScanSpec;
}

namespace facebook::velox::parquet {

StructColumnReader::StructColumnReader(
    const TypePtr& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    ParquetParams& params,
    common::ScanSpec& scanSpec)
    : SelectiveStructColumnReader(requestedType, fileType, params, scanSpec) {
  auto& childSpecs = scanSpec_->stableChildren();
  for (auto i = 0; i < childSpecs.size(); ++i) {
    auto childSpec = childSpecs[i];
    if (childSpecs[i]->isConstant()) {
      continue;
    }
    auto childFileType = fileType_->childByName(childSpec->fieldName());
    auto childRequestedType =
        requestedType_->asRow().findChild(childSpec->fieldName());
    addChild(ParquetColumnReader::build(
        childRequestedType, childFileType, params, *childSpec));

    childSpecs[i]->setSubscript(children_.size() - 1);
  }
  auto type = reinterpret_cast<const ParquetTypeWithId*>(fileType_.get());
  if (type->parent()) {
    levelMode_ = reinterpret_cast<const ParquetTypeWithId*>(fileType_.get())
                     ->makeLevelInfo(levelInfo_);
    childForRepDefs_ = findBestLeaf();
    // Set mode to struct over lists if the child for repdefs has a list between
    // this and the child.
    auto child = childForRepDefs_;
    for (;;) {
      assert(child);
      if (child->fileType().type()->kind() == TypeKind::ARRAY ||
          child->fileType().type()->kind() == TypeKind::MAP) {
        levelMode_ = LevelMode::kStructOverLists;
        break;
      }
      if (child->fileType().type()->kind() == TypeKind::ROW) {
        child = reinterpret_cast<StructColumnReader*>(child)->childForRepDefs();
        continue;
      }
      levelMode_ = LevelMode::kNulls;
      break;
    }
  }
}

dwio::common::SelectiveColumnReader* FOLLY_NONNULL
StructColumnReader::findBestLeaf() {
  SelectiveColumnReader* best = nullptr;
  for (auto i = 0; i < children_.size(); ++i) {
    auto child = children_[i];
    auto kind = child->fileType().type()->kind();
    // Complex type child repdefs must be read in any case.
    if (kind == TypeKind::ROW || kind == TypeKind::ARRAY) {
      return child;
    }
    if (!best) {
      best = child;
    } else if (best->scanSpec()->filter() && !child->scanSpec()->filter()) {
      continue;
    } else if (!best->scanSpec()->filter() && child->scanSpec()->filter()) {
      best = child;
      continue;
    } else if (kind < best->fileType().type()->kind()) {
      best = child;
    }
  }
  assert(best);
  return best;
}

void StructColumnReader::read(
    vector_size_t offset,
    RowSet rows,
    const uint64_t* /*incomingNulls*/) {
  ensureRepDefs(*this, offset + rows.back() + 1 - readOffset_);
  SelectiveStructColumnReader::read(offset, rows, nullptr);
}

std::shared_ptr<dwio::common::BufferedInput> StructColumnReader::loadRowGroup(
    uint32_t index,
    const std::shared_ptr<dwio::common::BufferedInput>& input) {
  if (isRowGroupBuffered(index, *input)) {
    enqueueRowGroup(index, *input);
    return input;
  }
  auto newInput = input->clone();
  enqueueRowGroup(index, *newInput);
  newInput->load(dwio::common::LogType::STRIPE);
  return newInput;
}

bool StructColumnReader::isRowGroupBuffered(
    uint32_t index,
    dwio::common::BufferedInput& input) {
  auto [offset, length] =
      formatData().as<ParquetData>().getRowGroupRegion(index);
  return input.isBuffered(offset, length);
}

void StructColumnReader::enqueueRowGroup(
    uint32_t index,
    dwio::common::BufferedInput& input) {
  for (auto& child : children_) {
    if (auto structChild = dynamic_cast<StructColumnReader*>(child)) {
      structChild->enqueueRowGroup(index, input);
    } else if (auto listChild = dynamic_cast<ListColumnReader*>(child)) {
      listChild->enqueueRowGroup(index, input);
    } else if (auto mapChild = dynamic_cast<MapColumnReader*>(child)) {
      mapChild->enqueueRowGroup(index, input);
    } else {
      child->formatData().as<ParquetData>().enqueueRowGroup(index, input);
    }
  }
}

void StructColumnReader::seekToRowGroup(uint32_t index) {
  SelectiveStructColumnReader::seekToRowGroup(index);
  BufferPtr noBuffer;
  formatData_->as<ParquetData>().setNulls(noBuffer, 0);
  readOffset_ = 0;
  for (auto& child : children_) {
    child->seekToRowGroup(index);
  }
}

void StructColumnReader::seekToEndOfPresetNulls() {
  auto numUnread = formatData_->as<ParquetData>().presetNullsLeft();
  for (auto i = 0; i < children_.size(); ++i) {
    auto child = children_[i];
    if (!child) {
      continue;
    }

    if (child->fileType().type()->kind() != TypeKind::ROW) {
      child->seekTo(readOffset_ + numUnread, false);
    } else if (child->fileType().type()->kind() == TypeKind::ROW) {
      reinterpret_cast<StructColumnReader*>(child)->seekToEndOfPresetNulls();
    }
  }
  readOffset_ += numUnread;
  formatData_->as<ParquetData>().skipNulls(numUnread, false);
}

void StructColumnReader::setNullsFromRepDefs(PageReader& pageReader) {
  if (levelInfo_.def_level == 0) {
    return;
  }
  auto repDefRange = pageReader.repDefRange();
  int32_t numRepDefs = repDefRange.second - repDefRange.first;
  dwio::common::ensureCapacity<uint64_t>(
      nullsInReadRange_, bits::nwords(numRepDefs), &memoryPool_);
  auto numStructs = pageReader.getLengthsAndNulls(
      levelMode_,
      levelInfo_,
      repDefRange.first,
      repDefRange.second,
      numRepDefs,
      nullptr,
      nullsInReadRange()->asMutable<uint64_t>(),
      0);
  formatData_->as<ParquetData>().setNulls(nullsInReadRange(), numStructs);
}

void StructColumnReader::filterRowGroups(
    uint64_t rowGroupSize,
    const dwio::common::StatsContext& context,
    dwio::common::FormatData::FilterRowGroupsResult& result) const {
  for (const auto& child : children_) {
    child->filterRowGroups(rowGroupSize, context, result);
  }
}

} // namespace facebook::velox::parquet
