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

#pragma once

#include "velox/dwio/dwrf/common/OutputStream.h"
#include "velox/dwio/dwrf/common/wrap/dwrf-proto-wrapper.h"
#include "velox/dwio/dwrf/writer/StatisticsBuilder.h"

namespace facebook::velox::dwrf {

namespace {

constexpr int32_t PRESENT_STREAM_INDEX_ENTRIES_UNPAGED = 3;
constexpr int32_t PRESENT_STREAM_INDEX_ENTRIES_PAGED =
    PRESENT_STREAM_INDEX_ENTRIES_UNPAGED + 1;

} // namespace

class IndexBuilder : public PositionRecorder {
 public:
  IndexBuilder(std::unique_ptr<BufferedOutputStream> out)
      : out_{std::move(out)} {}

  virtual ~IndexBuilder() = default;

  void add(uint64_t pos, int32_t index = -1) override {
    getEntry(index)->add_positions(pos);
  }

  virtual void addEntry(const StatisticsBuilder& writer) {
    auto stats = entry_.mutable_statistics();
    writer.toProto(*stats);
    *index_.add_entry() = entry_;
    entry_.Clear();
  }

  virtual size_t getEntrySize() const {
    int32_t size = index_.entry_size() + 1;
    DWIO_ENSURE_GT(size, 0, "Invalid entry size or missing current entry.");
    return size;
  }

  virtual void flush() {
    // remove isPresent positions if none is null
    index_.SerializeToZeroCopyStream(out_.get());
    out_->flush();
    index_.Clear();
    entry_.Clear();
  }

  void capturePresentStreamOffset() {
    if (!presentStreamOffset_) {
      presentStreamOffset_ = entry_.positions_size();
    } else {
      DWIO_ENSURE_EQ(presentStreamOffset_.value(), entry_.positions_size());
    }
  }

  void removePresentStreamPositions(bool isPaged) {
    DWIO_ENSURE(presentStreamOffset_.has_value());
    auto streamCount = isPaged ? PRESENT_STREAM_INDEX_ENTRIES_PAGED
                               : PRESENT_STREAM_INDEX_ENTRIES_UNPAGED;
    // Only need to process entries that have been added to the row index
    for (uint32_t i = 0; i < index_.entry_size(); ++i) {
      index_.mutable_entry(i)->mutable_positions()->ExtractSubrange(
          presentStreamOffset_.value(), streamCount, nullptr);
    }
  }

 private:
  friend class IndexBuilderTest;

  std::unique_ptr<BufferedOutputStream> out_;
  proto::RowIndex index_;
  proto::RowIndexEntry entry_;
  std::optional<int32_t> presentStreamOffset_;

  proto::RowIndexEntry* getEntry(int32_t index) {
    if (index < 0) {
      return &entry_;
    } else if (index < index_.entry_size()) {
      return index_.mutable_entry(index);
    } else {
      DWIO_ENSURE_EQ(index, index_.entry_size());
      return &entry_;
    }
  }
};

} // namespace facebook::velox::dwrf
