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

#include "velox/common/base/SpillConfig.h"
#include "velox/common/compression/Compression.h"
#include "velox/exec/HashBitRange.h"
#include "velox/exec/RowContainer.h"

namespace facebook::velox::exec {
namespace test {
class SpillerTest;
}

class SpillerBase {
 public:
  using SpillRows = std::vector<char*, memory::StlAllocator<char*>>;

  virtual ~SpillerBase() = default;

  void finishSpill(SpillPartitionSet& partitionSet);

  const HashBitRange& hashBits() const {
    return bits_;
  }

  const SpillState& state() const {
    return state_;
  }

  bool finalized() const {
    return finalized_;
  }

  common::SpillStats stats() const;

  std::string toString() const;

 protected:
  SpillerBase(
      RowContainer* container,
      RowTypePtr rowType,
      HashBitRange bits,
      int32_t numSortingKeys,
      const std::vector<CompareFlags>& sortCompareFlags,
      uint64_t targetFileSize,
      uint64_t maxSpillRunRows,
      const common::SpillConfig* spillConfig,
      folly::Synchronized<common::SpillStats>* spillStats);

  // Invoked to spill. If 'startRowIter' is not null, then we only spill rows
  // from row container starting at the offset pointed by 'startRowIter'.
  void spill(const RowContainerIterator* startRowIter);

  // Writes out all the rows collected in spillRuns_.
  virtual void runSpill(bool lastRun);

  // Extracts the keys, dependents or accumulators for 'rows' into '*result'.
  // Creates '*results' in spillPool() if nullptr. Used from Spiller and
  // RowContainerSpillMergeStream.
  virtual void extractSpill(folly::Range<char**> rows, RowVectorPtr& resultPtr);

  virtual bool needSort() const = 0;

  virtual std::string type() const = 0;

  // Marks all the partitions have been spilled as we don't support
  // fine-grained spilling as for now.
  void markAllPartitionsSpilled();

  void updateSpillFillTime(uint64_t timeNs);

  void checkEmptySpillRuns() const;

  // Represents a run of rows from a spillable partition of
  // a RowContainer. Rows that hash to the same partition are accumulated here
  // and sorted in the case of sorted spilling. The run is then
  // spilled into storage as multiple batches. The rows are deleted
  // from this and the RowContainer as they are written. When 'rows'
  // goes empty this is refilled from the RowContainer for the next
  // spill run from the same partition.
  struct SpillRun {
    explicit SpillRun(memory::MemoryPool& pool)
        : rows(0, memory::StlAllocator<char*>(pool)) {}
    // Spillable rows from the RowContainer.
    SpillRows rows;
    // The total byte size of rows referenced from 'rows'.
    uint64_t numBytes{0};
    // True if 'rows' are sorted on their key.
    bool sorted{false};

    void clear() {
      rows.clear();
      // Clears the memory allocated in rows after a spill run finishes.
      rows.shrink_to_fit();
      numBytes = 0;
      sorted = false;
    }

    std::string toString() const {
      return fmt::format(
          "[{} ROWS {} BYTES {}]",
          rows.size(),
          numBytes,
          sorted ? "SORTED" : "UNSORTED");
    }
  };

  struct SpillStatus {
    const int32_t partition;
    const int32_t rowsWritten;
    const std::exception_ptr error;

    SpillStatus(
        int32_t _partition,
        int32_t _numWritten,
        std::exception_ptr _error)
        : partition(_partition), rowsWritten(_numWritten), error(_error) {}
  };

  RowContainer* const container_{nullptr};

  folly::Executor* const executor_;

  const HashBitRange bits_;

  const RowTypePtr rowType_;

  const uint64_t maxSpillRunRows_;

  folly::Synchronized<common::SpillStats>* const spillStats_;

  // True if all rows of spilling partitions are in 'spillRuns_', so
  // that one can start reading these back.
  bool finalized_{false};

  SpillState state_;

  // Collects the rows to spill for each partition.
  std::vector<SpillRun> spillRuns_;

 private:
  // Function for writing a spill partition on an executor. Writes to
  // 'partition' until all rows in spillRuns_[partition] are written
  // or spill file size limit is exceeded. Returns the number of rows
  // written.
  std::unique_ptr<SpillStatus> writeSpill(int32_t partition);

  // Prepares spill runs for the spillable data from all the hash partitions.
  // If 'startRowIter' is not null, we prepare runs starting from the offset
  // pointed by 'startRowIter'.
  // The function returns true if it is the last spill run.
  bool fillSpillRuns(RowContainerIterator* startRowIter = nullptr);

  void updateSpillExtractVectorTime(uint64_t timeNs);

  void updateSpillSortTime(uint64_t timeNs);

  // Sorts 'run' if not already sorted.
  void ensureSorted(SpillRun& run);

  // Extracts up to 'maxRows' or 'maxBytes' from 'rows' into 'spillVector'. The
  // extract starts at nextBatchIndex and updates nextBatchIndex to be the
  // index of the first non-extracted element of 'rows'. Returns the byte size
  // of the extracted rows.
  int64_t extractSpillVector(
      SpillRows& rows,
      int32_t maxRows,
      int64_t maxBytes,
      RowVectorPtr& spillVector,
      size_t& nextBatchIndex);

  // Invoked to finalize the spiller and flush any buffered spill to disk.
  void finalizeSpill();

  friend class test::SpillerTest;
};

class NoRowContainerSpiller : public SpillerBase {
 public:
  static constexpr std::string_view kType = "NoRowContainerSpiller";

  NoRowContainerSpiller(
      RowTypePtr rowType,
      HashBitRange bits,
      const common::SpillConfig* spillConfig,
      folly::Synchronized<common::SpillStats>* spillStats);

  void spill(uint32_t partition, const RowVectorPtr& spillVector);

  void setPartitionsSpilled(const SpillPartitionNumSet& partitions) {
    for (const auto& partition : partitions) {
      state_.setPartitionSpilled(partition);
    }
  }

 private:
  std::string type() const override {
    return std::string(kType);
  }

  bool needSort() const override {
    return false;
  }
};

class SortInputSpiller : public SpillerBase {
 public:
  static constexpr std::string_view kType = "SortInputSpiller";

  SortInputSpiller(
      RowContainer* container,
      RowTypePtr rowType,
      int32_t numSortingKeys,
      const std::vector<CompareFlags>& sortCompareFlags,
      const common::SpillConfig* spillConfig,
      folly::Synchronized<common::SpillStats>* spillStats)
      : SpillerBase(
            container,
            std::move(rowType),
            HashBitRange{},
            numSortingKeys,
            sortCompareFlags,
            std::numeric_limits<uint64_t>::max(),
            spillConfig->maxSpillRunRows,
            spillConfig,
            spillStats) {}

  void spill();

 private:
  std::string type() const override {
    return std::string(kType);
  }

  bool needSort() const override {
    return true;
  }
};

class SortOutputSpiller : public SpillerBase {
 public:
  static constexpr std::string_view kType = "SortOutputSpiller";

  SortOutputSpiller(
      RowContainer* container,
      RowTypePtr rowType,
      const common::SpillConfig* spillConfig,
      folly::Synchronized<common::SpillStats>* spillStats);

  void spill(SpillRows& rows);

 private:
  void runSpill(bool lastRun) override;

  bool needSort() const override {
    return false;
  }

  std::string type() const override {
    return std::string(kType);
  }
};
} // namespace facebook::velox::exec
