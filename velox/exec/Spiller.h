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

#include "velox/exec/RowContainer.h"

namespace facebook::velox::exec {

// Describes a bit range inside a 64 bit hash number for use in
// partitioning data over multiple sets of spill files.
class HashBitRange {
 public:
  HashBitRange(uint8_t begin, uint8_t end)
      : begin_(begin), end_(end), fieldMask_(bits::lowMask(end - begin)) {}

  int32_t partition(uint64_t hash, int32_t numPartitions) const {
    int32_t number = (hash >> begin_) & fieldMask_;
    return number < numPartitions ? number : -1;
  }

  int32_t numPartitions() const {
    return 1 << (end_ - begin_);
  }

 private:
  // Low bit number of hash number bit range.
  const uint8_t begin_;
  // Bit number of first bit above the hash number bit range.
  const uint8_t end_;

  const uint64_t fieldMask_;
};

// Manages spilling data from a RowContainer.
class Spiller {
 public:
  using SpillRows = std::vector<char*, memory::StlMappedMemoryAllocator<char*>>;
  Spiller(
      RowContainer& container,
      RowContainer::Eraser eraser,
      RowTypePtr rowType,
      HashBitRange bits,
      int32_t numSortingKeys,
      const std::string& path,
      int64_t targetFileSize,
      memory::MemoryPool& pool,
      folly::Executor* executor)
      : container_(container),
        eraser_(eraser),
        rowType_(std::move(rowType)),
        bits_(bits),
        state_(
            path,
            bits.numPartitions(),
            numSortingKeys,
            targetFileSize,
            pool,
            spillMappedMemory()),
        pool_(pool),
        executor_(executor) {}

  // Spills rows from 'this' until there are under 'targetRows' rows
  // and 'targetBytes' of allocated variable length space in
  // use. 'iterator' should be at the start of 'container_' on first
  // call.  spill() starts with one spill partition and initializes
  // more spill partitions as needed to hit the size target. If there
  // is no more data to spill in one hash partition, it starts spilling
  // another hash partition until all hash partitions are spilling. 'bits'
  // specifies the bit field of the hash number of a row that determines which
  // hash partition the row belongs to. A spillable hash partition has a
  // SpillRun struct in 'spillRuns_' A targetRows of 0 causes all data to be
  // spilled and 'container_' to become empty.
  void spill(
      uint64_t targetRows,
      uint64_t targetBytes,
      RowContainerIterator& iterator);

  bool isSpilled(int32_t partition) const {
    return state_.hasFiles(partition);
  }

  // Finishes spilling and returns the rows that are in partitions that have not
  // started spilling.
  SpillRows finishSpill();

  RowContainer& container() const {
    return container_;
  }

  // For testing.
  SpillState& state() {
    return state_;
  }

  std::unique_ptr<TreeOfLosers<SpillStream>> startMerge(int32_t partition) {
    return state_.startMerge(partition, spillStreamOverRows(partition));
  }

  int64_t spilledBytes() const {
    return state_.spilledBytes();
  }

  // Extracts the keys, dependents or accumulators for 'rows' into '*result'.
  // Creates '*results' in spillPool() if nullptr. Used from Spiller and
  // RowContainerSpillStream.
  void extractSpill(folly::Range<char**> rows, RowVectorPtr& result);

  // Extracts up to 'maxRows' or 'maxBytes' from 'rows' into
  // 'spillVector'. The extract starts at nextBatchIndex and updates
  // nextBatchIndex to be the index of the first non-extracted element
  // of 'rows'. Returns the byte size of the extracted rows.
  int64_t extractSpillVector(
      SpillRows& rows_,
      int32_t maxRows,
      int64_t maxBytes,
      RowVectorPtr& spillVector,
      size_t& nextBatchIndex);

  // Returns the MappedMemory to use for intermediate storage for
  // spilling. This is not directly the RowContainer's memory because
  // this is usually at limit when starting spilling.
  static memory::MappedMemory& spillMappedMemory();

  // Global memory pool for spill intermediates. ~1MB per spill executor thread
  // is the expected peak utilization.
  static memory::MemoryPool& spillPool();

  // Returns a mergeable stream that goes over unspilled in-memory
  // rows for the spill partition  'partition'. finishSpill()
  // first and 'partition' must specify a partition that has started spilling.
  std::unique_ptr<SpillStream> spillStreamOverRows(int32_t partition);

 private:
  // Represents a run of rows from a spillable partition of
  // a RowContainer. Rows that hash to the same partition are accumulated here
  // and sorted in the case of sorted spilling. The run is then
  // spilled into storage as multiple batches. The rows are deleted
  // from this and the RowContainer as they are written. When 'rows'
  // goes empty this is refilled from the RowContainer for the next
  // spill run from the same partition.
  struct SpillRun {
    explicit SpillRun(memory::MappedMemory& mappedMemory)
        : rows(0, memory::StlMappedMemoryAllocator<char*>(&mappedMemory)) {}
    // Spillable rows from the RowContainer.
    SpillRows rows;
    // The total byte size of rows referenced from 'rows'.
    uint64_t numBytes{0};
    // True if 'rows' are sorted on their key.
    bool sorted{false};

    void clear() {
      rows.clear();
      numBytes = 0;
      sorted = false;
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

  // Prepares spill runs for the spillable hash number ranges in
  // 'spill'. Returns true if at end of 'iterator'. Returns false
  // before reaching end of iterator if found enough to spill. Adds spillable
  // runs to 'pendingSpillPartitions_'.
  bool fillSpillRuns(
      RowContainerIterator& iterator,
      uint64_t targetSize,
      SpillRows* rowsFromNonSpillingPartitions = nullptr);

  // Clears pending spill state.
  void clearSpillRuns();

  // Clears runs that have not started spilling.
  void clearNonSpillingRuns();

  // Sorts 'run' if not already sorted.
  void ensureSorted(SpillRun& run);

  // Function for writing a spill partition on an executor. Writes to
  // 'partition' until all rows in spillRuns_[partition] are written
  // or 'maxBytes' is exceeded. Returns the number of rows
  // written.
  std::unique_ptr<SpillStatus> writeSpill(int32_t partition, uint64_t maxBytes);

  // Writes out  and erases rows marked for spilling.
  void advanceSpill(uint64_t maxBytes);

  RowContainer& container_;
  const RowContainer::Eraser eraser_;
  RowTypePtr rowType_;
  const HashBitRange bits_;
  SpillState state_;

  // One spill run for each partition of spillable data.
  std::vector<SpillRun> spillRuns_;

  // Indices into 'spillRuns_' that are currently getting spilled.
  std::unordered_set<int32_t> pendingSpillPartitions_;

  // True if all rows of spilling partitions are in 'spillRuns_', so
  // that one can start reading these back. This means that the rows
  // that are not written out and deleted will be captured by
  // spillStreamOverRows().
  bool spillFinalized_{false};
  memory::MemoryPool& pool_;
  folly::Executor* const executor_;
};

} // namespace facebook::velox::exec
