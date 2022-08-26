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

#include "velox/exec/HashPartitionFunction.h"
#include "velox/exec/RowContainer.h"

namespace facebook::velox::exec {

// Manages spilling data from a RowContainer.
class Spiller {
 public:
  // Define the spiller types.
  enum class Type {
    // Used for aggregation.
    kAggregate = 0,
    // Used for hash join.
    kHashJoin = 1,
    // Used for order by.
    kOrderBy = 2,
  };
  static constexpr int kNumTypes = 3;
  static std::string typeName(Type);

  using SpillRows = std::vector<char*, memory::StlMappedMemoryAllocator<char*>>;

  // The constructor without specifying hash bits which will only use one
  // partition by default. It is only used by kOrderBy spiller type as for now.
  Spiller(
      Type type,
      RowContainer& container,
      RowContainer::Eraser eraser,
      RowTypePtr rowType,
      int32_t numSortingKeys,
      const std::vector<CompareFlags>& sortCompareFlags,
      const std::string& path,
      int64_t targetFileSize,
      memory::MemoryPool& pool,
      folly::Executor* executor);

  Spiller(
      Type type,
      RowContainer& container,
      RowContainer::Eraser eraser,
      RowTypePtr rowType,
      HashBitRange bits,
      int32_t numSortingKeys,
      const std::vector<CompareFlags>& sortCompareFlags,
      const std::string& path,
      int64_t targetFileSize,
      memory::MemoryPool& pool,
      folly::Executor* executor);

  // Spills rows from 'this' until there are under 'targetRows' rows
  // and 'targetBytes' of allocated variable length space in use. spill() starts
  // with the partition with the most spillable data first. If there is no more
  // data to spill in one hash partition, it starts spilling another hash
  // partition until all hash partitions are spilling. A spillable hash
  // partition has a SpillRun struct in 'spillRuns_' A targetRows of 0 causes
  // all data to be spilled and 'container_' to become empty.
  void spill(uint64_t targetRows, uint64_t targetBytes);

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

  bool isSpilled(int32_t partition) const {
    return state_.isPartitionSpilled(partition);
  }

  std::unique_ptr<TreeOfLosers<SpillStream>> startMerge(int32_t partition) {
    return state_.startMerge(partition, spillStreamOverRows(partition));
  }

  // Define the spiller stats.
  struct Stats {
    uint64_t spilledBytes = 0;
    uint64_t spilledRows = 0;
    uint32_t spilledPartitions = 0;

    Stats(
        uint64_t _spilledBytes,
        uint64_t _spilledRows,
        uint32_t _spilledPartitions)
        : spilledBytes(_spilledBytes),
          spilledRows(_spilledRows),
          spilledPartitions(_spilledPartitions) {}

    Stats() = default;
  };

  Stats stats() const {
    return Stats{
        state_.spilledBytes(), spilledRows_, state_.spilledPartitions()};
  }

  int64_t spilledFiles() const {
    return state_.spilledFiles();
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
      SpillRows& rows,
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

  std::string toString() const;

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

  // Prepares spill runs for the spillable data from all the hash partitions.
  // If 'rowsFromNonSpillingPartitions' is not null, the function is invoked
  // to finish spill, and it will collect rows from the non-spilling partitions
  // in 'rowsFromNonSpillingPartitions' instead of 'spillRuns_'.
  void fillSpillRuns(SpillRows* rowsFromNonSpillingPartitions = nullptr);

  // Picks the next partition to spill. In case of non kHashJoin type, the
  // function picks the partition with spillable data no matter it has spilled
  // or not. For kHashJoin, the function first tries to pick the one from the
  // spilling partition first. If all the spilling partition has no spillable
  // data, it tries to look for one from non-spilling partitions. The function
  // returns -1 if all the partitions have no spillable data which should only
  // happen when finish spill to collect non-spilling rows.
  int32_t pickNextPartitionToSpill();

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

  // Writes out and erases rows marked for spilling.
  void advanceSpill(uint64_t maxBytes);

  const Type type_;
  RowContainer& container_;
  const RowContainer::Eraser eraser_;
  const HashBitRange bits_;
  const RowTypePtr rowType_;

  SpillState state_;

  // Indices into 'spillRuns_' that are currently getting spilled.
  std::unordered_set<int32_t> pendingSpillPartitions_;

  // One spill run for each partition of spillable data.
  std::vector<SpillRun> spillRuns_;

  // True if all rows of spilling partitions are in 'spillRuns_', so
  // that one can start reading these back. This means that the rows
  // that are not written out and deleted will be captured by
  // spillStreamOverRows().
  bool spillFinalized_{false};
  memory::MemoryPool& pool_;
  folly::Executor* const executor_;
  uint64_t spilledRows_{0};
};

} // namespace facebook::velox::exec
