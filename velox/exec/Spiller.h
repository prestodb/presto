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

#include "velox/exec/HashBitRange.h"
#include "velox/exec/RowContainer.h"

namespace facebook::velox::exec {

/// Manages spilling data from a RowContainer.
class Spiller {
 public:
  // Define the spiller types.
  enum class Type {
    // Used for aggregation.
    kAggregate = 0,
    // Used for hash join build.
    kHashJoinBuild = 1,
    // Used for hash join probe.
    kHashJoinProbe = 2,
    // Used for order by.
    kOrderBy = 3,
  };
  static constexpr int kNumTypes = 3;
  static std::string typeName(Type);

  // Specifies the config for spilling.
  struct Config {
    Config(
        const std::string& _filePath,
        double _fileSizeFactor,
        folly::Executor* FOLLY_NULLABLE _executor,
        int32_t _spillableReservationGrowthPct,
        const HashBitRange& _hashBitRange,
        int32_t _testSpillPct)
        : filePath(_filePath),
          fileSizeFactor(_fileSizeFactor),
          executor(_executor),
          spillableReservationGrowthPct(_spillableReservationGrowthPct),
          hashBitRange(_hashBitRange),
          testSpillPct(_testSpillPct) {}

    // Filesystem path for spill files.
    std::string filePath;

    // Used to calculate the spill file size based on the associated task or
    // operator's memory usage.
    double fileSizeFactor;

    // Executor for spilling. If nullptr spilling writes on the Driver's thread.
    folly::Executor* FOLLY_NULLABLE executor; // Not owned.

    // The spillable memory reservation growth percentage of the current
    // reservation size.
    int32_t spillableReservationGrowthPct;

    // Used to calculate the spill hash partition number.
    HashBitRange hashBitRange;

    // Percentage of input batches to be spilled for testing. 0 means no
    // spilling for test.
    int32_t testSpillPct;
  };

  using SpillRows = std::vector<char*, memory::StlMappedMemoryAllocator<char*>>;

  // The constructor without specifying hash bits which will only use one
  // partition by default. It is only used by kOrderBy spiller type as for now.
  Spiller(
      Type type,
      RowContainer* FOLLY_NONNULL container,
      RowContainer::Eraser eraser,
      RowTypePtr rowType,
      int32_t numSortingKeys,
      const std::vector<CompareFlags>& sortCompareFlags,
      const std::string& path,
      int64_t targetFileSize,
      memory::MemoryPool& pool,
      folly::Executor* FOLLY_NULLABLE executor);

  Spiller(
      Type type,
      RowTypePtr rowType,
      HashBitRange bits,
      const std::string& path,
      int64_t targetFileSize,
      memory::MemoryPool& pool,
      folly::Executor* FOLLY_NULLABLE executor);

  Spiller(
      Type type,
      RowContainer* FOLLY_NULLABLE container,
      RowContainer::Eraser eraser,
      RowTypePtr rowType,
      HashBitRange bits,
      int32_t numSortingKeys,
      const std::vector<CompareFlags>& sortCompareFlags,
      const std::string& path,
      int64_t targetFileSize,
      memory::MemoryPool& pool,
      folly::Executor* FOLLY_NULLABLE executor);

  /// Spills rows from 'this' until there are under 'targetRows' rows
  /// and 'targetBytes' of allocated variable length space in use. spill()
  /// starts with the partition with the most spillable data first. If there is
  /// no more data to spill in one hash partition, it starts spilling another
  /// hash partition until all hash partitions are spilling. A spillable hash
  /// partition has a SpillRun struct in 'spillRuns_' A targetRows of 0 causes
  /// all data to be spilled and 'container_' to become empty.
  void spill(uint64_t targetRows, uint64_t targetBytes);

  /// Spills all the spillable rows collected in 'spillRuns_' from specified
  /// 'partitions'. It is now only used by spilling operator which needs
  /// spilling coordination across multiple drivers such as hash build. One of
  /// the driver is selected as the spill coordinator which first picks up a set
  /// of partitions which have the most spillable from all the participated
  /// drivers, and then spill the chosen partitions on all the drivers. Once
  /// after that, for those spilled partitions, the spilling operator will
  /// append new incoming vector to the spill file directly without buffering in
  /// row container anymore.
  void spill(const SpillPartitionNumSet& partitions);

  /// Append 'spillVector' into the spill file of given 'partition'. It is now
  /// only used by the spilling operator which doesn't need data sort, such as
  /// hash join build and hash join probe.
  ///
  /// NOTE: the spilling operator should first mark 'partition' as spilling and
  /// spill any data buffered in row container before call this.
  void spill(uint32_t partition, const RowVectorPtr& spillVector);

  /// Finishes spilling and returns the rows that are in partitions that have
  /// not started spilling.
  SpillRows finishSpill();

  /// Finishes spilling and accumulate the spilled partition data in
  /// 'partitionSet' by spill partition id.
  void finishSpill(SpillPartitionSet& partitionSet);

  const SpillState& state() const {
    return state_;
  }

  const HashBitRange& hashBits() const {
    return bits_;
  }

  bool isSpilled(int32_t partition) const {
    return state_.isPartitionSpilled(partition);
  }

  /// Indicates if all the partitions have spilled.
  bool isAllSpilled() const {
    return state_.isAllPartitionSpilled();
  }

  /// Indicates if any one of the partitions has spilled.
  bool isAnySpilled() const {
    return state_.spilledPartitions() != 0;
  }

  /// Invokes to set a set of 'partitions' as spilling.
  void setPartitionsSpilled(const SpillPartitionNumSet& partitions) {
    for (const auto& partition : partitions) {
      state_.setPartitionSpilled(partition);
    }
  }

  /// Contains the amount of spillable data of a partition which includes the
  /// number of spillable rows and bytes.
  struct SpillableStats {
    int64_t numRows = 0;
    int64_t numBytes = 0;

    inline SpillableStats& operator+=(const SpillableStats& other) {
      this->numRows += other.numRows;
      this->numBytes += other.numBytes;
      return *this;
    }
  };

  /// Invoked to fill spill runs on all partitions and accumulate the spillable
  /// stats in 'statsList' by partition number.
  void fillSpillRuns(std::vector<SpillableStats>& statsList);

  std::unique_ptr<TreeOfLosers<SpillMergeStream>> startMerge(
      int32_t partition) {
    if (FOLLY_UNLIKELY(!needSort())) {
      VELOX_FAIL("Can't sort merge the unsorted spill data: {}", toString());
    }
    return state_.startMerge(partition, spillMergeStreamOverRows(partition));
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
  // RowContainerSpillMergeStream.
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
  std::unique_ptr<SpillMergeStream> spillMergeStreamOverRows(int32_t partition);

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
  void fillSpillRuns(
      SpillRows* FOLLY_NULLABLE rowsFromNonSpillingPartitions = nullptr);

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
  // or spill file size limit is exceeded. Returns the number of rows
  // written.
  std::unique_ptr<SpillStatus> writeSpill(int32_t partition);

  // Writes out and erases rows marked for spilling.
  void advanceSpill();

  // Indicates if the spill data needs to be sorted before write to file. It is
  // based on the spiller type. As for now, we need to sort spill data for any
  // non hash join types of spilling.
  bool needSort() const;

  const Type type_;
  // NOTE: for hash join probe type, there is no associated row container for
  // the spiller.
  RowContainer* const FOLLY_NULLABLE container_; // Not owned.
  const RowContainer::Eraser eraser_;
  const HashBitRange bits_;
  const RowTypePtr rowType_;

  SpillState state_;

  // Indices into 'spillRuns_' that are currently getting spilled.
  SpillPartitionNumSet pendingSpillPartitions_;

  // One spill run for each partition of spillable data.
  std::vector<SpillRun> spillRuns_;

  // True if all rows of spilling partitions are in 'spillRuns_', so
  // that one can start reading these back. This means that the rows
  // that are not written out and deleted will be captured by
  // spillMergeStreamOverRows().
  bool spillFinalized_{false};
  memory::MemoryPool& pool_;
  folly::Executor* FOLLY_NULLABLE const executor_;
  uint64_t spilledRows_{0};
};

} // namespace facebook::velox::exec
