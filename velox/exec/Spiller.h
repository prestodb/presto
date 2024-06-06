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

/// Manages spilling data from a RowContainer.
class Spiller {
 public:
  // Define the spiller types.
  enum class Type : int8_t {
    // Used for aggregation input processing stage.
    kAggregateInput = 0,
    // Used for aggregation output processing stage.
    kAggregateOutput = 1,
    // Used for hash join build.
    kHashJoinBuild = 2,
    // Used for hash join probe.
    kHashJoinProbe = 3,
    // Used for order by input processing stage.
    kOrderByInput = 4,
    // Used for order by output processing stage.
    kOrderByOutput = 5,
    // Used for row number.
    kRowNumber = 6,
    // Number of spiller types.
    kNumTypes = 7,
  };

  static std::string typeName(Type);

  using SpillRows = std::vector<char*, memory::StlAllocator<char*>>;

  /// The constructor without specifying hash bits which will only use one
  /// partition by default.

  /// type == Type::kAggregateInput
  Spiller(
      Type type,
      RowContainer* container,
      RowTypePtr rowType,
      const HashBitRange& hashBitRange,
      int32_t numSortingKeys,
      const std::vector<CompareFlags>& sortCompareFlags,
      const common::SpillConfig* spillConfig,
      folly::Synchronized<common::SpillStats>* spillStats);

  /// type == Type::kOrderByInput
  Spiller(
      Type type,
      RowContainer* container,
      RowTypePtr rowType,
      int32_t numSortingKeys,
      const std::vector<CompareFlags>& sortCompareFlags,
      const common::SpillConfig* spillConfig,
      folly::Synchronized<common::SpillStats>* spillStats);

  /// type == Type::kAggregateOutput || type == Type::kOrderByOutput
  Spiller(
      Type type,
      RowContainer* container,
      RowTypePtr rowType,
      const common::SpillConfig* spillConfig,
      folly::Synchronized<common::SpillStats>* spillStats);

  /// type == Type::kHashJoinProbe
  Spiller(
      Type type,
      RowTypePtr rowType,
      HashBitRange bits,
      const common::SpillConfig* spillConfig,
      folly::Synchronized<common::SpillStats>* spillStats);

  /// type == Type::kHashJoinBuild
  Spiller(
      Type type,
      core::JoinType joinType,
      RowContainer* container,
      RowTypePtr rowType,
      HashBitRange bits,
      const common::SpillConfig* spillConfig,
      folly::Synchronized<common::SpillStats>* spillStats);

  /// type == Type::kRowNumber
  Spiller(
      Type type,
      RowContainer* container,
      RowTypePtr rowType,
      HashBitRange bits,
      const common::SpillConfig* spillConfig,
      folly::Synchronized<common::SpillStats>* spillStats);

  Type type() const {
    return type_;
  }

  /// Spills all the rows from 'this' to disk. The spilled rows stays in the
  /// row container. The caller needs to erase the spilled rows from the row
  /// container.
  void spill();

  /// Spill all rows starting from 'startRowIter'. This is only used by
  /// 'kAggregateOutput' spiller type to spill during the aggregation output
  /// processing. Similarly, the spilled rows still stays in the row container.
  /// The caller needs to erase them from the row container.
  void spill(const RowContainerIterator& startRowIter);

  /// Invoked to spill all the rows pointed by rows. This is used by
  /// 'kOrderByOutput' spiller type to spill during the order by
  /// output processing. Similarly, the spilled rows still stays in the row
  /// container. The caller needs to erase them from the row container.
  void spill(std::vector<char*>& rows);

  /// Append 'spillVector' into the spill file of given 'partition'. It is now
  /// only used by the spilling operator which doesn't need data sort, such as
  /// hash join build and hash join probe.
  ///
  /// NOTE: the spilling operator should first mark 'partition' as spilling and
  /// spill any data buffered in row container before call this.
  void spill(uint32_t partition, const RowVectorPtr& spillVector);

  /// Extracts up to 'maxRows' or 'maxBytes' from 'rows' into 'spillVector'. The
  /// extract starts at nextBatchIndex and updates nextBatchIndex to be the
  /// index of the first non-extracted element of 'rows'. Returns the byte size
  /// of the extracted rows.
  int64_t extractSpillVector(
      SpillRows& rows,
      int32_t maxRows,
      int64_t maxBytes,
      RowVectorPtr& spillVector,
      size_t& nextBatchIndex);

  /// Finishes spilling and accumulate the spilled partition metadata in
  /// 'partitionSet' indexed by spill partition id.
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
    return state_.isAnyPartitionSpilled();
  }

  /// Returns the spilled partition number set.
  SpillPartitionNumSet spilledPartitionSet() const {
    return state_.spilledPartitionSet();
  }

  /// Invokes to set a set of 'partitions' as spilling.
  void setPartitionsSpilled(const SpillPartitionNumSet& partitions) {
    VELOX_CHECK_EQ(
        type_,
        Spiller::Type::kHashJoinProbe,
        "Unexpected spiller type: ",
        typeName(type_));
    for (const auto& partition : partitions) {
      state_.setPartitionSpilled(partition);
    }
  }

  /// Indicates if this spiller has finalized or not.
  bool finalized() const {
    return finalized_;
  }

  common::SpillStats stats() const;

  std::string toString() const;

 private:
  Spiller(
      Type type,
      RowContainer* container,
      RowTypePtr rowType,
      HashBitRange bits,
      int32_t numSortingKeys,
      const std::vector<CompareFlags>& sortCompareFlags,
      bool spillProbedFlag,
      const common::GetSpillDirectoryPathCB& getSpillDirPathCb,
      const common::UpdateAndCheckSpillLimitCB& updateAndCheckSpillLimitCb,
      const std::string& fileNamePrefix,
      uint64_t targetFileSize,
      uint64_t writeBufferSize,
      common::CompressionKind compressionKind,
      folly::Executor* executor,
      uint64_t maxSpillRunRows,
      const std::string& fileCreateConfig,
      folly::Synchronized<common::SpillStats>* spillStats);

  // Invoked to spill. If 'startRowIter' is not null, then we only spill rows
  // from row container starting at the offset pointed by 'startRowIter'.
  void spill(const RowContainerIterator* startRowIter);

  // Extracts the keys, dependents or accumulators for 'rows' into '*result'.
  // Creates '*results' in spillPool() if nullptr. Used from Spiller and
  // RowContainerSpillMergeStream.
  void extractSpill(folly::Range<char**> rows, RowVectorPtr& result);

  // Returns a mergeable stream that goes over unspilled in-memory
  // rows for the spill partition  'partition'. finishSpill()
  // first and 'partition' must specify a partition that has started spilling.
  std::unique_ptr<SpillMergeStream> spillMergeStreamOverRows(int32_t partition);

  // Invoked to finalize the spiller and flush any buffered spill to disk.
  void finalizeSpill();

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

  void checkEmptySpillRuns() const;

  // Marks all the partitions have been spilled as we don't support
  // fine-grained spilling as for now.
  void markAllPartitionsSpilled();

  // Prepares spill runs for the spillable data from all the hash partitions.
  // If 'startRowIter' is not null, we prepare runs starting from the offset
  // pointed by 'startRowIter'.
  // The function returns true if it is the last spill run.
  bool fillSpillRuns(RowContainerIterator* startRowIter = nullptr);

  // Prepares spill run of a single partition for the spillable data from the
  // rows.
  void fillSpillRun(std::vector<char*>& rows);

  // Writes out all the rows collected in spillRuns_.
  void runSpill(bool lastRun);

  // Sorts 'run' if not already sorted.
  void ensureSorted(SpillRun& run);

  // Function for writing a spill partition on an executor. Writes to
  // 'partition' until all rows in spillRuns_[partition] are written
  // or spill file size limit is exceeded. Returns the number of rows
  // written.
  std::unique_ptr<SpillStatus> writeSpill(int32_t partition);

  // Indicates if the spill data needs to be sorted before write to file. It is
  // based on the spiller type. As for now, we need to sort spill data for any
  // non hash join types of spilling.
  bool needSort() const;

  void updateSpillFillTime(uint64_t timeUs);

  void updateSpillSortTime(uint64_t timeUs);

  const Type type_;
  // NOTE: for hash join probe type, there is no associated row container for
  // the spiller.
  RowContainer* const container_{nullptr};
  folly::Executor* const executor_;
  const HashBitRange bits_;
  const RowTypePtr rowType_;
  const bool spillProbedFlag_;
  const uint64_t maxSpillRunRows_;

  folly::Synchronized<common::SpillStats>* const spillStats_;

  // True if all rows of spilling partitions are in 'spillRuns_', so
  // that one can start reading these back. This means that the rows
  // that are not written out and deleted will be captured by
  // spillMergeStreamOverRows().
  bool finalized_{false};

  SpillState state_;

  // Collects the rows to spill for each partition.
  std::vector<SpillRun> spillRuns_;
};
} // namespace facebook::velox::exec

template <>
struct fmt::formatter<facebook::velox::exec::Spiller::Type> : formatter<int> {
  auto format(facebook::velox::exec::Spiller::Type s, format_context& ctx) {
    return formatter<int>::format(static_cast<int>(s), ctx);
  }
};
