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

#include <folly/container/F14Set.h>

#include "velox/common/base/SpillConfig.h"
#include "velox/common/base/SpillStats.h"
#include "velox/common/compression/Compression.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/SpillFile.h"
#include "velox/exec/TreeOfLosers.h"
#include "velox/exec/UnorderedStreamReader.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/VectorStream.h"

namespace facebook::velox::exec {
// A source of sorted spilled RowVectors coming either from a file or memory.
class SpillMergeStream : public MergeStream {
 public:
  SpillMergeStream() = default;
  virtual ~SpillMergeStream() = default;

  /// Returns the id of a spill merge stream which is unique in the merge set.
  virtual uint32_t id() const = 0;

  bool hasData() const final {
    return index_ < size_;
  }

  bool operator<(const MergeStream& other) const final {
    return compare(other) < 0;
  }

  int32_t compare(const MergeStream& other) const override;

  void pop();

  const RowVector& current() const {
    return *rowVector_;
  }

  /// Invoked to get the current row index in 'rowVector_'. If 'isLastRow' is
  /// not null, it is set to true if current row is the last one in the current
  /// batch, in which case the caller must call copy out current batch data if
  /// required before calling pop().
  vector_size_t currentIndex(bool* isLastRow = nullptr) const {
    if (isLastRow != nullptr) {
      *isLastRow = (index_ == (rowVector_->size() - 1));
    }
    return index_;
  }

  /// Returns a DecodedVector set decoding the 'index'th child of 'rowVector_'
  DecodedVector& decoded(int32_t index) {
    ensureDecodedValid(index);
    return decoded_[index];
  }

 protected:
  virtual int32_t numSortKeys() const = 0;

  virtual const std::vector<CompareFlags>& sortCompareFlags() const = 0;

  virtual void nextBatch() = 0;

  // loads the next 'rowVector' and sets 'decoded_' if this is initialized.
  void setNextBatch() {
    nextBatch();
    if (!decoded_.empty()) {
      ensureRows();
      for (auto i = 0; i < decoded_.size(); ++i) {
        decoded_[i].decode(*rowVector_->childAt(i), rows_);
      }
    }
  }

  void ensureDecodedValid(int32_t index) {
    int32_t oldSize = decoded_.size();
    if (index < oldSize) {
      return;
    }
    ensureRows();
    decoded_.resize(index + 1);
    for (auto i = oldSize; i <= index; ++i) {
      decoded_[index].decode(*rowVector_->childAt(index), rows_);
    }
  }

  void ensureRows() {
    if (rows_.size() != rowVector_->size()) {
      rows_.resize(size_);
    }
  }

  // Current batch of rows.
  RowVectorPtr rowVector_;

  // The current row in 'rowVector_'
  vector_size_t index_{0};

  // Number of rows in 'rowVector_'
  vector_size_t size_{0};

  // Decoded vectors for leading parts of 'rowVector_'. Initialized on first
  // use and maintained when updating 'rowVector_'.
  std::vector<DecodedVector> decoded_;

  // Covers all rows inn 'rowVector_' Set if 'decoded_' is non-empty.
  SelectivityVector rows_;
};

// A source of spilled RowVectors coming from a file.
class FileSpillMergeStream : public SpillMergeStream {
 public:
  static std::unique_ptr<SpillMergeStream> create(
      std::unique_ptr<SpillReadFile> spillFile) {
    auto* spillStream = new FileSpillMergeStream(std::move(spillFile));
    spillStream->nextBatch();
    return std::unique_ptr<SpillMergeStream>(spillStream);
  }

  uint32_t id() const override;

 private:
  explicit FileSpillMergeStream(std::unique_ptr<SpillReadFile> spillFile)
      : spillFile_(std::move(spillFile)) {
    VELOX_CHECK_NOT_NULL(spillFile_);
  }

  int32_t numSortKeys() const override {
    return spillFile_->numSortKeys();
  }

  const std::vector<CompareFlags>& sortCompareFlags() const override {
    return spillFile_->sortCompareFlags();
  }

  void nextBatch() override;

  std::unique_ptr<SpillReadFile> spillFile_;
};

/// A source of spilled RowVectors coming from a file. The spill data might not
/// be sorted.
///
/// NOTE: this object is not thread-safe.
class FileSpillBatchStream : public BatchStream {
 public:
  static std::unique_ptr<BatchStream> create(
      std::unique_ptr<SpillReadFile> spillFile) {
    auto* spillStream = new FileSpillBatchStream(std::move(spillFile));
    return std::unique_ptr<BatchStream>(spillStream);
  }

  bool nextBatch(RowVectorPtr& batch) override {
    return spillFile_->nextBatch(batch);
  }

 private:
  explicit FileSpillBatchStream(std::unique_ptr<SpillReadFile> spillFile)
      : spillFile_(std::move(spillFile)) {
    VELOX_CHECK_NOT_NULL(spillFile_);
  }

  std::unique_ptr<SpillReadFile> spillFile_;
};

/// Identifies a spill partition generated from a given spilling operator. It
/// consists of partition start bit offset and the actual partition number. The
/// start bit offset is used to calculate the partition number of spill data. It
/// is required for the recursive spilling handling as we advance the start bit
/// offset when we go to the next level of recursive spilling.
///
/// NOTE: multiple shards created from the same SpillPartition by split()
/// will share the same id.
class SpillPartitionId {
 public:
  SpillPartitionId(uint8_t partitionBitOffset, int32_t partitionNumber)
      : partitionBitOffset_(partitionBitOffset),
        partitionNumber_(partitionNumber) {}

  bool operator==(const SpillPartitionId& other) const {
    return std::tie(partitionBitOffset_, partitionNumber_) ==
        std::tie(other.partitionBitOffset_, other.partitionNumber_);
  }

  bool operator!=(const SpillPartitionId& other) const {
    return !(*this == other);
  }

  /// Customize the compare operator for recursive spilling control. It ensures
  /// the partition with higher partition bit is handled prior than one with
  /// lower partition bit. With the same partition bit, the one with smaller
  /// partition number is handled first. We put all spill partitions in an
  /// ordered map sorted based on the partition id. The recursive spilling will
  /// advance the partition start bit when go to the next level of recursive
  /// spilling.
  bool operator<(const SpillPartitionId& other) const {
    if (partitionBitOffset_ != other.partitionBitOffset_) {
      return partitionBitOffset_ > other.partitionBitOffset_;
    }
    return partitionNumber_ < other.partitionNumber_;
  }

  bool operator>(const SpillPartitionId& other) const {
    return (*this != other) && !(*this < other);
  }

  std::string toString() const {
    return fmt::format("[{},{}]", partitionBitOffset_, partitionNumber_);
  }

  uint8_t partitionBitOffset() const {
    return partitionBitOffset_;
  }

  int32_t partitionNumber() const {
    return partitionNumber_;
  }

 private:
  uint8_t partitionBitOffset_{0};
  int32_t partitionNumber_{0};
};

inline std::ostream& operator<<(std::ostream& os, SpillPartitionId id) {
  os << id.toString();
  return os;
}

using SpillPartitionIdSet = folly::F14FastSet<SpillPartitionId>;
using SpillPartitionNumSet = folly::F14FastSet<uint32_t>;

/// Contains a spill partition data which includes the partition id and
/// corresponding spill files.
class SpillPartition {
 public:
  explicit SpillPartition(const SpillPartitionId& id)
      : SpillPartition(id, {}) {}

  SpillPartition(const SpillPartitionId& id, SpillFiles files) : id_(id) {
    addFiles(std::move(files));
  }

  void addFiles(SpillFiles files) {
    files_.reserve(files_.size() + files.size());
    for (auto& file : files) {
      size_ += file.size;
      files_.push_back(std::move(file));
    }
  }

  const SpillPartitionId& id() const {
    return id_;
  }

  int numFiles() const {
    return files_.size();
  }

  /// Returns the total file byte size of this spilled partition.
  uint64_t size() const {
    return size_;
  }

  /// Invoked to split this spill partition into 'numShards' to process in
  /// parallel.
  ///
  /// NOTE: the split spill partition shards will have the same id as this.
  std::vector<std::unique_ptr<SpillPartition>> split(int numShards);

  /// Invoked to create an unordered stream reader from this spill partition.
  /// The created reader will take the ownership of the spill files.
  std::unique_ptr<UnorderedStreamReader<BatchStream>> createUnorderedReader(
      memory::MemoryPool* pool);

  /// Invoked to create an ordered stream reader from this spill partition.
  /// The created reader will take the ownership of the spill files.
  std::unique_ptr<TreeOfLosers<SpillMergeStream>> createOrderedReader(
      memory::MemoryPool* pool);

  std::string toString() const;

 private:
  SpillPartitionId id_;
  SpillFiles files_;
  // Counts the total file size in bytes from this spilled partition.
  uint64_t size_{0};
};

using SpillPartitionSet =
    std::map<SpillPartitionId, std::unique_ptr<SpillPartition>>;

/// Represents all spilled data of an operator, e.g. order by or group
/// by. This has one SpillFileList per partition of spill data.
class SpillState {
 public:
  /// Constructs a SpillState. 'type' is the content RowType. 'path' is the file
  /// system path prefix. 'bits' is the hash bit field for partitioning data
  /// between files. This also gives the maximum number of partitions.
  /// 'numSortKeys' is the number of leading columns on which the data is
  /// sorted, 0 if only hash partitioning is used. 'targetFileSize' is the
  /// target size of a single file.  'pool' owns the memory for state and
  /// results.
  SpillState(
      const common::GetSpillDirectoryPathCB& getSpillDirectoryPath,
      const common::UpdateAndCheckSpillLimitCB& updateAndCheckSpillLimitCb,
      const std::string& fileNamePrefix,
      int32_t maxPartitions,
      int32_t numSortKeys,
      const std::vector<CompareFlags>& sortCompareFlags,
      uint64_t targetFileSize,
      uint64_t writeBufferSize,
      common::CompressionKind compressionKind,
      memory::MemoryPool* pool,
      folly::Synchronized<common::SpillStats>* stats,
      const std::string& fileCreateConfig = {});

  /// Indicates if a given 'partition' has been spilled or not.
  bool isPartitionSpilled(uint32_t partition) const {
    VELOX_DCHECK_LT(partition, maxPartitions_);
    return spilledPartitionSet_.contains(partition);
  }

  // Sets a partition as spilled.
  void setPartitionSpilled(uint32_t partition);

  // Returns how many ways spilled data can be partitioned.
  int32_t maxPartitions() const {
    return maxPartitions_;
  }

  uint64_t targetFileSize() const {
    return targetFileSize_;
  }

  common::CompressionKind compressionKind() const {
    return compressionKind_;
  }

  const std::vector<CompareFlags>& sortCompareFlags() const {
    return sortCompareFlags_;
  }

  bool isAnyPartitionSpilled() const {
    return !spilledPartitionSet_.empty();
  }

  bool isAllPartitionSpilled() const {
    VELOX_CHECK_LE(spilledPartitionSet_.size(), maxPartitions_);
    return spilledPartitionSet_.size() == maxPartitions_;
  }

  /// Appends data to 'partition'. The rows given by 'indices' must be sorted
  /// for a sorted spill and must hash to 'partition'. It is safe to call this
  /// on multiple threads if all threads specify a different partition. Returns
  /// the size to append to partition.
  uint64_t appendToPartition(uint32_t partition, const RowVectorPtr& rows);

  /// Finishes a sorted run for 'partition'. If write is called for 'partition'
  /// again, the data does not have to be sorted relative to the data written so
  /// far.
  void finishFile(uint32_t partition);

  /// Returns the spill file objects from a given 'partition'. The function
  /// returns an empty list if either the partition has not been spilled or has
  /// no spilled data.
  SpillFiles finish(uint32_t partition);

  /// Returns the spilled partition number set.
  const SpillPartitionNumSet& spilledPartitionSet() const;

  /// Returns the spilled file paths from all the partitions.
  std::vector<std::string> testingSpilledFilePaths() const;

  /// Returns the file ids from a given partition.
  std::vector<uint32_t> testingSpilledFileIds(int32_t partitionNum) const;

  /// Returns the set of partitions that have spilled data.
  SpillPartitionNumSet testingNonEmptySpilledPartitionSet() const;

 private:
  void updateSpilledInputBytes(uint64_t bytes);

  SpillWriter* partitionWriter(uint32_t partition) const;

  const RowTypePtr type_;

  // A callback function that returns the spill directory path. Implementations
  // can use it to ensure the path exists before returning.
  common::GetSpillDirectoryPathCB getSpillDirPathCb_;

  // Updates the aggregated spill bytes of this query, and throws if exceeds
  // the max spill bytes limit.
  common::UpdateAndCheckSpillLimitCB updateAndCheckSpillLimitCb_;

  /// Prefix for spill files.
  const std::string fileNamePrefix_;
  const int32_t maxPartitions_;
  const int32_t numSortKeys_;
  const std::vector<CompareFlags> sortCompareFlags_;
  const uint64_t targetFileSize_;
  const uint64_t writeBufferSize_;
  const common::CompressionKind compressionKind_;
  const std::string fileCreateConfig_;
  memory::MemoryPool* const pool_;
  folly::Synchronized<common::SpillStats>* const stats_;

  // A set of spilled partition numbers.
  SpillPartitionNumSet spilledPartitionSet_;

  // A file list for each spilled partition. Only partitions that have
  // started spilling have an entry here.
  std::vector<std::unique_ptr<SpillWriter>> partitionWriters_;
};

/// Generate partition id set from given spill partition set.
SpillPartitionIdSet toSpillPartitionIdSet(
    const SpillPartitionSet& partitionSet);
} // namespace facebook::velox::exec

// Adding the custom hash for SpillPartitionId to std::hash to make it usable
// with maps and other standard data structures.
namespace std {
template <>
struct hash<::facebook::velox::exec::SpillPartitionId> {
  size_t operator()(const ::facebook::velox::exec::SpillPartitionId& id) const {
    return facebook::velox::bits::hashMix(
        id.partitionBitOffset(), id.partitionNumber());
  }
};
} // namespace std

template <>
struct fmt::formatter<facebook::velox::exec::SpillPartitionId>
    : formatter<std::string> {
  auto format(facebook::velox::exec::SpillPartitionId s, format_context& ctx) {
    return formatter<std::string>::format(s.toString(), ctx);
  }
};
