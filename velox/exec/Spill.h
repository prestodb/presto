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

#include <re2/re2.h>
#include "velox/common/base/SpillConfig.h"
#include "velox/common/base/SpillStats.h"
#include "velox/common/compression/Compression.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/SpillFile.h"
#include "velox/exec/TreeOfLosers.h"
#include "velox/exec/UnorderedStreamReader.h"
#include "velox/exec/VectorHasher.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/VectorStream.h"

namespace facebook::velox::exec {
class VectorHasher;

/// A source of sorted spilled RowVectors coming either from a file or memory.
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
    VELOX_CHECK(!closed_);
    return *rowVector_;
  }

  /// Invoked to get the current row index in 'rowVector_'. If 'isLastRow' is
  /// not null, it is set to true if current row is the last one in the current
  /// batch, in which case the caller must call copy out current batch data if
  /// required before calling pop().
  vector_size_t currentIndex(bool* isLastRow = nullptr) const {
    VELOX_CHECK(!closed_);
    if (isLastRow != nullptr) {
      *isLastRow = (index_ == (rowVector_->size() - 1));
    }
    return index_;
  }

  /// Returns a DecodedVector set decoding the 'index'th child of 'rowVector_'
  DecodedVector& decoded(int32_t index) {
    VELOX_CHECK(!closed_);
    ensureDecodedValid(index);
    return decoded_[index];
  }

 protected:
  virtual const std::vector<SpillSortKey>& sortingKeys() const = 0;

  virtual void nextBatch() = 0;

  virtual void close();

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

  // True if the stream is closed.
  bool closed_{false};

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

/// A source of spilled RowVectors coming from a file.
class FileSpillMergeStream : public SpillMergeStream {
 public:
  static std::unique_ptr<SpillMergeStream> create(
      std::unique_ptr<SpillReadFile> spillFile) {
    auto spillStream = std::unique_ptr<SpillMergeStream>(
        new FileSpillMergeStream(std::move(spillFile)));
    static_cast<FileSpillMergeStream*>(spillStream.get())->nextBatch();
    return spillStream;
  }

  uint32_t id() const override;

 private:
  explicit FileSpillMergeStream(std::unique_ptr<SpillReadFile> spillFile)
      : spillFile_(std::move(spillFile)) {
    VELOX_CHECK_NOT_NULL(spillFile_);
  }

  const std::vector<SpillSortKey>& sortingKeys() const override {
    VELOX_CHECK(!closed_);
    return spillFile_->sortingKeys();
  }

  void nextBatch() override;

  void close() override;

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

/// A source of spilled RowVectors coming from a sequence of files.
///
/// NOTE: this object is not thread-safe.
class ConcatFilesSpillBatchStream final : public BatchStream {
 public:
  static std::unique_ptr<BatchStream> create(
      std::vector<std::unique_ptr<SpillReadFile>> spillFiles);

  bool nextBatch(RowVectorPtr& batch) override;

 private:
  explicit ConcatFilesSpillBatchStream(
      std::vector<std::unique_ptr<SpillReadFile>> spillFiles)
      : spillFiles_(std::move(spillFiles)) {
    VELOX_CHECK(!spillFiles_.empty());
  }

  std::vector<std::unique_ptr<SpillReadFile>> spillFiles_;
  size_t fileIndex_{0};
  bool atEnd_{false};
};

/// A SpillMergeStream that contains a sequence of sorted spill files, the
/// sorted keys are ordered both within each file and across files.
class ConcatFilesSpillMergeStream final : public SpillMergeStream {
 public:
  static std::unique_ptr<SpillMergeStream> create(
      uint32_t id,
      std::vector<std::unique_ptr<SpillReadFile>> spillFiles);

 private:
  ConcatFilesSpillMergeStream(
      uint32_t id,
      std::vector<std::unique_ptr<SpillReadFile>> spillFiles)
      : id_(id), spillFiles_(std::move(spillFiles)) {}

  uint32_t id() const override;

  void nextBatch() override;

  void close() override;

  const std::vector<SpillSortKey>& sortingKeys() const override;

  const uint32_t id_;
  std::vector<std::unique_ptr<SpillReadFile>> spillFiles_;
  size_t fileIndex_{0};
};

/// Identifies a spill partition generated from a given spilling operator. It
/// provides with informattion on spill level and the partition number of each
/// spill level. When recursive spilling happens, there will be more than one
/// spill level.
///
/// NOTE: multiple shards created from the same SpillPartition by split()
/// will share the same id.
class SpillPartitionId {
 public:
  /// Maximum spill level supported by 'SpillPartitionId'.
  static constexpr uint32_t kMaxSpillLevel{3};

  /// Maximum number of partition bits per spill level supported by
  /// 'SpillPartitionId'.
  static constexpr uint32_t kMaxPartitionBits{3};

  /// Constructs a default invalid id.
  SpillPartitionId() = default;

  /// Constructs a root spill level id.
  explicit SpillPartitionId(uint32_t partitionNumber);

  /// Constructs a child spill level id, descending from provided 'parent'.
  SpillPartitionId(SpillPartitionId parent, uint32_t partitionNumber);

  bool operator==(const SpillPartitionId& other) const = default;

  /// Customize the compare operator for recursive spilling control. It
  /// ensures the order such that:
  /// 1.  For partitions with the same parent, it orders them by their partition
  /// number, smaller partition numbers in the front.
  /// 2.  For partitions with different levels, it compares them level by level
  /// on condition 1 starting from the root. And if all common ancestors are the
  /// same, putting shallower spill level partitions in the front.
  ///
  /// e.g.: p_0 < p_1 < p_2_0 < p_2_1 < p_2_2 < p_2_3 < p_3
  ///
  /// This creates a hierarchical ordering where child partitions are grouped
  /// after their parent. This ordering ensures that related partitions are
  /// processed together and in a predictable sequence. We put all spill
  /// partitions in an ordered map sorted based on the partition id. The
  /// recursive spilling will advance the partition start bit when go to the
  /// next level of recursive spilling.
  bool operator<(const SpillPartitionId& other) const;

  bool operator>(const SpillPartitionId& other) const {
    return other < *this;
  }

  std::string toString() const;

  uint32_t spillLevel() const;

  /// Returns the partition number of the current spill level.
  uint32_t partitionNumber() const;

  /// Overloaded method that returns the partition number of the requested spill
  /// level.
  uint32_t partitionNumber(uint32_t spillLevel) const;

  uint32_t encodedId() const;

  std::optional<SpillPartitionId> parentId() const;

  bool valid() const;

 private:
  // Default invalid encoded id.
  static constexpr uint32_t kInvalidEncodedId{0xFFFFFFFF};

  // Number of bits to represent one spill level, details see 'encodedId_'.
  static constexpr uint8_t kNumPartitionBits = 3;
  static constexpr uint8_t kSpillLevelBitOffset = 29;

  // Bit mask for the partition number of the spill level, details see
  // 'encodedId_'
  static constexpr uint32_t kPartitionBitMask = 0x00000007;

  // Bit mask for the depth of this spill level, details see 'encodedId_'.
  static constexpr uint32_t kSpillLevelBitMask = 0xE0000000;

  // Encoded hirachical spill partition id. Below shows the layout from the low
  // bits.
  //   <LSB>
  //   (0 ~ 2 bits): Represents the partition number at the 1st level.
  //   (3 ~ 5 bits): Represents the partition number at the 2nd level.
  //   (6 ~ 8 bits): Represents the partition number at the 3rd level.
  //   (9 ~ 11 bits): Represents the partition number at the 4th level.
  //   (12 ~ 28 bits): Unused
  //   (29 ~ 31 bits): Represents the current spill level.
  //   <MSB>
  uint32_t encodedId_{kInvalidEncodedId};
};

inline std::ostream& operator<<(std::ostream& os, SpillPartitionId id) {
  os << id.toString();
  return os;
}
} // namespace facebook::velox::exec

// Adding the custom hash for SpillPartitionId to std::hash to make it usable
// with maps and other standard data structures.
namespace std {
template <>
struct hash<::facebook::velox::exec::SpillPartitionId> {
  uint32_t operator()(
      const ::facebook::velox::exec::SpillPartitionId& id) const {
    return std::hash<uint32_t>()(id.encodedId());
  }
};
} // namespace std

template <>
struct fmt::formatter<facebook::velox::exec::SpillPartitionId>
    : formatter<std::string> {
  auto format(facebook::velox::exec::SpillPartitionId s, format_context& ctx)
      const {
    return formatter<std::string>::format(s.toString(), ctx);
  }
};

namespace facebook::velox::exec {
using SpillPartitionIdSet = folly::F14FastSet<SpillPartitionId>;
using SpillPartitionNumSet = folly::F14FastSet<uint32_t>;
using SpillPartitionWriterSet =
    folly::F14FastMap<SpillPartitionId, std::unique_ptr<SpillWriter>>;

/// Provides the mapping from the computed hash value to 'SpillPartitionId'. It
/// is used to lookup the spill partition id for a spilled row.
class SpillPartitionIdLookup {
 public:
  SpillPartitionIdLookup(
      const SpillPartitionIdSet& spillPartitionIds,
      uint32_t startPartitionBit,
      uint32_t numPartitionBits);

  SpillPartitionId partition(uint64_t hash) const;

 private:
  void generateLookup(
      const SpillPartitionId& id,
      uint32_t startPartitionBit,
      uint32_t numPartitionBits,
      uint32_t numLookupBits);

  void generateLookupHelper(
      const SpillPartitionId& id,
      uint32_t currentBit,
      uint32_t endBit,
      uint64_t lookupBits);

  const uint64_t partitionBitsMask_;

  // The lookup_ array index is the extracted hash value key, value is the
  // corresponding spill partition id. The vector is sized to accommodate all
  // possible combinations of partition bits (2^numLookupBits entries). When a
  // hash value is used to partition, its lookup bits range will be used as
  // index to check against 'lookup_' and find the corresponding id.
  std::vector<SpillPartitionId> lookup_;
};

/// Vectorized partitioning function for spill. The partitioning takes advantage
/// of SpillPartitionIdLookup and performs a fast partitioning for the input
/// vector.
class SpillPartitionFunction {
 public:
  SpillPartitionFunction(
      const SpillPartitionIdLookup& lookup,
      const RowTypePtr& inputType,
      const std::vector<column_index_t>& keyChannels);

  void partition(
      const RowVector& input,
      std::vector<SpillPartitionId>& partitionIds);

 private:
  const SpillPartitionIdLookup lookup_;

  std::vector<std::unique_ptr<VectorHasher>> hashers_;

  // Reusable resources for hashing.
  SelectivityVector rows_;
  raw_vector<uint64_t> hashes_;
};

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

  SpillFiles files() const {
    return files_;
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
  /// 'bufferSize' specifies the read size from the storage. If the file
  /// system supports async read mode, then reader allocates two buffers with
  /// one buffer prefetch ahead. 'spillStats' is provided to collect the spill
  /// stats when reading data from spilled files.
  std::unique_ptr<UnorderedStreamReader<BatchStream>> createUnorderedReader(
      uint64_t bufferSize,
      memory::MemoryPool* pool,
      folly::Synchronized<common::SpillStats>* spillStats);

  /// Invoked to create an ordered stream reader from this spill partition.
  /// The created reader will take the ownership of the spill files.
  /// 'bufferSize' specifies the read size from the storage. If the file
  /// system supports async read mode, then reader allocates two buffers with
  /// one buffer prefetch ahead. 'spillStats' is provided to collect the spill
  /// stats when reading data from spilled files.
  std::unique_ptr<TreeOfLosers<SpillMergeStream>> createOrderedReader(
      uint64_t bufferSize,
      memory::MemoryPool* pool,
      folly::Synchronized<common::SpillStats>* spillStats);

  std::string toString() const;

 private:
  SpillPartitionId id_;
  SpillFiles files_;
  // Counts the total file size in bytes from this spilled partition.
  uint64_t size_{0};
};

using SpillPartitionSet =
    std::map<SpillPartitionId, std::unique_ptr<SpillPartition>>;

/// Used in recursive spilling scenario. Capable of handling spill partition
/// insertion while handling the partition retrieving order such that deeper and
/// lower partition number partitions are retrieved first. It also preserves the
/// entire leaf spill partition set that could be retrieved by calling
/// 'extractAll()' after the set is fully iterated.
class IterableSpillPartitionSet {
 public:
  IterableSpillPartitionSet();

  /// Inserts 'spillPartitionSet', and replaces the common parent of
  /// 'spillPartitionSet' in the iteration list if not root.
  ///
  /// NOTE: All spill partitions in 'spillPartitionSet' must have the same
  /// direct parent if they are not root. And the common parent must be the
  /// same as the partition latest returned by next().
  void insert(SpillPartitionSet&& spillPartitionSet);

  bool hasNext() const;

  SpillPartition next();

  /// Retrieves the entire leaf spill partition set. This method can be called
  /// only after the set is fully iterated. This is used in hash join in mixed
  /// mode spilling.
  const SpillPartitionSet& spillPartitions() const;

  void reset();

  void clear();

 private:
  // Iterator on 'spillPartitions_' pointing to the next returning spill
  // partition.
  SpillPartitionSet::iterator spillPartitionIter_;
  SpillPartitionSet spillPartitions_;
};

/// Represents all spilled data of an operator, e.g. order by or group
/// by. This has one SpillFileList per partition of spill data.
class SpillState {
 public:
  /// Constructs a SpillState. 'type' is the content RowType. 'path' is the
  /// file system path prefix. 'bits' is the hash bit field for partitioning
  /// data between files. This also gives the maximum number of partitions.
  /// 'numSortKeys' is the number of leading columns on which the data is
  /// sorted, 0 if only hash partitioning is used. 'targetFileSize' is the
  /// target size of a single file.  'pool' owns the memory for state and
  /// results.
  SpillState(
      const common::GetSpillDirectoryPathCB& getSpillDirectoryPath,
      const common::UpdateAndCheckSpillLimitCB& updateAndCheckSpillLimitCb,
      const std::string& fileNamePrefix,
      const std::vector<SpillSortKey>& sortingKeys,
      uint64_t targetFileSize,
      uint64_t writeBufferSize,
      common::CompressionKind compressionKind,
      const std::optional<common::PrefixSortConfig>& prefixSortConfig,
      memory::MemoryPool* pool,
      folly::Synchronized<common::SpillStats>* stats,
      const std::string& fileCreateConfig = {});

  static std::vector<SpillSortKey> makeSortingKeys(
      const std::vector<CompareFlags>& compareFlags = {});

  static std::vector<SpillSortKey> makeSortingKeys(
      const std::vector<column_index_t>& indices,
      const std::vector<CompareFlags>& compareFlags);

  /// Indicates if a given 'partition' has been spilled or not.
  bool isPartitionSpilled(const SpillPartitionId& id) const {
    return spilledPartitionIdSet_.contains(id);
  }

  // Sets a partition as spilled.
  void setPartitionSpilled(const SpillPartitionId& id);

  uint64_t targetFileSize() const {
    return targetFileSize_;
  }

  common::CompressionKind compressionKind() const {
    return compressionKind_;
  }

  const std::optional<common::PrefixSortConfig>& prefixSortConfig() const {
    return prefixSortConfig_;
  }

  const std::vector<SpillSortKey>& sortingKeys() const {
    return sortingKeys_;
  }

  bool isAnyPartitionSpilled() const {
    return !spilledPartitionIdSet_.empty();
  }

  /// Appends data to partition with 'id'. The rows given by 'indices' must be
  /// sorted for a sorted spill and must hash to 'partition'. It is safe to call
  /// this on multiple threads if all threads specify a different partition.
  /// Returns the size to append to partition.
  uint64_t appendToPartition(
      const SpillPartitionId& id,
      const RowVectorPtr& rows);

  /// Finishes a sorted run for partition with 'id'. If write is called for
  /// 'partition' again, the data does not have to be sorted relative to the
  /// data written so far.
  void finishFile(const SpillPartitionId& id);

  /// Returns the current number of finished files from a given partition.
  ///
  /// NOTE: the fucntion returns zero if the state has finished or the
  /// partition is not spilled yet.
  size_t numFinishedFiles(const SpillPartitionId& id) const;

  /// Returns the spill file objects from a given 'partition'. The function
  /// returns an empty list if either the partition has not been spilled or
  /// has no spilled data.
  SpillFiles finish(const SpillPartitionId& id);

  /// Returns the spilled partition id set.
  const SpillPartitionIdSet& spilledPartitionIdSet() const;

  /// Returns the spilled file paths from all the partitions.
  std::vector<std::string> testingSpilledFilePaths() const;

  /// Returns the file ids from a given partition id.
  std::vector<uint32_t> testingSpilledFileIds(const SpillPartitionId& id) const;

  /// Returns the set of partition ids that have spilled data.
  SpillPartitionIdSet testingNonEmptySpilledPartitionIdSet() const;

 private:
  // Ensures that the bytes to spill is within the limit of
  // maxSpillBytesPerWrite_ for a given spill write/appendToPartition call.
  // NOTE: the Presto serializer used for spill serialization can't handle more
  // than 2GB data size. Hence we can't spill a vector which exceeds
  // 2GB serialized buffer.
  void validateSpillBytesSize(uint64_t bytes);

  void updateSpilledInputBytes(uint64_t bytes);

  SpillWriter* partitionWriter(const SpillPartitionId& id) const;

  const RowTypePtr type_;

  // A callback function that returns the spill directory path.
  // Implementations can use it to ensure the path exists before returning.
  common::GetSpillDirectoryPathCB getSpillDirPathCb_;

  // Updates the aggregated spill bytes of this query, and throws if exceeds
  // the max spill bytes limit.
  common::UpdateAndCheckSpillLimitCB updateAndCheckSpillLimitCb_;

  // Prefix for spill files.
  const std::string fileNamePrefix_;
  const std::vector<SpillSortKey> sortingKeys_;
  const uint64_t targetFileSize_;
  const uint64_t writeBufferSize_;
  const common::CompressionKind compressionKind_;
  const std::optional<common::PrefixSortConfig> prefixSortConfig_;
  const std::string fileCreateConfig_;
  memory::MemoryPool* const pool_;
  folly::Synchronized<common::SpillStats>* const stats_;

  // A set of spilled partition ids.
  SpillPartitionIdSet spilledPartitionIdSet_;

  // A file writer list for each spilled partition. Only partitions that have
  // started spilling have an entry here. It is made thread safe because
  // concurrent writes may involve concurrent creations of writers.
  folly::Synchronized<SpillPartitionWriterSet> partitionWriters_;
};

/// Returns the partition bit offset of the current spill level of 'id'.
uint8_t partitionBitOffset(
    const SpillPartitionId& id,
    uint8_t startPartitionBitOffset,
    uint8_t numPartitionBits);

/// Generate partition id set from given spill partition set.
SpillPartitionIdSet toSpillPartitionIdSet(
    const SpillPartitionSet& partitionSet);

/// Scoped spill percentage utility that allows user to set the behavior of
/// triggered spill.
/// 'spillPct' indicates the chance of triggering spilling. 100% means spill
/// will always be triggered.
/// 'pools' is a regular expression string used to define the specific memory
/// pools targeted for injecting spilling.
/// 'maxInjections' indicates the max number of actual triggering. e.g. when
/// 'spillPct' is 20 and 'maxInjections' is 10, continuous calls to
/// testingTriggerSpill(poolName) will keep rolling the dice that has a
/// chance of 20% triggering until 10 triggers have been invoked.
class TestScopedSpillInjection {
 public:
  explicit TestScopedSpillInjection(
      int32_t spillPct,
      const std::string& poolRegExp = ".*",
      uint32_t maxInjections = std::numeric_limits<uint32_t>::max());

  ~TestScopedSpillInjection();
};

/// Test utility that returns true if triggered spill is evaluated to happen,
/// false otherwise.
bool testingTriggerSpill(const std::string& poolName = "");

tsan_atomic<uint32_t>& injectedSpillCount();

/// Removes empty partitions from given spill partition set.
void removeEmptyPartitions(SpillPartitionSet& partitionSet);
} // namespace facebook::velox::exec
