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

#include "velox/exec/Spill.h"
#include "velox/common/base/RuntimeMetrics.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/serializers/PrestoSerializer.h"

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::exec {
void SpillMergeStream::pop() {
  VELOX_CHECK(!closed_);
  if (++index_ >= size_) {
    setNextBatch();
  }
}

int32_t SpillMergeStream::compare(const MergeStream& other) const {
  VELOX_CHECK(!closed_);
  const auto& otherStream = static_cast<const SpillMergeStream&>(other);
  const auto& children = rowVector_->children();
  const auto& otherChildren = otherStream.current().children();
  for (const auto& [key, compareFlags] : sortingKeys()) {
    const auto result = children[key]
                            ->compare(
                                otherChildren[key].get(),
                                index_,
                                otherStream.index_,
                                compareFlags)
                            .value();
    if (result != 0) {
      return result;
    }
  }
  return 0;
}

void SpillMergeStream::close() {
  VELOX_CHECK(!closed_);
  closed_ = true;
  rowVector_.reset();
  decoded_.clear();
  rows_.resize(0);
  index_ = 0;
  size_ = 0;
}

SpillState::SpillState(
    const common::GetSpillDirectoryPathCB& getSpillDirPathCb,
    const common::UpdateAndCheckSpillLimitCB& updateAndCheckSpillLimitCb,
    const std::string& fileNamePrefix,
    const std::vector<SpillSortKey>& sortingKeys,
    uint64_t targetFileSize,
    uint64_t writeBufferSize,
    common::CompressionKind compressionKind,
    const std::optional<common::PrefixSortConfig>& prefixSortConfig,
    memory::MemoryPool* pool,
    folly::Synchronized<common::SpillStats>* stats,
    const std::string& fileCreateConfig)
    : getSpillDirPathCb_(getSpillDirPathCb),
      updateAndCheckSpillLimitCb_(updateAndCheckSpillLimitCb),
      fileNamePrefix_(fileNamePrefix),
      sortingKeys_(sortingKeys),
      targetFileSize_(targetFileSize),
      writeBufferSize_(writeBufferSize),
      compressionKind_(compressionKind),
      prefixSortConfig_(prefixSortConfig),
      fileCreateConfig_(fileCreateConfig),
      pool_(pool),
      stats_(stats) {}

std::vector<SpillSortKey> SpillState::makeSortingKeys(
    const std::vector<CompareFlags>& compareFlags) {
  std::vector<SpillSortKey> sortingKeys;
  sortingKeys.reserve(compareFlags.size());
  for (column_index_t i = 0; i < compareFlags.size(); ++i) {
    sortingKeys.emplace_back(i, compareFlags[i]);
  }
  return sortingKeys;
}

std::vector<SpillSortKey> SpillState::makeSortingKeys(
    const std::vector<column_index_t>& indices,
    const std::vector<CompareFlags>& compareFlags) {
  VELOX_CHECK(!indices.empty());
  VELOX_CHECK_EQ(indices.size(), compareFlags.size());
  std::vector<SpillSortKey> sortingKeys;
  sortingKeys.reserve(indices.size());
  for (auto i = 0; i < indices.size(); i++) {
    sortingKeys.emplace_back(indices[i], compareFlags[i]);
  }
  return sortingKeys;
}

void SpillState::setPartitionSpilled(const SpillPartitionId& id) {
  VELOX_DCHECK(!spilledPartitionIdSet_.contains(id));
  spilledPartitionIdSet_.emplace(id);
  ++stats_->wlock()->spilledPartitions;
  common::incrementGlobalSpilledPartitionStats();
}

/*static*/
void SpillState::validateSpillBytesSize(uint64_t bytes) {
  static constexpr uint64_t kMaxSpillBytesPerWrite =
      std::numeric_limits<int32_t>::max();
  if (bytes >= kMaxSpillBytesPerWrite) {
    VELOX_GENERIC_SPILL_FAILURE(fmt::format(
        "Spill bytes will overflow. Bytes {}, kMaxSpillBytesPerWrite: {}",
        bytes,
        kMaxSpillBytesPerWrite));
  }
}

void SpillState::updateSpilledInputBytes(uint64_t bytes) {
  auto statsLocked = stats_->wlock();
  statsLocked->spilledInputBytes += bytes;
  common::updateGlobalSpillMemoryBytes(bytes);
}

uint64_t SpillState::appendToPartition(
    const SpillPartitionId& id,
    const RowVectorPtr& rows) {
  VELOX_CHECK(
      isPartitionSpilled(id), "Partition {} is not spilled", id.toString());

  TestValue::adjust(
      "facebook::velox::exec::SpillState::appendToPartition", this);

  VELOX_CHECK_NOT_NULL(
      getSpillDirPathCb_, "Spill directory callback not specified.");
  auto spillDir = getSpillDirPathCb_();
  VELOX_CHECK(!spillDir.empty(), "Spill directory does not exist");

  partitionWriters_.withWLock([&](auto& lockedWriters) {
    // Ensure that partition exist before writing.
    if (!lockedWriters.contains(id)) {
      lockedWriters.emplace(
          id,
          std::make_unique<SpillWriter>(
              std::static_pointer_cast<const RowType>(rows->type()),
              sortingKeys_,
              compressionKind_,
              fmt::format(
                  "{}/{}-spill-{}", spillDir, fileNamePrefix_, id.encodedId()),
              targetFileSize_,
              writeBufferSize_,
              fileCreateConfig_,
              updateAndCheckSpillLimitCb_,
              pool_,
              stats_));
    }
  });

  const uint64_t bytes = rows->estimateFlatSize();
  validateSpillBytesSize(bytes);
  updateSpilledInputBytes(bytes);

  IndexRange range{0, rows->size()};
  return partitionWriter(id)->write(rows, folly::Range<IndexRange*>(&range, 1));
}

SpillWriter* SpillState::partitionWriter(const SpillPartitionId& id) const {
  VELOX_DCHECK(isPartitionSpilled(id));
  auto partitionWriters = partitionWriters_.rlock();
  return partitionWriters->contains(id) ? partitionWriters->at(id).get()
                                        : nullptr;
}

void SpillState::finishFile(const SpillPartitionId& id) {
  auto* writer = partitionWriter(id);
  if (writer == nullptr) {
    return;
  }
  writer->finishFile();
}

size_t SpillState::numFinishedFiles(const SpillPartitionId& id) const {
  if (!isPartitionSpilled(id)) {
    return 0;
  }
  const auto* writer = partitionWriter(id);
  if (writer == nullptr) {
    return 0;
  }
  return writer->numFinishedFiles();
}

SpillFiles SpillState::finish(const SpillPartitionId& id) {
  auto* writer = partitionWriter(id);
  if (writer == nullptr) {
    return {};
  }
  return writer->finish();
}

const SpillPartitionIdSet& SpillState::spilledPartitionIdSet() const {
  return spilledPartitionIdSet_;
}

std::vector<std::string> SpillState::testingSpilledFilePaths() const {
  std::vector<std::string> spilledFiles;
  partitionWriters_.withRLock([&](const auto& partitionWriters) {
    for (const auto& [id, writer] : partitionWriters) {
      const auto partitionSpilledFiles = writer->testingSpilledFilePaths();
      spilledFiles.insert(
          spilledFiles.end(),
          partitionSpilledFiles.begin(),
          partitionSpilledFiles.end());
    }
  });
  return spilledFiles;
}

std::vector<uint32_t> SpillState::testingSpilledFileIds(
    const SpillPartitionId& id) const {
  auto partitionWriters = partitionWriters_.rlock();
  VELOX_CHECK(partitionWriters->contains(id));
  return partitionWriters->at(id)->testingSpilledFileIds();
}

SpillPartitionIdSet SpillState::testingNonEmptySpilledPartitionIdSet() const {
  SpillPartitionIdSet partitionIdSet;
  partitionWriters_.withRLock([&](const auto& partitionWriters) {
    for (const auto& [id, writer] : partitionWriters) {
      partitionIdSet.emplace(id);
    }
  });
  return partitionIdSet;
}

std::vector<std::unique_ptr<SpillPartition>> SpillPartition::split(
    int numShards) {
  std::vector<std::unique_ptr<SpillPartition>> shards(numShards);
  const auto numFilesPerShard = files_.size() / numShards;
  int32_t numRemainingFiles = files_.size() % numShards;
  int fileIdx{0};
  for (int shard = 0; shard < numShards; ++shard) {
    SpillFiles files;
    auto numFiles = numFilesPerShard;
    if (numRemainingFiles-- > 0) {
      ++numFiles;
    }
    files.reserve(numFiles);
    while (files.size() < numFiles) {
      files.push_back(std::move(files_[fileIdx++]));
    }
    shards[shard] = std::make_unique<SpillPartition>(id_, std::move(files));
  }
  VELOX_CHECK_EQ(fileIdx, files_.size());
  files_.clear();
  return shards;
}

std::string SpillPartition::toString() const {
  return fmt::format(
      "SPILLED PARTITION[ID:{} FILES:{} SIZE:{}]",
      id_.toString(),
      files_.size(),
      succinctBytes(size_));
}

std::unique_ptr<UnorderedStreamReader<BatchStream>>
SpillPartition::createUnorderedReader(
    uint64_t bufferSize,
    memory::MemoryPool* pool,
    folly::Synchronized<common::SpillStats>* spillStats) {
  VELOX_CHECK_NOT_NULL(pool);
  std::vector<std::unique_ptr<BatchStream>> streams;
  streams.reserve(files_.size());
  for (auto& fileInfo : files_) {
    streams.push_back(FileSpillBatchStream::create(
        SpillReadFile::create(fileInfo, bufferSize, pool, spillStats)));
  }
  files_.clear();
  return std::make_unique<UnorderedStreamReader<BatchStream>>(
      std::move(streams));
}

std::unique_ptr<TreeOfLosers<SpillMergeStream>>
SpillPartition::createOrderedReader(
    uint64_t bufferSize,
    memory::MemoryPool* pool,
    folly::Synchronized<common::SpillStats>* spillStats) {
  std::vector<std::unique_ptr<SpillMergeStream>> streams;
  streams.reserve(files_.size());
  for (auto& fileInfo : files_) {
    streams.push_back(FileSpillMergeStream::create(
        SpillReadFile::create(fileInfo, bufferSize, pool, spillStats)));
  }
  files_.clear();
  // Check if the partition is empty or not.
  if (FOLLY_UNLIKELY(streams.empty())) {
    return nullptr;
  }
  return std::make_unique<TreeOfLosers<SpillMergeStream>>(std::move(streams));
}

IterableSpillPartitionSet::IterableSpillPartitionSet() {
  spillPartitionIter_ = spillPartitions_.begin();
}

void IterableSpillPartitionSet::insert(SpillPartitionSet&& spillPartitionSet) {
  VELOX_CHECK(
      !spillPartitionSet.empty(),
      "Inserted spill partition set must not be empty.");

  const auto parentId = spillPartitionSet.begin()->first.parentId();
  if (!spillPartitions_.empty()) {
    VELOX_CHECK(parentId.has_value());
    VELOX_CHECK(spillPartitionIter_ != spillPartitions_.begin());
    VELOX_CHECK_EQ(
        std::prev(spillPartitionIter_)->first,
        parentId.value(),
        "Partition set does not have the same parent.");
    spillPartitions_.erase(std::prev(spillPartitionIter_));
  } else {
    VELOX_CHECK(!parentId.has_value());
  }

  for (const auto& [id, partition] : spillPartitionSet) {
    VELOX_CHECK_EQ(
        id.parentId().value_or(SpillPartitionId()),
        parentId.value_or(SpillPartitionId()));
    spillPartitions_.emplace(id, std::make_unique<SpillPartition>(*partition));
  }
  spillPartitionIter_ = spillPartitions_.find(spillPartitionSet.begin()->first);
}

bool IterableSpillPartitionSet::hasNext() const {
  return spillPartitionIter_ != spillPartitions_.end();
}

SpillPartition IterableSpillPartitionSet::next() {
  VELOX_CHECK(hasNext(), "No more spill partitions to read.");
  return *((spillPartitionIter_++)->second);
}

const SpillPartitionSet& IterableSpillPartitionSet::spillPartitions() const {
  VELOX_CHECK(
      !hasNext(),
      "Spill partitions can only be extracted out after entire set is read.");
  return spillPartitions_;
}

void IterableSpillPartitionSet::reset() {
  spillPartitionIter_ = spillPartitions_.begin();
}

void IterableSpillPartitionSet::clear() {
  spillPartitions_.clear();
  spillPartitionIter_ = spillPartitions_.begin();
}

uint32_t FileSpillMergeStream::id() const {
  VELOX_CHECK(!closed_);
  return spillFile_->id();
}

void FileSpillMergeStream::nextBatch() {
  VELOX_CHECK(!closed_);
  index_ = 0;
  if (!spillFile_->nextBatch(rowVector_)) {
    size_ = 0;
    close();
    return;
  }
  size_ = rowVector_->size();
}

void FileSpillMergeStream::close() {
  VELOX_CHECK(!closed_);
  SpillMergeStream::close();
  spillFile_.reset();
}

std::unique_ptr<SpillMergeStream> ConcatFilesSpillMergeStream::create(
    uint32_t id,
    std::vector<std::unique_ptr<SpillReadFile>> spillFiles) {
  auto spillStream = std::unique_ptr<ConcatFilesSpillMergeStream>(
      new ConcatFilesSpillMergeStream(id, std::move(spillFiles)));
  spillStream->nextBatch();
  return spillStream;
}

uint32_t ConcatFilesSpillMergeStream::id() const {
  return id_;
}

void ConcatFilesSpillMergeStream::nextBatch() {
  VELOX_CHECK(!closed_);
  index_ = 0;
  for (; fileIndex_ < spillFiles_.size(); ++fileIndex_) {
    VELOX_CHECK_NOT_NULL(spillFiles_[fileIndex_]);
    if (spillFiles_[fileIndex_]->nextBatch(rowVector_)) {
      VELOX_CHECK_NOT_NULL(rowVector_);
      size_ = rowVector_->size();
      return;
    }
    spillFiles_[fileIndex_].reset();
  }
  size_ = 0;
  close();
}

void ConcatFilesSpillMergeStream::close() {
  VELOX_CHECK(!closed_);
  SpillMergeStream::close();
  spillFiles_.clear();
}

const std::vector<SpillSortKey>& ConcatFilesSpillMergeStream::sortingKeys()
    const {
  VELOX_CHECK(!closed_);
  return spillFiles_[fileIndex_]->sortingKeys();
}

std::unique_ptr<BatchStream> ConcatFilesSpillBatchStream::create(
    std::vector<std::unique_ptr<SpillReadFile>> spillFiles) {
  auto* spillStream = new ConcatFilesSpillBatchStream(std::move(spillFiles));
  return std::unique_ptr<BatchStream>(spillStream);
}

bool ConcatFilesSpillBatchStream::nextBatch(RowVectorPtr& batch) {
  VELOX_CHECK_NULL(batch);
  VELOX_CHECK(!atEnd_);
  for (; fileIndex_ < spillFiles_.size(); ++fileIndex_) {
    VELOX_CHECK_NOT_NULL(spillFiles_[fileIndex_]);
    if (spillFiles_[fileIndex_]->nextBatch(batch)) {
      VELOX_CHECK_NOT_NULL(batch);
      return true;
    }
    spillFiles_[fileIndex_].reset();
  }
  spillFiles_.clear();
  atEnd_ = true;
  return false;
}

SpillPartitionId::SpillPartitionId(uint32_t partitionNumber)
    : encodedId_(partitionNumber) {
  if (FOLLY_UNLIKELY(partitionNumber >= (1 << kMaxPartitionBits))) {
    VELOX_FAIL(fmt::format(
        "Partition number {} exceeds max partition number {}",
        partitionNumber,
        1 << kMaxPartitionBits));
  }
}

SpillPartitionId::SpillPartitionId(
    SpillPartitionId parent,
    uint32_t partitionNumber) {
  const auto childSpillLevel = parent.spillLevel() + 1;
  if (FOLLY_UNLIKELY(childSpillLevel > kMaxSpillLevel)) {
    VELOX_FAIL(fmt::format(
        "Spill level {} exceeds max spill level {}",
        childSpillLevel,
        kMaxSpillLevel));
  }
  encodedId_ = parent.encodedId_;
  encodedId_ = encodedId_ & ~kSpillLevelBitMask;

  // Set spill levels.
  encodedId_ |= childSpillLevel << kSpillLevelBitOffset;

  // Set partition number.
  encodedId_ |= partitionNumber << (kNumPartitionBits * childSpillLevel);
}

bool SpillPartitionId::operator==(const SpillPartitionId& other) const {
  return encodedId_ == other.encodedId_;
}

bool SpillPartitionId::operator!=(const SpillPartitionId& other) const {
  return !(*this == other);
}

bool SpillPartitionId::operator<(const SpillPartitionId& other) const {
  for (auto i = 0; i <= std::min(spillLevel(), other.spillLevel()); ++i) {
    const auto selfPartitionNum = partitionNumber(i);
    const auto otherPartitionNum = other.partitionNumber(i);
    if (selfPartitionNum == otherPartitionNum) {
      continue;
    }
    return selfPartitionNum < otherPartitionNum;
  }
  return spillLevel() < other.spillLevel();
}

std::string SpillPartitionId::toString() const {
  std::stringstream ss;
  if (!valid()) {
    return "[invalid]";
  }
  ss << "[levels: " << (spillLevel() + 1) << ", partitions: [";
  for (auto i = 0; i <= spillLevel(); ++i) {
    ss << partitionNumber(i);
    if (i < spillLevel()) {
      ss << ",";
    }
  }
  ss << "]]";
  return ss.str();
}

uint32_t SpillPartitionId::spillLevel() const {
  return bits::extractBits(encodedId_, kSpillLevelBitMask);
}

uint32_t SpillPartitionId::partitionNumber() const {
  return bits::extractBits(
      encodedId_, kPartitionBitMask << (spillLevel() * kNumPartitionBits));
}

/// Overloaded method that returns the partition number of the requested spill
/// level.
uint32_t SpillPartitionId::partitionNumber(uint32_t level) const {
  const auto leafLevel = spillLevel();
  if (FOLLY_UNLIKELY(level > leafLevel)) {
    VELOX_FAIL(
        "spillLevel needs to be equal or smaller than leaf level {} vs {}",
        level,
        leafLevel);
  }
  return bits::extractBits(
      encodedId_, kPartitionBitMask << (level * kNumPartitionBits));
}

uint32_t SpillPartitionId::encodedId() const {
  return encodedId_;
}

std::optional<SpillPartitionId> SpillPartitionId::parentId() const {
  VELOX_CHECK(valid());
  if (spillLevel() == 0) {
    return std::nullopt;
  }

  SpillPartitionId parent;
  parent.encodedId_ = encodedId_;

  // Clear the current level's partition number bits
  const auto currentLevel = spillLevel();
  const auto currentLevelBitOffset = currentLevel * kNumPartitionBits;
  parent.encodedId_ &= ~(kPartitionBitMask << currentLevelBitOffset);

  // Decrement the spill level
  parent.encodedId_ &= ~kSpillLevelBitMask;
  parent.encodedId_ |= (currentLevel - 1) << kSpillLevelBitOffset;

  return parent;
}

bool SpillPartitionId::valid() const {
  return encodedId_ != kInvalidEncodedId;
}

namespace {
uint32_t numSpillLevels(const SpillPartitionIdSet& spillPartitionIds) {
  VELOX_CHECK(!spillPartitionIds.empty());
  uint32_t maxSpillLevel{0};
  for (const auto& id : spillPartitionIds) {
    maxSpillLevel = std::max(maxSpillLevel, id.spillLevel());
  }
  return maxSpillLevel + 1;
}
} // namespace

SpillPartitionIdLookup::SpillPartitionIdLookup(
    const SpillPartitionIdSet& spillPartitionIds,
    uint32_t startPartitionBit,
    uint32_t numPartitionBits)
    : partitionBitsMask_(
          bits::lowMask(numPartitionBits * (numSpillLevels(spillPartitionIds)))
          << startPartitionBit) {
  const auto numLookupBits =
      (numSpillLevels(spillPartitionIds)) * numPartitionBits;
  VELOX_CHECK_LT(
      startPartitionBit,
      sizeof(uint64_t) * 8 - numLookupBits,
      "Insufficient lookup bits.");
  lookup_.resize(1UL << numLookupBits, SpillPartitionId());

  for (const auto& id : spillPartitionIds) {
    generateLookup(id, startPartitionBit, numPartitionBits, numLookupBits);
  }
}

void SpillPartitionIdLookup::generateLookup(
    const SpillPartitionId& id,
    uint32_t startPartitionBit,
    uint32_t numPartitionBits,
    uint32_t numLookupBits) {
  // Enumerate all possible numbers for enumeration range and combine with spill
  // level partition bits range to form the lookup keys.
  //
  // |..MSB..|...enumeration range...|...partition bits range...|..LSB..|
  //
  // Calculate the range of bits that need to be enumerated [start, end).
  const auto enumStartBit =
      startPartitionBit + (id.spillLevel() + 1) * numPartitionBits;
  const auto enumEndBit = startPartitionBit + numLookupBits;

  // Calculate the spill level partition bits range bits which are fixed.
  uint64_t lookupBits{0};
  for (auto i = 0; i <= id.spillLevel(); ++i) {
    const auto partitionNum = id.partitionNumber(i);
    VELOX_CHECK_LT(
        partitionNum,
        1UL << numPartitionBits,
        "Partition number exceeds max partition number");
    lookupBits |= static_cast<uint64_t>(partitionNum) << (i * numPartitionBits);
  }
  lookupBits = lookupBits << startPartitionBit;

  // Start building from fixed bits and enumerate the rest.
  generateLookupHelper(id, enumStartBit, enumEndBit, lookupBits);
}

void SpillPartitionIdLookup::generateLookupHelper(
    const SpillPartitionId& id,
    uint32_t currentBit,
    uint32_t endBit,
    uint64_t lookupBits) {
  if (currentBit == endBit) {
    const auto index = bits::extractBits(lookupBits, partitionBitsMask_);
    VELOX_CHECK(
        !lookup_[index].valid(),
        "Duplicated lookup key {}, likely due to non-leaf spill partition id used "
        "to construct lookup.",
        id.toString());
    lookup_[index] = id;
    return;
  }
  generateLookupHelper(
      id, currentBit + 1, endBit, lookupBits | (1UL << currentBit));
  generateLookupHelper(id, currentBit + 1, endBit, lookupBits);
}

SpillPartitionId SpillPartitionIdLookup::partition(uint64_t hash) const {
  return lookup_[bits::extractBits(hash, partitionBitsMask_)];
}

SpillPartitionFunction::SpillPartitionFunction(
    const SpillPartitionIdLookup& lookup,
    const RowTypePtr& inputType,
    const std::vector<column_index_t>& keyChannels)
    : lookup_(lookup) {
  VELOX_CHECK(!keyChannels.empty(), "Key channels must not be empty.");
  hashers_.reserve(keyChannels.size());
  for (const auto channel : keyChannels) {
    VELOX_CHECK_NE(channel, kConstantChannel);
    hashers_.emplace_back(
        VectorHasher::create(inputType->childAt(channel), channel));
  }
}

void SpillPartitionFunction::partition(
    const RowVector& input,
    std::vector<SpillPartitionId>& partitionIds) {
  const auto size = input.size();
  rows_.resize(size);
  rows_.setAll();

  hashes_.resize(size);
  for (auto i = 0; i < hashers_.size(); ++i) {
    auto& hasher = hashers_[i];
    hashers_[i]->decode(*input.childAt(hasher->channel()), rows_);
    hashers_[i]->hash(rows_, i > 0, hashes_);
  }

  partitionIds.resize(size);

  for (auto i = 0; i < size; ++i) {
    partitionIds[i] = lookup_.partition(hashes_[i]);
  }
}

uint8_t partitionBitOffset(
    const SpillPartitionId& id,
    uint8_t startPartitionBitOffset,
    uint8_t numPartitionBits) {
  const auto partitionOffset =
      startPartitionBitOffset + numPartitionBits * id.spillLevel();
  VELOX_CHECK_LE(startPartitionBitOffset, partitionOffset);
  return partitionOffset;
}

SpillPartitionIdSet toSpillPartitionIdSet(
    const SpillPartitionSet& partitionSet) {
  SpillPartitionIdSet partitionIdSet;
  partitionIdSet.reserve(partitionSet.size());
  for (auto& partitionEntry : partitionSet) {
    partitionIdSet.insert(partitionEntry.first);
  }
  return partitionIdSet;
}

namespace {
tsan_atomic<uint32_t>& maxSpillInjections() {
  static tsan_atomic<uint32_t> maxInjections{0};
  return maxInjections;
}

tsan_atomic<uint32_t>& testingSpillPct() {
  static tsan_atomic<uint32_t> spillPct{0};
  return spillPct;
}

std::string& testingSpillPoolRegExp() {
  static std::string spillPoolRegExp{".*"};
  return spillPoolRegExp;
}
} // namespace

TestScopedSpillInjection::TestScopedSpillInjection(
    int32_t spillPct,
    const std::string& poolRegExp,
    uint32_t maxInjections) {
  VELOX_CHECK_EQ(injectedSpillCount(), 0);
  testingSpillPct() = spillPct;
  testingSpillPoolRegExp() = poolRegExp;
  maxSpillInjections() = maxInjections;
  injectedSpillCount() = 0;
}

TestScopedSpillInjection::~TestScopedSpillInjection() {
  testingSpillPct() = 0;
  injectedSpillCount() = 0;
  testingSpillPoolRegExp() = ".*";
  maxSpillInjections() = 0;
}

tsan_atomic<uint32_t>& injectedSpillCount() {
  static tsan_atomic<uint32_t> injectedCount{0};
  return injectedCount;
}

bool testingTriggerSpill(const std::string& pool) {
  // Put cheap check first to reduce CPU consumption in release code.
  if (testingSpillPct() <= 0) {
    return false;
  }

  if (injectedSpillCount() >= maxSpillInjections()) {
    return false;
  }

  if (folly::Random::rand32() % 100 > testingSpillPct()) {
    return false;
  }

  if (!pool.empty() && !RE2::FullMatch(pool, testingSpillPoolRegExp())) {
    return false;
  }

  ++injectedSpillCount();
  return true;
}

void removeEmptyPartitions(SpillPartitionSet& partitionSet) {
  auto it = partitionSet.begin();
  while (it != partitionSet.end()) {
    if (it->second->numFiles() > 0) {
      ++it;
    } else {
      it = partitionSet.erase(it);
    }
  }
}
} // namespace facebook::velox::exec
