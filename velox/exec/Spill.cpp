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
  if (++index_ >= size_) {
    setNextBatch();
  }
}

int32_t SpillMergeStream::compare(const MergeStream& other) const {
  auto& otherStream = static_cast<const SpillMergeStream&>(other);
  auto& children = rowVector_->children();
  auto& otherChildren = otherStream.current().children();
  int32_t key = 0;
  if (sortCompareFlags().empty()) {
    do {
      auto result = children[key]
                        ->compare(
                            otherChildren[key].get(),
                            index_,
                            otherStream.index_,
                            CompareFlags())
                        .value();
      if (result != 0) {
        return result;
      }
    } while (++key < numSortKeys());
  } else {
    do {
      auto result = children[key]
                        ->compare(
                            otherChildren[key].get(),
                            index_,
                            otherStream.index_,
                            sortCompareFlags()[key])
                        .value();
      if (result != 0) {
        return result;
      }
    } while (++key < numSortKeys());
  }
  return 0;
}

SpillState::SpillState(
    const common::GetSpillDirectoryPathCB& getSpillDirPathCb,
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
    const std::string& fileCreateConfig)
    : getSpillDirPathCb_(getSpillDirPathCb),
      updateAndCheckSpillLimitCb_(updateAndCheckSpillLimitCb),
      fileNamePrefix_(fileNamePrefix),
      maxPartitions_(maxPartitions),
      numSortKeys_(numSortKeys),
      sortCompareFlags_(sortCompareFlags),
      targetFileSize_(targetFileSize),
      writeBufferSize_(writeBufferSize),
      compressionKind_(compressionKind),
      fileCreateConfig_(fileCreateConfig),
      pool_(pool),
      stats_(stats),
      partitionWriters_(maxPartitions_) {}

void SpillState::setPartitionSpilled(uint32_t partition) {
  VELOX_DCHECK_LT(partition, maxPartitions_);
  VELOX_DCHECK_LT(spilledPartitionSet_.size(), maxPartitions_);
  VELOX_DCHECK(!spilledPartitionSet_.contains(partition));
  spilledPartitionSet_.insert(partition);
  ++stats_->wlock()->spilledPartitions;
  common::incrementGlobalSpilledPartitionStats();
}

void SpillState::updateSpilledInputBytes(uint64_t bytes) {
  auto statsLocked = stats_->wlock();
  statsLocked->spilledInputBytes += bytes;
  common::updateGlobalSpillMemoryBytes(bytes);
}

uint64_t SpillState::appendToPartition(
    uint32_t partition,
    const RowVectorPtr& rows) {
  VELOX_CHECK(
      isPartitionSpilled(partition), "Partition {} is not spilled", partition);

  TestValue::adjust(
      "facebook::velox::exec::SpillState::appendToPartition", this);

  VELOX_CHECK_NOT_NULL(
      getSpillDirPathCb_, "Spill directory callback not specified.");
  auto spillDir = getSpillDirPathCb_();
  VELOX_CHECK(!spillDir.empty(), "Spill directory does not exist");
  // Ensure that partition exist before writing.
  if (partitionWriters_.at(partition) == nullptr) {
    partitionWriters_[partition] = std::make_unique<SpillWriter>(
        std::static_pointer_cast<const RowType>(rows->type()),
        numSortKeys_,
        sortCompareFlags_,
        compressionKind_,
        fmt::format("{}/{}-spill-{}", spillDir, fileNamePrefix_, partition),
        targetFileSize_,
        writeBufferSize_,
        fileCreateConfig_,
        updateAndCheckSpillLimitCb_,
        pool_,
        stats_);
  }

  updateSpilledInputBytes(rows->estimateFlatSize());

  IndexRange range{0, rows->size()};
  return partitionWriters_[partition]->write(
      rows, folly::Range<IndexRange*>(&range, 1));
}

SpillWriter* SpillState::partitionWriter(uint32_t partition) const {
  VELOX_DCHECK(isPartitionSpilled(partition));
  return partitionWriters_[partition].get();
}

void SpillState::finishFile(uint32_t partition) {
  auto* writer = partitionWriter(partition);
  if (writer == nullptr) {
    return;
  }
  writer->finishFile();
}

size_t SpillState::numFinishedFiles(uint32_t partition) const {
  if (!isPartitionSpilled(partition)) {
    return 0;
  }
  const auto* writer = partitionWriter(partition);
  if (writer == nullptr) {
    return 0;
  }
  return writer->numFinishedFiles();
}

SpillFiles SpillState::finish(uint32_t partition) {
  auto* writer = partitionWriter(partition);
  if (writer == nullptr) {
    return {};
  }
  return writer->finish();
}

const SpillPartitionNumSet& SpillState::spilledPartitionSet() const {
  return spilledPartitionSet_;
}

std::vector<std::string> SpillState::testingSpilledFilePaths() const {
  std::vector<std::string> spilledFiles;
  for (const auto& writer : partitionWriters_) {
    if (writer != nullptr) {
      const auto partitionSpilledFiles = writer->testingSpilledFilePaths();
      spilledFiles.insert(
          spilledFiles.end(),
          partitionSpilledFiles.begin(),
          partitionSpilledFiles.end());
    }
  }
  return spilledFiles;
}

std::vector<uint32_t> SpillState::testingSpilledFileIds(
    int32_t partitionNum) const {
  return partitionWriters_[partitionNum]->testingSpilledFileIds();
}

SpillPartitionNumSet SpillState::testingNonEmptySpilledPartitionSet() const {
  SpillPartitionNumSet partitionSet;
  for (uint32_t partition = 0; partition < maxPartitions_; ++partition) {
    if (partitionWriters_[partition] != nullptr) {
      partitionSet.insert(partition);
    }
  }
  return partitionSet;
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

uint32_t FileSpillMergeStream::id() const {
  return spillFile_->id();
}

void FileSpillMergeStream::nextBatch() {
  index_ = 0;
  if (!spillFile_->nextBatch(rowVector_)) {
    size_ = 0;
    return;
  }
  size_ = rowVector_->size();
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
