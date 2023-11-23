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
namespace {
// Spilling currently uses the default PrestoSerializer which by default
// serializes timestamp with millisecond precision to maintain compatibility
// with presto. Since velox's native timestamp implementation supports
// nanosecond precision, we use this serde option to ensure the serializer
// preserves precision.
static const bool kDefaultUseLosslessTimestamp = true;
} // namespace
std::atomic<int32_t> SpillFile::ordinalCounter_;

void SpillInput::next(bool /*throwIfPastEnd*/) {
  int32_t readBytes = std::min(input_->size() - offset_, buffer_->capacity());
  VELOX_CHECK_LT(0, readBytes, "Reading past end of spill file");
  setRange({buffer_->asMutable<uint8_t>(), readBytes, 0});
  input_->pread(offset_, readBytes, buffer_->asMutable<char>());
  offset_ += readBytes;
}

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
    } while (++key < numSortingKeys());
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
    } while (++key < numSortingKeys());
  }
  return 0;
}

SpillFile::SpillFile(
    uint32_t id,
    RowTypePtr type,
    int32_t numSortingKeys,
    const std::vector<CompareFlags>& sortCompareFlags,
    const std::string& path,
    common::CompressionKind compressionKind,
    memory::MemoryPool* pool,
    const std::unordered_map<std::string, std::string>& writeFileOptions)
    : id_(id),
      type_(std::move(type)),
      numSortingKeys_(numSortingKeys),
      sortCompareFlags_(sortCompareFlags),
      ordinal_(ordinalCounter_++),
      path_(fmt::format("{}-{}", path, ordinal_)),
      compressionKind_(compressionKind),
      writeFileOptions_(filesystems::FileOptions{writeFileOptions, nullptr}),
      pool_(pool) {
  // NOTE: if the spilling operator has specified the sort comparison flags,
  // then it must match the number of sorting keys.
  VELOX_CHECK(
      sortCompareFlags_.empty() || sortCompareFlags_.size() == numSortingKeys_);
}

WriteFile& SpillFile::output() {
  if (!output_) {
    auto fs = filesystems::getFileSystem(path_, nullptr);
    output_ = fs->openFileForWrite(path_, writeFileOptions_);
  }
  return *output_;
}

void SpillFile::startRead() {
  constexpr uint64_t kMaxReadBufferSize =
      (1 << 20) - AlignedBuffer::kPaddedSize; // 1MB - padding.
  VELOX_CHECK(!output_);
  VELOX_CHECK(!input_);
  auto fs = filesystems::getFileSystem(path_, nullptr);
  auto file = fs->openFileForRead(path_);
  auto buffer = AlignedBuffer::allocate<char>(
      std::min<uint64_t>(fileSize_, kMaxReadBufferSize), pool_);
  input_ = std::make_unique<SpillInput>(std::move(file), std::move(buffer));
}

bool SpillFile::nextBatch(RowVectorPtr& rowVector) {
  if (input_->atEnd()) {
    return false;
  }
  serializer::presto::PrestoVectorSerde::PrestoOptions options = {
      kDefaultUseLosslessTimestamp, compressionKind_};
  VectorStreamGroup::read(input_.get(), pool_, type_, &rowVector, &options);
  return true;
}

SpillFileList::SpillFileList(
    const RowTypePtr& type,
    int32_t numSortingKeys,
    const std::vector<CompareFlags>& sortCompareFlags,
    const std::string& path,
    uint64_t targetFileSize,
    uint64_t writeBufferSize,
    common::CompressionKind compressionKind,
    memory::MemoryPool* pool,
    folly::Synchronized<common::SpillStats>* stats,
    const std::unordered_map<std::string, std::string>& writeFileOptions)
    : type_(type),
      numSortingKeys_(numSortingKeys),
      sortCompareFlags_(sortCompareFlags),
      path_(path),
      targetFileSize_(targetFileSize),
      writeBufferSize_(writeBufferSize),
      compressionKind_(compressionKind),
      writeFileOptions_(writeFileOptions),
      pool_(pool),
      stats_(stats) {
  // NOTE: if the associated spilling operator has specified the sort
  // comparison flags, then it must match the number of sorting keys.
  VELOX_CHECK(
      sortCompareFlags_.empty() || sortCompareFlags_.size() == numSortingKeys_);
}

WriteFile& SpillFileList::currentOutput() {
  if (files_.empty() || !files_.back()->isWritable() ||
      files_.back()->size() > targetFileSize_) {
    if (!files_.empty() && files_.back()->isWritable()) {
      files_.back()->finishWrite();
      updateSpilledFiles(files_.back()->size());
    }
    files_.push_back(std::make_unique<SpillFile>(
        nextFileId_++,
        type_,
        numSortingKeys_,
        sortCompareFlags_,
        fmt::format("{}-{}", path_, files_.size()),
        compressionKind_,
        pool_,
        writeFileOptions_));
  }
  return files_.back()->output();
}

uint64_t SpillFileList::flush() {
  uint64_t writtenBytes = 0;
  if (batch_) {
    IOBufOutputStream out(
        *pool_, nullptr, std::max<int64_t>(64 * 1024, batch_->size()));
    uint64_t flushTimeUs{0};
    {
      MicrosecondTimer timer(&flushTimeUs);
      batch_->flush(&out);
    }

    batch_.reset();
    auto iobuf = out.getIOBuf();
    auto& file = currentOutput();
    uint64_t writeTimeUs{0};
    uint32_t numDiskWrites{0};
    {
      MicrosecondTimer timer(&writeTimeUs);
      for (auto& range : *iobuf) {
        ++numDiskWrites;
        file.append(std::string_view(
            reinterpret_cast<const char*>(range.data()), range.size()));
        writtenBytes += range.size();
      }
    }
    updateWriteStats(numDiskWrites, writtenBytes, flushTimeUs, writeTimeUs);
  }
  return writtenBytes;
}

uint64_t SpillFileList::write(
    const RowVectorPtr& rows,
    const folly::Range<IndexRange*>& indices) {
  uint64_t timeUs{0};
  {
    MicrosecondTimer timer(&timeUs);
    if (batch_ == nullptr) {
      serializer::presto::PrestoVectorSerde::PrestoOptions options = {
          kDefaultUseLosslessTimestamp, compressionKind_};
      batch_ = std::make_unique<VectorStreamGroup>(pool_);
      batch_->createStreamTree(
          std::static_pointer_cast<const RowType>(rows->type()),
          1000,
          &options);
    }
    batch_->append(rows, indices);
  }
  updateAppendStats(rows->size(), timeUs);
  if (batch_->size() < writeBufferSize_) {
    return 0;
  }
  return flush();
}

void SpillFileList::updateAppendStats(
    uint64_t numRows,
    uint64_t serializationTimeUs) {
  auto statsLocked = stats_->wlock();
  statsLocked->spilledRows += numRows;
  statsLocked->spillSerializationTimeUs += serializationTimeUs;
  common::updateGlobalSpillAppendStats(numRows, serializationTimeUs);
}

void SpillFileList::updateWriteStats(
    uint32_t numDiskWrites,
    uint64_t spilledBytes,
    uint64_t flushTimeUs,
    uint64_t fileWriteTimeUs) {
  auto statsLocked = stats_->wlock();
  statsLocked->spilledBytes += spilledBytes;
  statsLocked->spillFlushTimeUs += flushTimeUs;
  statsLocked->spillWriteTimeUs += fileWriteTimeUs;
  statsLocked->spillDiskWrites += numDiskWrites;
  common::updateGlobalSpillWriteStats(
      numDiskWrites, spilledBytes, flushTimeUs, fileWriteTimeUs);
}

void SpillFileList::updateSpilledFiles(uint64_t fileSize) {
  ++stats_->wlock()->spilledFiles;
  addThreadLocalRuntimeStat(
      "spillFileSize", RuntimeCounter(fileSize, RuntimeCounter::Unit::kBytes));
  common::incrementGlobalSpilledFiles();
}

void SpillFileList::finishFile() {
  flush();
  if (files_.empty()) {
    return;
  }
  if (files_.back()->isWritable()) {
    files_.back()->finishWrite();
    updateSpilledFiles(files_.back()->size());
  }
}

std::vector<std::string> SpillFileList::testingSpilledFilePaths() const {
  std::vector<std::string> spilledFiles;
  for (auto& file : files_) {
    spilledFiles.push_back(file->testingFilePath());
  }
  return spilledFiles;
}

std::vector<uint32_t> SpillFileList::testingSpilledFileIds() const {
  std::vector<uint32_t> fileIds;
  for (auto& file : files_) {
    fileIds.push_back(file->id());
  }
  return fileIds;
}

SpillState::SpillState(
    common::GetSpillDirectoryPathCB getSpillDirPathCb,
    const std::string& fileNamePrefix,
    int32_t maxPartitions,
    int32_t numSortingKeys,
    const std::vector<CompareFlags>& sortCompareFlags,
    uint64_t targetFileSize,
    uint64_t writeBufferSize,
    common::CompressionKind compressionKind,
    memory::MemoryPool* pool,
    folly::Synchronized<common::SpillStats>* stats,
    const std::unordered_map<std::string, std::string>& writeFileOptions)
    : getSpillDirPathCb_(getSpillDirPathCb),
      fileNamePrefix_(fileNamePrefix),
      maxPartitions_(maxPartitions),
      numSortingKeys_(numSortingKeys),
      sortCompareFlags_(sortCompareFlags),
      targetFileSize_(targetFileSize),
      writeBufferSize_(writeBufferSize),
      compressionKind_(compressionKind),
      writeFileOptions_(writeFileOptions),
      pool_(pool),
      stats_(stats),
      files_(maxPartitions_) {}

void SpillState::setPartitionSpilled(int32_t partition) {
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
    int32_t partition,
    const RowVectorPtr& rows) {
  VELOX_CHECK(
      isPartitionSpilled(partition), "Partition {} is not spilled", partition);

  TestValue::adjust(
      "facebook::velox::exec::SpillState::appendToPartition", this);

  VELOX_CHECK_NOT_NULL(
      getSpillDirPathCb_, "Spill directory callback not specified.");
  const std::string& spillDir = getSpillDirPathCb_();
  VELOX_CHECK(!spillDir.empty(), "Spill directory does not exist");
  // Ensure that partition exist before writing.
  if (!files_.at(partition)) {
    files_[partition] = std::make_unique<SpillFileList>(
        std::static_pointer_cast<const RowType>(rows->type()),
        numSortingKeys_,
        sortCompareFlags_,
        fmt::format("{}/{}-spill-{}", spillDir, fileNamePrefix_, partition),
        targetFileSize_,
        writeBufferSize_,
        compressionKind_,
        pool_,
        stats_,
        writeFileOptions_);
  }
  updateSpilledInputBytes(rows->estimateFlatSize());

  IndexRange range{0, rows->size()};
  return files_[partition]->write(rows, folly::Range<IndexRange*>(&range, 1));
}

SpillFiles SpillState::files(int32_t partition) {
  VELOX_CHECK_LT(partition, files_.size());

  auto list = std::move(files_[partition]);
  if (list == nullptr) {
    return {};
  }
  return list->files();
}

const SpillPartitionNumSet& SpillState::spilledPartitionSet() const {
  return spilledPartitionSet_;
}

std::vector<std::string> SpillState::testingSpilledFilePaths() const {
  std::vector<std::string> spilledFiles;
  for (const auto& list : files_) {
    if (list != nullptr) {
      const auto spilledFilesFromList = list->testingSpilledFilePaths();
      spilledFiles.insert(
          spilledFiles.end(),
          spilledFilesFromList.begin(),
          spilledFilesFromList.end());
    }
  }
  return spilledFiles;
}

std::vector<uint32_t> SpillState::testingSpilledFileIds(
    int32_t partitionNum) const {
  return files_[partitionNum]->testingSpilledFileIds();
}

SpillPartitionNumSet SpillState::testingNonEmptySpilledPartitionSet() const {
  SpillPartitionNumSet partitionSet;
  for (uint32_t partition = 0; partition < maxPartitions_; ++partition) {
    if (files_[partition] != nullptr) {
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
SpillPartition::createUnorderedReader() {
  std::vector<std::unique_ptr<BatchStream>> streams;
  streams.reserve(files_.size());
  for (auto& file : files_) {
    streams.push_back(FileSpillBatchStream::create(std::move(file)));
  }
  files_.clear();
  return std::make_unique<UnorderedStreamReader<BatchStream>>(
      std::move(streams));
}

std::unique_ptr<TreeOfLosers<SpillMergeStream>>
SpillPartition::createOrderedReader() {
  std::vector<std::unique_ptr<SpillMergeStream>> streams;
  streams.reserve(files_.size());
  for (auto& file : files_) {
    streams.push_back(FileSpillMergeStream::create(std::move(file)));
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
} // namespace facebook::velox::exec
