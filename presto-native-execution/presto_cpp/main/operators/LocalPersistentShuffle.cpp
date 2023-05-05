/*
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
#include "presto_cpp/main/operators/LocalPersistentShuffle.h"
#include "presto_cpp/external/json/json.hpp"
#include "presto_cpp/main/common/Configs.h"

using namespace facebook::velox::exec;
using namespace facebook::velox;

namespace facebook::presto::operators {

namespace {
inline std::string createShuffleFileName(
    const std::string& rootPath,
    const std::string& queryId,
    uint32_t shuffleId,
    int32_t partition,
    int fileIndex,
    const std::thread::id& id) {
  // Follow Spark's shuffle file name format: shuffle_shuffleId_0_reduceId
  return fmt::format(
      "{}/{}_shuffle_{}_0_{}_{}_{}.bin",
      rootPath,
      queryId,
      shuffleId,
      partition,
      fileIndex,
      id);
}

// This file is used to indicate that the shuffle system is ready to be used for
// reading (acts as a sync point between readers if needed). Mostly used for
// test purposes.
const static std::string kReadyForReadFilename = "readyForRead";
}; // namespace

LocalPersistentShuffleWriter::LocalPersistentShuffleWriter(
    const std::string& rootPath,
    const std::string& queryId,
    uint32_t shuffleId,
    uint32_t numPartitions,
    uint64_t maxBytesPerPartition,
    velox::memory::MemoryPool* FOLLY_NONNULL pool)
    : maxBytesPerPartition_(maxBytesPerPartition),
      threadId_(std::this_thread::get_id()),
      pool_(pool),
      numPartitions_(numPartitions),
      rootPath_(std::move(rootPath)),
      shuffleId_(shuffleId),
      queryId_(std::move(queryId)) {
  // Use resize/assign instead of resize(size, val).
  inProgressPartitions_.resize(numPartitions_);
  inProgressPartitions_.assign(numPartitions_, nullptr);
  inProgressSizes_.resize(numPartitions_);
  inProgressSizes_.assign(numPartitions_, 0);
  fileSystem_ = velox::filesystems::getFileSystem(rootPath_, nullptr);
}

std::unique_ptr<velox::WriteFile>
LocalPersistentShuffleWriter::getNextOutputFile(int32_t partition) {
  auto filename = nextAvailablePartitionFileName(rootPath_, partition);
  return fileSystem_->openFileForWrite(filename);
}

std::string LocalPersistentShuffleWriter::nextAvailablePartitionFileName(
    const std::string& root,
    int32_t partition) const {
  int fileCount = 0;
  std::string filename;
  // TODO: consider to maintain the next to create file count in memory as we
  // always do cleanup when switch to a new root directory path.
  do {
    filename = createShuffleFileName(
        root, queryId_, shuffleId_, partition, fileCount, threadId_);
    if (!fileSystem_->exists(filename)) {
      break;
    }
    ++fileCount;
  } while (true);

  return filename;
}

void LocalPersistentShuffleWriter::storePartitionBlock(int32_t partition) {
  auto& buffer = inProgressPartitions_[partition];
  auto file = getNextOutputFile(partition);
  file->append(
      std::string_view(buffer->as<char>(), inProgressSizes_[partition]));
  file->close();
  inProgressPartitions_[partition].reset();
  inProgressSizes_[partition] = 0;
}

void LocalPersistentShuffleWriter::collect(
    int32_t partition,
    std::string_view data) {
  using TRowSize = uint32_t;

  auto& buffer = inProgressPartitions_[partition];
  const TRowSize rowSize = data.size();
  const auto size = sizeof(TRowSize) + rowSize;

  // Check if there is enough space in the buffer.
  if ((buffer != nullptr) &&
      (inProgressSizes_[partition] + size >= buffer->capacity())) {
    storePartitionBlock(partition);
    // NOTE: the referenced 'buffer' will be reset in storePartitionBlock.
  }

  // Allocate buffer if needed.
  if (buffer == nullptr) {
    buffer = AlignedBuffer::allocate<char>(
        std::max((uint64_t)size, maxBytesPerPartition_), pool_);
    inProgressSizes_[partition] = 0;
    inProgressPartitions_[partition] = buffer;
  }

  // Copy data.
  auto offset = inProgressSizes_[partition];
  auto rawBuffer = buffer->asMutable<char>() + offset;

  *(TRowSize*)(rawBuffer) = folly::Endian::big(rowSize);
  ::memcpy(rawBuffer + sizeof(TRowSize), data.data(), rowSize);

  inProgressSizes_[partition] += size;
}

void LocalPersistentShuffleWriter::noMoreData(bool success) {
  // Delete all shuffle files on failure.
  if (!success) {
    cleanup();
  }
  for (auto i = 0; i < numPartitions_; ++i) {
    if (inProgressSizes_[i] > 0) {
      storePartitionBlock(i);
    }
  }
}

LocalPersistentShuffleReader::LocalPersistentShuffleReader(
    const std::string& rootPath,
    const std::string& queryId,
    std::vector<std::string> partitionIds,
    const int32_t partition,
    velox::memory::MemoryPool* FOLLY_NONNULL pool)
    : rootPath_(rootPath),
      queryId_(queryId),
      partitionIds_(std::move(partitionIds)),
      partition_(partition),
      pool_(pool) {
  fileSystem_ = velox::filesystems::getFileSystem(rootPath_, nullptr);
}

bool LocalPersistentShuffleReader::hasNext() {
  if (readPartitionFiles_.empty()) {
    readPartitionFiles_ = getReadPartitionFiles();
  }

  return readPartitionFileIndex_ < readPartitionFiles_.size();
}

BufferPtr LocalPersistentShuffleReader::next(bool success) {
  // On failure, reset the index of the files to be read.
  if (!success) {
    readPartitionFileIndex_ = 0;
  }

  auto filename = readPartitionFiles_[readPartitionFileIndex_];
  auto file = fileSystem_->openFileForRead(filename);
  auto buffer = AlignedBuffer::allocate<char>(file->size(), pool_, 0);
  file->pread(0, file->size(), buffer->asMutable<void>());
  ++readPartitionFileIndex_;
  return buffer;
}

std::vector<std::string> LocalPersistentShuffleReader::getReadPartitionFiles()
    const {
  // Get rid of excess '/' characters in the path.
  auto trimmedRootPath = rootPath_;
  while (trimmedRootPath.length() > 0 &&
         trimmedRootPath[trimmedRootPath.length() - 1] == '/') {
    trimmedRootPath.erase(trimmedRootPath.length() - 1, 1);
  }

  std::vector<std::string> partitionFiles;
  for (const auto& partitionId : partitionIds_) {
    auto prefix =
        fmt::format("{}/{}_{}_", trimmedRootPath, queryId_, partitionId);
    auto files = fileSystem_->list(fmt::format("{}/", rootPath_));
    for (const auto& file : files) {
      if (file.find(prefix) == 0) {
        partitionFiles.push_back(file);
      }
    }
  }

  return partitionFiles;
}

void LocalPersistentShuffleWriter::cleanup() {
  auto files = fileSystem_->list(rootPath_);
  for (auto& file : files) {
    fileSystem_->remove(file);
  }
}

using json = nlohmann::json;

// static
LocalShuffleWriteInfo LocalShuffleWriteInfo::deserialize(
    const std::string& info) {
  const auto jsonReadInfo = json::parse(info);
  LocalShuffleWriteInfo shuffleInfo;
  jsonReadInfo.at("rootPath").get_to(shuffleInfo.rootPath);
  jsonReadInfo.at("queryId").get_to(shuffleInfo.queryId);
  jsonReadInfo.at("shuffleId").get_to(shuffleInfo.shuffleId);
  jsonReadInfo.at("numPartitions").get_to(shuffleInfo.numPartitions);
  return shuffleInfo;
}

LocalShuffleReadInfo LocalShuffleReadInfo::deserialize(
    const std::string& info) {
  const auto jsonReadInfo = json::parse(info);
  LocalShuffleReadInfo shuffleInfo;
  jsonReadInfo.at("rootPath").get_to(shuffleInfo.rootPath);
  jsonReadInfo.at("queryId").get_to(shuffleInfo.queryId);
  jsonReadInfo.at("partitionIds").get_to(shuffleInfo.partitionIds);
  jsonReadInfo.at("numPartitions").get_to(shuffleInfo.numPartitions);
  return shuffleInfo;
}

std::shared_ptr<ShuffleReader> LocalPersistentShuffleFactory::createReader(
    const std::string& serializedStr,
    const int32_t partition,
    velox::memory::MemoryPool* pool) {
  static const uint64_t maxBytesPerPartition =
      SystemConfig::instance()->localShuffleMaxPartitionBytes();
  const operators::LocalShuffleReadInfo readInfo =
      operators::LocalShuffleReadInfo::deserialize(serializedStr);
  return std::make_shared<operators::LocalPersistentShuffleReader>(
      readInfo.rootPath,
      readInfo.queryId,
      readInfo.partitionIds,
      partition,
      pool);
}

std::shared_ptr<ShuffleWriter> LocalPersistentShuffleFactory::createWriter(
    const std::string& serializedStr,
    velox::memory::MemoryPool* pool) {
  static const uint64_t maxBytesPerPartition =
      SystemConfig::instance()->localShuffleMaxPartitionBytes();
  const operators::LocalShuffleWriteInfo writeInfo =
      operators::LocalShuffleWriteInfo::deserialize(serializedStr);
  return std::make_shared<operators::LocalPersistentShuffleWriter>(
      writeInfo.rootPath,
      writeInfo.queryId,
      writeInfo.shuffleId,
      writeInfo.numPartitions,
      maxBytesPerPartition,
      pool);
}

} // namespace facebook::presto::operators
