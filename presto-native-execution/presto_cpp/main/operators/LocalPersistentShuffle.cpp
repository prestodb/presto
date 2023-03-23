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
    int32_t partition,
    int fileIndex,
    const std::thread::id& id) {
  return fmt::format("{}/{}_{}_{}.bin", rootPath, partition, fileIndex, id);
}

// This file is used to indicate that the shuffle system is ready to be used for
// reading (acts as a sync point between readers if needed). Mostly used for
// test purposes.
const static std::string kReadyForReadFilename = "readyForRead";
}; // namespace

LocalPersistentShuffleWriter::LocalPersistentShuffleWriter(
    const std::string& rootPath,
    uint32_t numPartitions,
    uint64_t maxBytesPerPartition,
    velox::memory::MemoryPool* FOLLY_NONNULL pool)
    : maxBytesPerPartition_(maxBytesPerPartition),
      threadId_(std::this_thread::get_id()),
      pool_(pool),
      numPartitions_(numPartitions),
      rootPath_(std::move(rootPath)) {
  // Use resize/assign instead of resize(size, val).
  inProgressPartitions_.resize(numPartitions_);
  inProgressPartitions_.assign(numPartitions_, nullptr);
  inProgressSizes_.resize(numPartitions_);
  inProgressSizes_.assign(numPartitions_, 0);
  fileSystem_ = velox::filesystems::getFileSystem(rootPath_, nullptr);
}

std::unique_ptr<velox::WriteFile>
LocalPersistentShuffleWriter::getNextOutputFile(int32_t partition) {
  auto fileCount = getWritePartitionFilesCount(partition);
  const std::string filename =
      createShuffleFileName(rootPath_, partition, fileCount, threadId_);
  return fileSystem_->openFileForWrite(filename);
}

int LocalPersistentShuffleWriter::getWritePartitionFilesCount(
    int32_t partition) const {
  int fileCount = 0;
  // TODO: consider to maintain the next to create file count in memory as we
  // always do cleanup when switch to a new root directy path.
  do {
    const std::string filename =
        createShuffleFileName(rootPath_, partition, fileCount, threadId_);
    if (!fileSystem_->exists(filename)) {
      break;
    }
    ++fileCount;
  } while (true);
  return fileCount;
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
  auto& buffer = inProgressPartitions_[partition];
  const auto size = data.size();

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
  auto rawBuffer = buffer->asMutable<char>();
  auto offset = inProgressSizes_[partition];

  ::memcpy(rawBuffer + offset, data.data(), size);

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
  // Write "ready to read" file.
  auto readyToRead = fileSystem_->openFileForWrite(
      fmt::format("{}/{}", rootPath_, kReadyForReadFilename));
  readyToRead->close();
}

LocalPersistentShuffleReader::LocalPersistentShuffleReader(
    const std::string& rootPath,
    const int32_t partition,
    velox::memory::MemoryPool* FOLLY_NONNULL pool)
    : rootPath_(std::move(rootPath)), partition_(partition), pool_(pool) {
  fileSystem_ = velox::filesystems::getFileSystem(rootPath_, nullptr);
}

bool LocalPersistentShuffleReader::hasNext() {
  while (!readyForRead()) {
    // This sleep is only for testing purposes.
    // For the test cases in which the shuffle reader tasks run before shuffle
    // writer tasks finish.
    sleep(1);
  }
  return readPartitionFileIndex_ < getReadPartitionFiles().size();
}

BufferPtr LocalPersistentShuffleReader::next(bool success) {
  // On failure, reset the index of the files to be read.
  if (!success) {
    readPartitionFileIndex_ = 0;
  }
  if (readPartitionFiles_.empty()) {
    readPartitionFiles_ = getReadPartitionFiles();
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

  const std::string partitionFilePrefix =
      fmt::format("{}/{}_", trimmedRootPath, partition_);
  std::vector<std::string> partitionFiles;
  auto files = fileSystem_->list(rootPath_);
  for (auto& file : files) {
    if (file.find(partitionFilePrefix) == 0) {
      partitionFiles.push_back(file);
    }
  }
  return partitionFiles;
}

bool LocalPersistentShuffleReader::readyForRead() const {
  return fileSystem_->openFileForRead(
             fmt::format("{}/{}", rootPath_, kReadyForReadFilename)) != nullptr;
}

void LocalPersistentShuffleWriter::cleanup() {
  auto files = fileSystem_->list(rootPath_);
  for (auto& file : files) {
    fileSystem_->remove(file);
  }
}

using json = nlohmann::json;

// static
LocalShuffleInfo LocalShuffleInfo::deserialize(const std::string& info) {
  const auto jsonReadInfo = json::parse(info);
  LocalShuffleInfo shuffleInfo;
  jsonReadInfo.at("rootPath").get_to(shuffleInfo.rootPath);
  jsonReadInfo.at("numPartitions").get_to(shuffleInfo.numPartitions);
  return shuffleInfo;
}

std::shared_ptr<ShuffleReader> LocalPersistentShuffleFactory::createReader(
    const std::string& serializedStr,
    const int32_t partition,
    velox::memory::MemoryPool* pool) {
  static const uint64_t maxBytesPerPartition =
      SystemConfig::instance()->localShuffleMaxPartitionBytes();
  const operators::LocalShuffleInfo readInfo =
      operators::LocalShuffleInfo::deserialize(serializedStr);
  return std::make_shared<operators::LocalPersistentShuffleReader>(
      readInfo.rootPath, partition, pool);
}

std::shared_ptr<ShuffleWriter> LocalPersistentShuffleFactory::createWriter(
    const std::string& serializedStr,
    velox::memory::MemoryPool* pool) {
  static const uint64_t maxBytesPerPartition =
      SystemConfig::instance()->localShuffleMaxPartitionBytes();
  const operators::LocalShuffleInfo writeInfo =
      operators::LocalShuffleInfo::deserialize(serializedStr);
  return std::make_shared<operators::LocalPersistentShuffleWriter>(
      writeInfo.rootPath, writeInfo.numPartitions, maxBytesPerPartition, pool);
}

} // namespace facebook::presto::operators
