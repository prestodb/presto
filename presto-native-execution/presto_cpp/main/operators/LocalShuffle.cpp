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
#include "presto_cpp/main/operators/LocalShuffle.h"
#include "presto_cpp/external/json/nlohmann/json.hpp"
#include "presto_cpp/main/common/Configs.h"

#include "velox/common/Casts.h"
#include "velox/common/file/FileInputStream.h"

#include <boost/range/algorithm/sort.hpp>

namespace facebook::presto::operators {

using json = nlohmann::json;

namespace {

using TStreamIdx = uint16_t;

// Default buffer size for SortedFileInputStream
// This buffer is used for streaming reads from shuffle files during k-way
// merge.
constexpr uint64_t kDefaultInputStreamBufferSize = 8 * 1024 * 1024; // 8MB

/// SortedFileInputStream reads sorted (key, data) pairs from a single
/// shuffle file with buffered I/O. It extends FileInputStream for efficient
/// buffered I/O and implements MergeStream interface for k-way merge.
class SortedFileInputStream final : public velox::common::FileInputStream,
                                    public velox::MergeStream {
 public:
  SortedFileInputStream(
      const std::string& filePath,
      TStreamIdx streamIdx,
      velox::memory::MemoryPool* pool,
      size_t bufferSize = kDefaultInputStreamBufferSize)
      : velox::common::FileInputStream(
            velox::filesystems::getFileSystem(filePath, nullptr)
                ->openFileForRead(filePath),
            bufferSize,
            pool),
        streamIdx_(streamIdx) {
    next();
  }

  ~SortedFileInputStream() override = default;

  bool next() {
    if (atEnd()) {
      currentKey_.clear();
      currentValue_.clear();
      return false;
    }
    const TRowSize keySize = folly::Endian::big(read<TRowSize>());
    const TRowSize valueSize = folly::Endian::big(read<TRowSize>());

    // TODO: Optimize with zero-copy approach when data is contiguous in buffer.
    readString(currentKey_, keySize);
    readString(currentValue_, valueSize);
    return true;
  }

  std::string_view currentKey() const {
    return currentKey_;
  }

  std::string_view currentValue() const {
    return currentValue_;
  }

  bool hasData() const override {
    return !currentValue_.empty() || !atEnd();
  }

  bool operator<(const velox::MergeStream& other) const override {
    const auto* otherReader = static_cast<const SortedFileInputStream*>(&other);
    if (currentKey_ != otherReader->currentKey_) {
      return compareKeys(currentKey_, otherReader->currentKey_);
    }
    return streamIdx_ < otherReader->streamIdx_;
  }

 private:
  void readString(std::string& target, TRowSize size) {
    if (size > 0) {
      target.resize(size);
      readBytes(reinterpret_cast<uint8_t*>(target.data()), size);
    } else {
      target.clear();
    }
  }

  const TStreamIdx streamIdx_;
  std::string currentKey_;
  std::string currentValue_;
};

class LocalShuffleSerializedPage : public ShuffleSerializedPage {
 public:
  LocalShuffleSerializedPage(
      const std::vector<std::string_view>& rows,
      velox::BufferPtr buffer)
      : rows_{std::move(rows)}, buffer_{std::move(buffer)} {}

  const std::vector<std::string_view>& rows() override {
    return rows_;
  }

  uint64_t size() const override {
    return buffer_->size();
  }

  std::optional<int64_t> numRows() const override {
    return rows_.size();
  }

 private:
  const std::vector<std::string_view> rows_;
  const velox::BufferPtr buffer_;
};

std::vector<RowMetadata>
extractRowMetadata(const char* buffer, size_t bufferSize, bool sortedShuffle) {
  std::vector<RowMetadata> rows;
  size_t offset = 0;

  if (sortedShuffle) {
    // Format: keySize | dataSize | key | data
    while (offset + sizeof(TRowSize) * 2 <= bufferSize) {
      const size_t rowStart = offset;

      const TRowSize keySize = folly::Endian::big(
          *reinterpret_cast<const TRowSize*>(buffer + offset));
      offset += sizeof(TRowSize);

      const TRowSize dataSize = folly::Endian::big(
          *reinterpret_cast<const TRowSize*>(buffer + offset));
      offset += sizeof(TRowSize);

      VELOX_CHECK_LE(
          offset + keySize + dataSize,
          bufferSize,
          "Corrupted shuffle data: expected {} bytes for row (offset={}, keySize={}, dataSize={}) but only {} bytes available in buffer",
          offset + keySize + dataSize,
          offset,
          keySize,
          dataSize,
          bufferSize);

      rows.push_back(
          RowMetadata{
              .rowStart = rowStart, .keySize = keySize, .dataSize = dataSize});

      offset += keySize + dataSize;
    }
  } else {
    // Format: dataSize | data
    while (offset + sizeof(TRowSize) <= bufferSize) {
      const size_t rowStart = offset;

      const TRowSize dataSize = folly::Endian::big(
          *reinterpret_cast<const TRowSize*>(buffer + offset));
      offset += sizeof(TRowSize);

      VELOX_CHECK_LE(
          offset + dataSize,
          bufferSize,
          "Corrupted shuffle data: expected {} bytes for row (offset={}, dataSize={}) but only {} bytes available in buffer",
          offset + dataSize,
          offset,
          dataSize,
          bufferSize);

      rows.push_back(
          RowMetadata{
              .rowStart = rowStart, .keySize = 0, .dataSize = dataSize});

      offset += dataSize;
    }
  }

  return rows;
}

inline std::string_view
extractRowData(const RowMetadata& row, const char* buffer, bool sortedShuffle) {
  const auto dataOffset = row.rowStart +
      (sortedShuffle ? (kUint32Size * 2) + row.keySize : kUint32Size);
  return {buffer + dataOffset, row.dataSize};
}

std::vector<RowMetadata> extractAndSortRowMetadata(
    const char* buffer,
    size_t bufferSize,
    bool sortedShuffle) {
  auto rows = extractRowMetadata(buffer, bufferSize, sortedShuffle);
  if (!rows.empty() && sortedShuffle) {
    boost::range::sort(
        rows, [buffer](const RowMetadata& lhs, const RowMetadata& rhs) {
          const char* lhsKey = buffer + lhs.rowStart + (kUint32Size * 2);
          const char* rhsKey = buffer + rhs.rowStart + (kUint32Size * 2);
          return compareKeys(
              std::string_view(lhsKey, lhs.keySize),
              std::string_view(rhsKey, rhs.keySize));
        });
  }
  return rows;
}

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
} // namespace

std::string LocalShuffleWriteInfo::serialize() const {
  json obj;
  obj["rootPath"] = rootPath;
  obj["queryId"] = queryId;
  obj["shuffleId"] = shuffleId;
  obj["numPartitions"] = numPartitions;
  obj["sortedShuffle"] = sortedShuffle;
  return obj.dump();
}

LocalShuffleWriteInfo LocalShuffleWriteInfo::deserialize(
    const std::string& info) {
  const auto jsonReadInfo = json::parse(info);
  LocalShuffleWriteInfo shuffleInfo;
  jsonReadInfo.at("rootPath").get_to(shuffleInfo.rootPath);
  jsonReadInfo.at("queryId").get_to(shuffleInfo.queryId);
  jsonReadInfo.at("shuffleId").get_to(shuffleInfo.shuffleId);
  jsonReadInfo.at("numPartitions").get_to(shuffleInfo.numPartitions);
  shuffleInfo.sortedShuffle = jsonReadInfo.value("sortedShuffle", false);
  return shuffleInfo;
}

std::string LocalShuffleReadInfo::serialize() const {
  json obj;
  obj["rootPath"] = rootPath;
  obj["queryId"] = queryId;
  obj["partitionIds"] = partitionIds;
  obj["sortedShuffle"] = sortedShuffle;
  return obj.dump();
}

LocalShuffleReadInfo LocalShuffleReadInfo::deserialize(
    const std::string& info) {
  const auto jsonReadInfo = json::parse(info);
  LocalShuffleReadInfo shuffleInfo;
  jsonReadInfo.at("rootPath").get_to(shuffleInfo.rootPath);
  jsonReadInfo.at("queryId").get_to(shuffleInfo.queryId);
  jsonReadInfo.at("partitionIds").get_to(shuffleInfo.partitionIds);
  shuffleInfo.sortedShuffle = jsonReadInfo.value("sortedShuffle", false);
  return shuffleInfo;
}

LocalShuffleWriter::LocalShuffleWriter(
    const std::string& rootPath,
    const std::string& queryId,
    uint32_t shuffleId,
    uint32_t numPartitions,
    uint64_t maxBytesPerPartition,
    bool sortedShuffle,
    velox::memory::MemoryPool* pool)
    : threadId_(std::this_thread::get_id()),
      pool_(pool),
      numPartitions_(numPartitions),
      maxBytesPerPartition_(maxBytesPerPartition),
      sortedShuffle_(sortedShuffle),
      rootPath_(rootPath),
      queryId_(queryId),
      shuffleId_(shuffleId) {
  inProgressPartitions_.assign(numPartitions_, nullptr);
  inProgressSizes_.assign(numPartitions_, 0);
  fileSystem_ = velox::filesystems::getFileSystem(rootPath_, nullptr);
}

void LocalShuffleWriter::writeBlock(int32_t partition) {
  auto& buffer = inProgressPartitions_[partition];
  const auto bufferSize = inProgressSizes_[partition];

  VELOX_DCHECK_NOT_NULL(buffer, "Buffer should be allocated before writeBlock");
  VELOX_DCHECK_GT(bufferSize, 0, "Buffer size should be positive");

  auto file = getNextOutputFile(partition);
  const char* data = buffer->as<char>();

  // For non-sorted shuffle, write buffer directly
  if (!sortedShuffle_) {
    file->append(std::string_view(data, bufferSize));
  } else {
    // For sorted shuffle, parse and sort rows, then write
    const auto sortedRows =
        extractAndSortRowMetadata(data, bufferSize, sortedShuffle_);
    for (const auto& row : sortedRows) {
      const size_t rowLen = sortedShuffle_
          ? (kUint32Size * 2) + row.keySize + row.dataSize
          : kUint32Size + row.dataSize;
      file->append(std::string_view(data + row.rowStart, rowLen));
    }
  }
  file->close();
  inProgressSizes_[partition] = 0;
}

std::unique_ptr<velox::WriteFile> LocalShuffleWriter::getNextOutputFile(
    int32_t partition) {
  auto filename = nextAvailablePartitionFileName(rootPath_, partition);
  return fileSystem_->openFileForWrite(filename);
}

std::string LocalShuffleWriter::nextAvailablePartitionFileName(
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

size_t LocalShuffleWriter::rowSize(size_t keySize, size_t dataSize) const {
  return sortedShuffle_ ? (kUint32Size * 2) + keySize + dataSize
                        : kUint32Size + dataSize;
}

void LocalShuffleWriter::appendRow(
    char* writePos,
    std::string_view key,
    std::string_view data) {
  if (sortedShuffle_) {
    const auto keySize = static_cast<TRowSize>(key.size());
    const auto dataSize = static_cast<TRowSize>(data.size());
    *reinterpret_cast<TRowSize*>(writePos) = folly::Endian::big(keySize);
    writePos += sizeof(TRowSize);
    *reinterpret_cast<TRowSize*>(writePos) = folly::Endian::big(dataSize);
    writePos += sizeof(TRowSize);
    if (keySize > 0) {
      memcpy(writePos, key.data(), keySize);
      writePos += keySize;
    }
    if (dataSize > 0) {
      memcpy(writePos, data.data(), dataSize);
    }
  } else {
    const auto dataSize = static_cast<TRowSize>(data.size());
    *reinterpret_cast<TRowSize*>(writePos) = folly::Endian::big(dataSize);
    writePos += sizeof(TRowSize);
    if (dataSize > 0) {
      memcpy(writePos, data.data(), dataSize);
    }
  }
}

void LocalShuffleWriter::collect(
    int32_t partition,
    std::string_view key,
    std::string_view data) {
  VELOX_CHECK_LT(partition, numPartitions_);
  VELOX_CHECK(
      sortedShuffle_ || key.empty(),
      "key '{}' must be empty for non-sorted shuffle",
      key);
  velox::common::testutil::TestValue::adjust(
      "facebook::presto::operators::LocalShuffleWriter::collect", this);

  const auto rowSize = this->rowSize(key.size(), data.size());
  auto& buffer = inProgressPartitions_[partition];
  if (buffer == nullptr) {
    buffer = velox::AlignedBuffer::allocate<char>(
        std::max(static_cast<uint64_t>(rowSize), maxBytesPerPartition_),
        pool_,
        0);
    inProgressSizes_[partition] = 0;
  } else if (inProgressSizes_[partition] + rowSize >= buffer->capacity()) {
    writeBlock(partition);
  }
  auto* rawBuffer = buffer->asMutable<char>();
  auto* writePos = rawBuffer + inProgressSizes_[partition];
  appendRow(writePos, key, data);
  inProgressSizes_[partition] += rowSize;
}

void LocalShuffleWriter::noMoreData(bool success) {
  // Delete all shuffle files on failure.
  if (!success) {
    cleanup();
  }
  for (auto i = 0; i < numPartitions_; ++i) {
    if (inProgressSizes_[i] > 0) {
      writeBlock(i);
    }
  }
}

LocalShuffleReader::LocalShuffleReader(
    const std::string& rootPath,
    const std::string& queryId,
    std::vector<std::string> partitionIds,
    bool sortedShuffle,
    velox::memory::MemoryPool* pool)
    : rootPath_(rootPath),
      queryId_(queryId),
      partitionIds_(std::move(partitionIds)),
      sortedShuffle_(sortedShuffle),
      pool_(pool) {
  fileSystem_ = velox::filesystems::getFileSystem(rootPath_, nullptr);
}

void LocalShuffleReader::initialize() {
  VELOX_CHECK(!initialized_, "LocalShuffleReader already initialized");
  readPartitionFiles_ = getReadPartitionFiles();
  if (sortedShuffle_ && !readPartitionFiles_.empty()) {
    initSortedShuffleRead();
  }

  initialized_ = true;
}

void LocalShuffleReader::initSortedShuffleRead() {
  std::vector<std::unique_ptr<velox::MergeStream>> streams;
  streams.reserve(readPartitionFiles_.size());
  TStreamIdx streamIdx = 0;
  for (const auto& filename : readPartitionFiles_) {
    VELOX_CHECK(
        !filename.empty(),
        "Invalid empty shuffle file path for query {}, partitions: [{}]",
        queryId_,
        folly::join(", ", partitionIds_));
    auto reader =
        std::make_unique<SortedFileInputStream>(filename, streamIdx, pool_);
    if (reader->hasData()) {
      streams.push_back(std::move(reader));
      ++streamIdx;
    }
  }
  if (!streams.empty()) {
    merge_ =
        std::make_unique<velox::TreeOfLosers<velox::MergeStream, uint16_t>>(
            std::move(streams));
  }
}

std::vector<std::unique_ptr<ShuffleSerializedPage>>
LocalShuffleReader::nextSorted(uint64_t maxBytes) {
  std::vector<std::unique_ptr<ShuffleSerializedPage>> batches;

  if (merge_ == nullptr) {
    return batches;
  }

  auto batchBuffer = velox::AlignedBuffer::allocate<char>(maxBytes, pool_, 0);
  std::vector<std::string_view> rows;
  uint64_t bufferUsed = 0;

  while (auto* stream = merge_->next()) {
    auto* reader = velox::checkedPointerCast<SortedFileInputStream>(stream);
    const auto data = reader->currentValue();

    if (bufferUsed + data.size() > maxBytes) {
      if (bufferUsed > 0) {
        batches.push_back(
            std::make_unique<LocalShuffleSerializedPage>(
                std::move(rows), std::move(batchBuffer)));
        return batches;
      }
      // Single row exceeds buffer - allocate larger buffer
      batchBuffer = velox::AlignedBuffer::allocate<char>(data.size(), pool_, 0);
    }

    char* writePos = batchBuffer->asMutable<char>() + bufferUsed;
    if (!data.empty()) {
      memcpy(writePos, data.data(), data.size());
    }

    rows.emplace_back(batchBuffer->as<char>() + bufferUsed, data.size());
    bufferUsed += data.size();
    reader->next();
  }

  if (!rows.empty()) {
    batches.push_back(
        std::make_unique<LocalShuffleSerializedPage>(
            std::move(rows), std::move(batchBuffer)));
  }

  return batches;
}

std::vector<std::unique_ptr<ShuffleSerializedPage>>
LocalShuffleReader::nextUnsorted(uint64_t maxBytes) {
  std::vector<std::unique_ptr<ShuffleSerializedPage>> batches;
  uint64_t totalBytes{0};

  while (readPartitionFileIndex_ < readPartitionFiles_.size()) {
    const auto filename = readPartitionFiles_[readPartitionFileIndex_];
    auto file = fileSystem_->openFileForRead(filename);
    const auto fileSize = file->size();

    // TODO: Refactor to use streaming I/O with bounded buffer size instead of
    // loading entire files into memory at once. A streaming approach would
    // reduce peak memory consumption and enable processing arbitrarily large
    // shuffle files while maintaining constant memory usage.
    if (!batches.empty() && totalBytes + fileSize > maxBytes) {
      break;
    }

    auto buffer = velox::AlignedBuffer::allocate<char>(fileSize, pool_, 0);
    file->pread(0, fileSize, buffer->asMutable<void>());
    ++readPartitionFileIndex_;

    const char* data = buffer->as<char>();
    const auto parsedRows = extractRowMetadata(data, fileSize, sortedShuffle_);
    std::vector<std::string_view> rows;
    rows.reserve(parsedRows.size());
    for (const auto& row : parsedRows) {
      rows.push_back(extractRowData(row, data, sortedShuffle_));
    }

    totalBytes += fileSize;
    batches.push_back(
        std::make_unique<LocalShuffleSerializedPage>(
            std::move(rows), std::move(buffer)));
  }

  return batches;
}

folly::SemiFuture<std::vector<std::unique_ptr<ShuffleSerializedPage>>>
LocalShuffleReader::next(uint64_t maxBytes) {
  VELOX_CHECK(
      initialized_,
      "LocalShuffleReader::initialize() must be called before next()");
  velox::common::testutil::TestValue::adjust(
      "facebook::presto::operators::LocalShuffleReader::next", this);

  return folly::makeSemiFuture(
      sortedShuffle_ ? nextSorted(maxBytes) : nextUnsorted(maxBytes));
}

void LocalShuffleReader::noMoreData(bool success) {
  // On failure, reset the index of the files to be read.
  if (!success) {
    readPartitionFileIndex_ = 0;
  }
}

std::vector<std::string> LocalShuffleReader::getReadPartitionFiles() const {
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
      if (file.starts_with(prefix)) {
        partitionFiles.push_back(file);
      }
    }
  }

  return partitionFiles;
}

void LocalShuffleWriter::cleanup() {
  auto files = fileSystem_->list(rootPath_);
  for (auto& file : files) {
    fileSystem_->remove(file);
  }
}

std::shared_ptr<ShuffleReader> LocalPersistentShuffleFactory::createReader(
    const std::string& serializedStr,
    const int32_t /*partition*/,
    velox::memory::MemoryPool* pool) {
  const operators::LocalShuffleReadInfo readInfo =
      operators::LocalShuffleReadInfo::deserialize(serializedStr);

  auto reader = std::make_shared<LocalShuffleReader>(
      readInfo.rootPath,
      readInfo.queryId,
      readInfo.partitionIds,
      readInfo.sortedShuffle,
      pool);
  reader->initialize();
  return reader;
}

std::shared_ptr<ShuffleWriter> LocalPersistentShuffleFactory::createWriter(
    const std::string& serializedStr,
    velox::memory::MemoryPool* pool) {
  static const uint64_t maxBytesPerPartition =
      SystemConfig::instance()->localShuffleMaxPartitionBytes();
  const operators::LocalShuffleWriteInfo writeInfo =
      operators::LocalShuffleWriteInfo::deserialize(serializedStr);

  return std::make_shared<LocalShuffleWriter>(
      writeInfo.rootPath,
      writeInfo.queryId,
      writeInfo.shuffleId,
      writeInfo.numPartitions,
      maxBytesPerPartition,
      writeInfo.sortedShuffle,
      pool);
}
} // namespace facebook::presto::operators
