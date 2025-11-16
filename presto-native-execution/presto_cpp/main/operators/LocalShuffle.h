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
#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "velox/common/base/TreeOfLosers.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/Operator.h"

#include "presto_cpp/main/operators/ShuffleInterface.h"

namespace facebook::presto::operators {

using TRowSize = uint32_t;

constexpr size_t kUint32Size = sizeof(TRowSize);

// Default buffer size for SortedFileInputStream
// This buffer is used for streaming reads from shuffle files during k-way
// merge.
constexpr uint64_t kDefaultInputStreamBufferSize = 8 * 1024 * 1024; // 8MB

// Metadata describing a serialized row's location and sizes in a buffer
struct RowMetadata {
  size_t rowStart; // Offset to the start of this row
  uint32_t keySize; // Size of key (0 for non-sorted)
  uint32_t dataSize; // Size of data
};

// Compares sort keys lexicographically using three-way comparison.
inline std::strong_ordering compareKeys(
    std::string_view key1,
    std::string_view key2) noexcept {
  return std::lexicographical_compare_three_way(
      reinterpret_cast<const uint8_t*>(key1.data()),
      reinterpret_cast<const uint8_t*>(key1.data() + key1.size()),
      reinterpret_cast<const uint8_t*>(key2.data()),
      reinterpret_cast<const uint8_t*>(key2.data() + key2.size()));
}

// Testing function to expose extractRowMetadata for tests.
std::vector<RowMetadata> testingExtractRowMetadata(
    const char* buffer,
    size_t bufferSize,
    bool sortedShuffle);

// LocalShuffleWriteInfo is used for containing shuffle write information.
// This struct is a 1:1 strict API mapping to
// presto-spark-base/src/main/java/com/facebook/presto/spark/execution/PrestoSparkLocalShuffleWriteInfo.java
// Please refrain changes to this API class. If any changes have to be made to
// this struct, one should make sure to make corresponding changes in the above
// Java classes and its corresponding serde functionalities.
struct LocalShuffleWriteInfo {
  std::string rootPath;
  std::string queryId;
  uint32_t numPartitions;
  uint32_t shuffleId;

  /// Deserializes shuffle information that is used by LocalPersistentShuffle.
  /// Structures are assumed to be encoded in JSON format.
  static LocalShuffleWriteInfo deserialize(const std::string& info);
};

// LocalShuffleReadInfo is used for containing shuffle read metadata
// This struct is a 1:1 strict API mapping to
// presto-spark-base/src/main/java/com/facebook/presto/spark/execution/PrestoSparkLocalShuffleReadInfo.java.
// Please refrain changes to this API class. If any changes have to be made to
// this struct, one should make sure to make corresponding changes in the above
// Java classes and its corresponding serde functionalities.
struct LocalShuffleReadInfo {
  std::string rootPath;
  std::string queryId;
  std::vector<std::string> partitionIds;

  /// Deserializes shuffle information that is used by LocalPersistentShuffle.
  /// Structures are assumed to be encoded in JSON format.
  static LocalShuffleReadInfo deserialize(const std::string& info);
};

/// This class is a persistent shuffle server that implements
/// ShuffleInterface for read and write and also uses generalized Velox
/// file system to maintain its state and data.
///
/// Except for in-progress blocks of current output vectors in the writer,
/// each produced vector is stored as a binary file of unsafe rows. Each block
/// filename reflects the partition and sequence number of the block (vector)
/// for that partition. For example <ROOT_PATH>/10_12.bin is the 12th (block)
/// vector in partition #10.
///
/// The class also uses Velox filesystem to figure out the number of written
/// shuffle files for each partition. This enables the multi-threaded or
/// multi-process use scenarios as long as each producer or consumer is assigned
/// to a distinct group of partition IDs. Each of them can create an instance of
/// this class (pointing to the same root path) to read and write shuffle data.
class LocalShuffleWriter : public ShuffleWriter {
 public:
  LocalShuffleWriter(
      const std::string& rootPath,
      const std::string& queryId,
      uint32_t shuffleId,
      uint32_t numPartitions,
      uint64_t maxBytesPerPartition,
      bool sortedShuffle,
      velox::memory::MemoryPool* pool);

  void collect(int32_t partition, std::string_view key, std::string_view data)
      override;

  void noMoreData(bool success) override;

  folly::F14FastMap<std::string, int64_t> stats() const override {
    // Fake counter for testing only.
    return {{"local.write", 2345}};
  }

 private:
  void appendRow(char* writePos, std::string_view key, std::string_view data);

  size_t rowSize(size_t keySize, size_t dataSize) const;

  // Finds and creates the next file for writing the next block of the
  // given 'partition'.
  std::unique_ptr<velox::WriteFile> getNextOutputFile(int32_t partition);

  // Writes the in-progress block to the given partition.
  void writeBlock(int32_t partition);

  // Deletes all the files in the root directory.
  void cleanup();

  // find next available partition file name to store shuffle data
  std::string nextAvailablePartitionFileName(
      const std::string& root,
      int32_t partition) const;

  // Used to make sure files created by this thread have unique names.
  const std::thread::id threadId_;
  velox::memory::MemoryPool* pool_;
  const uint32_t numPartitions_;
  const uint64_t maxBytesPerPartition_;
  const bool sortedShuffle_;
  // The top directory of the shuffle files and its file system.
  const std::string rootPath_;
  const std::string queryId_;
  const uint32_t shuffleId_;

  /// The latest written block buffers and sizes.
  std::vector<velox::BufferPtr> inProgressPartitions_;
  std::vector<size_t> inProgressSizes_;
  std::shared_ptr<velox::filesystems::FileSystem> fileSystem_;
};

class LocalShuffleReader : public ShuffleReader {
 public:
  LocalShuffleReader(
      const std::string& rootPath,
      const std::string& queryId,
      std::vector<std::string> partitionIds,
      bool sortedShuffle,
      velox::memory::MemoryPool* pool);

  /// Initializes the reader by discovering shuffle files and setting up merge
  /// infrastructure for sorted shuffle. Must be called before next().
  /// For sorted shuffle, this opens all shuffle files and prepares k-way merge.
  void initialize();

  folly::SemiFuture<std::vector<std::unique_ptr<ReadBatch>>> next(
      uint64_t maxBytes) override;

  void noMoreData(bool success) override;

  folly::F14FastMap<std::string, int64_t> stats() const override {
    // Fake counter for testing only.
    return {{"local.read", 123}};
  }

 private:
  // Returns all created shuffle files for 'partition_'.
  std::vector<std::string> getReadPartitionFiles() const;

  // Reads sorted shuffle data using k-way merge with TreeOfLosers.
  std::vector<std::unique_ptr<ReadBatch>> nextSorted(uint64_t maxBytes);

  // Reads unsorted shuffle data in batch-based file reading.
  std::vector<std::unique_ptr<ReadBatch>> nextUnsorted(uint64_t maxBytes);

  const std::string rootPath_;
  const std::string queryId_;
  const std::vector<std::string> partitionIds_;
  const bool sortedShuffle_;
  velox::memory::MemoryPool* pool_;

  // Latest read block (file) index in 'readPartitionFiles_' for 'partition_'.
  size_t readPartitionFileIndex_{0};

  // List of generated files for 'partition_'.
  std::vector<std::string> readPartitionFiles_;

  // The top directory of the shuffle files and its file system.
  std::shared_ptr<velox::filesystems::FileSystem> fileSystem_;

  // Used to merge sorted streams from multiple shuffle files for k-way merge.
  std::unique_ptr<velox::TreeOfLosers<velox::MergeStream, uint16_t>> merge_;

  bool initialized_{false};
};

class LocalPersistentShuffleFactory : public ShuffleInterfaceFactory {
 public:
  static constexpr folly::StringPiece kShuffleName{"local"};
  std::shared_ptr<ShuffleReader> createReader(
      const std::string& serializedStr,
      const int32_t partition,
      velox::memory::MemoryPool* pool) override;

  std::shared_ptr<ShuffleWriter> createWriter(
      const std::string& serializedStr,
      velox::memory::MemoryPool* pool) override;
};

} // namespace facebook::presto::operators
