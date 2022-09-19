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

#include "presto_cpp/main/operators/ShuffleInterface.h"
#include "velox/buffer/Buffer.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"

namespace facebook::presto::operators {

/// This class is a persistent shuffle server that implements
/// ShuffleInterface for read and write and also uses generalized Velox
/// file system to maintain its state and data.
///
/// Except for in-progress blocks of current output vectors in the writer,
/// each produced vector is stored as a binary file of unsafe rows. Each block
/// filename reflects the partition and sequence number of the block (vector)
/// for that partition. For example <ROOT_PATH>/10_12.bin is the 13th (block)
/// vector in partition #10.
///
/// The class uses filesystem also to figure out the number of written shuffle
/// files for each partition. This allows this class to be usable under
/// different multi-threaded or multi-process scenarios as long as each producer
/// or consumer is assigned to a distinct group of partition IDs. Each of them
/// can create an instance of this class (pointing to the same root path) and
/// use it to read and write shuffle data.
class TestingPersistentShuffle : public ShuffleInterface {
 public:
  TestingPersistentShuffle(uint32_t maxBytesPerPartition)
      : maxBytesPerPartition_(maxBytesPerPartition),
        threadId_(std::this_thread::get_id()) {}

  /// This method resets the state of the shuffle using a specific number of
  /// partitions and a rootPath.
  /// This can be used multiple times on the same shuffle object. If the
  /// current rootPath is not an empty string, the method also cleans up the
  /// its previous root path folder (does not delete the folder itself).
  void initialize(
      velox::memory::MemoryPool* pool,
      uint32_t numPartitions,
      std::string rootPath) {
    pool_ = pool;
    numPartitions_ = numPartitions;
    // Use resize/assign instead of resize(size, val).
    inProgressPartitions_.resize(numPartitions_);
    inProgressPartitions_.assign(numPartitions_, nullptr);
    inProgressSizes_.resize(numPartitions_);
    inProgressSizes_.assign(numPartitions_, 0);
    readPartitionsFileIndex_.resize(numPartitions_);
    readPartitionsFileIndex_.assign(numPartitions_, 0);
    readPartitionFiles_.resize(numPartitions_);
    readPartitionFiles_.assign(numPartitions_, {});
    if (rootPath_ != "") {
      cleanup();
    }
    rootPath_ = std::move(rootPath);
    fileSystem_ = velox::filesystems::getFileSystem(rootPath_, nullptr);
  }

  void collect(int32_t partition, std::string_view data) override;

  void noMoreData(bool success) override;

  bool hasNext(int32_t partition) const override;

  velox::BufferPtr next(int32_t partition, bool success) override;

  bool readyForRead() const override;

  static TestingPersistentShuffle* instance() {
    return &kInstance_;
  }

 private:
  /// Find the next file for writing the next block of the partition.
  std::unique_ptr<velox::WriteFile> getNextOutputFile(int32_t partition);

  /// Return the number of stored files for a given partition.
  int getWritePartitionFilesCount(int32_t partition) const;

  /// Returns all created shuffle files for a given partition.
  std::vector<std::string> getReadPartitionFiles(int32_t partition) const;

  /// Write the in-progress block to the given partition.
  void storePartitionBlock(int32_t partition);

  /// Delete all the files in the root path.
  void cleanup();

  velox::memory::MemoryPool* pool_;
  uint32_t numPartitions_;
  const uint32_t maxBytesPerPartition_;
  /// The latest written block buffers and sizes.
  std::vector<velox::BufferPtr> inProgressPartitions_;
  std::vector<size_t> inProgressSizes_;
  /// The latest read block index of each partition.
  std::vector<size_t> readPartitionsFileIndex_;
  /// List of generated files for each partition.
  std::vector<std::vector<std::string>> readPartitionFiles_;
  /// The top directory of the shuffle files and its file system.
  std::string rootPath_;
  std::shared_ptr<velox::filesystems::FileSystem> fileSystem_;
  /// Thread Id is used to make sure files created by this thread have unique
  /// names.
  std::thread::id threadId_;
  /// Singleton always-alive object to be used for the purpose of testing.
  static TestingPersistentShuffle kInstance_;
};

} // namespace facebook::presto::operators