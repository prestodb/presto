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

/// This class is a persistent shuffle server that implements ShuffleInterface
/// for read and write and persists the shuffle state on local storage through
/// Velox file system interface.
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
class LocalPersistentShuffle : public ShuffleInterface {
 public:
  static constexpr folly::StringPiece kShuffleName{"local"};

  LocalPersistentShuffle(uint32_t maxBytesPerPartition)
      : maxBytesPerPartition_(maxBytesPerPartition),
        threadId_(std::this_thread::get_id()) {}

  /// Resets the shuffle state with the specified 'numPartitions'
  /// partitions and the root directory path 'rootPath'. This function can be
  /// called multiple times. If the current 'rootPath_' is not empty, the
  /// function also cleans up all the data files underneath it (but does not
  /// delete the old root director itself).
  void initialize(
      velox::memory::MemoryPool* FOLLY_NONNULL pool,
      uint32_t numPartitions,
      std::string rootPath);

  void collect(int32_t partition, std::string_view data) override;

  void noMoreData(bool success) override;

  bool hasNext(int32_t partition) const override;

  velox::BufferPtr next(int32_t partition, bool success) override;

  bool readyForRead() const override;

 private:
  // Finds and creates the next file for writing the next block of the
  // given 'partition'.
  std::unique_ptr<velox::WriteFile> getNextOutputFile(int32_t partition);

  // Returns the number of stored files for a given partition.
  int getWritePartitionFilesCount(int32_t partition) const;

  // Returns all created shuffle files for a given partition.
  std::vector<std::string> getReadPartitionFiles(int32_t partition) const;

  // Writes the in-progress block to the given partition.
  void storePartitionBlock(int32_t partition);

  // Deletes all the files in the root directory.
  void cleanup();

  const uint32_t maxBytesPerPartition_;

  velox::memory::MemoryPool* FOLLY_NONNULL pool_;
  uint32_t numPartitions_;
  // The latest written block buffers and sizes.
  std::vector<velox::BufferPtr> inProgressPartitions_;
  std::vector<size_t> inProgressSizes_;
  // The latest read block index of each partition.
  std::vector<size_t> readPartitionsFileIndex_;
  // List of generated files for each partition.
  std::vector<std::vector<std::string>> readPartitionFiles_;
  // The top directory of the shuffle files and its file system.
  std::string rootPath_;
  std::shared_ptr<velox::filesystems::FileSystem> fileSystem_;
  // Used to make sure files created by this thread have unique names.
  std::thread::id threadId_;
};

} // namespace facebook::presto::operators
