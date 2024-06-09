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

#include "velox/common/base/SpillConfig.h"
#include "velox/common/base/SpillStats.h"
#include "velox/common/compression/Compression.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/TreeOfLosers.h"
#include "velox/exec/UnorderedStreamReader.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/VectorStream.h"

namespace facebook::velox::exec {

/// Represents a spill file for writing the serialized spilled data into a disk
/// file.
class SpillWriteFile {
 public:
  static std::unique_ptr<SpillWriteFile> create(
      uint32_t id,
      const std::string& pathPrefix,
      const std::string& fileCreateConfig);

  uint32_t id() const {
    return id_;
  }

  /// Returns the file size in bytes.
  uint64_t size() const;

  const std::string& path() const {
    return path_;
  }

  uint64_t write(std::unique_ptr<folly::IOBuf> iobuf);

  WriteFile* file() {
    return file_.get();
  }

  /// Finishes writing and flushes any unwritten data.
  void finish();

 private:
  static inline std::atomic<int32_t> ordinalCounter_{0};

  SpillWriteFile(
      uint32_t id,
      const std::string& pathPrefix,
      const std::string& fileCreateConfig);

  // The spill file id which is monotonically increasing and unique for each
  // associated spill partition.
  const uint32_t id_;
  const std::string path_;

  std::unique_ptr<WriteFile> file_;
  // Byte size of the backing file. Set when finishing writing.
  uint64_t size_{0};
};

/// Records info of a finished spill file which is used for read.
struct SpillFileInfo {
  uint32_t id;
  RowTypePtr type;
  std::string path;
  /// The file size in bytes.
  uint64_t size;
  uint32_t numSortKeys;
  std::vector<CompareFlags> sortFlags;
  common::CompressionKind compressionKind;
};

using SpillFiles = std::vector<SpillFileInfo>;

/// Used to write the spilled data to a sequence of files for one partition. If
/// data is sorted, each file is sorted. The globally sorted order is produced
/// by merging the constituent files.
class SpillWriter {
 public:
  /// 'type' is a RowType describing the content. 'numSortKeys' is the number
  /// of leading columns on which the data is sorted. 'path' is a file path
  /// prefix. ' 'targetFileSize' is the target byte size of a single file.
  /// 'writeBufferSize' specifies the size limit of the buffered data before
  /// write to file. 'fileOptions' specifies the file layout on remote storage
  /// which is storage system specific. 'pool' is used for buffering and
  /// constructing the result data read from 'this'. 'stats' is used to collect
  /// the spill write stats.
  ///
  /// When writing sorted spill runs, the caller is responsible for buffering
  /// and sorting the data. write is called multiple times, followed by flush().
  SpillWriter(
      const RowTypePtr& type,
      const uint32_t numSortKeys,
      const std::vector<CompareFlags>& sortCompareFlags,
      common::CompressionKind compressionKind,
      const std::string& pathPrefix,
      uint64_t targetFileSize,
      uint64_t writeBufferSize,
      const std::string& fileCreateConfig,
      common::UpdateAndCheckSpillLimitCB& updateAndCheckSpillLimitCb,
      memory::MemoryPool* pool,
      folly::Synchronized<common::SpillStats>* stats);

  /// Adds 'rows' for the positions in 'indices' into 'this'. The indices
  /// must produce a view where the rows are sorted if sorting is desired.
  /// Consecutive calls must have sorted data so that the first row of the
  /// next call is not less than the last row of the previous call.
  /// Returns the size to write.
  uint64_t write(
      const RowVectorPtr& rows,
      const folly::Range<IndexRange*>& indices);

  /// Closes the current output file if any. Subsequent calls to write will
  /// start a new one.
  void finishFile();

  /// Returns the number of current finished files.
  size_t numFinishedFiles() const;

  /// Finishes this file writer and returns the written spill files info.
  ///
  /// NOTE: we don't allow write to a spill writer after t
  SpillFiles finish();

  std::vector<std::string> testingSpilledFilePaths() const;

  std::vector<uint32_t> testingSpilledFileIds() const;

 private:
  FOLLY_ALWAYS_INLINE void checkNotFinished() const {
    VELOX_CHECK(!finished_, "SpillWriter has finished");
  }

  // Returns an open spill file for write. If there is no open spill file, then
  // the function creates a new one. If the current open spill file exceeds the
  // target file size limit, then it first closes the current one and then
  // creates a new one. 'currentFile_' points to the current open spill file.
  SpillWriteFile* ensureFile();

  // Closes the current open spill file pointed by 'currentFile_'.
  void closeFile();

  // Writes data from 'batch_' to the current output file. Returns the actual
  // written size.
  uint64_t flush();

  // Invoked to increment the number of spilled files and the file size.
  void updateSpilledFileStats(uint64_t fileSize);

  // Invoked to update the number of spilled rows.
  void updateAppendStats(uint64_t numRows, uint64_t serializationTimeUs);

  // Invoked to update the disk write stats.
  void updateWriteStats(
      uint64_t spilledBytes,
      uint64_t flushTimeUs,
      uint64_t writeTimeUs);

  const RowTypePtr type_;
  const uint32_t numSortKeys_;
  const std::vector<CompareFlags> sortCompareFlags_;
  const common::CompressionKind compressionKind_;
  const std::string pathPrefix_;
  const uint64_t targetFileSize_;
  const uint64_t writeBufferSize_;
  const std::string fileCreateConfig_;

  // Updates the aggregated spill bytes of this query, and throws if exceeds
  // the max spill bytes limit.
  common::UpdateAndCheckSpillLimitCB updateAndCheckSpillLimitCb_;
  memory::MemoryPool* const pool_;
  folly::Synchronized<common::SpillStats>* const stats_;

  bool finished_{false};
  uint32_t nextFileId_{0};
  std::unique_ptr<VectorStreamGroup> batch_;
  std::unique_ptr<SpillWriteFile> currentFile_;
  SpillFiles finishedFiles_;
};

/// Input stream backed by spill file.
///
/// TODO Usage of ByteInputStream as base class is hacky and just happens to
/// work. For example, ByteInputStream::size(), seekp(), tellp(),
/// remainingSize() APIs do not work properly.
class SpillInputStream : public ByteInputStream {
 public:
  /// Reads from 'input' using 'buffer' for buffering reads.
  SpillInputStream(
      std::unique_ptr<ReadFile>&& file,
      uint64_t bufferSize,
      memory::MemoryPool* pool,
      folly::Synchronized<common::SpillStats>* stats);

  ~SpillInputStream() override;

  /// True if all of the file has been read into vectors.
  bool atEnd() const override {
    return offset_ >= fileSize_ && ranges()[0].position >= ranges()[0].size;
  }

 private:
  void updateSpillStats(uint64_t readBytes, uint64_t readTimeUs) const;

  void next(bool throwIfPastEnd) override;

  // Issues readahead if underlying fs supports async mode read.
  //
  // TODO: we might consider to use AsyncSource to support read-ahead on
  // filesystem which doesn't support async mode read.
  void maybeIssueReadahead();

  inline uint32_t bufferIndex() const {
    return bufferIndex_;
  }

  inline uint32_t nextBufferIndex() const {
    return (bufferIndex_ + 1) % buffers_.size();
  }

  // Advances buffer index to point to the next buffer for read.
  inline void advanceBuffer() {
    bufferIndex_ = nextBufferIndex();
  }

  inline Buffer* buffer() const {
    return buffers_[bufferIndex()].get();
  }

  inline Buffer* nextBuffer() const {
    return buffers_[nextBufferIndex()].get();
  }

  // Returns the next read size in bytes.
  inline uint64_t readSize() const;

  const std::unique_ptr<ReadFile> file_;
  const uint64_t fileSize_;
  const uint64_t bufferSize_;
  memory::MemoryPool* const pool_;
  const bool readaEnabled_;
  folly::Synchronized<common::SpillStats>* const stats_;

  std::vector<BufferPtr> buffers_;
  uint32_t bufferIndex_{0};
  // Sets to read-ahead future if valid.
  folly::SemiFuture<uint64_t> readaWait_{
      folly::SemiFuture<uint64_t>::makeEmpty()};
  // Offset of first byte not in 'buffer()'.
  uint64_t offset_ = 0;
};

/// Represents a spill file for read which turns the serialized spilled data on
/// disk back into a sequence of spilled row vectors.
///
/// NOTE: The class will not delete spill file upon destruction, so the user
/// needs to remove the unused spill files at some point later. For example, a
/// query Task deletes all the generated spill files in one operation using
/// rmdir() call.
class SpillReadFile {
 public:
  static std::unique_ptr<SpillReadFile> create(
      const SpillFileInfo& fileInfo,
      uint64_t bufferSize,
      memory::MemoryPool* pool,
      folly::Synchronized<common::SpillStats>* stats);

  uint32_t id() const {
    return id_;
  }

  int32_t numSortKeys() const {
    return numSortKeys_;
  }

  const std::vector<CompareFlags>& sortCompareFlags() const {
    return sortCompareFlags_;
  }

  bool nextBatch(RowVectorPtr& rowVector);

  /// Returns the file size in bytes.
  uint64_t size() const {
    return size_;
  }

  const std::string& testingFilePath() const {
    return path_;
  }

 private:
  SpillReadFile(
      uint32_t id,
      const std::string& path,
      uint64_t size,
      uint64_t bufferSize,
      const RowTypePtr& type,
      uint32_t numSortKeys,
      const std::vector<CompareFlags>& sortCompareFlags,
      common::CompressionKind compressionKind,
      memory::MemoryPool* pool,
      folly::Synchronized<common::SpillStats>* stats);

  // The spill file id which is monotonically increasing and unique for each
  // associated spill partition.
  const uint32_t id_;
  const std::string path_;
  // The file size in bytes.
  const uint64_t size_;
  // The data type of spilled data.
  const RowTypePtr type_;
  const uint32_t numSortKeys_;
  const std::vector<CompareFlags> sortCompareFlags_;
  const common::CompressionKind compressionKind_;
  const serializer::presto::PrestoVectorSerde::PrestoOptions readOptions_;
  memory::MemoryPool* const pool_;
  folly::Synchronized<common::SpillStats>* const stats_;

  std::unique_ptr<SpillInputStream> input_;
};
} // namespace facebook::velox::exec
