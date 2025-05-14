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
#include "velox/common/file/FileInputStream.h"
#include "velox/exec/TreeOfLosers.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/VectorStream.h"

namespace facebook::velox::exec {
using SpillSortKey = std::pair<column_index_t, CompareFlags>;

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
  std::vector<SpillSortKey> sortingKeys;
  common::CompressionKind compressionKind;
};

using SpillFiles = std::vector<SpillFileInfo>;

/// Used to write the spilled data to a sequence of files for one partition.
/// This base class provides the functionality of managing buffer and write
/// files. The derived classes are responsible for:
/// 1. Creating write API to accommodate the type of data to be spilled.
/// 2. Implementing various buffer APIs and manage the buffer.
class SpillWriterBase {
 public:
  using WriteCb = std::function<uint64_t()>;

  SpillWriterBase(
      uint64_t writeBufferSize,
      uint64_t targetFileSize,
      const std::string& pathPrefix,
      const std::string& fileCreateConfig,
      common::UpdateAndCheckSpillLimitCB& updateAndCheckSpillLimitCb,
      memory::MemoryPool* pool,
      folly::Synchronized<common::SpillStats>* stats);

  virtual ~SpillWriterBase() = default;

  /// Finishes the current file writing.
  void finishFile();

  /// Finishes this file writer. Further writes to this spill writer are not
  /// allowed after this call.
  SpillFiles finish();

  uint64_t numFinishedFiles() const {
    return finishedFiles_.size();
  }

 protected:
  virtual void flushBuffer(
      SpillWriteFile* file,
      uint64_t& writtenBytes,
      uint64_t& flushTimeNs,
      uint64_t& writeTimeNs) = 0;

  virtual bool bufferEmpty() const = 0;

  virtual uint64_t bufferSize() const = 0;

  virtual void addFinishedFile(SpillWriteFile* file) = 0;

  // Write wrapper with buffer control. Derived class needs to implement
  // 'writeCb' that needs to only write data to buffer, without taking care of
  // the buffer limit. 'args' are the arguments to be passed to 'writeCb'.
  uint64_t writeWithBufferControl(const std::function<uint64_t()>& writeCb);

  // Writes data from buffer to the current output file. Returns the actual
  // written size.
  uint64_t flush();

  FOLLY_ALWAYS_INLINE void checkNotFinished() const {
    VELOX_CHECK(!finished_, "SpillWriter has finished");
  }

  memory::MemoryPool* const pool_;

  folly::Synchronized<common::SpillStats>* const stats_;

  std::unique_ptr<SpillWriteFile> currentFile_;

  SpillFiles finishedFiles_;

 private:
  // Returns an open spill file for write. If there is no open spill file, then
  // the function creates a new one. If the current open spill file exceeds the
  // target file size limit, then it first closes the current one and then
  // creates a new one. 'currentFile_' points to the current open spill file.
  SpillWriteFile* ensureFile();

  // Closes the current open spill file pointed by 'currentFile_'.
  void closeFile();

  // Invoked to update the disk write stats.
  void updateWriteStats(
      uint64_t spilledBytes,
      uint64_t flushTimeNs,
      uint64_t writeTimeNs);

  // Invoked to increment the number of spilled files and the file size.
  void updateSpilledFileStats(uint64_t fileSize);

  // Invoked to update the number of spilled rows.
  void updateAppendStats(uint64_t numRows, uint64_t serializationTimeUs);

  // Updates the aggregated spill bytes of this query, and throws if exceeds
  // the max spill bytes limit.
  const common::UpdateAndCheckSpillLimitCB updateAndCheckSpillLimitCb_;

  const std::string fileCreateConfig_;

  const std::string pathPrefix_;

  const uint64_t writeBufferSize_;

  const uint64_t targetFileSize_;

  uint64_t nextFileId_{0};

  bool finished_{false};
};

/// If data is sorted, each file is sorted. The globally sorted order is
/// produced by merging the constituent files.
class SpillWriter : public SpillWriterBase {
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
      const std::vector<SpillSortKey>& sortingKeys,
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

  std::vector<std::string> testingSpilledFilePaths() const;

  std::vector<uint32_t> testingSpilledFileIds() const;

 private:
  bool bufferEmpty() const override {
    return batch_ == nullptr;
  }

  uint64_t bufferSize() const override {
    return batch_->size();
  }

  void flushBuffer(
      SpillWriteFile* file,
      uint64_t& writtenBytes,
      uint64_t& flushTimeNs,
      uint64_t& writeTimeNs) override;

  void addFinishedFile(SpillWriteFile* file) override;

  const RowTypePtr type_;

  const std::vector<SpillSortKey> sortingKeys_;

  const common::CompressionKind compressionKind_;

  VectorSerde* const serde_;

  std::unique_ptr<VectorStreamGroup> batch_;
};

/// Represents a spill file for read which turns the serialized spilled data
/// on disk back into a sequence of spilled row vectors.
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

  const std::vector<SpillSortKey>& sortingKeys() const {
    return sortingKeys_;
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
      const std::vector<SpillSortKey>& sortingKeys,
      common::CompressionKind compressionKind,
      memory::MemoryPool* pool,
      folly::Synchronized<common::SpillStats>* stats);

  // Invoked to record spill read stats at the end of read input.
  void recordSpillStats();

  // The spill file id which is monotonically increasing and unique for each
  // associated spill partition.
  const uint32_t id_;
  const std::string path_;
  // The file size in bytes.
  const uint64_t size_;
  // The data type of spilled data.
  const RowTypePtr type_;
  const std::vector<SpillSortKey> sortingKeys_;
  const common::CompressionKind compressionKind_;
  const serializer::presto::PrestoVectorSerde::PrestoOptions readOptions_;
  memory::MemoryPool* const pool_;
  VectorSerde* const serde_;
  folly::Synchronized<common::SpillStats>* const stats_;

  std::unique_ptr<common::FileInputStream> input_;
};
} // namespace facebook::velox::exec
