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

#include "velox/common/base/SpillStats.h"
#include "velox/common/file/FileInputStream.h"
#include "velox/exec/ExchangeQueue.h"
#include "velox/exec/SpillFile.h"

namespace facebook::velox::exec {
namespace test {
class SerializedPageSpillerHelper;
class SerializedPageSpillReaderHelper;
} // namespace test

/// Used for spilling a sequence of 'SerializedPage'. The spiller preserves the
/// order of the pages.
class SerializedPageSpiller : public SpillWriterBase {
 public:
  struct Result {
    SpillFiles spillFiles;
    uint64_t totalPages;
  };

  SerializedPageSpiller(
      uint64_t writeBufferSize,
      uint64_t targetFileSize,
      const std::string& pathPrefix,
      const std::string& fileCreateConfig,
      common::UpdateAndCheckSpillLimitCB& updateAndCheckSpillLimitCb,
      memory::MemoryPool* pool,
      folly::Synchronized<common::SpillStats>* stats)
      : SpillWriterBase(
            writeBufferSize,
            targetFileSize,
            pathPrefix,
            fileCreateConfig,
            updateAndCheckSpillLimitCb,
            pool,
            stats) {
    VELOX_CHECK_NOT_NULL(pool_);
  }

  /// Spill 'pages' to disk. The method does not free the original in-memory
  /// structure of 'pages'. It is caller's responsibility to free them.
  void spill(const std::vector<std::shared_ptr<SerializedPage>>& pages);

  /// Finishes the spilling and return the spilled result.
  Result finishSpill();

 private:
  void flushBuffer(
      SpillWriteFile* file,
      uint64_t& writtenBytes,
      uint64_t& flushTimeNs,
      uint64_t& writeTimeNs) override;

  bool bufferEmpty() const override;

  uint64_t bufferSize() const override;

  void addFinishedFile(SpillWriteFile* file) override;

  uint64_t totalBytes_{0};

  uint64_t totalPages_{0};

  std::unique_ptr<IOBufOutputStream> bufferStream_;

  friend class test::SerializedPageSpillerHelper;
};

/// Used for reading a sequence of 'SerializedPage' that were spilled by
/// 'SerializedPageSpiller'. The reader preserves the page order, and provides
/// random index access functionality in a load-on-read manner. It is used by
/// 'DestinationBuffer' and provides convenient APIs for reading and deleting
/// the pages, with unspilling handled transparently.
class SerializedPageSpillReader {
 public:
  SerializedPageSpillReader(
      SerializedPageSpiller::Result&& spillResult,
      uint64_t readBufferSize,
      memory::MemoryPool* pool,
      folly::Synchronized<common::SpillStats>* spillStats)
      : readBufferSize_(readBufferSize),
        pool_(pool),
        spillStats_(spillStats),
        numPages_(spillResult.totalPages) {
    for (const auto& spillFileInfo : spillResult.spillFiles) {
      spillFilePaths_.push_back(spillFileInfo.path);
    }
  }

  /// Returns true if there are any remaining pages left to read.
  bool empty() const;

  /// Returns the current number of pages in the reader.
  uint64_t numPages() const;

  /// Returns the page at 'index' in the reader.
  std::shared_ptr<SerializedPage> at(uint64_t index);

  /// Delete 'numPages' from the front.
  void deleteFront(uint64_t numPages);

  /// Delete all pages from the reader.
  void deleteAll();

 private:
  // Ensures the current file stream is open. If not, opens the next file.
  void ensureSpillFile();

  // Ensures all pages up to 'index' are loaded in memory.
  void ensurePages(uint64_t index);

  // Unspills one serialized page and returns it.
  std::shared_ptr<SerializedPage> unspillNextPage();

  const uint64_t readBufferSize_;

  memory::MemoryPool* const pool_;

  folly::Synchronized<common::SpillStats>* const spillStats_;

  // The current file stream.
  std::unique_ptr<common::FileInputStream> curFileStream_;

  // A small number of front pages buffered in memory from spilled pages.
  // These pages will be kept in memory and won't be spilled again.
  std::vector<std::shared_ptr<SerializedPage>> bufferedPages_;

  std::deque<std::string> spillFilePaths_;

  uint64_t numPages_;

  friend class test::SerializedPageSpillReaderHelper;
};
} // namespace facebook::velox::exec
