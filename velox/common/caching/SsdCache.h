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

#include "velox/common/caching/SsdFile.h"

namespace facebook::velox::cache {

#define VELOX_SSD_CACHE_LOG_PREFIX "[SSDCA] "
#define VELOX_SSD_CACHE_LOG(severity) \
  LOG(severity) << VELOX_SSD_CACHE_LOG_PREFIX

class SsdCache {
 public:
  /// Constructs a cache with backing files at path 'filePrefix'.<ordinal>.
  /// <ordinal> ranges from 0 to 'numShards' - 1.
  /// 'maxBytes' is the total capacity of the cache. This is rounded up to the
  /// next multiple of kRegionSize * 'numShards'. This means that all the shards
  /// have an equal number of regions. For 2 shards and 200MB size, the size
  /// rounds up to 256M with 2 shards each of 128M (2 regions).
  /// If 'checkpointIntervalBytes' is non-zero, the cache makes a durable
  /// checkpointed state that survives restart after each
  /// 'checkpointIntervalBytes' written.
  /// If 'disableFileCow' is true, the cache disables the file COW (copy on
  /// write) feature if the underlying filesystem (such as brtfs) supports it.
  /// This prevents the actual cache space usage on disk from exceeding the
  /// 'maxBytes' limit and stop working.
  SsdCache(
      std::string_view filePrefix,
      uint64_t maxBytes,
      int32_t numShards,
      folly::Executor* executor,
      int64_t checkpointIntervalBytes = 0,
      bool disableFileCow = false);

  /// Returns the shard corresponding to 'fileId'. 'fileId' is a file id from
  /// e.g. FileCacheKey.
  SsdFile& file(uint64_t fileId);

  /// Returns the maximum capacity, rounded up from the capacity passed to the
  /// constructor.
  uint64_t maxBytes() const {
    return files_[0]->maxRegions() * SsdFile::kRegionSize * files_.size();
  }

  /// Returns true if no write is in progress. Atomically sets a write to be in
  /// progress. write() must be called after this. The writing state is reset
  /// asynchronously after writing to SSD finishes.
  bool startWrite();

  bool writeInProgress() const {
    return writesInProgress_ != 0;
  }

  /// Stores the entries of 'pins' into the corresponding files. Sets the file
  /// for the successfully stored entries. May evict existing entries from
  /// unpinned regions. startWrite() must have been called first and it must
  /// have returned true.
  void write(std::vector<CachePin> pins);

  /// Removes cached entries from all SsdFiles for files in the fileNum set
  /// 'filesToRemove'. If successful, return true, and 'filesRetained' contains
  /// entries that should not be removed, ex., from pinned regions. Otherwise,
  /// return false and 'filesRetained' could be ignored.
  bool removeFileEntries(
      const folly::F14FastSet<uint64_t>& filesToRemove,
      folly::F14FastSet<uint64_t>& filesRetained);

  /// Returns stats aggregated from all shards.
  SsdCacheStats stats() const;

  FileGroupStats& groupStats() const {
    return *groupStats_;
  }

  /// Drops all entries. Outstanding pins become invalid but reading them will
  /// mostly succeed since the files will not be rewritten until new content is
  /// stored.
  void testingClear();

  /// Deletes backing files. Used in testing.
  void testingDeleteFiles();

  /// Stops writing to the cache files and waits for pending writes to finish.
  /// If checkpointing is on, makes a checkpoint.
  void shutdown();

  std::string toString() const;

  const std::string& filePrefix() const {
    return filePrefix_;
  }

 private:
  const std::string filePrefix_;
  const int32_t numShards_;
  // Stats for selecting entries to save from AsyncDataCache.
  const std::unique_ptr<FileGroupStats> groupStats_;
  folly::Executor* const executor_;

  std::vector<std::unique_ptr<SsdFile>> files_;

  // Count of shards with unfinished writes.
  std::atomic_int32_t writesInProgress_{0};
  std::atomic_bool isShutdown_{false};
};

} // namespace facebook::velox::cache
