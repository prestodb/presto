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

#include "velox/common/caching/SsdCache.h"
#include "velox/common/caching/SsdFile.h"
#include "velox/common/process/TraceContext.h"

#include <filesystem>

namespace facebook::velox::cache::test {

class SsdFileTestHelper {
 public:
  explicit SsdFileTestHelper(SsdFile* ssdFile) : ssdFile_(ssdFile) {}

  uint64_t writeFileSize() {
    return ssdFile_->writeFile_->size();
  }

  /// Returns true if copy on write is disabled for this file.
  bool isCowDisabled() const {
#ifdef linux
    const auto attributes = ssdFile_->writeFile_->getAttributes();
    const auto it =
        attributes.find(std::string(LocalWriteFile::Attributes::kNoCow));
    return it != attributes.end() && it->second == "true";
#else
    return false;
#endif // linux
  }

  std::vector<double> copyScores() {
    return ssdFile_->tracker_.copyScores();
  }

  int32_t numWritableRegions() const {
    return ssdFile_->writableRegions_.size();
  }

  const folly::F14FastMap<FileCacheKey, SsdRun>& eEntries() {
    return ssdFile_->entries_;
  }

  bool checksumReadVerificationEnabled() const {
    return ssdFile_->checksumReadVerificationEnabled_;
  }

  /// Deletes the backing file.
  void deleteFile() {
    process::TraceContext trace("SsdFile::testingDeleteFile");
    if (ssdFile_->writeFile_) {
      ssdFile_->writeFile_->close();
      ssdFile_->writeFile_.reset();
    }
    try {
      ssdFile_->fs_->remove(ssdFile_->fileName_);
    } catch (const std::exception& /*e*/) {
      VELOX_SSD_CACHE_LOG(ERROR)
          << "Failed to delete cache file " << ssdFile_->fileName_
          << ", error code: " << errno
          << ", error string: " << folly::errnoStr(errno);
    }
  }

 private:
  SsdFile* const ssdFile_;
};

class SsdCacheTestHelper {
 public:
  explicit SsdCacheTestHelper(SsdCache* ssdCache) : ssdCache_(ssdCache) {}

  int32_t numShards() {
    return ssdCache_->numShards_;
  }

  uint64_t writeFileSize(uint64_t fileId) {
    return ssdCache_->file(fileId).writeFile_->size();
  }

  /// Deletes backing files.
  void deleteFiles() {
    for (auto& file : ssdCache_->files_) {
      auto ssdFileHelper = SsdFileTestHelper(file.get());
      ssdFileHelper.deleteFile();
    }
  }

  /// Deletes checkpoint files.
  void deleteCheckpoints() {
    for (auto& file : ssdCache_->files_) {
      file->deleteCheckpoint();
    }
  }

  /// Returns the total size of eviction log files.
  uint64_t totalEvictionLogFilesSize() {
    uint64_t size = 0;
    for (auto& file : ssdCache_->files_) {
      std::filesystem::path p{file->evictLogFilePath()};
      size += std::filesystem::file_size(p);
    }
    return size;
  }

 private:
  SsdCache* const ssdCache_;
};

class AsyncDataCacheEntryTestHelper {
 public:
  explicit AsyncDataCacheEntryTestHelper(
      AsyncDataCacheEntry* asyncDataCacheEntry)
      : asyncDataCacheEntry_(asyncDataCacheEntry) {}

  const AccessStats& accessStats() const {
    return asyncDataCacheEntry_->accessStats_;
  }

  bool firstUse() const {
    return asyncDataCacheEntry_->isFirstUse_;
  }

 private:
  AsyncDataCacheEntry* const asyncDataCacheEntry_;
};

class CacheShardTestHelper {
 public:
  explicit CacheShardTestHelper(CacheShard* cacheShard)
      : cacheShard_(cacheShard) {}

  std::vector<AsyncDataCacheEntry*> cacheEntries() const {
    std::vector<AsyncDataCacheEntry*> entries;
    std::lock_guard<std::mutex> l(cacheShard_->mutex_);
    entries.reserve(cacheShard_->entries_.size());
    for (const auto& entry : cacheShard_->entries_) {
      entries.push_back(entry.get());
    }
    return entries;
  }

 private:
  CacheShard* const cacheShard_;
};

class AsyncDataCacheTestHelper {
 public:
  explicit AsyncDataCacheTestHelper(AsyncDataCache* asyncDataCache)
      : asyncDataCache_(asyncDataCache) {}

  std::vector<AsyncDataCacheEntry*> cacheEntries() const {
    std::vector<AsyncDataCacheEntry*> totalEntries;
    for (const auto& shard : asyncDataCache_->shards_) {
      auto shardHelper = CacheShardTestHelper(shard.get());
      const auto shardEntries = shardHelper.cacheEntries();
      std::copy(
          shardEntries.begin(),
          shardEntries.end(),
          std::back_inserter(totalEntries));
    }
    return totalEntries;
  }

  uint64_t ssdSavable() const {
    return asyncDataCache_->ssdSaveable_;
  }

  int32_t numShards() const {
    return asyncDataCache_->shards_.size();
  }

 private:
  AsyncDataCache* const asyncDataCache_;
};
} // namespace facebook::velox::cache::test
