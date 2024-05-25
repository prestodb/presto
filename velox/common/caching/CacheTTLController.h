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

#include "velox/common/time/Timer.h"

#include "folly/Synchronized.h"
#include "folly/container/F14Map.h"
#include "folly/container/F14Set.h"

namespace facebook::velox::cache {

class AsyncDataCache;

struct RawFileInfo {
  int64_t openTimeSec;
  bool removeInProgress;

  bool operator==(const RawFileInfo& other) {
    return openTimeSec == other.openTimeSec &&
        removeInProgress == other.removeInProgress;
  }
};

struct CacheAgeStats {
  // Age in seconds of the oldest opened file loaded to the caches.
  int64_t maxAgeSecs{0};
};

/// A process-wide singleton to handle TTL of AsyncDataCache and SsdCache.
class CacheTTLController {
 public:
  /// Create and return a singleton instance of CacheTTLController.
  static CacheTTLController* create(AsyncDataCache& cache) {
    if (instance_ == nullptr) {
      instance_ =
          std::unique_ptr<CacheTTLController>(new CacheTTLController(cache));
    }
    return instance_.get();
  }

  /// Return the process-wide singleton instance of CacheTTLController if it has
  /// been created. Otherwise, return nullptr.
  static CacheTTLController* getInstance() {
    if (instance_ == nullptr) {
      return nullptr;
    }
    return instance_.get();
  }

  static void testingClear() {
    instance_ = nullptr;
  }

  /// Add file opening info for a file identified by fileNum. Return true if a
  /// new file entry is inserted, or if the existing file entry is updated
  /// while cache deletion for the file is in progress. Return false otherwise
  /// if the existing file entry is not updated.
  bool addOpenFileInfo(
      uint64_t fileNum,
      int64_t openTimeSec = getCurrentTimeSec());

  /// Compute age related stats of the cached files.
  CacheAgeStats getCacheAgeStats() const;

  void applyTTL(int64_t ttlSecs);

 private:
  /// A process-wide singleton instance of CacheTTLController.
  static std::unique_ptr<CacheTTLController> instance_;

 private:
  // Prevent creating a random instance of CacheTTLController.
  explicit CacheTTLController(AsyncDataCache& cache) : cache_(cache) {}

  folly::F14FastSet<uint64_t> getAndMarkAgedOutFiles(int64_t maxOpenTimeSecs);

  /// Clean up file entries with removeInProgress true but keep entries for
  /// fileNums in filesToRetain.
  void cleanUp(const folly::F14FastSet<uint64_t>& filesToRetain);

  void reset();

  AsyncDataCache& cache_;

  /// A Map of fileNum to RawFileInfo.
  folly::Synchronized<folly::F14FastMap<uint64_t, RawFileInfo>> fileInfoMap_;
};

} // namespace facebook::velox::cache
