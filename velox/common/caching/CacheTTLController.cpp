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

#include "velox/common/caching/CacheTTLController.h"

#include "velox/common/caching/AsyncDataCache.h"

namespace facebook::velox::cache {

std::unique_ptr<CacheTTLController> CacheTTLController::instance_ = nullptr;

bool CacheTTLController::addOpenFileInfo(
    uint64_t fileNum,
    int64_t openTimeSec) {
  auto lockedFileMap = fileInfoMap_.wlock();
  auto it = lockedFileMap->find(fileNum);
  if (it == lockedFileMap->end() || it->second.removeInProgress) {
    lockedFileMap->insert_or_assign(fileNum, RawFileInfo{openTimeSec, false});
    return true;
  }
  return false;
}

CacheAgeStats CacheTTLController::getCacheAgeStats() const {
  auto lockedFileMap = fileInfoMap_.rlock();

  if (lockedFileMap->empty()) {
    return CacheAgeStats{.maxAgeSecs = 0};
  }

  // Use the oldest file open time to calculate the max possible age of cache
  // entries loaded from the files.
  int64_t minOpenTime = std::numeric_limits<int64_t>::max();
  for (auto it = lockedFileMap->cbegin(); it != lockedFileMap->cend(); it++) {
    minOpenTime = std::min<int64_t>(minOpenTime, it->second.openTimeSec);
  }

  int64_t maxAge = getCurrentTimeSec() - minOpenTime;
  return CacheAgeStats{.maxAgeSecs = std::max<int64_t>(maxAge, 0)};
}

void CacheTTLController::applyTTL(int64_t ttlSecs) {
  int64_t maxOpenTime = getCurrentTimeSec() - ttlSecs;

  folly::F14FastSet<uint64_t> filesToRemove =
      getAndMarkAgedOutFiles(maxOpenTime);
  if (filesToRemove.empty()) {
    LOG(INFO) << "No cache entry is out of TTL " << ttlSecs << ".";
    return;
  }

  folly::F14FastSet<uint64_t> filesRetained;
  bool success = cache_.removeFileEntries(filesToRemove, filesRetained);

  LOG(INFO) << (success ? "Succeeded" : "Failed") << " applying cache TTL of "
            << ttlSecs << " seconds. Entries from " << filesToRemove.size()
            << " files are to be removed, while " << filesRetained.size()
            << " files are retained";
  if (success) {
    cleanUp(filesRetained);
  } else {
    reset();
  }
}

folly::F14FastSet<uint64_t> CacheTTLController::getAndMarkAgedOutFiles(
    int64_t maxOpenTimeSecs) {
  auto lockedFileMap = fileInfoMap_.wlock();

  folly::F14FastSet<uint64_t> fileNums;

  for (auto it = lockedFileMap->begin(); it != lockedFileMap->end(); it++) {
    if (it->second.removeInProgress ||
        it->second.openTimeSec < maxOpenTimeSecs) {
      fileNums.insert(it->first);
      it->second.removeInProgress = true;
    }
  }

  return fileNums;
}

void CacheTTLController::cleanUp(
    const folly::F14FastSet<uint64_t>& filesToRetain) {
  fileInfoMap_.withWLock([&](auto& fileMap) {
    auto it = fileMap.begin();
    while (it != fileMap.end()) {
      if (!it->second.removeInProgress) {
        it++;
        continue;
      }
      if (filesToRetain.count(it->first) > 0) {
        it->second.removeInProgress = false;
        it++;
        continue;
      }
      it = fileMap.erase(it);
    }
  });
}

void CacheTTLController::reset() {
  fileInfoMap_.withWLock([](auto& fileMap) {
    for (auto& [_, fileInfo] : fileMap) {
      fileInfo.removeInProgress = false;
    }
  });
}

} // namespace facebook::velox::cache
