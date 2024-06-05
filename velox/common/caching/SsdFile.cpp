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

#include "velox/common/caching/SsdFile.h"

#include <folly/Executor.h>
#include <folly/portability/SysUio.h>
#include "velox/common/base/AsyncSource.h"
#include "velox/common/base/Crc.h"
#include "velox/common/base/SuccinctPrinter.h"
#include "velox/common/caching/FileIds.h"
#include "velox/common/caching/SsdCache.h"
#include "velox/common/process/TraceContext.h"

#include <fcntl.h>
#ifdef linux
#include <linux/fs.h>
#endif // linux
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fstream>
#include <numeric>

DEFINE_bool(ssd_odirect, true, "Use O_DIRECT for SSD cache IO");
DEFINE_bool(ssd_verify_write, false, "Read back data after writing to SSD");

namespace facebook::velox::cache {

namespace {
// Disable 'copy on write' on the given file. Will throw if failed for any
// reason, including file system not supporting cow feature.
void disableCow(int32_t fd) {
#ifdef linux
  int attr{0};
  auto res = ioctl(fd, FS_IOC_GETFLAGS, &attr);
  VELOX_CHECK_EQ(
      0,
      res,
      "ioctl(FS_IOC_GETFLAGS) failed: {}, {}",
      res,
      folly::errnoStr(errno));
  attr |= FS_NOCOW_FL;
  res = ioctl(fd, FS_IOC_SETFLAGS, &attr);
  VELOX_CHECK_EQ(
      0,
      res,
      "ioctl(FS_IOC_SETFLAGS, FS_NOCOW_FL) failed: {}, {}",
      res,
      folly::errnoStr(errno));
#endif // linux
}

void addEntryToIovecs(AsyncDataCacheEntry& entry, std::vector<iovec>& iovecs) {
  if (entry.tinyData() != nullptr) {
    iovecs.push_back({entry.tinyData(), static_cast<size_t>(entry.size())});
    return;
  }
  const auto& data = entry.data();
  iovecs.reserve(iovecs.size() + data.numRuns());
  int64_t bytesLeft = entry.size();
  for (auto i = 0; i < data.numRuns(); ++i) {
    auto run = data.runAt(i);
    iovecs.push_back(
        {run.data<char>(), std::min<size_t>(bytesLeft, run.numBytes())});
    bytesLeft -= run.numBytes();
    if (bytesLeft <= 0) {
      break;
    };
  }
}
} // namespace

SsdPin::SsdPin(SsdFile& file, SsdRun run) : file_(&file), run_(run) {
  file_->checkPinned(run_.offset());
}

SsdPin::~SsdPin() {
  clear();
}

void SsdPin::clear() {
  if (file_) {
    file_->unpinRegion(run_.offset());
  }
  file_ = nullptr;
}

void SsdPin::operator=(SsdPin&& other) {
  if (file_ != nullptr) {
    file_->unpinRegion(run_.offset());
  }
  file_ = other.file_;
  other.file_ = nullptr;
  run_ = other.run_;
}

std::string SsdPin::toString() const {
  if (empty()) {
    return "<empty SsdPin>";
  }
  return fmt::format(
      "SsdPin(shard {} offset {} size {})",
      file_->shardId(),
      run_.offset(),
      run_.size());
}

SsdFile::SsdFile(const Config& config)
    : fileName_(config.fileName),
      maxRegions_(config.maxRegions),
      disableFileCow_(config.disableFileCow),
      checksumEnabled_(config.checksumEnabled),
      checksumReadVerificationEnabled_(
          config.checksumEnabled && config.checksumReadVerificationEnabled),
      shardId_(config.shardId),
      checkpointIntervalBytes_(config.checkpointIntervalBytes),
      executor_(config.executor) {
  process::TraceContext trace("SsdFile::SsdFile");
  int32_t oDirect = 0;
#ifdef linux
  oDirect = FLAGS_ssd_odirect ? O_DIRECT : 0;
#endif // linux
  fd_ = open(fileName_.c_str(), O_CREAT | O_RDWR | oDirect, S_IRUSR | S_IWUSR);
  if (fd_ < 0) {
    ++stats_.openFileErrors;
  }
  // TODO: add fault tolerant handling for open file errors.
  VELOX_CHECK_GE(
      fd_,
      0,
      "Cannot open or create {}. Error: {}",
      fileName_,
      folly::errnoStr(errno));

  if (disableFileCow_) {
    disableCow(fd_);
  }

  readFile_ = std::make_unique<LocalReadFile>(fd_);
  const uint64_t size = lseek(fd_, 0, SEEK_END);
  numRegions_ = std::min<int32_t>(size / kRegionSize, maxRegions_);
  fileSize_ = numRegions_ * kRegionSize;
  if ((size % kRegionSize > 0) || (size > numRegions_ * kRegionSize)) {
    ::ftruncate(fd_, fileSize_);
  }
  // The existing regions in the file are writable.
  writableRegions_.resize(numRegions_);
  std::iota(writableRegions_.begin(), writableRegions_.end(), 0);
  tracker_.resize(maxRegions_);
  regionSizes_.resize(maxRegions_, 0);
  erasedRegionSizes_.resize(maxRegions_, 0);
  regionPins_.resize(maxRegions_, 0);
  if (checkpointEnabled()) {
    initializeCheckpoint();
  }
}

void SsdFile::pinRegion(uint64_t offset) {
  std::lock_guard<std::shared_mutex> l(mutex_);
  pinRegionLocked(offset);
}

void SsdFile::unpinRegion(uint64_t offset) {
  std::lock_guard<std::shared_mutex> l(mutex_);
  const auto count = --regionPins_[regionIndex(offset)];
  VELOX_CHECK_GE(count, 0);
  if (suspended_ && count == 0) {
    growOrEvictLocked();
  }
}

SsdPin SsdFile::find(RawFileCacheKey key) {
  FileCacheKey ssdKey{StringIdLease(fileIds(), key.fileNum), key.offset};
  SsdRun run;
  {
    std::lock_guard<std::shared_mutex> l(mutex_);
    if (suspended_) {
      return SsdPin();
    }
    tracker_.fileTouched(entries_.size());
    auto it = entries_.find(ssdKey);
    if (it == entries_.end()) {
      return SsdPin();
    }
    run = it->second;
    pinRegionLocked(run.offset());
  }
  return SsdPin(*this, run);
}

bool SsdFile::erase(RawFileCacheKey key) {
  FileCacheKey ssdKey{StringIdLease(fileIds(), key.fileNum), key.offset};
  std::lock_guard<std::shared_mutex> l(mutex_);
  const auto it = entries_.find(ssdKey);
  if (it == entries_.end()) {
    return false;
  }
  entries_.erase(it);
  return true;
}

CoalesceIoStats SsdFile::load(
    const std::vector<SsdPin>& ssdPins,
    const std::vector<CachePin>& pins) {
  VELOX_CHECK_EQ(ssdPins.size(), pins.size());
  if (pins.empty()) {
    return CoalesceIoStats();
  }
  size_t totalPayloadBytes = 0;
  for (auto i = 0; i < pins.size(); ++i) {
    const auto runSize = ssdPins[i].run().size();
    auto* entry = pins[i].checkedEntry();
    if (FOLLY_UNLIKELY(runSize < entry->size())) {
      ++stats_.readSsdErrors;
      VELOX_FAIL(
          "IOERR: SSD cache cache entry {} short than requested range {}",
          succinctBytes(runSize),
          succinctBytes(entry->size()));
    }
    totalPayloadBytes += entry->size();
    regionRead(regionIndex(ssdPins[i].run().offset()), runSize);
    ++stats_.entriesRead;
    stats_.bytesRead += entry->size();
  }

  // Do coalesced IO for the pins. For short payloads, the break-even between
  // discrete pread calls and a single preadv that discards gaps is ~25K per
  // gap. For longer payloads this is ~50-100K.
  const auto stats = readPins(
      pins,
      totalPayloadBytes / pins.size() < 10000 ? 25000 : 50000,
      // Max ranges in one preadv call. Longest gap + longest cache entry are
      // under 12 ranges. If a system has a limit of 1K ranges, coalesce limit
      // of 1000 is safe.
      900,
      [&](int32_t index) { return ssdPins[index].run().offset(); },
      [&](const std::vector<CachePin>& /*pins*/,
          int32_t /*begin*/,
          int32_t /*end*/,
          uint64_t offset,
          const std::vector<folly::Range<char*>>& buffers) {
        read(offset, buffers);
      });

  for (auto i = 0; i < ssdPins.size(); ++i) {
    pins[i].checkedEntry()->setSsdFile(this, ssdPins[i].run().offset());
    auto* entry = pins[i].checkedEntry();
    auto ssdRun = ssdPins[i].run();
    maybeVerifyChecksum(*entry, ssdRun);
  }
  return stats;
}

void SsdFile::read(
    uint64_t offset,
    const std::vector<folly::Range<char*>>& buffers) {
  process::TraceContext trace("SsdFile::read");
  readFile_->preadv(offset, buffers);
}

std::optional<std::pair<uint64_t, int32_t>> SsdFile::getSpace(
    const std::vector<CachePin>& pins,
    int32_t begin) {
  int32_t next = begin;
  std::lock_guard<std::shared_mutex> l(mutex_);
  for (;;) {
    if (writableRegions_.empty()) {
      if (!growOrEvictLocked()) {
        return std::nullopt;
      }
    }
    assert(!writableRegions_.empty());
    const auto region = writableRegions_[0];
    const auto offset = regionSizes_[region];
    auto available = kRegionSize - offset;
    int64_t toWrite = 0;
    for (; next < pins.size(); ++next) {
      auto* entry = pins[next].checkedEntry();
      if (entry->size() > available) {
        break;
      }
      available -= entry->size();
      toWrite += entry->size();
    }
    if (toWrite > 0) {
      // At least some pins got space from this region. If the region is full
      // the next call will get space from another region.
      regionSizes_[region] += toWrite;
      return std::make_pair<uint64_t, int32_t>(
          region * kRegionSize + offset, toWrite);
    }

    tracker_.regionFilled(region);
    writableRegions_.erase(writableRegions_.begin());
  }
}

bool SsdFile::growOrEvictLocked() {
  process::TraceContext trace("SsdFile::growOrEvictLocked");
  if (numRegions_ < maxRegions_) {
    const auto newSize = (numRegions_ + 1) * kRegionSize;
    const auto rc = ::ftruncate(fd_, newSize);
    if (rc >= 0) {
      fileSize_ = newSize;
      writableRegions_.push_back(numRegions_);
      regionSizes_[numRegions_] = 0;
      erasedRegionSizes_[numRegions_] = 0;
      ++numRegions_;
      return true;
    }

    ++stats_.growFileErrors;
    VELOX_SSD_CACHE_LOG(ERROR)
        << "Failed to grow cache file " << fileName_ << " to " << newSize;
  }

  const auto candidates =
      tracker_.findEvictionCandidates(3, numRegions_, regionPins_);
  if (candidates.empty()) {
    suspended_ = true;
    return false;
  }

  logEviction(candidates);
  clearRegionEntriesLocked(candidates);
  stats_.regionsEvicted += candidates.size();
  writableRegions_ = std::move(candidates);
  suspended_ = false;
  return true;
}

void SsdFile::clearRegionEntriesLocked(const std::vector<int32_t>& regions) {
  std::unordered_set<int32_t> regionSet{regions.begin(), regions.end()};
  // Remove all 'entries_' where the dependent points one of 'regionIndices'.
  auto it = entries_.begin();
  while (it != entries_.end()) {
    const auto region = regionIndex(it->second.offset());
    if (regionSet.count(region) != 0) {
      it = entries_.erase(it);
    } else {
      ++it;
    }
  }
  for (const auto region : regions) {
    // While the region is being filled, it may get score from hits. When it is
    // full, it will get a score boost to be a little ahead of the best.
    tracker_.regionCleared(region);
    regionSizes_[region] = 0;
    erasedRegionSizes_[region] = 0;
  }
}

void SsdFile::write(std::vector<CachePin>& pins) {
  process::TraceContext trace("SsdFile::write");
  // Sorts the pins by their file/offset. In this way what is adjacent in
  // storage is likely adjacent on SSD.
  std::sort(pins.begin(), pins.end());
  for (const auto& pin : pins) {
    auto* entry = pin.checkedEntry();
    VELOX_CHECK_NULL(entry->ssdFile());
  }

  int32_t storeIndex = 0;
  while (storeIndex < pins.size()) {
    auto space = getSpace(pins, storeIndex);
    if (!space.has_value()) {
      // No space can be reclaimed. The pins are freed when the caller is freed.
      ++stats_.writeSsdDropped;
      return;
    }

    auto [offset, available] = space.value();
    int32_t numWritten = 0;
    int32_t bytes = 0;
    std::vector<iovec> iovecs;
    for (auto i = storeIndex; i < pins.size(); ++i) {
      auto* entry = pins[i].checkedEntry();
      const auto entrySize = entry->size();
      if (bytes + entrySize > available) {
        break;
      }
      addEntryToIovecs(*entry, iovecs);
      bytes += entrySize;
      ++numWritten;
    }
    VELOX_CHECK_GE(fileSize_, offset + bytes);

    const auto rc = folly::pwritev(fd_, iovecs.data(), iovecs.size(), offset);
    if (rc != bytes) {
      VELOX_SSD_CACHE_LOG(ERROR)
          << "Failed to write to SSD, file name: " << fileName_
          << ", fd: " << fd_ << ", size: " << iovecs.size()
          << ", offset: " << offset << ", error code: " << errno
          << ", error string: " << folly::errnoStr(errno);
      ++stats_.writeSsdErrors;
      // If write fails, we return without adding the pins to the cache. The
      // entries are unchanged.
      return;
    }

    {
      std::lock_guard<std::shared_mutex> l(mutex_);
      for (auto i = storeIndex; i < storeIndex + numWritten; ++i) {
        auto* entry = pins[i].checkedEntry();
        VELOX_CHECK_NULL(entry->ssdFile());
        entry->setSsdFile(this, offset);
        const auto size = entry->size();
        FileCacheKey key = {
            entry->key().fileNum, static_cast<uint64_t>(entry->offset())};
        uint32_t checksum = 0;
        if (checksumEnabled_) {
          checksum = checksumEntry(*entry);
        }
        entries_[std::move(key)] = SsdRun(offset, size, checksum);
        if (FLAGS_ssd_verify_write) {
          verifyWrite(*entry, SsdRun(offset, size, checksum));
        }
        offset += size;
        ++stats_.entriesWritten;
        stats_.bytesWritten += size;
        bytesAfterCheckpoint_ += size;
      }
    }
    storeIndex += numWritten;
  }

  if (checkpointEnabled()) {
    checkpoint();
  }
}

namespace {
int32_t indexOfFirstMismatch(char* x, char* y, int n) {
  for (auto i = 0; i < n; ++i) {
    if (x[i] != y[i]) {
      return i;
    }
  }
  return -1;
}
} // namespace

void SsdFile::verifyWrite(AsyncDataCacheEntry& entry, SsdRun ssdRun) {
  process::TraceContext trace("SsdFile::verifyWrite");
  auto testData = std::make_unique<char[]>(entry.size());
  const auto rc = ::pread(fd_, testData.get(), entry.size(), ssdRun.offset());
  VELOX_CHECK_EQ(rc, entry.size());
  if (entry.tinyData() != nullptr) {
    if (::memcmp(testData.get(), entry.tinyData(), entry.size()) != 0) {
      VELOX_FAIL("bad read back");
    }
  } else {
    const auto& data = entry.data();
    int64_t bytesLeft = entry.size();
    int64_t offset = 0;
    for (auto i = 0; i < data.numRuns(); ++i) {
      const auto run = data.runAt(i);
      const auto compareSize = std::min<int64_t>(bytesLeft, run.numBytes());
      const auto badIndex = indexOfFirstMismatch(
          run.data<char>(), testData.get() + offset, compareSize);
      VELOX_CHECK_EQ(badIndex, -1, "Bad read back");
      bytesLeft -= run.numBytes();
      offset += run.numBytes();
      if (bytesLeft <= 0) {
        break;
      };
    }
  }
}

void SsdFile::updateStats(SsdCacheStats& stats) const {
  // Lock only in tsan build. Incrementing the counters has no synchronized
  // semantics.
  std::shared_lock<std::shared_mutex> l(mutex_);
  stats.entriesWritten += stats_.entriesWritten;
  stats.bytesWritten += stats_.bytesWritten;
  stats.checkpointsWritten += stats_.checkpointsWritten;
  stats.entriesRead += stats_.entriesRead;
  stats.bytesRead += stats_.bytesRead;
  stats.checkpointsRead += stats_.checkpointsRead;
  stats.entriesCached += entries_.size();
  stats.regionsCached += numRegions_;
  for (auto i = 0; i < numRegions_; i++) {
    stats.bytesCached += (regionSizes_[i] - erasedRegionSizes_[i]);
  }
  stats.entriesAgedOut += stats_.entriesAgedOut;
  stats.regionsAgedOut += stats_.regionsAgedOut;
  stats.regionsEvicted += stats_.regionsEvicted;
  for (auto pins : regionPins_) {
    stats.numPins += pins;
  }

  stats.openFileErrors += stats_.openFileErrors;
  stats.openCheckpointErrors += stats_.openCheckpointErrors;
  stats.openLogErrors += stats_.openLogErrors;
  stats.deleteCheckpointErrors += stats_.deleteCheckpointErrors;
  stats.growFileErrors += stats_.growFileErrors;
  stats.writeSsdErrors += stats_.writeSsdErrors;
  stats.writeCheckpointErrors += stats_.writeCheckpointErrors;
  stats.readSsdErrors += stats_.readSsdErrors;
  stats.readCheckpointErrors += stats_.readCheckpointErrors;
}

void SsdFile::testingClear() {
  std::lock_guard<std::shared_mutex> l(mutex_);
  entries_.clear();
  std::fill(regionSizes_.begin(), regionSizes_.end(), 0);
  std::fill(erasedRegionSizes_.begin(), erasedRegionSizes_.end(), 0);
  writableRegions_.resize(numRegions_);
  std::iota(writableRegions_.begin(), writableRegions_.end(), 0);
  tracker_.testingClear();
}

void SsdFile::testingDeleteFile() {
  process::TraceContext trace("SsdFile::testingDeleteFile");
  if (fd_) {
    close(fd_);
    fd_ = 0;
  }
  auto rc = unlink(fileName_.c_str());
  if (rc < 0) {
    VELOX_SSD_CACHE_LOG(ERROR)
        << "Error deleting cache file " << fileName_ << " rc: " << rc;
  }
}

bool SsdFile::removeFileEntries(
    const folly::F14FastSet<uint64_t>& filesToRemove,
    folly::F14FastSet<uint64_t>& filesRetained) {
  if (filesToRemove.empty()) {
    VELOX_SSD_CACHE_LOG(INFO)
        << "Removed 0 entry from " << fileName_ << ". And erased 0 region with "
        << kMaxErasedSizePct << "% entries removed.";
    return true;
  }

  std::lock_guard<std::shared_mutex> l(mutex_);

  int64_t entriesAgedOut = 0;
  auto it = entries_.begin();
  while (it != entries_.end()) {
    const FileCacheKey& cacheKey = it->first;
    const SsdRun& ssdRun = it->second;

    if (!cacheKey.fileNum.hasValue()) {
      ++it;
      continue;
    }
    if (filesToRemove.count(cacheKey.fileNum.id()) == 0) {
      ++it;
      continue;
    }

    auto region = regionIndex(ssdRun.offset());
    if (regionPins_[region] > 0) {
      filesRetained.insert(cacheKey.fileNum.id());
      ++it;
      continue;
    }

    ++entriesAgedOut;
    erasedRegionSizes_[region] += ssdRun.size();

    it = entries_.erase(it);
  }

  std::vector<int32_t> toFree;
  toFree.reserve(numRegions_);
  for (auto region = 0; region < numRegions_; ++region) {
    if (regionPins_[region] == 0 &&
        erasedRegionSizes_[region] >
            regionSizes_[region] * kMaxErasedSizePct / 100) {
      toFree.push_back(region);
    }
  }
  if (toFree.size() > 0) {
    VELOX_CHECK(!suspended_);
    logEviction(toFree);
    clearRegionEntriesLocked(toFree);
    writableRegions_.reserve(
        std::min<size_t>(writableRegions_.size() + toFree.size(), numRegions_));
    folly::F14FastSet<uint64_t> existingWritableRegions(
        writableRegions_.begin(), writableRegions_.end());
    for (int32_t region : toFree) {
      if (existingWritableRegions.count(region) == 0) {
        writableRegions_.push_back(region);
      }
      VELOX_CHECK_EQ(regionSizes_[region], 0);
      VELOX_CHECK_EQ(erasedRegionSizes_[region], 0);
    }
  }

  stats_.entriesAgedOut += entriesAgedOut;
  stats_.regionsAgedOut += toFree.size();
  stats_.regionsEvicted += toFree.size();
  VELOX_SSD_CACHE_LOG(INFO)
      << "Removed " << entriesAgedOut << " entries from " << fileName_
      << ". And erased " << toFree.size() << " regions with "
      << kMaxErasedSizePct << "% entries removed.";

  return true;
}

void SsdFile::logEviction(const std::vector<int32_t>& regions) {
  if (checkpointEnabled()) {
    const int32_t rc = ::write(
        evictLogFd_, regions.data(), regions.size() * sizeof(regions[0]));
    if (rc != regions.size() * sizeof(regions[0])) {
      checkpointError(rc, "Failed to log eviction");
    }
  }
}

void SsdFile::deleteCheckpoint(bool keepLog) {
  if (checkpointDeleted_) {
    return;
  }
  if (evictLogFd_ >= 0) {
    if (keepLog) {
      ::lseek(evictLogFd_, 0, SEEK_SET);
      ::ftruncate(evictLogFd_, 0);
      ::fsync(evictLogFd_);
    } else {
      ::close(evictLogFd_);
      evictLogFd_ = -1;
    }
  }

  checkpointDeleted_ = true;
  const auto logPath = getEvictLogFilePath();
  int32_t logRc = 0;
  if (!keepLog) {
    logRc = ::unlink(logPath.c_str());
  }
  const auto checkpointPath = getCheckpointFilePath();
  const auto checkpointRc = ::unlink(checkpointPath.c_str());
  if ((logRc != 0) || (checkpointRc != 0)) {
    ++stats_.deleteCheckpointErrors;
    VELOX_SSD_CACHE_LOG(ERROR)
        << "Error in deleting log and checkpoint. log: " << logRc
        << " checkpoint: " << checkpointRc;
  }
}

void SsdFile::checkpointError(int32_t rc, const std::string& error) {
  VELOX_SSD_CACHE_LOG(ERROR)
      << error << " with rc=" << rc
      << ", deleting checkpoint and continuing with checkpointing off";
  deleteCheckpoint();
  checkpointIntervalBytes_ = 0;
}

namespace {
template <typename T>
inline char* asChar(T ptr) {
  return reinterpret_cast<char*>(ptr);
}

template <typename T>
inline const char* asChar(const T* ptr) {
  return reinterpret_cast<const char*>(ptr);
}
} // namespace

void SsdFile::checkpoint(bool force) {
  process::TraceContext trace("SsdFile::checkpoint");
  std::lock_guard<std::shared_mutex> l(mutex_);
  if (!needCheckpoint(force)) {
    return;
  }

  VELOX_SSD_CACHE_LOG(INFO)
      << "Checkpointing shard " << shardId_ << ", force: " << force
      << " bytesAfterCheckpoint: " << succinctBytes(bytesAfterCheckpoint_)
      << " checkpointIntervalBytes: "
      << succinctBytes(checkpointIntervalBytes_);

  checkpointDeleted_ = false;
  bytesAfterCheckpoint_ = 0;
  try {
    const auto checkRc = [&](int32_t rc, const std::string& errMsg) {
      if (rc < 0) {
        VELOX_FAIL("{} with rc {} :{}", errMsg, rc, folly::errnoStr(errno));
      }
      return rc;
    };

    // We schedule the potentially long fsync of the cache file on another
    // thread of the cache write executor, if available. If there is none, we do
    // the sync on this thread at the end.
    auto fileSync = std::make_shared<AsyncSource<int>>(
        [fd = fd_]() { return std::make_unique<int>(::fsync(fd)); });
    if (executor_ != nullptr) {
      executor_->add([fileSync]() { fileSync->prepare(); });
    }

    std::ofstream state;
    const auto checkpointPath = getCheckpointFilePath();
    try {
      state.exceptions(std::ofstream::failbit);
      state.open(checkpointPath, std::ios_base::out | std::ios_base::trunc);
      // The checkpoint state file contains:
      // int32_t The 4 bytes of checkpoint version,
      // int32_t maxRegions,
      // int32_t numRegions,
      // regionScores from the 'tracker_',
      // {fileId, fileName} pairs,
      // kMapMarker,
      // {fileId, offset, SSdRun} triples,
      // kEndMarker.
      state.write(checkpointVersion().data(), sizeof(int32_t));
      state.write(asChar(&maxRegions_), sizeof(maxRegions_));
      state.write(asChar(&numRegions_), sizeof(numRegions_));

      // Copy the region scores before writing out for tsan.
      const auto scoresCopy = tracker_.copyScores();
      state.write(asChar(scoresCopy.data()), maxRegions_ * sizeof(uint64_t));
      std::unordered_set<uint64_t> fileNums;
      for (const auto& entry : entries_) {
        const auto fileNum = entry.first.fileNum.id();
        if (fileNums.insert(fileNum).second) {
          state.write(asChar(&fileNum), sizeof(fileNum));
          const auto name = fileIds().string(fileNum);
          const int32_t length = name.size();
          state.write(asChar(&length), sizeof(length));
          state.write(name.data(), length);
        }
      }

      const auto mapMarker = kCheckpointMapMarker;
      state.write(asChar(&mapMarker), sizeof(mapMarker));
      for (auto& pair : entries_) {
        const auto id = pair.first.fileNum.id();
        state.write(asChar(&id), sizeof(id));
        state.write(asChar(&pair.first.offset), sizeof(pair.first.offset));
        const auto offsetAndSize = pair.second.fileBits();
        state.write(asChar(&offsetAndSize), sizeof(offsetAndSize));
        if (checksumEnabled_) {
          const auto checksum = pair.second.checksum();
          state.write(asChar(&checksum), sizeof(checksum));
        }
      }
    } catch (const std::exception& e) {
      fileSync->close();
      std::rethrow_exception(std::current_exception());
    }

    // NOTE: we need to ensure cache file data sync update completes before
    // updating checkpoint file.
    const auto fileSyncRc = fileSync->move();
    checkRc(*fileSyncRc, "Sync of cache data file");

    const auto endMarker = kCheckpointEndMarker;
    state.write(asChar(&endMarker), sizeof(endMarker));

    if (state.bad()) {
      ++stats_.writeCheckpointErrors;
      checkRc(-1, "Write of checkpoint file");
    } else {
      ++stats_.checkpointsWritten;
    }
    state.close();

    // Sync checkpoint data file. ofstream does not have a sync method, so open
    // as fd and sync that.
    const auto checkpointFd = checkRc(
        ::open(checkpointPath.c_str(), O_WRONLY),
        "Open of checkpoint file for sync");
    // TODO: add this as file open option after we migrate to use velox
    // filesystem for ssd file access.
    if (disableFileCow_) {
      disableCow(checkpointFd);
    }
    VELOX_CHECK_GE(checkpointFd, 0);
    checkRc(::fsync(checkpointFd), "Sync of checkpoint file");
    ::close(checkpointFd);

    // NOTE: we shall truncate eviction log after checkpoint file sync
    // completes so that we never recover from an old checkpoint file without
    // log evictions. The latter might lead to data consistent issue.
    checkRc(::ftruncate(evictLogFd_, 0), "Truncate of event log");
    checkRc(::fsync(evictLogFd_), "Sync of evict log");
  } catch (const std::exception& e) {
    try {
      checkpointError(-1, e.what());
    } catch (const std::exception&) {
    }
    // Ignore nested exception.
  }
}

void SsdFile::initializeCheckpoint() {
  if (!checkpointEnabled()) {
    return;
  }

  bool hasCheckpoint = true;
  std::ifstream state(getCheckpointFilePath());
  if (!state.is_open()) {
    hasCheckpoint = false;
    ++stats_.openCheckpointErrors;
    VELOX_SSD_CACHE_LOG(WARNING) << fmt::format(
        "Starting shard {} without checkpoint, with checksum write {}, read verification {}.",
        shardId_,
        checksumEnabled_ ? "enabled" : "disabled",
        checksumReadVerificationEnabled_ ? "enabled" : "disabled");
  }
  const auto logPath = getEvictLogFilePath();
  evictLogFd_ = ::open(logPath.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
  if (disableFileCow_) {
    disableCow(evictLogFd_);
  }
  if (evictLogFd_ < 0) {
    ++stats_.openLogErrors;
    // Failure to open the log at startup is a process terminating error.
    VELOX_FAIL(
        "Could not open evict log {}, rc {}: {}",
        logPath,
        evictLogFd_,
        folly::errnoStr(errno));
  }

  try {
    if (hasCheckpoint) {
      state.exceptions(std::ifstream::failbit);
      readCheckpoint(state);
    }
  } catch (const std::exception& e) {
    ++stats_.readCheckpointErrors;
    try {
      VELOX_SSD_CACHE_LOG(ERROR) << "Error recovering from checkpoint "
                                 << e.what() << ": Starting without checkpoint";
      entries_.clear();
      deleteCheckpoint(true);
    } catch (const std::exception&) {
    }
  }
}

uint32_t SsdFile::checksumEntry(const AsyncDataCacheEntry& entry) const {
  bits::Crc32 crc;
  if (entry.tinyData()) {
    crc.process_bytes(entry.tinyData(), entry.size());
  } else {
    int64_t bytesLeft = entry.size();
    const auto& data = entry.data();
    for (auto i = 0; i < data.numRuns() && bytesLeft > 0; ++i) {
      const auto run = data.runAt(i);
      const auto bytesToProcess = std::min<size_t>(bytesLeft, run.numBytes());
      crc.process_bytes(run.data<char>(), bytesToProcess);
      bytesLeft -= bytesToProcess;
    }
    VELOX_CHECK_EQ(bytesLeft, 0);
  }
  return crc.checksum();
}

void SsdFile::maybeVerifyChecksum(
    const AsyncDataCacheEntry& entry,
    const SsdRun& ssdRun) {
  if (!checksumReadVerificationEnabled_) {
    return;
  }

  // Verifies that the checksum matches after we read from SSD.
  const auto checksum = checksumEntry(entry);
  if (checksum != ssdRun.checksum()) {
    ++stats_.readSsdCorruptions;
    VELOX_FAIL(
        "IOERR: Corrupt SSD cache entry - File: {}, Offset: {}, Size: {}",
        fileName_,
        ssdRun.offset(),
        ssdRun.size())
  }
}

bool SsdFile::testingIsCowDisabled() const {
#ifdef linux
  int attr{0};
  const auto res = ioctl(fd_, FS_IOC_GETFLAGS, &attr);
  VELOX_CHECK_EQ(
      0,
      res,
      "ioctl(FS_IOC_GETFLAGS) failed: {}, {}",
      res,
      folly::errnoStr(errno));

  return (attr & FS_NOCOW_FL) == FS_NOCOW_FL;
#else
  return false;
#endif // linux
}

namespace {
template <typename T>
T readNumber(std::ifstream& stream) {
  T data;
  stream.read(asChar(&data), sizeof(T));
  return data;
}
} // namespace

void SsdFile::readCheckpoint(std::ifstream& state) {
  char versionMagic[4];
  state.read(versionMagic, sizeof(versionMagic));
  const auto checkpoinHasChecksum =
      isChecksumEnabledOnCheckpointVersion(std::string(versionMagic, 4));
  if (checksumEnabled_ && !checkpoinHasChecksum) {
    VELOX_SSD_CACHE_LOG(WARNING) << fmt::format(
        "Starting shard {} without checkpoint: checksum is enabled but the checkpoint was made without checksum, so skip the checkpoint recovery.",
        shardId_);
    return;
  }

  const auto maxRegions = readNumber<int32_t>(state);
  VELOX_CHECK_EQ(
      maxRegions,
      maxRegions_,
      "Trying to start from checkpoint with a different capacity");
  numRegions_ = readNumber<int32_t>(state);
  std::vector<double> scores(maxRegions);
  state.read(asChar(scores.data()), maxRegions_ * sizeof(double));
  std::unordered_map<uint64_t, StringIdLease> idMap;
  for (;;) {
    const auto id = readNumber<uint64_t>(state);
    if (id == kCheckpointMapMarker) {
      break;
    }
    std::string name;
    name.resize(readNumber<int32_t>(state));
    state.read(name.data(), name.size());
    auto lease = StringIdLease(fileIds(), name);
    idMap[id] = std::move(lease);
  }

  const auto logSize = ::lseek(evictLogFd_, 0, SEEK_END);
  std::vector<uint32_t> evicted(logSize / sizeof(uint32_t));
  const auto rc = ::pread(evictLogFd_, evicted.data(), logSize, 0);
  VELOX_CHECK_EQ(logSize, rc, "Failed to read eviction log");
  std::unordered_set<uint32_t> evictedMap;
  for (auto region : evicted) {
    evictedMap.insert(region);
  }
  for (;;) {
    const auto fileNum = readNumber<uint64_t>(state);
    if (fileNum == kCheckpointEndMarker) {
      break;
    }
    const auto offset = readNumber<uint64_t>(state);
    const auto fileBits = readNumber<uint64_t>(state);
    uint32_t checksum = 0;
    if (checkpoinHasChecksum) {
      checksum = readNumber<uint32_t>(state);
    }
    const auto run = SsdRun(fileBits, checksum);
    // Check that the recovered entry does not fall in an evicted region.
    if (evictedMap.find(regionIndex(run.offset())) == evictedMap.end()) {
      // The file may have a different id on restore.
      auto it = idMap.find(fileNum);
      VELOX_CHECK(it != idMap.end());
      FileCacheKey key{it->second, offset};
      entries_[std::move(key)] = run;
    }
  }
  ++stats_.checkpointsRead;
  // The state is successfully read. Install the access frequency scores and
  // evicted regions.
  VELOX_CHECK_EQ(scores.size(), tracker_.regionScores().size());
  // Set the writable regions by deduplicated evicted regions.
  writableRegions_.clear();
  for (auto region : evictedMap) {
    writableRegions_.push_back(region);
  }
  tracker_.setRegionScores(scores);
  VELOX_SSD_CACHE_LOG(INFO) << fmt::format(
      "Starting shard {} from checkpoint with {} entries, {} regions with {} free, with checksum write {}, read verification {}.",
      shardId_,
      entries_.size(),
      numRegions_,
      writableRegions_.size(),
      checksumEnabled_ ? "enabled" : "disabled",
      checksumReadVerificationEnabled_ ? "enabled" : "disabled");
}

} // namespace facebook::velox::cache
