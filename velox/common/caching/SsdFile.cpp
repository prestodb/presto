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
#include "velox/common/caching/FileIds.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <numeric>

#include <fstream>

DEFINE_bool(ssd_odirect, true, "Use O_DIRECT for SSD cache IO");
DEFINE_bool(ssd_verify_write, false, "Read back data after writing to SSD");

namespace facebook::velox::cache {

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
  if (file_) {
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

SsdFile::SsdFile(
    const std::string& filename,
    int32_t shardId,
    int32_t maxRegions,
    int64_t checkpointIntervalBytes,
    folly::Executor* FOLLY_NULLABLE executor)
    : fileName_(filename),
      shardId_(shardId),
      maxRegions_(maxRegions),
      filename_(filename),
      checkpointIntervalBytes_(checkpointIntervalBytes),
      executor_(executor) {
  int32_t oDirect = 0;
#ifdef linux
  oDirect = FLAGS_ssd_odirect ? O_DIRECT : 0;
#endif
  fd_ = open(filename_.c_str(), O_CREAT | O_RDWR | oDirect, S_IRUSR | S_IWUSR);
  if (fd_ < 0) {
    LOG(ERROR) << "Cannot open or create " << filename << " error " << errno;
    exit(1);
  }
  readFile_ = std::make_unique<LocalReadFile>(fd_);
  uint64_t size = lseek(fd_, 0, SEEK_END);
  numRegions_ = size / kRegionSize;
  if (numRegions_ > maxRegions_) {
    numRegions_ = maxRegions_;
  }
  fileSize_ = numRegions_ * kRegionSize;
  if (size % kRegionSize > 0 || size > numRegions_ * kRegionSize) {
    ftruncate(fd_, fileSize_);
  }
  // The existing regions in the file are writable.
  writableRegions_.resize(numRegions_);
  std::iota(writableRegions_.begin(), writableRegions_.end(), 0);
  tracker_.resize(maxRegions_);
  regionSize_.resize(maxRegions_);
  regionPins_.resize(maxRegions_);
  if (checkpointIntervalBytes_) {
    initializeCheckpoint();
  }
}

void SsdFile::pinRegion(uint64_t offset) {
  std::lock_guard<std::mutex> l(mutex_);
  pinRegionLocked(offset);
}

void SsdFile::unpinRegion(uint64_t offset) {
  std::lock_guard<std::mutex> l(mutex_);
  auto count = --regionPins_[regionIndex(offset)];
  VELOX_CHECK_LE(0, count);
  if (suspended_ && count == 0) {
    growOrEvictLocked();
  }
}

namespace {
void addEntryToIovecs(AsyncDataCacheEntry& entry, std::vector<iovec>& iovecs) {
  if (entry.tinyData()) {
    iovecs.push_back({entry.tinyData(), static_cast<size_t>(entry.size())});
    return;
  }
  auto& data = entry.data();
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

SsdPin SsdFile::find(RawFileCacheKey key) {
  FileCacheKey ssdKey{StringIdLease(fileIds(), key.fileNum), key.offset};
  SsdRun run;
  {
    std::lock_guard<std::mutex> l(mutex_);
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
  std::lock_guard<std::mutex> l(mutex_);
  auto it = entries_.find(ssdKey);
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
  int payloadTotal = 0;
  for (auto i = 0; i < pins.size(); ++i) {
    auto runSize = ssdPins[i].run().size();
    auto entry = pins[i].checkedEntry();
    if (runSize > entry->size()) {
      LOG(INFO) << "IOERR: Requested prefix of SSD cache entry: " << runSize
                << " entry: " << entry->size();
    }
    VELOX_CHECK_GE(
        runSize,
        entry->size(),
        "IOERR SSd cache entry shorter than requested range");
    payloadTotal += entry->size();
    regionRead(regionIndex(ssdPins[i].run().offset()), runSize);
    ++stats_.entriesRead;
    stats_.bytesRead += entry->size();
  }
  // Do coalesced IO for the pins. For short payloads, the break-even
  // between discrete pread calls and a single preadv that discards
  // gaps is ~25K per gap. For longer payloads this is ~50-100K.
  auto stats = readPins(
      pins,
      payloadTotal / pins.size() < 10000 ? 25000 : 50000,
      // Max ranges in one preadv call. Longest gap + longest cache
      // entry are under 12 ranges. If a system has a limit of 1K
      // ranges, coalesce limit of 1000 is safe.
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
  }
  return stats;
}

void SsdFile::read(
    uint64_t offset,
    const std::vector<folly::Range<char*>>& buffers) {
  readFile_->preadv(offset, buffers);
}

std::optional<std::pair<uint64_t, int32_t>> SsdFile::getSpace(
    const std::vector<CachePin>& pins,
    int32_t begin) {
  std::lock_guard<std::mutex> l(mutex_);
  for (;;) {
    if (writableRegions_.empty()) {
      if (!growOrEvictLocked()) {
        return std::nullopt;
        ;
      }
    }
    assert(!writableRegions_.empty());
    auto region = writableRegions_[0];
    auto offset = regionSize_[region];
    auto available = kRegionSize - offset;
    int64_t toWrite = 0;
    for (; begin < pins.size(); ++begin) {
      auto entry = pins[begin].checkedEntry();
      if (entry->size() > available) {
        break;
      }
      available -= entry->size();
      toWrite += entry->size();
    }
    if (toWrite) {
      // At least some pins got space from this region. If the region is full
      // the next call will get space from another region.
      regionSize_[region] += toWrite;
      return std::make_pair<uint64_t, int32_t>(
          region * kRegionSize + offset, toWrite);
    }

    tracker_.regionFilled(region);
    writableRegions_.erase(writableRegions_.begin());
  }
}

bool SsdFile::growOrEvictLocked() {
  if (numRegions_ < maxRegions_) {
    auto newSize = (numRegions_ + 1) * kRegionSize;
    auto rc = ftruncate(fd_, newSize);
    if (rc >= 0) {
      fileSize_ = newSize;
      writableRegions_.push_back(numRegions_);
      regionSize_[numRegions_] = 0;
      ++numRegions_;
      return true;
    } else {
      LOG(ERROR) << "Failed to grow cache file " << filename_ << " to "
                 << newSize;
    }
  }
  auto candidates =
      tracker_.findEvictionCandidates(3, numRegions_, regionPins_);
  if (candidates.empty()) {
    suspended_ = true;
    return false;
  }
  logEviction(candidates);
  clearRegionEntriesLocked(candidates);
  writableRegions_ = std::move(candidates);
  suspended_ = false;
  return true;
}

void SsdFile::clearRegionEntriesLocked(
    const std::vector<int32_t>& regionIndices) {
  // Remove all 'entries_' where the dependent points one of 'regionIndices'.
  auto it = entries_.begin();
  while (it != entries_.end()) {
    auto region = regionIndex(it->second.offset());
    if (std::find(regionIndices.begin(), regionIndices.end(), region) !=
        regionIndices.end()) {
      it = entries_.erase(it);
    } else {
      ++it;
    }
  }
  for (auto region : regionIndices) {
    // While the region is being filled it may get score from
    // hits. When it is full, it will get a score boost to be a little
    // ahead of the best.
    tracker_.regionCleared(region);
    regionSize_[region] = 0;
  }
}

void SsdFile::write(std::vector<CachePin>& pins) {
  // Sorts the pins by their file/offset. In this way what is ajacent
  // in storage is likely adjacent on SSD.
  std::sort(pins.begin(), pins.end());
  uint64_t total = 0;
  for (auto& pin : pins) {
    auto entry = pin.checkedEntry();
    VELOX_CHECK_NULL(entry->ssdFile());
    total += entry->size();
  }
  int32_t storeIndex = 0;
  while (storeIndex < pins.size()) {
    auto space = getSpace(pins, storeIndex);

    if (!space.has_value()) {
      // No space can be reclaimed. The pins are freed when the caller is freed.
      return;
    }
    auto [offset, available] = space.value();
    int32_t numWritten = 0;
    int32_t bytes = 0;
    std::vector<iovec> iovecs;
    for (auto i = storeIndex; i < pins.size(); ++i) {
      auto entry = pins[i].checkedEntry();
      auto entrySize = entry->size();
      if (bytes + entrySize > available) {
        break;
      }
      addEntryToIovecs(*entry, iovecs);
      bytes += entrySize;
      ++numWritten;
    }
    VELOX_CHECK_GE(fileSize_, offset + bytes);
    auto rc = folly::pwritev(fd_, iovecs.data(), iovecs.size(), offset);
    if (rc != bytes) {
      LOG(ERROR) << "Failed to write to SSD " << errno;
      // If the write fails we return without adding the pins to the cache. The
      // entries are unchanged.
      return;
    }
    {
      std::lock_guard<std::mutex> l(mutex_);
      for (auto i = storeIndex; i < storeIndex + numWritten; ++i) {
        auto entry = pins[i].checkedEntry();
        entry->setSsdFile(this, offset);
        auto size = entry->size();
        FileCacheKey key = {
            entry->key().fileNum, static_cast<uint64_t>(entry->offset())};
        entries_[std::move(key)] = SsdRun(offset, size);
        if (FLAGS_ssd_verify_write) {
          verifyWrite(*entry, SsdRun(offset, size));
        }
        offset += size;
        ++stats_.entriesWritten;
        stats_.bytesWritten += size;
        bytesAfterCheckpoint_ += size;
      }
    }
    storeIndex += numWritten;
  }

  if (checkpointIntervalBytes_ &&
      bytesAfterCheckpoint_ > checkpointIntervalBytes_) {
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
  auto testData = std::make_unique<char[]>(entry.size());
  auto rc = pread(fd_, testData.get(), entry.size(), ssdRun.offset());
  VELOX_CHECK_EQ(rc, entry.size());
  if (entry.tinyData()) {
    if (0 != memcmp(testData.get(), entry.tinyData(), entry.size())) {
      VELOX_FAIL("bad read back");
    }
  } else {
    auto& data = entry.data();
    int64_t bytesLeft = entry.size();
    int64_t offset = 0;
    for (auto i = 0; i < data.numRuns(); ++i) {
      auto run = data.runAt(i);
      auto compareSize = std::min<int64_t>(bytesLeft, run.numBytes());
      auto badIndex = indexOfFirstMismatch(
          run.data<char>(), testData.get() + offset, compareSize);
      if (badIndex != -1) {
        VELOX_FAIL("Bad read back");
      }
      bytesLeft -= run.numBytes();
      offset += run.numBytes();
      if (bytesLeft <= 0) {
        break;
      };
    }
  }
}

void SsdFile::updateStats(SsdCacheStats& stats) const {
  stats.entriesWritten += stats_.entriesWritten;
  stats.bytesWritten += stats_.bytesWritten;
  stats.entriesRead += stats_.entriesRead;
  stats.bytesRead += stats_.bytesRead;
  stats.entriesCached += entries_.size();
  for (auto& regionSize : regionSize_) {
    stats.bytesCached += regionSize;
  }
  for (auto pins : regionPins_) {
    stats.numPins += pins;
  }
}

void SsdFile::clear() {
  std::lock_guard<std::mutex> l(mutex_);
  entries_.clear();
  std::fill(regionSize_.begin(), regionSize_.end(), 0);
  writableRegions_.resize(numRegions_);
  std::iota(writableRegions_.begin(), writableRegions_.end(), 0);
}

void SsdFile::deleteFile() {
  if (fd_) {
    close(fd_);
    fd_ = 0;
  }
  auto rc = unlink(filename_.c_str());
  if (rc < 0) {
    LOG(ERROR) << "Error deleting cache file " << filename_ << " rc: " << rc;
  }
}

void SsdFile::logEviction(const std::vector<int32_t>& regions) {
  if (checkpointIntervalBytes_) {
    int32_t rc = ::write(
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
  if (evictLogFd_) {
    if (keepLog) {
      lseek(evictLogFd_, 0, SEEK_SET);
      ftruncate(evictLogFd_, 0);
      fsync(evictLogFd_);
    } else {
      close(evictLogFd_);
      evictLogFd_ = 0;
    }
  }
  checkpointDeleted_ = true;
  auto logPath = fileName_ + kLogExtension;
  int32_t logRc = 0;
  if (!keepLog) {
    logRc = unlink(logPath.c_str());
  }
  auto checkpointPath = fileName_ + kCheckpointExtension;
  auto checkpointRc = unlink(checkpointPath.c_str());
  if (logRc || checkpointRc) {
    LOG(ERROR) << "Error in deleting log and checkpoint. log:  " << logRc
               << " checkpoint: " << checkpointRc;
  }
}

void SsdFile::checkpointError(int32_t rc, const std::string& error) {
  LOG(ERROR) << error << " with rc=" << rc
             << " Deleting checkpoint and continuing with checkpointing off";
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
  std::lock_guard<std::mutex> l(mutex_);
  if (!force && bytesAfterCheckpoint_ < checkpointIntervalBytes_) {
    return;
  }
  checkpointDeleted_ = false;
  bytesAfterCheckpoint_ = 0;
  try {
    // We schedule the potentially ;long fsync of the cache file on
    // another thread of the cache write executor, if available. If
    // there is none, we do the sync on this thread at the end.
    auto sync = std::make_shared<AsyncSource<int>>(
        [fd = fd_]() { return std::make_unique<int>(fsync(fd)); });
    if (executor_) {
      executor_->add([sync]() { sync->prepare(); });
    }
    std::ofstream state;
    auto checkpointPath = fileName_ + kCheckpointExtension;
    state.exceptions(std::ofstream::failbit);
    state.open(checkpointPath, std::ios_base::out | std::ios_base::trunc);
    // The checkpoint state file contains:
    // int32_t The 4 bytes of kCheckpointMagic,
    // int32_t maxRegions,
    // int32_t numRegions,
    // regionScores from the 'tracker_',
    // {fileId, fileName} pairs,
    // kMapMarker,
    // {fileId, offset, SSdRun} triples,
    // kEndMarker.
    state.write(kCheckpointMagic, sizeof(int32_t));
    state.write(asChar(&maxRegions_), sizeof(maxRegions_));
    state.write(asChar(&numRegions_), sizeof(numRegions_));
    state.write(
        asChar(tracker_.regionScores().data()), maxRegions_ * sizeof(uint64_t));
    std::unordered_set<uint64_t> fileNums;
    for (auto pair : entries_) {
      auto fileNum = pair.first.fileNum.id();
      if (fileNums.insert(fileNum).second) {
        state.write(asChar(&fileNum), sizeof(fileNum));
        auto name = fileIds().string(fileNum);
        int32_t length = name.size();
        state.write(asChar(&length), sizeof(length));
        state.write(name.data(), length);
      }
    }
    const auto mapMarker = kCheckpointMapMarker;
    state.write(asChar(&mapMarker), sizeof(mapMarker));
    for (auto& pair : entries_) {
      auto id = pair.first.fileNum.id();
      state.write(asChar(&id), sizeof(id));
      state.write(asChar(&pair.first.offset), sizeof(pair.first.offset));
      auto offsetAndSize = pair.second.bits();
      state.write(asChar(&offsetAndSize), sizeof(offsetAndSize));
    }
    const auto endMarker = kCheckpointEndMarker;
    state.write(asChar(&endMarker), sizeof(endMarker));
    auto checkRc = [&](int32_t rc, const std::string& message) {
      if (rc < 0) {
        throw std::runtime_error(fmt::format("{} with rc {}", message, rc));
      }
      return rc;
    };
    if (state.bad()) {
      checkRc(-1, "Writing checkpoint file");
    }
    state.close();
    ftruncate(evictLogFd_, 0);
    checkRc(fsync(evictLogFd_), "Sync of evict log");
    auto syncRc = sync->move();
    checkRc(*syncRc, fmt::format("Error in cache file fsync {}", *syncRc));

    // Sync checkpoint data file. ofstream does not have a sync method, so open
    // as fd and sync that.
    auto fd = checkRc(
        open(checkpointPath.c_str(), O_WRONLY),
        "Open of checkpoint file for sync");
    if (fd > 0) {
      checkRc(fsync(fd), "Sync checkpoint file");
      close(fd);
    }

  } catch (const std::exception& e) {
    try {
      checkpointError(-1, e.what());
    } catch (const std::exception& inner) {
    }
    // Ignore nested exception.
  }
}

void SsdFile::initializeCheckpoint() {
  if (!checkpointIntervalBytes_) {
    return;
  }
  bool hasCheckpoint = true;
  std::ifstream state(fileName_ + kCheckpointExtension);
  if (!state.is_open()) {
    hasCheckpoint = false;
    LOG(INFO) << "Starting shard " << shardId_ << " without checkpoint";
  }
  auto logPath = fileName_ + kLogExtension;
  evictLogFd_ = open(logPath.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
  if (evictLogFd_ < 0) {
    // Failure to open the log at startup is a process terminating error.
    LOG(ERROR) << "Could not open evict log " << logPath << " rc "
               << evictLogFd_;
    exit(1);
  }

  try {
    if (hasCheckpoint) {
      state.exceptions(std::ifstream::failbit);
      readCheckpoint(state);
    }
  } catch (const std::exception& e) {
    try {
      LOG(ERROR) << "Error recovering from checkpoint " << e.what()
                 << ": Starting without checkpoint";
      entries_.clear();
      deleteCheckpoint(true);
    } catch (const std::exception& e) {
    }
  }
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
  char magic[4];
  state.read(magic, sizeof(magic));
  VELOX_CHECK(strncmp(magic, kCheckpointMagic, 4) == 0);
  auto maxRegions = readNumber<int32_t>(state);
  VELOX_CHECK_EQ(
      maxRegions,
      maxRegions_,
      "Trying to start from checkpoint with a different capacity");
  numRegions_ = readNumber<int32_t>(state);
  std::vector<int64_t> scores(maxRegions);
  state.read(asChar(scores.data()), maxRegions_ * sizeof(uint64_t));
  std::unordered_map<uint64_t, StringIdLease> idMap;
  for (;;) {
    auto id = readNumber<uint64_t>(state);
    if (id == kCheckpointMapMarker) {
      break;
    }
    std::string name;
    name.resize(readNumber<int32_t>(state));
    state.read(name.data(), name.size());
    auto lease = StringIdLease(fileIds(), name);
    idMap[id] = std::move(lease);
  }
  auto logSize = lseek(evictLogFd_, 0, SEEK_END);
  std::vector<uint32_t> evicted(logSize / sizeof(uint32_t));
  auto rc = ::pread(evictLogFd_, evicted.data(), logSize, 0);
  VELOX_CHECK_EQ(logSize, rc, "Failed to read eviction log");
  std::unordered_set<uint32_t> evictedMap;
  for (auto region : evicted) {
    evictedMap.insert(region);
  }
  for (;;) {
    uint64_t fileNum = readNumber<uint64_t>(state);
    if (fileNum == kCheckpointEndMarker) {
      break;
    }
    uint64_t offset = readNumber<uint64_t>(state);
    auto run = SsdRun(readNumber<uint64_t>(state));
    // Check that the recovered entry does not fall in an evicted region.
    if (evictedMap.find(regionIndex(run.offset())) == evictedMap.end()) {
      // The file may have a different id on restore.
      auto it = idMap.find(fileNum);
      VELOX_CHECK(it != idMap.end());
      FileCacheKey key{it->second, offset};
      entries_[std::move(key)] = run;
    }
  }
  // The state is successfully read. Install the access frequency scores and
  // evicted regions.
  VELOX_CHECK_EQ(scores.size(), tracker_.regionScores().size());
  // Set the writable regions by deduplicated evicted regions.
  writableRegions_.clear();
  for (auto region : evictedMap) {
    writableRegions_.push_back(region);
  }
  tracker_.regionScores() = scores;
  LOG(INFO) << fmt::format(
      "Starting shard {} from checkpoint with {} entries, {} regions with {} free.",
      shardId_,
      entries_.size(),
      numRegions_,
      writableRegions_.size());
}

} // namespace facebook::velox::cache
