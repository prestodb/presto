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

#include <folly/portability/SysUio.h>
#include "velox/common/base/AsyncSource.h"
#include "velox/common/base/Crc.h"
#include "velox/common/base/SuccinctPrinter.h"
#include "velox/common/caching/FileIds.h"
#include "velox/common/caching/SsdCache.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/process/TraceContext.h"

#include <fcntl.h>
#ifdef linux
#include <linux/fs.h>
#endif // linux
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <numeric>

DECLARE_bool(velox_ssd_odirect);
DECLARE_bool(velox_ssd_verify_write);

namespace facebook::velox::cache {

namespace {

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

// Returns the number of entries in a cache 'entry'.
uint32_t numIoVectorsFromEntry(AsyncDataCacheEntry& entry) {
  if (entry.tinyData() != nullptr) {
    return 1;
  }
  return entry.data().numRuns();
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
      fs_(filesystems::getFileSystem(fileName_, nullptr)),
      checkpointIntervalBytes_(config.checkpointIntervalBytes),
      executor_(config.executor) {
  process::TraceContext trace("SsdFile::SsdFile");
  filesystems::FileOptions fileOptions;
  fileOptions.shouldThrowOnFileAlreadyExists = false;
  fileOptions.bufferIo = !FLAGS_velox_ssd_odirect;
  writeFile_ = fs_->openFileForWrite(fileName_, fileOptions);
  readFile_ = fs_->openFileForRead(fileName_, fileOptions);

  const uint64_t size = writeFile_->size();
  numRegions_ = std::min<int32_t>(size / kRegionSize, maxRegions_);
  fileSize_ = numRegions_ * kRegionSize;
  if ((size % kRegionSize > 0) || (size > numRegions_ * kRegionSize)) {
    writeFile_->truncate(fileSize_);
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

  if (disableFileCow_) {
    disableFileCow();
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
    VELOX_CHECK(!writableRegions_.empty());
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
    try {
      writeFile_->truncate(newSize);
      fileSize_ = newSize;
      writableRegions_.push_back(numRegions_);
      regionSizes_[numRegions_] = 0;
      erasedRegionSizes_[numRegions_] = 0;
      ++numRegions_;
      VELOX_SSD_CACHE_LOG(INFO)
          << "Grow cache file " << fileName_ << " to " << numRegions_
          << " regions (max: " << maxRegions_ << ")";
      return true;
    } catch (const std::exception& e) {
      ++stats_.growFileErrors;
      VELOX_SSD_CACHE_LOG(ERROR)
          << "Failed to grow cache file " << fileName_ << " to " << numRegions_
          << " regions. Error: " << e.what();
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
  stats_.regionsEvicted += candidates.size();
  writableRegions_ = candidates;
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

  int32_t writeIndex = 0;
  while (writeIndex < pins.size()) {
    auto space = getSpace(pins, writeIndex);
    if (!space.has_value()) {
      // No space can be reclaimed. The pins are freed when the caller is freed.
      ++stats_.writeSsdDropped;
      return;
    }

    auto [offset, available] = space.value();
    int32_t numWrittenEntries = 0;
    uint64_t writeOffset = offset;
    int32_t writeLength = 0;
    std::vector<iovec> writeIovecs;
    for (auto i = writeIndex; i < pins.size(); ++i) {
      auto* entry = pins[i].checkedEntry();
      const auto entrySize = entry->size();
      const auto numIovecs = numIoVectorsFromEntry(*entry);
      VELOX_CHECK_LE(numIovecs, IOV_MAX);
      if (writeIovecs.size() + numIovecs > IOV_MAX) {
        // Writes out the accumulated iovecs if it exceeds IOV_MAX limit.
        if (!write(writeOffset, writeLength, writeIovecs)) {
          // If write fails, we return without adding the pins to the cache. The
          // entries are unchanged.
          return;
        }
        writeIovecs.clear();
        available -= writeLength;
        writeOffset += writeLength;
        writeLength = 0;
      }
      if (writeLength + entrySize > available) {
        break;
      }
      addEntryToIovecs(*entry, writeIovecs);
      writeLength += entrySize;
      ++numWrittenEntries;
    }
    if (writeLength > 0) {
      VELOX_CHECK(!writeIovecs.empty());
      if (!write(writeOffset, writeLength, writeIovecs)) {
        return;
      }
      writeIovecs.clear();
      available -= writeLength;
      writeOffset += writeLength;
      writeLength = 0;
    }
    VELOX_CHECK_GE(fileSize_, writeOffset);

    {
      std::lock_guard<std::shared_mutex> l(mutex_);
      for (auto i = writeIndex; i < writeIndex + numWrittenEntries; ++i) {
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
        if (FLAGS_velox_ssd_verify_write) {
          verifyWrite(*entry, SsdRun(offset, size, checksum));
        }
        offset += size;
        ++stats_.entriesWritten;
        stats_.bytesWritten += size;
        bytesAfterCheckpoint_ += size;
      }
    }
    writeIndex += numWrittenEntries;
  }

  if (checkpointEnabled()) {
    checkpoint();
  }
}

bool SsdFile::write(
    int64_t offset,
    int64_t length,
    const std::vector<iovec>& iovecs) {
  try {
    writeFile_->write(iovecs, offset, length);
    return true;
  } catch (const std::exception&) {
    VELOX_SSD_CACHE_LOG(ERROR)
        << "Failed to write to SSD, file name: " << fileName_
        << ", size: " << iovecs.size() << ", offset: " << offset
        << ", error code: " << errno
        << ", error string: " << folly::errnoStr(errno);
    ++stats_.writeSsdErrors;
    return false;
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
  const auto rc =
      readFile_->pread(ssdRun.offset(), entry.size(), testData.get());
  VELOX_CHECK_EQ(rc.size(), entry.size());
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
  stats.deleteMetaFileErrors += stats_.deleteMetaFileErrors;
  stats.growFileErrors += stats_.growFileErrors;
  stats.writeSsdErrors += stats_.writeSsdErrors;
  stats.writeCheckpointErrors += stats_.writeCheckpointErrors;
  stats.readSsdErrors += stats_.readSsdErrors;
  stats.readCheckpointErrors += stats_.readCheckpointErrors;
  stats.readSsdCorruptions += stats_.readSsdCorruptions;
  stats.readWithoutChecksumChecks += stats_.readWithoutChecksumChecks;
}

void SsdFile::clear() {
  std::lock_guard<std::shared_mutex> l(mutex_);
  entries_.clear();
  std::fill(regionSizes_.begin(), regionSizes_.end(), 0);
  std::fill(erasedRegionSizes_.begin(), erasedRegionSizes_.end(), 0);
  writableRegions_.resize(numRegions_);
  std::iota(writableRegions_.begin(), writableRegions_.end(), 0);
  tracker_.clear();
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
      << kMaxErasedSizePct << "% entries removed, and " << entries_.size()
      << " left.";
  return true;
}

void SsdFile::logEviction(std::vector<int32_t>& regions) {
  if (!checkpointEnabled()) {
    return;
  }
  const auto length = regions.size() * sizeof(regions[0]);
  const std::vector<iovec> iovecs = {{regions.data(), length}};
  try {
    evictLogWriteFile_->write(iovecs, 0, static_cast<int64_t>(length));
  } catch (const std::exception& e) {
    ++stats_.writeSsdErrors;
    VELOX_SSD_CACHE_LOG(ERROR) << "Failed to log eviction: " << e.what();
  }
}

void SsdFile::deleteCheckpoint(bool keepLog) {
  if (checkpointDeleted_) {
    return;
  }

  if (evictLogWriteFile_ != nullptr) {
    if (keepLog) {
      truncateFile(evictLogWriteFile_.get());
    } else {
      deleteFile(std::move(evictLogWriteFile_));
    }
  }

  if (checkpointWriteFile_ != nullptr) {
    deleteFile(std::move(checkpointWriteFile_));
  }
}

void SsdFile::truncateFile(WriteFile* file) {
  VELOX_CHECK_NOT_NULL(file);
  file->truncate(0);
  file->flush();
}

void SsdFile::deleteFile(std::unique_ptr<WriteFile> file) {
  VELOX_CHECK_NOT_NULL(file);
  const auto filePath = file->getName();
  try {
    file->close();
    file.reset();
    if (fs_->exists(filePath)) {
      fs_->remove(filePath);
    }
  } catch (const std::exception& e) {
    ++stats_.deleteMetaFileErrors;
    VELOX_SSD_CACHE_LOG(ERROR)
        << fmt::format("Error in deleting file {}: {}", filePath, e.what());
  }
}

void SsdFile::checkpointError(int32_t rc, const std::string& error) {
  VELOX_SSD_CACHE_LOG(ERROR)
      << error << " with rc=" << rc
      << ", deleting checkpoint and continuing with checkpointing off";
  deleteCheckpoint();
  checkpointIntervalBytes_ = 0;
}

void SsdFile::allocateCheckpointBuffer() {
  VELOX_CHECK_NULL(checkpointBuffer_);
  checkpointBuffer_ = memory::memoryManager()->allocator()->allocateBytes(
      kCheckpointBufferSize);
  checkpointBufferedDataSize_ = 0;
}

void SsdFile::freeCheckpointBuffer() {
  VELOX_CHECK_NOT_NULL(checkpointBuffer_);
  memory::memoryManager()->allocator()->freeBytes(
      checkpointBuffer_, kCheckpointBufferSize);
  checkpointBuffer_ = nullptr;
  checkpointBufferedDataSize_ = 0;
}

void SsdFile::appendToCheckpointBuffer(const void* source, int32_t size) {
  VELOX_CHECK_NOT_NULL(checkpointBuffer_);
  maybeFlushCheckpointBuffer(size);
  ::memcpy(
      static_cast<void*>(
          static_cast<char*>(checkpointBuffer_) + checkpointBufferedDataSize_),
      source,
      size);
  checkpointBufferedDataSize_ += size;
}

void SsdFile::appendToCheckpointBuffer(const std::string& string) {
  appendToCheckpointBuffer(string.data(), string.length());
}

void SsdFile::maybeFlushCheckpointBuffer(uint32_t appendBytes, bool force) {
  if (checkpointBufferedDataSize_ > 0 &&
      (force ||
       checkpointBufferedDataSize_ + appendBytes >= kCheckpointBufferSize)) {
    VELOX_CHECK_NOT_NULL(checkpointBuffer_);
    checkpointWriteFile_->append(std::string_view(
        static_cast<const char*>(checkpointBuffer_),
        checkpointBufferedDataSize_));
    checkpointBufferedDataSize_ = 0;
  }
}

void SsdFile::flushCheckpointFile() {
  VELOX_CHECK_NOT_NULL(checkpointWriteFile_);
  maybeFlushCheckpointBuffer(0, true);
  checkpointWriteFile_->flush();
}

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
    // We schedule the potentially long fsync of the cache file on another
    // thread of the cache write executor, if available. If there is none, we do
    // the sync on this thread at the end.
    auto fileSync = std::make_shared<AsyncSource<int>>([this]() {
      writeFile_->flush();
      return std::make_unique<int>(0);
    });

    executor_->add([source = fileSync]() { source->prepare(); });

    try {
      VELOX_CHECK_NOT_NULL(checkpointWriteFile_);
      truncateFile(checkpointWriteFile_.get());
      // The checkpoint state file contains:
      // int32_t The 4 bytes of checkpoint version,
      // int32_t maxRegions,
      // int32_t numRegions,
      // regionScores from the 'tracker_',
      // {fileId, fileName} pairs,
      // kMapMarker,
      // {fileId, offset, SSdRun} triples,
      // kEndMarker.
      allocateCheckpointBuffer();
      SCOPE_EXIT {
        freeCheckpointBuffer();
      };
      const auto version = checkpointVersion();
      appendToCheckpointBuffer(checkpointVersion());
      appendToCheckpointBuffer(maxRegions_);
      appendToCheckpointBuffer(numRegions_);

      // Copy the region scores before writing out for tsan.
      appendToCheckpointBuffer(tracker_.copyScores());
      std::unordered_set<uint64_t> fileNums;
      for (const auto& entry : entries_) {
        const auto fileNum = entry.first.fileNum.id();
        if (fileNums.insert(fileNum).second) {
          appendToCheckpointBuffer(fileNum);
          const auto name = fileIds().string(fileNum);
          const int32_t length = name.size();
          appendToCheckpointBuffer(length);
          appendToCheckpointBuffer(name);
        }
      }

      appendToCheckpointBuffer(kCheckpointMapMarker);
      for (auto& pair : entries_) {
        const auto id = pair.first.fileNum.id();
        appendToCheckpointBuffer(id);
        appendToCheckpointBuffer(pair.first.offset);
        const auto offsetAndSize = pair.second.fileBits();
        appendToCheckpointBuffer(offsetAndSize);
        if (checksumEnabled_) {
          const auto checksum = pair.second.checksum();
          appendToCheckpointBuffer(checksum);
        }
      }

      // NOTE: we need to ensure cache file data sync update completes before
      // updating checkpoint file.
      fileSync->move();

      appendToCheckpointBuffer(kCheckpointEndMarker);
      flushCheckpointFile();
      ++stats_.checkpointsWritten;
    } catch (const std::exception& e) {
      ++stats_.writeCheckpointErrors;
      VELOX_SSD_CACHE_LOG(ERROR) << "Error in writing cehckpoint: " << e.what();
      fileSync->close();
      std::rethrow_exception(std::current_exception());
    }

    // NOTE: we shall truncate eviction log after checkpoint file sync
    // completes so that we never recover from an old checkpoint file without
    // log evictions. The latter might lead to data consistent issue.
    VELOX_CHECK_NOT_NULL(evictLogWriteFile_);
    evictLogWriteFile_->truncate(0);
    evictLogWriteFile_->flush();

    VELOX_SSD_CACHE_LOG(INFO)
        << "Checkpoint persisted with " << entries_.size() << " cache entries";
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

  filesystems::FileOptions writeFileOptions;
  writeFileOptions.shouldThrowOnFileAlreadyExists = false;

  const auto checkpointPath = checkpointFilePath();
  try {
    checkpointWriteFile_ =
        fs_->openFileForWrite(checkpointPath, writeFileOptions);
  } catch (const std::exception& e) {
    ++stats_.writeCheckpointErrors;
    VELOX_SSD_CACHE_LOG(ERROR) << fmt::format(
        "Could not initilize checkpoint file {} for writing: {}: ",
        checkpointPath,
        e.what());
  }

  const auto logPath = evictLogFilePath();
  try {
    evictLogWriteFile_ = fs_->openFileForWrite(logPath, writeFileOptions);
  } catch (std::exception& e) {
    ++stats_.openLogErrors;
    // Failure to open the log at startup is a process terminating error.
    VELOX_FAIL("Could not open evict log {}: {}", logPath, e.what());
  }

  try {
    readCheckpoint();
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
  VELOX_DCHECK_EQ(ssdRun.size(), entry.size());
  if (ssdRun.size() != entry.size()) {
    ++stats_.readWithoutChecksumChecks;
    VELOX_CACHE_LOG_EVERY_MS(WARNING, 1'000)
        << "SSD read without checksum due to cache request size mismatch, SSD cache size "
        << ssdRun.size() << " request size " << entry.size()
        << ", cache request: " << entry.toString();
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
        ssdRun.size());
  }
}

void SsdFile::disableFileCow() {
#ifdef linux
  const std::unordered_map<std::string, std::string> attributes = {
      {std::string(LocalWriteFile::Attributes::kNoCow), "true"}};
  writeFile_->setAttributes(attributes);
  if (evictLogWriteFile_ != nullptr) {
    evictLogWriteFile_->setAttributes(attributes);
  }
  if (checkpointWriteFile_ != nullptr) {
    checkpointWriteFile_->setAttributes(attributes);
  }
#endif // linux
}

namespace {
template <typename T>
T readNumber(common::FileInputStream* stream) {
  T data;
  stream->readBytes(reinterpret_cast<uint8_t*>(&data), sizeof(T));
  return data;
}

std::string readString(common::FileInputStream* stream, int32_t length) {
  std::string data(length, '\0');
  stream->readBytes(
      reinterpret_cast<uint8_t*>(const_cast<char*>(data.data())), length);
  return data;
}

template <typename T>
std::vector<T> readVector(common::FileInputStream* stream, int32_t size) {
  std::vector<T> dataVector(size);
  stream->readBytes(
      reinterpret_cast<uint8_t*>(dataVector.data()), size * sizeof(T));
  return dataVector;
}
} // namespace

void SsdFile::readCheckpoint() {
  const auto checkpointPath = checkpointFilePath();
  std::unique_ptr<common::FileInputStream> stream;
  try {
    auto checkpointReadFile = fs_->openFileForRead(checkpointPath);
    stream = std::make_unique<common::FileInputStream>(
        std::move(checkpointReadFile),
        1 << 20,
        memory::memoryManager()->cachePool());
  } catch (std::exception& e) {
    ++stats_.openCheckpointErrors;
    // Either the checkpoint file is corrupted or the file is just created, we
    // can start here and the writer threads will create the checkpoint file
    // later on flush
    VELOX_SSD_CACHE_LOG(WARNING) << fmt::format(
        "Error opening checkpoint file {}: Starting without checkpoint",
        e.what());
    return;
  }

  const auto versionMagic = readString(stream.get(), 4);
  const auto checkpoinHasChecksum =
      isChecksumEnabledOnCheckpointVersion(versionMagic);
  if (checksumEnabled_ && !checkpoinHasChecksum) {
    VELOX_SSD_CACHE_LOG(WARNING) << fmt::format(
        "Starting shard {} without checkpoint: checksum is enabled but the checkpoint was made without checksum, so skip the checkpoint recovery, checkpoint file {}",
        shardId_,
        checkpointPath);
    return;
  }

  const auto maxRegions = readNumber<int32_t>(stream.get());
  VELOX_CHECK_EQ(
      maxRegions,
      maxRegions_,
      "Trying to start from checkpoint with a different capacity");
  numRegions_ = readNumber<int32_t>(stream.get());

  const auto scores = readVector<double>(stream.get(), maxRegions_);
  std::unordered_map<uint64_t, StringIdLease> idMap;
  for (;;) {
    const auto id = readNumber<uint64_t>(stream.get());
    if (id == kCheckpointMapMarker) {
      break;
    }
    const auto length = readNumber<int32_t>(stream.get());
    const auto name = readString(stream.get(), length);
    idMap[id] = StringIdLease(fileIds(), id, name);
  }

  const auto logPath = evictLogFilePath();
  const auto evictLogReadFile = fs_->openFileForRead(logPath);
  const auto logSize = evictLogReadFile->size();
  std::vector<uint32_t> evicted(logSize / sizeof(uint32_t));
  try {
    evictLogReadFile->pread(0, logSize, evicted.data());
  } catch (const std::exception& e) {
    ++stats_.readCheckpointErrors;
    VELOX_FAIL("Failed to read eviction log: {}", e.what());
  }
  std::unordered_set<uint32_t> evictedMap;
  for (auto region : evicted) {
    evictedMap.insert(region);
  }

  std::vector<uint32_t> regionCacheSizes(numRegions_, 0);
  for (;;) {
    const auto fileNum = readNumber<uint64_t>(stream.get());
    if (fileNum == kCheckpointEndMarker) {
      break;
    }
    const auto offset = readNumber<uint64_t>(stream.get());
    const auto fileBits = readNumber<uint64_t>(stream.get());
    uint32_t checksum = 0;
    if (checkpoinHasChecksum) {
      checksum = readNumber<uint32_t>(stream.get());
    }
    const auto run = SsdRun(fileBits, checksum);
    const auto region = regionIndex(run.offset());
    // Check that the recovered entry does not fall in an evicted region.
    if (evictedMap.find(region) != evictedMap.end()) {
      continue;
    }
    // The file may have a different id on restore.
    const auto it = idMap.find(fileNum);
    VELOX_CHECK(it != idMap.end());
    FileCacheKey key{it->second, offset};
    entries_[std::move(key)] = run;
    regionCacheSizes[region] += run.size();
    regionSizes_[region] = std::max<uint32_t>(
        regionSizes_[region], regionOffset(run.offset()) + run.size());
  }

  // NOTE: we might erase entries from a region for TTL eviction, so we need to
  // set the region size to the max offset of the recovered cache entry from the
  // region. Correspondingly, we substract the cached size from the region size
  // to get the erased size.
  for (auto region = 0; region < numRegions_; ++region) {
    VELOX_CHECK_LE(regionSizes_[region], kRegionSize);
    VELOX_CHECK_LE(regionCacheSizes[region], regionSizes_[region]);
    erasedRegionSizes_[region] =
        regionSizes_[region] - regionCacheSizes[region];
  }

  ++stats_.checkpointsRead;
  stats_.entriesRecovered += entries_.size();

  // The state is successfully read. Install the access frequency scores and
  // evicted regions.
  VELOX_CHECK_EQ(scores.size(), tracker_.regionScores().size());
  // Set the writable regions by deduplicated evicted regions.
  writableRegions_.clear();
  for (auto region : evictedMap) {
    writableRegions_.push_back(region);
  }
  tracker_.setRegionScores(scores);

  uint64_t cachedBytes{0};
  for (const auto regionSize : regionSizes_) {
    cachedBytes += regionSize;
  }
  VELOX_SSD_CACHE_LOG(INFO) << fmt::format(
      "Starting shard {} from checkpoint with {} entries, {} cached data, {} regions with {} free, with checksum write {}, read verification {}, checkpoint file {}",
      shardId_,
      entries_.size(),
      succinctBytes(cachedBytes),
      numRegions_,
      writableRegions_.size(),
      checksumEnabled_ ? "enabled" : "disabled",
      checksumReadVerificationEnabled_ ? "enabled" : "disabled",
      checkpointFilePath());
}

} // namespace facebook::velox::cache
