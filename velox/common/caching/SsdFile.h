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

#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/caching/SsdFileTracker.h"
#include "velox/common/file/File.h"

#include <gflags/gflags.h>

DECLARE_bool(ssd_odirect);
DECLARE_bool(ssd_verify_write);

namespace facebook::velox::cache {

// A 64 bit word describing a SSD cache entry in an SsdFile. The low
// 23 bits are the size, for a maximum entry size of 8MB. The high
// bits are the offset.
class SsdRun {
 public:
  static constexpr int32_t kSizeBits = 23;

  SsdRun() : bits_(0) {}

  SsdRun(uint64_t offset, uint32_t size)
      : bits_((offset << kSizeBits) | ((size - 1))) {
    VELOX_CHECK_LT(offset, 1L << (64 - kSizeBits));
    VELOX_CHECK_LT(size - 1, 1 << kSizeBits);
  }

  SsdRun(uint64_t bits) : bits_(bits) {}

  SsdRun(const SsdRun& other) = default;
  SsdRun(SsdRun&& other) = default;

  void operator=(const SsdRun& other) {
    bits_ = other.bits_;
  }
  void operator=(SsdRun&& other) {
    bits_ = other.bits_;
  }

  uint64_t offset() const {
    return (bits_ >> kSizeBits);
  }

  uint32_t size() const {
    return (bits_ & ((1 << kSizeBits) - 1)) + 1;
  }

  // Returns raw bits for serialization.
  uint64_t bits() const {
    return bits_;
  }

 private:
  uint64_t bits_;
};

// Represents an SsdFile entry that is planned for load or being
// loaded. This is destroyed after load. Destruction decrements the
// pin count of the corresponding region of 'file_'. While there are
// pins, the region cannot be evicted.
class SsdPin {
 public:
  SsdPin() : file_(nullptr) {}

  // Constructs a pin referencing 'run' in 'file'. The region must be
  // pinned before constructing the pin.
  SsdPin(SsdFile& file, SsdRun run);

  SsdPin(const SsdPin& other) = delete;

  void operator=(const SsdPin& OTHER) = delete;

  SsdPin(SsdPin&& other) noexcept {
    run_ = other.run_;
    file_ = other.file_;
    other.file_ = nullptr;
  }

  ~SsdPin();

  // Resets 'this' to default-constructed state.
  void clear();

  void operator=(SsdPin&&);

  bool empty() const {
    return file_ == nullptr;
  }
  SsdFile* FOLLY_NONNULL file() const {
    return file_;
  }

  SsdRun run() const {
    return run_;
  }

  std::string toString() const;

 private:
  SsdFile* FOLLY_NULLABLE file_;
  SsdRun run_;
};

// Metrics for SSD cache. Maintained by SsdFile and aggregated by SsdCache.
struct SsdCacheStats {
  uint64_t entriesWritten{0};
  uint64_t bytesWritten{0};
  uint64_t entriesRead{0};
  uint64_t bytesRead{0};
  uint64_t entriesCached{0};
  uint64_t bytesCached{0};
  int32_t numPins{0};
};

// A shard of SsdCache. Corresponds to one file on SSD.  The data
// backed by each SsdFile is selected on a hash of the storage file
// number of the cached data. Each file consists of an integer number
// of 64MB regions. Each region has a pin count and an read
// count. Cache replacement takes place region by region, preferring
// regions with a smaller read count. Entries do not span
// regions. Otherwise entries are consecutive byte ranges inside
// their region.
class SsdFile {
 public:
  static constexpr uint64_t kRegionSize = 1 << 26; // 64MB

  // Constructs a cache backed by filename. Discards any previous
  // contents of filename.
  SsdFile(
      const std::string& filename,
      int32_t shardId,
      int32_t maxRegions,
      int64_t checkpointInternalBytes = 0,
      folly::Executor* FOLLY_NULLABLE executor = nullptr);

  // Adds entries of  'pins'  to this file. 'pins' must be in read mode and
  // those pins that are successfully added to SSD are marked as being on SSD.
  // The file of the entries must be a file that is backed by 'this'.
  void write(std::vector<CachePin>& pins);

  // Finds an entry for 'key'. If no entry is found, the returned pin is empty.
  SsdPin find(RawFileCacheKey key);

  // Erases 'key'
  bool erase(RawFileCacheKey key);

  // Copies the data in 'ssdPins' into 'pins'. Coalesces IO for nearby
  // entries if they are in ascending order and near enough.
  CoalesceIoStats load(
      const std::vector<SsdPin>& ssdPins,
      const std::vector<CachePin>& pins);

  // Increments the pin count of the region of 'offset'.
  void pinRegion(uint64_t offset);

  // Decrements the pin count of the region of 'offset'. If the pin
  // count goes to zero and evict is due, starts the evict.
  void unpinRegion(uint64_t offset);

  // Asserts that the region of 'offset' is pinned. This is called by
  // the pin holder. The pin count can be read without mutex.
  void checkPinned(uint64_t offset) const {
    VELOX_CHECK_LT(0, regionPins_[regionIndex(offset)]);
  }

  // Returns the region number corresponding to offset.
  static int32_t regionIndex(uint64_t offset) {
    return offset / kRegionSize;
  }

  // Updates the read count of a region.
  void regionRead(int32_t region, int32_t size) {
    tracker_.regionRead(region, size);
  }

  int32_t maxRegions() const {
    return maxRegions_;
  }

  int32_t shardId() const {
    return shardId_;
  }

  // Adds 'stats_' to 'stats'.
  void updateStats(SsdCacheStats& stats) const;

  // Resets this' to a post-construction empty state. See SsdCache::clear().
  void clear();

  // Deletes the backing file. Used in testing.
  void deleteFile();

  // Writes a checkpoint state that can be recovered from. The
  // checkpoint is serialized on 'mutex_'. If 'force' is false,
  // rechecks that at least 'checkpointIntervalBytes_' have been
  // written since last checkpoint and silently returns if not.
  void checkpoint(bool force = false);

 private:
  // 4 first bytes of a checkpoint file. Allows distinguishing between format
  // versions.
  static constexpr const char* FOLLY_NONNULL kCheckpointMagic = "CPT1";
  // Magic number separating file names from cache entry data in checkpoint
  // file.
  static constexpr int64_t kCheckpointMapMarker = 0xfffffffffffffffe;
  // Magic number at end of completed checkpoint file.
  static constexpr int64_t kCheckpointEndMarker = 0xcbedf11e;

  // Increments the pin count of the region of 'offset'. Caller must hold
  // 'mutex_'.
  void pinRegionLocked(uint64_t offset) {
    ++regionPins_[regionIndex(offset)];
  }

  // Returns [offset, size] of contiguous space for storing data of a
  // number of contiguous 'pins' starting with the pin at index
  // 'begin'.  Returns nullopt if there is no space. The space does
  // not necessarily cover all the pins, so multiple calls starting at
  // the first unwritten pin may be needed.
  std::optional<std::pair<uint64_t, int32_t>> getSpace(
      const std::vector<CachePin>& pins,
      int32_t begin);

  // Removes all 'entries_' that reference data in regions described by
  // 'regionIndices'.
  void clearRegionEntriesLocked(const std::vector<int32_t>& regionIndices);

  // Clears one or more  regions for accommodating new entries. The regions are
  // added to 'writableRegions_'. Returns true if regions could be cleared.
  bool growOrEvictLocked();

  // Reads the backing file with ReadFile::preadv().
  void read(uint64_t offset, const std::vector<folly::Range<char*>>& buffers);

  // Verifies that 'entry' has the data at 'run'.
  void verifyWrite(AsyncDataCacheEntry& entry, SsdRun run);

  // Deletes checkpoint files. If 'keepLog' is true, truncates and syncs the
  // eviction log and leaves this open.
  void deleteCheckpoint(bool keepLog = false);

  // Reads a checkpoint state file and sets 'this' accordingly if read
  // is successful. Return true for successful read. A failed read
  // deletes the checkpoint and leaves the log truncated open.
  void readCheckpoint(std::ifstream& state);

  // Logs an error message, deletes the checkpoint and stop making new
  // checkpoints.
  void checkpointError(int32_t rc, const std::string& error);

  // Looks for a checkpointed state and sets the state of 'this' by
  // the checkpointed state iif the state is complete and
  // readable. Does not modify 'this' if the state is corrupt,
  // e.g. there was a crash during writing the checkpoint. Initializes
  // the files for making new checkpoints.
  void initializeCheckpoint();

  // Synchronously logs that 'regions' are no longer valid in a possibly xisting
  // checkpoint.
  void logEviction(const std::vector<int32_t>& regions);

  // Serializes access to all private data members.
  std::mutex mutex_;
  // Name of cache file, used as prefix for checkpoint files.
  std::string fileName_;
  static constexpr const char* FOLLY_NONNULL kLogExtension = ".log";
  static constexpr const char* FOLLY_NONNULL kCheckpointExtension = ".cpt";

  // Shard index within 'cache_'.
  int32_t shardId_;

  // Number of kRegionSize regions in the file.
  int32_t numRegions_{0};

  // True if stopped serving traffic. Happens if no evictions are
  // possible due to everything being pinned. Clears when pins
  // decrease and space can be cleared.
  bool suspended_{false};

  // Maximum size of the backing file in kRegionSize units.
  const int32_t maxRegions_;

  // Number of used bytes in in each region. A new entry must fit
  // between the offset and the end of the region. This is subscripted
  // with the region index. The regionIndex times kRegionSize is an
  // offset into the file.
  std::vector<uint32_t> regionSize_;

  // Indices of regions available for writing new entries.
  std::vector<int32_t> writableRegions_;

  // Tracker for access frequencies and eviction.
  SsdFileTracker tracker_;

  // Pin count for each region.
  std::vector<int32_t> regionPins_;

  // Map of file number and offset to location in file.
  folly::F14FastMap<FileCacheKey, SsdRun> entries_;

  // Name of backing file.
  const std::string filename_;

  // File descriptor. 0 (stdin) means file not open.
  int32_t fd_{0};

  // Size of the backing file in bytes. Must be multiple of kRegionSize.
  uint64_t fileSize_{0};

  // ReadFile made from 'fd_'.
  std::unique_ptr<ReadFile> readFile_;

  // Counters.
  SsdCacheStats stats_;

  // Checkpoint after every 'checkpointIntervalBytes_' written into
  // this file. 0 means no checkpointing. This is set to 0 if
  // checkpointing fails.
  int64_t checkpointIntervalBytes_{0};

  // Executor for async fsync in checkpoint.
  folly::Executor* FOLLY_NULLABLE executor_;

  // Count of bytes written after last checkpoint.
  std::atomic<uint64_t> bytesAfterCheckpoint_{0};

  // fd for logging evictions.
  int32_t evictLogFd_{0};

  // True if there was an error with checkpoint and the checkpoint was deleted.
  bool checkpointDeleted_{false};
};

} // namespace facebook::velox::cache
