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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/caching/FileIds.h"
#include "velox/common/caching/SsdCache.h"
#include "velox/common/memory/Memory.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

#include <fcntl.h>
#include <folly/executors/QueuedImmediateExecutor.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

using namespace facebook::velox;
using namespace facebook::velox::cache;

using facebook::velox::memory::MemoryAllocator;

// Represents an entry written to SSD.
struct TestEntry {
  FileCacheKey key;
  uint64_t ssdOffset;
  int32_t size;

  TestEntry(FileCacheKey _key, uint64_t _ssdOffset, int32_t _size)
      : key(_key), ssdOffset(_ssdOffset), size(_size) {}
};

class SsdFileTest : public testing::Test {
 protected:
  static constexpr int64_t kMB = 1 << 20;

  void SetUp() override {
    memory::MemoryManager::testingSetInstance({});
  }

  void TearDown() override {
    if (ssdFile_) {
      ssdFile_->testingDeleteFile();
    }
    if (cache_) {
      cache_->shutdown();
    }
    fileIds().testingReset();
  }

  void initializeCache(
      int64_t ssdBytes = 0,
      uint64_t checkpointIntervalBytes = 0,
      bool checksumEnabled = false,
      bool checksumReadVerificationEnabled = false,
      bool disableFileCow = false) {
    // tmpfs does not support O_DIRECT, so turn this off for testing.
    FLAGS_ssd_odirect = false;
    cache_ = AsyncDataCache::create(memory::memoryManager()->allocator());
    fileName_ = StringIdLease(fileIds(), "fileInStorage");
    tempDirectory_ = exec::test::TempDirectoryPath::create();
    initializeSsdFile(
        ssdBytes,
        checkpointIntervalBytes,
        checksumEnabled,
        checksumReadVerificationEnabled,
        disableFileCow);
  }

  void initializeSsdFile(
      int64_t ssdBytes = 0,
      uint64_t checkpointIntervalBytes = 0,
      bool checksumEnabled = false,
      bool checksumReadVerificationEnabled = false,
      bool disableFileCow = false) {
    SsdFile::Config config(
        fmt::format("{}/ssdtest", tempDirectory_->getPath()),
        0, // shardId
        bits::roundUp(ssdBytes, SsdFile::kRegionSize) / SsdFile::kRegionSize,
        checkpointIntervalBytes,
        disableFileCow,
        checksumEnabled,
        checksumReadVerificationEnabled);
    ssdFile_ = std::make_unique<SsdFile>(config);
  }

  // Corrupts the file by invalidate the last 1/10th of its content.
  void corruptSsdFile(const std::string& path) {
    const auto fd = ::open(path.c_str(), O_WRONLY);
    const auto size = ::lseek(fd, 0, SEEK_END);
    ASSERT_EQ(ftruncate(fd, size / 10 * 9), 0);
    ASSERT_EQ(ftruncate(fd, size), 0);
  }

  static void initializeContents(int64_t sequence, memory::Allocation& alloc) {
    bool first = true;
    for (int32_t i = 0; i < alloc.numRuns(); ++i) {
      memory::Allocation::PageRun run = alloc.runAt(i);
      int64_t* ptr = reinterpret_cast<int64_t*>(run.data());
      int32_t numWords =
          run.numPages() * memory::AllocationTraits::kPageSize / sizeof(void*);
      for (int32_t offset = 0; offset < numWords; offset++) {
        if (first) {
          ptr[offset] = sequence;
          first = false;
        } else {
          ptr[offset] = offset + sequence;
        }
      }
    }
  }

  // Checks that the contents are consistent with what is set in
  // initializeContents.
  static void checkContents(
      const memory::Allocation& alloc,
      int32_t numBytes,
      bool expectEqual = true) {
    bool first = true;
    int64_t sequence;
    int32_t bytesChecked = sizeof(int64_t);
    for (int32_t i = 0; i < alloc.numRuns(); ++i) {
      memory::Allocation::PageRun run = alloc.runAt(i);
      int64_t* ptr = reinterpret_cast<int64_t*>(run.data());
      int32_t numWords =
          run.numPages() * memory::AllocationTraits::kPageSize / sizeof(void*);
      for (int32_t offset = 0; offset < numWords; offset++) {
        if (first) {
          sequence = ptr[offset];
          first = false;
        } else {
          bytesChecked += sizeof(int64_t);
          if (bytesChecked >= numBytes) {
            return;
          }
          if (expectEqual) {
            ASSERT_EQ(ptr[offset], offset + sequence);
          } else {
            ASSERT_NE(ptr[offset], offset + sequence);
          }
        }
      }
    }
  }

  // Gets consecutive entries from file 'fileId' starting at 'startOffset' with
  // sizes between 'minSize' and 'maxSize'. Sizes start at 'minSize' and double
  // each time and go back to 'minSize' after exceeding 'maxSize'. This stops
  // after the total size has exceeded 'totalSize'. The entries are returned as
  // pins. The pins are exclusive for newly created entries and shared for
  // existing ones. New entries are deterministically initialized from 'fileId'
  // and the entry's offset.
  std::vector<CachePin> makePins(
      uint64_t fileId,
      uint64_t startOffset,
      int32_t minSize,
      int32_t maxSize,
      int64_t totalSize) {
    auto offset = startOffset;
    int64_t bytesFromCache = 0;
    auto size = minSize;
    std::vector<CachePin> pins;
    while (bytesFromCache < totalSize) {
      pins.push_back(
          cache_->findOrCreate(RawFileCacheKey{fileId, offset}, size, nullptr));
      bytesFromCache += size;
      EXPECT_FALSE(pins.back().empty());
      auto entry = pins.back().entry();
      if (entry && entry->isExclusive()) {
        initializeContents(fileId + offset, entry->data());
      }
      offset += size;
      size *= 2;
      if (size > maxSize) {
        size = minSize;
      }
    }
    return pins;
  }

  std::vector<SsdPin> pinAllRegions(const std::vector<TestEntry>& entries) {
    std::vector<SsdPin> pins;
    int32_t lastRegion = -1;
    for (auto& entry : entries) {
      if (entry.ssdOffset / SsdFile::kRegionSize != lastRegion) {
        lastRegion = entry.ssdOffset / SsdFile::kRegionSize;
        pins.push_back(
            ssdFile_->find(RawFileCacheKey{fileName_.id(), entry.key.offset}));
        EXPECT_FALSE(pins.back().empty());
      }
    }
    return pins;
  }

  void readAndCheckPins(const std::vector<CachePin>& pins) {
    std::vector<SsdPin> ssdPins;
    ssdPins.reserve(pins.size());
    for (auto& pin : pins) {
      ssdPins.push_back(ssdFile_->find(RawFileCacheKey{
          pin.entry()->key().fileNum.id(), pin.entry()->key().offset}));
      EXPECT_FALSE(ssdPins.back().empty());
    }
    ssdFile_->load(ssdPins, pins);
    for (auto& pin : pins) {
      checkContents(pin.entry()->data(), pin.entry()->size());
    }
  }

  void checkEvictionBlocked(
      std::vector<TestEntry>& allEntries,
      uint64_t ssdSize) {
    auto ssdPins = pinAllRegions(allEntries);
    auto pins = makePins(fileName_.id(), ssdSize, 4096, 2048 * 1025, 62 * kMB);
    ssdFile_->write(pins);
    // Only Some pins get written because space cannot be cleared
    // because all regions are pinned. The file will not give out new
    // pins so that this situation is not continued
    EXPECT_TRUE(
        ssdFile_->find(RawFileCacheKey{fileName_.id(), ssdSize}).empty());
    int32_t numWritten = 0;
    for (auto& pin : pins) {
      if (pin.entry()->ssdFile()) {
        ++numWritten;
        allEntries.emplace_back(
            pin.entry()->key(), pin.entry()->ssdOffset(), pin.entry()->size());
      }
    }
    EXPECT_LT(numWritten, pins.size());
    // vector::clear() does not guarantee the release order; we need to clear
    // the pins in the correct order explicitly.
    for (auto& pin : ssdPins) {
      pin.clear();
    }
    ssdPins.clear();

    // The pins were cleared and the file is no longer suspended. Check that the
    // entry that was not found is found now.
    EXPECT_FALSE(
        ssdFile_->find(RawFileCacheKey{fileName_.id(), ssdSize}).empty());

    pins.erase(pins.begin(), pins.begin() + numWritten);
    ssdFile_->write(pins);
    for (auto& pin : pins) {
      if (pin.entry()->ssdFile()) {
        allEntries.emplace_back(
            pin.entry()->key(), pin.entry()->ssdOffset(), pin.entry()->size());
      }
    }
    // Clear the pins and read back. Clear must complete before
    // makePins so that there are no pre-existing exclusive pins for the same
    // key.
    pins.clear();
    pins = makePins(fileName_.id(), ssdSize, 4096, 2048 * 1025, 62 * kMB);
    readAndCheckPins(pins);
  }

  // Reads back the found entries and check their contents, return the number of
  // entries found.
  int32_t checkEntries(
      const std::vector<TestEntry>& entries,
      bool expectEqual = true) {
    int32_t numFound = 0;
    for (auto& entry : entries) {
      std::vector<CachePin> pins;
      pins.push_back(cache_->findOrCreate(
          RawFileCacheKey{entry.key.fileNum.id(), entry.key.offset},
          entry.size,
          nullptr));
      if (pins.back().entry()->isExclusive()) {
        std::vector<SsdPin> ssdPins;
        ssdPins.push_back(ssdFile_->find(
            RawFileCacheKey{entry.key.fileNum.id(), entry.key.offset}));
        if (!ssdPins.back().empty()) {
          ++numFound;
          ssdFile_->load(ssdPins, pins);
          checkContents(
              pins[0].entry()->data(), pins[0].entry()->size(), expectEqual);
        }
      }
    }
    return numFound;
  }

  std::shared_ptr<exec::test::TempDirectoryPath> tempDirectory_;

  std::shared_ptr<AsyncDataCache> cache_;
  StringIdLease fileName_;

  std::unique_ptr<SsdFile> ssdFile_;
};

TEST_F(SsdFileTest, writeAndRead) {
  constexpr int64_t kSsdSize = 16 * SsdFile::kRegionSize;
  std::vector<TestEntry> allEntries;
  initializeCache(kSsdSize);
  FLAGS_ssd_verify_write = true;
  for (auto startOffset = 0; startOffset <= kSsdSize - SsdFile::kRegionSize;
       startOffset += SsdFile::kRegionSize) {
    auto pins =
        makePins(fileName_.id(), startOffset, 4096, 2048 * 1025, 62 * kMB);
    ssdFile_->write(pins);
    for (auto& pin : pins) {
      EXPECT_EQ(ssdFile_.get(), pin.entry()->ssdFile());
      allEntries.emplace_back(
          pin.entry()->key(), pin.entry()->ssdOffset(), pin.entry()->size());
    };
  }

  // The SsdFile is almost full and the memory cache has the last batch written
  // and a few entries from the batch before that.
  // We read back the same batches and check
  // contents.
  for (auto startOffset = 0; startOffset <= kSsdSize - SsdFile::kRegionSize;
       startOffset += SsdFile::kRegionSize) {
    auto pins =
        makePins(fileName_.id(), startOffset, 4096, 2048 * 1025, 62 * kMB);
    readAndCheckPins(pins);
  }
  checkEvictionBlocked(allEntries, kSsdSize);
  // We rewrite the cache with new entries.

  for (auto startOffset = kSsdSize + SsdFile::kRegionSize;
       startOffset <= kSsdSize * 2;
       startOffset += SsdFile::kRegionSize) {
    auto pins =
        makePins(fileName_.id(), startOffset, 4096, 2048 * 1025, 62 * kMB);
    ssdFile_->write(pins);
    for (auto& pin : pins) {
      EXPECT_EQ(ssdFile_.get(), pin.entry()->ssdFile());
      allEntries.emplace_back(
          pin.entry()->key(), pin.entry()->ssdOffset(), pin.entry()->size());
    }
  }

  // We check how many entries are found. The earliest writes will have been
  // evicted. We read back the found entries and check their contents.
  for (auto& entry : allEntries) {
    std::vector<CachePin> pins;

    pins.push_back(cache_->findOrCreate(
        RawFileCacheKey{fileName_.id(), entry.key.offset},
        entry.size,
        nullptr));
    if (pins.back().entry()->isExclusive()) {
      std::vector<SsdPin> ssdPins;

      ssdPins.push_back(
          ssdFile_->find(RawFileCacheKey{fileName_.id(), entry.key.offset}));
      if (!ssdPins.back().empty()) {
        ssdFile_->load(ssdPins, pins);
        checkContents(pins[0].entry()->data(), pins[0].entry()->size());
      }
    }
  }

  // Test cache writes with different iobufs sizes.
  for (int numPins : {0, 1, IOV_MAX - 1, IOV_MAX, IOV_MAX + 1}) {
    SCOPED_TRACE(fmt::format("numPins: {}", numPins));
    auto pins = makePins(fileName_.id(), 0, 4096, 4096, 4096 * numPins);
    EXPECT_EQ(pins.size(), numPins);
    ssdFile_->write(pins);
    readAndCheckPins(pins);
    pins.clear();
  }
}

TEST_F(SsdFileTest, checkpoint) {
  constexpr int64_t kSsdSize = 16 * SsdFile::kRegionSize;
  const uint64_t checkpointIntervalBytes = 5 * SsdFile::kRegionSize;
  const auto fileNameAlt = StringIdLease(fileIds(), "fileInStorageAlt");
  FLAGS_ssd_verify_write = true;
  initializeCache(kSsdSize, checkpointIntervalBytes);

  std::vector<TestEntry> allEntries;
  for (auto startOffset = 0; startOffset <= kSsdSize - SsdFile::kRegionSize;
       startOffset += SsdFile::kRegionSize) {
    auto pins =
        makePins(fileName_.id(), startOffset, 4096, 2048 * 1025, 62 * kMB);
    // Each region has one entry from `fileNameAlt`.
    pins.push_back(cache_->findOrCreate(
        RawFileCacheKey{fileNameAlt.id(), (uint64_t)startOffset},
        1024,
        nullptr));
    ssdFile_->write(pins);
    for (auto& pin : pins) {
      EXPECT_EQ(ssdFile_.get(), pin.entry()->ssdFile());
      allEntries.emplace_back(
          pin.entry()->key(), pin.entry()->ssdOffset(), pin.entry()->size());
    };
    readAndCheckPins(pins);
  }
  const auto originalRegionScores = ssdFile_->testingCopyScores();
  EXPECT_EQ(originalRegionScores.size(), 16);

  // Re-initialize SSD file from checkpoint.
  ssdFile_->checkpoint(true);
  initializeSsdFile(kSsdSize, checkpointIntervalBytes);
  const auto recoveredRegionScores = ssdFile_->testingCopyScores();
  EXPECT_EQ(recoveredRegionScores.size(), 16);
  EXPECT_EQ(originalRegionScores, recoveredRegionScores);

  // Reconstruct cache pins and check the recovered content from cache file.
  for (auto startOffset = 0; startOffset <= kSsdSize - SsdFile::kRegionSize;
       startOffset += SsdFile::kRegionSize) {
    auto pins =
        makePins(fileName_.id(), startOffset, 4096, 2048 * 1025, 62 * kMB);
    pins.push_back(cache_->findOrCreate(
        RawFileCacheKey{fileNameAlt.id(), (uint64_t)startOffset},
        1024,
        nullptr));
    readAndCheckPins(pins);
  }
  // All entries can be found.
  auto numEntriesFound = checkEntries(allEntries);
  EXPECT_EQ(numEntriesFound, allEntries.size());

  // Test removeFileEntries.
  folly::F14FastSet<uint64_t> filesToRemove{fileName_.id()};
  folly::F14FastSet<uint64_t> filesRetained{};
  auto stats = ssdFile_->testingStats();
  EXPECT_EQ(stats.entriesAgedOut, 0);
  EXPECT_EQ(stats.regionsAgedOut, 0);
  EXPECT_EQ(stats.regionsEvicted, 0);

  // Block eviction.
  auto ssdPins = pinAllRegions(allEntries);
  ssdFile_->removeFileEntries(filesToRemove, filesRetained);
  EXPECT_EQ(ssdFile_->testingNumWritableRegions(), 0);
  EXPECT_EQ(filesRetained.size(), 1);
  numEntriesFound = checkEntries(allEntries);
  EXPECT_EQ(numEntriesFound, allEntries.size());
  auto prevStats = stats;
  stats = ssdFile_->testingStats();
  EXPECT_EQ(stats.entriesAgedOut - prevStats.entriesAgedOut, 0);
  EXPECT_EQ(stats.regionsAgedOut - prevStats.regionsAgedOut, 0);
  EXPECT_EQ(stats.regionsEvicted - prevStats.regionsEvicted, 0);

  // Unblock eviction.
  ssdPins.clear();
  filesRetained.clear();
  ssdFile_->removeFileEntries(filesToRemove, filesRetained);
  // All regions have been evicted and marked as writable.
  EXPECT_EQ(ssdFile_->testingNumWritableRegions(), 16);
  EXPECT_EQ(filesRetained.size(), 0);
  numEntriesFound = checkEntries(allEntries);
  EXPECT_EQ(numEntriesFound, 0);
  prevStats = stats;
  stats = ssdFile_->testingStats();
  EXPECT_EQ(
      stats.entriesAgedOut - prevStats.entriesAgedOut, allEntries.size() - 16);
  EXPECT_EQ(stats.regionsAgedOut - prevStats.regionsAgedOut, 16);
  EXPECT_EQ(stats.regionsEvicted - prevStats.regionsEvicted, 16);

  // Re-initialize SSD file from checkpoint. Since all regions were evicted, no
  // entries should be found.
  initializeSsdFile(kSsdSize, checkpointIntervalBytes);
  numEntriesFound = checkEntries(allEntries);
  EXPECT_EQ(numEntriesFound, 0);
}

TEST_F(SsdFileTest, fileCorruption) {
  constexpr int64_t kSsdSize = 16 * SsdFile::kRegionSize;
  const uint64_t checkpointIntervalBytes = 5 * SsdFile::kRegionSize;
  FLAGS_ssd_verify_write = true;

  const auto populateCache = [&](std::vector<TestEntry>& entries) {
    entries.clear();
    for (auto startOffset = 0; startOffset <= kSsdSize - SsdFile::kRegionSize;
         startOffset += SsdFile::kRegionSize) {
      auto pins =
          makePins(fileName_.id(), startOffset, 4096, 2048 * 1025, 62 * kMB);
      ssdFile_->write(pins);
      for (auto& pin : pins) {
        EXPECT_EQ(ssdFile_.get(), pin.entry()->ssdFile());
        entries.emplace_back(
            pin.entry()->key(), pin.entry()->ssdOffset(), pin.entry()->size());
      };
    }
  };

  // Initialize cache with checksum write enabled.
  initializeCache(kSsdSize, checkpointIntervalBytes, true, true);
  std::vector<TestEntry> allEntries;
  populateCache(allEntries);
  // All entries can be found.
  EXPECT_EQ(checkEntries(allEntries), allEntries.size());
  EXPECT_EQ(ssdFile_->testingStats().readSsdCorruptions, 0);

  // Corrupt the SSD file, initialize the cache from checkpoint without read
  // verification.
  ssdFile_->checkpoint(true);
  corruptSsdFile(fmt::format("{}/ssdtest", tempDirectory_->getPath()));
  initializeSsdFile(kSsdSize, checkpointIntervalBytes, true, false);
  // Cache can be loaded but the data of the last part is corrupted.
  EXPECT_EQ(checkEntries({allEntries.begin(), allEntries.begin() + 100}), 100);
  EXPECT_EQ(
      checkEntries({allEntries.end() - 100, allEntries.end()}, false), 100);

  // Corrupt the SSD file, initialize the cache from checkpoint with read
  // verification enabled.
  ssdFile_->checkpoint(true);
  initializeSsdFile(kSsdSize, checkpointIntervalBytes, true, true);
  // Entries at the front are still loadable.
  EXPECT_EQ(checkEntries({allEntries.begin(), allEntries.begin() + 100}), 100);
  EXPECT_EQ(ssdFile_->testingStats().readSsdCorruptions, 0);
  // The last 1/10 entries are corrupted and cannot be loaded.
  VELOX_ASSERT_THROW(checkEntries(allEntries), "Corrupt SSD cache entry");
  EXPECT_GT(ssdFile_->testingStats().readSsdCorruptions, 0);
  // New entries can be written.
  populateCache(allEntries);

  // Corrupt the Checkpoint file. Cache cannot be recovered. All entries are
  // lost.
  ssdFile_->checkpoint(true);
  corruptSsdFile(ssdFile_->getCheckpointFilePath());
  EXPECT_EQ(ssdFile_->testingStats().readCheckpointErrors, 0);
  initializeSsdFile(kSsdSize, checkpointIntervalBytes, true, true);
  EXPECT_EQ(checkEntries(allEntries), 0);
  EXPECT_EQ(ssdFile_->testingStats().readCheckpointErrors, 1);
  // New entries can be written.
  populateCache(allEntries);
}

TEST_F(SsdFileTest, recoverFromCheckpointWithChecksum) {
  constexpr int64_t kSsdSize = 4 * SsdFile::kRegionSize;
  const uint64_t checkpointIntervalBytes = 3 * SsdFile::kRegionSize;
  FLAGS_ssd_verify_write = true;

  // Test if cache data can be recovered with different settings.
  struct {
    bool writeEnabled;
    bool readVerificationEnabled;
    bool writeEnabledOnRecovery;
    bool readVerificationEnabledOnRecovery;
    bool expectedReadVerificationEnabled;
    bool expectedReadVerificationEnabledOnRecovery;
    bool expectedCheckpointOnRecovery;

    std::string debugString() const {
      return fmt::format(
          "writeEnabled {}, readVerificationEnabled {}, writeEnabledOnRecovery {}, readVerificationEnabledOnRecovery {}, expectedReadVerificationEnabled {}, expectedReadVerificationEnabledOnRecovery {}, expectedCheckpointOnRecovery {}",
          writeEnabled,
          readVerificationEnabled,
          writeEnabledOnRecovery,
          readVerificationEnabledOnRecovery,
          expectedReadVerificationEnabled,
          expectedReadVerificationEnabledOnRecovery,
          expectedCheckpointOnRecovery);
    }
  } testSettings[] = {
      {false, false, false, false, false, false, true},
      {false, false, false, true, false, false, true},
      {false, false, true, false, false, false, false},
      {false, false, true, true, false, true, false},
      {false, true, false, false, false, false, true},
      {false, true, false, true, false, false, true},
      {false, true, true, false, false, false, false},
      {false, true, true, true, false, true, false},
      {true, false, false, false, false, false, true},
      {true, false, false, true, false, false, true},
      {true, false, true, false, false, false, true},
      {true, false, true, true, false, true, true},
      {true, true, false, false, true, false, true},
      {true, true, false, true, true, false, true},
      {true, true, true, false, true, false, true},
      {true, true, true, true, true, true, true}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    // Initialize cache with checksum write enabled/disabled.
    initializeCache(
        kSsdSize,
        checkpointIntervalBytes,
        testData.writeEnabled,
        testData.readVerificationEnabled);
    EXPECT_EQ(
        ssdFile_->testingChecksumReadVerificationEnabled(),
        testData.expectedReadVerificationEnabled);

    // Populate the cache with some entries.
    std::vector<TestEntry> allEntries;
    for (auto startOffset = 0; startOffset <= kSsdSize - SsdFile::kRegionSize;
         startOffset += SsdFile::kRegionSize) {
      auto pins =
          makePins(fileName_.id(), startOffset, 4096, 2048 * 1025, 62 * kMB);
      ssdFile_->write(pins);
      for (auto& pin : pins) {
        EXPECT_EQ(ssdFile_.get(), pin.entry()->ssdFile());
        allEntries.emplace_back(
            pin.entry()->key(), pin.entry()->ssdOffset(), pin.entry()->size());
      };
    }
    // All entries can be found.
    EXPECT_EQ(checkEntries(allEntries), allEntries.size());

    // Try reinitializing cache from checkpoint with read verification
    // enabled/disabled.
    ssdFile_->checkpoint(true);
    initializeSsdFile(
        kSsdSize,
        checkpointIntervalBytes,
        testData.writeEnabledOnRecovery,
        testData.readVerificationEnabledOnRecovery);
    EXPECT_EQ(
        ssdFile_->testingChecksumReadVerificationEnabled(),
        testData.expectedReadVerificationEnabledOnRecovery);

    // Check if cache data is recoverable as expected.
    if (testData.expectedCheckpointOnRecovery) {
      EXPECT_EQ(checkEntries(allEntries), allEntries.size());
    } else {
      EXPECT_EQ(checkEntries(allEntries), 0);
    }
    cache_->shutdown();
    memory::MemoryManager::testingSetInstance({});
  }
}

TEST_F(SsdFileTest, ssdReadWithoutChecksumCheck) {
  constexpr int64_t kSsdSize = 16 * SsdFile::kRegionSize;

  // Initialize cache with checksum read/write enabled.
  initializeCache(kSsdSize, 0, true, true);

  // Test with one SSD cache entry only.
  auto pins = makePins(fileName_.id(), 0, 4096, 4096, 4096);
  ssdFile_->write(pins);
  ASSERT_EQ(pins.size(), 1);
  pins.back().entry()->setExclusiveToShared();
  auto stats = ssdFile_->testingStats();
  ASSERT_EQ(stats.readWithoutChecksumChecks, 0);

  std::vector<TestEntry> entries;
  for (auto& pin : pins) {
    ASSERT_EQ(ssdFile_.get(), pin.entry()->ssdFile());
    entries.emplace_back(
        pin.entry()->key(), pin.entry()->ssdOffset(), pin.entry()->size());
  };
  std::vector<TestEntry> shortEntries;
  for (auto& pin : pins) {
    ASSERT_EQ(ssdFile_.get(), pin.entry()->ssdFile());
    shortEntries.emplace_back(
        pin.entry()->key(), pin.entry()->ssdOffset(), pin.entry()->size() / 2);
  };

  pins.clear();
  cache_->clear();
  ASSERT_EQ(cache_->refreshStats().numEntries, 0);

  ASSERT_EQ(checkEntries(entries), entries.size());
  ASSERT_EQ(ssdFile_->testingStats().readWithoutChecksumChecks, 0);

  cache_->clear();
  ASSERT_EQ(cache_->refreshStats().numEntries, 0);

#ifndef NDEBUG
  VELOX_ASSERT_THROW(checkEntries(shortEntries), "");
  ASSERT_EQ(ssdFile_->testingStats().readWithoutChecksumChecks, 0);
#else
  ASSERT_EQ(checkEntries(shortEntries), shortEntries.size());
  ASSERT_EQ(ssdFile_->testingStats().readWithoutChecksumChecks, 1);
#endif
}

#ifdef VELOX_SSD_FILE_TEST_SET_NO_COW_FLAG
TEST_F(SsdFileTest, disabledCow) {
  constexpr int64_t kSsdSize = 16 * SsdFile::kRegionSize;
  initializeCache(kSsdSize, 0, false, false, true);
  EXPECT_TRUE(ssdFile_->testingIsCowDisabled());
}

TEST_F(SsdFileTest, notDisabledCow) {
  constexpr int64_t kSsdSize = 16 * SsdFile::kRegionSize;
  initializeCache(kSsdSize, 0, false, false, false);
  EXPECT_FALSE(ssdFile_->testingIsCowDisabled());
}
#endif // VELOX_SSD_FILE_TEST_SET_NO_COW_FLAG
