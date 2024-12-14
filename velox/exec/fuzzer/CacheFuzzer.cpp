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

#include "velox/exec/fuzzer/CacheFuzzer.h"

#include <boost/random/uniform_int_distribution.hpp>

#include <folly/executors/IOThreadPoolExecutor.h>
#include <gtest/gtest.h>
#include "velox/common/caching/FileIds.h"
#include "velox/common/caching/SsdCache.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/dwio/common/CachedBufferedInput.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/fuzzer/Utils.h"

DEFINE_int32(steps, 10, "Number of plans to generate and test.");

DEFINE_int32(
    duration_sec,
    0,
    "For how long it should run (in seconds). If zero, "
    "it executes exactly --steps iterations and exits.");

DEFINE_int32(num_threads, 16, "Number of read threads.");

DEFINE_int32(
    read_iteration_sec,
    10,
    "For how long each read thread should run (in seconds).");

DEFINE_int32(num_source_files, 8, "Number of data files to be created.");

DEFINE_int64(
    source_file_bytes,
    -1,
    "Source file size in bytes. When set to -1, a random value from 32MB to 48MB will be used, inclusively.");

DEFINE_int64(
    memory_cache_bytes,
    -1,
    "Memory cache size in bytes. When set to -1, a random value from 48 to 64MB will be used, inclusively.");

DEFINE_int64(
    ssd_cache_bytes,
    -1,
    "SSD cache size in bytes. When set to -1, 1 out of 10 times SSD cache will be disabled, "
    "while the other times, a random value from 128MB to 256MB will be used, inclusively.");

DEFINE_int32(
    num_ssd_cache_shards,
    -1,
    "Number of SSD cache shards. When set to -1, a random value from 1 to 4 will be used, inclusively.");

DEFINE_int64(
    ssd_checkpoint_interval_bytes,
    -1,
    "Checkpoint after every 'ssd_checkpoint_interval_bytes'/'num_ssd_cache_shards', written into "
    "each file. 0 means no checkpointing. When set to -1, 1 out of 4 times checkpoint will be disabled, "
    "while the other times, a random value from 32MB to 64MB will be used, inclusively.");

using namespace facebook::velox::cache;
using namespace facebook::velox::dwio::common;

namespace facebook::velox::exec::test {
namespace {

class CacheFuzzer {
 public:
  explicit CacheFuzzer(size_t seed);

  void go();

 private:
  static constexpr int32_t kRandomized = -1;

  void seed(size_t seed) {
    currentSeed_ = seed;
    rng_.seed(currentSeed_);
  }

  void reSeed() {
    seed(rng_());
  }

  int32_t getSourceFileBytes() {
    if (FLAGS_source_file_bytes == kRandomized) {
      return boost::random::uniform_int_distribution<int64_t>(
          32 << 20 /*32MB*/, 48 << 20 /*48MB*/)(rng_);
    }
    return FLAGS_source_file_bytes;
  }

  int64_t getMemoryCacheBytes() {
    if (FLAGS_memory_cache_bytes == kRandomized) {
      return boost::random::uniform_int_distribution<int64_t>(
          48 << 20 /*48MB*/, 64 << 20 /*64MB*/)(rng_);
    }
    return FLAGS_memory_cache_bytes;
  }

  int32_t getSsdCacheBytes() {
    if (FLAGS_ssd_cache_bytes == kRandomized) {
      // Enable SSD cache 90% of the time.
      return folly::Random::oneIn(10, rng_)
          ? 0
          : boost::random::uniform_int_distribution<int64_t>(
                128 << 20 /*128MB*/, 256 << 20 /*256MB*/)(rng_);
    }
    return FLAGS_ssd_cache_bytes;
  }

  int32_t getSsdCacheShards() {
    if (FLAGS_num_ssd_cache_shards == kRandomized) {
      // Use 1-4 shards to test different cases. The number of shards shouldn't
      // be too larger so that each shard has enough space to hold large cache
      // entries.
      return boost::random::uniform_int_distribution<int32_t>(1, 4)(rng_);
    }
    return FLAGS_num_ssd_cache_shards;
  }

  int32_t getSsdCheckpointIntervalBytes() {
    if (FLAGS_ssd_checkpoint_interval_bytes == kRandomized) {
      // Enable checkpoint 75% of the time as checksum depends on it.
      return folly::Random::oneIn(4, rng_)
          ? 0
          : boost::random::uniform_int_distribution<uint64_t>(
                32 << 20 /*32MB*/, 64 << 20 /*64MB*/)(rng_);
    }
    return FLAGS_ssd_checkpoint_interval_bytes;
  }

  void initSourceDataFiles();

  void initializeCache();

  void initializeInputs();

  void readCache();

  void reset();

  void read(uint32_t fileIdx, int32_t fragmentIdx);

  FuzzerGenerator rng_;
  size_t currentSeed_{0};
  std::shared_ptr<memory::MemoryPool> rootPool_{
      memory::memoryManager()->addRootPool()};
  std::shared_ptr<memory::MemoryPool> pool_{rootPool_->addLeafChild("leaf")};
  std::shared_ptr<filesystems::FileSystem> fs_;
  std::vector<std::string> fileNames_;
  std::vector<StringIdLease> fileIds_;
  std::vector<size_t> fileSizes_;
  // The file fragments used to perform random reads by different threads.
  // NOTE: the production file reader reads from the specific offset from a file
  // instead of random location for cache reuse.
  std::vector<std::vector<std::pair<int32_t, int32_t>>> fileFragments_;
  std::vector<std::unique_ptr<CachedBufferedInput>> inputs_;
  std::shared_ptr<exec::test::TempDirectoryPath> sourceDataDir_;
  std::unique_ptr<memory::MemoryManager> memoryManager_;
  std::unique_ptr<folly::IOThreadPoolExecutor> executor_;
  std::shared_ptr<AsyncDataCache> cache_;
};

template <typename T>
bool isDone(size_t i, T startTime) {
  if (FLAGS_duration_sec > 0) {
    const std::chrono::duration<double> elapsed =
        std::chrono::system_clock::now() - startTime;
    return elapsed.count() >= FLAGS_duration_sec;
  }
  return i >= FLAGS_steps;
}

CacheFuzzer::CacheFuzzer(size_t initialSeed) {
  seed(initialSeed);
  filesystems::registerLocalFileSystem();
}

void CacheFuzzer::initSourceDataFiles() {
  sourceDataDir_ = exec::test::TempDirectoryPath::create();
  fs_ = filesystems::getFileSystem(sourceDataDir_->getPath(), nullptr);

  // Create files with random sizes.
  if (fileIds_.empty()) {
    for (auto i = 0; i < FLAGS_num_source_files; ++i) {
      const auto fileName =
          fmt::format("{}/file_{}", sourceDataDir_->getPath(), i);
      const size_t fileSize = getSourceFileBytes();
      auto writeFile = fs_->openFileForWrite(fileName);
      size_t writtenSize = 0;
      int32_t offset = 0;
      while (writtenSize < fileSize) {
        const size_t chunkSize = std::min(
            fileSize - writtenSize,
            size_t(4 << 20)); // Write in chunks of 4MB
        auto buffer = folly::IOBuf::create(chunkSize);
        buffer->append(chunkSize);
        // Fill buffer with data.
        std::generate_n(
            buffer->writableData(), chunkSize, [&offset]() -> uint8_t {
              return static_cast<uint8_t>(offset++ % 256);
            });
        writeFile->append(std::move(buffer));
        writtenSize += chunkSize;
      }
      writeFile->close();

      fileNames_.emplace_back(fileName);
      fileIds_.emplace_back(fileIds(), fileName);
      fileSizes_.emplace_back(fileSize);
    }
  }
}

void CacheFuzzer::initializeCache() {
  // We have up to 20 threads and 16 threads are used for reading so
  // there are some threads left over for SSD background write.
  executor_ = std::make_unique<folly::IOThreadPoolExecutor>(20);
  const auto memoryCacheBytes = getMemoryCacheBytes();
  const auto ssdCacheBytes = getSsdCacheBytes();

  std::unique_ptr<SsdCache> ssdCache;
  if (ssdCacheBytes > 0) {
    const auto numSsdCacheShards = getSsdCacheShards();
    const auto checkpointIntervalBytes = getSsdCheckpointIntervalBytes();
    const auto enableChecksum = folly::Random::oneIn(2, rng_);
    const auto enableChecksumReadVerification = folly::Random::oneIn(2, rng_);

    SsdCache::Config config(
        fmt::format("{}/cache", sourceDataDir_->getPath()),
        ssdCacheBytes,
        numSsdCacheShards,
        executor_.get(),
        checkpointIntervalBytes,
        false,
        enableChecksum,
        enableChecksumReadVerification);
    ssdCache = std::make_unique<SsdCache>(config);
    LOG(INFO) << fmt::format(
        "Initialized SSD cache with {} shards, {}, with checkpoint {}, checksum write {}, read verification {}",
        succinctBytes(ssdCacheBytes),
        numSsdCacheShards,
        checkpointIntervalBytes > 0
            ? fmt::format("enabled({})", succinctBytes(checkpointIntervalBytes))
            : "disabled",
        enableChecksum ? "enabled" : "disabled",
        enableChecksumReadVerification ? "enabled" : "disabled");
  }

  memory::MemoryManagerOptions options;
  options.useMmapAllocator = true;
  options.allocatorCapacity = memoryCacheBytes;
  options.arbitratorCapacity = memoryCacheBytes;
  options.trackDefaultUsage = true;
  memoryManager_ = std::make_unique<memory::MemoryManager>(options);
  cache_ = AsyncDataCache::create(
      dynamic_cast<memory::MmapAllocator*>(memoryManager_->allocator()),
      std::move(ssdCache),
      {});

  LOG(INFO) << fmt::format(
      "Initialized cache with {} memory space, {} SSD cache",
      succinctBytes(memoryCacheBytes),
      ssdCacheBytes == 0 ? "with" : "without");
}

void CacheFuzzer::initializeInputs() {
  const auto readOptions = io::ReaderOptions(pool_.get());
  auto tracker = std::make_shared<ScanTracker>(
      "testTracker", nullptr, 256 << 10 /*256KB*/);
  auto ioStats = std::make_shared<IoStatistics>();
  inputs_.reserve(FLAGS_num_source_files);
  for (auto i = 0; i < FLAGS_num_source_files; ++i) {
    // Initialize buffered input.
    auto readFile = fs_->openFileForRead(fileNames_[i]);
    auto const withExecutor = !folly::Random::oneIn(3, rng_);
    inputs_.emplace_back(std::make_unique<CachedBufferedInput>(
        std::move(readFile),
        MetricsLog::voidLog(),
        fileIds_[i].id(), // NOLINT
        cache_.get(),
        tracker,
        fileIds_[i].id(), // NOLINT
        ioStats,
        withExecutor ? executor_.get() : nullptr,
        readOptions));

    // Divide file into fragments.
    std::vector<std::pair<int32_t, int32_t>> fragments;
    int32_t offset = 0;
    while (offset < fileSizes_[i]) {
      // Limit the fragment size to 4MB to avoid too large cache entries.
      const auto length = std::min(
          boost::random::uniform_int_distribution<int32_t>(
              1, fileSizes_[i] - offset)(rng_),
          4 << 20 /*4MB*/);
      fragments.emplace_back(offset, length);
      offset += length;
    }
    fileFragments_.emplace_back(std::move(fragments));
  }
}

void CacheFuzzer::readCache() {
  std::atomic_bool readStopped{false};
  std::vector<std::thread> threads;
  threads.reserve(FLAGS_num_threads);
  for (int32_t i = 0; i < FLAGS_num_threads; ++i) {
    threads.emplace_back([&, i]() {
      FuzzerGenerator rng(currentSeed_ + i);
      while (!readStopped) {
        const auto fileIdx = boost::random::uniform_int_distribution<int32_t>(
            0, FLAGS_num_source_files - 1)(rng);
        const auto fragmentIdx =
            boost::random::uniform_int_distribution<int32_t>(
                0, fileFragments_[fileIdx].size() - 1)(rng);
        read(fileIdx, fragmentIdx);
      }
    });
  }
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_read_iteration_sec));
  readStopped = true;
  for (auto& thread : threads) {
    thread.join();
  }
}

void CacheFuzzer::reset() {
  cache_->shutdown();
  if (cache_->ssdCache() != nullptr) {
    cache_->ssdCache()->waitForWriteToFinish();
  }
  executor_->join();
  executor_.reset();
  fileNames_.clear();
  fileIds_.clear();
  fileSizes_.clear();
  fileFragments_.clear();
  inputs_.clear();
  fs_.reset();
  cache_.reset();
  memoryManager_.reset();
  sourceDataDir_.reset();
  fileIds().testingReset();
}

void CacheFuzzer::read(uint32_t fileIdx, int32_t fragmentIdx) {
  // TODO: Faulty injection.
  const auto [offset, length] = fileFragments_[fileIdx][fragmentIdx];
  auto stream = inputs_[fileIdx]->read(offset, length, LogType::TEST);
  const void* buffer;
  int32_t size;
  int32_t numRead = 0;
  while (numRead < length) {
    try {
      if (stream->Next(&buffer, &size)) {
        if (folly::Random::oneIn(4)) {
          // Verify read content.
          const auto* data = reinterpret_cast<const uint8_t*>(buffer);
          for (int32_t sequence = 0; sequence < size; ++sequence) {
            ASSERT_EQ(data[sequence], (offset + numRead + sequence) % 256);
          }
        }
        numRead += size;
      } else {
        break;
      }
    } catch (const VeloxException& e) {
      if (e.errorCode() == error_code::kNoCacheSpace.c_str()) {
        LOG(WARNING) << e.what();
      } else {
        std::rethrow_exception(std::current_exception());
      }
    }
  }
  ASSERT_EQ(numRead, length);
}

void CacheFuzzer::go() {
  VELOX_CHECK(
      FLAGS_steps > 0 || FLAGS_duration_sec > 0,
      "Either --steps or --duration_sec needs to be greater than zero.");

  auto startTime = std::chrono::system_clock::now();
  size_t iteration = 0;

  while (!isDone(iteration, startTime)) {
    LOG(INFO) << "==============================> Started iteration "
              << iteration << " (seed: " << currentSeed_ << ")";

    initSourceDataFiles();

    initializeCache();

    initializeInputs();

    readCache();

    // TODO: Test cache restart.

    LOG(INFO) << cache_->refreshStats().toString();

    reset();

    LOG(INFO) << "==============================> Done with iteration "
              << iteration;

    reSeed();
    ++iteration;
  }
}

} // namespace

void cacheFuzzer(size_t seed) {
  auto cacheFuzzer = CacheFuzzer(seed);
  cacheFuzzer.go();
}
} // namespace facebook::velox::exec::test
