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

#include <folly/Random.h>
#include <folly/container/F14Map.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include "velox/common/caching/FileIds.h"
#include "velox/dwio/dwrf/common/CachedBufferedInput.h"

#include <gtest/gtest.h>

using namespace facebook::velox;
using namespace facebook::dwio;
using namespace facebook::velox::cache;

using facebook::dwio::common::Region;
using memory::MappedMemory;

// Testing stream producing deterministic data. The byte at offset is
// the low byte of 'seed_' + offset.
class TestInputStream : public common::InputStream {
 public:
  TestInputStream(const std::string& path, uint64_t seed, uint64_t length)
      : InputStream(path), seed_(seed), length_(length) {}

  uint64_t getLength() const override {
    return length_;
  }

  uint64_t getNaturalReadSize() const override {
    return 1024 * 1024;
  }

  void read(void* buffer, uint64_t length, uint64_t offset, common::LogType)
      override {
    int fill;
    uint64_t content = offset + seed_;
    uint64_t available = std::min(length_ - offset, length);
    for (fill = 0; fill < (available); ++fill) {
      reinterpret_cast<char*>(buffer)[fill] = content + fill;
    }
  }

  // Asserts that 'bytes' is as would be read from 'offset'.
  void checkData(const void* bytes, uint64_t offset, int32_t size) {
    for (auto i = 0; i < size; ++i) {
      char expected = seed_ + offset + i;
      ASSERT_EQ(expected, reinterpret_cast<const char*>(bytes)[i])
          << " at " << offset + i;
    }
  }

 private:
  const uint64_t seed_;
  const uint64_t length_;
};
class TestInputStreamHolder : public dwrf::AbstractInputStreamHolder {
 public:
  explicit TestInputStreamHolder(std::shared_ptr<common::InputStream> stream)
      : stream_(std::move(stream)) {}

  common::InputStream& get() override {
    return *stream_;
  }

 private:
  std::shared_ptr<common::InputStream> stream_;
};

class CacheTest : public testing::Test {
 protected:
  static constexpr int32_t kMaxStreams = 100;

  // Describes a piece of file potentially read by this test.
  struct StripeData {
    TestInputStream* file;
    std::unique_ptr<dwrf::CachedBufferedInput> input;
    std::vector<std::unique_ptr<dwrf::SeekableInputStream>> streams;
    std::vector<common::Region> regions;
  };

  void SetUp() override {
    executor_ = std::make_unique<folly::IOThreadPoolExecutor>(10, 10);
    rng_.seed(1);
    ioStats_ = std::make_shared<common::IoStatistics>();
  }

  void TearDown() override {
    executor_->join();
  }

  void initializeCache(int64_t maxBytes) {
    cache_ = std::make_unique<AsyncDataCache>(
        MappedMemory::createDefaultInstance(), maxBytes);
    for (auto i = 0; i < kMaxStreams; ++i) {
      streamIds_.push_back(std::make_unique<dwrf::StreamIdentifier>(
          i, i, 0, dwrf::StreamKind_DATA));
    }
    streamStarts_.resize(kMaxStreams + 1);
    streamStarts_[0] = 0;
    int32_t spacing = 100;
    for (auto i = 1; i <= kMaxStreams; ++i) {
      streamStarts_[i] = streamStarts_[i - 1] + spacing * i;
      if (i < kMaxStreams / 3) {
        spacing += 1000;
      } else if (i < kMaxStreams / 3 * 2) {
        spacing += 10000;
      } else {
        spacing += 100000;
      }
    }
  }

  uint64_t seedByPath(const std::string& path) {
    StringIdLease lease(fileIds(), path);
    return lease.id();
  }

  std::shared_ptr<common::InputStream>
  inputByPath(const std::string& path, uint64_t& fileId, uint64_t& groupId) {
    std::lock_guard<std::mutex> l(mutex_);
    StringIdLease lease(fileIds(), path);
    fileId = lease.id();
    // 2 files in a group.
    groupId = fileId / 2;
    auto it = pathToInput_.find(lease.id());
    if (it == pathToInput_.end()) {
      fileIds_.push_back(lease);
      auto stream =
          std::make_shared<TestInputStream>(path, lease.id(), 1UL << 63);
      pathToInput_[lease.id()] = stream;
      return stream;
    }
    return it->second;
  }

  std::unique_ptr<StripeData> makeStripeData(
      std::shared_ptr<common::InputStream> inputStream,
      std::shared_ptr<ScanTracker> tracker,
      uint64_t fileId,
      uint64_t groupId,
      int64_t offset) {
    auto data = std::make_unique<StripeData>();
    common::DataCacheConfig config{nullptr, fileId};
    data->input = std::make_unique<dwrf::CachedBufferedInput>(
        *inputStream,
        *pool_,
        &config,
        cache_.get(),
        tracker,
        groupId,
        [inputStream]() {
          return std::make_unique<TestInputStreamHolder>(inputStream);
        },
        ioStats_,
        executor_.get());
    data->file = dynamic_cast<TestInputStream*>(inputStream.get());
    for (auto i = 0; i < streamStarts_.size() - 1; ++i) {
      // Each region is covers half the space from its start to the
      // start of the next or at max a little under 20MB.
      Region region{
          offset + streamStarts_[i],
          std::min<uint64_t>(
              (1 << 20) - 11, (streamStarts_[i + 1] - streamStarts_[i]) / 2)};
      data->streams.push_back(
          data->input->enqueue(region, streamIds_[i].get()));
      data->regions.push_back(region);
    }
    return data;
  }

  bool shouldRead(int32_t columnIndex, int32_t readPct, int32_t modulo) {
    return folly::Random::rand32(rng_) % 100 <
        readPct / ((columnIndex % modulo) + 1);
  }

  void readStream(const StripeData& stripe, int32_t columnIndex) {
    const void* data;
    int32_t size;
    int64_t numRead = 0;
    auto& stream = *stripe.streams[columnIndex];
    auto region = stripe.regions[columnIndex];
    do {
      stream.Next(&data, &size);
      stripe.file->checkData(data, region.offset + numRead, size);
      numRead += size;
    } while (size > 0);
    EXPECT_EQ(numRead, region.length);
    // Test random access
    std::vector<uint64_t> offsets = {
        0, region.length / 3, region.length * 2 / 3};
    dwrf::PositionProvider positions(offsets);
    for (auto i = 0; i < offsets.size(); ++i) {
      stream.seekToRowGroup(positions);
      checkRandomRead(stripe, stream, offsets, i, region);
    }
  }

  void checkRandomRead(
      const StripeData& stripe,
      dwrf::SeekableInputStream& stream,
      const std::vector<uint64_t>& offsets,
      int32_t i,
      common::Region region) {
    const void* data;
    int32_t size;
    int64_t numRead = 0;
    auto offset = offsets[i];
    // Reads from offset to halfway to the next offset or end.
    auto toRead =
        ((i == offsets.size() - 1 ? region.length : offsets[i + 1]) - offset) /
        2;

    do {
      stream.Next(&data, &size);
      stripe.file->checkData(data, region.offset + offset, size);
      numRead += size;
      offset += size;
      if (size == 0 && numRead) {
        FAIL() << "Stream end prematurely after  random seek";
      }
    } while (numRead < toRead);
  }

  // Makes a series of kReadAhead CachedBufferedInputs for consecutive
  // stripes and starts background load guided by the load frequency
  // in the previous stripes. When at end, destroys a set of
  // CachedBufferedInputs and their streams while they are in a
  // background loading state.
  void readLoop(
      const std::string& filename,
      int numColumns,
      int32_t readPct,
      int32_t readPctModulo,
      int32_t numStripes) {
    auto tracker = std::make_shared<ScanTracker>();
    std::deque<std::unique_ptr<StripeData>> stripes;
    constexpr int32_t kReadAhead = 8;
    uint64_t fileId;
    uint64_t groupId;
    std::shared_ptr<common::InputStream> input =
        inputByPath(filename, fileId, groupId);
    for (auto stripeIndex = 0; stripeIndex < numStripes; ++stripeIndex) {
      stripes.push_back(makeStripeData(
          input,
          tracker,
          fileId,
          groupId,
          stripeIndex * streamStarts_[kMaxStreams - 1]));
      stripes.back()->input->load(common::LogType::TEST);
      if (stripeIndex > 0) {
        while (stripes.size() < kReadAhead) {
          stripes.push_back(makeStripeData(
              input,
              tracker,
              fileId,
              groupId,
              stripeIndex * streamStarts_[kMaxStreams - 1]));
          if (stripes.back()->input->shouldPreload()) {
            stripes.back()->input->load(common::LogType::TEST);
          }
        }
      }
      auto currentStripe = std::move(stripes.front());
      stripes.pop_front();
      currentStripe->input->load(common::LogType::TEST);
      for (auto i = 0; i < numColumns; ++i) {
        int32_t columnIndex = i * (kMaxStreams / numColumns);
        if (shouldRead(columnIndex, readPct, readPctModulo)) {
          readStream(*currentStripe, columnIndex);
        }
      }
    }
  }

  // Serializes 'pathToInput_' and 'fileIds_' in multithread test.
  std::mutex mutex_;
  std::vector<StringIdLease> fileIds_;
  folly::F14FastMap<uint64_t, std::shared_ptr<common::InputStream>>
      pathToInput_;
  common::DataCacheConfig config_;
  std::unique_ptr<AsyncDataCache> cache_;
  std::shared_ptr<common::IoStatistics> ioStats_;
  std::unique_ptr<folly::IOThreadPoolExecutor> executor_;
  std::unique_ptr<memory::MemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};

  // Id of simulated streams. Corresponds 1:1 to 'streamStarts_'.
  std::vector<std::unique_ptr<dwrf::StreamIdentifier>> streamIds_;

  // Start offset of each simulated stream in a simulated stripe.
  std::vector<uint64_t> streamStarts_;
  folly::Random::DefaultGenerator rng_;
};

TEST_F(CacheTest, bufferedInput) {
  // Size 160 MB. Frequent evictions and not everything fits in prefetch window.
  initializeCache(160 << 20);
  readLoop("testfile", 30, 70, 10, 20);
  readLoop("testfile", 30, 70, 10, 20);
  readLoop("testfile2", 30, 70, 70, 20);
}

TEST_F(CacheTest, TestSingleFileThreads) {
  initializeCache(1 << 30);

  const int numThreads = 4;
  std::vector<std::thread> threads;
  threads.reserve(numThreads);
  for (int i = 0; i < numThreads; ++i) {
    threads.push_back(std::thread([this, i]() {
      readLoop(fmt::format("testfile{}", i), 10, 70, 10, 20);
    }));
  }
  for (int i = 0; i < numThreads; ++i) {
    threads[i].join();
  }
}
