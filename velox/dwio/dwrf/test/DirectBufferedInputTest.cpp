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

#include "velox/dwio/common/DirectBufferedInput.h"
#include <folly/Random.h>
#include <folly/container/F14Map.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include "velox/common/io/IoStatistics.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/dwrf/common/Common.h"
#include "velox/dwio/dwrf/test/TestReadFile.h"

#include <gtest/gtest.h>

using namespace facebook::velox;
using namespace facebook::velox::dwio;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::cache;

using facebook::velox::common::Region;

using memory::MemoryAllocator;
using IoStatisticsPtr = std::shared_ptr<IoStatistics>;

struct TestRegion {
  int32_t offset;
  int32_t length;
};

class DirectBufferedInputTest : public testing::Test {
 protected:
  static constexpr int32_t kLoadQuantum = 8 << 20;

  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    executor_ = std::make_unique<folly::IOThreadPoolExecutor>(10, 10);
    ioStats_ = std::make_shared<IoStatistics>();
    fileIoStats_ = std::make_shared<IoStatistics>();
    tracker_ = std::make_shared<cache::ScanTracker>("", nullptr, kLoadQuantum);
    file_ = std::make_shared<TestReadFile>(11, 100 << 20, fileIoStats_);
    opts_ = std::make_unique<dwio::common::ReaderOptions>(pool_.get());
    opts_->setLoadQuantum(kLoadQuantum);
  }

  void TearDown() override {
    executor_->join();
  }

  std::unique_ptr<DirectBufferedInput> makeInput() {
    return std::make_unique<DirectBufferedInput>(
        file_,
        dwio::common::MetricsLog::voidLog(),
        1,
        tracker_,
        2,
        ioStats_,
        executor_.get(),
        *opts_);
  }

  // Reads and checks the result of reading ''regions' and checks that this
  // causes 'numIos' accesses to the file.
  void testLoads(std::vector<TestRegion> regions, int32_t numIos) {
    auto previous = file_->numIos();
    auto input = makeInput();
    std::vector<std::unique_ptr<SeekableInputStream>> streams;
    for (auto i = 0; i < regions.size(); ++i) {
      if (regions[i].length > 0) {
        Region region;
        region.offset = regions[i].offset;
        region.length = regions[i].length;
        StreamIdentifier si(i);
        streams.push_back(input->enqueue(region, &si));
      }
    }
    input->load(LogType::FILE);
    for (auto i = 0; i < regions.size(); ++i) {
      if (regions[i].length > 0) {
        checkRead(streams[i].get(), regions[i]);
      }
    }
    EXPECT_EQ(numIos, file_->numIos() - previous);
  }

  // Marks the numStreams first streams as densely read. A large number of
  // references that all end in a read.
  void makeDense(int32_t numStreams) {
    for (auto i = 0; i < numStreams; ++i) {
      StreamIdentifier si(i);
      auto trackId = TrackingId(si.getId());
      for (auto counter = 0; counter < 100; ++counter) {
        tracker_->recordReference(trackId, 1000000, 1, 1);
        tracker_->recordRead(trackId, 1000000, 1, 1);
      }
    }
  }

  void checkRead(SeekableInputStream* stream, TestRegion region) {
    int32_t size;
    int32_t totalRead = 0;
    const void* buffer;
    while (stream->Next(&buffer, &size)) {
      file_->checkData(buffer, region.offset + totalRead, size);
      totalRead += size;
    }
    EXPECT_EQ(region.length, totalRead);
  }

  std::unique_ptr<dwio::common::ReaderOptions> opts_;
  std::shared_ptr<TestReadFile> file_;
  std::shared_ptr<cache::ScanTracker> tracker_;
  std::shared_ptr<IoStatistics> ioStats_;
  std::shared_ptr<IoStatistics> fileIoStats_;
  std::unique_ptr<folly::IOThreadPoolExecutor> executor_;
  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
};

TEST_F(DirectBufferedInputTest, basic) {
  // The small leading parts coalesce with the the 7M.  The 2M goes standalone.
  // the last is read in 2 parts. This is because these are not yet densely
  // accessed and thus coalescing only works to load quantum of 8MB.
  testLoads(
      {{100, 100},
       {300, 100},
       {1000, 7000000},
       {7004000, 2000000},
       {20000000, 10000000}},
      4);

  // All but the last coalesce into one , the last is read in 2 parts. The
  // columns are now dense and coalesce goes up to 128MB if gaps are small
  // enough.
  testLoads(
      {{100, 100},
       {300, 100},
       {1000, 7000000},
       {7004000, 2000000},
       {20000000, 10000000}},
      3);

  // Mark the first 4 ranges as densely accessed.
  makeDense(4);

  // The first and first part of second coalesce.
  testLoads({{100, 100}, {1000, 10000000}}, 2);

  // The first is read in two parts, the tail of the first does not coalesce
  // with the second.
  testLoads({{1000, 10000000}, {10001000, 1000}}, 3);

  // One large standalone read in 2 parts.
  testLoads({{1000, 10000000}}, 2);

  // Small standalone read in 1 part.
  testLoads({{100, 100}}, 1);

  // Two small far apart
  testLoads({{100, 100}, {1000000, 100}}, 2);
  // The two coalesce because the first fits within load quantum + max coalesce
  // distance.
  testLoads({{1000, 8500000}, {8510000, 1000000}}, 1);

  // The two coalesce because the first fits within load quantum + max coalesce
  // distance. The tail of the second does not coalesce.
  testLoads({{1000, 8500000}, {8510000, 8400000}}, 2);

  // The first reads in 2 parts and does not coalesce to the second, which reads
  // in one part.
  testLoads({{1000, 9000000}, {9010000, 1000000}}, 3);
}
