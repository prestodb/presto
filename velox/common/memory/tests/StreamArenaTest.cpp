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
#include "velox/common/memory/ByteStream.h"
#include "velox/common/memory/MemoryAllocator.h"
#include "velox/common/memory/MmapAllocator.h"

#include <gflags/gflags.h>
#include <gtest/gtest.h>

using namespace facebook::velox;
using namespace facebook::velox::memory;

class StreamArenaTest : public testing::Test {
 protected:
  void SetUp() override {
    constexpr uint64_t kMaxMappedMemory = 64 << 20;
    MemoryManagerOptions options;
    options.allocatorCapacity = kMaxMappedMemory;
    options.useMmapAllocator = true;
    memoryManager_ = std::make_unique<MemoryManager>(options);
    mmapAllocator_ = static_cast<MmapAllocator*>(memoryManager_->allocator());
    pool_ = memoryManager_->addLeafPool("ByteStreamTest");
    rng_.seed(124);
  }

  void TearDown() override {}

  std::unique_ptr<StreamArena> newArena() {
    return std::make_unique<StreamArena>(pool_.get());
  }

  folly::Random::DefaultGenerator rng_;
  std::unique_ptr<MemoryManager> memoryManager_;
  MmapAllocator* mmapAllocator_;
  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_F(StreamArenaTest, newRange) {
  const int largestClassPageSize = 1 << 20;
  struct {
    std::vector<uint64_t> requestRangeSizes;
    std::vector<uint64_t> expectedRangeSizes;
    uint64_t expectedNonContiguousAllocationSize;
    uint64_t expectedContiguousAllocationSize;

    std::string debugString() const {
      return fmt::format(
          "requestRangeSizes[{}]\nexpectedRangeSizes[{}]\nexpectedNonContiguousAllocationSize[{}]\nexpectedContiguousAllocationSize[{}]\n",
          folly::join(",", requestRangeSizes),
          folly::join(",", expectedRangeSizes),
          succinctBytes(expectedNonContiguousAllocationSize),
          succinctBytes(expectedContiguousAllocationSize));
    }
  } testSettings[] = {
      {{largestClassPageSize + 1,
        largestClassPageSize + 1,
        largestClassPageSize + 1,
        2 * largestClassPageSize},
       {largestClassPageSize + AllocationTraits::kPageSize,
        largestClassPageSize + AllocationTraits::kPageSize,
        largestClassPageSize + AllocationTraits::kPageSize,
        2 * largestClassPageSize},
       0,
       largestClassPageSize * 5 + AllocationTraits::kPageSize * 3},
      {{largestClassPageSize - 1,
        1,
        largestClassPageSize + 1,
        largestClassPageSize / 2,
        1,
        AllocationTraits::kPageSize + 1,
        AllocationTraits::kPageSize},
       {largestClassPageSize - 1,
        1,
        largestClassPageSize + AllocationTraits::kPageSize,
        largestClassPageSize / 2,
        1,
        AllocationTraits::kPageSize + 1,
        AllocationTraits::kPageSize - 2},
       largestClassPageSize + largestClassPageSize / 2 +
           AllocationTraits::kPageSize * 2,
       largestClassPageSize + AllocationTraits::kPageSize}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    ASSERT_EQ(
        testData.requestRangeSizes.size(), testData.expectedRangeSizes.size());

    auto arena = newArena();
    ByteRange range;
    for (int i = 0; i < testData.requestRangeSizes.size(); ++i) {
      arena->newRange(testData.requestRangeSizes[i], nullptr, &range);
      ASSERT_EQ(range.size, testData.expectedRangeSizes[i]) << range.toString();
      ASSERT_EQ(range.position, 0);
      ASSERT_TRUE(range.buffer != nullptr);
    }
    ASSERT_EQ(
        testData.expectedContiguousAllocationSize +
            testData.expectedNonContiguousAllocationSize,
        arena->size());
    const int allocateBytes =
        AllocationTraits::pageBytes(mmapAllocator_->numAllocated());
    const int contiguousBytes =
        AllocationTraits::pageBytes(mmapAllocator_->numExternalMapped());
    ASSERT_EQ(
        testData.expectedNonContiguousAllocationSize,
        allocateBytes - contiguousBytes);
    ASSERT_EQ(testData.expectedContiguousAllocationSize, contiguousBytes);
  }
}

TEST_F(StreamArenaTest, randomRange) {
  const int numRanges = 30;
  auto arena = newArena();
  ByteRange range;
  for (int i = 0; i < numRanges; ++i) {
    if (folly::Random::oneIn(4)) {
      const int requestSize =
          1 + folly::Random::rand32() % (2 * AllocationTraits::kPageSize);
      arena->newTinyRange(requestSize, nullptr, &range);
      ASSERT_EQ(range.size, requestSize);
    } else if (folly::Random::oneIn(3)) {
      const int requestSize =
          AllocationTraits::pageBytes(pool_->largestSizeClass()) +
          (folly::Random::rand32() % (4 << 20));
      arena->newRange(requestSize, nullptr, &range);
      ASSERT_EQ(AllocationTraits::roundUpPageBytes(requestSize), range.size);
    } else {
      const int requestSize =
          1 + folly::Random::rand32() % pool_->largestSizeClass();
      arena->newRange(requestSize, nullptr, &range);
      ASSERT_LE(range.size, AllocationTraits::roundUpPageBytes(requestSize));
    }
    ASSERT_EQ(range.position, 0);
    ASSERT_TRUE(range.buffer != nullptr);
  }
}

TEST_F(StreamArenaTest, error) {
  auto arena = newArena();
  ByteRange range;
  VELOX_ASSERT_THROW(
      arena->newTinyRange(0, nullptr, &range),
      "StreamArena::newTinyRange can't be zero length");
  VELOX_ASSERT_THROW(
      arena->newRange(0, nullptr, &range),
      "StreamArena::newRange can't be zero length");
}
