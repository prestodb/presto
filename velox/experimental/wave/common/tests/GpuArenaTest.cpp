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

#include "velox/experimental/wave/common/GpuArena.h"
#include <folly/Random.h>
#include <gtest/gtest.h>
#include "velox/common/base/BitUtil.h"

using namespace facebook::velox;
using namespace facebook::velox::wave;

class TestingGpuAllocator : public GpuAllocator {
 public:
  void* allocate(size_t bytes) override {
    return malloc(bytes);
  }

  void free(void* ptr, size_t /*size*/) override {
    ::free(ptr);
  }
};

class GpuArenaTest : public testing::Test {
 public:
  // 32 MB arena space
  static constexpr uint64_t kArenaCapacityBytes = 1l << 25;

 protected:
  void SetUp() override {
    rng_.seed(1);
    allocator_ = std::make_unique<TestingGpuAllocator>();
  }

  void* allocateAndPad(GpuSlab* arena, uint64_t bytes) {
    void* buffer = arena->allocate(bytes);
    memset(buffer, 0xff, bytes);
    return buffer;
  }

  void unpadAndFree(GpuSlab* arena, void* buffer, uint64_t bytes) {
    memset(buffer, 0x00, bytes);
    arena->free(buffer, bytes);
  }

  uint64_t randomPowTwo(uint64_t lowerBound, uint64_t upperBound) {
    lowerBound = bits::nextPowerOfTwo(lowerBound);
    auto attemptedUpperBound = bits::nextPowerOfTwo(upperBound);
    upperBound = attemptedUpperBound == upperBound ? upperBound
                                                   : attemptedUpperBound / 2;
    uint64_t moveSteps;
    if (lowerBound == 0) {
      uint64_t one = 1;
      moveSteps =
          (folly::Random::rand64(
               bits::countLeadingZeros(one) + 1 -
                   bits::countLeadingZeros(upperBound),
               rng_) +
           1);
      return moveSteps == 0 ? 0 : (1l << (moveSteps - 1));
    }
    moveSteps =
        (folly::Random::rand64(
             bits::countLeadingZeros(lowerBound) -
                 bits::countLeadingZeros(upperBound),
             rng_) +
         1);
    return lowerBound << moveSteps;
  }

  folly::Random::DefaultGenerator rng_;
  std::unique_ptr<TestingGpuAllocator> allocator_;
};

TEST_F(GpuArenaTest, slab) {
  // 0 Byte lower bound for revealing edge cases.
  const uint64_t kAllocLowerBound = 0;

  // 1 KB upper bound
  const uint64_t kAllocUpperBound = 1l << 10;
  std::unique_ptr<GpuSlab> arena = std::make_unique<GpuSlab>(
      allocator_->allocate(kArenaCapacityBytes),
      kArenaCapacityBytes,
      allocator_.get());
  memset(arena->address(), 0x00, kArenaCapacityBytes);

  std::unordered_map<uint64_t, uint64_t> allocations;

  // First phase allocate only
  for (size_t i = 0; i < 1000; i++) {
    auto bytes = randomPowTwo(kAllocLowerBound, kAllocUpperBound);
    allocations.emplace(
        reinterpret_cast<uint64_t>(allocateAndPad(arena.get(), bytes)), bytes);
  }
  EXPECT_TRUE(arena->checkConsistency());

  // Second phase alloc and free called in an interleaving way
  for (size_t i = 0; i < 10000; i++) {
    auto bytes = randomPowTwo(kAllocLowerBound, kAllocUpperBound);
    allocations.emplace(
        reinterpret_cast<uint64_t>(allocateAndPad(arena.get(), bytes)), bytes);

    auto itrToFree = allocations.begin();
    auto bytesFree = itrToFree->second;
    unpadAndFree(
        arena.get(), reinterpret_cast<void*>(itrToFree->first), bytesFree);
    allocations.erase(itrToFree);
  }
  EXPECT_TRUE(arena->checkConsistency());

  // Third phase free only
  auto itr = allocations.begin();
  while (itr != allocations.end()) {
    auto bytes = itr->second;
    unpadAndFree(arena.get(), reinterpret_cast<void*>(itr->first), bytes);
    itr++;
  }
  EXPECT_TRUE(arena->checkConsistency());
}

TEST_F(GpuArenaTest, buffers) {
  auto arena = std::make_unique<GpuArena>(1 << 20, allocator_.get());
  std::vector<WaveBufferPtr> buffers;
  for (auto i = 0; i < 5000; ++i) {
    buffers.push_back(arena->allocate<char>(1024));
  }
  EXPECT_EQ(5, arena->slabs().size());
  // We clear some of the first allocated buffers.
  buffers.erase(buffers.begin(), buffers.begin() + 2300);
  EXPECT_EQ(3, arena->slabs().size());
  // Allocate some more. Check that slabs  with unuused capacity get used before
  // making new ones.
  for (auto i = 0; i < 100; ++i) {
    buffers.push_back(arena->allocate<char>(1024));
  }
  EXPECT_EQ(3, arena->slabs().size());
  for (auto i = 0; i < 500; ++i) {
    buffers.push_back(arena->allocate<char>(1024));
  }
  EXPECT_EQ(4, arena->slabs().size());

  // We clear all and expect one arena to be left at the end.
  buffers.clear();
  EXPECT_EQ(1, arena->slabs().size());
}

TEST_F(GpuArenaTest, views) {
  auto arena = std::make_unique<GpuArena>(1 << 20, allocator_.get());
  WaveBufferPtr buffer = arena->allocate<char>(1024);
  EXPECT_EQ(1, buffer->refCount());
  WaveBufferPtr view = WaveBufferView<WaveBufferPtr>::create(
      buffer->as<uint8_t>() + 10, 10, buffer);
  EXPECT_EQ(2, buffer->refCount());
  EXPECT_EQ(1, view->refCount());
  auto view2 = view;
  EXPECT_EQ(2, buffer->refCount());
  EXPECT_EQ(2, view->refCount());
  auto raw = buffer.get();
  buffer = nullptr;
  EXPECT_EQ(1, raw->refCount());
  view = nullptr;
  view2 = nullptr;
  // This is reference to freed but the header is still in the arena.
  EXPECT_EQ(0, raw->refCount());
}
