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

#include "velox/common/memory/RawVector.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/Memory.h"

#include <gtest/gtest.h>

using namespace facebook::velox;

struct TestParam {
  bool useMemoryPool{false};
};

class RawVectorTest : public testing::WithParamInterface<TestParam>,
                      public testing::Test {
 protected:
  void SetUp() override {
    if (GetParam().useMemoryPool) {
      constexpr uint64_t kMaxMappedMemory = 64 << 20;
      memory::MemoryManagerOptions options;
      options.allocatorCapacity = kMaxMappedMemory;
      options.useMmapAllocator = false;
      memoryManager_ = std::make_unique<memory::MemoryManager>(options);
      pool_ = memoryManager_->addLeafPool("common-leaf");
      rng_.seed(124);
    }
  }

  void TearDown() override {}

  folly::Random::DefaultGenerator rng_;
  std::unique_ptr<memory::MemoryManager> memoryManager_;
  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_P(RawVectorTest, basic) {
  raw_vector<int32_t> ints(pool_.get());
  EXPECT_TRUE(ints.empty());
  EXPECT_EQ(0, ints.capacity());
  EXPECT_EQ(0, ints.size());
  ints.reserve(10000);
  EXPECT_LE(10000, ints.capacity());
  EXPECT_TRUE(ints.empty());
  EXPECT_EQ(0, ints.size());
}

TEST_P(RawVectorTest, padding) {
  raw_vector<int32_t> ints(1000, pool_.get());
  EXPECT_EQ(1000, ints.size());
  // Check padding. Write a vector right below start and right after
  // capacity. These should fit and give no error with asan.
  auto v = xsimd::batch<int64_t>::broadcast(-1);
  v.store_unaligned(simd::addBytes(ints.data(), -simd::kPadding));
  v.store_unaligned(
      simd::addBytes(ints.data(), ints.capacity() * sizeof(int32_t)));
}

TEST_P(RawVectorTest, resize) {
  raw_vector<int32_t> ints(1000, pool_.get());
  ints.resize(ints.capacity());
  auto size = ints.size();
  ints[size - 1] = 12345;
  auto oldCapacity = ints.capacity();
  EXPECT_EQ(12345, ints[size - 1]);
  ints.push_back(321);
  EXPECT_EQ(321, ints[size]);
  EXPECT_LE(oldCapacity * 2, ints.capacity());
  ints.clear();
  EXPECT_TRUE(ints.empty());
}

TEST_P(RawVectorTest, copyAndMove) {
  if (!GetParam().useMemoryPool) {
    return;
  }
  auto leaf0 = memoryManager_->addLeafPool("leaf-0");
  auto leaf1 = memoryManager_->addLeafPool("leaf-1");
  struct TestData {
    memory::MemoryPool* sourcePool;
    memory::MemoryPool* destPool;
  };
  std::vector<TestData> testData{
      {nullptr, nullptr},
      {leaf0.get(), leaf0.get()},
      {leaf0.get(), leaf1.get()},
      {leaf0.get(), nullptr},
      {nullptr, leaf0.get()}};
  for (auto& data : testData) {
    raw_vector<int32_t> ints(1000, data.sourcePool);
    // a raw_vector is intentionally not initialized.
    memset(ints.data(), 11, ints.size() * sizeof(int32_t));
    ints[ints.size() - 1] = 12345;
    raw_vector<int32_t> intsCopy(data.destPool);
    intsCopy = ints;
    EXPECT_EQ(
        0, memcmp(ints.data(), intsCopy.data(), ints.size() * sizeof(int32_t)));

    raw_vector<int32_t> intsMoved(data.destPool);
    intsMoved = std::move(ints);
    EXPECT_TRUE(ints.empty());

    EXPECT_EQ(
        0,
        memcmp(
            intsMoved.data(),
            intsCopy.data(),
            intsCopy.size() * sizeof(int32_t)));
  }
}

TEST_P(RawVectorTest, iota) {
  raw_vector<int32_t> storage(pool_.get());
  // Small sizes are preallocated.
  EXPECT_EQ(11, iota(12, storage)[11]);
  EXPECT_TRUE(storage.empty());
  EXPECT_EQ(110000, iota(110001, storage)[110000]);
  // Larger sizes are allocated in 'storage'.
  EXPECT_FALSE(storage.empty());
}

TEST_P(RawVectorTest, iterator) {
  raw_vector<int> data(pool_.get());
  data.push_back(11);
  data.push_back(22);
  data.push_back(33);
  int32_t sum = 0;
  for (auto d : data) {
    sum += d;
  }
  EXPECT_EQ(66, sum);
}

TEST_P(RawVectorTest, toStdVector) {
  raw_vector<int> data(pool_.get());
  data.push_back(11);
  data.push_back(22);
  data.push_back(33);
  std::vector<int32_t> converted = data;
  EXPECT_EQ(3, converted.size());
  for (auto i = 0; i < converted.size(); ++i) {
    EXPECT_EQ(data[i], converted[i]);
    ;
  }
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    RawVectorTest,
    RawVectorTest,
    testing::ValuesIn(std::vector<TestParam>{{false}, {true}}));
