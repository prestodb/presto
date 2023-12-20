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

#include <folly/container/F14Set.h>
#include <folly/hash/Hash.h>
#include "velox/common/base/BigintIdMap.h"
#include "velox/common/base/SelectivityInfo.h"

#include <gtest/gtest.h>

using namespace facebook::velox;

struct IdMapHasher {
  size_t operator()(const std::pair<int64_t, int32_t>& item) const {
    return folly::hasher<int64_t>()(item.first);
  }
};

struct IdMapComparer {
  bool operator()(
      const std::pair<int64_t, int32_t>& left,
      const std::pair<int64_t, int32_t>& right) const {
    return left.first == right.first;
  }
};

class F14IdMap {
 public:
  F14IdMap(int32_t initial) : set_(initial) {}

  int32_t id(int64_t value) {
    std::pair<int64_t, int32_t> item(
        value, static_cast<int32_t>(set_.size() + 1));
    return set_.insert(item).first->second;
  }

  int64_t findId(int64_t value) {
    std::pair<int64_t, int32_t> item(
        value, static_cast<int32_t>(set_.size() + 1));
    auto it = set_.find(item);
    if (it == set_.end()) {
      return BigintIdMap::kNotFound;
    }
    return it->second;
  }

 private:
  folly::F14FastSet<std::pair<int64_t, int32_t>, IdMapHasher, IdMapComparer>
      set_;
};

class IdMapTest : public testing::Test {
 protected:
  static constexpr int32_t kBatchSize =
      xsimd::batch<int64_t, xsimd::default_arch>::size;

  using int64x4 = int64_t[4];

  struct int64x4s {
    int64_t n1;
    int64_t n2;
    int64_t n3;
    int64_t n4;
  };

  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    root_ = memory::memoryManager()->addRootPool("IdMapRoot");
    pool_ = root_->addLeafChild("IdMapLeakLeaf");
  }

  void testCase(int64_t size, int64_t range) {
    std::vector<int64_t> data;
    testData(size, range, data);
    auto result = test(data);
    std::cout << fmt::format(
                     "Size={} range={} clocks IdMap={} F14={} ({}%)",
                     size,
                     range,
                     result.first,
                     result.second,
                     100 * result.second / result.first)
              << std::endl;
  }

  void testData(int64_t size, int64_t range, std::vector<int64_t>& data) {
    size = bits::roundUp(size, kBatchSize);
    data.reserve(size);
    for (auto i = 0; i < size; ++i) {
      data.push_back(1 + (i % range));
    }
  }

  // Feeds 'data' into a BigintIdMap and the F14IdMap reference implementation
  // and checks that the outcome is the same. returns the total clocks for
  // BigIntIdMap and F14IdMap.
  std::pair<float, float> test(const std::vector<int64_t>& data) {
    BigintIdMap map(1024, *pool_);
    F14IdMap f14(1024);
    SelectivityInfo mapInfo;
    SelectivityInfo f14Info;
    {
      SelectivityTimer t(mapInfo, data.size());
      for (auto i = 0; i + kBatchSize <= data.size(); i += kBatchSize) {
        map.makeIds(xsimd::batch<int64_t>::load_unaligned(data.data() + i));
      }
    }
    {
      SelectivityTimer t(f14Info, data.size());
      for (auto i = 0; i < data.size(); ++i) {
        f14.id(data[i]);
      }
    }
    for (auto i = 0; i + kBatchSize <= data.size(); i += kBatchSize) {
      auto ids =
          map.findIds(xsimd::batch<int64_t>::load_unaligned(data.data() + i));
      auto idsArray = reinterpret_cast<int64_t*>(&ids);
      for (auto j = 0; j < kBatchSize; ++j) {
        auto reference = f14.findId(data[i + j]);
        EXPECT_EQ(reference, idsArray[j]);
        if (reference != idsArray[j]) {
          break;
        }
      }
    }
    return std::make_pair<float, float>(
        mapInfo.timeToDropValue(), f14Info.timeToDropValue());
  }

  void expect4(int64_t n1, int64_t n2, int64_t n3, int64_t n4, int64x4s data) {
    EXPECT_EQ(n1, data.n1);
    EXPECT_EQ(n2, data.n2);
    EXPECT_EQ(n3, data.n3);
    EXPECT_EQ(n4, data.n4);
  }

  // A test function with exactly 4 lanes. Does 2x2 lanes, for lanes or 4 lanes
  // twice depending on the actual width.
  int64x4s makeIds4(BigintIdMap& map, int64x4 values, int16_t mask = 15) {
    int64x4s result;
    if constexpr (kBatchSize == 2) {
      auto r1 = map.makeIds(xsimd::load_unaligned(values), mask & 3);
      auto r2 = map.makeIds(xsimd::load_unaligned(&values[0] + 2), mask >> 2);
      r1.store_unaligned(&result.n1);
      r2.store_unaligned(&result.n3);
    } else if constexpr (kBatchSize == 4) {
      auto r1 = map.makeIds(xsimd::load_unaligned(values), mask);
      memcpy(&result.n1, &r1, sizeof(result));
    } else if constexpr (kBatchSize == 8) {
      int64_t values8[8];
      memcpy(values8, values, sizeof(result));
      memcpy(&values8[4], values, sizeof(result));
      auto r8 = map.makeIds(xsimd::load_unaligned(values8), mask | (mask << 4));
      EXPECT_EQ(
          0,
          memcmp(&r8, reinterpret_cast<int64_t*>(&r8) + 4, 4 * sizeof(int64_t)))
          << "The 4 first and last lanes of an 8 wide operation must match";
      memcpy(&result.n1, values8, sizeof(result));
    }
    return result;
  }

  int64x4s findIds4(BigintIdMap& map, int64x4 values, int16_t mask = 15) {
    int64x4s result;
    if constexpr (kBatchSize == 2) {
      auto r1 = map.findIds(xsimd::load_unaligned(values), mask & 3);
      auto r2 = map.findIds(xsimd::load_unaligned(&values[0] + 2), mask >> 2);
      r1.store_unaligned(&result.n1);
      r2.store_unaligned(&result.n3);
    } else if constexpr (kBatchSize == 4) {
      auto r1 = map.findIds(xsimd::load_unaligned(values), mask);
      memcpy(&result.n1, &r1, sizeof(result));
    } else if constexpr (kBatchSize == 8) {
      int64_t values8[8];
      memcpy(values8, values, sizeof(result));
      memcpy(&values8[4], values, sizeof(result));
      auto r8 = map.findIds(xsimd::load_unaligned(values8), mask | (mask << 4));
      EXPECT_EQ(
          0,
          memcmp(&r8, reinterpret_cast<int64_t*>(&r8) + 4, 4 * sizeof(int64_t)))
          << "The 4 first and last lanes of an 8 wide operation must match";
      memcpy(&result.n1, values8, sizeof(result));
    }

    return result;
  }

  std::shared_ptr<memory::MemoryPool> root_;
  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_F(IdMapTest, basic) {
  testCase(1000, 3);
  testCase(1000, 1000);
  testCase(10000, 2500);
  testCase(1000000, 1000000);
  testCase(5000000, 1000000);
}

TEST_F(IdMapTest, zerosAndMasks) {
  constexpr int64_t kNotFound = BigintIdMap::kNotFound;

  BigintIdMap map(1024, *pool_);
  int64_t zeros[4] = {0, 0, 0, 0};
  int64_t oneZero[4] = {1, 0, 2, 3};

  // All lanes disabled makes all 0.
  expect4(0, 0, 0, 0, makeIds4(map, oneZero, 0));

  // Last lane is on, gets first id 1.
  expect4(0, 0, 0, 1, makeIds4(map, oneZero, 8));

  // All lanes are on, the zero gets the next id (2) and the non-zeros get 3
  // and 4.
  expect4(3, 2, 4, 1, makeIds4(map, oneZero));
  expect4(3, 2, 4, 1, findIds4(map, oneZero));

  // All zeros gets 2 (id of 0)  for the active lanes and 0 for inactive.
  expect4(2, 0, 2, 0, makeIds4(map, zeros, 5));

  expect4(2, 2, 2, 2, findIds4(map, zeros));

  BigintIdMap mapWithNoZero(1024, *pool_);
  // We insert the same values and mask out the 0.
  expect4(1, 0, 2, 3, makeIds4(mapWithNoZero, oneZero, 13));
  expect4(
      kNotFound,
      kNotFound,
      kNotFound,
      kNotFound,
      findIds4(mapWithNoZero, zeros));

  // Zero for inactive, not found for active.
  expect4(kNotFound, 0, kNotFound, 0, findIds4(mapWithNoZero, zeros, 5));

  int64_t mix[4] = {10, 1, 0, 2};
  expect4(kNotFound, 1, kNotFound, 2, findIds4(mapWithNoZero, mix));

  expect4(kNotFound, 0, kNotFound, 0, findIds4(mapWithNoZero, mix, 5));
}

TEST_F(IdMapTest, collisions) {
  constexpr int64_t kNotFound = BigintIdMap::kNotFound;
  // We check the found and not found stay the same as the table gets filled
  F14IdMap reference(32);
  BigintIdMap map(8, *pool_);
  std::vector<int64_t> data;
  for (auto i = 0; i < 2048; ++i) {
    data.push_back((i + 1) * 0xfeedda7a58ff1e00);
  }
  // Add an empty marker.
  data[1333] = 0;
  for (auto fill = 0; fill < data.size(); fill += kBatchSize) {
    // Check that data not inserted is not found.
    for (auto i = fill; i < data.size(); i += kBatchSize) {
      auto expectedEmpty = map.findIds(xsimd::load_unaligned(data.data() + i));
      for (auto j = 0; j < kBatchSize; ++j) {
        EXPECT_EQ(kNotFound, reinterpret_cast<int64_t*>(&expectedEmpty)[j]);
        EXPECT_EQ(kNotFound, reference.findId(data[i + j]));
      }
    }

    // Add a group of 4 new entries.
    auto ids = map.makeIds(xsimd::load_unaligned(data.data() + fill));
    for (auto j = 0; j < kBatchSize; ++j) {
      // If there is a zero added, add it to 'reference' before the other values
      // to match the special treatment of empty marker.
      if (data[fill + j] == 0) {
        reference.id(0);
      }
    }
    for (auto j = 0; j < kBatchSize; ++j) {
      EXPECT_EQ(
          reference.id(data[fill + j]), reinterpret_cast<int64_t*>(&ids)[j]);
    }

    // Check that all inserted is still found.
    for (auto i = 0; i <= fill; i += kBatchSize) {
      auto ids = map.findIds(xsimd::load_unaligned(data.data() + i));
      for (auto j = 0; j < kBatchSize; ++j) {
        EXPECT_EQ(
            reference.findId(data[i + j]), reinterpret_cast<int64_t*>(&ids)[j]);
      }
    }
  }
}
