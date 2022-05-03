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

#include "velox/common/base/RawVector.h"

#include <gtest/gtest.h>

using namespace facebook::velox;

TEST(RawVectorTest, basic) {
  raw_vector<int32_t> ints;
  EXPECT_TRUE(ints.empty());
  EXPECT_EQ(0, ints.capacity());
  EXPECT_EQ(0, ints.size());
  ints.reserve(10000);
  EXPECT_LE(10000, ints.capacity());
  EXPECT_TRUE(ints.empty());
  EXPECT_EQ(0, ints.size());
}

TEST(RawVectorTest, padding) {
  raw_vector<int32_t> ints(1000);
  EXPECT_EQ(1000, ints.size());
  // Check padding. Write a vector right below start and right after
  // capacity. These should fit and give no error with asan.
  auto v = xsimd::batch<int64_t>::broadcast(-1);
  v.store_unaligned(simd::addBytes(ints.data(), -simd::kPadding));
  v.store_unaligned(
      simd::addBytes(ints.data(), ints.capacity() * sizeof(int32_t)));
}

TEST(RawVectorTest, resize) {
  raw_vector<int32_t> ints(1000);
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

TEST(RawVectorTest, copyAndMove) {
  raw_vector<int32_t> ints(1000);
  // a raw_vector is intentionally not initialized.
  memset(ints.data(), 11, ints.size() * sizeof(int32_t));
  ints[ints.size() - 1] = 12345;
  raw_vector<int32_t> intsCopy(ints);
  EXPECT_EQ(
      0, memcmp(ints.data(), intsCopy.data(), ints.size() * sizeof(int32_t)));

  raw_vector<int32_t> intsMoved(std::move(ints));
  EXPECT_TRUE(ints.empty());

  EXPECT_EQ(
      0,
      memcmp(
          intsMoved.data(),
          intsCopy.data(),
          intsCopy.size() * sizeof(int32_t)));
}

TEST(RawVectorTest, iota) {
  raw_vector<int32_t> storage;
  // Small sizes are preallocated.
  EXPECT_EQ(11, iota(12, storage)[11]);
  EXPECT_TRUE(storage.empty());
  EXPECT_EQ(110000, iota(110001, storage)[110000]);
  // Larger sizes are allocated in 'storage'.
  EXPECT_FALSE(storage.empty());
}

TEST(RawVectorTest, iterator) {
  raw_vector<int> data;
  data.push_back(11);
  data.push_back(22);
  data.push_back(33);
  int32_t sum = 0;
  for (auto d : data) {
    sum += d;
  }
  EXPECT_EQ(66, sum);
}

TEST(RawVectorTest, toStdVector) {
  raw_vector<int> data;
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
