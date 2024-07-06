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

#include "velox/common/caching/StringIdMap.h"

#include "gtest/gtest.h"
#include "velox/common/base/tests/GTestUtils.h"

using namespace facebook::velox;

TEST(StringIdMapTest, basic) {
  constexpr const char* kFile1 = "file_1";
  StringIdMap map;
  uint64_t id = 0;
  {
    StringIdLease lease1;
    EXPECT_FALSE(lease1.hasValue());
    StringIdLease lease2(map, kFile1);
    EXPECT_TRUE(lease2.hasValue());
    id = lease2.id();
    lease1 = lease2;
    EXPECT_EQ(id, lease1.id());
    EXPECT_EQ(strlen(kFile1), map.pinnedSize());
  }
  StringIdLease lease3(map, kFile1);
  EXPECT_NE(lease3.id(), id);
  lease3.clear();
  EXPECT_EQ(0, map.pinnedSize());
}

TEST(StringIdMapTest, rehash) {
  constexpr int32_t kCount = 10000;
  StringIdMap map;
  std::vector<StringIdLease> ids;
  for (auto i = 0; i < kCount; ++i) {
    auto name = fmt::format("filename_{}", i);
    ids.push_back(StringIdLease(map, name));
  }
  for (auto i = 0; i < kCount; ++i) {
    auto name = fmt::format("filename_{}", i);
    EXPECT_EQ(ids[i].id(), StringIdLease(map, name).id());
  }
}

TEST(StringIdMapTest, recover) {
  constexpr const char* kRecoverFile1 = "file_1";
  constexpr const char* kRecoverFile2 = "file_2";
  constexpr const char* kRecoverFile3 = "file_3";
  StringIdMap map;
  const uint64_t recoverId1{10};
  const uint64_t recoverId2{20};
  {
    StringIdLease lease(map, recoverId1, kRecoverFile1);
    ASSERT_TRUE(lease.hasValue());
    ASSERT_EQ(map.pinnedSize(), ::strlen(kRecoverFile1));
    ASSERT_EQ(map.testingLastId(), recoverId1);
    VELOX_ASSERT_THROW(
        std::make_unique<StringIdLease>(map, recoverId1, kRecoverFile2),
        "(1 vs. 0) Reused recover id 10 assigned to file_2");
    VELOX_ASSERT_THROW(
        std::make_unique<StringIdLease>(map, recoverId2, kRecoverFile1),
        "(20 vs. 10) Multiple recover ids assigned to file_1");
  }
  ASSERT_EQ(map.pinnedSize(), 0);

  StringIdLease lease1(map, kRecoverFile1);
  ASSERT_EQ(map.pinnedSize(), ::strlen(kRecoverFile1));
  ASSERT_EQ(map.testingLastId(), recoverId1 + 1);

  {
    StringIdLease lease(map, recoverId2, kRecoverFile2);
    ASSERT_TRUE(lease.hasValue());
    ASSERT_EQ(lease.id(), recoverId2);
    ASSERT_EQ(
        map.pinnedSize(), ::strlen(kRecoverFile1) + ::strlen(kRecoverFile2));
    ASSERT_EQ(map.testingLastId(), recoverId2);
    VELOX_ASSERT_THROW(
        std::make_unique<StringIdLease>(map, recoverId2, kRecoverFile3),
        "(1 vs. 0) Reused recover id 20 assigned to file_3");
    VELOX_ASSERT_THROW(
        std::make_unique<StringIdLease>(map, recoverId2, kRecoverFile1),
        "(20 vs. 11) Multiple recover ids assigned to file_1");
    StringIdLease dupLease(map, recoverId2, kRecoverFile2);
    ASSERT_TRUE(lease.hasValue());
    ASSERT_EQ(lease.id(), recoverId2);
    ASSERT_EQ(
        map.pinnedSize(), ::strlen(kRecoverFile1) + ::strlen(kRecoverFile2));
  }

  ASSERT_EQ(map.testingLastId(), recoverId2);
  ASSERT_EQ(map.pinnedSize(), ::strlen(kRecoverFile1));
}
