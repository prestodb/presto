/*
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
#include "presto_cpp/main/dpp/DppFilterCache.h"

#include <gtest/gtest.h>

namespace facebook::presto::dpp {
namespace {

class DppFilterCacheTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    velox::memory::MemoryManager::testingSetInstance(
        velox::memory::MemoryManager::Options{});
  }
};

// Build a TupleDomain with a simple single-column NOT NULL domain so the
// JSON round-trip is non-trivial.
protocol::TupleDomain<std::string> sampleTupleDomain(
    const std::string& column) {
  protocol::TupleDomain<std::string> td;
  td.domains =
      std::make_shared<protocol::Map<std::string, protocol::Domain>>();
  protocol::Domain domain;
  domain.nullAllowed = false;
  (*td.domains)[column] = std::move(domain);
  return td;
}

TEST_F(DppFilterCacheTest, serializeRoundTrip) {
  const auto td = sampleTupleDomain("col0");
  const auto bytes = serializeTupleDomain(td);
  ASSERT_FALSE(bytes.empty());

  const auto restored = deserializeTupleDomain(bytes.data(), bytes.size());
  ASSERT_NE(restored.domains, nullptr);
  ASSERT_EQ(restored.domains->size(), 1);
  ASSERT_EQ(restored.domains->begin()->first, "col0");
  ASSERT_FALSE(restored.domains->begin()->second.nullAllowed);
}

TEST_F(DppFilterCacheTest, registerTouchForgetLifecycle) {
  DppFilterCache::initialize(64ULL << 20);
  auto* cache = DppFilterCache::getInstance();
  ASSERT_NE(cache, nullptr);

  PrestoTask* fakeTask = reinterpret_cast<PrestoTask*>(0x1234);

  auto pool = cache->createTaskPool("test.task.lifecycle");
  ASSERT_NE(pool, nullptr);

  void* buf = pool->allocate(1024);
  ASSERT_NE(buf, nullptr);

  bool evicted = false;
  cache->registerFilter(
      fakeTask, "filterA", 1024, [&](const std::string& fid) {
        EXPECT_EQ(fid, "filterA");
        evicted = true;
        pool->free(buf, 1024);
        return true;
      });

  cache->touch(fakeTask, "filterA");

  const auto freed = cache->evictUpTo(1024);
  EXPECT_EQ(freed, 1024);
  EXPECT_TRUE(evicted);

  // Subsequent eviction is a no-op.
  EXPECT_EQ(cache->evictUpTo(1024), 0);
}

TEST_F(DppFilterCacheTest, evictUpToOldestFirst) {
  DppFilterCache::initialize(64ULL << 20);
  auto* cache = DppFilterCache::getInstance();
  PrestoTask* fakeTask = reinterpret_cast<PrestoTask*>(0x2345);

  auto pool = cache->createTaskPool("test.task.lru.order");

  void* a = pool->allocate(256);
  void* b = pool->allocate(256);
  void* c = pool->allocate(256);

  std::vector<std::string> evictionOrder;
  auto makeCb = [&](void* buf) {
    return [&, buf](const std::string& fid) {
      evictionOrder.push_back(fid);
      pool->free(buf, 256);
      return true;
    };
  };
  cache->registerFilter(fakeTask, "f1", 256, makeCb(a));
  cache->registerFilter(fakeTask, "f2", 256, makeCb(b));
  cache->registerFilter(fakeTask, "f3", 256, makeCb(c));

  // Touching f1 makes it the most recently used; LRU is now [f2, f3, f1].
  cache->touch(fakeTask, "f1");

  // Ask for 256 bytes — should evict only f2.
  EXPECT_EQ(cache->evictUpTo(256), 256);
  ASSERT_EQ(evictionOrder.size(), 1);
  EXPECT_EQ(evictionOrder[0], "f2");

  // Asking for 1024 should evict the remaining two: f3 then f1.
  EXPECT_EQ(cache->evictUpTo(1024), 512);
  ASSERT_EQ(evictionOrder.size(), 3);
  EXPECT_EQ(evictionOrder[1], "f3");
  EXPECT_EQ(evictionOrder[2], "f1");
}

TEST_F(DppFilterCacheTest, capEnforced) {
  // Use a dedicated root pool with a tiny cap; DppFilterCache::initialize
  // is idempotent across tests so we can't use the singleton here.
  auto root = velox::memory::MemoryManager::getInstance()->addRootPool(
      "dppCapTest", 1ULL << 20);
  auto pool = root->addLeafChild("leaf");

  std::vector<std::pair<void*, int64_t>> buffers;
  buffers.emplace_back(pool->allocate(256 * 1024), 256 * 1024);
  buffers.emplace_back(pool->allocate(256 * 1024), 256 * 1024);

  bool threw = false;
  try {
    buffers.emplace_back(pool->allocate(1024 * 1024), 1024 * 1024);
  } catch (const std::exception&) {
    threw = true;
  }
  EXPECT_TRUE(threw);

  for (auto& [buf, size] : buffers) {
    if (buf != nullptr) {
      pool->free(buf, size);
    }
  }
}

TEST_F(DppFilterCacheTest, pushRejectedCounter) {
  DppFilterCache::initialize(64ULL << 20);
  auto* cache = DppFilterCache::getInstance();
  const auto before = cache->pushRejectedCount();
  cache->incrementPushRejected();
  cache->incrementPushRejected();
  EXPECT_EQ(cache->pushRejectedCount(), before + 2);
}

TEST_F(DppFilterCacheTest, forgetTaskRemovesAllOwnedEntries) {
  DppFilterCache::initialize(64ULL << 20);
  auto* cache = DppFilterCache::getInstance();
  PrestoTask* taskA = reinterpret_cast<PrestoTask*>(0xA);
  PrestoTask* taskB = reinterpret_cast<PrestoTask*>(0xB);

  auto pool = cache->createTaskPool("test.task.forget");

  cache->registerFilter(taskA, "fA1", 128, [](const std::string&) {
    return true;
  });
  cache->registerFilter(taskA, "fA2", 128, [](const std::string&) {
    return true;
  });
  cache->registerFilter(taskB, "fB1", 128, [](const std::string&) {
    return true;
  });

  cache->forgetTask(taskA);

  // Only the surviving entry from taskB should be evictable.
  EXPECT_EQ(cache->evictUpTo(1024), 128);
  EXPECT_EQ(cache->evictUpTo(1024), 0);
}

} // namespace
} // namespace facebook::presto::dpp
