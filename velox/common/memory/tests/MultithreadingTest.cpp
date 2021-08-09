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

#include <future>
#include <limits>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include "velox/common/memory/Memory.h"

using namespace ::testing;

namespace facebook {
namespace velox {
namespace memory {

namespace {
void* addChildAndAlloc(
    MemoryPool& root,
    const std::string& name,
    size_t iterations,
    int32_t refreshInterval,
    int64_t initialSize,
    int64_t sizeIncrement) {
  auto& pool = root.addChild(name);
  void* p = pool.allocate(initialSize);
  for (size_t i = 0; i != iterations; ++i) {
    int64_t size = initialSize + i * sizeIncrement;
    p = pool.reallocate(p, size, size + sizeIncrement);
    usleep(refreshInterval * 1000);
  }
  return p;
}
} // namespace

class MultiThreadingAggregateTest : public Test {
 public:
  explicit MultiThreadingAggregateTest()
      : manager_{std::numeric_limits<int64_t>::max(), kRefreshInterval},
        root_{manager_.getRoot()},
        allocFromSubtree_{[&](MemoryPool& subtree,
                              const std::string& name,
                              int64_t sizeIncrement) {
          return addChildAndAlloc(
              subtree,
              name,
              kIterations,
              kRefreshInterval,
              kInitialSize,
              sizeIncrement);
        }} {
    manager_.resumeAggregation();
  }

 protected:
  void SetUp() override {
    ASSERT_EQ(0, root_.getCurrentBytes());
  }

  static constexpr int32_t kRefreshInterval = 2;
  static constexpr int32_t kRefreshIntervalUsec = 2 * 1000;
  static constexpr size_t kIterations = 100;
  static constexpr int64_t kInitialSize = 500;

  MemoryManager<MemoryAllocator, kNoAlignment> manager_;
  MemoryPool& root_;
  std::function<void*(MemoryPool&, const std::string&, int64_t)>
      allocFromSubtree_;
};

TEST_F(MultiThreadingAggregateTest, Flat) {
  auto allocFromRoot = [&, this](
                           const std::string& name, int64_t sizeIncrement) {
    return allocFromSubtree_(root_, name, sizeIncrement);
  };

  auto future1 =
      std::async(std::launch::async, allocFromRoot, "realloc_high", 5);
  auto future2 =
      std::async(std::launch::async, allocFromRoot, "realloc_same", 0);
  auto future3 =
      std::async(std::launch::async, allocFromRoot, "realloc_low", -3);

  void* p1 = future1.get();
  void* p2 = future2.get();
  void* p3 = future3.get();

  usleep(3 * kRefreshIntervalUsec);
  EXPECT_EQ(1700, root_.getCurrentBytes());

  root_.free(p1, 1000);
  root_.free(p2, 500);
  root_.free(p3, 200);
}

TEST_F(MultiThreadingAggregateTest, SimpleTree) {
  // It's ok to set up the tree in the current thread.
  auto& subtree_a = root_.addChild("subtree_a");
  auto& subtree_b = root_.addChild("subtree_b");
  auto& subtree_ba = subtree_b.addChild("subtree_ba");

  auto future_aa = std::async(
      std::launch::async, allocFromSubtree_, std::ref(subtree_a), "pool_aa", 0);
  auto future_ab = std::async(
      std::launch::async, allocFromSubtree_, std::ref(subtree_a), "pool_ab", 5);
  auto future_bb = std::async(
      std::launch::async, allocFromSubtree_, std::ref(subtree_b), "pool_bb", 5);
  auto future_baa = std::async(
      std::launch::async,
      allocFromSubtree_,
      std::ref(subtree_ba),
      "pool_baa",
      -3);
  auto future_bab = std::async(
      std::launch::async,
      allocFromSubtree_,
      std::ref(subtree_ba),
      "pool_bab",
      0);
  auto future_c = std::async(
      std::launch::async, allocFromSubtree_, std::ref(root_), "pool_c", -3);

  void* p_aa = future_aa.get();
  void* p_ab = future_ab.get();
  void* p_bb = future_bb.get();
  void* p_baa = future_baa.get();
  void* p_bab = future_bab.get();
  void* p_c = future_c.get();

  usleep(kRefreshInterval * 1000);
  EXPECT_EQ(3400, root_.getCurrentBytes());
  EXPECT_EQ(1500, subtree_a.getCurrentBytes());
  EXPECT_EQ(1700, subtree_b.getCurrentBytes());
  EXPECT_EQ(700, subtree_ba.getCurrentBytes());

  root_.free(p_aa, 500);
  root_.free(p_ab, 1000);
  root_.free(p_bb, 1000);
  root_.free(p_baa, 200);
  root_.free(p_bab, 500);
  root_.free(p_c, 200);
}
/*
TEST(MultiThreadingCappingTest, Flat) {
  constexpr int32_t kRefreshInterval = 20;
  constexpr int32_t kRefreshIntervalUsec = 20 * 1000;
  // Disengage MemoryManager level capping.
  MemoryManager<> manager{std::numeric_limits<int64_t>::max(),
kRefreshInterval}; auto& root = manager.getRoot();

  auto& boundedRoot = root.addChild("bounded", 70);
  std::atomic<int> threadsStartedCount{0};
  auto allocFromRoot = [&, this](
                           const std::string& name, int64_t sizeIncrement) {
    // Synchronize on startup.
    threadsStartedCount++;
    while (threadsStartedCount != 3) {
      // Spin. Could use a conditional variable here if we care enough.
    }
    auto& pool = boundedRoot.addChild(name);
    int64_t size = sizeIncrement;
    void* p = pool.allocate(size);
    for (size_t i = 1; i < 20; ++i) {
      usleep(kRefreshIntervalUsec);
      try {
        p = pool.reallocate(p, size, size + sizeIncrement);
        size += sizeIncrement;
      } catch (const util::KoskiUserError& ex) {
        // ignore capping exception and keep trying.
      }
    }
    return p;
  };

  auto future_a = std::async(std::launch::async, allocFromRoot, "pool_a", 1);
  auto future_b = std::async(std::launch::async, allocFromRoot, "pool_b", 2);
  auto future_c = std::async(std::launch::async, allocFromRoot, "pool_c", 4);

  void* p_a = future_a.get();
  void* p_b = future_b.get();
  void* p_c = future_c.get();

  manager.refresh(true);
  // Actual allocation should be within one refresh cycle of hitting the cap.
  EXPECT_GE(70 + 7, boundedRoot.getCurrentBytes());
}

TEST(MultiThreadingCappingTest, SimpleTree) {
  constexpr int32_t kRefreshInterval = 20;
  constexpr int32_t kRefreshIntervalUsec = 20 * 1000;
  MemoryManager<> manager{1000, kRefreshInterval};
  auto& root = manager.getRoot();

  // Capping one subtree first.
  auto& subtree_a = root.addChild("subtree_a", 50);
  // Capping this subtree next, without hitting its child's cap.
  auto& subtree_b = root.addChild("subtree_b", 500);
  auto& subtree_ba = subtree_b.addChild("subtree_ba", 400);

  constexpr size_t kIterations = 20;
  std::atomic<int> initializedCount{0};
  auto allocFromSubtree = [&, this](
                              MemoryPool& subtree,
                              const std::string& name,
                              int64_t initialSize,
                              int64_t sizeIncrement) {
    auto& pool = subtree.addChild(name);
    int64_t size = initialSize;
    void* p = pool.allocate(size);
    // Synchronize on startup.
    initializedCount++;
    while (initializedCount != 10) {
      // Spin. Could use a conditional variable here if we care enough.
    }

    for (size_t i = 0; i < kIterations; ++i) {
      usleep(kRefreshIntervalUsec);
      try {
        p = pool.reallocate(p, size, size + sizeIncrement);
        size += sizeIncrement;
      } catch (const util::KoskiUserError& ex) {
        // ignore capping exception and keep trying.
      }
      LOG(INFO) << fmt::format(
          "POV {} iteration #{}: subtree {} has total usage {}, local usage {}",
          name,
          i,
          subtree.getName(),
          subtree.getCurrentBytes(),
          pool.getCurrentBytes());
    }
    return p;
  };

  // Capping subtree_a first.
  auto future_aa = std::async(
      std::launch::async,
      allocFromSubtree,
      std::ref(subtree_a),
      "pool_aa",
      25,
      5);
  auto future_ab = std::async(
      std::launch::async,
      allocFromSubtree,
      std::ref(subtree_a),
      "pool_ab",
      25,
      5);

  // Capping subtree_b next, without capping subtree_ba.
  auto future_bb = std::async(
      std::launch::async,
      allocFromSubtree,
      std::ref(subtree_b),
      "pool_bb",
      25,
      5);
  auto future_bc = std::async(
      std::launch::async,
      allocFromSubtree,
      std::ref(subtree_b),
      "pool_bc",
      25,
      5);
  auto future_bd = std::async(
      std::launch::async,
      allocFromSubtree,
      std::ref(subtree_b),
      "pool_bd",
      25,
      5);
  auto future_be = std::async(
      std::launch::async,
      allocFromSubtree,
      std::ref(subtree_b),
      "pool_be",
      25,
      5);
  auto future_baa = std::async(
      std::launch::async,
      allocFromSubtree,
      std::ref(subtree_ba),
      "pool_baa",
      100,
      5);
  auto future_bab = std::async(
      std::launch::async,
      allocFromSubtree,
      std::ref(subtree_ba),
      "pool_bab",
      100,
      0);

  // This node always has enough quota.
  auto future_c = std::async(
      std::launch::async, allocFromSubtree, std::ref(root), "pool_c", 50, 5);
  auto future_d = std::async(
      std::launch::async, allocFromSubtree, std::ref(root), "pool_d", 50, 5);

  void* p_aa = future_aa.get();
  void* p_ab = future_ab.get();
  void* p_bb = future_bb.get();
  void* p_bc = future_bc.get();
  void* p_bd = future_bd.get();
  void* p_be = future_be.get();
  void* p_baa = future_baa.get();
  void* p_bab = future_bab.get();
  void* p_c = future_c.get();
  void* p_d = future_d.get();

  manager.refresh(true);
  // Actual allocation should be within one refresh cycle of hitting the cap.
  EXPECT_GE(850 + 5 * 9, root.getCurrentBytes());
  EXPECT_GE(50 + 5 * 2, subtree_a.getCurrentBytes());
  EXPECT_GE(500 + 5 * 5, subtree_b.getCurrentBytes());
}

TEST(MultiThreadingUncappingTest, Flat) {
  constexpr int32_t kRefreshInterval = 20;
  constexpr int32_t kRefreshIntervalUsec = 20 * 1000;
  // Disengage MemoryManager level capping.
  MemoryManager<> manager{std::numeric_limits<int64_t>::max(),
kRefreshInterval}; auto& root = manager.getRoot(); auto& boundedRoot =
root.addChild("bounded", 69);

  manager.pauseAggregation();
  auto& pool_a = boundedRoot.addChild("pool_a");
  void* p_a = pool_a.allocate(10);
  auto& pool_b = boundedRoot.addChild("pool_b");
  void* p_b = pool_b.allocate(20);
  auto& pool_c = boundedRoot.addChild("pool_c");
  void* p_c = pool_c.allocate(40);
  manager.refresh(true);
  ASSERT_TRUE(boundedRoot.isMemoryCapped());

  manager.resumeAggregation();
  std::atomic<int> threadsStartedCount{0};
  auto resumeAlloc =
      [&, this](
          MemoryPool& pool, void* p, int64_t size, int64_t sizeIncrement) {
        // Synchronize on startup.
        threadsStartedCount++;
        while (threadsStartedCount != 3) {
          // Spin. Could use a conditional variable here if we care enough.
        }
        for (size_t i = 1; i < 10; ++i) {
          usleep(kRefreshIntervalUsec);
          size += sizeIncrement;
          p = pool.reallocate(p, size, size + sizeIncrement);
        }
        return p;
      };

  auto future_a = std::async(
      std::launch::async, resumeAlloc, std::ref(pool_a), p_a, 10, -1);
  auto future_b = std::async(
      std::launch::async, resumeAlloc, std::ref(pool_b), p_b, 20, -2);
  auto future_c = std::async(
      std::launch::async, resumeAlloc, std::ref(pool_c), p_c, 40, -4);

  p_a = future_a.get();
  p_b = future_b.get();
  p_c = future_c.get();

  manager.refresh(true);
  EXPECT_EQ(7, boundedRoot.getCurrentBytes());
  EXPECT_FALSE(boundedRoot.isMemoryCapped());
}

TEST(MultiThreadingUncappingTest, SimpleTree) {
  constexpr int32_t kRefreshInterval = 20;
  constexpr int32_t kRefreshIntervalUsec = 20 * 1000;
  MemoryManager<> manager{999, kRefreshInterval};

  auto& root = manager.getRoot();
  auto& subtree_a = root.addChild("subtree_a", 49);
  auto& pool_aa = subtree_a.addChild("pool_aa");
  auto& pool_ab = subtree_a.addChild("pool_ab");
  auto& subtree_b = root.addChild("subtree_b", 499);
  auto& pool_bb = subtree_b.addChild("pool_bb");
  auto& subtree_ba = subtree_b.addChild("subtree_ba", 399);
  auto& pool_baa = subtree_ba.addChild("pool_baa");
  auto& pool_bab = subtree_ba.addChild("pool_bab");
  auto& pool_c = root.addChild("pool_c");

  manager.pauseAggregation();
  void* p_aa = pool_aa.allocate(25);
  void* p_ab = pool_ab.allocate(25);
  void* p_bb = pool_bb.allocate(260);
  void* p_baa = pool_baa.allocate(140);
  void* p_bab = pool_bab.allocate(100);
  void* p_c = pool_c.allocate(300);

  manager.refresh(true);
  ASSERT_TRUE(subtree_ba.isMemoryCapped());
  ASSERT_FALSE(root.isMemoryCapped());

  pool_bb.reallocate(p_bb, 260, 250);
  manager.resumeAggregation();

  constexpr size_t kIterations = 20;
  std::atomic<int> threadsStartedCount{0};

  auto resumeAlloc = [&](MemoryPool& pool,
                         void* p,
                         int64_t size,
                         int64_t sizeIncrement) {
    // Synchronize on startup.
    threadsStartedCount++;
    while (threadsStartedCount != 6) {
      // Spin. Could use a conditional variable here if we care enough.
    }
    for (size_t i = 0; i < kIterations; ++i) {
      usleep(kRefreshIntervalUsec);
      size += sizeIncrement;
      try {
        p = pool.reallocate(p, size, size + sizeIncrement);
      } catch (const util::KoskiUserError& ex) {
        // ignore capping exception and keep retrying.
      }
      LOG(INFO) << fmt::format(
          "POV {} iteration #{}: total usage {}.",
          pool.getName(),
          i,
          pool.getCurrentBytes());
    }
    return p;
  };

  // Still capping on these threads.
  auto future_aa = std::async(
      std::launch::async, resumeAlloc, std::ref(pool_aa), p_aa, 25, 5);
  auto future_ab = std::async(
      std::launch::async, resumeAlloc, std::ref(pool_ab), p_ab, 25, 5);

  // Reduce memory usage from pool_bb.
  auto future_bb = std::async(
      std::launch::async, resumeAlloc, std::ref(pool_bb), p_bb, 250, -12);
  auto future_baa = std::async(
      std::launch::async, resumeAlloc, std::ref(pool_baa), p_baa, 140, 0);
  auto future_bab = std::async(
      std::launch::async, resumeAlloc, std::ref(pool_bab), p_bab, 100, 10);

  auto future_c = std::async(
      std::launch::async, resumeAlloc, std::ref(pool_c), p_c, 300, 10);

  p_aa = future_aa.get();
  p_ab = future_ab.get();
  p_bb = future_bb.get();
  p_baa = future_baa.get();
  p_bab = future_bab.get();
  p_c = future_c.get();

  manager.refresh(true);
  // Can instead check that memory cap is exceeded by no more than one round of
  // allocation.
  EXPECT_GE(960, root.getCurrentBytes());
  EXPECT_EQ(50, subtree_a.getCurrentBytes());
  EXPECT_EQ(10, pool_bb.getCurrentBytes());
  EXPECT_EQ(140, pool_baa.getCurrentBytes());
  EXPECT_GE(260, pool_bab.getCurrentBytes());
  EXPECT_GE(400, subtree_ba.getCurrentBytes());
  EXPECT_GE(410, subtree_b.getCurrentBytes());
}
*/

} // namespace memory
} // namespace velox
} // namespace facebook
