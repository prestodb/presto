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

#include "velox/common/base/IndexedPriorityQueue.h"

#include <gtest/gtest.h>

#include "folly/Random.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/time/Timer.h"

namespace facebook::velox {
namespace {
class IndexedPriorityQueueTest : public testing::Test {
 protected:
  template <typename T, bool MaxQueue>
  void verify(
      const IndexedPriorityQueue<T, MaxQueue>& queue,
      const std::vector<T>& expectedValues) {
    auto clone = queue;
    verifyAndRemove(expectedValues, clone);
  }

  template <typename T, bool MaxQueue>
  void verifyAndRemove(
      const std::vector<T>& expectedValues,
      IndexedPriorityQueue<T, MaxQueue>& queue) {
    ASSERT_EQ(expectedValues.size(), queue.size());
    int i{0};
    while (!queue.empty()) {
      ASSERT_EQ(queue.pop(), expectedValues[i++]);
    }
    ASSERT_TRUE(queue.empty());
  }

  template <typename T, bool MaxQueue>
  void fuzz(
      IndexedPriorityQueue<T, MaxQueue>& queue,
      int numIterations,
      std::mt19937& rng) {
    SCOPED_TRACE(fmt::format("MaxQueue: ", MaxQueue));
    std::unordered_map<uint32_t, uint64_t> valuePriorities;
    for (auto i = 0; i < numIterations; ++i) {
      const uint32_t value = folly::Random::rand32(100, rng);
      const uint64_t priority = folly::Random::rand32(80, rng);
      valuePriorities[value] = priority;
      queue.addOrUpdate(value, priority);
    }
    ASSERT_EQ(queue.size(), valuePriorities.size());
    auto size = queue.size();
    std::unordered_set<uint32_t> queuedValues;
    uint64_t prev = MaxQueue ? std::numeric_limits<uint64_t>::max() : 0;
    while (!queue.empty()) {
      auto value = queue.pop();
      queuedValues.insert(value);
      ASSERT_TRUE(valuePriorities.find(value) != valuePriorities.end());
      if (MaxQueue) {
        ASSERT_LE(valuePriorities[value], prev);
        prev = valuePriorities[value];
      } else {
        ASSERT_GE(valuePriorities[value], prev);
        prev = valuePriorities[value];
      }
    }
    ASSERT_EQ(queuedValues.size(), size);
    ASSERT_EQ(size, valuePriorities.size());
  }
};

TEST_F(IndexedPriorityQueueTest, insertOnly) {
  const int numValues{5};
  const std::vector<uint32_t> priorities = {0, 10, 100, 20, 80};
  const std::vector<uint32_t> expectedMaxValues = {2, 4, 3, 1, 0};
  const std::vector<uint32_t> expectedMinValues = {0, 1, 3, 4, 2};

  IndexedPriorityQueue<uint32_t, true> maxQueue;
  IndexedPriorityQueue<uint32_t, false> minQueue;
  ASSERT_EQ(maxQueue.size(), 0);
  ASSERT_EQ(minQueue.size(), 0);
  ASSERT_TRUE(maxQueue.empty());
  ASSERT_TRUE(minQueue.empty());

  for (int value = 0; value < numValues; ++value) {
    maxQueue.addOrUpdate(value, priorities[value]);
    minQueue.addOrUpdate(value, priorities[value]);
  }
  ASSERT_EQ(maxQueue.size(), numValues);
  ASSERT_EQ(minQueue.size(), numValues);
  verifyAndRemove(expectedMaxValues, maxQueue);
  verifyAndRemove(expectedMinValues, minQueue);
}

TEST_F(IndexedPriorityQueueTest, priorityTie) {
  const uint32_t value1{31};
  const uint32_t value2{32};
  const uint32_t value3{33};
  const std::vector<uint32_t> expectedValues{31, 32, 33};

  IndexedPriorityQueue<uint32_t, true> maxQueue;
  maxQueue.addOrUpdate(value1, /*priority=*/1);
  maxQueue.addOrUpdate(value2, /*priority=*/1);
  maxQueue.addOrUpdate(value3, /*priority=*/1);
  verify(maxQueue, expectedValues);
  verifyAndRemove(expectedValues, maxQueue);

  IndexedPriorityQueue<uint32_t, false> minQueue;
  minQueue.addOrUpdate(value1, /*priority=*/1);
  minQueue.addOrUpdate(value2, /*priority=*/1);
  minQueue.addOrUpdate(value3, /*priority=*/1);
  verify(minQueue, expectedValues);
  verifyAndRemove(expectedValues, minQueue);
}

TEST_F(IndexedPriorityQueueTest, insertWithUpdate) {
  const uint32_t value1{31};
  const uint32_t value2{32};
  const uint32_t value3{33};
  IndexedPriorityQueue<uint32_t, true> maxQueue;
  maxQueue.addOrUpdate(value1, /*priority=*/1);
  maxQueue.addOrUpdate(value2, /*priority=*/1);
  maxQueue.addOrUpdate(value3, /*priority=*/1);
  verify(maxQueue, {31, 32, 33});

  maxQueue.addOrUpdate(value2, 20);
  verify(maxQueue, {32, 31, 33});

  maxQueue.addOrUpdate(value2, 0);
  verify(maxQueue, {31, 33, 32});

  maxQueue.addOrUpdate(value2, 10);
  verify(maxQueue, {32, 31, 33});

  maxQueue.addOrUpdate(value1, 20);
  verify(maxQueue, {31, 32, 33});

  maxQueue.addOrUpdate(value3, 15);
  verify(maxQueue, {31, 33, 32});

  maxQueue.addOrUpdate(value2, 40);
  verify(maxQueue, {32, 31, 33});

  IndexedPriorityQueue<uint32_t, false> minQueue;
  minQueue.addOrUpdate(value1, /*priority=*/1);
  minQueue.addOrUpdate(value2, /*priority=*/1);
  minQueue.addOrUpdate(value3, /*priority=*/1);
  verify(minQueue, {31, 32, 33});

  minQueue.addOrUpdate(value2, 20);
  verify(minQueue, {31, 33, 32});

  minQueue.addOrUpdate(value2, 0);
  verify(minQueue, {32, 31, 33});

  minQueue.addOrUpdate(value2, 10);
  verify(minQueue, {31, 33, 32});

  minQueue.addOrUpdate(value1, 20);
  verify(minQueue, {33, 32, 31});

  minQueue.addOrUpdate(value3, 15);
  verify(minQueue, {32, 33, 31});

  minQueue.addOrUpdate(value2, 40);
  verify(minQueue, {33, 31, 32});
}

TEST_F(IndexedPriorityQueueTest, remove) {
  const uint32_t value1{31};
  const uint32_t value2{32};
  const uint32_t value3{33};
  IndexedPriorityQueue<uint32_t, true> maxQueue;
  maxQueue.addOrUpdate(value1, /*priority=*/1);
  maxQueue.addOrUpdate(value2, /*priority=*/2);
  maxQueue.addOrUpdate(value3, /*priority=*/3);
  verify(maxQueue, {33, 32, 31});
  ASSERT_EQ(maxQueue.pop(), 33);
  verify(maxQueue, {32, 31});
  maxQueue.addOrUpdate(value2, 0);
  ASSERT_EQ(maxQueue.pop(), 31);
  verify(maxQueue, {32});
  ASSERT_EQ(maxQueue.pop(), 32);
  ASSERT_TRUE(maxQueue.empty());

  IndexedPriorityQueue<uint32_t, false> minQueue;
  minQueue.addOrUpdate(value1, /*priority=*/1);
  minQueue.addOrUpdate(value2, /*priority=*/2);
  minQueue.addOrUpdate(value3, /*priority=*/3);
  verify(minQueue, {31, 32, 33});
  ASSERT_EQ(minQueue.pop(), 31);
  verify(minQueue, {32, 33});
  minQueue.addOrUpdate(value2, 20);
  ASSERT_EQ(minQueue.pop(), 33);
  verify(minQueue, {32});
  ASSERT_EQ(minQueue.pop(), 32);
  ASSERT_TRUE(minQueue.empty());
}

TEST_F(IndexedPriorityQueueTest, fuzz) {
  std::mt19937 rng{100};
  IndexedPriorityQueue<uint32_t, true> maxQueue;
  fuzz<uint32_t, true>(maxQueue, 1'000, rng);

  IndexedPriorityQueue<uint32_t, false> minQueue;
  fuzz<uint32_t, false>(minQueue, 1'000, rng);
}
} // namespace
} // namespace facebook::velox
