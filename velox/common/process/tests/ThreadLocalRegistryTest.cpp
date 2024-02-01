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

#include "velox/common/process/ThreadLocalRegistry.h"

#include <folly/synchronization/Baton.h>
#include <folly/synchronization/Latch.h>
#include <gtest/gtest.h>

#include <atomic>

namespace facebook::velox::process {
namespace {

template <typename Tag>
class TestObject {
 public:
  static std::atomic_int& count() {
    static std::atomic_int value;
    return value;
  }

  TestObject() : threadId_(std::this_thread::get_id()) {
    ++count();
  }

  ~TestObject() {
    --count();
  }

  std::thread::id threadId() const {
    return threadId_;
  }

 private:
  const std::thread::id threadId_;
};

TEST(ThreadLocalRegistryTest, basic) {
  struct Tag {};
  using T = TestObject<Tag>;
  ASSERT_EQ(T::count(), 0);
  auto registry = std::make_shared<ThreadLocalRegistry<T>>();
  registry->forAllValues([](const T&) { FAIL(); });
  thread_local ThreadLocalRegistry<T>::Reference ref(registry);
  const T* object = ref.withValue([](const T& x) {
    EXPECT_EQ(T::count(), 1);
    return &x;
  });
  ASSERT_EQ(object->threadId(), std::this_thread::get_id());
  ref.withValue([&](const T& x) { ASSERT_EQ(&x, object); });
  int count = 0;
  registry->forAllValues([&](const T& x) {
    ++count;
    ASSERT_EQ(x.threadId(), std::this_thread::get_id());
  });
  ASSERT_EQ(count, 1);
  ASSERT_EQ(T::count(), 1);
}

TEST(ThreadLocalRegistryTest, multiThread) {
  struct Tag {};
  using T = TestObject<Tag>;
  ASSERT_EQ(T::count(), 0);
  auto registry = std::make_shared<ThreadLocalRegistry<T>>();
  constexpr int kNumThreads = 7;
  std::vector<std::thread> threads;
  folly::Latch latch(kNumThreads);
  folly::Baton<> batons[kNumThreads];
  const T* objects[kNumThreads];
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&, i] {
      thread_local ThreadLocalRegistry<T>::Reference ref(registry);
      objects[i] = ref.withValue([](const T& x) { return &x; });
      latch.count_down();
      batons[i].wait();
    });
  }
  latch.wait();
  std::vector<int> indices;
  registry->forAllValues([&](const T& x) {
    auto it = std::find(std::begin(objects), std::end(objects), &x);
    indices.push_back(it - std::begin(objects));
  });
  ASSERT_EQ(indices.size(), kNumThreads);
  std::sort(indices.begin(), indices.end());
  for (int i = 0; i < kNumThreads; ++i) {
    ASSERT_EQ(indices[i], i);
    ASSERT_EQ(objects[i]->threadId(), threads[i].get_id());
    ASSERT_EQ(T::count(), kNumThreads - i);
    batons[i].post();
    threads[i].join();
  }
  ASSERT_EQ(T::count(), 0);
  registry->forAllValues([](const T&) { FAIL(); });
}

} // namespace
} // namespace facebook::velox::process
