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

#include <gtest/gtest.h>

#include <folly/executors/CPUThreadPoolExecutor.h>
#include "velox/common/base/Semaphore.h"

using namespace facebook::velox;
std::atomic<int32_t> numReleased = 0;

struct Callable {
  Callable(Semaphore& sem) : sem(sem) {}
  Semaphore& sem;

  void operator()() {
    ++numReleased;
    sem.release();
  }
};

TEST(SemaphoreTest, threads) {
  // Makes a pool of producer threads that release a semaphore and a set of
  // consumer threads that acquire the same semaphore. Once a consumer sees that
  // the expected number of acquires have been done, it releases the semaphore
  // enough times to unblock the other consumers.
  constexpr int32_t kNumProducers = 20;
  constexpr int32_t kNumConsumers = 20;
  constexpr int32_t kNumOps = 10000;
  auto executor = std::make_unique<folly::CPUThreadPoolExecutor>(kNumProducers);
  Semaphore sem(0);
  std::atomic<int32_t> numDone = 0;
  std::vector<std::thread> consumers;
  consumers.reserve(kNumConsumers);
  for (auto i = 0; i < kNumConsumers; ++i) {
    consumers.emplace_back(std::thread([&]() {
      for (;;) {
        sem.acquire();
        int32_t done = ++numDone;
        if (numDone == kNumOps) {
          // All producers are finished, continue the other consumers.
          for (auto i = 0; i < kNumConsumers - 1; ++i) {
            ++numReleased;
            sem.release();
          }
        }
        if (done >= kNumOps) {
          return;
        }
      }
    }));
  }
  std::vector<Callable> ops;
  ops.reserve(kNumOps);
  for (auto i = 0; i < kNumOps; ++i) {
    ops.emplace_back(sem);
    executor->add(ops.back());
  }
  for (auto& thread : consumers) {
    thread.join();
  }
  EXPECT_EQ(kNumOps + kNumConsumers - 1, numDone);
  EXPECT_EQ(numDone, numReleased);
}
