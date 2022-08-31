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

#include "velox/common/base/AsyncSource.h"
#include <fmt/format.h>
#include <folly/Random.h>
#include <folly/Synchronized.h>
#include <gtest/gtest.h>
#include <thread>
#include "velox/common/base/Exceptions.h"

using namespace facebook::velox;

// A sample class to be constructed via AsyncSource.
struct Gizmo {
  explicit Gizmo(int32_t _id) : id(_id) {}

  const int32_t id;
};

TEST(AsyncSourceTest, basic) {
  AsyncSource<Gizmo> gizmo([]() { return std::make_unique<Gizmo>(11); });
  EXPECT_FALSE(gizmo.hasValue());
  gizmo.prepare();
  EXPECT_TRUE(gizmo.hasValue());
  auto value = gizmo.move();
  EXPECT_FALSE(gizmo.hasValue());
  EXPECT_EQ(11, value->id);

  AsyncSource<Gizmo> error(
      []() -> std::unique_ptr<Gizmo> { VELOX_USER_FAIL("Testing error"); });
  EXPECT_THROW(error.move(), VeloxException);
  EXPECT_TRUE(error.hasValue());
}

TEST(AsyncSourceTest, threads) {
  constexpr int32_t kNumThreads = 10;
  constexpr int32_t kNumGizmos = 2000;
  folly::Synchronized<std::unordered_set<int32_t>> results;
  std::vector<std::shared_ptr<AsyncSource<Gizmo>>> gizmos;
  for (auto i = 0; i < kNumGizmos; ++i) {
    gizmos.push_back(std::make_shared<AsyncSource<Gizmo>>([i]() {
      std::this_thread::sleep_for(std::chrono::milliseconds(1)); // NOLINT
      return std::make_unique<Gizmo>(i);
    }));
  }

  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (int32_t threadIndex = 0; threadIndex < kNumThreads; ++threadIndex) {
    threads.push_back(std::thread([threadIndex, &gizmos, &results]() {
      if (threadIndex < kNumThreads / 2) {
        // The first half of the threads prepare Gizmos in the background.
        for (auto i = 0; i < kNumGizmos; ++i) {
          gizmos[i]->prepare();
        }
      } else {
        // The rest of the threads first get random Gizmos and then do a pass
        // over all the Gizmos to make sure all get collected. We assert that
        // each Gizmo is obtained once.
        folly::Random::DefaultGenerator rng;
        for (auto i = 0; i < kNumGizmos / 3; ++i) {
          auto gizmo =
              gizmos[folly::Random::rand32(rng) % gizmos.size()]->move();
          if (gizmo) {
            results.withWLock([&](auto& set) {
              EXPECT_TRUE(set.find(gizmo->id) == set.end());
              set.insert(gizmo->id);
            });
          }
        }
        for (auto i = 0; i < gizmos.size(); ++i) {
          auto gizmo = gizmos[i]->move();
          if (gizmo) {
            results.withWLock([&](auto& set) {
              EXPECT_TRUE(set.find(gizmo->id) == set.end());
              set.insert(gizmo->id);
            });
          }
        }
      }
    }));
  }
  for (auto& thread : threads) {
    thread.join();
  }
  results.withRLock([&](auto& set) {
    for (auto i = 0; i < kNumGizmos; ++i) {
      EXPECT_TRUE(set.find(i) != set.end());
    }
  });
}

TEST(AsyncSourceTest, errorsWithThreads) {
  constexpr int32_t kNumGizmos = 50;
  constexpr int32_t kNumThreads = 10;
  std::vector<std::shared_ptr<AsyncSource<Gizmo>>> gizmos;
  std::atomic<int32_t> numErrors{0};
  for (auto i = 0; i < kNumGizmos; ++i) {
    gizmos.push_back(
        std::make_shared<AsyncSource<Gizmo>>([i]() -> std::unique_ptr<Gizmo> {
          std::this_thread::sleep_for(std::chrono::milliseconds(1)); // NOLINT
          VELOX_USER_FAIL("Testing error");
        }));
  }

  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (int32_t threadIndex = 0; threadIndex < kNumThreads; ++threadIndex) {
    threads.push_back(std::thread([threadIndex, &gizmos, &numErrors]() {
      if (threadIndex < kNumThreads / 2) {
        // The first half of the threads prepare Gizmos in the background.
        for (auto i = 0; i < kNumGizmos; ++i) {
          gizmos[i]->prepare();
        }
      } else {
        // The rest of the threads get random gizmos. They are
        // expected to produce an error or nullptr in the event
        // another thread is already waiting for the same gizmo.
        folly::Random::DefaultGenerator rng;
        for (auto i = 0; i < kNumGizmos / 3; ++i) {
          try {
            auto gizmo =
                gizmos[folly::Random::rand32(rng) % gizmos.size()]->move();
            EXPECT_EQ(nullptr, gizmo);
          } catch (std::exception& e) {
            ++numErrors;
          }
        }
      }
    }));
  }
  for (auto& thread : threads) {
    thread.join();
  }
  // There will always be errors since the first to wait for any given
  // gizmo is sure to get an error.
  EXPECT_LT(0, numErrors);
}
