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

#include "velox/common/base/ConcurrentCounter.h"

#include <fmt/format.h>
#include <folly/Random.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::velox::common::test {

class ConcurrentCounterTest : public testing::TestWithParam<bool> {
 protected:
  void SetUp() override {
    setupCounter();
  }

  void update(int64_t delta) {
    if (useUpdateFn_) {
      counter_->update(
          delta, [&](int64_t& counter, int64_t delta, std::mutex& lock) {
            std::lock_guard<std::mutex> l(lock);
            counter += delta;
            return true;
          });
    } else {
      counter_->update(delta);
    }
  }

  int64_t read() const {
    return counter_->read();
  }

  void setupCounter() {
    counter_ = std::make_unique<ConcurrentCounter<int64_t>>(
        std::thread::hardware_concurrency());
  }

  const bool useUpdateFn_{GetParam()};

  std::unique_ptr<ConcurrentCounter<int64_t>> counter_;
};

TEST_P(ConcurrentCounterTest, basic) {
  ASSERT_EQ(read(), 0);
  update(1);
  ASSERT_EQ(read(), 1);
  update(1);
  ASSERT_EQ(read(), 2);
  update(-1);
  ASSERT_EQ(read(), 1);
  update(-3);
  ASSERT_EQ(read(), -2);
}

TEST_P(ConcurrentCounterTest, multithread) {
  const int32_t numUpdatesPerThread = 5'000;
  std::vector<int> numThreads;
  numThreads.push_back(1);
  numThreads.push_back(std::thread::hardware_concurrency());
  numThreads.push_back(std::thread::hardware_concurrency() * 2);
  for (int numThreads : numThreads) {
    SCOPED_TRACE(fmt::format("numThreads: {}", numThreads));
    counter_->testingClear();
    ASSERT_EQ(counter_->read(), 0);

    std::vector<std::thread> threads;
    threads.reserve(numThreads);
    std::vector<int64_t> counts(numThreads, 0);
    for (size_t i = 0; i < numThreads; ++i) {
      ASSERT_EQ(counts[i], 0);
      threads.emplace_back([&, i]() {
        folly::Random::DefaultGenerator rng;
        rng.seed(1234 + i);
        ASSERT_EQ(counts[i], 0);
        for (int j = 0; j < numUpdatesPerThread; ++j) {
          const int delta = folly::Random::rand32(rng);
          counts[i] += delta;
          update(delta);
        }
      });
    }

    for (auto& th : threads) {
      th.join();
    }
    int64_t expectedCount{0};
    for (int i = 0; i < numThreads; ++i) {
      expectedCount += counts[i];
    }
    ASSERT_EQ(read(), expectedCount);
  }
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    ConcurrentCounterTestSuite,
    ConcurrentCounterTest,
    testing::ValuesIn({false, true}));

} // namespace facebook::velox::common::test
