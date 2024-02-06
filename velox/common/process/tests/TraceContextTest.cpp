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

#include "velox/common/process/TraceContext.h"
#include "velox/common/process/TraceHistory.h"

#include <fmt/format.h>
#include <folly/futures/Promise.h>
#include <folly/synchronization/Baton.h>
#include <folly/synchronization/Latch.h>
#include <gtest/gtest.h>

#include <thread>

namespace facebook::velox::process {
namespace {

class TraceContextTest : public testing::Test {
 public:
  void SetUp() override {
    ASSERT_TRUE(TraceContext::status().empty());
  }

  void TearDown() override {
    ASSERT_TRUE(TraceContext::status().empty());
  }
};

TEST_F(TraceContextTest, basic) {
  constexpr int kNumThreads = 3;
  std::vector<std::thread> threads;
  folly::Baton<> batons[2][kNumThreads];
  folly::Latch latches[2] = {
      folly::Latch(kNumThreads),
      folly::Latch(kNumThreads),
  };
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&, i]() {
      {
        TraceContext trace1("process data");
        TraceContext trace2(fmt::format("Process chunk {}", i), true);
        latches[0].count_down();
        batons[0][i].wait();
      }
      latches[1].count_down();
      batons[1][i].wait();
    });
  }
  latches[0].wait();
  auto status = TraceContext::status();
  ASSERT_EQ(1 + kNumThreads, status.size());
  ASSERT_EQ(kNumThreads, status.at("process data").numThreads);
  for (int i = 0; i < kNumThreads; ++i) {
    ASSERT_EQ(1, status.at(fmt::format("Process chunk {}", i)).numThreads);
  }
  for (int i = 0; i < kNumThreads; ++i) {
    batons[0][i].post();
  }
  latches[1].wait();
  status = TraceContext::status();
  ASSERT_EQ(1, status.size());
  ASSERT_EQ(0, status.at("process data").numThreads);
  ASSERT_EQ(kNumThreads, status.at("process data").numEnters);
  for (int i = 0; i < kNumThreads; ++i) {
    batons[1][i].post();
    threads[i].join();
  }
}

TEST_F(TraceContextTest, traceHistory) {
  std::thread([] {
    TraceContext trace("test");
    TraceContext trace2(
        std::string(TraceHistory::Entry::kLabelCapacity + 10, 'x'));
    auto results = TraceHistory::listAll();
    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0].entries.size(), 2);
    ASSERT_STREQ(results[0].entries[0].label, "test");
    ASSERT_EQ(
        results[0].entries[1].label,
        std::string(TraceHistory::Entry::kLabelCapacity - 1, 'x'));
  }).join();
}

TEST_F(TraceContextTest, transferBetweenThreads) {
  auto [promise, future] =
      folly::makePromiseContract<std::unique_ptr<TraceContext>>();
  folly::Baton<> batons[2];
  std::chrono::steady_clock::time_point timeLow, timeHigh;
  std::thread receiver([&, future = std::move(future)]() mutable {
    auto trace = std::move(future).get(std::chrono::seconds(1));
    {
      SCOPE_EXIT {
        batons[0].post();
      };
      auto status = TraceContext::status();
      ASSERT_EQ(1, status.size());
      auto& data = status.at("test");
      ASSERT_EQ(data.numThreads, 1);
      ASSERT_EQ(data.numEnters, 1);
      ASSERT_LE(timeLow, data.startTime);
      ASSERT_LE(data.startTime, timeHigh);
    }
    batons[1].wait();
    auto status = TraceContext::status();
    ASSERT_EQ(1, status.size());
    auto& data = status.at("test");
    ASSERT_EQ(data.numThreads, 1);
    ASSERT_EQ(data.numEnters, 1);
    ASSERT_LE(timeLow, data.startTime);
    ASSERT_LE(data.startTime, timeHigh);
  });
  timeLow = std::chrono::steady_clock::now();
  std::thread([&, promise = std::move(promise)]() mutable {
    auto trace = std::make_unique<TraceContext>("test");
    timeHigh = std::chrono::steady_clock::now();
    promise.setValue(std::move(trace));
    batons[0].wait();
  }).join();
  batons[1].post();
  receiver.join();
}

} // namespace
} // namespace facebook::velox::process
