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

#include <fmt/format.h>
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

} // namespace
} // namespace facebook::velox::process
