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
#include <gtest/gtest.h>
#include <thread>

using namespace facebook::velox::process;

TEST(TraceContextTest, basic) {
  constexpr int32_t kNumThreads = 10;
  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (int32_t i = 0; i < kNumThreads; ++i) {
    threads.push_back(std::thread([i]() {
      TraceContext trace1("process data");
      TraceContext trace2(fmt::format("Process chunk {}", i), true);
      std::this_thread::sleep_for(std::chrono::milliseconds(3));
    }));
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(1));
  LOG(INFO) << TraceContext::statusLine();
  for (auto& thread : threads) {
    thread.join();
  }
  LOG(INFO) << TraceContext::statusLine();
  // We expect one entry for "process data". The temporary entries
  // are deleted after the treads complete.
  auto after = TraceContext::status();
  EXPECT_EQ(1, after.size());
  EXPECT_EQ(kNumThreads, after["process data"].numEnters);
  EXPECT_EQ(0, after["process data"].numThreads);
}
