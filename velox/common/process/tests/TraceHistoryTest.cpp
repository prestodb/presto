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

#include "velox/common/process/TraceHistory.h"

#include <fmt/format.h>
#include <folly/synchronization/Baton.h>
#include <folly/synchronization/Latch.h>
#include <gtest/gtest.h>

namespace facebook::velox::process {
namespace {

class TraceHistoryTest : public testing::Test {
 public:
  void SetUp() override {
    ASSERT_TRUE(TraceHistory::listAll().empty());
  }

  void TearDown() override {
    ASSERT_TRUE(TraceHistory::listAll().empty());
  }
};

TEST_F(TraceHistoryTest, basic) {
  std::thread([] {
    auto timeLow = std::chrono::steady_clock::now();
    constexpr int kStartLine = __LINE__;
    for (int i = 0; i < TraceHistory::kCapacity + 10; ++i) {
      VELOX_TRACE_HISTORY_PUSH("Test %d", i);
    }
    auto timeHigh = std::chrono::steady_clock::now();
    auto results = TraceHistory::listAll();
    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0].threadId, std::this_thread::get_id());
    ASSERT_EQ(results[0].osTid, folly::getOSThreadID());
    ASSERT_EQ(results[0].entries.size(), TraceHistory::kCapacity);
    auto lastTime = timeLow;
    for (int i = 0; i < TraceHistory::kCapacity; ++i) {
      auto& entry = results[0].entries[i];
      ASSERT_EQ(entry.line, kStartLine + 2);
      ASSERT_STREQ(
          entry.file + strlen(entry.file) - 20, "TraceHistoryTest.cpp");
      ASSERT_LE(lastTime, entry.time);
      lastTime = entry.time;
      ASSERT_EQ(strncmp(entry.label, "Test ", 5), 0);
      ASSERT_EQ(atoi(entry.label + 5), i + 10);
    }
    ASSERT_LE(lastTime, timeHigh);
  }).join();
}

TEST_F(TraceHistoryTest, multiThread) {
  constexpr int kNumThreads = 3;
  folly::Latch latch(kNumThreads);
  folly::Baton<> batons[kNumThreads];
  std::vector<std::thread> threads;
  auto timeLow = std::chrono::steady_clock::now();
  constexpr int kStartLine = __LINE__;
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&, i] {
      VELOX_TRACE_HISTORY_PUSH("Test");
      VELOX_TRACE_HISTORY_PUSH("Test %d", i);
      latch.count_down();
      batons[i].wait();
    });
  }
  latch.wait();
  auto timeHigh = std::chrono::steady_clock::now();
  auto results = TraceHistory::listAll();
  ASSERT_EQ(results.size(), kNumThreads);
  for (auto& result : results) {
    auto threadIndex =
        std::find_if(
            threads.begin(),
            threads.end(),
            [&](auto& t) { return t.get_id() == result.threadId; }) -
        threads.begin();
    ASSERT_EQ(result.entries.size(), 2);
    ASSERT_EQ(result.entries[0].line, kStartLine + 3);
    ASSERT_EQ(result.entries[1].line, kStartLine + 4);
    ASSERT_STREQ(result.entries[0].label, "Test");
    ASSERT_EQ(result.entries[1].label, fmt::format("Test {}", threadIndex));
    for (auto& entry : result.entries) {
      ASSERT_LE(timeLow, entry.time);
      ASSERT_LE(entry.time, timeHigh);
      ASSERT_TRUE(entry.file);
      ASSERT_STREQ(
          entry.file + strlen(entry.file) - 20, "TraceHistoryTest.cpp");
    }
  }
  for (int i = 0; i < kNumThreads; ++i) {
    ASSERT_EQ(TraceHistory::listAll().size(), kNumThreads - i);
    batons[i].post();
    threads[i].join();
  }
}

TEST_F(TraceHistoryTest, largeLabel) {
  std::thread([] {
    VELOX_TRACE_HISTORY_PUSH(
        "%s",
        std::string(TraceHistory::Entry::kLabelCapacity + 10, 'x').c_str());
    auto results = TraceHistory::listAll();
    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0].entries.size(), 1);
    ASSERT_EQ(
        results[0].entries[0].label,
        std::string(TraceHistory::Entry::kLabelCapacity - 1, 'x'));
  }).join();
}

} // namespace
} // namespace facebook::velox::process
