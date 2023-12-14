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

#include "velox/dwio/common/ParallelFor.h"
#include "folly/Executor.h"
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "folly/executors/InlineExecutor.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "velox/common/base/VeloxException.h"

using namespace ::testing;
using namespace ::facebook::velox::dwio::common;

namespace {

class CountingExecutor : public folly::Executor {
 public:
  explicit CountingExecutor(folly::Executor& executor)
      : executor_(executor), count_(0) {}

  void add(folly::Func f) override {
    executor_.add(std::move(f));
    ++count_;
  }

  size_t getCount() const {
    return count_;
  }

 private:
  folly::Executor& executor_;
  size_t count_;
};

void testParallelFor(
    folly::Executor* executor,
    size_t from,
    size_t to,
    size_t parallelismFactor) {
  std::optional<CountingExecutor> countedExecutor;
  std::ostringstream oss;
  oss << "ParallelFor(executor: " << executor << ", from: " << from
      << ", to: " << to << ", parallelismFactor: " << parallelismFactor << ")";
  SCOPED_TRACE(oss.str());
  if (executor) {
    countedExecutor.emplace(*executor);
    executor = &countedExecutor.value();
  }

  std::unordered_map<size_t, std::atomic<size_t>> indexInvoked;
  for (size_t i = from; i < to; ++i) {
    indexInvoked[i] = 0UL;
  }

  ParallelFor(executor, from, to, parallelismFactor)
      .execute([&indexInvoked](size_t i) {
        auto it = indexInvoked.find(i);
        ASSERT_NE(it, indexInvoked.end());
        ++it->second;
      });

  // Parallel For should have thrown otherwise
  ASSERT_LE(from, to);

  // The method was called for each index just once, and didn't call out of
  // bounds indices.
  EXPECT_EQ(indexInvoked.size(), (to - from));
  for (auto& [i, count] : indexInvoked) {
    if (i < from || i >= to) {
      EXPECT_EQ(indexInvoked[i], 0);
    } else {
      EXPECT_EQ(indexInvoked[i], 1);
    }
  }

  if (countedExecutor) {
    const auto extraThreadsUsed = countedExecutor->getCount();
    const auto numTasks = to - from;
    const auto expectedExtraThreads = std::min(
        parallelismFactor > 0 ? parallelismFactor - 1 : 0,
        numTasks > 0 ? numTasks - 1 : 0);
    EXPECT_EQ(extraThreadsUsed, expectedExtraThreads);
  }
}

} // namespace

TEST(ParallelForTest, E2E) {
  auto inlineExecutor = folly::InlineExecutor::instance();
  for (size_t parallelism = 0; parallelism < 25; ++parallelism) {
    for (size_t begin = 0; begin < 25; ++begin) {
      for (size_t end = 0; end < 25; ++end) {
        if (begin <= end) {
          testParallelFor(&inlineExecutor, begin, end, parallelism);
        } else {
          EXPECT_THROW(
              testParallelFor(&inlineExecutor, begin, end, parallelism),
              facebook::velox::VeloxRuntimeError);
        }
      }
    }
  }
}

TEST(ParallelForTest, E2EParallel) {
  for (size_t parallelism = 1; parallelism < 2; ++parallelism) {
    folly::CPUThreadPoolExecutor executor(parallelism);
    for (size_t begin = 0; begin < 25; ++begin) {
      for (size_t end = 0; end < 25; ++end) {
        if (begin <= end) {
          testParallelFor(&executor, begin, end, parallelism);
        } else {
          EXPECT_THROW(
              testParallelFor(&executor, begin, end, parallelism),
              facebook::velox::VeloxRuntimeError);
        }
      }
    }
  }
}

TEST(ParallelForTest, CanOwnExecutor) {
  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(2);
  const size_t indexInvokedSize = 100;
  std::unordered_map<size_t, std::atomic<size_t>> indexInvoked;
  indexInvoked.reserve(indexInvokedSize);
  for (size_t i = 0; i < indexInvokedSize; ++i) {
    indexInvoked[i] = 0UL;
  }

  ParallelFor pf(executor, 0, indexInvokedSize, 9);
  pf.execute([&indexInvoked](size_t i) { ++indexInvoked[i]; });

  EXPECT_EQ(indexInvoked.size(), indexInvokedSize);
  for (size_t i = 0; i < indexInvokedSize; ++i) {
    EXPECT_EQ(indexInvoked[i], 1);
  }
}
