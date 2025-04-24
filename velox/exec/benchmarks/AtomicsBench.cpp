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

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <atomic>
#include <thread>
#include <vector>
#include "velox/exec/OneWayStatusFlag.h"

using namespace ::testing;
using namespace facebook::velox;
static const size_t kNumThreads = 88;
static const size_t kNumIterations = 10000;

void runParallelUpdates(
    std::function<void(size_t iter)> callback,
    int threadCount,
    int updateCount) {
  std::vector<std::thread> threads;

  for (size_t i = 0; i < threadCount; ++i) {
    threads.emplace_back([&]() {
      for (size_t j = 0; j < updateCount; ++j) {
        callback(j);
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }
}

BENCHMARK(std_atomic_bool_write) {
  std::atomic<bool> flag{false};
  runParallelUpdates(
      [&](size_t iters) {
        for (size_t i = 0; i < iters; ++i) {
          flag.store(true);
        }
      },
      kNumThreads, // Threads
      kNumIterations); // Iterations per thread
}

BENCHMARK(std_atomic_bool_write_relaxed) {
  std::atomic<bool> flag{false};
  runParallelUpdates(
      [&](size_t iters) {
        for (size_t i = 0; i < iters; ++i) {
          flag.store(true, std::memory_order_relaxed);
        }
      },
      kNumThreads, // Threads
      kNumIterations); // Iterations per thread
}

BENCHMARK(std_atomic_bool_read_write_relaxed) {
  std::atomic<bool> flag{false};
  runParallelUpdates(
      [&](size_t iters) {
        for (size_t i = 0; i < iters; ++i) {
          if (!flag.load(std::memory_order_relaxed)) {
            flag.store(true, std::memory_order_acq_rel);
          }
        }
      },
      kNumThreads, // Threads
      kNumIterations); // Iterations per thread
}

BENCHMARK(one_way_flag_write) {
  exec::OneWayStatusFlag flag;
  runParallelUpdates(
      [&](size_t iters) {
        for (size_t i = 0; i < iters; ++i) {
          flag.set();
        }
      },
      kNumThreads, // Threads
      kNumIterations); // Iterations per thread
}

// Read Benchmarks
BENCHMARK(std_atomic_bool_read) {
  std::atomic<bool> flag{false};
  runParallelUpdates(
      [&](size_t iters) {
        for (size_t i = 0; i < iters; ++i) {
          folly::doNotOptimizeAway(flag.load());
        }
      },
      kNumThreads, // Threads
      kNumIterations); // Iterations per thread
}

BENCHMARK(std_atomic_bool_relaxed_read) {
  std::atomic<bool> flag{false};
  runParallelUpdates(
      [&](size_t iters) {
        for (size_t i = 0; i < iters; ++i) {
          folly::doNotOptimizeAway(flag.load(std::memory_order_relaxed));
        }
      },
      kNumThreads, // Threads
      kNumIterations); // Iterations per thread
}

BENCHMARK(std_atomic_bool_read_relaxed_acquire) {
  std::atomic<bool> flag{false};
  runParallelUpdates(
      [&](size_t iters) {
        for (size_t i = 0; i < iters; ++i) {
          folly::doNotOptimizeAway(
              flag.load(std::memory_order_relaxed) ||
              flag.load(std::memory_order_acquire));
        }
      },
      kNumThreads, // Threads
      kNumIterations); // Iterations per thread
}

BENCHMARK(one_way_flag_read) {
  exec::OneWayStatusFlag flag;
  runParallelUpdates(
      [&](size_t iters) {
        for (size_t i = 0; i < iters; ++i) {
          folly::doNotOptimizeAway(flag.check());
        }
      },
      kNumThreads, // Threads
      kNumIterations); // Iterations per thread
}

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  folly::runBenchmarks();
}
