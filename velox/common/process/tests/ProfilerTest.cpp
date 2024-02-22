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

#include "velox/common/process/Profiler.h"
#include <gtest/gtest.h>
#include <thread>
#include "velox/common/process/TraceContext.h"

using namespace facebook::velox::process;
using namespace facebook::velox;

namespace {
int32_t fi(int32_t x) {
  return x < 2 ? x : fi(x - 1) + fi(x - 2);
}
void compute(int32_t seconds) {
  auto start = getCurrentTimeMs();
  constexpr int32_t kNumThreads = 10;
  for (;;) {
    std::vector<std::thread> threads;
    threads.reserve(kNumThreads);
    std::atomic<int32_t> sum = 0;
    for (int32_t i = 0; i < kNumThreads; ++i) {
      threads.push_back(std::thread([&]() {
        sum += fi(40);
        std::this_thread::sleep_for(std::chrono::milliseconds(3)); // NOLINT
      }));
    }
    for (auto& thread : threads) {
      thread.join();
    }
    LOG(INFO) << "Sum " << sum;
    if (getCurrentTimeMs() - start > seconds * 1000) {
      break;
    }
  }
}

} // namespace

TEST(ProfilerTest, basic) {
#if !defined(linux)
  return;
#endif
  filesystems::registerLocalFileSystem();
  // We have seconds of busy and idle activity. We set the profiler to
  // check every second and to trigger after 1s at 200%. A burst of
  // under 2s is not recorded and a new file is started after every 4s
  // of cpu busy.

  FLAGS_profiler_check_interval_seconds = 1;
  FLAGS_profiler_min_cpu_pct = 200;
  FLAGS_profiler_max_sample_seconds = 4;
  FLAGS_profiler_max_sample_seconds = 2;

  Profiler::start("/tmp/profilertest");
  compute(5);
  std::this_thread::sleep_for(std::chrono::seconds(2)); // NOLINT
  compute(1);
  std::this_thread::sleep_for(std::chrono::seconds(2)); // NOLINT

  compute(3);
  Profiler::stop();

  // We set the profiler to start regardless of load and wait 30s before
  // producing the next result.
  FLAGS_profiler_check_interval_seconds = 30;
  FLAGS_profiler_min_cpu_pct = 0;
  FLAGS_profiler_min_sample_seconds = 0;
  Profiler::start("/tmp/profilertest");
  compute(2);
  // The test exits during the measurement interval. We expect no
  // crash on exit if the threads are properly joined.
}
