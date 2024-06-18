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
#include <folly/futures/Future.h>
#include <folly/futures/Promise.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <signal.h>
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

int main(int argc, char** argv) {
  // Fork a child process to run all the tests.
  int32_t pid = fork();
  if (pid < 0) {
    LOG(ERROR) << "Failed to fork child";
    exit(1);
  }
  if (pid > 0) {
    // The parent waits for the child to return. If the child returns
    // in time, the child's return code is returned. If the child does
    // not return in time, we return 0 and the test fails silently.
    std::atomic<bool> timedOut = false;
    std::atomic<bool> completed = false;
    auto sleepPromise = folly::Promise<bool>();
    folly::SemiFuture<bool> sleepFuture(false);
    sleepFuture = sleepPromise.getSemiFuture();
    std::thread timer([&]() {
      try {
        auto& executor = folly::QueuedImmediateExecutor::instance();
        // Wait for up to 100 seconds. The test is normally ~20s unless it
        // hangs.
        std::move(sleepFuture).via(&executor).wait((std::chrono::seconds(100)));
      } catch (std::exception&) {
      }
      if (completed) {
        return;
      }
      timedOut = true;
      LOG(INFO) << "Killing the test process for timeout";
      kill(pid, SIGKILL);
    });

    int wstatus;
    int w = waitpid(pid, &wstatus, WUNTRACED | WCONTINUED);
    LOG(INFO) << "Test completed";
    completed = true;
    sleepPromise.setValue(true);
    timer.join();

    if (timedOut) {
      return 0;
    }
    return WEXITSTATUS(wstatus);
  }

  testing::InitGoogleTest(&argc, argv);
  // Signal handler required for ThreadDebugInfoTest
  folly::Init init(&argc, &argv, false);
  return RUN_ALL_TESTS();
  return 0;
}
