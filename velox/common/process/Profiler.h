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

#include <folly/futures/Future.h>
#include <folly/futures/Promise.h>
#include <string>
#include "velox/common/file/FileSystems.h"

DECLARE_int32(profiler_check_interval_seconds);
DECLARE_int32(profiler_min_cpu_pct);
DECLARE_int32(profiler_min_sample_seconds);
DECLARE_int32(profiler_max_sample_seconds);

namespace facebook::velox::process {

class Profiler {
 public:
  /// Starts periodic production of perf reports.
  static void start(const std::string& path);

  // Stops profiling background associated threads. Threads are stopped on
  // return.
  static void stop();

  static bool isRunning();

 private:
  static void copyToResult(const std::string* result = nullptr);
  static void makeProfileDir(std::string path);
  static std::thread startSample();
  // Returns after 'seconds' of wall time or sooner if interrupted by stop().
  static bool interruptibleSleep(int32_t seconds);
  static void stopSample(std::thread thread);
  static void threadFunction();

  static tsan_atomic<bool> profileStarted_;
  static std::thread profileThread_;
  static std::mutex profileMutex_;
  static std::shared_ptr<velox::filesystems::FileSystem> fileSystem_;
  static tsan_atomic<bool> isSleeping_;
  static tsan_atomic<bool> shouldStop_;
  static folly::Promise<bool> sleepPromise_;

  // Directory where results are deposited. Results have unique names within
  // this.
  static std::string resultPath_;

  // indicates if the results of the the profile should be saved at stop.
  static tsan_atomic<bool> shouldSaveResult_;

  // Time of starting the profile. Seconds from epoch.
  static int64_t sampleStartTime_;

  // CPU time at start of profile.
  static int64_t cpuAtSampleStart_;

  // CPU time at last periodic check.
  static int64_t cpuAtLastCheck_;
};

} // namespace facebook::velox::process
