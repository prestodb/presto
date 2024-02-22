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
#include "velox/common/file/File.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <memory>
#include <mutex>
#include <thread>

#include <fcntl.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>

DEFINE_string(profiler_tmp_dir, "/tmp", "Writable temp for perf.data");

DEFINE_int32(
    profiler_check_interval_seconds,
    60,
    "Frequency of checking CPU load and turning profiling on/off");

DEFINE_int32(
    profiler_min_cpu_pct,
    200,
    "Minimum CPU percent to justify profile. 100 is one core busy");

DEFINE_int32(
    profiler_min_sample_seconds,
    60,
    "Minimum amount of time at above minimum load to justify producing a result file");

DEFINE_int32(
    profiler_max_sample_seconds,
    300,
    "Number of seconds before switching to new file");

DEFINE_string(profiler_perf_flags, "", "Extra flags for Linux perf");

namespace facebook::velox::process {

tsan_atomic<bool> Profiler::profileStarted_;
std::thread Profiler::profileThread_;
std::mutex Profiler::profileMutex_;
std::shared_ptr<velox::filesystems::FileSystem> Profiler::fileSystem_;
tsan_atomic<bool> Profiler::isSleeping_;
std::string Profiler::resultPath_;
tsan_atomic<bool> Profiler::shouldStop_;
folly::Promise<bool> Profiler::sleepPromise_;
tsan_atomic<bool> Profiler::shouldSaveResult_;
int64_t Profiler::sampleStartTime_;
int64_t Profiler::cpuAtSampleStart_;
int64_t Profiler::cpuAtLastCheck_;

namespace {
std::string hostname;

// Check that paths do not have shell escapes.
void checkSafe(const std::string& str) {
  if (strchr(str.c_str(), '`') != nullptr ||
      strchr(str.c_str(), '$') != nullptr) {
    LOG(ERROR) << "Unsafe path " << str << ". Exiting.";
    ::exit(1);
  }
}

void testWritable(const std::string& dir) {
  auto testPath = fmt::format("{}/test", dir);
  int32_t fd =
      open(testPath.c_str(), O_RDWR | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
  if (fd < 0) {
    LOG(ERROR) << "Can't open " << testPath << " for write errno=" << errno;
    return;
  }
  if (4 != write(fd, "test", 4)) {
    LOG(ERROR) << "Can't write to " << testPath << " errno=" << errno;
  }
  close(fd);
}

// Returns user+system cpu seconds from getrusage()
int64_t cpuSeconds() {
  struct rusage ru;
  getrusage(RUSAGE_SELF, &ru);
  return ru.ru_utime.tv_sec + ru.ru_stime.tv_sec;
}

int64_t nowSeconds() {
  struct timeval tv;
  struct timezone tz;
  gettimeofday(&tv, &tz);
  return tv.tv_sec;
}

std::string timeString(time_t seconds) {
  struct tm tm;
  localtime_r(&seconds, &tm);
  char temp[100];
  strftime(temp, sizeof(temp), "%Y-%m-%d_%H:%M:%S", &tm);
  return std::string(temp);
}
} // namespace

void Profiler::copyToResult(const std::string* data) {
  char* buffer;
  int32_t resultSize;
  std::string temp;
  if (data) {
    buffer = const_cast<char*>(data->data());
    resultSize = std::min<int32_t>(data->size(), 400000);
  } else {
    testWritable(FLAGS_profiler_tmp_dir);
    auto reportFile = fmt::format("{}/perf", FLAGS_profiler_tmp_dir);
    int32_t fd = open(reportFile.c_str(), O_RDONLY);
    if (fd < 0) {
      LOG(ERROR) << "PROFILE: << Could not open report file at " << reportFile;
      return;
    }
    auto bufferSize = 400000;
    temp.resize(400000);
    buffer = temp.data();
    resultSize = ::read(fd, buffer, bufferSize);
    close(fd);
  }

  std::string dt = timeString(nowSeconds());
  auto target =
      fmt::format("{}/prof-{}-{}-{}", resultPath_, hostname, dt, getpid());
  try {
    try {
      fileSystem_->remove(target);
    } catch (const std::exception& e) {
      // ignore
    }
    auto out = fileSystem_->openFileForWrite(target);
    auto now = nowSeconds();
    auto elapsed = (now - sampleStartTime_);
    auto cpu = cpuSeconds();
    out->append(fmt::format(
        "Profile from {} to {} at {}% CPU\n\n",

        timeString(sampleStartTime_),
        timeString(now),
        100 * (cpu - cpuAtSampleStart_) / std::max<int64_t>(1, elapsed)));
    out->append(std::string_view(buffer, resultSize));
    out->flush();
    LOG(INFO) << "PROFILE: Produced result " << target << " " << resultSize
              << " bytes";
  } catch (const std::exception& e) {
    LOG(ERROR) << "PROFILE: Error opening/writing " << target << ":"
               << e.what();
  }
}

void Profiler::makeProfileDir(std::string path) {
  try {
    fileSystem_->mkdir(path);
  } catch (const std::exception& e) {
    LOG(ERROR) << "PROFILE: Failed to create directory " << path << ":"
               << e.what();
  }
}

std::thread Profiler::startSample() {
  std::thread thread([&]() {
    // We run perf under a shell because running it with fork + rexec
    // and killing it with SIGINT produces a corrupt perf.data
    // file. The perf.data file generated when called via system() is
    // good, though. Unsolved mystery.
    system(fmt::format(
               "(cd {}; /usr/bin/perf record --pid {} {};"
               "perf report --sort symbol > perf ;"
               "sed --in-place 's/          / /'g perf;"
               "sed --in-place 's/        / /'g perf; date) "
               ">> {}/perftrace 2>>{}/perftrace2",
               FLAGS_profiler_tmp_dir,
               getpid(),
               FLAGS_profiler_perf_flags,
               FLAGS_profiler_tmp_dir,
               FLAGS_profiler_tmp_dir)
               .c_str()); // NOLINT
    if (shouldSaveResult_) {
      copyToResult();
    }
  });

  cpuAtSampleStart_ = cpuSeconds();
  sampleStartTime_ = nowSeconds();
  return thread;
}

bool Profiler::interruptibleSleep(int32_t seconds) {
  sleepPromise_ = folly::Promise<bool>();

  folly::SemiFuture<bool> sleepFuture(false);
  {
    std::lock_guard<std::mutex> l(profileMutex_);
    isSleeping_ = true;
    sleepPromise_ = folly::Promise<bool>();
    sleepFuture = sleepPromise_.getSemiFuture();
  }
  if (!shouldStop_) {
    try {
      auto& executor = folly::QueuedImmediateExecutor::instance();
      std::move(sleepFuture)
          .via(&executor)
          .wait((std::chrono::seconds(seconds)));
    } catch (std::exception& e) {
    }
  }
  {
    std::lock_guard<std::mutex> l(profileMutex_);
    isSleeping_ = false;
  }

  return shouldStop_;
}

void Profiler::stopSample(std::thread systemThread) {
  LOG(INFO) << "PROFILE: Signalling perf";

  system("killall -2 perf");
  systemThread.join();

  sampleStartTime_ = 0;
}

void Profiler::threadFunction() {
  makeProfileDir(resultPath_);
  cpuAtLastCheck_ = cpuSeconds();
  std::thread sampleThread;
  for (int32_t counter = 0;; ++counter) {
    if (FLAGS_profiler_min_cpu_pct == 0) {
      sampleThread = startSample();
      // First two times sleep for one interval and then five intervals.
      if (interruptibleSleep(
              FLAGS_profiler_check_interval_seconds * (counter < 2 ? 1 : 5))) {
        break;
      }
      stopSample(std::move(sampleThread)); // NOLINT
    } else {
      int64_t now = nowSeconds();
      int64_t cpuNow = cpuSeconds();
      int64_t lastPct = counter == 0 ? 0
                                     : (100 * (cpuNow - cpuAtLastCheck_) /
                                        FLAGS_profiler_check_interval_seconds);
      if (sampleStartTime_ != 0) {
        if (now - sampleStartTime_ > FLAGS_profiler_max_sample_seconds) {
          shouldSaveResult_ = true;
          stopSample(std::move(sampleThread)); // NOLINT
        }
      }
      if (lastPct > FLAGS_profiler_min_cpu_pct) {
        if (sampleStartTime_ == 0) {
          sampleThread = startSample();
        }
      } else {
        if (sampleStartTime_ != 0) {
          shouldSaveResult_ =
              now - sampleStartTime_ >= FLAGS_profiler_min_sample_seconds;
          stopSample(std::move(sampleThread)); // NOLINT
        }
      }
      cpuAtLastCheck_ = cpuNow;
      if (interruptibleSleep(FLAGS_profiler_check_interval_seconds)) {
        break;
      }
    }
  }
  if (sampleStartTime_ != 0) {
    auto now = nowSeconds();
    shouldSaveResult_ =
        now - sampleStartTime_ >= FLAGS_profiler_min_sample_seconds;
    stopSample(std::move(sampleThread)); // NOLINT
  }
}

bool Profiler::isRunning() {
  std::lock_guard<std::mutex> l(profileMutex_);
  return profileStarted_;
}

void Profiler::start(const std::string& path) {
  {
#if !defined(linux)
    VELOX_FAIL("Profiler is only available for Linux");
#endif
    resultPath_ = path;
    std::lock_guard<std::mutex> l(profileMutex_);
    if (profileStarted_) {
      return;
    }
    profileStarted_ = true;
  }
  checkSafe(FLAGS_profiler_tmp_dir);
  checkSafe(FLAGS_profiler_perf_flags);
  char temp[1000] = {};
  gethostname(temp, sizeof(temp) - 1);
  hostname = std::string(temp);
  fileSystem_ = velox::filesystems::getFileSystem(path, nullptr);
  if (!fileSystem_) {
    LOG(ERROR) << "PROFILE: Failed to find file system for " << path
               << ". Profiler not started.";
    return;
  }
  makeProfileDir(path);
  atexit(Profiler::stop);
  LOG(INFO) << "PROFILE: Starting profiling to " << path;
  profileThread_ = std::thread([]() { threadFunction(); });
}

void Profiler::stop() {
  {
    std::lock_guard<std::mutex> l(profileMutex_);
    shouldStop_ = true;
    if (!profileStarted_) {
      return;
    }
    if (isSleeping_) {
      sleepPromise_.setValue(true);
    }
  }
  profileThread_.join();
  {
    std::lock_guard<std::mutex> l(profileMutex_);
    profileStarted_ = false;
  }
  LOG(INFO) << "Stopped profiling";
}

} // namespace facebook::velox::process
