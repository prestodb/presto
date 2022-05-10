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

#include "velox/common/process/ProcessBase.h"

#include <limits.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include <folly/CpuId.h>
#include <folly/FileUtil.h>
#include <folly/String.h>
#include <gflags/gflags.h>

constexpr const char* kProcSelfCmdline = "/proc/self/cmdline";

DECLARE_bool(avx2); // Enables use of AVX2 when available NOLINT

DECLARE_bool(bmi2); // Enables use of BMI2 when available NOLINT

namespace facebook {
namespace velox {
namespace process {

using namespace std;

/**
 * Current executable's name.
 */
string getAppName() {
  const char* result = getenv("_");
  if (result) {
    return result;
  }

  // if we're running under gtest, getenv will return null
  std::string appName;
  if (folly::readFile(kProcSelfCmdline, appName)) {
    auto pos = appName.find('\0');
    if (pos != std::string::npos) {
      appName = appName.substr(0, pos);
    }

    return appName;
  }

  return "";
}

/**
 * This machine's name.
 */
string getHostName() {
  char hostbuf[_POSIX_HOST_NAME_MAX + 1];
  if (gethostname(hostbuf, _POSIX_HOST_NAME_MAX + 1) < 0) {
    return "";
  } else {
    // When the host name is precisely HOST_NAME_MAX bytes long, gethostname
    // returns 0 even though the result is not NUL-terminated. Manually NUL-
    // terminate to handle that case.
    hostbuf[_POSIX_HOST_NAME_MAX] = '\0';
    return hostbuf;
  }
}

/**
 * Process identifier.
 */
pid_t getProcessId() {
  return getpid();
}

/**
 * Current thread's identifier.
 */
pthread_t getThreadId() {
  return pthread_self();
}

/**
 * Get current working directory.
 */
string getCurrentDirectory() {
  char buf[PATH_MAX];
  return getcwd(buf, PATH_MAX);
}

uint64_t threadCpuNanos() {
  timespec ts;
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
  return ts.tv_sec * 1'000'000'000 + ts.tv_nsec;
}

namespace {
bool bmi2CpuFlag = folly::CpuId().bmi2();
bool avx2CpuFlag = folly::CpuId().avx2();
} // namespace

bool hasAvx2() {
#ifdef __AVX2__
  return avx2CpuFlag && FLAGS_avx2;
#else
  return false;
#endif
}

bool hasBmi2() {
#ifdef __BMI2__
  return bmi2CpuFlag && FLAGS_bmi2;
#else
  return false;
#endif
}

} // namespace process
} // namespace velox
} // namespace facebook
