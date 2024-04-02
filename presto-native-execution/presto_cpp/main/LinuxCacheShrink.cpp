/*
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
#include "presto_cpp/main/LinuxCacheShrink.h"
#include <folly/File.h>
#include <folly/FileUtil.h>
#include <sys/sysinfo.h>
#include <sys/types.h>
#include <cinttypes>

namespace facebook::presto {

static constexpr size_t kDefaultShrinkSize{20'000'000'000};
static constexpr size_t kDefaultInterval{100'000};

bool LinuxCacheShrink::shrinkConditionFunc() {
  static long long containerMemoryLimit = 0;
  struct sysinfo memInfo;
  long long inUseMemory;
  unsigned long long minReserveGB = 5;

  sysinfo(&memInfo);

  if (!containerMemoryLimit) {
    auto containerMemoryLimitFile =
        folly::File("/sys/fs/cgroup/memory/memory.limit_in_bytes", O_RDONLY);
    // Enough storage for the container memory limit
    std::array<char, 50> buf;
    if (!folly::readNoInt(
            containerMemoryLimitFile.fd(), buf.data(), buf.size())) {
      LOG(INFO)
          << "/sys/fs/cgroup/memory/memory.limit_in_bytes not found or is empty. Using system limit";
      containerMemoryLimit = memInfo.totalram;
    } else if (sscanf(buf.data(), "%" SCNu64, &containerMemoryLimit) != 1) {
      LOG(INFO) << "No integer value read. Using system limit";
      containerMemoryLimit = memInfo.totalram;
    }
    LOG(INFO) << "System memory limit: " << containerMemoryLimit;
  }
  inUseMemory = (memInfo.totalram - memInfo.freeram) * memInfo.mem_unit;
  return ((containerMemoryLimit - inUseMemory) < (minReserveGB << 30));
}

unsigned long long LinuxCacheShrink::shrinkAmount() {
  return kDefaultShrinkSize;
}

size_t LinuxCacheShrink::interval() {
  return kDefaultInterval;
}
} // namespace facebook::presto
