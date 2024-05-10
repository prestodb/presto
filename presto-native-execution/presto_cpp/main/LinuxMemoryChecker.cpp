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

#include "presto_cpp/main/LinuxMemoryChecker.h"
#ifdef __linux__
#include <boost/regex.hpp>
#include <folly/File.h>
#include <folly/FileUtil.h>
#include <folly/gen/Base.h>
#include <folly/gen/File.h>
#include <folly/gen/String.h>

#include <folly/Conv.h>
#include <folly/CppAttributes.h>
#include <folly/Format.h>
#include <folly/Range.h>
#include <folly/String.h>

#include <sys/stat.h>

#endif

namespace facebook::presto {

// Memory calculations from
// https://docs.docker.com/reference/cli/docker/container/stats/#:~:text=On%20
// Linux%2C%20the%20Docker%20CLI,use%20the%20data%20as%20needed
// On Linux, the Docker CLI reports memory usage by subtracting cache usage from
// the total memory usage. The API does not perform such a calculation but
// rather provides the total memory usage and the amount from the cache so that
// clients can use the data as needed. The cache usage is defined as the value
// of total_inactive_file field in the memory.stat file on cgroup v1 hosts.
//
// On Docker 19.03 and older, the cache usage was defined as the value of cache
// field. On cgroup v2 hosts, the cache usage is defined as the value of
// inactive_file field.

int64_t LinuxMemoryChecker::systemUsedMemoryBytes() {
#ifdef __linux__
  static int cgroupVersion = -1;
  size_t memAvailable = 0;
  size_t memTotal = 0;
  size_t inUseMemory = 0;
  size_t cacheMemory = 0;
  boost::cmatch match;
  std::array<char, 50> buf;

  // Find out what cgroup version we have
  if (cgroupVersion == -1) {
    struct stat buffer;
    if ((stat("/sys/fs/cgroup/memory/memory.usage_in_bytes", &buffer) == 0)) {
      cgroupVersion = 1;
    } else if ((stat("/sys/fs/cgroup/memory.current", &buffer) == 0)) {
      cgroupVersion = 2;
    } else {
      cgroupVersion = 0;
    }
    LOG(INFO) << fmt::format("Using cgroup version {}", cgroupVersion);
  }

  if (cgroupVersion == 1) {
    auto currentMemoryUsageFile =
        folly::File("/sys/fs/cgroup/memory/memory.usage_in_bytes", O_RDONLY);
    static const boost::regex cacheRegex(R"!(total_inactive_file\s*(\d+)\s*)!");

    // Read current in use memory from memory.usage_in_bytes
    if (folly::readNoInt(currentMemoryUsageFile.fd(), buf.data(), buf.size())) {
      if (sscanf(buf.data(), "%" SCNu64, &inUseMemory) == 1) {
        // Get total cached memory from memory.stat and subtract from
        // inUseMemory
        folly::gen::byLine("/sys/fs/cgroup/memory/memory.stat") |
            [&](folly::StringPiece line) -> void {
          if (boost::regex_match(line.begin(), line.end(), match, cacheRegex)) {
            folly::StringPiece numStr(
                line.begin() + match.position(1), size_t(match.length(1)));
            cacheMemory = folly::to<size_t>(numStr);
          }
        };
        return inUseMemory - cacheMemory;
      }
    }
  } else if (cgroupVersion == 2) {
    auto currentMemoryUsageFile =
        folly::File("/sys/fs/cgroup/memory.current", O_RDONLY);
    static const boost::regex cacheRegex(R"!(inactive_file\s*(\d+)\s*)!");
    if (folly::readNoInt(currentMemoryUsageFile.fd(), buf.data(), buf.size())) {
      if (sscanf(buf.data(), "%" SCNu64, &inUseMemory) == 1) {
        // Get total cached memory from memory.stat and subtract from
        // inUseMemory
        folly::gen::byLine("/sys/fs/cgroup/memory.stat") |
            [&](folly::StringPiece line) -> void {
          if (boost::regex_match(line.begin(), line.end(), match, cacheRegex)) {
            folly::StringPiece numStr(
                line.begin() + match.position(1), size_t(match.length(1)));
            cacheMemory = folly::to<size_t>(numStr);
          }
        };
        return inUseMemory - cacheMemory;
      }
    }
  }

  // Default case variables
  static const boost::regex memAvailableRegex(
      R"!(MemAvailable:\s*(\d+)\s*kB)!");
  static const boost::regex memTotalRegex(R"!(MemTotal:\s*(\d+)\s*kB)!");
  // Last resort use host machine info
  folly::gen::byLine("/proc/meminfo") | [&](folly::StringPiece line) -> void {
    if (boost::regex_match(
            line.begin(), line.end(), match, memAvailableRegex)) {
      folly::StringPiece numStr(
          line.begin() + match.position(1), size_t(match.length(1)));
      memAvailable = folly::to<size_t>(numStr) * 1024;
    }
    if (boost::regex_match(line.begin(), line.end(), match, memTotalRegex)) {
      folly::StringPiece numStr(
          line.begin() + match.position(1), size_t(match.length(1)));
      memTotal = folly::to<size_t>(numStr) * 1024;
    }
  };
  return (memAvailable && memTotal) ? memTotal - memAvailable : 0;

#else
  return 0;
#endif
}
} // namespace facebook::presto
