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

#endif // __linux__

namespace facebook::presto {

// Current memory calculation used is inactive_anon + active_anon
// Our first attempt was using memInfo memTotal - memAvailable.
// However memInfo is not containerized so we reserve this as a
// last resort.
//
// Next we tried to use what docker/kubernetes uses for their
// calculation. cgroup usage_in_bytes - total_inactive_files.
// However we found out that usage_in_bytes is a fuzz value
// and has a chance for the sync to occur after the shrink
// polling interval. This would result in double shrinks.
//
// Therefore we decided on values from the memory.stat file
// that are real time statistics. At first we tried to use
// the calculation suggested by the kernel team RSS+CACHE(+SWAP)
// However we noticed that this value was not closely related to the
// value in usage_in_bytes which is used to OOMKill. We then looked
// at all of the values in the stat file and decided that
// inactive_anon + active_anon moves closest to that of
// usage_in_bytes
//
// NOTE: We do not know if cgroup V2 memory.current is a fuzz
// value. It may be better than what we currently use. For
// consistency we will match cgroup V1 and change if
// necessary.

int64_t LinuxMemoryChecker::systemUsedMemoryBytes() {
#ifdef __linux__
  static std::string statFile;
  size_t memAvailable = 0;
  size_t memTotal = 0;
  size_t inactiveAnon = 0;
  size_t activeAnon = 0;
  boost::cmatch match;
  std::array<char, 50> buf;
  static const boost::regex inactiveAnonRegex(R"!(inactive_anon\s*(\d+)\s*)!");
  static const boost::regex activeAnonRegex(R"!(active_anon\s*(\d+)\s*)!");

  // Find out what cgroup version (v1 or v2) we have based on the directory it's
  // mounted.
  static const char* cgroupV1Path = "/sys/fs/cgroup/memory/memory.stat";
  static const char* cgroupV2Path = "/sys/fs/cgroup/memory.stat";
  if (statFile.empty()) {
    struct stat buffer;
    if ((stat(cgroupV1Path, &buffer) == 0)) {
      statFile = cgroupV1Path;
    } else if ((stat(cgroupV2Path, &buffer) == 0)) {
      statFile = cgroupV2Path;
    } else {
      statFile = "None";
    }
    LOG(INFO) << fmt::format("Using memory stat file {}", statFile);
  }

  if (statFile != "None") {
    folly::gen::byLine(statFile.c_str()) |
        [&](folly::StringPiece line) -> void {
      inactiveAnon = matchLineWithRegex(line, match, inactiveAnonRegex);
      activeAnon = matchLineWithRegex(line, match, activeAnonRegex);
    };

    // Unit is in bytes.
    return inactiveAnon + activeAnon;
  }

  // Default case variables.
  static const boost::regex memAvailableRegex(
      R"!(MemAvailable:\s*(\d+)\s*kB)!");
  static const boost::regex memTotalRegex(R"!(MemTotal:\s*(\d+)\s*kB)!");
  // Last resort use host machine info.
  folly::gen::byLine("/proc/meminfo") | [&](folly::StringPiece line) -> void {
    memAvailable = matchLineWithRegex(line, match, memAvailableRegex) * 1024;
    memTotal = matchLineWithRegex(line, match, memTotalRegex) * 1024;
  };
  // Unit is in bytes.
  return (memAvailable && memTotal) ? memTotal - memAvailable : 0;

#else
  return 0;
#endif
}

size_t LinuxMemoryChecker::matchLineWithRegex(
    folly::StringPiece& line,
    boost::smatch& match,
    const boost::regex& regex) {
  if (boost::regex_match(line.begin(), line.end(), match, regex)) {
    folly::StringPiece numStr(
        line.begin() + match.position(1), size_t(match.length(1)));
    return folly::to<size_t>(numStr);
  }
  return 0;
}
} // namespace facebook::presto
