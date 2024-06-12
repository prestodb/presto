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

#include <boost/regex.hpp>
#include <folly/Singleton.h>
#include <folly/gen/File.h>
#include <sys/stat.h>
#include "presto_cpp/main/PeriodicMemoryChecker.h"
#include "presto_cpp/main/common/Configs.h"

namespace facebook::presto {

class LinuxMemoryChecker : public PeriodicMemoryChecker {
 public:
  explicit LinuxMemoryChecker(const PeriodicMemoryChecker::Config& config)
      : PeriodicMemoryChecker(config) {
    // Find out what cgroup version (v1 or v2) we have based on the directory
    // it's mounted.
    struct stat buffer;
    if ((stat(kCgroupV1Path, &buffer) == 0)) {
      statFile_ = kCgroupV1Path;
    } else if ((stat(kCgroupV2Path, &buffer) == 0)) {
      statFile_ = kCgroupV2Path;
    } else {
      statFile_ = "None";
    }
    LOG(INFO) << fmt::format("Using memory stat file {}", statFile_);
  }

  ~LinuxMemoryChecker() override {}

  int64_t getUsedMemory() {
    return systemUsedMemoryBytes();
  }

  void setStatFile(std::string statFile) {
    statFile_ = statFile;
    LOG(INFO) << fmt::format("Changed to using memory stat file {}", statFile_);
  }

 protected:
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
  int64_t systemUsedMemoryBytes() override {
    size_t memAvailable = 0;
    size_t memTotal = 0;
    size_t inactiveAnon = 0;
    size_t activeAnon = 0;

    if (statFile_ != "None") {
      folly::gen::byLine(statFile_.c_str()) |
          [&](const folly::StringPiece& line) -> void {
        if (inactiveAnon == 0) {
          inactiveAnon =
              extractNumericConfigValueWithRegex(line, kInactiveAnonRegex);
        }

        if (activeAnon == 0) {
          activeAnon =
              extractNumericConfigValueWithRegex(line, kActiveAnonRegex);
        }

        if (activeAnon != 0 && inactiveAnon != 0) {
          return;
        }
      };

      // Unit is in bytes.
      return inactiveAnon + activeAnon;
    }

    // Last resort use host machine info.
    folly::gen::byLine("/proc/meminfo") |
        [&](const folly::StringPiece& line) -> void {
      if (memAvailable == 0) {
        memAvailable =
            extractNumericConfigValueWithRegex(line, kMemAvailableRegex) * 1024;
      }

      if (memTotal == 0) {
        memTotal =
            extractNumericConfigValueWithRegex(line, kMemTotalRegex) * 1024;
      }

      if (memAvailable != 0 && memTotal != 0) {
        return;
      }
    };
    // Unit is in bytes.
    return (memAvailable && memTotal) ? memTotal - memAvailable : 0;
  }

  int64_t mallocBytes() const override {
    VELOX_UNSUPPORTED();
  }

  void periodicCb() override {
    return;
  }

  bool heapDumpCb(const std::string& filePath) const override {
    VELOX_UNSUPPORTED();
  }

  void removeDumpFile(const std::string& filePath) const override {
    VELOX_UNSUPPORTED();
  }

 private:
  const boost::regex kInactiveAnonRegex{R"!(inactive_anon\s*(\d+)\s*)!"};
  const boost::regex kActiveAnonRegex{R"!(active_anon\s*(\d+)\s*)!"};
  const boost::regex kMemAvailableRegex{R"!(MemAvailable:\s*(\d+)\s*kB)!"};
  const boost::regex kMemTotalRegex{R"!(MemTotal:\s*(\d+)\s*kB)!"};
  const char* kCgroupV1Path = "/sys/fs/cgroup/memory/memory.stat";
  const char* kCgroupV2Path = "/sys/fs/cgroup/memory.stat";
  std::string statFile_;

  size_t extractNumericConfigValueWithRegex(
      const folly::StringPiece& line,
      const boost::regex& regex) {
    boost::cmatch match;
    if (boost::regex_match(line.begin(), line.end(), match, regex)) {
      if (match.size() > 1) {
        std::string numStr(match[1].str());
        return folly::to<size_t>(numStr);
      }
    }
    return 0;
  }
};

folly::Singleton<facebook::presto::PeriodicMemoryChecker> checker(
    []() -> facebook::presto::PeriodicMemoryChecker* {
      PeriodicMemoryChecker::Config config;
      auto* systemConfig = SystemConfig::instance();
      config.systemMemPushbackEnabled =
          systemConfig->systemMemPushbackEnabled();
      config.systemMemLimitBytes =
          static_cast<uint64_t>(systemConfig->systemMemLimitGb()) << 30;
      config.systemMemShrinkBytes =
          static_cast<uint64_t>(systemConfig->systemMemShrinkGb()) << 30;
      return std::make_unique<LinuxMemoryChecker>(config).release();
    });

} // namespace facebook::presto
