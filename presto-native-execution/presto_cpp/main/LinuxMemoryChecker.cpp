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
#include "presto_cpp/main/common/Utils.h"

namespace facebook::presto {

using int128_t = __int128_t;

class LinuxMemoryChecker : public PeriodicMemoryChecker {
 public:
  explicit LinuxMemoryChecker(const PeriodicMemoryChecker::Config& config)
      : PeriodicMemoryChecker(config) {
    // Find out what cgroup version (v1 or v2) we have based on the directory
    // it's mounted.
    struct stat buffer;
    if ((stat(kCgroupV1Path, &buffer) == 0)) {
      PRESTO_STARTUP_LOG(INFO) << "Using cgroup v1.";
      if (stat(kCgroupV1MemStatFile, &buffer) == 0) {
        memStatFile_ = kCgroupV1MemStatFile;
      }
      if ((stat(kCgroupV1MaxMemFile, &buffer) == 0)) {
        memMaxFile_ = kCgroupV1MaxMemFile;
      }
    }

    // In cgroup v2.
    else {
      PRESTO_STARTUP_LOG(INFO) << "Using cgroup v2.";
      if (stat(kCgroupV2MemStatFile, &buffer) == 0) {
        memStatFile_ = kCgroupV2MemStatFile;
      }
      if ((stat(kCgroupV2MaxMemFile, &buffer) == 0)) {
        memMaxFile_ = kCgroupV2MaxMemFile;
      }
    }

    PRESTO_STARTUP_LOG(INFO) << fmt::format(
        "Using memory stat file: {}",
        memStatFile_.empty() ? memInfoFile_ : memStatFile_);
    PRESTO_STARTUP_LOG(INFO) << fmt::format(
        "Using memory max file {}",
        memMaxFile_.empty() ? memInfoFile_ : memMaxFile_);
  }

  ~LinuxMemoryChecker() override {}

  int64_t getUsedMemory() {
    return systemUsedMemoryBytes();
  }

  void setStatFile(std::string statFile) {
    memStatFile_ = statFile;
    LOG(INFO) << fmt::format(
        "Changed to using memory stat file {}", memStatFile_);
  }

  // This function is used for testing.
  void setMemMaxFile(const std::string& memMaxFile) {
    memMaxFile_ = memMaxFile;
  }

  // This function is used for testing.
  void setMemInfoFile(const std::string& memInfoFile) {
    memInfoFile_ = memInfoFile;
  }

  void start() override {
    // Check system-memory-gb < system-mem-limit-gb < available machine memory
    // of deployment.
    auto* systemConfig = SystemConfig::instance();
    int64_t systemMemoryInBytes =
        static_cast<int64_t>(systemConfig->systemMemoryGb()) << 30;
    PRESTO_STARTUP_LOG(INFO)
        << fmt::format("System memory in bytes: {}", systemMemoryInBytes);

    PRESTO_STARTUP_LOG(INFO) << fmt::format(
        "System memory limit in bytes: {}", config_.systemMemLimitBytes);

    auto availableMemoryOfDeployment = getAvailableMemoryOfDeployment();
    PRESTO_STARTUP_LOG(INFO) << fmt::format(
        "Available machine memory of deployment in bytes: {}",
        availableMemoryOfDeployment);

    VELOX_CHECK_LE(
        config_.systemMemLimitBytes,
        availableMemoryOfDeployment,
        "system memory limit = {} bytes is higher than the available machine memory of deployment = {} bytes.",
        config_.systemMemLimitBytes,
        availableMemoryOfDeployment);

    if (config_.systemMemLimitBytes < systemMemoryInBytes) {
      LOG(WARNING) << "system-mem-limit-gb is smaller than system-memory-gb. "
                   << "Expected: system-mem-limit-gb >= system-memory-gb.";
    }

    PeriodicMemoryChecker::start();
  }

  int128_t getAvailableMemoryOfDeployment() {
    // Set the available machine memory of deployment to be the smaller number
    // between /proc/meminfo and memMaxFile_.
    int128_t availableMemoryOfDeployment = 0;
    // meminfo's units is in kB.
    folly::gen::byLine(memInfoFile_.c_str()) |
        [&](const folly::StringPiece& line) -> void {
      if (availableMemoryOfDeployment != 0) {
        return;
      }
      availableMemoryOfDeployment = static_cast<int128_t>(
          extractNumericConfigValueWithRegex(line, kMemTotalRegex) * 1024);
    };

    // For cgroup v1, memory.limit_in_bytes can default to a really big numeric
    // value in bytes like 9223372036854771712 to represent that
    // memory.limit_in_bytes is not set to a value. The default value here is
    // set to PAGE_COUNTER_MAX, which is LONG_MAX/PAGE_SIZE on the 64-bit
    // platform. The default value can vary based upon the platform's PAGE_SIZE.
    // If memory.limit_in_bytes contains a really big numeric value, then we
    // will use MemTotal from /proc/meminfo.

    // For cgroup v2, memory.max can contain a numeric value in bytes or string
    // "max" which represents no value has been set. If memory.max contains
    // "max", then we will use MemTotal from /proc/meminfo.
    if (!memMaxFile_.empty()) {
      folly::gen::byLine(memMaxFile_.c_str()) |
          [&](const folly::StringPiece& line) -> void {
        if (line == "max") {
          return;
        }
        availableMemoryOfDeployment =
            std::min(availableMemoryOfDeployment, folly::to<int128_t>(line));
        return;
      };
    }

    // Unit is in bytes.
    return availableMemoryOfDeployment;
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

    if (!memStatFile_.empty()) {
      folly::gen::byLine(memStatFile_.c_str()) |
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
      const auto memBytes = inactiveAnon + activeAnon;
      cachedSystemUsedMemoryBytes_ = memBytes;
      return memBytes;
    }

    // Last resort use host machine info.
    folly::gen::byLine(memInfoFile_.c_str()) |
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
    const auto memBytes =
        (memAvailable && memTotal) ? memTotal - memAvailable : 0;
    cachedSystemUsedMemoryBytes_ = memBytes;
    return memBytes;
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
  const boost::regex kMemTotalRegex{R"!(MemTotal:\s*(\d+)\s+kB)!"};
  const char* kCgroupV1Path = "/sys/fs/cgroup/memory";
  const char* kCgroupV1MemStatFile = "/sys/fs/cgroup/memory/memory.stat";
  const char* kCgroupV2MemStatFile = "/sys/fs/cgroup/memory.stat";
  const char* kCgroupV1MaxMemFile =
      "/sys/fs/cgroup/memory/memory.limit_in_bytes";
  const char* kCgroupV2MaxMemFile = "/sys/fs/cgroup/memory.max";
  std::string memInfoFile_ = "/proc/meminfo";
  std::string memStatFile_;
  std::string memMaxFile_;

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

std::unique_ptr<PeriodicMemoryChecker> createMemoryChecker() {
  auto* systemConfig = SystemConfig::instance();
  if (systemConfig->systemMemPushbackEnabled()) {
    PeriodicMemoryChecker::Config config;
    config.systemMemPushbackEnabled = systemConfig->systemMemPushbackEnabled();
    config.systemMemLimitBytes =
        static_cast<uint64_t>(systemConfig->systemMemLimitGb()) << 30;
    config.systemMemShrinkBytes =
        static_cast<uint64_t>(systemConfig->systemMemShrinkGb()) << 30;
    return std::make_unique<LinuxMemoryChecker>(config);
  }
  return nullptr;
}

} // namespace facebook::presto
