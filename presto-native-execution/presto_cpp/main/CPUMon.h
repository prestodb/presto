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
#pragma once

#include <folly/Range.h>
#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

namespace facebook::presto {

/// Used to keep track of the system's CPU usage.
class CPUMon {
 public:
  /// Call this periodically to update the CPU load. Not thread-safe.
  void update();

  /// Returns the latest host-wide CPU load, as a percentage of all the CPU time
  /// available on the machine. Thread-safe.
  inline double getCPULoadPct() const {
    return cpuLoadPct_.load();
  }

  /// Returns the latest CPU load of this process' cgroup, as a percentage of
  /// the CPU quota assigned to it. Thread-safe.
  ///
  /// Inside a container the host-wide load above is not a usable signal: the
  /// kernel exposes the machine's '/proc/stat' regardless of the cgroup, so a
  /// worker saturating a 10-core quota on an 80-core node never reports more
  /// than 12.5%, and the value also moves with unrelated co-tenants sharing the
  /// node. This one is derived from the cgroup's own CPU accounting, so 100%
  /// means the worker is using all the CPU it is allowed to use.
  ///
  /// Falls back to getCPULoadPct() when no CPU quota applies - bare metal, or a
  /// container without a CPU limit - where the two are equivalent.
  inline double getCgroupCPULoadPct() const {
    return cgroupCpuLoadPct_.load();
  }

  /// Computes the cgroup CPU load percentage from two samples of the cgroup's
  /// cumulative CPU usage. Returns -1 when the inputs cannot produce a
  /// meaningful value, either because a sample is missing, because no quota is
  /// known, or because the counters did not advance. Exposed for testing.
  static double computeCgroupCpuLoadPct(
      int64_t prevUsageUsec,
      int64_t usageUsec,
      int64_t prevElapsedUsec,
      int64_t elapsedUsec,
      double quotaCores);

  /// Returns the path of the current process' cgroup relative to the controller
  /// mount root, parsed out of the contents of '/proc/self/cgroup'.
  /// 'controller' names the cgroup v1 controller to look for ('cpu' or
  /// 'cpuacct'); pass an empty string for cgroup v2, whose entry has no
  /// controller list. Empty when
  /// the process is in the root cgroup - which is what a container started with
  /// 'cgroupns=private' reports, as the kernel shows it its own cgroup as the
  /// root - or when no matching entry is present. Exposed for testing.
  static std::string parseCgroupRelativePath(
      const std::string& procSelf,
      folly::StringPiece controller);

  /// Overrides the files the cgroup accounting is read from. 'periodFile' is
  /// only used for cgroup v1, where the quota and the period live in separate
  /// files. Test only.
  void setCgroupFilesForTest(
      bool useV2,
      const std::string& usageFile,
      const std::string& quotaFile,
      const std::string& periodFile = "");

  /// Reads the cgroup's cumulative CPU usage in microseconds, or -1 when it
  /// cannot be read. Exposed for testing.
  int64_t readCgroupCpuUsageUsec() const;

  /// Reads the number of cores the cgroup's CPU quota allows, or 0 when no
  /// quota applies or it cannot be read. Fractional quotas are preserved, so a
  /// 500m Kubernetes limit returns 0.5. Exposed for testing.
  double readCgroupCpuQuotaCores() const;

 private:
  /// Locates the cgroup files to read, honouring both cgroup versions and both
  /// cgroup namespace modes. Leaves the paths empty when the accounting is not
  /// available, which turns getCgroupCPULoadPct() into getCPULoadPct(). Runs on
  /// the first update() rather than in the constructor, so that what it finds
  /// can be logged - a CPUMon is built before logging is initialized.
  void detectCgroupFiles();

  /// Recomputes cgroupCpuLoadPct_. Reports 'hostLoadPct' when no CPU quota
  /// applies, where the two are the same measure, and 0 when a quota applies
  /// but this window produced no usable delta - reporting the host-wide value
  /// there would mix two scales.
  void updateCgroupCpuLoad(double hostLoadPct);

  std::vector<uint64_t> prev_{8};
  bool firstTime_{true};
  std::atomic<double> cpuLoadPct_{0.0};

  // Paths stay empty when the cgroup CPU accounting could not be located.
  bool cgroupDetected_{false};
  bool cgroupV2_{true};
  std::string cgroupUsageFile_;
  std::string cgroupQuotaFile_;
  std::string cgroupPeriodFile_;

  // Previous cgroup sample. -1 marks "no sample taken yet".
  int64_t prevCgroupUsageUsec_{-1};
  int64_t prevCgroupElapsedUsec_{-1};
  std::atomic<double> cgroupCpuLoadPct_{0.0};
};

} // namespace facebook::presto
