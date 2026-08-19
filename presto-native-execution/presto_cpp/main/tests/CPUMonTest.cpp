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
#include "presto_cpp/main/CPUMon.h"
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <memory>
#include "velox/common/testutil/TempFilePath.h"

namespace facebook::presto {
namespace {

using velox::common::testutil::TempFilePath;

class CPUMonTest : public testing::Test {
 protected:
  // Writes 'content' to a temporary file kept alive for the whole test, and
  // returns its path. Used to stand in for the cgroup pseudo-files.
  std::string writeFile(const std::string& content) {
    auto file = TempFilePath::create();
    file->append(content);
    files_.push_back(file);
    return file->getPath();
  }

  void useCgroupV2(const std::string& cpuStat, const std::string& cpuMax) {
    mon_.setCgroupFilesForTest(
        /*useV2=*/true, writeFile(cpuStat), writeFile(cpuMax));
  }

  void useCgroupV1(
      const std::string& cpuacctUsage,
      const std::string& cfsQuota,
      const std::string& cfsPeriod) {
    mon_.setCgroupFilesForTest(
        /*useV2=*/false,
        writeFile(cpuacctUsage),
        writeFile(cfsQuota),
        writeFile(cfsPeriod));
  }

  CPUMon mon_;
  std::vector<std::shared_ptr<TempFilePath>> files_;

  // A real 'cpu.stat'. The keys are deliberately not in the order we scan for.
  const std::string kCpuStat_ =
      "usage_usec 5000000\n"
      "user_usec 4000000\n"
      "system_usec 1000000\n"
      "nr_periods 100\n"
      "nr_throttled 3\n"
      "throttled_usec 12345\n";

  // One second, in microseconds - the interval CPUMon::update() runs at.
  static constexpr int64_t kOneSecond = 1'000'000;
};

// ---------------------------- quota parsing ----------------------------

TEST_F(CPUMonTest, cgroupV2Quota) {
  // 'cpu.max' is '<quota> <period>' in microseconds: 10 cores.
  useCgroupV2(kCpuStat_, "1000000 100000\n");
  ASSERT_DOUBLE_EQ(mon_.readCgroupCpuQuotaCores(), 10.0);
}

TEST_F(CPUMonTest, cgroupV2FractionalQuota) {
  // A Kubernetes '500m' limit is half a core, and must not round to zero - that
  // would be read as 'no limit' and fall back to the host-wide load.
  useCgroupV2(kCpuStat_, "50000 100000\n");
  ASSERT_DOUBLE_EQ(mon_.readCgroupCpuQuotaCores(), 0.5);
}

TEST_F(CPUMonTest, cgroupV2NoQuota) {
  // A quota of the literal 'max' means no CPU limit is set.
  useCgroupV2(kCpuStat_, "max 100000\n");
  ASSERT_EQ(mon_.readCgroupCpuQuotaCores(), 0);
}

TEST_F(CPUMonTest, cgroupV2MalformedQuota) {
  for (const auto& content : {"garbage\n", "1000000\n", "\n", "a b\n"}) {
    SCOPED_TRACE(content);
    useCgroupV2(kCpuStat_, content);
    ASSERT_EQ(mon_.readCgroupCpuQuotaCores(), 0);
  }
}

TEST_F(CPUMonTest, cgroupV1Quota) {
  useCgroupV1("5000000000\n", "1000000\n", "100000\n");
  ASSERT_DOUBLE_EQ(mon_.readCgroupCpuQuotaCores(), 10.0);
}

TEST_F(CPUMonTest, cgroupV1NoQuota) {
  // cgroup v1 uses -1 for 'no limit set'.
  useCgroupV1("5000000000\n", "-1\n", "100000\n");
  ASSERT_EQ(mon_.readCgroupCpuQuotaCores(), 0);
}

TEST_F(CPUMonTest, quotaUnavailable) {
  // No cgroup accounting was found, so nothing can be read.
  mon_.setCgroupFilesForTest(/*useV2=*/true, "", "");
  ASSERT_EQ(mon_.readCgroupCpuQuotaCores(), 0);
  ASSERT_EQ(mon_.readCgroupCpuUsageUsec(), -1);
}

// ---------------------------- usage parsing ----------------------------

TEST_F(CPUMonTest, cgroupV2Usage) {
  useCgroupV2(kCpuStat_, "1000000 100000\n");
  ASSERT_EQ(mon_.readCgroupCpuUsageUsec(), 5'000'000);
}

TEST_F(CPUMonTest, cgroupV2UsageKeyMissing) {
  // 'user_usec' is not 'usage_usec', and a prefix match must not accept it.
  useCgroupV2("user_usec 4000000\nnr_periods 100\n", "1000000 100000\n");
  ASSERT_EQ(mon_.readCgroupCpuUsageUsec(), -1);
}

TEST_F(CPUMonTest, cgroupV1Usage) {
  // 'cpuacct.usage' is a single value in nanoseconds.
  useCgroupV1("5000000000\n", "1000000\n", "100000\n");
  ASSERT_EQ(mon_.readCgroupCpuUsageUsec(), 5'000'000);
}

TEST_F(CPUMonTest, cgroupV1UsageMalformed) {
  useCgroupV1("not a number\n", "1000000\n", "100000\n");
  ASSERT_EQ(mon_.readCgroupCpuUsageUsec(), -1);
}

// ------------------------- load percentage math -------------------------

TEST_F(CPUMonTest, loadPctAcrossQuota) {
  // 10 cores for one second is 10s of CPU time available.
  struct {
    int64_t usedUsec;
    double expectedPct;
  } testCases[] = {
      {0, 0.0},
      {2'500'000, 25.0},
      {5'000'000, 50.0},
      {10'000'000, 100.0},
      // CFS throttling is applied per period rather than instantaneously, so a
      // cgroup can slightly exceed its quota within one window.
      {12'000'000, 100.0},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(fmt::format("usedUsec: {}", testCase.usedUsec));
    ASSERT_DOUBLE_EQ(
        CPUMon::computeCgroupCpuLoadPct(
            /*prevUsageUsec=*/0,
            /*usageUsec=*/testCase.usedUsec,
            /*prevElapsedUsec=*/0,
            /*elapsedUsec=*/kOneSecond,
            /*quotaCores=*/10.0),
        testCase.expectedPct);
  }
}

TEST_F(CPUMonTest, loadPctFractionalQuota) {
  // Half a core for one second: 250ms of CPU time is half of what is allowed.
  ASSERT_DOUBLE_EQ(
      CPUMon::computeCgroupCpuLoadPct(0, 250'000, 0, kOneSecond, 0.5), 50.0);
}

TEST_F(CPUMonTest, loadPctUsesTheDeltaNotTheTotal) {
  // The counters are cumulative since the cgroup was created, so only what was
  // consumed within the window counts.
  ASSERT_DOUBLE_EQ(
      CPUMon::computeCgroupCpuLoadPct(
          /*prevUsageUsec=*/900'000'000,
          /*usageUsec=*/905'000'000,
          /*prevElapsedUsec=*/50'000'000,
          /*elapsedUsec=*/50'000'000 + kOneSecond,
          /*quotaCores=*/10.0),
      50.0);
}

TEST_F(CPUMonTest, loadPctUnknown) {
  // Every case where no meaningful percentage exists reports -1, which
  // update() turns into either the host-wide load or idle.
  struct {
    const char* name;
    int64_t prevUsageUsec;
    int64_t usageUsec;
    int64_t prevElapsedUsec;
    int64_t elapsedUsec;
    double quotaCores;
  } testCases[] = {
      {"no quota", 0, 5'000'000, 0, kOneSecond, 0.0},
      {"negative quota", 0, 5'000'000, 0, kOneSecond, -1.0},
      {"first sample", -1, 5'000'000, -1, kOneSecond, 10.0},
      {"usage unreadable", 0, -1, 0, kOneSecond, 10.0},
      {"no time elapsed", 0, 5'000'000, kOneSecond, kOneSecond, 10.0},
      {"clock went backwards", 0, 5'000'000, kOneSecond, 0, 10.0},
      // A counter that goes backwards means the cgroup was replaced underneath
      // us; wait for the next window rather than report a spike.
      {"counter reset", 5'000'000, 1'000'000, 0, kOneSecond, 10.0},
  };

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.name);
    ASSERT_EQ(
        CPUMon::computeCgroupCpuLoadPct(
            testCase.prevUsageUsec,
            testCase.usageUsec,
            testCase.prevElapsedUsec,
            testCase.elapsedUsec,
            testCase.quotaCores),
        -1);
  }
}

// --------------------- cgroup v2 path from /proc/self ---------------------

TEST_F(CPUMonTest, cgroupV2PathPrivateNamespace) {
  // A container started with 'cgroupns=private' sees its own cgroup as the
  // root, so the files at the mount root are already the right ones.
  ASSERT_EQ(CPUMon::parseCgroupRelativePath("0::/\n", ""), "");
}

TEST_F(CPUMonTest, cgroupV2PathHostNamespace) {
  // With 'cgroupns=host' the mount root describes the machine, and the cgroup
  // of this process has to be appended to reach its own accounting.
  const std::string path =
      "/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod123.slice"
      "/cri-containerd-abc.scope";
  ASSERT_EQ(CPUMon::parseCgroupRelativePath("0::" + path + "\n", ""), path);
}

TEST_F(CPUMonTest, cgroupV2PathAmongV1Entries) {
  // On a host running cgroup v1, or in the hybrid layout, only the entry with
  // an empty controller list is the v2 one.
  ASSERT_EQ(
      CPUMon::parseCgroupRelativePath(
          "12:cpu,cpuacct:/kubepods/pod123\n"
          "11:memory:/kubepods/pod123\n"
          "0::/kubepods/pod123\n",
          ""),
      "/kubepods/pod123");
  ASSERT_EQ(
      CPUMon::parseCgroupRelativePath(
          "12:cpu,cpuacct:/kubepods/pod123\n11:memory:/kubepods/pod123\n", ""),
      "");
}

TEST_F(CPUMonTest, cgroupV2PathWithColon) {
  // A cgroup name may contain a colon, so the path is everything after the
  // second separator rather than the third field of a split.
  ASSERT_EQ(
      CPUMon::parseCgroupRelativePath("0::/machine.slice/unit:name\n", ""),
      "/machine.slice/unit:name");
}

TEST_F(CPUMonTest, cgroupV2PathMissing) {
  for (const auto& content : {"", "\n", "garbage\n", "0:cpu\n"}) {
    SCOPED_TRACE(content);
    ASSERT_EQ(CPUMon::parseCgroupRelativePath(content, ""), "");
  }
}

// --------------------- cgroup v1 path from /proc/self ---------------------

// A realistic '/proc/self/cgroup' on a cgroup v1 host, where 'cpu' and
// 'cpuacct' are mounted together and the process sits in a nested cgroup.
const char* kProcSelfCgroupV1 =
    "11:memory:/kubepods/burstable/pod123\n"
    "10:cpu,cpuacct:/kubepods/burstable/pod123/abc\n"
    "9:cpuset:/kubepods/burstable/pod123\n";

TEST_F(CPUMonTest, cgroupV1PathPerController) {
  // Both controller names must resolve through the shared 'cpu,cpuacct' entry.
  ASSERT_EQ(
      CPUMon::parseCgroupRelativePath(kProcSelfCgroupV1, "cpu"),
      "/kubepods/burstable/pod123/abc");
  ASSERT_EQ(
      CPUMon::parseCgroupRelativePath(kProcSelfCgroupV1, "cpuacct"),
      "/kubepods/burstable/pod123/abc");
}

TEST_F(CPUMonTest, cgroupV1PathSeparateMounts) {
  // 'cpu' and 'cpuacct' can be mounted separately, and then they may sit at
  // different paths, so each has to be resolved against its own entry.
  const char* procSelf =
      "10:cpuacct:/kubepods/pod123/acct\n"
      "9:cpu:/kubepods/pod123/quota\n";
  ASSERT_EQ(
      CPUMon::parseCgroupRelativePath(procSelf, "cpuacct"),
      "/kubepods/pod123/acct");
  ASSERT_EQ(
      CPUMon::parseCgroupRelativePath(procSelf, "cpu"),
      "/kubepods/pod123/quota");
}

TEST_F(CPUMonTest, cgroupV1PathControllerNotPresent) {
  // A controller the process is not in must not fall through to another entry.
  ASSERT_EQ(CPUMon::parseCgroupRelativePath(kProcSelfCgroupV1, "blkio"), "");
  // 'cpu' must not match 'cpuset' by prefix, nor the v2 entry.
  ASSERT_EQ(
      CPUMon::parseCgroupRelativePath("9:cpuset:/kubepods/pod123\n", "cpu"),
      "");
  ASSERT_EQ(
      CPUMon::parseCgroupRelativePath("0::/kubepods/pod123\n", "cpu"), "");
}

TEST_F(CPUMonTest, cgroupV1PathPrivateNamespace) {
  // As with v2, a container that sees its own cgroup as the root reports '/'
  // and there is nothing to append.
  ASSERT_EQ(CPUMon::parseCgroupRelativePath("10:cpu,cpuacct:/\n", "cpu"), "");
}

} // namespace
} // namespace facebook::presto
