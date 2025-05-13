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
#include "presto_cpp/main/LinuxMemoryChecker.cpp"
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <gtest/gtest.h>
#include "velox/common/base/VeloxException.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/TempFilePath.h"

using namespace facebook::velox;

namespace facebook::presto {
class LinuxMemoryCheckerTest : public testing::Test {
 protected:
  LinuxMemoryChecker memChecker_;

  LinuxMemoryCheckerTest() : memChecker_(PeriodicMemoryChecker::Config{}) {}

  void checkAvailableMemoryOfDeployment(
      const std::string& content,
      int64_t expectedMemoryMax) {
    auto tempMemInfoFile = exec::test::TempFilePath::create();
    tempMemInfoFile->append(kMemInfoText_);
    auto memInfoPath = tempMemInfoFile->getPath();

    auto tempMemMaxFile = exec::test::TempFilePath::create();
    tempMemMaxFile->append(content);
    auto tempMemMaxFilePath = tempMemMaxFile->getPath();

    memChecker_.setMemInfoFile(memInfoPath);
    memChecker_.setMemMaxFile(tempMemMaxFilePath);
    ASSERT_EQ(memChecker_.getAvailableMemoryOfDeployment(), expectedMemoryMax);
  }

  void checkMemoryUsage(
      const std::string& content,
      int64_t expectedMemoryUsage) {
    auto tempTestFile = exec::test::TempFilePath::create();
    tempTestFile->append(content);
    auto testFilePath = tempTestFile->getPath();

    memChecker_.setStatFile(testFilePath);
    ASSERT_EQ(memChecker_.getUsedMemory(), expectedMemoryUsage);
  }

  const std::string kMemInfoText_ =
      "MemTotal:       129294272 kB\n"
      "MemFree:        127334232 kB\n"
      "MemAvailable:   127637400 kB\n"
      "Buffers:            2948 kB\n"
      "Cached:          1315676 kB\n"
      "SwapCached:            0 kB\n"
      "Active:           769056 kB\n"
      "Inactive:         810360 kB\n"
      "Active(anon):     277920 kB\n"
      "Inactive(anon):        0 kB\n"
      "Active(file):     491136 kB\n"
      "Inactive(file):   810360 kB\n"
      "Unevictable:          12 kB\n"
      "Mlocked:              12 kB\n"
      "SwapTotal:             0 kB\n"
      "SwapFree:              0 kB\n"
      "Zswap:                 0 kB\n"
      "Zswapped:              0 kB\n"
      "Dirty:                 4 kB\n"
      "Writeback:             0 kB\n"
      "AnonPages:        257672 kB\n"
      "Mapped:           341044 kB\n"
      "Shmem:             17128 kB\n"
      "KReclaimable:      70060 kB\n"
      "Slab:             172876 kB\n"
      "SReclaimable:      70060 kB\n"
      "SUnreclaim:       102816 kB\n"
      "KernelStack:        6640 kB\n"
      "PageTables:         5832 kB\n"
      "SecPageTables:         0 kB\n"
      "NFS_Unstable:          0 kB\n"
      "Bounce:                0 kB\n"
      "WritebackTmp:          0 kB\n"
      "CommitLimit:    64647136 kB\n"
      "Committed_AS:   1077692288 kB\n"
      "VmallocTotal:   34359738367 kB\n"
      "VmallocUsed:       22332 kB\n"
      "VmallocChunk:          0 kB\n"
      "Percpu:             7616 kB\n"
      "HardwareCorrupted:     0 kB\n"
      "AnonHugePages:    141312 kB\n"
      "ShmemHugePages:        0 kB\n"
      "ShmemPmdMapped:        0 kB\n"
      "FileHugePages:      2048 kB\n"
      "FilePmdMapped:         0 kB\n"
      "CmaTotal:              0 kB\n"
      "CmaFree:               0 kB\n"
      "Unaccepted:            0 kB\n"
      "HugePages_Total:       0\n"
      "HugePages_Free:        0\n"
      "HugePages_Rsvd:        0\n"
      "HugePages_Surp:        0\n"
      "Hugepagesize:       2048 kB\n"
      "Hugetlb:               0 kB\n"
      "DirectMap4k:      264100 kB\n"
      "DirectMap2M:     9156608 kB\n"
      "DirectMap1G:    122683392 kB\n";
};

TEST_F(LinuxMemoryCheckerTest, basic) {
  // Default config.
  ASSERT_NO_THROW(LinuxMemoryChecker(PeriodicMemoryChecker::Config{}));

  ASSERT_NO_THROW(LinuxMemoryChecker(PeriodicMemoryChecker::Config{
      1'000, true, 1024, 32, true, 5, "/path/to/dir", "prefix", 5, 512}));
  VELOX_ASSERT_THROW(
      LinuxMemoryChecker(PeriodicMemoryChecker::Config{
          1'000, true, 0, 32, true, 5, "/path/to/dir", "prefix", 5, 512}),
      "(0 vs. 0)");
  VELOX_ASSERT_THROW(
      LinuxMemoryChecker(PeriodicMemoryChecker::Config{
          1'000, true, 1024, 32, true, 5, "", "prefix", 5, 512}),
      "heapDumpLogDir cannot be empty when heap dump is enabled.");
  VELOX_ASSERT_THROW(
      LinuxMemoryChecker(PeriodicMemoryChecker::Config{
          1'000, true, 1024, 32, true, 5, "/path/to/dir", "", 5, 512}),
      "heapDumpFilePrefix cannot be empty when heap dump is enabled.");
  LinuxMemoryChecker memChecker(PeriodicMemoryChecker::Config{
      1'000, false, 0, 0, false, 5, "/path/to/dir", "prefix", 5, 512});

  ASSERT_NO_THROW(memChecker.start());
  VELOX_ASSERT_THROW(memChecker.start(), "start() called more than once");
  ASSERT_NO_THROW(memChecker.stop());
}

TEST_F(LinuxMemoryCheckerTest, sysMemLimitBytesCheck) {
  auto tempMemInfoFile = exec::test::TempFilePath::create();
  tempMemInfoFile->append(kMemInfoText_);
  auto memInfoPath = tempMemInfoFile->getPath();

  auto tempTestFile = exec::test::TempFilePath::create();
  tempTestFile->append("131000000000\n");
  auto memMaxFilePath = tempTestFile->getPath();

  // system-mem-limit-gb should be set less than or equal to
  // the available memory of deployment.
  // systemMemLimitBytes = 130,000,000,000 bytes.
  // available memory of deployment = 131,000,000,000 bytes.
  LinuxMemoryChecker memChecker(PeriodicMemoryChecker::Config{
      1'000,
      true,
      130000000000,
      32,
      true,
      5,
      "/path/to/dir",
      "prefix",
      5,
      512});
  memChecker.setMemInfoFile(memInfoPath);
  memChecker.setMemMaxFile(memMaxFilePath);
  ASSERT_NO_THROW(memChecker.start());
  ASSERT_NO_THROW(memChecker.stop());

  // systemMemLimitBytes = 131,000,000,001 bytes.
  // available memory of deployment = 131,000,000,000 bytes.
  LinuxMemoryChecker memChecker2(PeriodicMemoryChecker::Config{
      1'000,
      true,
      131000000001,
      32,
      true,
      5,
      "/path/to/dir",
      "prefix",
      5,
      512});
  memChecker2.setMemInfoFile(memInfoPath);
  memChecker2.setMemMaxFile(memMaxFilePath);
  VELOX_ASSERT_THROW(memChecker2.start(), "(131000000001 vs. 131000000000)");
  VELOX_ASSERT_THROW(memChecker2.stop(), "");
}

TEST_F(LinuxMemoryCheckerTest, memory131gbMax) {
  // Testing for cgroup v1 and v2.
  // memory131gb.max is 131,000,000,000 bytes.
  // meminfo is 132,397,334,528 bytes.
  // The expected available memory of deployment should be 131,000,000,000 bytes
  // here.
  checkAvailableMemoryOfDeployment("131000000000\n", 131000000000);
}

TEST_F(LinuxMemoryCheckerTest, memory133gbMax) {
  // Testing for cgroup v1 and v2.
  // memory133gb.max is 133,000,000,000 bytes.
  // meminfo is 132,397,334,528 bytes.
  // The expected available memory of deployment should be 132,397,334,528 bytes
  // here.
  checkAvailableMemoryOfDeployment("133000000000\n", 132397334528);
}

TEST_F(LinuxMemoryCheckerTest, cgroupV1MemoryMaxNotSet) {
  // Testing for cgroup v1.
  // When memory.limit_in_bytes is not set to a value, it could default to
  // a huge value like 9223372036854771712 bytes.
  // The default value is set to PAGE_COUNTER_MAX, which is LONG_MAX/PAGE_SIZE
  // on 64-bit platform. The default value can vary based upon the platform's
  // PAGE_SIZE.

  // cgroupV1memoryNotSet.limit_in_bytes is 9,223,372,036,854,771,712 bytes.
  // meminfo is 132,397,334,528 bytes.
  // The expected available memory of deployment should be 132,397,334,528 bytes
  // here.
  checkAvailableMemoryOfDeployment("9223372036854771712\n", 132397334528);
}

TEST_F(LinuxMemoryCheckerTest, cgroupV2MemoryMaxNotSet) {
  // Testing for cgroup v2.
  // When memory.max is not set to a value, it defaults to contain string "max".

  // cgroupV2memoryNotSet.max is "max".
  // meminfo is 132,397,334,528 bytes.
  // The expected available memory of deployment should be 132,397,334,528 bytes
  // here.
  checkAvailableMemoryOfDeployment("max\n", 132397334528);
}

TEST_F(LinuxMemoryCheckerTest, memoryStatFileV1) {
  // Testing cgroup v1 memory.stat file.
  const std::string content =
      "cache 39313408\n"
      "rss 3600384\n"
      "rss_huge 0\n"
      "shmem 1757184\n"
      "mapped_file 12705792\n"
      "dirty 0\n"
      "writeback 0\n"
      "pgpgin 97614\n"
      "pgpgout 87091\n"
      "pgfault 55869\n"
      "pgmajfault 132\n"
      "inactive_anon 1486848\n"
      "active_anon 3649536\n"
      "inactive_file 20410368\n"
      "active_file 11894784\n"
      "unevictable 5406720\n"
      "hierarchical_memory_limit 9223372036854771712\n"
      "total_cache 239964160\n"
      "total_rss 109146112\n"
      "total_rss_huge 0\n"
      "total_shmem 2420736\n"
      "total_mapped_file 84344832\n"
      "total_dirty 135168\n"
      "total_writeback 0\n"
      "total_pgpgin 291606\n"
      "total_pgpgout 205533\n"
      "total_pgfault 296666\n"
      "total_pgmajfault 792\n"
      "total_inactive_anon 1486848\n"
      "total_active_anon 101105664\n"
      "total_inactive_file 165715968\n"
      "total_active_file 65421312\n"
      "total_unevictable 18653184\n";
  checkMemoryUsage(content, 5136384);
}

TEST_F(LinuxMemoryCheckerTest, memoryStatFileV2) {
  // Testing cgroup v2 memory.stat file.
  const std::string content =
      "anon 274528108544\n"
      "file 578768896\n"
      "kernel 565014528\n"
      "kernel_stack 9388032\n"
      "pagetables 543928320\n"
      "percpu 14040\n"
      "sock 102400\n"
      "vmalloc 86016\n"
      "shmem 0\n"
      "zswap 0\n"
      "zswapped 0\n"
      "file_mapped 0\n"
      "file_dirty 1142784\n"
      "file_writeback 0\n"
      "swapcached 0\n"
      "anon_thp 269563723776\n"
      "file_thp 0\n"
      "shmem_thp 0\n"
      "inactive_anon 274713391104\n"
      "active_anon 57344\n"
      "inactive_file 194953216\n"
      "active_file 383688704\n"
      "unevictable 0\n"
      "slab_reclaimable 3674304\n"
      "slab_unreclaimable 7674312\n"
      "slab 11348616\n"
      "workingset_refault_anon 0\n"
      "workingset_refault_file 0\n"
      "workingset_activate_anon 0\n"
      "workingset_activate_file 0\n"
      "workingset_restore_anon 0\n"
      "workingset_restore_file 0\n"
      "workingset_nodereclaim 0\n"
      "pgscan 0\n"
      "pgsteal 0\n"
      "pgscan_kswapd 0\n"
      "pgscan_direct 0\n"
      "pgsteal_kswapd 0\n"
      "pgsteal_direct 0\n"
      "pgfault 147931033\n"
      "pgmajfault 0\n"
      "pgrefill 0\n"
      "pgactivate 490211\n"
      "pgdeactivate 0\n"
      "pglazyfree 0\n"
      "pglazyfreed 0\n"
      "zswpin 0\n"
      "zswpout 0\n"
      "thp_fault_alloc 547392\n"
      "thp_collapse_alloc 0\n";
  checkMemoryUsage(content, 274713448448);
}

TEST_F(LinuxMemoryCheckerTest, hostMachineInfo) {
  // Testing host machine info /proc/meminfo for tracking current system memory
  // usage.
  ASSERT_GT(memChecker_.getUsedMemory(), 0);
}
} // namespace facebook::presto
