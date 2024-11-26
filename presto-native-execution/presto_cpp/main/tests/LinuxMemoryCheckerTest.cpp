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

namespace fs = boost::filesystem;

namespace {
std::string getFilePath(const std::string& fileName) {
  return fs::current_path().string() + "/examples/" + fileName;
}
} // namespace

namespace facebook::presto {
class LinuxMemoryCheckerTest : public testing::Test {
 protected:
  LinuxMemoryChecker memChecker_;

  LinuxMemoryCheckerTest() : memChecker_(PeriodicMemoryChecker::Config{}) {}

  void checkMemoryMax(
      const std::string& memMaxFileName,
      int64_t expectedMemoryMax) {
    auto memInfoFilePath = getFilePath("meminfo");
    memChecker_.setMemInfo(memInfoFilePath);
    auto memMaxFilePath = getFilePath(memMaxFileName);
    memChecker_.setMemMaxFile(memMaxFilePath);
    ASSERT_EQ(memChecker_.getActualTotalMemory(), expectedMemoryMax);
  }

  void checkMemoryUsage(
      const std::string& statFileName,
      int64_t expectedMemoryUsage) {
    auto statFilePath = getFilePath(statFileName);
    memChecker_.setStatFile(statFilePath);
    ASSERT_EQ(memChecker_.getUsedMemory(), expectedMemoryUsage);
  }
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
  LinuxMemoryChecker memChecker_(PeriodicMemoryChecker::Config{
      1'000, false, 0, 0, false, 5, "/path/to/dir", "prefix", 5, 512});

  ASSERT_NO_THROW(memChecker_.start());
  VELOX_ASSERT_THROW(memChecker_.start(), "start() called more than once");
  ASSERT_NO_THROW(memChecker_.stop());
}

TEST_F(LinuxMemoryCheckerTest, sysMemLimitBytesCheck) {
  // system-mem-limit-gb should be set less than or equal to
  // the actual total memory available.
  auto memInfoFilePath = getFilePath("meminfo");
  auto memMaxFilePath = getFilePath("memory131gb.max");

  // systemMemLimitBytes = 130,000,000,000 bytes.
  // actual total memory = 131,000,000,000 bytes.
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
  memChecker.setMemInfo(memInfoFilePath);
  memChecker.setMemMaxFile(memMaxFilePath);
  ASSERT_NO_THROW(memChecker.start());
  ASSERT_NO_THROW(memChecker.stop());

  // systemMemLimitBytes = 131,000,000,001 bytes.
  // actual total memory = 131,000,000,000 bytes.
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
  memChecker2.setMemInfo(memInfoFilePath);
  memChecker2.setMemMaxFile(memMaxFilePath);
  VELOX_ASSERT_THROW(memChecker2.start(), "(131000000001 vs. 131000000000)");
  VELOX_ASSERT_THROW(memChecker2.stop(), "");
}

TEST_F(LinuxMemoryCheckerTest, memory131gbMax) {
  // Testing for cgroup v1 and v2.
  // memory131gb.max is 131,000,000,000 bytes.
  // meminfo is 132,397,334,528 bytes.
  // The expected actual memory should be 131,000,000,000 bytes here.
  checkMemoryMax("memory131gb.max", 131000000000);
}

TEST_F(LinuxMemoryCheckerTest, memory133gbMax) {
  // Testing for cgroup v1 and v2.
  // memory133gb.max is 133,000,000,000 bytes.
  // meminfo is 132,397,334,528 bytes.
  // The expected actual memory should be 132,397,334,528 bytes here.
  checkMemoryMax("memory133gb.max", 132397334528);
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
  // The expected actual memory should be 132,397,334,528 bytes here.
  checkMemoryMax("cgroupV1memoryNotSet.limit_in_bytes", 132397334528);
}

TEST_F(LinuxMemoryCheckerTest, cgroupV2MemoryMaxNotSet) {
  // Testing for cgroup v2.
  // When memory.max is not set to a value, it defaults to contain string "max".

  // cgroupV2memoryNotSet.max is "max".
  // meminfo is 132,397,334,528 bytes.
  // The expected actual memory should be 132,397,334,528 bytes here.
  checkMemoryMax("cgroupV2memoryNotSet.max", 132397334528);
}

TEST_F(LinuxMemoryCheckerTest, memoryStatFileV1) {
  // Testing cgroup v1 memory.stat file.
  checkMemoryUsage("cgroupV1memory.stat", 5136384);
}

TEST_F(LinuxMemoryCheckerTest, memoryStatFileV2) {
  // Testing cgroup v2 memory.stat file.
  checkMemoryUsage("cgroupV2memory.stat", 274713448448);
}

TEST_F(LinuxMemoryCheckerTest, hostMachineInfo) {
  // Testing host machine info /proc/meminfo when None is specified for stat
  // file.
  memChecker_.setStatFile("None");
  ASSERT_GT(memChecker_.getUsedMemory(), 0);
}
} // namespace facebook::presto
