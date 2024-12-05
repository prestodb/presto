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
std::string getStatsFilePath(const std::string& fileName) {
  return fs::current_path().string() + "/examples/" + fileName;
}
} // namespace

namespace facebook::presto {
class LinuxMemoryCheckerTest : public testing::Test {
 protected:
  LinuxMemoryChecker memChecker;

  LinuxMemoryCheckerTest() : memChecker(PeriodicMemoryChecker::Config{}) {}

  void checkMemoryUsage(
      const std::string& statFileName,
      int64_t expectedMemoryUsage) {
    auto statFilePath = getStatsFilePath(statFileName);
    memChecker.setStatFile(statFilePath);
    ASSERT_EQ(memChecker.getUsedMemory(), expectedMemoryUsage);
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
  LinuxMemoryChecker memChecker(PeriodicMemoryChecker::Config{
      1'000, false, 0, 0, false, 5, "/path/to/dir", "prefix", 5, 512});

  ASSERT_NO_THROW(memChecker.start());
  VELOX_ASSERT_THROW(memChecker.start(), "start() called more than once");
  ASSERT_NO_THROW(memChecker.stop());
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
  memChecker.setStatFile("None");
  ASSERT_GT(memChecker.getUsedMemory(), 0);
}
} // namespace facebook::presto
