/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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

#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <memory>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/file/tests/FaultyFileSystem.h"
#include "velox/exec/Trace.h"
#include "velox/exec/TraceUtil.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::exec::trace::test {
class TraceUtilTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    filesystems::registerLocalFileSystem();
    tests::utils::registerFaultyFileSystem();
  }
};

TEST_F(TraceUtilTest, traceDir) {
  const auto outputDir = TempDirectoryPath::create();
  const auto rootDir = outputDir->getPath();
  const auto fs = filesystems::getFileSystem(rootDir, nullptr);
  auto dir1 = fmt::format("{}/{}", outputDir->getPath(), "t1");
  trace::createTraceDirectory(dir1);
  ASSERT_TRUE(fs->exists(dir1));

  auto dir2 = fmt::format("{}/{}", dir1, "t1_1");
  trace::createTraceDirectory(dir2);
  ASSERT_TRUE(fs->exists(dir2));

  // It will remove the old dir1 along with its subdir when created the dir1
  // again.
  trace::createTraceDirectory(dir1);
  ASSERT_TRUE(fs->exists(dir1));
  ASSERT_FALSE(fs->exists(dir2));

  const auto parentDir = fmt::format("{}/{}", outputDir->getPath(), "p");
  fs->mkdir(parentDir);

  constexpr auto numThreads = 5;
  std::vector<std::thread> traceThreads;
  traceThreads.reserve(numThreads);
  std::mutex mutex;
  std::set<std::string> expectedDirs;
  for (int i = 0; i < numThreads; ++i) {
    traceThreads.emplace_back([&, i]() {
      const auto dir = fmt::format("{}/s{}", parentDir, i);
      trace::createTraceDirectory(dir);
      std::lock_guard<std::mutex> l(mutex);
      expectedDirs.insert(dir);
    });
  }

  for (auto& traceThread : traceThreads) {
    traceThread.join();
  }

  const auto actualDirs = fs->list(parentDir);
  ASSERT_EQ(actualDirs.size(), numThreads);
  ASSERT_EQ(actualDirs.size(), expectedDirs.size());
  for (const auto& dir : actualDirs) {
    ASSERT_EQ(expectedDirs.count(dir), 1);
  }
}

TEST_F(TraceUtilTest, OperatorTraceSummary) {
  exec::trace::OperatorTraceSummary summary;
  summary.opType = "summary";
  summary.inputRows = 100;
  summary.peakMemory = 200;
  ASSERT_EQ(
      summary.toString(),
      "opType summary, inputRows 100,  inputBytes 0B, rawInputRows 0, rawInputBytes 0B, peakMemory 200B");
  summary.numSplits = 10;
  summary.rawInputBytes = 222;
  VELOX_ASSERT_THROW(summary.toString(), "summary vs. TableScan");
  summary.opType = "TableScan";
  ASSERT_EQ(
      summary.toString(),
      "opType TableScan, numSplits 10, inputRows 100, inputBytes 0B, rawInputRows 0, rawInputBytes 222B, peakMemory 200B");
}

TEST_F(TraceUtilTest, traceDirectoryLayoutUtilities) {
  const std::string traceRoot = "/traceRoot";
  const std::string queryId = "queryId";
  ASSERT_EQ(
      getQueryTraceDirectory(traceRoot, queryId),
      fmt::format("{}/{}", traceRoot, queryId));
  const std::string taskId = "taskId";
  const std::string taskTraceDir =
      getTaskTraceDirectory(traceRoot, queryId, taskId);
  ASSERT_EQ(taskTraceDir, fmt::format("{}/{}/{}", traceRoot, queryId, taskId));
  ASSERT_EQ(
      getTaskTraceMetaFilePath(
          getTaskTraceDirectory(traceRoot, queryId, taskId)),
      "/traceRoot/queryId/taskId/task_trace_meta.json");
  const std::string nodeId = "1";
  const std::string nodeTraceDir = getNodeTraceDirectory(taskTraceDir, nodeId);
  ASSERT_EQ(nodeTraceDir, "/traceRoot/queryId/taskId/1");
  const uint32_t pipelineId = 1;
  ASSERT_EQ(
      getPipelineTraceDirectory(nodeTraceDir, pipelineId),
      "/traceRoot/queryId/taskId/1/1");
  const uint32_t driverId = 1;
  const std::string opTraceDir =
      getOpTraceDirectory(taskTraceDir, nodeId, pipelineId, driverId);
  ASSERT_EQ(opTraceDir, "/traceRoot/queryId/taskId/1/1/1");
  ASSERT_EQ(
      getOpTraceDirectory(nodeTraceDir, pipelineId, driverId),
      "/traceRoot/queryId/taskId/1/1/1");
  ASSERT_EQ(
      getOpTraceInputFilePath(opTraceDir),
      "/traceRoot/queryId/taskId/1/1/1/op_input_trace.data");
  ASSERT_EQ(
      getOpTraceSummaryFilePath(opTraceDir),
      "/traceRoot/queryId/taskId/1/1/1/op_trace_summary.json");
  ASSERT_EQ(
      getOpTraceSplitFilePath(opTraceDir),
      "/traceRoot/queryId/taskId/1/1/1/op_split_trace.split");
}

TEST_F(TraceUtilTest, getTaskIds) {
  const auto rootDir = TempDirectoryPath::create();
  const auto rootPath = rootDir->getPath();
  const auto fs = filesystems::getFileSystem(rootPath, nullptr);
  const std::string queryId = "queryId";
  fs->mkdir(trace::getQueryTraceDirectory(rootPath, queryId));
  ASSERT_TRUE(getTaskIds(rootPath, queryId, fs).empty());
  const std::string taskId1 = "task1";
  fs->mkdir(trace::getTaskTraceDirectory(rootPath, queryId, taskId1));
  const std::string taskId2 = "task2";
  fs->mkdir(trace::getTaskTraceDirectory(rootPath, queryId, taskId2));
  auto taskIds = getTaskIds(rootPath, queryId, fs);
  ASSERT_EQ(taskIds.size(), 2);
  std::sort(taskIds.begin(), taskIds.end());
  ASSERT_EQ(taskIds[0], taskId1);
  ASSERT_EQ(taskIds[1], taskId2);
}

TEST_F(TraceUtilTest, getPipelineIds) {
  const auto rootDir = TempDirectoryPath::create();
  const auto rootPath = rootDir->getPath();
  const auto fs = filesystems::getFileSystem(rootPath, nullptr);
  const std::string queryId = "queryId";
  fs->mkdir(trace::getQueryTraceDirectory(rootPath, queryId));
  ASSERT_TRUE(getTaskIds(rootPath, queryId, fs).empty());
  const std::string taskId = "task";
  const std::string taskTraceDir =
      trace::getTaskTraceDirectory(rootPath, queryId, taskId);
  fs->mkdir(taskTraceDir);
  const std::string nodeId = "node";
  const std::string nodeTraceDir =
      trace::getNodeTraceDirectory(taskTraceDir, nodeId);
  fs->mkdir(nodeTraceDir);

  const std::vector<uint32_t> expectedPipelineIds{0, 1, 2};
  for (const auto pipelineId : expectedPipelineIds) {
    fs->mkdir(trace::getPipelineTraceDirectory(nodeTraceDir, pipelineId));
  }
  const auto pipelineIds = listPipelineIds(nodeTraceDir, fs);
  for (int i = 0; i < 3; ++i) {
    ASSERT_EQ(expectedPipelineIds[i], pipelineIds[i]);
  }

  // Bad pipeline id.
  const std::string badPipelineId = "badPipelineId";
  fs->mkdir(fmt::format("{}/{}", nodeTraceDir, badPipelineId));
  VELOX_ASSERT_THROW(
      listPipelineIds(nodeTraceDir, fs), "Failed to list pipeline IDs");
}

TEST_F(TraceUtilTest, getDriverIds) {
  const auto rootDir = TempDirectoryPath::create();
  const auto rootPath = rootDir->getPath();
  const auto fs = filesystems::getFileSystem(rootPath, nullptr);
  const std::string queryId = "queryId";
  fs->mkdir(trace::getQueryTraceDirectory(rootPath, queryId));
  ASSERT_TRUE(getTaskIds(rootPath, queryId, fs).empty());
  const std::string taskId = "task";
  const std::string taskTraceDir =
      trace::getTaskTraceDirectory(rootPath, queryId, taskId);
  fs->mkdir(taskTraceDir);
  const std::string nodeId = "node";
  const std::string nodeTraceDir =
      trace::getNodeTraceDirectory(taskTraceDir, nodeId);
  fs->mkdir(nodeTraceDir);
  const uint32_t pipelineId = 1;
  fs->mkdir(trace::getPipelineTraceDirectory(nodeTraceDir, pipelineId));
  ASSERT_TRUE(listDriverIds(nodeTraceDir, pipelineId, fs).empty());
  // create 3 drivers.
  const uint32_t driverId1 = 1;
  fs->mkdir(trace::getOpTraceDirectory(nodeTraceDir, pipelineId, driverId1));
  const uint32_t driverId2 = 2;
  fs->mkdir(trace::getOpTraceDirectory(nodeTraceDir, pipelineId, driverId2));
  const uint32_t driverId3 = 3;
  fs->mkdir(trace::getOpTraceDirectory(nodeTraceDir, pipelineId, driverId3));
  auto driverIds = listDriverIds(nodeTraceDir, pipelineId, fs);
  ASSERT_EQ(driverIds.size(), 3);
  std::sort(driverIds.begin(), driverIds.end());
  ASSERT_EQ(driverIds[0], driverId1);
  ASSERT_EQ(driverIds[1], driverId2);
  ASSERT_EQ(driverIds[2], driverId3);
  // Bad driver id.
  const std::string BadDriverId = "badDriverId";
  fs->mkdir(fmt::format("{}/{}/{}", nodeTraceDir, pipelineId, BadDriverId));
  ASSERT_ANY_THROW(listDriverIds(nodeTraceDir, pipelineId, fs));
  ASSERT_EQ(std::vector<uint32_t>({1, 2, 4}), extractDriverIds("1,2,4"));
  ASSERT_TRUE(extractDriverIds("").empty());
  ASSERT_NE(std::vector<uint32_t>({1, 2}), extractDriverIds("1,2,4"));
}

TEST_F(TraceUtilTest, createTraceDirectoryTest) {
  auto tmpRootDir = exec::test::TempDirectoryPath::create();
  auto tmpTraceDir = fmt::format(
      "{}{}/trace",
      tests::utils::FaultyFileSystem::scheme(),
      tmpRootDir->getPath());
  auto fs = std::dynamic_pointer_cast<tests::utils::FaultyFileSystem>(
      filesystems::getFileSystem(tmpTraceDir, nullptr));

  filesystems::DirectoryOptions expectedOptions;
  constexpr auto traceDirConfig = "dummy.value=123";
  expectedOptions.values.emplace(
      filesystems::DirectoryOptions::kMakeDirectoryConfig.toString(),
      traceDirConfig);

  bool createdTraceDir = false;
  tests::utils::FileSystemFaultInjectionHook hook = [&](auto* op) {
    auto mkdirOp =
        static_cast<tests::utils::FaultFileSystemMkdirOperation*>(op);
    if (mkdirOp->path == fmt::format("{}/trace", tmpRootDir->getPath())) {
      createdTraceDir = true;
      auto it = mkdirOp->options.values.find(
          filesystems::DirectoryOptions::kMakeDirectoryConfig.toString());
      EXPECT_TRUE(it != mkdirOp->options.values.end());
      EXPECT_EQ(it->second, traceDirConfig);
    }
    return;
  };
  fs->setFilesystemInjectionHook(hook);

  trace::createTraceDirectory(tmpTraceDir, traceDirConfig);
  // Check that injection hook was called.
  EXPECT_TRUE(createdTraceDir);
  EXPECT_TRUE(fs->exists(tmpTraceDir));
  fs->rmdir(tmpTraceDir);
  EXPECT_FALSE(fs->exists(tmpTraceDir));
}
} // namespace facebook::velox::exec::trace::test
