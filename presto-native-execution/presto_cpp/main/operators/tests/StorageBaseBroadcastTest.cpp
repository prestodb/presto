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
#include <string_view>

#include "presto_cpp/main/operators/StorageBasedBroadcastShuffle.h"
#include "presto_cpp/main/operators/StorageBasedBroadcastWrite.h"
#include "presto_cpp/main/operators/StorageBasedExchangeSource.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox;
using namespace facebook::presto;
using namespace facebook::presto::operators;

namespace facebook::presto::operators::test {

class StorageBasedBroadcastTest : public exec::test::OperatorTestBase {
 public:
  static std::string makeTaskId(
      const std::string& prefix,
      int num,
      const std::string& folderList) {
    // The task Id includes the list of broadcast folders.
    return fmt::format("storage-broadcast://{}-{}!{}", prefix, num, folderList);
  }

  std::shared_ptr<exec::Task> makeTask(
      const std::string& taskId,
      core::PlanNodePtr planNode,
      int destination) {
    auto queryCtx =
        core::QueryCtx::createForTest(std::make_shared<core::MemConfig>());
    core::PlanFragment planFragment{planNode};
    return std::make_shared<exec::Task>(
        taskId, std::move(planFragment), destination, std::move(queryCtx));
  }

  void addRemoteSplits(
      exec::Task* task,
      const std::vector<std::string>& remoteTaskIds) {
    for (auto& taskId : remoteTaskIds) {
      auto split =
          exec::Split(std::make_shared<exec::RemoteConnectorSplit>(taskId), -1);
      task->addSplit("0", std::move(split));
    }
    task->noMoreSplits("0");
  }
};

std::vector<std::string> extractFoldersFromTaskId(const std::string& taskId) {
  // Parse task id to extract list of broadcast folders.
  auto index = taskId.find("!");
  VELOX_CHECK_NE(
      index, std::string::npos, "Broadcast folder list not found in task id.");
  if (index == taskId.size() - 1) {
    return {};
  }
  auto folderList = taskId.substr(index + 1);
  std::vector<std::string> resultList;
  char* token = std::strtok(const_cast<char*>(folderList.c_str()), ",");
  while (token) {
    resultList.push_back(token);
    token = std::strtok(nullptr, ",");
  }
  return resultList;
}

void registerExchangeSource() {
  exec::ExchangeSource::registerFactory(
      [](const std::string& taskId,
         int destination,
         std::shared_ptr<exec::ExchangeQueue> queue,
         memory::MemoryPool* FOLLY_NONNULL pool)
          -> std::unique_ptr<exec::ExchangeSource> {
        if (strncmp(taskId.c_str(), "storage-broadcast://", 20) == 0) {
          auto folders = extractFoldersFromTaskId(taskId);
          return std::make_unique<StorageBasedExchangeSource>(
              taskId, destination, std::move(queue), folders, pool);
        }
        return nullptr;
      });
}

auto addStorageBasedBroadcastWriteNode(ShuffleInterface* shuffle) {
  return [shuffle](
             core::PlanNodeId nodeId,
             core::PlanNodePtr source) -> core::PlanNodePtr {
    return std::make_shared<StorageBasedBroadcastWriteNode>(
        nodeId, std::move(source), shuffle);
  };
}

TEST_F(StorageBasedBroadcastTest, endToEnd) {
  exec::Operator::registerOperator(
      std::make_unique<StorageBasedBroadcastWriteTranslator>());
  velox::filesystems::registerLocalFileSystem();

  // Set these parameters and the vector and container sizes to relatively
  // small numbers for the sake of workflow test time. For stress testing,
  // we should bump them up.
  static constexpr size_t kNumPartitions = 2;
  static constexpr size_t kNumWriterTasks = 1;
  static constexpr size_t kNumDrivers = 1;
  static constexpr int kIterations = 1;

  auto rowType = ROW(
      {{"c0", BOOLEAN()},
       {"c1", INTEGER()},
       {"c2", REAL()},
       {"c3", ARRAY(INTEGER())},
       {"c5", TINYINT()},
       {"c6", ARRAY(BIGINT())},
       {"c7", ROW({REAL(), VARCHAR(), TINYINT(), ARRAY(VARCHAR())})},
       {"c8", DOUBLE()},
       {"c9", VARCHAR()}});

  VectorFuzzer::Options opts;
  opts.vectorSize = 1;
  opts.nullRatio = 0.1;
  opts.containerHasNulls = false;
  opts.dictionaryHasNulls = false;
  opts.stringVariableLength = true;
  opts.stringLength = 20;
  opts.containerLength = 65;

  auto seed = folly::Random::rand32();
  VectorFuzzer fuzzer(opts, pool_.get(), seed);

  std::unique_ptr<ShuffleManager> manager =
      std::make_unique<StorageBasedBroadcastShuffleManager>();
  registerShuffleManager("broadcast", manager);

  for (size_t i = 0; i < kIterations; ++i) {
    const auto& data = fuzzer.fuzzRow(rowType);
    std::vector<std::string> leafTaskIds;

    std::vector<std::shared_ptr<velox::exec::test::TempDirectoryPath>>
        tempFolders;
    std::string tempFolderPaths;
    std::vector<std::string> broadcastFolders;

    std::vector<RowVectorPtr> inputVectors;
    // Create one folder per task. So there will be kNumPartitions files
    // in each folder.
    for (auto taskIndex = 0; taskIndex < kNumWriterTasks; taskIndex++) {
      auto tempFolder = velox::exec::test::TempDirectoryPath::create();
      tempFolders.push_back(tempFolder);
      broadcastFolders.push_back(tempFolder->path);
      // Stitch all folder names to be used for forming task Ids.
      tempFolderPaths += ((taskIndex == 0) ? "" : ",") + tempFolder->path;
      // Create the list of input vector for correctness check.
      for (int driver = 0; driver < kNumDrivers; driver++) {
        inputVectors.push_back(vectorMaker_.flatten<RowVector>(data));
      }
    }


    for (auto taskIndex = 0; taskIndex < kNumWriterTasks; taskIndex++) {
      StorageBasedBroadcastShuffleInfo shuffleInfo("broadcast", taskIndex, broadcastFolders);
      auto shuffle = createShuffleInstance(shuffleInfo, pool());
      auto leafPlan = exec::test::PlanBuilder()
                          .values({data}, true)
                          .addNode(addStorageBasedBroadcastWriteNode(shuffle.get()))
                          .planNode();
      auto leafTaskId = makeTaskId("leaf", 0, tempFolderPaths);
      auto leafTask = makeTask(leafTaskId, leafPlan, 0);
      leafTaskIds.push_back(leafTaskId);
      exec::Task::start(leafTask, kNumDrivers);
      ASSERT_TRUE(exec::test::waitForTaskCompletion(leafTask.get()));
    }
    registerExchangeSource();

    // Create and run multiple downstream tasks, one per partition, to read data
    // from all the broadcast producers.
    for (auto i = 0; i < kNumPartitions; ++i) {
      auto plan = exec::test::PlanBuilder()
                      .exchange(rowType)
                      .project(rowType->names())
                      .planNode();

      exec::test::CursorParameters params;
      params.planNode = plan;
      params.destination = i;

      bool noMoreSplits = false;
      auto [taskCursor, resultVectors] = readCursor(params, [&](auto* task) {
        if (noMoreSplits) {
          return;
        }
        for (auto& taskId : leafTaskIds) {
          addRemoteSplits(task, {taskId});
        }
        noMoreSplits = true;
      });

      velox::exec::test::assertEqualResults(inputVectors, resultVectors);
    }
  }
}

TEST_F(StorageBasedBroadcastTest, storageBasedBroadcastToString) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
      makeFlatVector<int64_t>(1'000, [](auto row) { return row * 10; }),
  });

  velox::filesystems::registerLocalFileSystem();
  auto localFileSystem = velox::filesystems::getFileSystem("/", nullptr);

  auto plan = exec::test::PlanBuilder()
                  .values({data}, true)
                  .addNode(addStorageBasedBroadcastWriteNode(nullptr))
                  .planNode();

  ASSERT_EQ(
      plan->toString(true, false),
      "-- StorageBasedBroadcastWrite[] -> c0:INTEGER, c1:BIGINT\n");
  ASSERT_EQ(
      plan->toString(true, true),
      "-- StorageBasedBroadcastWrite[] -> c0:INTEGER, c1:BIGINT\n"
      "  -- Values[1000 rows in 1 vectors] -> c0:INTEGER, c1:BIGINT\n");
  ASSERT_EQ(plan->toString(false, false), "-- StorageBasedBroadcastWrite\n");
}
} // namespace facebook::presto::operators::test