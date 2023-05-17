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
#include <folly/Uri.h>
#include "folly/init/Init.h"
#include "presto_cpp/external/json/json.hpp"
#include "presto_cpp/main/operators/BroadcastExchangeSource.h"
#include "presto_cpp/main/operators/BroadcastWrite.h"
#include "presto_cpp/main/operators/LocalPersistentShuffle.h"
#include "presto_cpp/main/operators/tests/PlanBuilder.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/QueryAssertions.h"

using namespace facebook::velox;
using namespace facebook::presto;
using namespace facebook::presto::operators;

namespace facebook::presto::operators::test {
class BroadcastTest : public exec::test::OperatorTestBase {
 public:
 protected:
  void SetUp() override {
    exec::test::OperatorTestBase::SetUp();
    filesystems::registerLocalFileSystem();
    exec::Operator::registerOperator(
        std::make_unique<BroadcastWriteTranslator>());
    exec::ExchangeSource::registerFactory(
        BroadcastExchangeSource::createExchangeSource);
  }

  static std::string makeTaskId(const std::string& prefix, int num) {
    auto url = fmt::format("batch/{}-{}", prefix, num);
    return url;
  }

  std::shared_ptr<exec::Task> makeTask(
      const std::string& taskId,
      core::PlanNodePtr planNode,
      int destination) {
    auto queryCtx = std::make_shared<core::QueryCtx>(executor_.get());
    core::PlanFragment planFragment{planNode};
    return exec::Task::create(
        taskId, std::move(planFragment), destination, std::move(queryCtx));
  }

  void runBroadcastTest(const std::vector<RowVectorPtr>& data) {
    exec::Operator::registerOperator(
        std::make_unique<BroadcastWriteTranslator>());

    auto dataType = asRowType(data[0]->type());
    auto writerTaskId = makeTaskId("leaf", 0);
    auto basePath = fmt::format("/tmp/{}", writerTaskId);
    auto writerPlan =
        exec::test::PlanBuilder()
            .values(data, true)
            .addNode(addBroadcastWriteNode(basePath))
            .planNode();

    exec::test::CursorParameters params;
    params.planNode = writerPlan;
    auto [taskCursor, results] = readCursor(params, [](auto /*task*/) {});

    // Expect one file for each request
    ASSERT_EQ(results.size(), 1);

    // Validate file path prefix is consistent
    auto broadcastFilePath = results.back()->childAt(0)->toString(0);
    ASSERT_TRUE(boost::starts_with(broadcastFilePath, basePath));

    // Create plan for read node using file path
    auto readerPlan = exec::test::PlanBuilder()
                          .exchange(dataType)
                          .planNode();

    // Read back result from Broadcast Read
    std::vector<RowVectorPtr> expectedOutputVectors;
    for (auto& input : data) {
      expectedOutputVectors.push_back(input);
    }
    std::vector<RowVectorPtr> outputVectors;

    exec::test::CursorParameters broadcastReadParams;
    broadcastReadParams.planNode = readerPlan;
    params.destination = 0;

    bool noMoreSplits = false;
    auto [broadcastReadCursor, broadcastReadResults] =
        readCursor(broadcastReadParams, [&](auto* task) {
          if (noMoreSplits) {
            return;
          }
          auto split = exec::Split(
              std::make_shared<exec::RemoteConnectorSplit>(fmt::format(
                  "batch://task?broadcastBasePath={}", broadcastFilePath)),
              -1);
          task->addSplit("0", std::move(split));
          task->noMoreSplits("0");
          noMoreSplits = true;
        });

    for (auto& resultVector : broadcastReadResults) {
      outputVectors.push_back(copyResultVector(resultVector));
    }

    // Assert its same as data
    velox::exec::test::assertEqualResults(expectedOutputVectors, outputVectors);

    cleanupDirectory(basePath);
  }

  RowVectorPtr copyResultVector(const RowVectorPtr& result) {
    auto vector = std::static_pointer_cast<RowVector>(
        BaseVector::create(result->type(), result->size(), pool()));
    vector->copy(result.get(), 0, 0, result->size());
    VELOX_CHECK_EQ(vector->size(), result->size());
    return vector;
  }

  void cleanupDirectory(const std::string& rootPath) {
    auto fileSystem = filesystems::getFileSystem(rootPath, nullptr);
    auto files = fileSystem->list(rootPath);
    for (auto& file : files) {
      fileSystem->remove(file);
    }
  }
};

TEST_F(BroadcastTest, endToEnd) {
  auto data = vectorMaker_.rowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50, 60}),
  });

  runBroadcastTest({data});
}

} // namespace facebook::presto::operators::test
