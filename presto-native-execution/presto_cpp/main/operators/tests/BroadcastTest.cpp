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
#include <boost/algorithm/string/join.hpp>
#include <folly/Uri.h>
#include "folly/init/Init.h"
#include "presto_cpp/external/json/nlohmann/json.hpp"
#include "presto_cpp/main/operators/BroadcastExchangeSource.h"
#include "presto_cpp/main/operators/BroadcastWrite.h"
#include "presto_cpp/main/operators/tests/PlanBuilder.h"
#include "velox/buffer/Buffer.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/serializers/PrestoSerializer.h"

using namespace facebook::velox;
using namespace facebook::presto;
using namespace facebook::presto::operators;

namespace facebook::presto::operators::test {
class BroadcastTest : public exec::test::OperatorTestBase {
 public:
  static constexpr std::string_view kBroadcastFileInfoFormat =
      "{{\"filePath\": \"{}\"}}";

 protected:
  void SetUp() override {
    exec::test::OperatorTestBase::SetUp();
    filesystems::registerLocalFileSystem();
    exec::Operator::registerOperator(
        std::make_unique<BroadcastWriteTranslator>());
    // Clear exchange source factories. This avoids conflict with factories
    // registered by other tests.
    // For example - UnsafeRowShuffleTest registers custom exchange source
    // factory which breaks tests execution for BroadcastTest.
    exec::ExchangeSource::factories().clear();
    exec::ExchangeSource::registerFactory(
        BroadcastExchangeSource::createExchangeSource);
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

  std::vector<std::string> executeBroadcastWrite(
      const std::vector<RowVectorPtr>& data,
      const std::string& basePath) {
    auto writerPlan = exec::test::PlanBuilder()
                          .values(data, true)
                          .addNode(addBroadcastWriteNode(basePath))
                          .planNode();

    exec::test::CursorParameters params;
    params.planNode = writerPlan;
    auto [taskCursor, results] = readCursor(params, [](auto /*task*/) {});

    std::vector<std::string> broadcastFilePaths;
    for (auto result : results) {
      broadcastFilePaths.emplace_back(
          result->childAt(0)->as<SimpleVector<StringView>>()->valueAt(0));
    }
    return broadcastFilePaths;
  }

  std::pair<
      std::unique_ptr<velox::exec::test::TaskCursor>,
      std::vector<RowVectorPtr>>
  executeBroadcastRead(
      RowTypePtr dataType,
      const std::string& basePath,
      const std::vector<std::string>& broadcastFilePaths) {
    // Create plan for read node using file path.
    auto readerPlan = exec::test::PlanBuilder().exchange(dataType).planNode();
    exec::test::CursorParameters broadcastReadParams;
    broadcastReadParams.planNode = readerPlan;

    std::vector<std::string> fileInfos;
    for (auto broadcastFilePath : broadcastFilePaths) {
      fileInfos.emplace_back(
          fmt::format(kBroadcastFileInfoFormat, broadcastFilePath));
    }

    uint8_t splitIndex = 0;
    // Read back result using BroadcastExchangeSource.
    return readCursor(broadcastReadParams, [&](auto* task) {
      if (splitIndex >= broadcastFilePaths.size()) {
        task->noMoreSplits("0");
        return;
      }

      auto split = exec::Split(
          std::make_shared<exec::RemoteConnectorSplit>(fmt::format(
              "batch://task?broadcastInfo={}",
              fmt::format(
                  kBroadcastFileInfoFormat, broadcastFilePaths[splitIndex]))),
          -1);
      task->addSplit("0", std::move(split));
      ++splitIndex;
    });
  }

  void runBroadcastTest(const std::vector<RowVectorPtr>& data) {
    exec::Operator::registerOperator(
        std::make_unique<BroadcastWriteTranslator>());

    auto dataType = asRowType(data[0]->type());
    auto tempDirectoryPath = exec::test::TempDirectoryPath::create();
    auto broadcastFilePaths =
        executeBroadcastWrite(data, tempDirectoryPath->path);

    // Expect one file for each request.
    ASSERT_EQ(broadcastFilePaths.size(), 1);

    // Validate file path prefix is consistent.
    ASSERT_EQ(broadcastFilePaths.back().find(tempDirectoryPath->path), 0);

    // Read back broadcast data from broadcast file.
    auto result = getRowVectorFromFile(broadcastFilePaths.back(), dataType);

    // Assert data from broadcast file matches input.
    velox::exec::test::assertEqualResults(data, {result});

    std::vector<RowVectorPtr> actualOutputVectors;

    // Read back result.
    auto [broadcastReadCursor, broadcastReadResults] = executeBroadcastRead(
        dataType, tempDirectoryPath->path, broadcastFilePaths);

    // Assert its same as data.
    velox::exec::test::assertEqualResults(data, broadcastReadResults);
  }

  RowVectorPtr getRowVectorFromFile(
      const std::string& filePath,
      RowTypePtr dataType) {
    auto fs = filesystems::getFileSystem(filePath, nullptr);
    auto readFile = fs->openFileForRead(filePath);
    auto buffer =
        AlignedBuffer::allocate<char>(readFile->size(), pool_.get(), 0);
    readFile->pread(0, readFile->size(), buffer->asMutable<char>());
    auto ioBuf = folly::IOBuf::wrapBuffer(buffer->as<char>(), buffer->size());
    std::vector<ByteRange> ranges;
    for (const auto& range : *ioBuf) {
      ranges.emplace_back(ByteRange{
          const_cast<uint8_t*>(range.data()), (int32_t)range.size(), 0});
    }
    ByteStream byteStream;
    byteStream.resetInput(std::move(ranges));

    RowVectorPtr result;
    VectorStreamGroup::read(&byteStream, pool(), dataType, &result);
    return result;
  }
};

TEST_F(BroadcastTest, endToEnd) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50, 60}),
  });
  runBroadcastTest({data});

  data = makeRowVector({
      makeFlatVector<std::string>({"1", "2", "3", "4", "abc", "xyz"}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50, 60}),
  });
  runBroadcastTest({data});

  data = makeRowVector({
      makeFlatVector<double>({1.0, 2.0, 3.0}),
      makeArrayVector<int32_t>({
          {1, 2},
          {3, 4, 5},
          {},
      }),
      makeMapVector<int64_t, int32_t>(
          {{{1, 10}, {2, 20}}, {{3, 30}, {4, 40}, {5, 50}}, {}}),
  });
  runBroadcastTest({data});
}

TEST_F(BroadcastTest, endToEndWithNoRows) {
  std::vector<RowVectorPtr> data = {makeRowVector(
      {makeFlatVector<double>({}), makeArrayVector<int32_t>({})})};
  auto tempDirectoryPath = exec::test::TempDirectoryPath::create();
  std::vector<std::string> broadcastFilePaths;

  // Execute write.
  auto results = executeBroadcastWrite({data}, tempDirectoryPath->path);

  // Assert no file path returned.
  ASSERT_EQ(broadcastFilePaths.size(), 0);

  auto fileSystem =
      velox::filesystems::getFileSystem(tempDirectoryPath->path, nullptr);
  auto files = fileSystem->list(tempDirectoryPath->path);

  // Assert no file was generated in broadcast directory path.
  ASSERT_EQ(files.size(), 0);
}

TEST_F(BroadcastTest, endToEndWithMultipleWriteNodes) {
  std::vector<RowVectorPtr> dataVector = {
      makeRowVector({
          makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6}),
          makeFlatVector<int64_t>({10, 20, 30, 40, 50, 60}),
      }),
      makeRowVector({
          makeFlatVector<int32_t>({11, 21, 31, 41, 51, 61}),
          makeFlatVector<int64_t>({102, 203, 304, 405, 506, 607}),
      })};
  auto tempDirectoryPath = exec::test::TempDirectoryPath::create();
  std::vector<std::string> broadcastFilePaths;

  // Execute write.
  for (auto data : dataVector) {
    auto results = executeBroadcastWrite({data}, tempDirectoryPath->path);
    broadcastFilePaths.emplace_back(results[0]);
  }

  // Read back result.
  auto [taskCursorReadNode, broadcastReadResults] = executeBroadcastRead(
      asRowType(dataVector[0]->type()),
      tempDirectoryPath->path,
      broadcastFilePaths);

  // Validate BroadcastExchange reads back output of both writes.
  velox::exec::test::assertEqualResults(dataVector, broadcastReadResults);
}

TEST_F(BroadcastTest, invalidFileSystem) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50, 60}),
  });
  auto dataType = asRowType(data->type());
  std::string basePath = "invalid-prefix:/invalid-path";

  VELOX_ASSERT_THROW(
      executeBroadcastWrite({data}, basePath),
      "No registered file system matched with file path 'invalid-prefix:/invalid-path'");
}

TEST_F(BroadcastTest, invalidBroadcastFilePath) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50, 60}),
  });
  auto dataType = asRowType(data->type());
  std::string basePath = "/tmp";
  std::string invalidBroadcastFilePath =
      "/tmp/this-should-not-exist/velox--missing-broadcast-file.bin";

  VELOX_ASSERT_THROW(
      executeBroadcastRead(dataType, basePath, {invalidBroadcastFilePath}),
      "No such file or directory");
}

TEST_F(BroadcastTest, malformedBroadcastInfoJson) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50, 60}),
  });
  auto dataType = asRowType(data->type());
  std::string basePath = "/tmp";
  std::string invalidBroadcastFilePath = "/tmp/file.bin";

  auto readerPlan = exec::test::PlanBuilder().exchange(dataType).planNode();
  exec::test::CursorParameters broadcastReadParams;
  broadcastReadParams.planNode = readerPlan;

  VELOX_ASSERT_THROW(
      readCursor(
          broadcastReadParams,
          [&](auto* task) {
            auto fileInfos =
                fmt::format(kBroadcastFileInfoFormat, invalidBroadcastFilePath);
            auto split = exec::Split(
                std::make_shared<exec::RemoteConnectorSplit>(fmt::format(
                    // basePath value(string) is not enclosed in quotes, making
                    // it invalid json.
                    "batch://task?broadcastInfo={{\"basePath\": {}, \"fileInfos\":[{}]}}",
                    basePath,
                    fileInfos)),
                -1);
            task->addSplit("0", std::move(split));
            task->noMoreSplits("0");
          }),
      "BroadcastInfo deserialization failed");
}

} // namespace facebook::presto::operators::test
