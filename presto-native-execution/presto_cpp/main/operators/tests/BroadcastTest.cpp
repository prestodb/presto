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
#include "presto_cpp/main/operators/BroadcastWrite.h"
#include "presto_cpp/main/operators/tests/PlanBuilder.h"
#include "velox/buffer/Buffer.h"
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
 protected:
  void SetUp() override {
    exec::test::OperatorTestBase::SetUp();
    filesystems::registerLocalFileSystem();
    exec::Operator::registerOperator(
        std::make_unique<BroadcastWriteTranslator>());
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
    auto basePath = exec::test::TempDirectoryPath::create()->path;
    auto writerPlan = exec::test::PlanBuilder()
                          .values(data, true)
                          .addNode(addBroadcastWriteNode(basePath))
                          .planNode();

    exec::test::CursorParameters params;
    params.planNode = writerPlan;
    auto [taskCursor, results] = readCursor(params, [](auto /*task*/) {});

    // Expect one file for each request.
    ASSERT_EQ(results.size(), 1);

    // Validate file path prefix is consistent.
    auto broadcastFilePath =
        results.back()->childAt(0)->as<SimpleVector<StringView>>()->valueAt(0);
    ASSERT_EQ(broadcastFilePath.str().find(basePath), 0);

    // Read back broadcast data from broadcast file.
    auto result = getRowVectorFromFile(broadcastFilePath.str(), dataType);

    // Assert data from broadcast file matches input.
    velox::exec::test::assertEqualResults(data, {result});

    // Cleanup - remove broadcast files.
    cleanupDirectory(basePath);
  }

  RowVectorPtr getRowVectorFromFile(std::string filePath, RowTypePtr dataType) {
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

  void cleanupDirectory(const std::string& rootPath) {
    auto fileSystem = filesystems::getFileSystem(rootPath, nullptr);
    auto files = fileSystem->list(rootPath);
    for (auto& file : files) {
      fileSystem->remove(file);
    }
  }
};

TEST_F(BroadcastTest, broadcastWrite) {
  auto data = vectorMaker_.rowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50, 60}),
  });
  runBroadcastTest({data});

  data = vectorMaker_.rowVector({
      makeFlatVector<std::string>({"1", "2", "3", "4", "abc", "xyz"}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50, 60}),
  });
  runBroadcastTest({data});

  makeRowVector({
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

} // namespace facebook::presto::operators::test
