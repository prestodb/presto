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
#include "presto_cpp/main/operators/BroadcastExchangeSource.h"
#include "presto_cpp/main/operators/BroadcastWrite.h"
#include "presto_cpp/main/operators/tests/PlanBuilder.h"
#include "velox/buffer/Buffer.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/compression/Compression.h"
#include "velox/common/file/FileSystems.h"
#include "velox/core/QueryConfig.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/serializers/PrestoSerializer.h"

using namespace facebook::velox;
using namespace facebook::presto;
using namespace facebook::presto::operators;

namespace facebook::presto::operators::test {

struct BroadcastTestParam {
  common::CompressionKind compressionKind;

  std::string toString() const {
    return common::compressionKindToString(compressionKind);
  }
};

class BroadcastTest : public exec::test::OperatorTestBase,
                      public testing::WithParamInterface<BroadcastTestParam> {
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

  std::unique_ptr<VectorSerde::Options> getVectorSerdeOptions(
      common::CompressionKind compressionKind) {
    std::unique_ptr<VectorSerde::Options> options = std::make_unique<
        serializer::presto::PrestoVectorSerde::PrestoOptions>();
    options->compressionKind = compressionKind;
    return options;
  }

  std::pair<RowTypePtr, std::vector<std::string>> executeBroadcastWrite(
      const std::vector<RowVectorPtr>& data,
      const std::string& basePath,
      const std::optional<std::vector<std::string>>& serdeLayout =
          std::nullopt) {
    auto writerPlan = exec::test::PlanBuilder()
                          .values(data, true)
                          .addNode(addBroadcastWriteNode(basePath, serdeLayout))
                          .planNode();

    auto serdeRowType =
        std::dynamic_pointer_cast<const BroadcastWriteNode>(writerPlan)
            ->serdeRowType();

    exec::CursorParameters params;
    params.planNode = writerPlan;

    // Set up query context with compression kind configuration
    std::unordered_map<std::string, std::string> configs;
    configs[core::QueryConfig::kShuffleCompressionKind] =
        common::compressionKindToString(GetParam().compressionKind);
    params.queryCtx = core::QueryCtx::create(
        executor_.get(), core::QueryConfig(std::move(configs)));

    auto [taskCursor, results] = exec::test::readCursor(params);

    std::vector<std::string> broadcastFilePaths;
    for (const auto& result : results) {
      broadcastFilePaths.emplace_back(
          result->childAt(0)->as<SimpleVector<StringView>>()->valueAt(0));
    }

    return {serdeRowType, broadcastFilePaths};
  }

  std::pair<std::unique_ptr<velox::exec::TaskCursor>, std::vector<RowVectorPtr>>
  executeBroadcastRead(
      RowTypePtr dataType,
      const std::string& basePath,
      const std::vector<std::string>& broadcastFilePaths) {
    // Create plan for read node using file path.
    auto readerPlan = exec::test::PlanBuilder()
                          .exchange(dataType, velox::VectorSerde::Kind::kPresto)
                          .planNode();
    exec::CursorParameters broadcastReadParams;
    broadcastReadParams.planNode = readerPlan;

    // Set up query context with compression kind configuration
    std::unordered_map<std::string, std::string> configs;
    configs[core::QueryConfig::kShuffleCompressionKind] =
        common::compressionKindToString(GetParam().compressionKind);
    broadcastReadParams.queryCtx = core::QueryCtx::create(
        executor_.get(), core::QueryConfig(std::move(configs)));

    std::vector<std::string> fileInfos;
    fileInfos.reserve(broadcastFilePaths.size());
    for (auto broadcastFilePath : broadcastFilePaths) {
      fileInfos.emplace_back(
          fmt::format(kBroadcastFileInfoFormat, broadcastFilePath));
    }

    // Read back result using BroadcastExchangeSource.
    return exec::test::readCursor(
        broadcastReadParams, [&](exec::TaskCursor* taskCursor) {
          if (taskCursor->noMoreSplits()) {
            return;
          }
          auto& task = taskCursor->task();
          for (int splitIndex = 0; splitIndex < broadcastFilePaths.size();
               ++splitIndex) {
            auto split = exec::Split(
                std::make_shared<exec::RemoteConnectorSplit>(fmt::format(
                    "batch://task?broadcastInfo={}",
                    fmt::format(
                        kBroadcastFileInfoFormat,
                        broadcastFilePaths[splitIndex]))),
                -1);
            task->addSplit("0", std::move(split));
          }
          task->noMoreSplits("0");
          taskCursor->setNoMoreSplits();
        });
  }

  std::vector<RowVectorPtr> reorderColumns(
      const std::vector<RowVectorPtr>& data,
      const std::optional<std::vector<std::string>>& newLayout,
      const RowTypePtr& newRowType) {
    std::vector<RowVectorPtr> reordered;
    if (!newLayout.has_value()) {
      return data;
    }

    for (const auto& vector : data) {
      auto rowType = asRowType(vector->type());
      std::vector<VectorPtr> columns;
      for (const auto& name : newLayout.value()) {
        columns.push_back(vector->childAt(rowType->getChildIdx(name)));
      }
      reordered.push_back(std::make_shared<RowVector>(
          pool(), newRowType, nullptr /*nulls*/, vector->size(), columns));
    }
    return reordered;
  }

  void runBroadcastTest(
      const std::vector<RowVectorPtr>& data,
      const std::optional<std ::vector<std::string>>& serdeLayout =
          std::nullopt) {
    exec::Operator::registerOperator(
        std::make_unique<BroadcastWriteTranslator>());

    auto tempDirectoryPath = exec::test::TempDirectoryPath::create();
    auto [serdeRowType, broadcastFilePaths] =
        executeBroadcastWrite(data, tempDirectoryPath->getPath(), serdeLayout);

    // Expect one file for each request.
    ASSERT_EQ(broadcastFilePaths.size(), 1);

    // Validate file path prefix is consistent.
    ASSERT_EQ(broadcastFilePaths.back().find(tempDirectoryPath->getPath()), 0);

    auto expected = reorderColumns(data, serdeLayout, serdeRowType);

    std::vector<RowVectorPtr> actualOutputVectors;

    // Read back result.
    auto [broadcastReadCursor, broadcastReadResults] = executeBroadcastRead(
        serdeRowType, tempDirectoryPath->getPath(), broadcastFilePaths);

    // Assert its same as data.
    velox::exec::test::assertEqualResults(expected, broadcastReadResults);
  }
};

TEST_P(BroadcastTest, endToEnd) {
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

TEST_P(BroadcastTest, endToEndSerdeLayout) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50, 60}),
      makeFlatVector<std::string>({"1", "2", "3", "4", "abc", "xyz"}),
  });

  // Serialize columns in reverse order.
  runBroadcastTest({data}, {{"c2", "c1", "c0"}});

  // Serialize some columns twice.
  runBroadcastTest({data}, {{"c2", "c1", "c0", "c2"}});

  // Skip some columns.
  runBroadcastTest({data}, {{"c0", "c2"}});

  // Skip some, duplicate other.
  runBroadcastTest({data}, {{"c1", "c1", "c2"}});

  // Skip all.
  runBroadcastTest({data}, {std::vector<std::string>{}});
}

TEST_P(BroadcastTest, endToEndWithNoRows) {
  std::vector<RowVectorPtr> data = {makeRowVector(
      {makeFlatVector<double>({}), makeArrayVector<int32_t>({})})};
  auto tempDirectoryPath = exec::test::TempDirectoryPath::create();
  std::vector<std::string> broadcastFilePaths;

  // Execute write.
  auto results = executeBroadcastWrite({data}, tempDirectoryPath->getPath());

  // Assert no file path returned.
  ASSERT_EQ(broadcastFilePaths.size(), 0);

  auto fileSystem =
      velox::filesystems::getFileSystem(tempDirectoryPath->getPath(), nullptr);
  auto files = fileSystem->list(tempDirectoryPath->getPath());

  // Assert no file was generated in broadcast directory path.
  ASSERT_EQ(files.size(), 0);
}

TEST_P(BroadcastTest, endToEndWithMultipleWriteNodes) {
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
  for (const auto& data : dataVector) {
    auto [serdeRowType, results] =
        executeBroadcastWrite({data}, tempDirectoryPath->getPath());
    broadcastFilePaths.emplace_back(results[0]);
  }

  // Read back result.
  auto [taskCursorReadNode, broadcastReadResults] = executeBroadcastRead(
      asRowType(dataVector[0]->type()),
      tempDirectoryPath->getPath(),
      broadcastFilePaths);

  // Validate BroadcastExchange reads back output of both writes.
  velox::exec::test::assertEqualResults(dataVector, broadcastReadResults);
}

TEST_P(BroadcastTest, invalidFileSystem) {
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

TEST_P(BroadcastTest, invalidBroadcastFilePath) {
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

TEST_P(BroadcastTest, malformedBroadcastInfoJson) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50, 60}),
  });
  auto dataType = asRowType(data->type());
  std::string basePath = "/tmp";
  std::string invalidBroadcastFilePath = "/tmp/file.bin";

  auto readerPlan = exec::test::PlanBuilder()
                        .exchange(dataType, velox::VectorSerde::Kind::kPresto)
                        .planNode();
  exec::CursorParameters broadcastReadParams;
  broadcastReadParams.planNode = readerPlan;

  VELOX_ASSERT_THROW(
      exec::test::readCursor(
          broadcastReadParams,
          [&](exec::TaskCursor* taskCursor) {
            if (taskCursor->noMoreSplits()) {
              return;
            }
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
            auto& task = taskCursor->task();
            task->addSplit("0", std::move(split));
            task->noMoreSplits("0");
            taskCursor->setNoMoreSplits();
          }),
      "BroadcastInfo deserialization failed");
}

TEST_P(BroadcastTest, broadcastFileWriter) {
  auto tempDirectoryPath = exec::test::TempDirectoryPath::create();
  auto fileSystem =
      velox::filesystems::getFileSystem(tempDirectoryPath->getPath(), nullptr);
  fileSystem->mkdir(tempDirectoryPath->getPath());

  auto filePath =
      fmt::format("{}/broadcast_writer_test", tempDirectoryPath->getPath());

  auto testData1 = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3}),
      makeFlatVector<std::string>({"a", "b", "c"}),
  });
  auto testData2 = makeRowVector({
      makeFlatVector<int32_t>({4, 5, 6}),
      makeFlatVector<std::string>({"d", "e", "f"}),
  });

  {
    auto writer = std::make_unique<BroadcastFileWriter>(
        filePath + "_success",
        1024,
        getVectorSerdeOptions(GetParam().compressionKind),
        pool());

    writer->write(testData1);
    writer->write(testData2);
    writer->noMoreData();

    auto fileStats = writer->fileStats();
    ASSERT_NE(fileStats, nullptr);
    ASSERT_EQ(fileStats->size(), 1);
    ASSERT_EQ(fileStats->childrenSize(), 3);

    auto createdFilePath =
        fileStats->childAt(0)->as<SimpleVector<StringView>>()->valueAt(0).str();
    ASSERT_TRUE(fileSystem->exists(createdFilePath));

    auto actualFileSize = fileSystem->openFileForRead(createdFilePath)->size();
    auto maxSerializedSize =
        fileStats->childAt(1)->as<SimpleVector<int64_t>>()->valueAt(0);
    ASSERT_EQ(maxSerializedSize, actualFileSize);
    ASSERT_GT(maxSerializedSize, 0);

    auto numRows =
        fileStats->childAt(2)->as<SimpleVector<int64_t>>()->valueAt(0);
    ASSERT_EQ(numRows, testData1->size() + testData2->size());
    ASSERT_EQ(numRows, 6);
  }

  // Test fileStats() before noMoreData() returns nullptr
  {
    auto writer = std::make_unique<BroadcastFileWriter>(
        filePath + "_before_no_more_data",
        1024,
        getVectorSerdeOptions(GetParam().compressionKind),
        pool());

    writer->write(testData1);
    auto fileStats = writer->fileStats();
    ASSERT_EQ(fileStats, nullptr);

    writer->noMoreData();
  }

  // Test write() after noMoreData() throws
  {
    auto writer = std::make_unique<BroadcastFileWriter>(
        filePath + "_write_after_no_more",
        1024,
        getVectorSerdeOptions(GetParam().compressionKind),
        pool());

    writer->write(testData1);
    writer->noMoreData();

    VELOX_ASSERT_THROW(
        writer->write(testData2), "SerializedPageFileWriter has finished");
  }

  // Test multiple calls to noMoreData() throw exception
  {
    auto writer = std::make_unique<BroadcastFileWriter>(
        filePath + "_multiple_no_more",
        1024,
        getVectorSerdeOptions(GetParam().compressionKind),
        pool());

    writer->write(testData1);
    writer->noMoreData();

    VELOX_ASSERT_THROW(
        writer->noMoreData(), "SerializedPageFileWriter has finished");

    auto fileStats = writer->fileStats();
    ASSERT_NE(fileStats, nullptr);
    ASSERT_EQ(fileStats->size(), 1);
  }

  {
    auto writer = std::make_unique<BroadcastFileWriter>(
        filePath + "_multiple_stats",
        1024,
        getVectorSerdeOptions(GetParam().compressionKind),
        pool());

    writer->write(testData1);
    writer->noMoreData();

    auto fileStats1 = writer->fileStats();
    auto fileStats2 = writer->fileStats();

    ASSERT_NE(fileStats1, nullptr);
    ASSERT_NE(fileStats2, nullptr);
    ASSERT_EQ(fileStats1->size(), fileStats2->size());

    auto filePath1 = fileStats1->childAt(0)
                         ->as<SimpleVector<StringView>>()
                         ->valueAt(0)
                         .str();
    auto filePath2 = fileStats2->childAt(0)
                         ->as<SimpleVector<StringView>>()
                         ->valueAt(0)
                         .str();
    ASSERT_EQ(filePath1, filePath2);

    auto maxSize1 =
        fileStats1->childAt(1)->as<SimpleVector<int64_t>>()->valueAt(0);
    auto maxSize2 =
        fileStats2->childAt(1)->as<SimpleVector<int64_t>>()->valueAt(0);
    ASSERT_EQ(maxSize1, maxSize2);

    auto numRows1 =
        fileStats1->childAt(2)->as<SimpleVector<int64_t>>()->valueAt(0);
    auto numRows2 =
        fileStats2->childAt(2)->as<SimpleVector<int64_t>>()->valueAt(0);
    ASSERT_EQ(numRows1, numRows2);
    ASSERT_EQ(numRows1, testData1->size());
  }

  {
    auto writer = std::make_unique<BroadcastFileWriter>(
        filePath + "_no_data",
        1024,
        getVectorSerdeOptions(GetParam().compressionKind),
        pool());

    writer->noMoreData();
    auto fileStats = writer->fileStats();
    ASSERT_EQ(fileStats, nullptr);
  }

  {
    auto emptyData = makeRowVector({
        makeFlatVector<int32_t>({}),
        makeFlatVector<std::string>({}),
    });

    auto writer = std::make_unique<BroadcastFileWriter>(
        filePath + "_empty_data",
        1024,
        getVectorSerdeOptions(GetParam().compressionKind),
        pool());

    writer->write(emptyData);
    writer->noMoreData();
    auto fileStats = writer->fileStats();
    ASSERT_EQ(fileStats, nullptr);
  }
}

TEST_P(BroadcastTest, endToEndWithDifferentWriterPageSizes) {
  const uint32_t numWrites = 64;
  // Create a data that is slightly larger than 1KB.
  const auto dataPerWrite = makeRowVector({
      makeFlatVector<int32_t>(20, [](auto row) { return row; }),
      makeFlatVector<int64_t>(20, [](auto row) { return row * 10; }),
      makeFlatVector<std::string>(
          20,
          [](auto row) {
            return fmt::format(
                "this_is_a_relatively_large_string_for_this_specifc_test_{}",
                row);
          }),
  });
  std::vector<RowVectorPtr> totalData;
  totalData.reserve(numWrites);
  for (auto i = 0; i < numWrites; ++i) {
    totalData.push_back(dataPerWrite);
  }

  const auto KB = 2 << 10;
  const std::vector<uint64_t> kPageSizes = {
      KB, // 1KB
      4 * KB, // 4KB
      64 * KB, // 64KB
  };

  for (size_t i = 0; i < kPageSizes.size(); ++i) {
    auto tempDirectoryPath = exec::test::TempDirectoryPath::create();

    // Create a modified factory that uses custom buffer size
    auto fileSystem = velox::filesystems::getFileSystem(
        tempDirectoryPath->getPath(), nullptr);
    fileSystem->mkdir(tempDirectoryPath->getPath());

    auto filePath =
        fmt::format("{}/broadcast_buffer_test", tempDirectoryPath->getPath());

    // Create writer with specific buffer size directly
    auto writer = std::make_unique<BroadcastFileWriter>(
        filePath,
        kPageSizes[i],
        getVectorSerdeOptions(GetParam().compressionKind),
        pool());

    // Write data and complete the write process
    for (auto i = 0; i < numWrites; ++i) {
      writer->write(dataPerWrite);
    }
    writer->noMoreData();

    // Get file stats
    auto fileStats = writer->fileStats();
    ASSERT_NE(fileStats, nullptr);
    ASSERT_EQ(fileStats->size(), 1);

    // Get the actual file path from the stats
    auto createdFilePath =
        fileStats->childAt(0)->as<SimpleVector<StringView>>()->valueAt(0).str();
    ASSERT_TRUE(fileSystem->exists(createdFilePath));

    // Create a BroadcastFileReader to verify page count
    auto broadcastFileInfo = std::make_unique<BroadcastFileInfo>();
    broadcastFileInfo->filePath_ = createdFilePath;
    auto reader = std::make_shared<BroadcastFileReader>(
        broadcastFileInfo, fileSystem, pool());

    // Get remaining page sizes to determine total page count
    auto remainingPageSizes = reader->remainingPageSizes();
    uint32_t totalPageCount = static_cast<uint32_t>(remainingPageSizes.size());

    // Verify that each page can be read individually
    uint32_t pagesRead = 0;
    while (reader->hasNext()) {
      auto pageBuffer = reader->next();
      ASSERT_NE(pageBuffer, nullptr);
      ASSERT_GT(pageBuffer->size(), 0);
      pagesRead++;
    }

    // Verify page counts match
    ASSERT_EQ(pagesRead, totalPageCount);

    // Read back the data and verify it matches the original input
    auto [_, pageResults] = executeBroadcastRead(
        asRowType(dataPerWrite->type()),
        tempDirectoryPath->getPath(),
        {createdFilePath});

    velox::exec::test::assertEqualResults(totalData, pageResults);
  }
}

INSTANTIATE_TEST_SUITE_P(
    BroadcastTest,
    BroadcastTest,
    testing::Values(
        BroadcastTestParam{common::CompressionKind_NONE},
        BroadcastTestParam{common::CompressionKind_ZSTD},
        BroadcastTestParam{common::CompressionKind_LZ4},
        BroadcastTestParam{common::CompressionKind_ZLIB},
        BroadcastTestParam{common::CompressionKind_SNAPPY}),
    [](const testing::TestParamInfo<BroadcastTestParam>& info) {
      return info.param.toString();
    });

} // namespace facebook::presto::operators::test
