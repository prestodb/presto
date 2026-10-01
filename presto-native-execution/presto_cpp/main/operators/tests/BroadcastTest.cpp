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
#include <folly/ScopeGuard.h>
#include <folly/Uri.h>
#include <folly/synchronization/Baton.h>
#include <thread>
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/common/Exception.h"
#include "presto_cpp/main/operators/BroadcastExchangeSource.h"
#include "presto_cpp/main/operators/BroadcastFile.h"
#include "presto_cpp/main/operators/BroadcastWrite.h"
#include "presto_cpp/main/operators/tests/PlanBuilder.h"
#include "velox/buffer/Buffer.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/compression/Compression.h"
#include "velox/common/file/FileSystems.h"
#include "velox/core/QueryConfig.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/ExchangeSource.h"
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
    auto writerPlan =
        exec::test::PlanBuilder()
            .values(data, true)
            .addNode(addBroadcastWriteNode(
                basePath, std::numeric_limits<uint64_t>::max(), serdeLayout))
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
    auto readerPlan =
        exec::test::PlanBuilder().exchange(dataType, "Presto").planNode();
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
      reordered.push_back(
          std::make_shared<RowVector>(
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

  auto readerPlan =
      exec::test::PlanBuilder().exchange(dataType, "Presto").planNode();
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
        std::numeric_limits<uint64_t>::max(),
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
        std::numeric_limits<uint64_t>::max(),
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
        std::numeric_limits<uint64_t>::max(),
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
        std::numeric_limits<uint64_t>::max(),
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
        std::numeric_limits<uint64_t>::max(),
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
        std::numeric_limits<uint64_t>::max(),
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
        std::numeric_limits<uint64_t>::max(),
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
        std::numeric_limits<uint64_t>::max(),
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

namespace {
// Holds a reader inside 'pread' so that a test can call close() while a read is
// still in progress.
class ReadGate {
 public:
  // Arms the gate for the next read only.
  // Long enough that a healthy run never reaches it, short enough that a
  // regression fails the test instead of hanging until the CI timeout.
  static constexpr std::chrono::seconds kProgressTimeout{30};
  // How long close() is given to prove it is blocked.
  static constexpr std::chrono::milliseconds kCloseBlockedFor{200};

  void arm() {
    armed_ = true;
  }

  // Runs on the reader thread from inside pread.
  void awaitCloseAttempt() {
    if (!armed_.exchange(false)) {
      return;
    }
    fired_ = true;
    readStarted_.post();
    sawCloseIssued_ = closeIssued_.try_wait_for(kProgressTimeout);
    // close() cannot return while this read is in flight, so this must time
    // out. A post instead means close() returned early -- the regression.
    closeReturnedDuringRead_ = closeReturned_.try_wait_for(kCloseBlockedFor);
  }

  bool awaitReadStarted() {
    return readStarted_.try_wait_for(kProgressTimeout);
  }

  void issueClose() {
    closeIssued_.post();
  }

  void postCloseReturned() {
    closeReturned_.post();
  }

  bool fired() const {
    return fired_.load();
  }

  bool sawCloseIssued() const {
    return sawCloseIssued_.load();
  }

  bool closeReturnedDuringRead() const {
    return closeReturnedDuringRead_.load();
  }

 private:
  std::atomic_bool armed_{false};
  std::atomic_bool fired_{false};
  std::atomic_bool sawCloseIssued_{false};
  std::atomic_bool closeReturnedDuringRead_{false};
  folly::Baton<> readStarted_;
  folly::Baton<> closeIssued_;
  folly::Baton<> closeReturned_;
};

// Runs 'gate' before delegating each read.
class GatedReadFile : public ReadFile {
 public:
  GatedReadFile(std::unique_ptr<ReadFile> delegate, ReadGate* gate)
      : delegate_(std::move(delegate)), gate_(gate) {}

  std::string_view pread(
      uint64_t offset,
      uint64_t length,
      void* buffer,
      const FileIoContext& context = {}) const override {
    gate_->awaitCloseAttempt();
    return delegate_->pread(offset, length, buffer, context);
  }

  bool shouldCoalesce() const override {
    return delegate_->shouldCoalesce();
  }

  uint64_t size() const override {
    return delegate_->size();
  }

  uint64_t memoryUsage() const override {
    return delegate_->memoryUsage();
  }

  std::string getName() const override {
    return delegate_->getName();
  }

  uint64_t getNaturalReadSize() const override {
    return delegate_->getNaturalReadSize();
  }

 private:
  const std::unique_ptr<ReadFile> delegate_;
  ReadGate* const gate_;
};

// Hands out gated read files over a delegate file system.
class GatedFileSystem : public filesystems::FileSystem {
 public:
  GatedFileSystem(std::shared_ptr<FileSystem> delegate, ReadGate* gate)
      : FileSystem(nullptr), delegate_(std::move(delegate)), gate_(gate) {}

  std::string name() const override {
    return "gated";
  }

  std::unique_ptr<ReadFile> openFileForRead(
      std::string_view path,
      const filesystems::FileOptions& options = {}) override {
    return std::make_unique<GatedReadFile>(
        delegate_->openFileForRead(path, options), gate_);
  }

  std::unique_ptr<WriteFile> openFileForWrite(
      std::string_view path,
      const filesystems::FileOptions& options = {}) override {
    return delegate_->openFileForWrite(path, options);
  }

  void remove(std::string_view path) override {
    delegate_->remove(path);
  }

  void rename(
      std::string_view oldPath,
      std::string_view newPath,
      bool overwrite = false) override {
    delegate_->rename(oldPath, newPath, overwrite);
  }

  bool exists(std::string_view path) override {
    return delegate_->exists(path);
  }

  std::vector<std::string> list(std::string_view path) override {
    return delegate_->list(path);
  }

  void mkdir(
      std::string_view path,
      const filesystems::DirectoryOptions& options = {}) override {
    delegate_->mkdir(path, options);
  }

  void rmdir(std::string_view path) override {
    delegate_->rmdir(path);
  }

 private:
  const std::shared_ptr<FileSystem> delegate_;
  ReadGate* const gate_;
};
} // namespace

TEST_P(BroadcastTest, closeDuringRead) {
  auto tempDirectoryPath = exec::test::TempDirectoryPath::create();
  auto localFileSystem =
      velox::filesystems::getFileSystem(tempDirectoryPath->getPath(), nullptr);
  localFileSystem->mkdir(tempDirectoryPath->getPath());

  // The payload has to stay larger than the reader's 1MB input stream buffer
  // after compression, otherwise the whole file is read up front and no read
  // remains for close() to race. Hashed values do not compress.
  const auto data = makeRowVector({
      makeFlatVector<int64_t>(
          200'000,
          [](auto row) {
            return static_cast<int64_t>(
                row * 6364136223846793005ULL + 1442695040888963407ULL);
          }),
  });
  auto writer = std::make_unique<BroadcastFileWriter>(
      fmt::format(
          "{}/broadcast_close_during_read", tempDirectoryPath->getPath()),
      std::numeric_limits<uint64_t>::max(),
      64 << 10,
      getVectorSerdeOptions(GetParam().compressionKind),
      pool());
  writer->write(data);
  writer->noMoreData();
  const auto filePath = writer->fileStats()
                            ->childAt(0)
                            ->as<SimpleVector<StringView>>()
                            ->valueAt(0)
                            .str();
  // The gate only sees a page read if the file outgrows the reader's input
  // buffer; otherwise the whole file is read up front. Read the bound from the
  // same config the reader uses, so raising the default cannot silently turn
  // this into a test that covers nothing.
  ASSERT_GT(
      localFileSystem->openFileForRead(filePath)->size(),
      SystemConfig::instance()->broadcastExchangeSourceReadBufferBytes());

  ReadGate gate;
  auto broadcastFileInfo = std::make_unique<BroadcastFileInfo>();
  broadcastFileInfo->filePath_ = filePath;
  auto reader = std::make_shared<BroadcastFileReader>(
      broadcastFileInfo,
      std::make_shared<GatedFileSystem>(localFileSystem, &gate),
      pool());

  // Read the footer up front so the gate catches a page read rather than the
  // footer read.
  ASSERT_TRUE(reader->hasNext());
  gate.arm();

  // An exception escaping a std::thread callable calls std::terminate, which
  // would abort the whole binary instead of failing this case -- and throwing
  // is exactly what the regression does. Capture and rethrow on this thread.
  std::exception_ptr readError;
  std::thread reads([&]() {
    try {
      while (reader->next() != nullptr) {
      }
    } catch (...) {
      readError = std::current_exception();
    }
  });

  // Join on every path, including a failed assertion below. Releasing the gate
  // first stops the reader thread waiting on a close that will never come.
  SCOPE_EXIT {
    gate.postCloseReturned();
    if (reads.joinable()) {
      reads.join();
    }
  };

  ASSERT_TRUE(gate.awaitReadStarted()) << "the gate never caught a page read";
  gate.issueClose();
  reader->close();
  gate.postCloseReturned();
  reads.join();

  if (readError != nullptr) {
    std::rethrow_exception(readError);
  }
  ASSERT_TRUE(gate.fired()) << "the gated read never ran";
  ASSERT_TRUE(gate.sawCloseIssued()) << "the gate timed out waiting for close";
  ASSERT_FALSE(gate.closeReturnedDuringRead())
      << "close() returned while a read was still in flight";

  ASSERT_EQ(reader->next(), nullptr);
  ASSERT_TRUE(reader->remainingPageSizes().empty());
}

TEST_P(BroadcastTest, exceedBroadcastFileWriterLimit) {
  auto tempDirectoryPath = exec::test::TempDirectoryPath::create();
  auto fileSystem =
      velox::filesystems::getFileSystem(tempDirectoryPath->getPath(), nullptr);
  fileSystem->mkdir(tempDirectoryPath->getPath());

  auto filePath =
      fmt::format("{}/broadcast_limit_test", tempDirectoryPath->getPath());

  auto testData = makeRowVector({
      makeFlatVector<int32_t>(100, [](auto row) { return row; }),
      makeFlatVector<int64_t>(100, [](auto row) { return row * 10; }),
      makeFlatVector<std::string>(
          100,
          [](auto row) {
            return fmt::format("test_string_with_some_length_{}", row);
          }),
  });

  auto writer = std::make_unique<BroadcastFileWriter>(
      filePath,
      100,
      1024,
      getVectorSerdeOptions(GetParam().compressionKind),
      pool());

  try {
    writer->write(testData);
    FAIL() << "Expected PrestoException to be thrown";
  } catch (const VeloxException& e) {
    EXPECT_EQ(e.errorSource(), velox::error_source::kErrorSourceRuntime);
    EXPECT_EQ(
        e.errorCode(),
        presto::error_code::kExceededLocalBroadcastJoinMemoryLimit);
    EXPECT_TRUE(
        e.message().find(
            "Storage broadcast join exceeded per task broadcast limit") !=
        std::string::npos);
  }
}

TEST_P(BroadcastTest, broadcastJoinExceedLimit) {
  auto tempDirectoryPath = exec::test::TempDirectoryPath::create();

  // Create build side data (data to broadcast)
  auto buildData = makeRowVector({
      makeFlatVector<int32_t>(100, [](auto row) { return row % 10; }),
      makeFlatVector<int64_t>(100, [](auto row) { return row * 100; }),
      makeFlatVector<std::string>(
          100,
          [](auto row) {
            return fmt::format("build_side_string_with_length_{}", row);
          }),
  });

  // Use a very small limit to trigger the exception
  const uint64_t smallLimit = 100; // 100 bytes - too small for the data

  auto writerPlan =
      exec::test::PlanBuilder()
          .values({buildData})
          .addNode(addBroadcastWriteNode(
              tempDirectoryPath->getPath(), smallLimit, std::nullopt))
          .planNode();

  exec::CursorParameters params;
  params.planNode = writerPlan;

  std::unordered_map<std::string, std::string> configs;
  configs[core::QueryConfig::kShuffleCompressionKind] =
      common::compressionKindToString(GetParam().compressionKind);
  params.queryCtx = core::QueryCtx::create(
      executor_.get(), core::QueryConfig(std::move(configs)));

  try {
    auto [writeTaskCursor, writeResults] = exec::test::readCursor(params);
    FAIL() << "Expected PrestoException to be thrown during broadcast write";
  } catch (const VeloxException& e) {
    EXPECT_EQ(e.errorSource(), velox::error_source::kErrorSourceRuntime);
    EXPECT_EQ(
        e.errorCode(),
        presto::error_code::kExceededLocalBroadcastJoinMemoryLimit);
    EXPECT_TRUE(
        e.message().find(
            "Storage broadcast join exceeded per task broadcast limit") !=
        std::string::npos);
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
