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
#include "presto_cpp/main/common/Exception.h"
#include "presto_cpp/main/operators/BroadcastExchangeSource.h"
#include "presto_cpp/main/operators/BroadcastFile.h"
#include "presto_cpp/main/operators/BroadcastWrite.h"
#include "presto_cpp/main/operators/tests/PlanBuilder.h"
#include "velox/buffer/Buffer.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/compression/Compression.h"
#include "velox/common/encode/Base64.h"
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

TEST_P(BroadcastTest, broadcastFileInfoCarriesDescriptor) {
  // The reader must recover the exact bytes, including any embedded NUL.
  const std::string rawDescriptor =
      std::string("\x01\x00\x02", 3) + "descriptor-bytes";

  const auto fileInfo = BroadcastFileInfo::deserialize(
      fmt::format(
          R"({{"filePath": "/tmp/file.bin", "descriptor": "{}"}})",
          encoding::Base64::encodeUrl(rawDescriptor)));

  EXPECT_EQ(fileInfo->filePath_, "/tmp/file.bin");
  EXPECT_EQ(fileInfo->descriptor_, rawDescriptor);
}

TEST_P(BroadcastTest, broadcastFileInfoSurvivesQueryParsing) {
  // folly matches a query value with `[^=&]*`, so padded base64 would drop
  // `broadcastInfo` entirely for two thirds of all descriptor lengths.
  for (size_t descriptorSize = 1; descriptorSize <= 6; ++descriptorSize) {
    SCOPED_TRACE(fmt::format("descriptorSize={}", descriptorSize));
    const std::string rawDescriptor(descriptorSize, '\xff');
    const auto serializedFileInfo = fmt::format(
        R"({{"filePath": "/tmp/file.bin", "descriptor": "{}"}})",
        encoding::Base64::encodeUrl(rawDescriptor));

    // Not const: getQueryParams() populates lazily.
    folly::Uri uri(
        fmt::format("batch://task?broadcastInfo={}", serializedFileInfo));
    std::string broadcastInfo;
    for (const auto& [name, value] : uri.getQueryParams()) {
      if (name == "broadcastInfo") {
        broadcastInfo = value;
      }
    }

    ASSERT_FALSE(broadcastInfo.empty());
    EXPECT_EQ(
        BroadcastFileInfo::deserialize(broadcastInfo)->descriptor_,
        rawDescriptor);
  }
}

TEST_P(BroadcastTest, redactBroadcastInfoHidesTheDescriptor) {
  // The descriptor carries Warm Storage bearer tokens, so no log or exception
  // may quote it, while the rest of the payload stays readable.
  // Re-serialized from the parsed payload, so spacing is normalised and key
  // order is preserved.
  const auto redacted = redactBroadcastInfo(
      R"({"filePath": "/tmp/file.bin", "descriptor": "c2VjcmV0LXRva2Vu"})");

  EXPECT_EQ(
      redacted, R"({"filePath":"/tmp/file.bin","descriptor":"<redacted>"})");

  // Payloads with nothing to hide are returned unchanged.
  for (const auto& untouched :
       {R"({"filePath": "/tmp/file.bin"})",
        R"({"filePath": "/tmp/file.bin", "descriptor": null})",
        R"({"filePath": "/tmp/file.bin", "descriptor": 42})"}) {
    SCOPED_TRACE(untouched);
    EXPECT_EQ(redactBroadcastInfo(untouched), untouched);
  }

  // A pretty-printed payload must be redacted just the same rather than passed
  // through intact.
  EXPECT_EQ(
      redactBroadcastInfo(
          "{\n  \"filePath\": \"/tmp/file.bin\",\n  \"descriptor\"\n  :\n  \"c2VjcmV0\"\n}"),
      R"({"filePath":"/tmp/file.bin","descriptor":"<redacted>"})");

  // A descriptor whose value cannot be located fails safe: the whole payload
  // is withheld rather than emitted on the chance it still holds a token.
  for (const auto& truncated :
       {R"({"filePath": "/tmp/file.bin", "descriptor")",
        R"({"filePath": "/tmp/file.bin", "descriptor": "unterminated)"}) {
    SCOPED_TRACE(truncated);
    EXPECT_EQ(redactBroadcastInfo(truncated), "<redacted>");
  }

  // The exception-text caller does not pass JSON at all. Text quoting a
  // descriptor is withheld whole; text without one stays readable.
  EXPECT_EQ(
      redactBroadcastInfo(
          R"(parse error at 1: {"filePath":"/f","descriptor":"c2VjcmV0"})"),
      "<redacted>");
  EXPECT_EQ(
      redactBroadcastInfo("parse error at line 1: unexpected end of input"),
      "parse error at line 1: unexpected end of input");
}

TEST_P(BroadcastTest, broadcastFileInfoWithMalformedDescriptor) {
  // An unusable descriptor must degrade to a path open, not fail the read --
  // whether it is undecodable or not even a string.
  for (const auto& serializedFileInfo :
       {R"({"filePath": "/tmp/file.bin", "descriptor": "!!!not-base64!!!"})",
        R"({"filePath": "/tmp/file.bin", "descriptor": 42})",
        R"({"filePath": "/tmp/file.bin", "descriptor": {"a": 1}})"}) {
    SCOPED_TRACE(serializedFileInfo);
    const auto fileInfo = BroadcastFileInfo::deserialize(serializedFileInfo);
    EXPECT_EQ(fileInfo->filePath_, "/tmp/file.bin");
    EXPECT_TRUE(fileInfo->descriptor_.empty());
  }
}

TEST_P(BroadcastTest, broadcastFileInfoWithoutDescriptor) {
  // Older writers omit the field; a file system with no handle emits an empty
  // one. Both leave the reader to open by path.
  for (const auto& serializedFileInfo :
       {R"({"filePath": "/tmp/file.bin"})",
        R"({"filePath": "/tmp/file.bin", "descriptor": ""})",
        R"({"filePath": "/tmp/file.bin", "descriptor": null})"}) {
    SCOPED_TRACE(serializedFileInfo);
    const auto fileInfo = BroadcastFileInfo::deserialize(serializedFileInfo);
    EXPECT_EQ(fileInfo->filePath_, "/tmp/file.bin");
    EXPECT_TRUE(fileInfo->descriptor_.empty());
  }
}

namespace {
// Rejects any open that carries a handle, standing in for a file system that
// refuses a stale descriptor, and delegates everything else.
class DescriptorRejectingFileSystem : public velox::filesystems::FileSystem {
 public:
  explicit DescriptorRejectingFileSystem(
      std::shared_ptr<velox::filesystems::FileSystem> delegate)
      : velox::filesystems::FileSystem(nullptr),
        delegate_(std::move(delegate)) {}

  std::string name() const override {
    return "DescriptorRejecting";
  }

  std::unique_ptr<velox::ReadFile> openFileForRead(
      std::string_view path,
      const velox::filesystems::FileOptions& options = {}) override {
    VELOX_CHECK_NULL(options.extraFileInfo, "handle rejected by the test");
    return delegate_->openFileForRead(path, options);
  }

  std::unique_ptr<velox::WriteFile> openFileForWrite(
      std::string_view path,
      const velox::filesystems::FileOptions& options = {}) override {
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
      const velox::filesystems::DirectoryOptions& options = {}) override {
    delegate_->mkdir(path, options);
  }

  void rmdir(std::string_view path) override {
    delegate_->rmdir(path);
  }

 private:
  const std::shared_ptr<velox::filesystems::FileSystem> delegate_;
};
} // namespace

TEST_P(BroadcastTest, broadcastReaderFallsBackWhenTheHandleIsRejected) {
  auto tempDirectoryPath = exec::test::TempDirectoryPath::create();
  auto fileSystem =
      velox::filesystems::getFileSystem(tempDirectoryPath->getPath(), nullptr);
  fileSystem->mkdir(tempDirectoryPath->getPath());

  auto writer = std::make_unique<BroadcastFileWriter>(
      fmt::format("{}/broadcast_stale_handle", tempDirectoryPath->getPath()),
      std::numeric_limits<uint64_t>::max(),
      1 << 20,
      getVectorSerdeOptions(GetParam().compressionKind),
      pool());
  const auto data =
      makeRowVector({makeFlatVector<int32_t>(8, [](auto row) { return row; })});
  writer->write(data);
  writer->noMoreData();
  const auto filePath = writer->fileStats()
                            ->childAt(0)
                            ->as<SimpleVector<StringView>>()
                            ->valueAt(0)
                            .str();

  auto fileInfo = std::make_unique<BroadcastFileInfo>();
  fileInfo->filePath_ = filePath;
  fileInfo->descriptor_ = "a-stale-handle";
  auto reader = std::make_shared<BroadcastFileReader>(
      fileInfo,
      std::make_shared<DescriptorRejectingFileSystem>(fileSystem),
      pool());

  // The rejected handle must degrade to a path open rather than fail the read.
  const auto pageSizes = reader->remainingPageSizes();
  EXPECT_FALSE(pageSizes.empty());

  const auto metrics = reader->metrics();
  EXPECT_EQ(metrics.at("broadcastExchangeSource.descriptorOpenCount").sum, 0);
  EXPECT_EQ(metrics.at("broadcastExchangeSource.pathOpenCount").sum, 1);

  // The footer read that failed mid-way must not leave stale page sizes behind.
  uint32_t pagesRead = 0;
  while (reader->hasNext()) {
    ASSERT_NE(reader->next(), nullptr);
    ++pagesRead;
  }
  EXPECT_EQ(pagesRead, pageSizes.size());
}

TEST_P(BroadcastTest, broadcastReaderCountsItsOpenPath) {
  auto tempDirectoryPath = exec::test::TempDirectoryPath::create();
  auto fileSystem =
      velox::filesystems::getFileSystem(tempDirectoryPath->getPath(), nullptr);
  fileSystem->mkdir(tempDirectoryPath->getPath());

  auto writer = std::make_unique<BroadcastFileWriter>(
      fmt::format("{}/broadcast_open_counts", tempDirectoryPath->getPath()),
      std::numeric_limits<uint64_t>::max(),
      1 << 20,
      getVectorSerdeOptions(GetParam().compressionKind),
      pool());
  writer->write(makeRowVector(
      {makeFlatVector<int32_t>(4, [](auto row) { return row; })}));
  writer->noMoreData();
  const auto filePath = writer->fileStats()
                            ->childAt(0)
                            ->as<SimpleVector<StringView>>()
                            ->valueAt(0)
                            .str();

  const auto openCounts = [&](const std::string& descriptor) {
    auto fileInfo = std::make_unique<BroadcastFileInfo>();
    fileInfo->filePath_ = filePath;
    fileInfo->descriptor_ = descriptor;
    auto reader =
        std::make_shared<BroadcastFileReader>(fileInfo, fileSystem, pool());
    reader->remainingPageSizes();
    const auto metrics = reader->metrics();
    return std::make_pair(
        metrics.at("broadcastExchangeSource.descriptorOpenCount").sum,
        metrics.at("broadcastExchangeSource.pathOpenCount").sum);
  };

  // No handle from the writer, so the reader has to resolve the file by path.
  EXPECT_EQ(openCounts(""), std::make_pair(int64_t{0}, int64_t{1}));
  // A handle present, so the reader takes the descriptor branch instead.
  EXPECT_EQ(openCounts("handle"), std::make_pair(int64_t{1}, int64_t{0}));
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
    ASSERT_EQ(fileStats->childrenSize(), 4);

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

    // The local file system cannot produce a reusable handle, so the reader is
    // left to open the broadcast file by path.
    auto descriptor =
        fileStats->childAt(3)->as<SimpleVector<StringView>>()->valueAt(0);
    ASSERT_TRUE(descriptor.empty());
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
