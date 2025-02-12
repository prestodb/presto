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

#include <arrow/type.h>
#include <folly/init/Init.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/connectors/hive/HiveConnector.h" // @manual
#include "velox/core/QueryCtx.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h" // @manual
#include "velox/dwio/parquet/reader/PageReader.h"
#include "velox/dwio/parquet/tests/ParquetTestBase.h"
#include "velox/exec/Cursor.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

namespace {

using namespace facebook::velox;
using namespace facebook::velox::common;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::parquet;

class ParquetWriterTest : public ParquetTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
    testutil::TestValue::enable();
    filesystems::registerLocalFileSystem();
    connector::registerConnectorFactory(
        std::make_shared<connector::hive::HiveConnectorFactory>());
    auto hiveConnector =
        connector::getConnectorFactory(
            connector::hive::HiveConnectorFactory::kHiveConnectorName)
            ->newConnector(
                kHiveConnectorId,
                std::make_shared<config::ConfigBase>(
                    std::unordered_map<std::string, std::string>()));
    connector::registerConnector(hiveConnector);
    parquet::registerParquetWriterFactory();
  }

  std::unique_ptr<RowReader> createRowReaderWithSchema(
      const std::unique_ptr<Reader> reader,
      const RowTypePtr& rowType) {
    auto rowReaderOpts = getReaderOpts(rowType);
    auto scanSpec = makeScanSpec(rowType);
    rowReaderOpts.setScanSpec(scanSpec);
    auto rowReader = reader->createRowReader(rowReaderOpts);
    return rowReader;
  };

  std::unique_ptr<facebook::velox::parquet::ParquetReader> createReaderInMemory(
      const dwio::common::MemorySink& sink,
      const dwio::common::ReaderOptions& opts) {
    std::string data(sink.data(), sink.size());
    return std::make_unique<facebook::velox::parquet::ParquetReader>(
        std::make_unique<dwio::common::BufferedInput>(
            std::make_shared<InMemoryReadFile>(std::move(data)),
            opts.memoryPool()),
        opts);
  };

  inline static const std::string kHiveConnectorId = "test-hive";
};

std::vector<CompressionKind> params = {
    CompressionKind::CompressionKind_NONE,
    CompressionKind::CompressionKind_SNAPPY,
    CompressionKind::CompressionKind_ZSTD,
    CompressionKind::CompressionKind_LZ4,
    CompressionKind::CompressionKind_GZIP,
};

TEST_F(ParquetWriterTest, compression) {
  auto schema =
      ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6"},
          {INTEGER(),
           DOUBLE(),
           BIGINT(),
           INTEGER(),
           BIGINT(),
           INTEGER(),
           DOUBLE()});
  const int64_t kRows = 10'000;
  const auto data = makeRowVector({
      makeFlatVector<int32_t>(kRows, [](auto row) { return row + 5; }),
      makeFlatVector<double>(kRows, [](auto row) { return row - 10; }),
      makeFlatVector<int64_t>(kRows, [](auto row) { return row - 15; }),
      makeFlatVector<uint32_t>(kRows, [](auto row) { return row + 20; }),
      makeFlatVector<uint64_t>(kRows, [](auto row) { return row + 25; }),
      makeFlatVector<int32_t>(kRows, [](auto row) { return row + 30; }),
      makeFlatVector<double>(kRows, [](auto row) { return row - 25; }),
  });

  // Create an in-memory writer
  auto sink = std::make_unique<MemorySink>(
      200 * 1024 * 1024,
      dwio::common::FileSink::Options{.pool = leafPool_.get()});
  auto sinkPtr = sink.get();
  facebook::velox::parquet::WriterOptions writerOptions;
  writerOptions.memoryPool = leafPool_.get();
  writerOptions.compressionKind = CompressionKind::CompressionKind_SNAPPY;

  const auto& fieldNames = schema->names();

  for (int i = 0; i < params.size(); i++) {
    writerOptions.columnCompressionsMap[fieldNames[i]] = params[i];
  }

  auto writer = std::make_unique<facebook::velox::parquet::Writer>(
      std::move(sink), writerOptions, rootPool_, schema);
  writer->write(data);
  writer->close();

  dwio::common::ReaderOptions readerOptions{leafPool_.get()};
  auto reader = createReaderInMemory(*sinkPtr, readerOptions);

  ASSERT_EQ(reader->numberOfRows(), kRows);
  ASSERT_EQ(*reader->rowType(), *schema);

  for (int i = 0; i < params.size(); i++) {
    EXPECT_EQ(
        reader->fileMetaData().rowGroup(0).columnChunk(i).compression(),
        (i < params.size()) ? params[i]
                            : CompressionKind::CompressionKind_SNAPPY);
  }

  auto rowReader = createRowReaderWithSchema(std::move(reader), schema);
  assertReadWithReaderAndExpected(schema, *rowReader, data, *leafPool_);
};

TEST_F(ParquetWriterTest, toggleDataPageVersion) {
  auto schema = ROW({"c0"}, {INTEGER()});
  const int64_t kRows = 1;
  const auto data = makeRowVector({
      makeFlatVector<int32_t>(kRows, [](auto row) { return 987; }),
  });

  // Write Parquet test data, then read and return the DataPage
  // (thrift::PageType::type) used.
  const auto testDataPageVersion =
      [&](std::unordered_map<std::string, std::string> configFromFile,
          std::unordered_map<std::string, std::string> sessionProperties) {
        // Create an in-memory writer.
        auto sink = std::make_unique<MemorySink>(
            200 * 1024 * 1024,
            dwio::common::FileSink::Options{.pool = leafPool_.get()});
        auto sinkPtr = sink.get();
        parquet::WriterOptions writerOptions;
        writerOptions.memoryPool = leafPool_.get();

        // Simulate setting of Hive config & connector session properties, then
        // write test data.
        auto connectorConfig = config::ConfigBase(std::move(configFromFile));
        auto connectorSessionProperties =
            config::ConfigBase(std::move(sessionProperties));

        writerOptions.processConfigs(
            connectorConfig, connectorSessionProperties);
        auto writer = std::make_unique<parquet::Writer>(
            std::move(sink), writerOptions, rootPool_, schema);
        writer->write(data);
        writer->close();

        // Read to identify DataPage used.
        dwio::common::ReaderOptions readerOptions{leafPool_.get()};
        auto reader = createReaderInMemory(*sinkPtr, readerOptions);

        auto colChunkPtr = reader->fileMetaData().rowGroup(0).columnChunk(0);
        std::string_view sinkData(sinkPtr->data(), sinkPtr->size());

        auto readFile = std::make_shared<InMemoryReadFile>(sinkData);
        auto file = std::make_shared<ReadFileInputStream>(std::move(readFile));

        auto inputStream = std::make_unique<SeekableFileInputStream>(
            std::move(file),
            colChunkPtr.dataPageOffset(),
            150,
            *leafPool_,
            LogType::TEST);
        auto pageReader = std::make_unique<PageReader>(
            std::move(inputStream),
            *leafPool_,
            colChunkPtr.compression(),
            colChunkPtr.totalCompressedSize());

        return pageReader->readPageHeader().type;
      };

  // Test default behavior - DataPage should be V1.
  ASSERT_EQ(testDataPageVersion({}, {}), thrift::PageType::type::DATA_PAGE);

  // Simulate setting DataPage version to V2 via Hive config from file.
  std::unordered_map<std::string, std::string> configFromFile = {
      {parquet::WriterOptions::kParquetHiveConnectorDataPageVersion, "V2"}};

  ASSERT_EQ(
      testDataPageVersion(configFromFile, {}),
      thrift::PageType::type::DATA_PAGE_V2);

  // Simulate setting DataPage version to V1 via Hive config from file.
  configFromFile = {
      {parquet::WriterOptions::kParquetHiveConnectorDataPageVersion, "V1"}};

  ASSERT_EQ(
      testDataPageVersion(configFromFile, {}),
      thrift::PageType::type::DATA_PAGE);

  // Simulate setting DataPage version to V2 via connector session property.
  std::unordered_map<std::string, std::string> sessionProperties = {
      {parquet::WriterOptions::kParquetSessionDataPageVersion, "V2"}};

  ASSERT_EQ(
      testDataPageVersion({}, sessionProperties),
      thrift::PageType::type::DATA_PAGE_V2);

  // Simulate setting DataPage version to V1 via connector session property.
  sessionProperties = {
      {parquet::WriterOptions::kParquetSessionDataPageVersion, "V1"}};

  ASSERT_EQ(
      testDataPageVersion({}, sessionProperties),
      thrift::PageType::type::DATA_PAGE);

  // Simulate setting DataPage version to V1 via connector session property,
  // and to V2 via Hive config from file. Session property should take
  // precedence.
  sessionProperties = {
      {parquet::WriterOptions::kParquetSessionDataPageVersion, "V1"}};
  configFromFile = {
      {parquet::WriterOptions::kParquetHiveConnectorDataPageVersion, "V2"}};

  ASSERT_EQ(
      testDataPageVersion({}, sessionProperties),
      thrift::PageType::type::DATA_PAGE);

  // Simulate setting DataPage version to V2 via connector session property,
  // and to V1 via Hive config from file. Session property should take
  // precedence.
  sessionProperties = {
      {parquet::WriterOptions::kParquetSessionDataPageVersion, "V2"}};
  configFromFile = {
      {parquet::WriterOptions::kParquetHiveConnectorDataPageVersion, "V1"}};

  ASSERT_EQ(
      testDataPageVersion({}, sessionProperties),
      thrift::PageType::type::DATA_PAGE_V2);
}

DEBUG_ONLY_TEST_F(ParquetWriterTest, unitFromWriterOptions) {
  SCOPED_TESTVALUE_SET(
      "facebook::velox::parquet::Writer::write",
      std::function<void(const ::arrow::Schema*)>(
          ([&](const ::arrow::Schema* arrowSchema) {
            const auto tsType =
                std::dynamic_pointer_cast<::arrow::TimestampType>(
                    arrowSchema->field(0)->type());
            ASSERT_EQ(tsType->unit(), ::arrow::TimeUnit::MICRO);
            ASSERT_EQ(tsType->timezone(), "America/Los_Angeles");
          })));

  const auto data = makeRowVector({makeFlatVector<Timestamp>(
      10'000, [](auto row) { return Timestamp(row, row); })});
  parquet::WriterOptions writerOptions;
  writerOptions.memoryPool = leafPool_.get();
  writerOptions.parquetWriteTimestampUnit = TimestampPrecision::kMicroseconds;
  writerOptions.parquetWriteTimestampTimeZone = "America/Los_Angeles";

  // Create an in-memory writer.
  auto sink = std::make_unique<MemorySink>(
      200 * 1024 * 1024,
      dwio::common::FileSink::Options{.pool = leafPool_.get()});
  auto writer = std::make_unique<parquet::Writer>(
      std::move(sink), writerOptions, rootPool_, ROW({"c0"}, {TIMESTAMP()}));
  writer->write(data);
  writer->close();
};

TEST_F(ParquetWriterTest, parquetWriteTimestampTimeZoneWithDefault) {
  SCOPED_TESTVALUE_SET(
      "facebook::velox::parquet::Writer::write",
      std::function<void(const ::arrow::Schema*)>(
          ([&](const ::arrow::Schema* arrowSchema) {
            const auto tsType =
                std::dynamic_pointer_cast<::arrow::TimestampType>(
                    arrowSchema->field(0)->type());
            ASSERT_EQ(tsType->unit(), ::arrow::TimeUnit::MICRO);
            ASSERT_EQ(tsType->timezone(), "");
          })));

  const auto data = makeRowVector({makeFlatVector<Timestamp>(
      10'000, [](auto row) { return Timestamp(row, row); })});
  parquet::WriterOptions writerOptions;
  writerOptions.memoryPool = leafPool_.get();
  writerOptions.parquetWriteTimestampUnit = TimestampPrecision::kMicroseconds;

  // Create an in-memory writer.
  auto sink = std::make_unique<MemorySink>(
      200 * 1024 * 1024,
      dwio::common::FileSink::Options{.pool = leafPool_.get()});
  auto writer = std::make_unique<parquet::Writer>(
      std::move(sink), writerOptions, rootPool_, ROW({"c0"}, {TIMESTAMP()}));
  writer->write(data);
  writer->close();
};

TEST_F(ParquetWriterTest, updateWriterOptionsFromHiveConfig) {
  std::unordered_map<std::string, std::string> configFromFile = {
      {parquet::WriterOptions::kParquetSessionWriteTimestampUnit, "3"}};
  const config::ConfigBase connectorConfig(std::move(configFromFile));
  const config::ConfigBase connectorSessionProperties({});

  parquet::WriterOptions options;
  options.compressionKind = facebook::velox::common::CompressionKind_ZLIB;

  options.processConfigs(connectorConfig, connectorSessionProperties);

  ASSERT_EQ(
      options.parquetWriteTimestampUnit.value(),
      TimestampPrecision::kMilliseconds);
}

#ifdef VELOX_ENABLE_PARQUET
DEBUG_ONLY_TEST_F(ParquetWriterTest, timestampUnitAndTimeZone) {
  SCOPED_TESTVALUE_SET(
      "facebook::velox::parquet::Writer::write",
      std::function<void(const ::arrow::Schema*)>(
          ([&](const ::arrow::Schema* arrowSchema) {
            const auto tsType =
                std::dynamic_pointer_cast<::arrow::TimestampType>(
                    arrowSchema->field(0)->type());
            ASSERT_EQ(tsType->unit(), ::arrow::TimeUnit::MICRO);
          })));

  SCOPED_TESTVALUE_SET(
      "facebook::velox::parquet::Writer::Writer",
      std::function<void(const ArrowOptions* options)>(
          ([&](const ArrowOptions* options) {
            ASSERT_TRUE(options->timestampTimeZone.has_value());
            ASSERT_EQ(options->timestampTimeZone.value(), "America/New_York");
          })));

  const auto data = makeRowVector({makeFlatVector<Timestamp>(
      10'000, [](auto row) { return Timestamp(row, row); })});
  const auto outputDirectory = TempDirectoryPath::create();

  auto writerOptions = std::make_shared<parquet::WriterOptions>();
  writerOptions->parquetWriteTimestampUnit = TimestampPrecision::kMicroseconds;

  const auto plan = PlanBuilder()
                        .values({data})
                        .tableWrite(
                            outputDirectory->getPath(),
                            dwio::common::FileFormat::PARQUET,
                            {},
                            writerOptions)
                        .planNode();
  AssertQueryBuilder(plan)
      .config(core::QueryConfig::kSessionTimezone, "America/New_York")
      .copyResults(pool_.get());
}
#endif

} // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
