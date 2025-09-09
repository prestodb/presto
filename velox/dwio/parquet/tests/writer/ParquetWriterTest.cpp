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
#include "velox/dwio/parquet/writer/arrow/tests/TestUtil.h"

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/connectors/hive/HiveConnector.h" // @manual
#include "velox/core/QueryCtx.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
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
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    testutil::TestValue::enable();
    filesystems::registerLocalFileSystem();
    connector::hive::HiveConnectorFactory factory;
    auto hiveConnector = factory.newConnector(
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

class ArrowMemoryPool final : public ::arrow::MemoryPool {
 public:
  explicit ArrowMemoryPool() : allocated_(0) {}

  ~ArrowMemoryPool() = default;

  ::arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out)
      override {
    *out = reinterpret_cast<uint8_t*>(malloc(size));
    VELOX_CHECK_NOT_NULL(*out, "Failed to allocate memory in ArrowMemoryPool.");

    allocated_ += size;
    return ::arrow::Status::OK();
  }

  ::arrow::Status Reallocate(
      int64_t oldSize,
      int64_t newSize,
      int64_t alignment,
      uint8_t** ptr) override {
    uint8_t* newBuffer = reinterpret_cast<uint8_t*>(realloc(*ptr, newSize));
    VELOX_CHECK_NOT_NULL(
        newBuffer, "Failed to reallocate memory in ArrowMemoryPool.");

    *ptr = newBuffer;
    allocated_ = allocated_ - oldSize + newSize;
    return ::arrow::Status::OK();
  }

  void Free(uint8_t* buffer, int64_t size, int64_t alignment) override {
    free(buffer);
    allocated_ -= size;
  }

  int64_t bytes_allocated() const override {
    return allocated_;
    ;
  }

  int64_t max_memory() const override {
    VELOX_UNSUPPORTED("ArrowMemoryPool#max_memory() unsupported");
  }

  int64_t total_bytes_allocated() const override {
    VELOX_UNSUPPORTED("ArrowMemoryPool#total_bytes_allocated() unsupported");
  }

  int64_t num_allocations() const override {
    VELOX_UNSUPPORTED("ArrowMemoryPool#num_allocations() unsupported");
  }

  std::string backend_name() const override {
    return "arrow memory pool";
  }

 private:
  int64_t allocated_;
};

std::vector<CompressionKind> params = {
    CompressionKind::CompressionKind_NONE,
    CompressionKind::CompressionKind_SNAPPY,
    CompressionKind::CompressionKind_ZSTD,
    CompressionKind::CompressionKind_LZ4,
    CompressionKind::CompressionKind_GZIP,
};

TEST_F(ParquetWriterTest, dictionaryEncodingWithDictionaryPageSize) {
  const auto schema = ROW({"c0"}, {SMALLINT()});
  constexpr int64_t kRows = 10'000;
  const auto data = makeRowVector({
      makeFlatVector<int16_t>(kRows, [](auto row) { return row + 1; }),
  });

  // Write Parquet test data, then read and return the DataPage
  // (thrift::PageType::type) used.
  const auto testEnableDictionaryAndDictionaryPageSizeToGetPageHeader =
      [&](std::unordered_map<std::string, std::string> configFromFile,
          std::unordered_map<std::string, std::string> sessionProperties,
          bool isFirstPageOrSecondPage) {
        // Create an in-memory writer.
        auto sink = std::make_unique<MemorySink>(
            200 * 1024 * 1024,
            dwio::common::FileSink::Options{.pool = leafPool_.get()});
        auto sinkPtr = sink.get();
        parquet::WriterOptions writerOptions;
        writerOptions.memoryPool = leafPool_.get();

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

        if (isFirstPageOrSecondPage) {
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
          return pageReader->readPageHeader();
        }
        constexpr int64_t kFirstDataPageCompressedSize = 1291;
        constexpr int64_t kFirstDataPageHeaderSize = 48;
        auto inputStream = std::make_unique<SeekableFileInputStream>(
            std::move(file),
            colChunkPtr.dataPageOffset() + kFirstDataPageCompressedSize +
                kFirstDataPageHeaderSize,
            150,
            *leafPool_,
            LogType::TEST);
        auto pageReader = std::make_unique<PageReader>(
            std::move(inputStream),
            *leafPool_,
            colChunkPtr.compression(),
            colChunkPtr.totalCompressedSize());
        return pageReader->readPageHeader();
      };

  // Test default config (i.e., no explicit config)

  const std::unordered_map<std::string, std::string> defaultConfigFromFile;
  const std::unordered_map<std::string, std::string>
      defaultSessionPropertiesFromFile;

  const auto defaultHeader =
      testEnableDictionaryAndDictionaryPageSizeToGetPageHeader(
          defaultConfigFromFile, defaultSessionPropertiesFromFile, true);
  // We use the default version of data page (V1)
  EXPECT_EQ(defaultHeader.type, thrift::PageType::type::DATA_PAGE);
  // Dictionary encoding is enabled as default
  EXPECT_EQ(
      defaultHeader.data_page_header.encoding,
      thrift::Encoding::RLE_DICTIONARY);
  // Default dictionary page size is 1MB (same as data page size), so it can
  // contain a dictionary for all values. So all data will be in the first
  // data page
  EXPECT_EQ(defaultHeader.data_page_header.num_values, kRows);

  // Test normal config

  // Set the dictionary page size limit to 1B so that the first data page will
  // only contain one batch of data encoded with dictionary, and from the
  // second batch it falls back to PLAIN encoding. If not set the dictionary
  // page size limit, the default is 1MB (same as data page default size) then
  // there will be only one data page contains all data encoded with dictionary
  const std::unordered_map<std::string, std::string> normalConfigFromFile = {
      {parquet::WriterOptions::kParquetHiveConnectorEnableDictionary, "true"},
      {parquet::WriterOptions::kParquetHiveConnectorDictionaryPageSizeLimit,
       "1B"},
  };
  const std::unordered_map<std::string, std::string> normalSessionProperties = {
      {parquet::WriterOptions::kParquetSessionEnableDictionary, "true"},
      {parquet::WriterOptions::kParquetSessionDictionaryPageSizeLimit, "1B"},
  };

  // Here we are reading the second data page. If we don't set the dictionary
  // page size, then there will be only one data page (See the comments above
  // the declaration of configFromFile)
  const auto normalHeader =
      testEnableDictionaryAndDictionaryPageSizeToGetPageHeader(
          normalConfigFromFile, normalSessionProperties, false);

  // We use the default version of data page (V1)
  EXPECT_EQ(normalHeader.type, thrift::PageType::type::DATA_PAGE);
  // The second data page will fall back to PLAIN encoding
  EXPECT_EQ(normalHeader.data_page_header.encoding, thrift::Encoding::PLAIN);

  // Test incorrect enable dictionary config

  const std::string invalidEnableDictionaryValue{"NaB"};
  const std::unordered_map<std::string, std::string>
      incorrectEnableDictionaryConfigFromFile = {
          {parquet::WriterOptions::kParquetHiveConnectorEnableDictionary,
           invalidEnableDictionaryValue},
      };
  const std::unordered_map<std::string, std::string>
      incorrectEnableDictionarySessionProperties = {
          {parquet::WriterOptions::kParquetSessionEnableDictionary,
           invalidEnableDictionaryValue},
      };

  // Values cannot be parsed so that the exception is thrown
  VELOX_ASSERT_THROW(
      testEnableDictionaryAndDictionaryPageSizeToGetPageHeader(
          incorrectEnableDictionaryConfigFromFile,
          incorrectEnableDictionarySessionProperties,
          true),
      fmt::format(
          "Invalid parquet writer enable dictionary option: Non-whitespace character found after end of conversion: \"{}\"",
          invalidEnableDictionaryValue.substr(1)));

  // Test incorrect dictionary page size config

  const std::string invalidDictionaryPageSizeValue{"NaN"};
  const std::unordered_map<std::string, std::string>
      incorrectDictionaryPageSizeConfigFromFile = {
          {parquet::WriterOptions::kParquetHiveConnectorDictionaryPageSizeLimit,
           invalidDictionaryPageSizeValue},
      };
  const std::unordered_map<std::string, std::string>
      incorrectDictionaryPageSizeSessionProperties = {
          {parquet::WriterOptions::kParquetSessionDictionaryPageSizeLimit,
           invalidDictionaryPageSizeValue},
      };

  // Values cannot be parsed so that the exception is thrown
  VELOX_ASSERT_THROW(
      testEnableDictionaryAndDictionaryPageSizeToGetPageHeader(
          incorrectDictionaryPageSizeConfigFromFile,
          incorrectDictionaryPageSizeSessionProperties,
          true),
      fmt::format(
          "Invalid capacity string '{}'", invalidDictionaryPageSizeValue));
}

TEST_F(ParquetWriterTest, dictionaryEncodingOff) {
  const auto schema = ROW({"c0"}, {SMALLINT()});
  constexpr int64_t kRows = 10'000;
  const auto data = makeRowVector({
      makeFlatVector<int16_t>(kRows, [](auto row) { return row + 1; }),
  });

  // Write Parquet test data, then read and return the DataPage
  // (thrift::PageType::type) used.
  const auto testEnableDictionaryAndDictionaryPageSizeToGetPageHeader =
      [&](std::unordered_map<std::string, std::string> configFromFile,
          std::unordered_map<std::string, std::string> sessionProperties) {
        // Create an in-memory writer.
        auto sink = std::make_unique<MemorySink>(
            200 * 1024 * 1024,
            dwio::common::FileSink::Options{.pool = leafPool_.get()});
        auto sinkPtr = sink.get();
        parquet::WriterOptions writerOptions;
        writerOptions.memoryPool = leafPool_.get();

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
        return pageReader->readPageHeader();
      };

  // Test only dictionary off without dictionary page size configured

  const std::unordered_map<std::string, std::string>
      withoutPageSizeConfigFromFile = {
          {parquet::WriterOptions::kParquetHiveConnectorEnableDictionary,
           "false"},
      };
  const std::unordered_map<std::string, std::string>
      withoutPageSizeSessionProperties = {
          {parquet::WriterOptions::kParquetSessionEnableDictionary, "false"},
      };

  const auto withoutPageSizeHeader =
      testEnableDictionaryAndDictionaryPageSizeToGetPageHeader(
          withoutPageSizeConfigFromFile, withoutPageSizeSessionProperties);

  // We use the default version of data page (V1)
  EXPECT_EQ(withoutPageSizeHeader.type, thrift::PageType::type::DATA_PAGE);
  // Since we turn off the dictionary encoding, and the default data page size
  // is 1MB, there is only one page, and its encoding should be PLAIN, which
  // means the configuration is applied
  EXPECT_EQ(
      withoutPageSizeHeader.data_page_header.encoding, thrift::Encoding::PLAIN);
  // All rows will be on the only data page, this is a sanity check
  EXPECT_EQ(withoutPageSizeHeader.data_page_header.num_values, kRows);

  // Test dictionary off but with dictionary page size configured

  const std::unordered_map<std::string, std::string>
      withPageSizeConfigFromFile = {
          {parquet::WriterOptions::kParquetHiveConnectorEnableDictionary,
           "false"},
          {parquet::WriterOptions::kParquetHiveConnectorDictionaryPageSizeLimit,
           "1B"},
      };
  const std::unordered_map<std::string, std::string>
      withPageSizeSessionProperties = {
          {parquet::WriterOptions::kParquetSessionEnableDictionary, "false"},
          {parquet::WriterOptions::kParquetSessionDictionaryPageSizeLimit,
           "1B"},
      };

  const auto withPageSizeHeader =
      testEnableDictionaryAndDictionaryPageSizeToGetPageHeader(
          withPageSizeConfigFromFile, withPageSizeSessionProperties);

  // Should be the same as without dictionary page size configured, because
  // when the dictionary is disabled, the dictionary page silze is meaningless
  EXPECT_EQ(withPageSizeHeader.type, thrift::PageType::type::DATA_PAGE);
  EXPECT_EQ(
      withPageSizeHeader.data_page_header.encoding, thrift::Encoding::PLAIN);
  EXPECT_EQ(withPageSizeHeader.data_page_header.num_values, kRows);
}

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

TEST_F(ParquetWriterTest, testPageSizeAndBatchSizeConfiguration) {
  const auto schema = ROW({"c0"}, {SMALLINT()});
  constexpr int64_t kRows = 10'000;
  const auto data = makeRowVector({
      makeFlatVector<int16_t>(kRows, [](auto row) { return row + 1; }),
  });

  // Write Parquet test data, then read and return the DataPage
  // (thrift::PageType::type) used.
  const auto testPageSizeAndBatchSizeToGetPageHeader =
      [&](std::unordered_map<std::string, std::string> configFromFile,
          std::unordered_map<std::string, std::string> sessionProperties) {
        // Create an in-memory writer.
        auto sink = std::make_unique<MemorySink>(
            200 * 1024 * 1024,
            dwio::common::FileSink::Options{.pool = leafPool_.get()});
        auto sinkPtr = sink.get();
        parquet::WriterOptions writerOptions;
        writerOptions.memoryPool = leafPool_.get();

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
        return pageReader->readPageHeader();
      };

  // Test default config (i.e., no explicit config)

  const std::unordered_map<std::string, std::string> defaultConfigFromFile;
  const std::unordered_map<std::string, std::string>
      defaultSessionPropertiesFromFile;

  const auto defaultHeader = testPageSizeAndBatchSizeToGetPageHeader(
      defaultConfigFromFile, defaultSessionPropertiesFromFile);
  // We use the default version of data page (V1)
  EXPECT_EQ(defaultHeader.type, thrift::PageType::type::DATA_PAGE);
  // We don't use compressor here
  EXPECT_EQ(
      defaultHeader.uncompressed_page_size, defaultHeader.compressed_page_size);
  // The default page size is 1MB, which can actually contains all data in one
  // page
  EXPECT_EQ(defaultHeader.compressed_page_size, 17529);
  // As mentioned above, the default page size can contain all data in one page
  // so the number of values of the first page equals to the total number
  EXPECT_EQ(defaultHeader.data_page_header.num_values, kRows);

  // Test normal config

  // We use 97 as the batch size to test, because 97 is a prime, if the number
  // of values in each page can be divided by 97, it means the batch size is
  // applied (default is 1024)
  const std::unordered_map<std::string, std::string> normalConfigFromFile = {
      {parquet::WriterOptions::kParquetHiveConnectorWritePageSize, "2KB"},
      {parquet::WriterOptions::kParquetHiveConnectorWriteBatchSize, "97"},
  };
  const std::unordered_map<std::string, std::string> normalSessionProperties = {
      {parquet::WriterOptions::kParquetSessionWritePageSize, "2KB"},
      {parquet::WriterOptions::kParquetSessionWriteBatchSize, "97"},
  };
  const auto normalHeader = testPageSizeAndBatchSizeToGetPageHeader(
      normalConfigFromFile, normalSessionProperties);
  // We use the default version of data page (V1)
  EXPECT_EQ(normalHeader.type, thrift::PageType::type::DATA_PAGE);
  // We don't use compressor here
  EXPECT_EQ(
      normalHeader.uncompressed_page_size, normalHeader.compressed_page_size);
  // 1485B < 2KB < 1MB, which means the page size is applied (default is 1MB)
  EXPECT_EQ(normalHeader.compressed_page_size, 1485);
  // 1067 % 97 == 0, which means the batch size is applied (default is 1024)
  EXPECT_EQ(normalHeader.data_page_header.num_values, 1067);

  // Test incorrect page size config

  const std::string invalidPageSizeAndBatchSizeValue{"NaN"};
  const std::unordered_map<std::string, std::string>
      incorrectPageSizeConfigFromFile = {
          {parquet::WriterOptions::kParquetHiveConnectorWritePageSize,
           invalidPageSizeAndBatchSizeValue},
      };
  const std::unordered_map<std::string, std::string>
      incorrectPageSizeSessionPropertiesFromFile = {
          {parquet::WriterOptions::kParquetSessionWritePageSize,
           invalidPageSizeAndBatchSizeValue},
      };

  // Values cannot be parsed so that the exception is thrown
  VELOX_ASSERT_THROW(
      testPageSizeAndBatchSizeToGetPageHeader(
          incorrectPageSizeConfigFromFile,
          incorrectPageSizeSessionPropertiesFromFile),
      fmt::format(
          "Invalid capacity string '{}'", invalidPageSizeAndBatchSizeValue))

  // Test incorrect batch size config

  const std::unordered_map<std::string, std::string>
      incorrectBatchSizeConfigFromFile = {
          {parquet::WriterOptions::kParquetHiveConnectorWriteBatchSize,
           invalidPageSizeAndBatchSizeValue},
      };
  const std::unordered_map<std::string, std::string>
      incorrectBatchSizeSessionPropertiesFromFile = {
          {parquet::WriterOptions::kParquetSessionWriteBatchSize,
           invalidPageSizeAndBatchSizeValue},
      };

  // Values cannot be parsed so that the exception is thrown
  VELOX_ASSERT_THROW(
      testPageSizeAndBatchSizeToGetPageHeader(
          incorrectBatchSizeConfigFromFile,
          incorrectBatchSizeSessionPropertiesFromFile),
      fmt::format(
          "Invalid parquet writer batch size: Invalid leading character: \"{}\"",
          invalidPageSizeAndBatchSizeValue));
}

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

DEBUG_ONLY_TEST_F(ParquetWriterTest, parquetWriteTimestampTimeZoneWithDefault) {
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

TEST_F(ParquetWriterTest, parquetWriteWithArrowMemoryPool) {
  const auto data = makeRowVector({makeFlatVector<Timestamp>(
      10'000, [](auto row) { return Timestamp(row, row); })});
  parquet::WriterOptions writerOptions;
  writerOptions.memoryPool = leafPool_.get();
  writerOptions.arrowMemoryPool = std::make_shared<ArrowMemoryPool>();

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

TEST_F(ParquetWriterTest, dictionaryEncodedVector) {
  const auto randomIndices = [this](vector_size_t size) {
    BufferPtr indices =
        AlignedBuffer::allocate<vector_size_t>(size, leafPool_.get());
    auto rawIndices = indices->asMutable<vector_size_t>();
    for (int32_t i = 0; i < size; i++) {
      rawIndices[i] = folly::Random::rand32(size);
    }
    return indices;
  };

  const auto wrapDictionaryVectors =
      [&](const std::vector<VectorPtr>& vectors) {
        std::vector<VectorPtr> wrappedVectors;
        wrappedVectors.reserve(vectors.size());

        for (const auto& vector : vectors) {
          auto wrappedVector = BaseVector::wrapInDictionary(
              BufferPtr(nullptr),
              randomIndices(vector->size()),
              vector->size(),
              vector);
          EXPECT_EQ(
              wrappedVector->encoding(), VectorEncoding::Simple::DICTIONARY);
          wrappedVectors.emplace_back(wrappedVector);
        }
        return wrappedVectors;
      };

  const auto writeToFile = [this](const RowVectorPtr& data) {
    parquet::WriterOptions writerOptions;
    writerOptions.memoryPool = leafPool_.get();

    // Create an in-memory writer.
    auto sink = std::make_unique<MemorySink>(
        200 * 1024 * 1024,
        dwio::common::FileSink::Options{.pool = leafPool_.get()});
    auto writer = std::make_unique<parquet::Writer>(
        std::move(sink), writerOptions, rootPool_, asRowType(data->type()));
    writer->write(data);
    writer->close();
  };

  // Dictionary encoded vectors with complex type.
  const auto size = 10'000;
  auto wrappedVectors = wrapDictionaryVectors({
      facebook::velox::test::BatchMaker::createVector<TypeKind::MAP>(
          MAP(VARCHAR(), INTEGER()), size, *leafPool_),
      facebook::velox::test::BatchMaker::createVector<TypeKind::ARRAY>(
          ARRAY(VARCHAR()), size, *leafPool_),
      facebook::velox::test::BatchMaker::createVector<TypeKind::ROW>(
          ROW({"c0", "c1"},
              {BIGINT(), ROW({"id", "name"}, {INTEGER(), VARCHAR()})}),
          size,
          *leafPool_),
  });

  writeToFile(makeRowVector(wrappedVectors));

  // Dictionary encoded constant vector of scalar type.
  const auto constantVector = makeConstant(static_cast<int64_t>(123'456), size);
  const auto wrappedVector = std::make_shared<DictionaryVector<int64_t>>(
      leafPool_.get(), nullptr, size, constantVector, randomIndices(size));
  EXPECT_EQ(wrappedVector->encoding(), VectorEncoding::Simple::DICTIONARY);
  VELOX_CHECK_NOT_NULL(wrappedVector->valueVector());
  EXPECT_FALSE(wrappedVector->wrappedVector()->isFlatEncoding());

  writeToFile(makeRowVector({wrappedVector}));
};

} // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
