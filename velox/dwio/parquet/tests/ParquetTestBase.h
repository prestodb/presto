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

#pragma once

#include <gtest/gtest.h>
#include <string>
#include "velox/common/base/Fs.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/common/Reader.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"
#include "velox/dwio/parquet/reader/PageReader.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::parquet {

class ParquetTestBase : public testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    dwio::common::LocalFileSink::registerFactory();
    rootPool_ = memory::memoryManager()->addRootPool("ParquetTests");
    leafPool_ = rootPool_->addLeafChild("ParquetTests");
    tempPath_ = exec::test::TempDirectoryPath::create();
  }

  static RowTypePtr sampleSchema() {
    return ROW({"a", "b"}, {BIGINT(), DOUBLE()});
  }

  static RowTypePtr dateSchema() {
    return ROW({"date"}, {DATE()});
  }

  static RowTypePtr intSchema() {
    return ROW({"int", "bigint"}, {INTEGER(), BIGINT()});
  }

  static RowTypePtr upperSchemaToLowerCase() {
    return ROW({"a", "b"}, {BIGINT(), BIGINT()});
  }

  std::unique_ptr<facebook::velox::parquet::ParquetReader> createReader(
      const std::string& path,
      const dwio::common::ReaderOptions& opts) {
    auto input = std::make_unique<dwio::common::BufferedInput>(
        std::make_shared<LocalReadFile>(path), opts.memoryPool());
    return std::make_unique<facebook::velox::parquet::ParquetReader>(
        std::move(input), opts);
  }

  dwio::common::RowReaderOptions getReaderOpts(
      const RowTypePtr& rowType,
      bool fileColumnNamesReadAsLowerCase = false) {
    dwio::common::RowReaderOptions rowReaderOpts;
    rowReaderOpts.select(
        std::make_shared<facebook::velox::dwio::common::ColumnSelector>(
            rowType,
            rowType->names(),
            nullptr,
            fileColumnNamesReadAsLowerCase));

    return rowReaderOpts;
  }

  std::shared_ptr<velox::common::ScanSpec> makeScanSpec(
      const RowTypePtr& rowType) {
    auto scanSpec = std::make_shared<velox::common::ScanSpec>("");
    scanSpec->addAllChildFields(*rowType);
    return scanSpec;
  }

  using FilterMap =
      std::unordered_map<std::string, std::unique_ptr<velox::common::Filter>>;

  void assertEqualVectorPart(
      const VectorPtr& expected,
      const VectorPtr& actual,
      vector_size_t offset) {
    ASSERT_GE(expected->size(), actual->size() + offset);
    ASSERT_EQ(expected->typeKind(), actual->typeKind());
    for (vector_size_t i = 0; i < actual->size(); i++) {
      ASSERT_TRUE(expected->equalValueAt(actual.get(), i + offset, i))
          << "at " << (i + offset) << ": expected "
          << expected->toString(i + offset) << ", but got "
          << actual->toString(i);
    }
  }

  void assertReadWithReaderAndExpected(
      std::shared_ptr<const RowType> outputType,
      dwio::common::RowReader& reader,
      RowVectorPtr expected,
      memory::MemoryPool& memoryPool) {
    uint64_t total = 0;
    VectorPtr result = BaseVector::create(outputType, 0, &memoryPool);
    while (total < expected->size()) {
      auto part = reader.next(1000, result);
      if (part > 0) {
        assertEqualVectorPart(expected, result, total);
        total += result->size();
      } else {
        break;
      }
    }
    EXPECT_EQ(total, expected->size());
    EXPECT_EQ(reader.next(1000, result), 0);
  }

  void assertReadWithReaderAndFilters(
      const std::unique_ptr<dwio::common::Reader> reader,
      const std::string& /* fileName */,
      const RowTypePtr& fileSchema,
      FilterMap filters,
      const RowVectorPtr& expected) {
    auto scanSpec = makeScanSpec(fileSchema);
    for (auto&& [column, filter] : filters) {
      scanSpec->getOrCreateChild(velox::common::Subfield(column))
          ->setFilter(std::move(filter));
    }

    auto rowReaderOpts = getReaderOpts(fileSchema);
    rowReaderOpts.setScanSpec(scanSpec);
    auto rowReader = reader->createRowReader(rowReaderOpts);
    assertReadWithReaderAndExpected(
        fileSchema, *rowReader, expected, *leafPool_);
  }

  std::unique_ptr<dwio::common::FileSink> createSink(
      const std::string& filePath) {
    auto sink = dwio::common::FileSink::create(
        fmt::format("file:{}", filePath), {.pool = rootPool_.get()});
    EXPECT_TRUE(sink->isBuffered());
    EXPECT_TRUE(fs::exists(filePath));
    EXPECT_FALSE(sink->isClosed());
    return sink;
  }

  std::unique_ptr<facebook::velox::parquet::Writer> createWriter(
      std::unique_ptr<dwio::common::FileSink> sink,
      std::function<
          std::unique_ptr<facebook::velox::parquet::DefaultFlushPolicy>()>
          flushPolicy,
      const RowTypePtr& rowType,
      facebook::velox::common::CompressionKind compressionKind =
          facebook::velox::common::CompressionKind_NONE) {
    facebook::velox::parquet::WriterOptions options;
    options.memoryPool = rootPool_.get();
    options.flushPolicyFactory = flushPolicy;
    options.compressionKind = compressionKind;
    return std::make_unique<facebook::velox::parquet::Writer>(
        std::move(sink), options, rowType);
  }

  std::vector<RowVectorPtr> createBatches(
      const RowTypePtr& rowType,
      uint64_t numBatches,
      uint64_t vectorSize) {
    std::vector<RowVectorPtr> batches;
    batches.reserve(numBatches);
    VectorFuzzer fuzzer({.vectorSize = vectorSize}, leafPool_.get());
    for (auto i = 0; i < numBatches; ++i) {
      batches.emplace_back(fuzzer.fuzzInputFlatRow(rowType));
    }
    return batches;
  }

  std::string getExampleFilePath(const std::string& fileName) {
    return test::getDataFilePath(
        "velox/dwio/parquet/tests/reader", "../examples/" + fileName);
  }

  static constexpr uint64_t kRowsInRowGroup = 10'000;
  static constexpr uint64_t kBytesInRowGroup = 128 * 1'024 * 1'024;
  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> leafPool_;
  std::shared_ptr<exec::test::TempDirectoryPath> tempPath_;
};
} // namespace facebook::velox::parquet
