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

#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/Statistics.h"
#include "velox/dwio/common/tests/utils/DataSetBuilder.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

namespace facebook::velox::parquet::test {

constexpr uint32_t kNumRowsPerBatch = 60000;
constexpr uint32_t kNumBatches = 50;
constexpr uint32_t kNumRowsPerRowGroup = 10000;
constexpr double kFilterErrorMargin = 0.2;

class ParquetReaderBenchmark {
 public:
  explicit ParquetReaderBenchmark(
      bool disableDictionary,
      const facebook::velox::RowTypePtr& rowType)
      : disableDictionary_(disableDictionary) {
    rootPool_ = facebook::velox::memory::memoryManager()->addRootPool(
        "ParquetReaderBenchmark");
    leafPool_ = rootPool_->addLeafChild("ParquetReaderBenchmark");
    dataSetBuilder_ =
        std::make_unique<facebook::velox::test::DataSetBuilder>(*leafPool_, 0);
    auto path = fileFolder_->path + "/" + fileName_;
    auto localWriteFile =
        std::make_unique<facebook::velox::LocalWriteFile>(path, true, false);
    auto sink = std::make_unique<facebook::velox::dwio::common::WriteFileSink>(
        std::move(localWriteFile), path);
    facebook::velox::parquet::WriterOptions options;
    if (disableDictionary_) {
      // The parquet file is in plain encoding format.
      options.enableDictionary = false;
    }
    options.memoryPool = rootPool_.get();
    writer_ = std::make_unique<facebook::velox::parquet::Writer>(
        std::move(sink), options, rowType);
  }

  ~ParquetReaderBenchmark() {}

  void writeToFile(
      const std::vector<facebook::velox::RowVectorPtr>& batches,
      bool /*forRowGroupSkip*/);

  facebook::velox::dwio::common::FilterSpec createFilterSpec(
      const std::string& columnName,
      float startPct,
      float selectPct,
      const facebook::velox::TypePtr& type,
      bool isForRowGroupSkip,
      bool allowNulls);

  std::shared_ptr<facebook::velox::dwio::common::ScanSpec> createScanSpec(
      const std::vector<facebook::velox::RowVectorPtr>& batches,
      facebook::velox::RowTypePtr& rowType,
      const std::vector<facebook::velox::dwio::common::FilterSpec>& filterSpecs,
      std::vector<uint64_t>& hitRows);

  std::unique_ptr<facebook::velox::dwio::common::RowReader> createReader(
      std::shared_ptr<facebook::velox::dwio::common::ScanSpec> scanSpec,
      const facebook::velox::RowTypePtr& rowType);

  // This method is the place where we do the read opeartions.
  // scanSpec contains the setting of filters. e.g.
  // filterRateX100 = 30 means it would filter out 70% of rows and 30% remain.
  // nullsRateX100 = 70 means it would filter out 70% of rows and 30% remain.
  // Return the number of rows after the filter and null-filter.
  int read(
      const facebook::velox::RowTypePtr& rowType,
      std::shared_ptr<facebook::velox::dwio::common::ScanSpec> scanSpec,
      uint32_t nextSize);

  void readSingleColumn(
      const std::string& columnName,
      const facebook::velox::TypePtr& type,
      float startPct,
      float selectPct,
      uint8_t nullsRateX100,
      uint32_t nextSize);

 private:
  const std::string fileName_ = "test.parquet";
  const std::shared_ptr<facebook::velox::exec::test::TempDirectoryPath>
      fileFolder_ = facebook::velox::exec::test::TempDirectoryPath::create();
  const bool disableDictionary_;

  std::unique_ptr<facebook::velox::test::DataSetBuilder> dataSetBuilder_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> rootPool_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> leafPool_;
  std::unique_ptr<facebook::velox::parquet::Writer> writer_;
  facebook::velox::dwio::common::RuntimeStatistics runtimeStats_;
};

void run(
    uint32_t,
    const std::string& columnName,
    const facebook::velox::TypePtr& type,
    float filterRateX100,
    uint8_t nullsRateX100,
    uint32_t nextSize,
    bool disableDictionary);

} // namespace facebook::velox::parquet::test
