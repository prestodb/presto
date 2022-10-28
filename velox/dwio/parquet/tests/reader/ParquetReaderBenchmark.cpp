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

#include "velox/dwio/common/DataSink.h"
#include "velox/dwio/common/MemoryInputStream.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/Statistics.h"
#include "velox/dwio/common/tests/utils/DataSetBuilder.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/duckdb_reader/ParquetReader.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/dwio/parquet/writer/Writer.h"

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

using namespace facebook::velox;
using namespace facebook::velox::dwio;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::parquet;
using namespace facebook::velox::test;

// TODO: Note that the kNumRowsPerBatch needs to be a small number for now. For
// details please see https://github.com/facebookincubator/velox/issues/2844
const uint32_t kNumRowsPerBatch = 60000;
const uint32_t kNumBatches = 40;
const uint32_t kNumRowsPerRowGroup = 10000;

class ParquetReaderBenchmark {
 public:
  ParquetReaderBenchmark() {
    pool_ = memory::getDefaultScopedMemoryPool();
    dataSetBuilder_ = std::make_unique<DataSetBuilder>(*pool_.get(), 0);

    auto sink = std::make_unique<FileSink>("test.parquet");
    auto writerProperties = ::parquet::WriterProperties::Builder().build();
    writer_ = std::make_unique<facebook::velox::parquet::Writer>(
        std::move(sink), *pool_, 10000, writerProperties);
  }

  ~ParquetReaderBenchmark() {
    writer_->close();
  }

  void writeToFile(
      const std::vector<RowVectorPtr>& batches,
      bool /*forRowGroupSkip*/) {
    for (auto& batch : batches) {
      writer_->write(batch);
    }
    writer_->flush();
  }

  std::shared_ptr<ScanSpec> createScanSpec(
      const std::vector<RowVectorPtr>& batches,
      RowTypePtr& rowType,
      const std::vector<FilterSpec>& filterSpecs) {
    std::unique_ptr<FilterGenerator> filterGenerator =
        std::make_unique<FilterGenerator>(rowType, 0);
    std::vector<uint64_t> hitRows;
    auto filters =
        filterGenerator->makeSubfieldFilters(filterSpecs, batches, hitRows);
    auto scanSpec = filterGenerator->makeScanSpec(std::move(filters));
    return scanSpec;
  }

  std::unique_ptr<RowReader> createReader(
      const ParquetReaderType& parquetReaderType,
      std::shared_ptr<ScanSpec> scanSpec) {
    dwio::common::ReaderOptions readerOpts;
    auto input = std::make_unique<FileInputStream>("test.parquet");

    std::unique_ptr<Reader> reader;
    switch (parquetReaderType) {
      case ParquetReaderType::NATIVE:
        reader = std::make_unique<ParquetReader>(std::move(input), readerOpts);
        break;
      case ParquetReaderType::DUCKDB:
        reader = std::make_unique<duckdb_reader::ParquetReader>(
            std::move(input), readerOpts);
        break;
      default:
        VELOX_UNSUPPORTED("Only native or DuckDB Parquet reader is supported");
    }

    dwio::common::RowReaderOptions rowReaderOpts;
    rowReaderOpts.setScanSpec(scanSpec);
    auto rowReader = reader->createRowReader(rowReaderOpts);

    return rowReader;
  }

  void read(
      const ParquetReaderType& parquetReaderType,
      const RowTypePtr& rowType,
      std::shared_ptr<ScanSpec> scanSpec,
      uint32_t nextSize) {
    auto rowReader = createReader(parquetReaderType, scanSpec);
    runtimeStats_ = dwio::common::RuntimeStatistics();

    rowReader->resetFilterCaches();
    auto result = BaseVector::create(rowType, 1, pool_.get());

    while (true) {
      {
        bool hasData = rowReader->next(nextSize, result);
        if (!hasData) {
          break;
        }

        if (result->size() == 0) {
          continue;
        }

        auto rowVector = result->asUnchecked<RowVector>();
        for (auto i = 0; i < rowVector->childrenSize(); ++i) {
          rowVector->childAt(i)->loadedVector();
        }
      }
    }

    rowReader->updateRuntimeStats(runtimeStats_);
  }

  void readSingleColumnPlain(
      const ParquetReaderType& parquetReaderType,
      const std::string& columnName,
      const TypePtr& type,
      uint8_t nullsRateX100,
      const FilterSpec& filterSpec,
      uint32_t nextSize) {
    folly::BenchmarkSuspender suspender;

    auto rowType = ROW({columnName}, {type});
    auto batches =
        dataSetBuilder_->makeDataset(rowType, kNumBatches, kNumRowsPerBatch)
            .withRowGroupSpecificData(kNumRowsPerRowGroup)
            .withNullsForField(Subfield(columnName), nullsRateX100)
            .build();
    writeToFile(*batches, true);

    auto scanSpec = createScanSpec(*batches, rowType, {filterSpec});

    suspender.dismiss();

    read(parquetReaderType, rowType, scanSpec, nextSize);
  }

 private:
  std::unique_ptr<test::DataSetBuilder> dataSetBuilder_;
  std::unique_ptr<memory::MemoryPool> pool_;
  dwio::common::DataSink* sinkPtr_;
  std::unique_ptr<facebook::velox::parquet::Writer> writer_;
  RuntimeStatistics runtimeStats_;
};

BENCHMARK(single_column_bigint_plain_nonull_filterNothing_10000) {
  std::string columnName = "bigint";
  FilterSpec filterSpec(columnName, 0, 100, FilterKind::kBigintRange, true);

  ParquetReaderBenchmark benchmark;
  benchmark.readSingleColumnPlain(
      ParquetReaderType::NATIVE, columnName, BIGINT(), 0, filterSpec, 10000);
}

BENCHMARK(single_column_bigint_plain_partialnulls_filterNothing_10000) {
  std::string columnName = "bigint";
  FilterSpec filterSpec(columnName, 0, 100, FilterKind::kBigintRange, true);

  ParquetReaderBenchmark benchmark;
  benchmark.readSingleColumnPlain(
      ParquetReaderType::NATIVE, columnName, BIGINT(), 50, filterSpec, 10000);
}

BENCHMARK(single_column_bigint_plain_nonull_filter50_10000) {
  std::string columnName = "bigint";
  FilterSpec filterSpec(columnName, 0, 50, FilterKind::kBigintRange, true);

  ParquetReaderBenchmark benchmark;
  benchmark.readSingleColumnPlain(
      ParquetReaderType::NATIVE, columnName, BIGINT(), 0, filterSpec, 10000);
}

BENCHMARK(single_column_bigint_plain_partialnulls_filter50_10000) {
  std::string columnName = "bigint";
  FilterSpec filterSpec(columnName, 0, 50, FilterKind::kBigintRange, true);

  ParquetReaderBenchmark benchmark;
  benchmark.readSingleColumnPlain(
      ParquetReaderType::NATIVE, columnName, BIGINT(), 50, filterSpec, 10000);
}

// TODO: Add all data types
// TODO: Add dictionary encoded data

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
