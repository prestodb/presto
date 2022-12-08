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

const uint32_t kNumRowsPerBatch = 60000;
const uint32_t kNumBatches = 50;
const uint32_t kNumRowsPerRowGroup = 10000;
const double kFilterErrorMargin = 0.2;

class ParquetReaderBenchmark {
 public:
  explicit ParquetReaderBenchmark(bool disableDictionary)
      : disableDictionary_(disableDictionary) {
    pool_ = memory::getDefaultMemoryPool();
    dataSetBuilder_ = std::make_unique<DataSetBuilder>(*pool_.get(), 0);

    auto sink = std::make_unique<FileSink>("test.parquet");
    std::shared_ptr<::parquet::WriterProperties> writerProperties;
    if (disableDictionary_) {
      // The parquet file is in plain encoding format.
      writerProperties =
          ::parquet::WriterProperties::Builder().disable_dictionary()->build();
    } else {
      // The parquet file is in dictionary encoding format.
      writerProperties = ::parquet::WriterProperties::Builder().build();
    }
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

  FilterSpec createFilterSpec(
      const std::string& columnName,
      float startPct,
      float selectPct,
      const TypePtr& type,
      bool isForRowGroupSkip,
      bool allowNulls) {
    switch (type->childAt(0)->kind()) {
      case TypeKind::BIGINT:
      case TypeKind::INTEGER:
        return FilterSpec(
            columnName,
            startPct,
            selectPct,
            FilterKind::kBigintRange,
            isForRowGroupSkip,
            allowNulls);
      case TypeKind::DOUBLE:
        return FilterSpec(
            columnName,
            startPct,
            selectPct,
            FilterKind::kDoubleRange,
            isForRowGroupSkip,
            allowNulls);
      default:
        VELOX_FAIL("Unsupported Data Type {}", type->childAt(0)->toString());
    }
    return FilterSpec(columnName, startPct, selectPct, FilterKind(), false);
  }

  std::shared_ptr<ScanSpec> createScanSpec(
      const std::vector<RowVectorPtr>& batches,
      RowTypePtr& rowType,
      const std::vector<FilterSpec>& filterSpecs,
      std::vector<uint64_t>& hitRows) {
    std::unique_ptr<FilterGenerator> filterGenerator =
        std::make_unique<FilterGenerator>(rowType, 0);
    auto filters =
        filterGenerator->makeSubfieldFilters(filterSpecs, batches, hitRows);
    auto scanSpec = filterGenerator->makeScanSpec(std::move(filters));
    return scanSpec;
  }

  std::unique_ptr<RowReader> createReader(
      const ParquetReaderType& parquetReaderType,
      std::shared_ptr<ScanSpec> scanSpec,
      const RowTypePtr& rowType) {
    dwio::common::ReaderOptions readerOpts;
    auto input = std::make_unique<BufferedInput>(
        std::make_shared<LocalReadFile>("test.parquet"),
        readerOpts.getMemoryPool());

    std::unique_ptr<Reader> reader;
    switch (parquetReaderType) {
      case ParquetReaderType::NATIVE:
        reader = std::make_unique<ParquetReader>(std::move(input), readerOpts);
        break;
      case ParquetReaderType::DUCKDB:
        reader = std::make_unique<duckdb_reader::ParquetReader>(
            input->getInputStream(), readerOpts);
        break;
      default:
        VELOX_UNSUPPORTED("Only native or DuckDB Parquet reader is supported");
    }

    dwio::common::RowReaderOptions rowReaderOpts;
    rowReaderOpts.select(
        std::make_shared<facebook::velox::dwio::common::ColumnSelector>(
            rowType, rowType->names()));
    rowReaderOpts.setScanSpec(scanSpec);
    auto rowReader = reader->createRowReader(rowReaderOpts);

    return rowReader;
  }

  int read(
      const ParquetReaderType& parquetReaderType,
      const RowTypePtr& rowType,
      std::shared_ptr<ScanSpec> scanSpec,
      uint32_t nextSize) {
    auto rowReader = createReader(parquetReaderType, scanSpec, rowType);
    runtimeStats_ = dwio::common::RuntimeStatistics();

    rowReader->resetFilterCaches();
    auto result = BaseVector::create(rowType, 1, pool_.get());
    int resultSize = 0;
    while (true) {
      bool hasData = rowReader->next(nextSize, result);

      if (!hasData) {
        break;
      }
      resultSize += result->size();

      if (result->size() == 0) {
        continue;
      }

      auto rowVector = result->asUnchecked<RowVector>();
      for (auto i = 0; i < rowVector->childrenSize(); ++i) {
        rowVector->childAt(i)->loadedVector();
      }
    }

    rowReader->updateRuntimeStats(runtimeStats_);
    return resultSize;
  }

  void readSingleColumn(
      const ParquetReaderType& parquetReaderType,
      const std::string& columnName,
      const TypePtr& type,
      float startPct,
      float selectPct,
      uint8_t nullsRateX100,
      uint32_t nextSize) {
    folly::BenchmarkSuspender suspender;

    auto rowType = ROW({columnName}, {type});
    auto batches =
        dataSetBuilder_->makeDataset(rowType, kNumBatches, kNumRowsPerBatch)
            .withRowGroupSpecificData(kNumRowsPerRowGroup)
            .withNullsForField(Subfield(columnName), nullsRateX100)
            .build();
    writeToFile(*batches, true);

    FilterSpec filterSpec = createFilterSpec(
        columnName, startPct, selectPct, rowType, false, false);

    std::vector<uint64_t> hitRows;
    auto scanSpec = createScanSpec(*batches, rowType, {filterSpec}, hitRows);

    suspender.dismiss();

    // Filter range is generated from a small sample data of 4096 rows. So the
    // upperBound and lowerBound are introduced to estimate the result size.
    auto resultSize = read(parquetReaderType, rowType, scanSpec, nextSize);

    // Add one to expected to avoid 0 in calculating upperBound and lowerBound.
    int expected = kNumBatches * kNumRowsPerBatch *
            (1 - (double)nullsRateX100 / 100) * ((double)selectPct / 100) +
        1;

    // Make the upperBound and lowerBound large enough to avoid very small
    // resultSize and expected size, where the diff ratio is relatively very
    // large.
    int upperBound = expected * (1 + kFilterErrorMargin) + 1;
    int lowerBound = expected * (1 - kFilterErrorMargin) - 1;
    upperBound = std::max(16, upperBound);
    lowerBound = std::max(0, lowerBound);

    VELOX_CHECK(
        resultSize <= upperBound && resultSize >= lowerBound,
        "Result Size {} and Expected Size {} Mismatch",
        resultSize,
        expected);
  }

 private:
  std::unique_ptr<test::DataSetBuilder> dataSetBuilder_;
  std::shared_ptr<memory::MemoryPool> pool_;
  dwio::common::DataSink* sinkPtr_;
  std::unique_ptr<facebook::velox::parquet::Writer> writer_;
  RuntimeStatistics runtimeStats_;
  bool disableDictionary_;
};

void run(
    uint32_t,
    const TypePtr& type,
    float filterRateX100,
    uint8_t nullsRateX100,
    uint32_t nextSize,
    bool disableDictionary) {
  ParquetReaderBenchmark benchmark(disableDictionary);
  BIGINT()->toString();
  benchmark.readSingleColumn(
      ParquetReaderType::NATIVE,
      type->toString(),
      type,
      0,
      filterRateX100,
      nullsRateX100,
      nextSize);
}

#define PARQUET_BENCHMARKS_NULLS_FILTER(_type_, _name_, _filter_, _null_) \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_5000_dict,       \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      5000,                                                               \
      false);                                                             \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_5000_plain,      \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      5000,                                                               \
      true);                                                              \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_10000_dict,      \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      10000,                                                              \
      false);                                                             \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_10000_plain,     \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      10000,                                                              \
      true);                                                              \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_20000_dict,      \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      20000,                                                              \
      false);                                                             \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_20000_plain,     \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      20000,                                                              \
      true);                                                              \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_50000_dict,      \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      50000,                                                              \
      false);                                                             \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_50000_plain,     \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      50000,                                                              \
      true);                                                              \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_100000_dict,     \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      100000,                                                             \
      false);                                                             \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_100000_plain,    \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      100000,                                                             \
      true);                                                              \
  BENCHMARK_DRAW_LINE();

#define PARQUET_BENCHMARKS_FILTERS(_type_, _name_, _filter_)    \
  PARQUET_BENCHMARKS_NULLS_FILTER(_type_, _name_, _filter_, 0)  \
  PARQUET_BENCHMARKS_NULLS_FILTER(_type_, _name_, _filter_, 20) \
  PARQUET_BENCHMARKS_NULLS_FILTER(_type_, _name_, _filter_, 50) \
  PARQUET_BENCHMARKS_NULLS_FILTER(_type_, _name_, _filter_, 70) \
  PARQUET_BENCHMARKS_NULLS_FILTER(_type_, _name_, _filter_, 100)

#define PARQUET_BENCHMARKS(_type_, _name_)        \
  PARQUET_BENCHMARKS_FILTERS(_type_, _name_, 0)   \
  PARQUET_BENCHMARKS_FILTERS(_type_, _name_, 20)  \
  PARQUET_BENCHMARKS_FILTERS(_type_, _name_, 50)  \
  PARQUET_BENCHMARKS_FILTERS(_type_, _name_, 70)  \
  PARQUET_BENCHMARKS_FILTERS(_type_, _name_, 100) \
  BENCHMARK_DRAW_LINE();

PARQUET_BENCHMARKS(BIGINT(), BigInt);
PARQUET_BENCHMARKS(DOUBLE(), Double);

// TODO: Add all data types

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}

/*
CPU model name: Intel(R) Xeon(R) Platinum 8163 CPU @ 2.50GHz
Core(s) used: 24
Memory(GB): 96

============================================================================
run(BigInt_Filter_0_Nulls_0_next_5000_dict)                  8.07ms   123.84
run(BigInt_Filter_0_Nulls_0_next_5000_plain)                32.87ms    30.43
run(BigInt_Filter_0_Nulls_0_next_10000_dict)                32.32ms    30.94
run(BigInt_Filter_0_Nulls_0_next_10000_plain)               30.28ms    33.03
run(BigInt_Filter_0_Nulls_0_next_20000_dict)                32.60ms    30.67
run(BigInt_Filter_0_Nulls_0_next_20000_plain)               32.96ms    30.34
run(BigInt_Filter_0_Nulls_0_next_50000_dict)                34.50ms    28.98
run(BigInt_Filter_0_Nulls_0_next_50000_plain)               32.24ms    31.02
run(BigInt_Filter_0_Nulls_0_next_100000_dict)               35.24ms    28.38
run(BigInt_Filter_0_Nulls_0_next_100000_plain)              31.55ms    31.70
----------------------------------------------------------------------------
run(BigInt_Filter_0_Nulls_20_next_5000_dict)                27.83ms    35.93
run(BigInt_Filter_0_Nulls_20_next_5000_plain)               25.78ms    38.79
run(BigInt_Filter_0_Nulls_20_next_10000_dict)               26.83ms    37.27
run(BigInt_Filter_0_Nulls_20_next_10000_plain)              25.81ms    38.75
run(BigInt_Filter_0_Nulls_20_next_20000_dict)               28.42ms    35.19
run(BigInt_Filter_0_Nulls_20_next_20000_plain)              25.81ms    38.75
run(BigInt_Filter_0_Nulls_20_next_50000_dict)               29.45ms    33.95
run(BigInt_Filter_0_Nulls_20_next_50000_plain)              73.67ms    13.57
run(BigInt_Filter_0_Nulls_20_next_100000_dict)              26.14ms    38.26
run(BigInt_Filter_0_Nulls_20_next_100000_plain)             26.17ms    38.21
----------------------------------------------------------------------------
run(BigInt_Filter_0_Nulls_50_next_5000_dict)                16.00ms    62.49
run(BigInt_Filter_0_Nulls_50_next_5000_plain)               14.59ms    68.54
run(BigInt_Filter_0_Nulls_50_next_10000_dict)               15.13ms    66.11
run(BigInt_Filter_0_Nulls_50_next_10000_plain)              14.54ms    68.78
run(BigInt_Filter_0_Nulls_50_next_20000_dict)               15.41ms    64.91
run(BigInt_Filter_0_Nulls_50_next_20000_plain)              14.79ms    67.60
run(BigInt_Filter_0_Nulls_50_next_50000_dict)               14.72ms    67.94
run(BigInt_Filter_0_Nulls_50_next_50000_plain)              15.12ms    66.15
run(BigInt_Filter_0_Nulls_50_next_100000_dict)              17.34ms    57.67
run(BigInt_Filter_0_Nulls_50_next_100000_plain)             15.67ms    63.82
----------------------------------------------------------------------------
run(BigInt_Filter_0_Nulls_70_next_5000_dict)                12.46ms    80.24
run(BigInt_Filter_0_Nulls_70_next_5000_plain)               10.99ms    91.01
run(BigInt_Filter_0_Nulls_70_next_10000_dict)               12.87ms    77.71
run(BigInt_Filter_0_Nulls_70_next_10000_plain)              10.98ms    91.08
run(BigInt_Filter_0_Nulls_70_next_20000_dict)               11.85ms    84.37
run(BigInt_Filter_0_Nulls_70_next_20000_plain)              10.72ms    93.29
run(BigInt_Filter_0_Nulls_70_next_50000_dict)               12.35ms    80.95
run(BigInt_Filter_0_Nulls_70_next_50000_plain)               8.34ms   119.97
run(BigInt_Filter_0_Nulls_70_next_100000_dict)              10.47ms    95.51
run(BigInt_Filter_0_Nulls_70_next_100000_plain)             10.86ms    92.12
----------------------------------------------------------------------------
run(BigInt_Filter_0_Nulls_100_next_5000_dict)              992.50us    1.01K
run(BigInt_Filter_0_Nulls_100_next_5000_plain)             913.45us    1.09K
run(BigInt_Filter_0_Nulls_100_next_10000_dict)             945.19us    1.06K
run(BigInt_Filter_0_Nulls_100_next_10000_plain)            911.22us    1.10K
run(BigInt_Filter_0_Nulls_100_next_20000_dict)             974.65us    1.03K
run(BigInt_Filter_0_Nulls_100_next_20000_plain)            906.66us    1.10K
run(BigInt_Filter_0_Nulls_100_next_50000_dict)               1.00ms   996.16
run(BigInt_Filter_0_Nulls_100_next_50000_plain)            920.88us    1.09K
run(BigInt_Filter_0_Nulls_100_next_100000_dict)            968.53us    1.03K
run(BigInt_Filter_0_Nulls_100_next_100000_plain            923.57us    1.08K
----------------------------------------------------------------------------
run(BigInt_Filter_20_Nulls_0_next_5000_dict)                44.13ms    22.66
run(BigInt_Filter_20_Nulls_0_next_5000_plain)               46.56ms    21.48
run(BigInt_Filter_20_Nulls_0_next_10000_dict)               48.55ms    20.60
run(BigInt_Filter_20_Nulls_0_next_10000_plain)              45.65ms    21.91
run(BigInt_Filter_20_Nulls_0_next_20000_dict)               73.05ms    13.69
run(BigInt_Filter_20_Nulls_0_next_20000_plain)              46.20ms    21.65
run(BigInt_Filter_20_Nulls_0_next_50000_dict)               72.26ms    13.84
run(BigInt_Filter_20_Nulls_0_next_50000_plain)              45.87ms    21.80
run(BigInt_Filter_20_Nulls_0_next_100000_dict)              72.07ms    13.87
run(BigInt_Filter_20_Nulls_0_next_100000_plain)             46.73ms    21.40
----------------------------------------------------------------------------
run(BigInt_Filter_20_Nulls_20_next_5000_dict)               61.59ms    16.24
run(BigInt_Filter_20_Nulls_20_next_5000_plain)              40.28ms    24.83
run(BigInt_Filter_20_Nulls_20_next_10000_dict)              63.84ms    15.66
run(BigInt_Filter_20_Nulls_20_next_10000_plain)             41.25ms    24.24
run(BigInt_Filter_20_Nulls_20_next_20000_dict)              62.43ms    16.02
run(BigInt_Filter_20_Nulls_20_next_20000_plain)             41.13ms    24.31
run(BigInt_Filter_20_Nulls_20_next_50000_dict)              62.83ms    15.91
run(BigInt_Filter_20_Nulls_20_next_50000_plain)             42.40ms    23.59
run(BigInt_Filter_20_Nulls_20_next_100000_dict)             58.72ms    17.03
run(BigInt_Filter_20_Nulls_20_next_100000_plain             41.12ms    24.32
----------------------------------------------------------------------------
run(BigInt_Filter_20_Nulls_50_next_5000_dict)               36.35ms    27.51
run(BigInt_Filter_20_Nulls_50_next_5000_plain)              23.48ms    42.59
run(BigInt_Filter_20_Nulls_50_next_10000_dict)              35.09ms    28.50
run(BigInt_Filter_20_Nulls_50_next_10000_plain)             22.62ms    44.21
run(BigInt_Filter_20_Nulls_50_next_20000_dict)              37.59ms    26.60
run(BigInt_Filter_20_Nulls_50_next_20000_plain)             23.73ms    42.14
run(BigInt_Filter_20_Nulls_50_next_50000_dict)              35.82ms    27.91
run(BigInt_Filter_20_Nulls_50_next_50000_plain)             23.69ms    42.20
run(BigInt_Filter_20_Nulls_50_next_100000_dict)             36.77ms    27.19
run(BigInt_Filter_20_Nulls_50_next_100000_plain             23.83ms    41.96
----------------------------------------------------------------------------
run(BigInt_Filter_20_Nulls_70_next_5000_dict)               26.97ms    37.08
run(BigInt_Filter_20_Nulls_70_next_5000_plain)              19.24ms    51.98
run(BigInt_Filter_20_Nulls_70_next_10000_dict)              28.22ms    35.43
run(BigInt_Filter_20_Nulls_70_next_10000_plain)             17.10ms    58.48
run(BigInt_Filter_20_Nulls_70_next_20000_dict)              24.61ms    40.64
run(BigInt_Filter_20_Nulls_70_next_20000_plain)             17.72ms    56.45
run(BigInt_Filter_20_Nulls_70_next_50000_dict)              26.21ms    38.15
run(BigInt_Filter_20_Nulls_70_next_50000_plain)             17.20ms    58.14
run(BigInt_Filter_20_Nulls_70_next_100000_dict)             24.95ms    40.08
run(BigInt_Filter_20_Nulls_70_next_100000_plain             17.39ms    57.49
----------------------------------------------------------------------------
run(BigInt_Filter_20_Nulls_100_next_5000_dict)             998.41us    1.00K
run(BigInt_Filter_20_Nulls_100_next_5000_plain)            944.29us    1.06K
run(BigInt_Filter_20_Nulls_100_next_10000_dict)            985.02us    1.02K
run(BigInt_Filter_20_Nulls_100_next_10000_plain            997.04us    1.00K
run(BigInt_Filter_20_Nulls_100_next_20000_dict)            995.81us    1.00K
run(BigInt_Filter_20_Nulls_100_next_20000_plain            942.19us    1.06K
run(BigInt_Filter_20_Nulls_100_next_50000_dict)            981.40us    1.02K
run(BigInt_Filter_20_Nulls_100_next_50000_plain            913.60us    1.09K
run(BigInt_Filter_20_Nulls_100_next_100000_dict              1.01ms   989.97
run(BigInt_Filter_20_Nulls_100_next_100000_plai            946.56us    1.06K
----------------------------------------------------------------------------
run(BigInt_Filter_50_Nulls_0_next_5000_dict)                57.85ms    17.29
run(BigInt_Filter_50_Nulls_0_next_5000_plain)               44.08ms    22.69
run(BigInt_Filter_50_Nulls_0_next_10000_dict)               86.27ms    11.59
run(BigInt_Filter_50_Nulls_0_next_10000_plain)              44.59ms    22.43
run(BigInt_Filter_50_Nulls_0_next_20000_dict)               84.18ms    11.88
run(BigInt_Filter_50_Nulls_0_next_20000_plain)              52.36ms    19.10
run(BigInt_Filter_50_Nulls_0_next_50000_dict)               85.51ms    11.69
run(BigInt_Filter_50_Nulls_0_next_50000_plain)              44.10ms    22.67
run(BigInt_Filter_50_Nulls_0_next_100000_dict)              85.23ms    11.73
run(BigInt_Filter_50_Nulls_0_next_100000_plain)             42.83ms    23.35
----------------------------------------------------------------------------
run(BigInt_Filter_50_Nulls_20_next_5000_dict)               73.92ms    13.53
run(BigInt_Filter_50_Nulls_20_next_5000_plain)              39.10ms    25.58
run(BigInt_Filter_50_Nulls_20_next_10000_dict)              68.55ms    14.59
run(BigInt_Filter_50_Nulls_20_next_10000_plain)             37.97ms    26.34
run(BigInt_Filter_50_Nulls_20_next_20000_dict)              72.31ms    13.83
run(BigInt_Filter_50_Nulls_20_next_20000_plain)             42.14ms    23.73
run(BigInt_Filter_50_Nulls_20_next_50000_dict)              72.10ms    13.87
run(BigInt_Filter_50_Nulls_20_next_50000_plain)             37.66ms    26.55
run(BigInt_Filter_50_Nulls_20_next_100000_dict)             68.95ms    14.50
run(BigInt_Filter_50_Nulls_20_next_100000_plain             37.49ms    26.67
----------------------------------------------------------------------------
run(BigInt_Filter_50_Nulls_50_next_5000_dict)               50.08ms    19.97
run(BigInt_Filter_50_Nulls_50_next_5000_plain)              22.04ms    45.38
run(BigInt_Filter_50_Nulls_50_next_10000_dict)              40.84ms    24.48
run(BigInt_Filter_50_Nulls_50_next_10000_plain)             23.74ms    42.11
run(BigInt_Filter_50_Nulls_50_next_20000_dict)              42.55ms    23.50
run(BigInt_Filter_50_Nulls_50_next_20000_plain)             22.87ms    43.73
run(BigInt_Filter_50_Nulls_50_next_50000_dict)              41.67ms    24.00
run(BigInt_Filter_50_Nulls_50_next_50000_plain)             22.20ms    45.04
run(BigInt_Filter_50_Nulls_50_next_100000_dict)             41.91ms    23.86
run(BigInt_Filter_50_Nulls_50_next_100000_plain             25.25ms    39.61
----------------------------------------------------------------------------
run(BigInt_Filter_50_Nulls_70_next_5000_dict)               29.32ms    34.11
run(BigInt_Filter_50_Nulls_70_next_5000_plain)              16.47ms    60.71
run(BigInt_Filter_50_Nulls_70_next_10000_dict)              28.34ms    35.29
run(BigInt_Filter_50_Nulls_70_next_10000_plain)             17.16ms    58.28
run(BigInt_Filter_50_Nulls_70_next_20000_dict)              26.58ms    37.62
run(BigInt_Filter_50_Nulls_70_next_20000_plain)             15.88ms    62.96
run(BigInt_Filter_50_Nulls_70_next_50000_dict)              28.69ms    34.86
run(BigInt_Filter_50_Nulls_70_next_50000_plain)             17.17ms    58.25
run(BigInt_Filter_50_Nulls_70_next_100000_dict)             28.67ms    34.88
run(BigInt_Filter_50_Nulls_70_next_100000_plain             17.40ms    57.48
----------------------------------------------------------------------------
run(BigInt_Filter_50_Nulls_100_next_5000_dict)               1.04ms   958.72
run(BigInt_Filter_50_Nulls_100_next_5000_plain)            955.89us    1.05K
run(BigInt_Filter_50_Nulls_100_next_10000_dict)              1.01ms   990.96
run(BigInt_Filter_50_Nulls_100_next_10000_plain            981.41us    1.02K
run(BigInt_Filter_50_Nulls_100_next_20000_dict)              1.07ms   934.00
run(BigInt_Filter_50_Nulls_100_next_20000_plain            980.98us    1.02K
run(BigInt_Filter_50_Nulls_100_next_50000_dict)              1.03ms   966.34
run(BigInt_Filter_50_Nulls_100_next_50000_plain            934.60us    1.07K
run(BigInt_Filter_50_Nulls_100_next_100000_dict            985.81us    1.01K
run(BigInt_Filter_50_Nulls_100_next_100000_plai            941.50us    1.06K
----------------------------------------------------------------------------
run(BigInt_Filter_70_Nulls_0_next_5000_dict)                53.86ms    18.57
run(BigInt_Filter_70_Nulls_0_next_5000_plain)               46.05ms    21.72
run(BigInt_Filter_70_Nulls_0_next_10000_dict)               81.67ms    12.25
run(BigInt_Filter_70_Nulls_0_next_10000_plain)              45.02ms    22.21
run(BigInt_Filter_70_Nulls_0_next_20000_dict)               79.90ms    12.52
run(BigInt_Filter_70_Nulls_0_next_20000_plain)              44.76ms    22.34
run(BigInt_Filter_70_Nulls_0_next_50000_dict)               85.65ms    11.68
run(BigInt_Filter_70_Nulls_0_next_50000_plain)              45.36ms    22.04
run(BigInt_Filter_70_Nulls_0_next_100000_dict)              80.33ms    12.45
run(BigInt_Filter_70_Nulls_0_next_100000_plain)             46.46ms    21.52
----------------------------------------------------------------------------
run(BigInt_Filter_70_Nulls_20_next_5000_dict)               82.04ms    12.19
run(BigInt_Filter_70_Nulls_20_next_5000_plain)              39.73ms    25.17
run(BigInt_Filter_70_Nulls_20_next_10000_dict)              68.96ms    14.50
run(BigInt_Filter_70_Nulls_20_next_10000_plain)             39.32ms    25.43
run(BigInt_Filter_70_Nulls_20_next_20000_dict)              66.46ms    15.05
run(BigInt_Filter_70_Nulls_20_next_20000_plain)             39.74ms    25.16
run(BigInt_Filter_70_Nulls_20_next_50000_dict)              65.60ms    15.24
run(BigInt_Filter_70_Nulls_20_next_50000_plain)             38.28ms    26.12
run(BigInt_Filter_70_Nulls_20_next_100000_dict)             68.41ms    14.62
run(BigInt_Filter_70_Nulls_20_next_100000_plain             48.79ms    20.50
----------------------------------------------------------------------------
run(BigInt_Filter_70_Nulls_50_next_5000_dict)               43.10ms    23.20
run(BigInt_Filter_70_Nulls_50_next_5000_plain)              23.60ms    42.38
run(BigInt_Filter_70_Nulls_50_next_10000_dict)              39.26ms    25.47
run(BigInt_Filter_70_Nulls_50_next_10000_plain)             22.94ms    43.59
run(BigInt_Filter_70_Nulls_50_next_20000_dict)              39.95ms    25.03
run(BigInt_Filter_70_Nulls_50_next_20000_plain)             21.92ms    45.62
run(BigInt_Filter_70_Nulls_50_next_50000_dict)              40.90ms    24.45
run(BigInt_Filter_70_Nulls_50_next_50000_plain)             12.76ms    78.39
run(BigInt_Filter_70_Nulls_50_next_100000_dict)             42.04ms    23.78
run(BigInt_Filter_70_Nulls_50_next_100000_plain             23.11ms    43.26
----------------------------------------------------------------------------
run(BigInt_Filter_70_Nulls_70_next_5000_dict)               28.48ms    35.11
run(BigInt_Filter_70_Nulls_70_next_5000_plain)              17.27ms    57.91
run(BigInt_Filter_70_Nulls_70_next_10000_dict)              26.54ms    37.68
run(BigInt_Filter_70_Nulls_70_next_10000_plain)             16.52ms    60.53
run(BigInt_Filter_70_Nulls_70_next_20000_dict)              27.72ms    36.07
run(BigInt_Filter_70_Nulls_70_next_20000_plain)             17.30ms    57.79
run(BigInt_Filter_70_Nulls_70_next_50000_dict)              26.29ms    38.04
run(BigInt_Filter_70_Nulls_70_next_50000_plain)             18.12ms    55.20
run(BigInt_Filter_70_Nulls_70_next_100000_dict)             26.30ms    38.02
run(BigInt_Filter_70_Nulls_70_next_100000_plain             16.36ms    61.14
----------------------------------------------------------------------------
run(BigInt_Filter_70_Nulls_100_next_5000_dict)               1.03ms   966.35
run(BigInt_Filter_70_Nulls_100_next_5000_plain)            958.21us    1.04K
run(BigInt_Filter_70_Nulls_100_next_10000_dict)              1.02ms   985.06
run(BigInt_Filter_70_Nulls_100_next_10000_plain            999.83us    1.00K
run(BigInt_Filter_70_Nulls_100_next_20000_dict)              1.02ms   981.06
run(BigInt_Filter_70_Nulls_100_next_20000_plain            970.79us    1.03K
run(BigInt_Filter_70_Nulls_100_next_50000_dict)              1.04ms   964.28
run(BigInt_Filter_70_Nulls_100_next_50000_plain            955.93us    1.05K
run(BigInt_Filter_70_Nulls_100_next_100000_dict              1.04ms   957.80
run(BigInt_Filter_70_Nulls_100_next_100000_plai            945.12us    1.06K
----------------------------------------------------------------------------
run(BigInt_Filter_100_Nulls_0_next_5000_dict)               36.38ms    27.49
run(BigInt_Filter_100_Nulls_0_next_5000_plain)              41.29ms    24.22
run(BigInt_Filter_100_Nulls_0_next_10000_dict)              69.65ms    14.36
run(BigInt_Filter_100_Nulls_0_next_10000_plain)             40.64ms    24.61
run(BigInt_Filter_100_Nulls_0_next_20000_dict)              65.37ms    15.30
run(BigInt_Filter_100_Nulls_0_next_20000_plain)             41.13ms    24.31
run(BigInt_Filter_100_Nulls_0_next_50000_dict)              63.88ms    15.65
run(BigInt_Filter_100_Nulls_0_next_50000_plain)             41.17ms    24.29
run(BigInt_Filter_100_Nulls_0_next_100000_dict)             68.44ms    14.61
run(BigInt_Filter_100_Nulls_0_next_100000_plain             42.79ms    23.37
----------------------------------------------------------------------------
run(BigInt_Filter_100_Nulls_20_next_5000_dict)              58.92ms    16.97
run(BigInt_Filter_100_Nulls_20_next_5000_plain)             35.55ms    28.13
run(BigInt_Filter_100_Nulls_20_next_10000_dict)             52.93ms    18.89
run(BigInt_Filter_100_Nulls_20_next_10000_plain             43.61ms    22.93
run(BigInt_Filter_100_Nulls_20_next_20000_dict)             56.22ms    17.79
run(BigInt_Filter_100_Nulls_20_next_20000_plain             34.09ms    29.34
run(BigInt_Filter_100_Nulls_20_next_50000_dict)             55.12ms    18.14
run(BigInt_Filter_100_Nulls_20_next_50000_plain             36.60ms    27.32
run(BigInt_Filter_100_Nulls_20_next_100000_dict             53.57ms    18.67
run(BigInt_Filter_100_Nulls_20_next_100000_plai             38.35ms    26.08
----------------------------------------------------------------------------
run(BigInt_Filter_100_Nulls_50_next_5000_dict)              39.52ms    25.31
run(BigInt_Filter_100_Nulls_50_next_5000_plain)             22.42ms    44.60
run(BigInt_Filter_100_Nulls_50_next_10000_dict)             33.82ms    29.57
run(BigInt_Filter_100_Nulls_50_next_10000_plain             21.50ms    46.50
run(BigInt_Filter_100_Nulls_50_next_20000_dict)             35.36ms    28.28
run(BigInt_Filter_100_Nulls_50_next_20000_plain             25.28ms    39.56
run(BigInt_Filter_100_Nulls_50_next_50000_dict)             33.64ms    29.73
run(BigInt_Filter_100_Nulls_50_next_50000_plain             20.10ms    49.74
run(BigInt_Filter_100_Nulls_50_next_100000_dict             31.41ms    31.83
run(BigInt_Filter_100_Nulls_50_next_100000_plai             19.84ms    50.41
----------------------------------------------------------------------------
run(BigInt_Filter_100_Nulls_70_next_5000_dict)              24.19ms    41.34
run(BigInt_Filter_100_Nulls_70_next_5000_plain)             14.59ms    68.55
run(BigInt_Filter_100_Nulls_70_next_10000_dict)             24.32ms    41.11
run(BigInt_Filter_100_Nulls_70_next_10000_plain             16.53ms    60.49
run(BigInt_Filter_100_Nulls_70_next_20000_dict)             23.55ms    42.46
run(BigInt_Filter_100_Nulls_70_next_20000_plain             16.96ms    58.97
run(BigInt_Filter_100_Nulls_70_next_50000_dict)             23.56ms    42.45
run(BigInt_Filter_100_Nulls_70_next_50000_plain             17.21ms    58.11
run(BigInt_Filter_100_Nulls_70_next_100000_dict             22.62ms    44.21
run(BigInt_Filter_100_Nulls_70_next_100000_plai             16.73ms    59.76
----------------------------------------------------------------------------
run(BigInt_Filter_100_Nulls_100_next_5000_dict)              1.03ms   967.89
run(BigInt_Filter_100_Nulls_100_next_5000_plain            971.75us    1.03K
run(BigInt_Filter_100_Nulls_100_next_10000_dict              1.03ms   970.09
run(BigInt_Filter_100_Nulls_100_next_10000_plai            975.05us    1.03K
run(BigInt_Filter_100_Nulls_100_next_20000_dict              1.02ms   981.96
run(BigInt_Filter_100_Nulls_100_next_20000_plai            979.39us    1.02K
run(BigInt_Filter_100_Nulls_100_next_50000_dict              1.01ms   985.63
run(BigInt_Filter_100_Nulls_100_next_50000_plai            946.22us    1.06K
run(BigInt_Filter_100_Nulls_100_next_100000_dic              1.03ms   968.01
run(BigInt_Filter_100_Nulls_100_next_100000_pla            973.41us    1.03K
----------------------------------------------------------------------------
----------------------------------------------------------------------------
run(Double_Filter_0_Nulls_0_next_5000_dict)                 16.93ms    59.05
run(Double_Filter_0_Nulls_0_next_5000_plain)                32.60ms    30.68
run(Double_Filter_0_Nulls_0_next_10000_dict)                36.43ms    27.45
run(Double_Filter_0_Nulls_0_next_10000_plain)               33.33ms    30.00
run(Double_Filter_0_Nulls_0_next_20000_dict)                34.84ms    28.70
run(Double_Filter_0_Nulls_0_next_20000_plain)               33.10ms    30.21
run(Double_Filter_0_Nulls_0_next_50000_dict)                34.65ms    28.86
run(Double_Filter_0_Nulls_0_next_50000_plain)               32.66ms    30.62
run(Double_Filter_0_Nulls_0_next_100000_dict)               33.55ms    29.80
run(Double_Filter_0_Nulls_0_next_100000_plain)              32.56ms    30.71
----------------------------------------------------------------------------
run(Double_Filter_0_Nulls_20_next_5000_dict)                32.66ms    30.62
run(Double_Filter_0_Nulls_20_next_5000_plain)               28.07ms    35.62
run(Double_Filter_0_Nulls_20_next_10000_dict)               35.44ms    28.22
run(Double_Filter_0_Nulls_20_next_10000_plain)              26.71ms    37.44
run(Double_Filter_0_Nulls_20_next_20000_dict)               28.87ms    34.64
run(Double_Filter_0_Nulls_20_next_20000_plain)              25.69ms    38.92
run(Double_Filter_0_Nulls_20_next_50000_dict)               30.32ms    32.98
run(Double_Filter_0_Nulls_20_next_50000_plain)              26.81ms    37.30
run(Double_Filter_0_Nulls_20_next_100000_dict)              28.81ms    34.71
run(Double_Filter_0_Nulls_20_next_100000_plain)             26.73ms    37.41
----------------------------------------------------------------------------
run(Double_Filter_0_Nulls_50_next_5000_dict)                23.98ms    41.70
run(Double_Filter_0_Nulls_50_next_5000_plain)               16.65ms    60.07
run(Double_Filter_0_Nulls_50_next_10000_dict)               19.28ms    51.87
run(Double_Filter_0_Nulls_50_next_10000_plain)              19.29ms    51.84
run(Double_Filter_0_Nulls_50_next_20000_dict)               15.83ms    63.19
run(Double_Filter_0_Nulls_50_next_20000_plain)              14.89ms    67.16
run(Double_Filter_0_Nulls_50_next_50000_dict)               17.78ms    56.24
run(Double_Filter_0_Nulls_50_next_50000_plain)              14.40ms    69.46
run(Double_Filter_0_Nulls_50_next_100000_dict)              18.38ms    54.40
run(Double_Filter_0_Nulls_50_next_100000_plain)             14.70ms    68.04
----------------------------------------------------------------------------
run(Double_Filter_0_Nulls_70_next_5000_dict)                13.52ms    73.96
run(Double_Filter_0_Nulls_70_next_5000_plain)               11.04ms    90.61
run(Double_Filter_0_Nulls_70_next_10000_dict)               12.61ms    79.28
run(Double_Filter_0_Nulls_70_next_10000_plain)              11.00ms    90.91
run(Double_Filter_0_Nulls_70_next_20000_dict)               12.37ms    80.87
run(Double_Filter_0_Nulls_70_next_20000_plain)              10.65ms    93.87
run(Double_Filter_0_Nulls_70_next_50000_dict)               12.34ms    81.01
run(Double_Filter_0_Nulls_70_next_50000_plain)              11.31ms    88.39
run(Double_Filter_0_Nulls_70_next_100000_dict)              12.04ms    83.04
run(Double_Filter_0_Nulls_70_next_100000_plain)             11.30ms    88.50
----------------------------------------------------------------------------
run(Double_Filter_0_Nulls_100_next_5000_dict)                1.03ms   971.81
run(Double_Filter_0_Nulls_100_next_5000_plain)             950.58us    1.05K
run(Double_Filter_0_Nulls_100_next_10000_dict)               1.04ms   964.03
run(Double_Filter_0_Nulls_100_next_10000_plain)            967.97us    1.03K
run(Double_Filter_0_Nulls_100_next_20000_dict)               1.04ms   965.02
run(Double_Filter_0_Nulls_100_next_20000_plain)            946.39us    1.06K
run(Double_Filter_0_Nulls_100_next_50000_dict)               1.03ms   968.13
run(Double_Filter_0_Nulls_100_next_50000_plain)            952.92us    1.05K
run(Double_Filter_0_Nulls_100_next_100000_dict)            978.88us    1.02K
run(Double_Filter_0_Nulls_100_next_100000_plain            940.64us    1.06K
----------------------------------------------------------------------------
run(Double_Filter_20_Nulls_0_next_5000_dict)                50.05ms    19.98
run(Double_Filter_20_Nulls_0_next_5000_plain)               46.57ms    21.47
run(Double_Filter_20_Nulls_0_next_10000_dict)               77.61ms    12.89
run(Double_Filter_20_Nulls_0_next_10000_plain)              47.71ms    20.96
run(Double_Filter_20_Nulls_0_next_20000_dict)               75.79ms    13.19
run(Double_Filter_20_Nulls_0_next_20000_plain)              22.19ms    45.06
run(Double_Filter_20_Nulls_0_next_50000_dict)               75.54ms    13.24
run(Double_Filter_20_Nulls_0_next_50000_plain)              46.60ms    21.46
run(Double_Filter_20_Nulls_0_next_100000_dict)              80.75ms    12.38
run(Double_Filter_20_Nulls_0_next_100000_plain)             45.81ms    21.83
----------------------------------------------------------------------------
run(Double_Filter_20_Nulls_20_next_5000_dict)               69.60ms    14.37
run(Double_Filter_20_Nulls_20_next_5000_plain)              39.80ms    25.13
run(Double_Filter_20_Nulls_20_next_10000_dict)              63.06ms    15.86
run(Double_Filter_20_Nulls_20_next_10000_plain)             37.63ms    26.57
run(Double_Filter_20_Nulls_20_next_20000_dict)              61.80ms    16.18
run(Double_Filter_20_Nulls_20_next_20000_plain)             39.14ms    25.55
run(Double_Filter_20_Nulls_20_next_50000_dict)              60.85ms    16.43
run(Double_Filter_20_Nulls_20_next_50000_plain)             39.29ms    25.45
run(Double_Filter_20_Nulls_20_next_100000_dict)             61.68ms    16.21
run(Double_Filter_20_Nulls_20_next_100000_plain             42.16ms    23.72
----------------------------------------------------------------------------
run(Double_Filter_20_Nulls_50_next_5000_dict)               40.78ms    24.52
run(Double_Filter_20_Nulls_50_next_5000_plain)              24.39ms    41.00
run(Double_Filter_20_Nulls_50_next_10000_dict)              42.00ms    23.81
run(Double_Filter_20_Nulls_50_next_10000_plain)             24.88ms    40.19
run(Double_Filter_20_Nulls_50_next_20000_dict)              36.25ms    27.59
run(Double_Filter_20_Nulls_50_next_20000_plain)             23.34ms    42.84
run(Double_Filter_20_Nulls_50_next_50000_dict)              36.43ms    27.45
run(Double_Filter_20_Nulls_50_next_50000_plain)             22.84ms    43.79
run(Double_Filter_20_Nulls_50_next_100000_dict)             38.94ms    25.68
run(Double_Filter_20_Nulls_50_next_100000_plain             22.77ms    43.92
----------------------------------------------------------------------------
run(Double_Filter_20_Nulls_70_next_5000_dict)               27.03ms    37.00
run(Double_Filter_20_Nulls_70_next_5000_plain)              18.22ms    54.87
run(Double_Filter_20_Nulls_70_next_10000_dict)              26.79ms    37.32
run(Double_Filter_20_Nulls_70_next_10000_plain)             17.61ms    56.78
run(Double_Filter_20_Nulls_70_next_20000_dict)              25.92ms    38.57
run(Double_Filter_20_Nulls_70_next_20000_plain)             18.08ms    55.30
run(Double_Filter_20_Nulls_70_next_50000_dict)              27.18ms    36.80
run(Double_Filter_20_Nulls_70_next_50000_plain)             17.78ms    56.24
run(Double_Filter_20_Nulls_70_next_100000_dict)             26.95ms    37.11
run(Double_Filter_20_Nulls_70_next_100000_plain             18.92ms    52.87
----------------------------------------------------------------------------
run(Double_Filter_20_Nulls_100_next_5000_dict)               1.03ms   974.38
run(Double_Filter_20_Nulls_100_next_5000_plain)            989.89us    1.01K
run(Double_Filter_20_Nulls_100_next_10000_dict)              1.04ms   962.33
run(Double_Filter_20_Nulls_100_next_10000_plain            969.66us    1.03K
run(Double_Filter_20_Nulls_100_next_20000_dict)              1.02ms   978.19
run(Double_Filter_20_Nulls_100_next_20000_plain            971.58us    1.03K
run(Double_Filter_20_Nulls_100_next_50000_dict)              1.08ms   926.03
run(Double_Filter_20_Nulls_100_next_50000_plain            982.96us    1.02K
run(Double_Filter_20_Nulls_100_next_100000_dict              1.03ms   969.36
run(Double_Filter_20_Nulls_100_next_100000_plai              1.01ms   994.29
----------------------------------------------------------------------------
run(Double_Filter_50_Nulls_0_next_5000_dict)                60.75ms    16.46
run(Double_Filter_50_Nulls_0_next_5000_plain)               43.76ms    22.85
run(Double_Filter_50_Nulls_0_next_10000_dict)               89.64ms    11.16
run(Double_Filter_50_Nulls_0_next_10000_plain)              43.14ms    23.18
run(Double_Filter_50_Nulls_0_next_20000_dict)               89.17ms    11.22
run(Double_Filter_50_Nulls_0_next_20000_plain)              44.32ms    22.57
run(Double_Filter_50_Nulls_0_next_50000_dict)               94.63ms    10.57
run(Double_Filter_50_Nulls_0_next_50000_plain)              43.73ms    22.87
run(Double_Filter_50_Nulls_0_next_100000_dict)              90.13ms    11.10
run(Double_Filter_50_Nulls_0_next_100000_plain)             43.07ms    23.22
----------------------------------------------------------------------------
run(Double_Filter_50_Nulls_20_next_5000_dict)               75.82ms    13.19
run(Double_Filter_50_Nulls_20_next_5000_plain)              38.75ms    25.81
run(Double_Filter_50_Nulls_20_next_10000_dict)              70.94ms    14.10
run(Double_Filter_50_Nulls_20_next_10000_plain)             41.69ms    23.99
run(Double_Filter_50_Nulls_20_next_20000_dict)              72.92ms    13.71
run(Double_Filter_50_Nulls_20_next_20000_plain)             37.63ms    26.58
run(Double_Filter_50_Nulls_20_next_50000_dict)              73.09ms    13.68
run(Double_Filter_50_Nulls_20_next_50000_plain)             37.24ms    26.85
run(Double_Filter_50_Nulls_20_next_100000_dict)             74.64ms    13.40
run(Double_Filter_50_Nulls_20_next_100000_plain             38.66ms    25.87
----------------------------------------------------------------------------
run(Double_Filter_50_Nulls_50_next_5000_dict)               76.24ms    13.12
run(Double_Filter_50_Nulls_50_next_5000_plain)              26.20ms    38.16
run(Double_Filter_50_Nulls_50_next_10000_dict)              47.02ms    21.27
run(Double_Filter_50_Nulls_50_next_10000_plain)             23.45ms    42.65
run(Double_Filter_50_Nulls_50_next_20000_dict)              42.83ms    23.35
run(Double_Filter_50_Nulls_50_next_20000_plain)             21.95ms    45.55
run(Double_Filter_50_Nulls_50_next_50000_dict)              43.95ms    22.75
run(Double_Filter_50_Nulls_50_next_50000_plain)             24.25ms    41.23
run(Double_Filter_50_Nulls_50_next_100000_dict)             43.21ms    23.14
run(Double_Filter_50_Nulls_50_next_100000_plain             12.26ms    81.56
----------------------------------------------------------------------------
run(Double_Filter_50_Nulls_70_next_5000_dict)               31.73ms    31.52
run(Double_Filter_50_Nulls_70_next_5000_plain)              17.60ms    56.83
run(Double_Filter_50_Nulls_70_next_10000_dict)              30.34ms    32.96
run(Double_Filter_50_Nulls_70_next_10000_plain)             17.59ms    56.85
run(Double_Filter_50_Nulls_70_next_20000_dict)              31.37ms    31.88
run(Double_Filter_50_Nulls_70_next_20000_plain)             17.78ms    56.25
run(Double_Filter_50_Nulls_70_next_50000_dict)              31.10ms    32.15
run(Double_Filter_50_Nulls_70_next_50000_plain)             18.49ms    54.08
run(Double_Filter_50_Nulls_70_next_100000_dict)             30.49ms    32.80
run(Double_Filter_50_Nulls_70_next_100000_plain             17.98ms    55.61
----------------------------------------------------------------------------
run(Double_Filter_50_Nulls_100_next_5000_dict)               1.05ms   952.20
run(Double_Filter_50_Nulls_100_next_5000_plain)            940.88us    1.06K
run(Double_Filter_50_Nulls_100_next_10000_dict)            997.32us    1.00K
run(Double_Filter_50_Nulls_100_next_10000_plain            964.69us    1.04K
run(Double_Filter_50_Nulls_100_next_20000_dict)            993.52us    1.01K
run(Double_Filter_50_Nulls_100_next_20000_plain            927.72us    1.08K
run(Double_Filter_50_Nulls_100_next_50000_dict)              1.06ms   943.52
run(Double_Filter_50_Nulls_100_next_50000_plain              1.00ms   998.13
run(Double_Filter_50_Nulls_100_next_100000_dict              1.01ms   987.37
run(Double_Filter_50_Nulls_100_next_100000_plai            971.42us    1.03K
----------------------------------------------------------------------------
run(Double_Filter_70_Nulls_0_next_5000_dict)                54.10ms    18.49
run(Double_Filter_70_Nulls_0_next_5000_plain)               46.98ms    21.29
run(Double_Filter_70_Nulls_0_next_10000_dict)               84.41ms    11.85
run(Double_Filter_70_Nulls_0_next_10000_plain)              47.35ms    21.12
run(Double_Filter_70_Nulls_0_next_20000_dict)               86.48ms    11.56
run(Double_Filter_70_Nulls_0_next_20000_plain)              48.22ms    20.74
run(Double_Filter_70_Nulls_0_next_50000_dict)               85.84ms    11.65
run(Double_Filter_70_Nulls_0_next_50000_plain)              47.65ms    20.98
run(Double_Filter_70_Nulls_0_next_100000_dict)              83.28ms    12.01
run(Double_Filter_70_Nulls_0_next_100000_plain)             48.58ms    20.59
----------------------------------------------------------------------------
run(Double_Filter_70_Nulls_20_next_5000_dict)               72.87ms    13.72
run(Double_Filter_70_Nulls_20_next_5000_plain)              41.28ms    24.22
run(Double_Filter_70_Nulls_20_next_10000_dict)              68.41ms    14.62
run(Double_Filter_70_Nulls_20_next_10000_plain)             42.69ms    23.43
run(Double_Filter_70_Nulls_20_next_20000_dict)              73.65ms    13.58
run(Double_Filter_70_Nulls_20_next_20000_plain)             42.63ms    23.46
run(Double_Filter_70_Nulls_20_next_50000_dict)              76.02ms    13.15
run(Double_Filter_70_Nulls_20_next_50000_plain)             37.97ms    26.34
run(Double_Filter_70_Nulls_20_next_100000_dict)             66.45ms    15.05
run(Double_Filter_70_Nulls_20_next_100000_plain             38.81ms    25.77
----------------------------------------------------------------------------
run(Double_Filter_70_Nulls_50_next_5000_dict)               55.83ms    17.91
run(Double_Filter_70_Nulls_50_next_5000_plain)              27.84ms    35.92
run(Double_Filter_70_Nulls_50_next_10000_dict)              40.23ms    24.86
run(Double_Filter_70_Nulls_50_next_10000_plain)             22.92ms    43.63
run(Double_Filter_70_Nulls_50_next_20000_dict)              40.95ms    24.42
run(Double_Filter_70_Nulls_50_next_20000_plain)             23.39ms    42.76
run(Double_Filter_70_Nulls_50_next_50000_dict)              42.80ms    23.37
run(Double_Filter_70_Nulls_50_next_50000_plain)             21.88ms    45.71
run(Double_Filter_70_Nulls_50_next_100000_dict)             35.93ms    27.84
run(Double_Filter_70_Nulls_50_next_100000_plain             24.71ms    40.47
----------------------------------------------------------------------------
run(Double_Filter_70_Nulls_70_next_5000_dict)               29.35ms    34.08
run(Double_Filter_70_Nulls_70_next_5000_plain)              17.64ms    56.68
run(Double_Filter_70_Nulls_70_next_10000_dict)              28.79ms    34.74
run(Double_Filter_70_Nulls_70_next_10000_plain)             16.69ms    59.92
run(Double_Filter_70_Nulls_70_next_20000_dict)              28.47ms    35.12
run(Double_Filter_70_Nulls_70_next_20000_plain)             18.71ms    53.44
run(Double_Filter_70_Nulls_70_next_50000_dict)              29.52ms    33.87
run(Double_Filter_70_Nulls_70_next_50000_plain)             17.42ms    57.39
run(Double_Filter_70_Nulls_70_next_100000_dict)             28.65ms    34.90
run(Double_Filter_70_Nulls_70_next_100000_plain             18.46ms    54.18
----------------------------------------------------------------------------
run(Double_Filter_70_Nulls_100_next_5000_dict)               1.07ms   936.64
run(Double_Filter_70_Nulls_100_next_5000_plain)              1.03ms   971.26
run(Double_Filter_70_Nulls_100_next_10000_dict)              1.07ms   938.79
run(Double_Filter_70_Nulls_100_next_10000_plain              1.06ms   941.38
run(Double_Filter_70_Nulls_100_next_20000_dict)              1.05ms   949.69
run(Double_Filter_70_Nulls_100_next_20000_plain            971.45us    1.03K
run(Double_Filter_70_Nulls_100_next_50000_dict)              1.06ms   939.00
run(Double_Filter_70_Nulls_100_next_50000_plain            980.12us    1.02K
run(Double_Filter_70_Nulls_100_next_100000_dict              1.06ms   945.02
run(Double_Filter_70_Nulls_100_next_100000_plai            982.13us    1.02K
----------------------------------------------------------------------------
run(Double_Filter_100_Nulls_0_next_5000_dict)               40.80ms    24.51
run(Double_Filter_100_Nulls_0_next_5000_plain)              49.75ms    20.10
run(Double_Filter_100_Nulls_0_next_10000_dict)              69.62ms    14.36
run(Double_Filter_100_Nulls_0_next_10000_plain)             17.99ms    55.59
run(Double_Filter_100_Nulls_0_next_20000_dict)              72.57ms    13.78
run(Double_Filter_100_Nulls_0_next_20000_plain)             45.56ms    21.95
run(Double_Filter_100_Nulls_0_next_50000_dict)              69.34ms    14.42
run(Double_Filter_100_Nulls_0_next_50000_plain)             45.71ms    21.88
run(Double_Filter_100_Nulls_0_next_100000_dict)             76.33ms    13.10
run(Double_Filter_100_Nulls_0_next_100000_plain             45.27ms    22.09
----------------------------------------------------------------------------
run(Double_Filter_100_Nulls_20_next_5000_dict)              65.50ms    15.27
run(Double_Filter_100_Nulls_20_next_5000_plain)             33.65ms    29.72
run(Double_Filter_100_Nulls_20_next_10000_dict)             53.99ms    18.52
run(Double_Filter_100_Nulls_20_next_10000_plain             37.36ms    26.77
run(Double_Filter_100_Nulls_20_next_20000_dict)             66.01ms    15.15
run(Double_Filter_100_Nulls_20_next_20000_plain             37.29ms    26.82
run(Double_Filter_100_Nulls_20_next_50000_dict)             56.38ms    17.74
run(Double_Filter_100_Nulls_20_next_50000_plain             36.22ms    27.61
run(Double_Filter_100_Nulls_20_next_100000_dict             57.45ms    17.41
run(Double_Filter_100_Nulls_20_next_100000_plai             47.34ms    21.12
----------------------------------------------------------------------------
run(Double_Filter_100_Nulls_50_next_5000_dict)              34.34ms    29.12
run(Double_Filter_100_Nulls_50_next_5000_plain)             21.40ms    46.72
run(Double_Filter_100_Nulls_50_next_10000_dict)             33.00ms    30.31
run(Double_Filter_100_Nulls_50_next_10000_plain             20.74ms    48.21
run(Double_Filter_100_Nulls_50_next_20000_dict)             34.59ms    28.91
run(Double_Filter_100_Nulls_50_next_20000_plain             20.42ms    48.96
run(Double_Filter_100_Nulls_50_next_50000_dict)             33.41ms    29.93
run(Double_Filter_100_Nulls_50_next_50000_plain             20.63ms    48.47
run(Double_Filter_100_Nulls_50_next_100000_dict             33.38ms    29.96
run(Double_Filter_100_Nulls_50_next_100000_plai             20.71ms    48.28
----------------------------------------------------------------------------
run(Double_Filter_100_Nulls_70_next_5000_dict)              25.27ms    39.58
run(Double_Filter_100_Nulls_70_next_5000_plain)             17.30ms    57.80
run(Double_Filter_100_Nulls_70_next_10000_dict)             25.28ms    39.56
run(Double_Filter_100_Nulls_70_next_10000_plain             15.70ms    63.71
run(Double_Filter_100_Nulls_70_next_20000_dict)             24.43ms    40.93
run(Double_Filter_100_Nulls_70_next_20000_plain             17.23ms    58.05
run(Double_Filter_100_Nulls_70_next_50000_dict)             24.63ms    40.60
run(Double_Filter_100_Nulls_70_next_50000_plain             16.71ms    59.83
run(Double_Filter_100_Nulls_70_next_100000_dict             24.18ms    41.36
run(Double_Filter_100_Nulls_70_next_100000_plai             18.43ms    54.27
----------------------------------------------------------------------------
run(Double_Filter_100_Nulls_100_next_5000_dict)              1.04ms   957.49
run(Double_Filter_100_Nulls_100_next_5000_plain            955.36us    1.05K
run(Double_Filter_100_Nulls_100_next_10000_dict              1.08ms   921.74
run(Double_Filter_100_Nulls_100_next_10000_plai              1.01ms   993.71
run(Double_Filter_100_Nulls_100_next_20000_dict              1.05ms   953.27
run(Double_Filter_100_Nulls_100_next_20000_plai            993.23us    1.01K
run(Double_Filter_100_Nulls_100_next_50000_dict              1.07ms   932.16
run(Double_Filter_100_Nulls_100_next_50000_plai            993.38us    1.01K
run(Double_Filter_100_Nulls_100_next_100000_dic              1.01ms   991.20
run(Double_Filter_100_Nulls_100_next_100000_pla              1.00ms   995.84
----------------------------------------------------------------------------
----------------------------------------------------------------------------
*/
