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
#include "velox/exec/tests/utils/TempDirectoryPath.h"

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
    pool_ = memory::addDefaultLeafMemoryPool();
    dataSetBuilder_ = std::make_unique<DataSetBuilder>(*pool_.get(), 0);
    auto sink =
        std::make_unique<LocalFileSink>(fileFolder_->path + "/" + fileName_);
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
    auto filters = filterGenerator->makeSubfieldFilters(
        filterSpecs, batches, nullptr, hitRows);
    auto scanSpec = filterGenerator->makeScanSpec(std::move(filters));
    return scanSpec;
  }

  std::unique_ptr<RowReader> createReader(
      const ParquetReaderType& parquetReaderType,
      std::shared_ptr<ScanSpec> scanSpec,
      const RowTypePtr& rowType) {
    dwio::common::ReaderOptions readerOpts{pool_.get()};
    auto input = std::make_unique<BufferedInput>(
        std::make_shared<LocalReadFile>(fileFolder_->path + "/" + fileName_),
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
      if (result->size() == 0) {
        continue;
      }

      auto rowVector = result->asUnchecked<RowVector>();
      for (auto i = 0; i < rowVector->childrenSize(); ++i) {
        rowVector->childAt(i)->loadedVector();
      }

      VELOX_CHECK_EQ(
          rowVector->childrenSize(),
          1,
          "The benchmark is performed on single columns. So the result should only contain one column.")

      for (int i = 0; i < rowVector->size(); i++) {
        resultSize += !rowVector->childAt(0)->isNullAt(i);
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
    std::vector<FilterSpec> filterSpecs;

    //    Filters on List and Map are not supported currently.
    if (type->kind() != TypeKind::ARRAY && type->kind() != TypeKind::MAP) {
      filterSpecs.emplace_back(createFilterSpec(
          columnName, startPct, selectPct, rowType, false, false));
    }

    std::vector<uint64_t> hitRows;
    auto scanSpec = createScanSpec(*batches, rowType, filterSpecs, hitRows);

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
  const std::string fileName_ = "test.parquet";
  const std::shared_ptr<facebook::velox::exec::test::TempDirectoryPath>
      fileFolder_ = facebook::velox::exec::test::TempDirectoryPath::create();
  const bool disableDictionary_;

  std::unique_ptr<test::DataSetBuilder> dataSetBuilder_;
  std::shared_ptr<memory::MemoryPool> pool_;
  dwio::common::DataSink* sinkPtr_;
  std::unique_ptr<facebook::velox::parquet::Writer> writer_;
  RuntimeStatistics runtimeStats_;
};

void run(
    uint32_t,
    const std::string& columnName,
    const TypePtr& type,
    float filterRateX100,
    uint8_t nullsRateX100,
    uint32_t nextSize,
    bool disableDictionary) {
  ParquetReaderBenchmark benchmark(disableDictionary);
  BIGINT()->toString();
  benchmark.readSingleColumn(
      ParquetReaderType::NATIVE,
      columnName,
      type,
      0,
      filterRateX100,
      nullsRateX100,
      nextSize);
}

#define PARQUET_BENCHMARKS_FILTER_NULLS(_type_, _name_, _filter_, _null_) \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_5k_dict,         \
      #_name_,                                                            \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      5000,                                                               \
      false);                                                             \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_5k_plain,        \
      #_name_,                                                            \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      5000,                                                               \
      true);                                                              \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_10k_dict,        \
      #_name_,                                                            \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      10000,                                                              \
      false);                                                             \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_10k_Plain,       \
      #_name_,                                                            \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      10000,                                                              \
      true);                                                              \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_20k_dict,        \
      #_name_,                                                            \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      20000,                                                              \
      false);                                                             \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_20k_plain,       \
      #_name_,                                                            \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      20000,                                                              \
      true);                                                              \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_50k_dict,        \
      #_name_,                                                            \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      50000,                                                              \
      false);                                                             \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_50k_plain,       \
      #_name_,                                                            \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      50000,                                                              \
      true);                                                              \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_100k_dict,       \
      #_name_,                                                            \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      100000,                                                             \
      false);                                                             \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_100k_plain,      \
      #_name_,                                                            \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      100000,                                                             \
      true);                                                              \
  BENCHMARK_DRAW_LINE();

#define PARQUET_BENCHMARKS_FILTERS(_type_, _name_, _filter_)    \
  PARQUET_BENCHMARKS_FILTER_NULLS(_type_, _name_, _filter_, 0)  \
  PARQUET_BENCHMARKS_FILTER_NULLS(_type_, _name_, _filter_, 20) \
  PARQUET_BENCHMARKS_FILTER_NULLS(_type_, _name_, _filter_, 50) \
  PARQUET_BENCHMARKS_FILTER_NULLS(_type_, _name_, _filter_, 70) \
  PARQUET_BENCHMARKS_FILTER_NULLS(_type_, _name_, _filter_, 100)

#define PARQUET_BENCHMARKS(_type_, _name_)        \
  PARQUET_BENCHMARKS_FILTERS(_type_, _name_, 0)   \
  PARQUET_BENCHMARKS_FILTERS(_type_, _name_, 20)  \
  PARQUET_BENCHMARKS_FILTERS(_type_, _name_, 50)  \
  PARQUET_BENCHMARKS_FILTERS(_type_, _name_, 70)  \
  PARQUET_BENCHMARKS_FILTERS(_type_, _name_, 100) \
  BENCHMARK_DRAW_LINE();

#define PARQUET_BENCHMARKS_NO_FILTER(_type_, _name_) \
  PARQUET_BENCHMARKS_FILTERS(_type_, _name_, 100)    \
  BENCHMARK_DRAW_LINE();

PARQUET_BENCHMARKS(BIGINT(), BigInt);
PARQUET_BENCHMARKS(DOUBLE(), Double);
PARQUET_BENCHMARKS_NO_FILTER(MAP(BIGINT(), BIGINT()), Map);
PARQUET_BENCHMARKS_NO_FILTER(ARRAY(BIGINT()), List);

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
relative                                                  time/iter  iters/s
============================================================================
run(BigInt_Filter_0_Nulls_0_next_5k_dict)                    7.75ms   129.03
run(BigInt_Filter_0_Nulls_0_next_5k_plain)                  41.18ms    24.28
run(BigInt_Filter_0_Nulls_0_next_10k_dict)                  29.86ms    33.49
run(BigInt_Filter_0_Nulls_0_next_10k_Plain)                 40.74ms    24.54
run(BigInt_Filter_0_Nulls_0_next_20k_dict)                  62.92ms    15.89
run(BigInt_Filter_0_Nulls_0_next_20k_plain)                 24.68ms    40.52
run(BigInt_Filter_0_Nulls_0_next_50k_dict)                  60.66ms    16.49
run(BigInt_Filter_0_Nulls_0_next_50k_plain)                 13.58ms    73.63
run(BigInt_Filter_0_Nulls_0_next_100k_dict)                 42.61ms    23.47
run(BigInt_Filter_0_Nulls_0_next_100k_plain)                31.32ms    31.93
----------------------------------------------------------------------------
run(BigInt_Filter_0_Nulls_20_next_5k_dict)                  24.36ms    41.04
run(BigInt_Filter_0_Nulls_20_next_5k_plain)                 27.84ms    35.92
run(BigInt_Filter_0_Nulls_20_next_10k_dict)                 12.15ms    82.33
run(BigInt_Filter_0_Nulls_20_next_10k_Plain)                30.98ms    32.28
run(BigInt_Filter_0_Nulls_20_next_20k_dict)                 36.97ms    27.05
run(BigInt_Filter_0_Nulls_20_next_20k_plain)                30.09ms    33.23
run(BigInt_Filter_0_Nulls_20_next_50k_dict)                 44.06ms    22.70
run(BigInt_Filter_0_Nulls_20_next_50k_plain)                11.44ms    87.43
run(BigInt_Filter_0_Nulls_20_next_100k_dict)                54.49ms    18.35
run(BigInt_Filter_0_Nulls_20_next_100k_plain)                8.10ms   123.39
----------------------------------------------------------------------------
run(BigInt_Filter_0_Nulls_50_next_5k_dict)                  24.37ms    41.03
run(BigInt_Filter_0_Nulls_50_next_5k_plain)                 30.68ms    32.60
run(BigInt_Filter_0_Nulls_50_next_10k_dict)                  7.20ms   138.94
run(BigInt_Filter_0_Nulls_50_next_10k_Plain)                15.61ms    64.04
run(BigInt_Filter_0_Nulls_50_next_20k_dict)                 17.66ms    56.63
run(BigInt_Filter_0_Nulls_50_next_20k_plain)                17.82ms    56.12
run(BigInt_Filter_0_Nulls_50_next_50k_dict)                 19.38ms    51.59
run(BigInt_Filter_0_Nulls_50_next_50k_plain)                11.77ms    84.97
run(BigInt_Filter_0_Nulls_50_next_100k_dict)                14.20ms    70.44
run(BigInt_Filter_0_Nulls_50_next_100k_plain)               14.86ms    67.31
----------------------------------------------------------------------------
run(BigInt_Filter_0_Nulls_70_next_5k_dict)                  19.18ms    52.15
run(BigInt_Filter_0_Nulls_70_next_5k_plain)                 10.37ms    96.47
run(BigInt_Filter_0_Nulls_70_next_10k_dict)                 11.02ms    90.76
run(BigInt_Filter_0_Nulls_70_next_10k_Plain)                11.13ms    89.89
run(BigInt_Filter_0_Nulls_70_next_20k_dict)                 10.43ms    95.85
run(BigInt_Filter_0_Nulls_70_next_20k_plain)                10.60ms    94.31
run(BigInt_Filter_0_Nulls_70_next_50k_dict)                 12.00ms    83.36
run(BigInt_Filter_0_Nulls_70_next_50k_plain)                12.04ms    83.03
run(BigInt_Filter_0_Nulls_70_next_100k_dict)                11.81ms    84.65
run(BigInt_Filter_0_Nulls_70_next_100k_plain)               10.59ms    94.39
----------------------------------------------------------------------------
run(BigInt_Filter_0_Nulls_100_next_5k_dict)                  1.07ms   938.83
run(BigInt_Filter_0_Nulls_100_next_5k_plain)                 1.01ms   991.27
run(BigInt_Filter_0_Nulls_100_next_10k_dict)                 1.06ms   941.26
run(BigInt_Filter_0_Nulls_100_next_10k_Plain)                1.02ms   979.09
run(BigInt_Filter_0_Nulls_100_next_20k_dict)                 1.06ms   945.97
run(BigInt_Filter_0_Nulls_100_next_20k_plain)                1.02ms   978.10
run(BigInt_Filter_0_Nulls_100_next_50k_dict)                 1.09ms   916.37
run(BigInt_Filter_0_Nulls_100_next_50k_plain)                1.02ms   976.12
run(BigInt_Filter_0_Nulls_100_next_100k_dict)                1.07ms   936.30
run(BigInt_Filter_0_Nulls_100_next_100k_plain)               1.01ms   986.33
----------------------------------------------------------------------------
run(BigInt_Filter_20_Nulls_0_next_5k_dict)                  50.42ms    19.83
run(BigInt_Filter_20_Nulls_0_next_5k_plain)                 55.73ms    17.94
run(BigInt_Filter_20_Nulls_0_next_10k_dict)                 80.05ms    12.49
run(BigInt_Filter_20_Nulls_0_next_10k_Plain)                58.70ms    17.04
run(BigInt_Filter_20_Nulls_0_next_20k_dict)                 70.42ms    14.20
run(BigInt_Filter_20_Nulls_0_next_20k_plain)                59.00ms    16.95
run(BigInt_Filter_20_Nulls_0_next_50k_dict)                 83.51ms    11.97
run(BigInt_Filter_20_Nulls_0_next_50k_plain)                25.46ms    39.27
run(BigInt_Filter_20_Nulls_0_next_100k_dict)                75.88ms    13.18
run(BigInt_Filter_20_Nulls_0_next_100k_plain)               60.11ms    16.64
----------------------------------------------------------------------------
run(BigInt_Filter_20_Nulls_20_next_5k_dict)                 65.20ms    15.34
run(BigInt_Filter_20_Nulls_20_next_5k_plain)                42.99ms    23.26
run(BigInt_Filter_20_Nulls_20_next_10k_dict)                67.89ms    14.73
run(BigInt_Filter_20_Nulls_20_next_10k_Plain)               40.18ms    24.89
run(BigInt_Filter_20_Nulls_20_next_20k_dict)                65.53ms    15.26
run(BigInt_Filter_20_Nulls_20_next_20k_plain)               40.92ms    24.44
run(BigInt_Filter_20_Nulls_20_next_50k_dict)                70.80ms    14.12
run(BigInt_Filter_20_Nulls_20_next_50k_plain)               38.04ms    26.29
run(BigInt_Filter_20_Nulls_20_next_100k_dict)               62.03ms    16.12
run(BigInt_Filter_20_Nulls_20_next_100k_plain)              43.54ms    22.97
----------------------------------------------------------------------------
run(BigInt_Filter_20_Nulls_50_next_5k_dict)                 49.09ms    20.37
run(BigInt_Filter_20_Nulls_50_next_5k_plain)                25.91ms    38.60
run(BigInt_Filter_20_Nulls_50_next_10k_dict)                37.13ms    26.93
run(BigInt_Filter_20_Nulls_50_next_10k_Plain)               27.16ms    36.82
run(BigInt_Filter_20_Nulls_50_next_20k_dict)                37.01ms    27.02
run(BigInt_Filter_20_Nulls_50_next_20k_plain)               26.73ms    37.41
run(BigInt_Filter_20_Nulls_50_next_50k_dict)                36.27ms    27.57
run(BigInt_Filter_20_Nulls_50_next_50k_plain)               25.01ms    39.98
run(BigInt_Filter_20_Nulls_50_next_100k_dict)               36.16ms    27.65
run(BigInt_Filter_20_Nulls_50_next_100k_plain)              27.30ms    36.63
----------------------------------------------------------------------------
run(BigInt_Filter_20_Nulls_70_next_5k_dict)                 34.33ms    29.13
run(BigInt_Filter_20_Nulls_70_next_5k_plain)                18.45ms    54.19
run(BigInt_Filter_20_Nulls_70_next_10k_dict)                25.55ms    39.14
run(BigInt_Filter_20_Nulls_70_next_10k_Plain)               20.94ms    47.75
run(BigInt_Filter_20_Nulls_70_next_20k_dict)                29.33ms    34.10
run(BigInt_Filter_20_Nulls_70_next_20k_plain)               19.46ms    51.38
run(BigInt_Filter_20_Nulls_70_next_50k_dict)                28.41ms    35.20
run(BigInt_Filter_20_Nulls_70_next_50k_plain)               18.20ms    54.93
run(BigInt_Filter_20_Nulls_70_next_100k_dict)               25.68ms    38.94
run(BigInt_Filter_20_Nulls_70_next_100k_plain)              18.88ms    52.97
----------------------------------------------------------------------------
run(BigInt_Filter_20_Nulls_100_next_5k_dict)                 1.16ms   860.57
run(BigInt_Filter_20_Nulls_100_next_5k_plain)                1.01ms   992.49
run(BigInt_Filter_20_Nulls_100_next_10k_dict)                1.10ms   907.85
run(BigInt_Filter_20_Nulls_100_next_10k_Plain)             999.27us    1.00K
run(BigInt_Filter_20_Nulls_100_next_20k_dict)                1.12ms   890.24
run(BigInt_Filter_20_Nulls_100_next_20k_plain)               1.06ms   942.98
run(BigInt_Filter_20_Nulls_100_next_50k_dict)                1.15ms   872.19
run(BigInt_Filter_20_Nulls_100_next_50k_plain)               1.06ms   945.59
run(BigInt_Filter_20_Nulls_100_next_100k_dict)               1.11ms   896.97
run(BigInt_Filter_20_Nulls_100_next_100k_plain)              1.08ms   926.88
----------------------------------------------------------------------------
run(BigInt_Filter_50_Nulls_0_next_5k_dict)                  94.90ms    10.54
run(BigInt_Filter_50_Nulls_0_next_5k_plain)                 26.45ms    37.81
run(BigInt_Filter_50_Nulls_0_next_10k_dict)                188.07ms     5.32
run(BigInt_Filter_50_Nulls_0_next_10k_Plain)                83.78ms    11.94
run(BigInt_Filter_50_Nulls_0_next_20k_dict)                 85.43ms    11.71
run(BigInt_Filter_50_Nulls_0_next_20k_plain)                56.34ms    17.75
run(BigInt_Filter_50_Nulls_0_next_50k_dict)                 99.34ms    10.07
run(BigInt_Filter_50_Nulls_0_next_50k_plain)                42.77ms    23.38
run(BigInt_Filter_50_Nulls_0_next_100k_dict)                97.26ms    10.28
run(BigInt_Filter_50_Nulls_0_next_100k_plain)               54.87ms    18.22
----------------------------------------------------------------------------
run(BigInt_Filter_50_Nulls_20_next_5k_dict)                 87.86ms    11.38
run(BigInt_Filter_50_Nulls_20_next_5k_plain)                45.31ms    22.07
run(BigInt_Filter_50_Nulls_20_next_10k_dict)                73.18ms    13.66
run(BigInt_Filter_50_Nulls_20_next_10k_Plain)               49.73ms    20.11
run(BigInt_Filter_50_Nulls_20_next_20k_dict)                73.67ms    13.57
run(BigInt_Filter_50_Nulls_20_next_20k_plain)               42.47ms    23.54
run(BigInt_Filter_50_Nulls_20_next_50k_dict)                73.13ms    13.67
run(BigInt_Filter_50_Nulls_20_next_50k_plain)               48.73ms    20.52
run(BigInt_Filter_50_Nulls_20_next_100k_dict)               74.09ms    13.50
run(BigInt_Filter_50_Nulls_20_next_100k_plain)              44.34ms    22.55
----------------------------------------------------------------------------
run(BigInt_Filter_50_Nulls_50_next_5k_dict)                 54.68ms    18.29
run(BigInt_Filter_50_Nulls_50_next_5k_plain)                29.64ms    33.74
run(BigInt_Filter_50_Nulls_50_next_10k_dict)                46.24ms    21.63
run(BigInt_Filter_50_Nulls_50_next_10k_Plain)               25.79ms    38.78
run(BigInt_Filter_50_Nulls_50_next_20k_dict)                44.20ms    22.62
run(BigInt_Filter_50_Nulls_50_next_20k_plain)               24.72ms    40.45
run(BigInt_Filter_50_Nulls_50_next_50k_dict)                45.32ms    22.06
run(BigInt_Filter_50_Nulls_50_next_50k_plain)               46.27ms    21.61
run(BigInt_Filter_50_Nulls_50_next_100k_dict)              101.24ms     9.88
run(BigInt_Filter_50_Nulls_50_next_100k_plain)              27.50ms    36.36
----------------------------------------------------------------------------
run(BigInt_Filter_50_Nulls_70_next_5k_dict)                 39.17ms    25.53
run(BigInt_Filter_50_Nulls_70_next_5k_plain)                19.61ms    51.00
run(BigInt_Filter_50_Nulls_70_next_10k_dict)                32.26ms    31.00
run(BigInt_Filter_50_Nulls_70_next_10k_Plain)               17.96ms    55.67
run(BigInt_Filter_50_Nulls_70_next_20k_dict)                31.52ms    31.72
run(BigInt_Filter_50_Nulls_70_next_20k_plain)               19.25ms    51.94
run(BigInt_Filter_50_Nulls_70_next_50k_dict)                32.78ms    30.50
run(BigInt_Filter_50_Nulls_70_next_50k_plain)               21.08ms    47.43
run(BigInt_Filter_50_Nulls_70_next_100k_dict)               31.40ms    31.85
run(BigInt_Filter_50_Nulls_70_next_100k_plain)              19.42ms    51.48
----------------------------------------------------------------------------
run(BigInt_Filter_50_Nulls_100_next_5k_dict)                 1.13ms   883.51
run(BigInt_Filter_50_Nulls_100_next_5k_plain)                1.01ms   989.30
run(BigInt_Filter_50_Nulls_100_next_10k_dict)                1.11ms   903.18
run(BigInt_Filter_50_Nulls_100_next_10k_Plain)               1.06ms   945.54
run(BigInt_Filter_50_Nulls_100_next_20k_dict)                1.17ms   853.65
run(BigInt_Filter_50_Nulls_100_next_20k_plain)               1.04ms   961.50
run(BigInt_Filter_50_Nulls_100_next_50k_dict)                1.10ms   905.35
run(BigInt_Filter_50_Nulls_100_next_50k_plain)               1.07ms   935.95
run(BigInt_Filter_50_Nulls_100_next_100k_dict)               1.14ms   876.28
run(BigInt_Filter_50_Nulls_100_next_100k_plain)              1.08ms   922.87
----------------------------------------------------------------------------
run(BigInt_Filter_70_Nulls_0_next_5k_dict)                  67.26ms    14.87
run(BigInt_Filter_70_Nulls_0_next_5k_plain)                 50.38ms    19.85
run(BigInt_Filter_70_Nulls_0_next_10k_dict)                 86.54ms    11.55
run(BigInt_Filter_70_Nulls_0_next_10k_Plain)                57.47ms    17.40
run(BigInt_Filter_70_Nulls_0_next_20k_dict)                 86.72ms    11.53
run(BigInt_Filter_70_Nulls_0_next_20k_plain)                30.38ms    32.92
run(BigInt_Filter_70_Nulls_0_next_50k_dict)                 86.81ms    11.52
run(BigInt_Filter_70_Nulls_0_next_50k_plain)                58.23ms    17.17
run(BigInt_Filter_70_Nulls_0_next_100k_dict)                91.78ms    10.90
run(BigInt_Filter_70_Nulls_0_next_100k_plain)               61.13ms    16.36
----------------------------------------------------------------------------
run(BigInt_Filter_70_Nulls_20_next_5k_dict)                 75.08ms    13.32
run(BigInt_Filter_70_Nulls_20_next_5k_plain)                50.99ms    19.61
run(BigInt_Filter_70_Nulls_20_next_10k_dict)                74.97ms    13.34
run(BigInt_Filter_70_Nulls_20_next_10k_Plain)               52.91ms    18.90
run(BigInt_Filter_70_Nulls_20_next_20k_dict)                72.78ms    13.74
run(BigInt_Filter_70_Nulls_20_next_20k_plain)               47.73ms    20.95
run(BigInt_Filter_70_Nulls_20_next_50k_dict)                75.43ms    13.26
run(BigInt_Filter_70_Nulls_20_next_50k_plain)               51.01ms    19.60
run(BigInt_Filter_70_Nulls_20_next_100k_dict)               70.30ms    14.22
run(BigInt_Filter_70_Nulls_20_next_100k_plain)              54.85ms    18.23
----------------------------------------------------------------------------
run(BigInt_Filter_70_Nulls_50_next_5k_dict)                 51.76ms    19.32
run(BigInt_Filter_70_Nulls_50_next_5k_plain)                31.75ms    31.50
run(BigInt_Filter_70_Nulls_50_next_10k_dict)                45.24ms    22.11
run(BigInt_Filter_70_Nulls_50_next_10k_Plain)               28.94ms    34.56
run(BigInt_Filter_70_Nulls_50_next_20k_dict)                41.10ms    24.33
run(BigInt_Filter_70_Nulls_50_next_20k_plain)               30.63ms    32.65
run(BigInt_Filter_70_Nulls_50_next_50k_dict)                46.30ms    21.60
run(BigInt_Filter_70_Nulls_50_next_50k_plain)               28.34ms    35.29
run(BigInt_Filter_70_Nulls_50_next_100k_dict)               47.60ms    21.01
run(BigInt_Filter_70_Nulls_50_next_100k_plain)              36.24ms    27.59
----------------------------------------------------------------------------
run(BigInt_Filter_70_Nulls_70_next_5k_dict)                 46.48ms    21.51
run(BigInt_Filter_70_Nulls_70_next_5k_plain)                17.26ms    57.92
run(BigInt_Filter_70_Nulls_70_next_10k_dict)                24.55ms    40.74
run(BigInt_Filter_70_Nulls_70_next_10k_Plain)               18.40ms    54.36
run(BigInt_Filter_70_Nulls_70_next_20k_dict)                32.09ms    31.16
run(BigInt_Filter_70_Nulls_70_next_20k_plain)               20.01ms    49.98
run(BigInt_Filter_70_Nulls_70_next_50k_dict)                32.16ms    31.09
run(BigInt_Filter_70_Nulls_70_next_50k_plain)               19.33ms    51.72
run(BigInt_Filter_70_Nulls_70_next_100k_dict)               28.15ms    35.53
run(BigInt_Filter_70_Nulls_70_next_100k_plain)              20.87ms    47.91
----------------------------------------------------------------------------
run(BigInt_Filter_70_Nulls_100_next_5k_dict)                 1.12ms   889.19
run(BigInt_Filter_70_Nulls_100_next_5k_plain)                1.08ms   925.58
run(BigInt_Filter_70_Nulls_100_next_10k_dict)                1.14ms   874.84
run(BigInt_Filter_70_Nulls_100_next_10k_Plain)               1.04ms   961.56
run(BigInt_Filter_70_Nulls_100_next_20k_dict)                1.10ms   905.32
run(BigInt_Filter_70_Nulls_100_next_20k_plain)               1.06ms   942.57
run(BigInt_Filter_70_Nulls_100_next_50k_dict)                1.13ms   887.46
run(BigInt_Filter_70_Nulls_100_next_50k_plain)               1.06ms   947.04
run(BigInt_Filter_70_Nulls_100_next_100k_dict)               1.11ms   900.77
run(BigInt_Filter_70_Nulls_100_next_100k_plain)              1.06ms   941.01
----------------------------------------------------------------------------
run(BigInt_Filter_100_Nulls_0_next_5k_dict)                 46.36ms    21.57
run(BigInt_Filter_100_Nulls_0_next_5k_plain)                59.90ms    16.69
run(BigInt_Filter_100_Nulls_0_next_10k_dict)                78.87ms    12.68
run(BigInt_Filter_100_Nulls_0_next_10k_Plain)               61.97ms    16.14
run(BigInt_Filter_100_Nulls_0_next_20k_dict)                75.71ms    13.21
run(BigInt_Filter_100_Nulls_0_next_20k_plain)               65.25ms    15.33
run(BigInt_Filter_100_Nulls_0_next_50k_dict)               129.60ms     7.72
run(BigInt_Filter_100_Nulls_0_next_50k_plain)              110.12ms     9.08
run(BigInt_Filter_100_Nulls_0_next_100k_dict)               91.04ms    10.98
run(BigInt_Filter_100_Nulls_0_next_100k_plain)              42.92ms    23.30
----------------------------------------------------------------------------
run(BigInt_Filter_100_Nulls_20_next_5k_dict)                77.86ms    12.84
run(BigInt_Filter_100_Nulls_20_next_5k_plain)               44.95ms    22.25
run(BigInt_Filter_100_Nulls_20_next_10k_dict)               72.22ms    13.85
run(BigInt_Filter_100_Nulls_20_next_10k_Plain)              52.12ms    19.19
run(BigInt_Filter_100_Nulls_20_next_20k_dict)               58.55ms    17.08
run(BigInt_Filter_100_Nulls_20_next_20k_plain)              49.64ms    20.15
run(BigInt_Filter_100_Nulls_20_next_50k_dict)               61.28ms    16.32
run(BigInt_Filter_100_Nulls_20_next_50k_plain)              47.43ms    21.09
run(BigInt_Filter_100_Nulls_20_next_100k_dict)              66.19ms    15.11
run(BigInt_Filter_100_Nulls_20_next_100k_plain)             45.53ms    21.97
----------------------------------------------------------------------------
run(BigInt_Filter_100_Nulls_50_next_5k_dict)                44.46ms    22.49
run(BigInt_Filter_100_Nulls_50_next_5k_plain)               26.50ms    37.73
run(BigInt_Filter_100_Nulls_50_next_10k_dict)               39.00ms    25.64
run(BigInt_Filter_100_Nulls_50_next_10k_Plain)              31.51ms    31.74
run(BigInt_Filter_100_Nulls_50_next_20k_dict)               35.79ms    27.94
run(BigInt_Filter_100_Nulls_50_next_20k_plain)              28.04ms    35.66
run(BigInt_Filter_100_Nulls_50_next_50k_dict)               36.43ms    27.45
run(BigInt_Filter_100_Nulls_50_next_50k_plain)              26.79ms    37.32
run(BigInt_Filter_100_Nulls_50_next_100k_dict)              37.73ms    26.50
run(BigInt_Filter_100_Nulls_50_next_100k_plain)             30.57ms    32.71
----------------------------------------------------------------------------
run(BigInt_Filter_100_Nulls_70_next_5k_dict)                31.60ms    31.64
run(BigInt_Filter_100_Nulls_70_next_5k_plain)               18.93ms    52.84
run(BigInt_Filter_100_Nulls_70_next_10k_dict)               25.33ms    39.48
run(BigInt_Filter_100_Nulls_70_next_10k_Plain)              20.15ms    49.63
run(BigInt_Filter_100_Nulls_70_next_20k_dict)               25.75ms    38.83
run(BigInt_Filter_100_Nulls_70_next_20k_plain)              18.67ms    53.57
run(BigInt_Filter_100_Nulls_70_next_50k_dict)               30.65ms    32.63
run(BigInt_Filter_100_Nulls_70_next_50k_plain)              54.27ms    18.43
run(BigInt_Filter_100_Nulls_70_next_100k_dict)              25.32ms    39.50
run(BigInt_Filter_100_Nulls_70_next_100k_plain)             18.07ms    55.35
----------------------------------------------------------------------------
run(BigInt_Filter_100_Nulls_100_next_5k_dict)                1.15ms   869.44
run(BigInt_Filter_100_Nulls_100_next_5k_plain)               1.05ms   951.19
run(BigInt_Filter_100_Nulls_100_next_10k_dict)               1.17ms   856.61
run(BigInt_Filter_100_Nulls_100_next_10k_Plain)              1.05ms   951.32
run(BigInt_Filter_100_Nulls_100_next_20k_dict)               1.12ms   891.29
run(BigInt_Filter_100_Nulls_100_next_20k_plain)              1.07ms   932.20
run(BigInt_Filter_100_Nulls_100_next_50k_dict)               1.11ms   899.75
run(BigInt_Filter_100_Nulls_100_next_50k_plain)              1.06ms   939.04
run(BigInt_Filter_100_Nulls_100_next_100k_dict)              1.11ms   897.09
run(BigInt_Filter_100_Nulls_100_next_100k_plain              1.08ms   926.95
----------------------------------------------------------------------------
----------------------------------------------------------------------------
run(Double_Filter_0_Nulls_0_next_5k_dict)                   46.04ms    21.72
run(Double_Filter_0_Nulls_0_next_5k_plain)                  10.34ms    96.76
run(Double_Filter_0_Nulls_0_next_10k_dict)                 116.04ms     8.62
run(Double_Filter_0_Nulls_0_next_10k_Plain)                 84.38ms    11.85
run(Double_Filter_0_Nulls_0_next_20k_dict)                  58.66ms    17.05
run(Double_Filter_0_Nulls_0_next_20k_plain)                 22.44ms    44.57
run(Double_Filter_0_Nulls_0_next_50k_dict)                  36.30ms    27.55
run(Double_Filter_0_Nulls_0_next_50k_plain)                 38.86ms    25.73
run(Double_Filter_0_Nulls_0_next_100k_dict)                 34.28ms    29.17
run(Double_Filter_0_Nulls_0_next_100k_plain)                40.39ms    24.76
----------------------------------------------------------------------------
run(Double_Filter_0_Nulls_20_next_5k_dict)                  36.77ms    27.20
run(Double_Filter_0_Nulls_20_next_5k_plain)                 29.27ms    34.16
run(Double_Filter_0_Nulls_20_next_10k_dict)                 28.53ms    35.05
run(Double_Filter_0_Nulls_20_next_10k_Plain)                48.94ms    20.43
run(Double_Filter_0_Nulls_20_next_20k_dict)                100.08ms     9.99
run(Double_Filter_0_Nulls_20_next_20k_plain)                61.53ms    16.25
run(Double_Filter_0_Nulls_20_next_50k_dict)                 47.61ms    21.01
run(Double_Filter_0_Nulls_20_next_50k_plain)                11.76ms    85.05
run(Double_Filter_0_Nulls_20_next_100k_dict)                29.92ms    33.42
run(Double_Filter_0_Nulls_20_next_100k_plain)               27.75ms    36.03
----------------------------------------------------------------------------
run(Double_Filter_0_Nulls_50_next_5k_dict)                  31.65ms    31.60
run(Double_Filter_0_Nulls_50_next_5k_plain)                 20.92ms    47.81
run(Double_Filter_0_Nulls_50_next_10k_dict)                 15.91ms    62.84
run(Double_Filter_0_Nulls_50_next_10k_Plain)                20.29ms    49.28
run(Double_Filter_0_Nulls_50_next_20k_dict)                 15.48ms    64.60
run(Double_Filter_0_Nulls_50_next_20k_plain)                19.93ms    50.17
run(Double_Filter_0_Nulls_50_next_50k_dict)                 23.60ms    42.37
run(Double_Filter_0_Nulls_50_next_50k_plain)                11.11ms    90.04
run(Double_Filter_0_Nulls_50_next_100k_dict)                18.63ms    53.69
run(Double_Filter_0_Nulls_50_next_100k_plain)               18.58ms    53.81
----------------------------------------------------------------------------
run(Double_Filter_0_Nulls_70_next_5k_dict)                  20.09ms    49.77
run(Double_Filter_0_Nulls_70_next_5k_plain)                 11.68ms    85.62
run(Double_Filter_0_Nulls_70_next_10k_dict)                 12.67ms    78.94
run(Double_Filter_0_Nulls_70_next_10k_Plain)                11.80ms    84.74
run(Double_Filter_0_Nulls_70_next_20k_dict)                 14.77ms    67.68
run(Double_Filter_0_Nulls_70_next_20k_plain)                11.40ms    87.69
run(Double_Filter_0_Nulls_70_next_50k_dict)                 12.71ms    78.65
run(Double_Filter_0_Nulls_70_next_50k_plain)                11.36ms    88.01
run(Double_Filter_0_Nulls_70_next_100k_dict)                12.45ms    80.35
run(Double_Filter_0_Nulls_70_next_100k_plain)               12.26ms    81.60
----------------------------------------------------------------------------
run(Double_Filter_0_Nulls_100_next_5k_dict)                  1.15ms   871.27
run(Double_Filter_0_Nulls_100_next_5k_plain)                 1.11ms   903.46
run(Double_Filter_0_Nulls_100_next_10k_dict)                 1.16ms   859.44
run(Double_Filter_0_Nulls_100_next_10k_Plain)                1.08ms   924.40
run(Double_Filter_0_Nulls_100_next_20k_dict)                 1.13ms   886.21
run(Double_Filter_0_Nulls_100_next_20k_plain)                1.05ms   947.89
run(Double_Filter_0_Nulls_100_next_50k_dict)                 1.17ms   853.94
run(Double_Filter_0_Nulls_100_next_50k_plain)                1.11ms   904.17
run(Double_Filter_0_Nulls_100_next_100k_dict)                1.13ms   881.83
run(Double_Filter_0_Nulls_100_next_100k_plain)               1.10ms   906.42
----------------------------------------------------------------------------
run(Double_Filter_20_Nulls_0_next_5k_dict)                  70.59ms    14.17
run(Double_Filter_20_Nulls_0_next_5k_plain)                 54.44ms    18.37
run(Double_Filter_20_Nulls_0_next_10k_dict)                 65.36ms    15.30
run(Double_Filter_20_Nulls_0_next_10k_Plain)                56.26ms    17.77
run(Double_Filter_20_Nulls_0_next_20k_dict)                107.09ms     9.34
run(Double_Filter_20_Nulls_0_next_20k_plain)                32.33ms    30.93
run(Double_Filter_20_Nulls_0_next_50k_dict)                 82.43ms    12.13
run(Double_Filter_20_Nulls_0_next_50k_plain)                55.24ms    18.10
run(Double_Filter_20_Nulls_0_next_100k_dict)                90.81ms    11.01
run(Double_Filter_20_Nulls_0_next_100k_plain)               45.57ms    21.95
----------------------------------------------------------------------------
run(Double_Filter_20_Nulls_20_next_5k_dict)                 68.66ms    14.56
run(Double_Filter_20_Nulls_20_next_5k_plain)                45.86ms    21.80
run(Double_Filter_20_Nulls_20_next_10k_dict)                85.67ms    11.67
run(Double_Filter_20_Nulls_20_next_10k_Plain)               43.48ms    23.00
run(Double_Filter_20_Nulls_20_next_20k_dict)                50.89ms    19.65
run(Double_Filter_20_Nulls_20_next_20k_plain)               45.35ms    22.05
run(Double_Filter_20_Nulls_20_next_50k_dict)                65.92ms    15.17
run(Double_Filter_20_Nulls_20_next_50k_plain)               52.20ms    19.16
run(Double_Filter_20_Nulls_20_next_100k_dict)               54.88ms    18.22
run(Double_Filter_20_Nulls_20_next_100k_plain)              44.52ms    22.46
----------------------------------------------------------------------------
run(Double_Filter_20_Nulls_50_next_5k_dict)                 50.85ms    19.67
run(Double_Filter_20_Nulls_50_next_5k_plain)                27.56ms    36.29
run(Double_Filter_20_Nulls_50_next_10k_dict)                40.32ms    24.80
run(Double_Filter_20_Nulls_50_next_10k_Plain)               25.39ms    39.38
run(Double_Filter_20_Nulls_50_next_20k_dict)                40.65ms    24.60
run(Double_Filter_20_Nulls_50_next_20k_plain)               31.98ms    31.27
run(Double_Filter_20_Nulls_50_next_50k_dict)                36.55ms    27.36
run(Double_Filter_20_Nulls_50_next_50k_plain)               39.60ms    25.25
run(Double_Filter_20_Nulls_50_next_100k_dict)               34.30ms    29.15
run(Double_Filter_20_Nulls_50_next_100k_plain)              24.74ms    40.43
----------------------------------------------------------------------------
run(Double_Filter_20_Nulls_70_next_5k_dict)                 35.97ms    27.80
run(Double_Filter_20_Nulls_70_next_5k_plain)                18.81ms    53.17
run(Double_Filter_20_Nulls_70_next_10k_dict)                29.70ms    33.67
run(Double_Filter_20_Nulls_70_next_10k_Plain)               21.07ms    47.47
run(Double_Filter_20_Nulls_70_next_20k_dict)                26.30ms    38.02
run(Double_Filter_20_Nulls_70_next_20k_plain)               17.80ms    56.17
run(Double_Filter_20_Nulls_70_next_50k_dict)                29.23ms    34.21
run(Double_Filter_20_Nulls_70_next_50k_plain)               19.76ms    50.61
run(Double_Filter_20_Nulls_70_next_100k_dict)               27.26ms    36.68
run(Double_Filter_20_Nulls_70_next_100k_plain)              19.21ms    52.06
----------------------------------------------------------------------------
run(Double_Filter_20_Nulls_100_next_5k_dict)                 1.19ms   838.33
run(Double_Filter_20_Nulls_100_next_5k_plain)                1.08ms   923.65
run(Double_Filter_20_Nulls_100_next_10k_dict)                1.15ms   867.52
run(Double_Filter_20_Nulls_100_next_10k_Plain)               1.09ms   917.52
run(Double_Filter_20_Nulls_100_next_20k_dict)                1.13ms   881.77
run(Double_Filter_20_Nulls_100_next_20k_plain)               1.08ms   922.16
run(Double_Filter_20_Nulls_100_next_50k_dict)                1.14ms   875.53
run(Double_Filter_20_Nulls_100_next_50k_plain)               1.09ms   914.11
run(Double_Filter_20_Nulls_100_next_100k_dict)               1.15ms   866.91
run(Double_Filter_20_Nulls_100_next_100k_plain)              1.12ms   888.90
----------------------------------------------------------------------------
run(Double_Filter_50_Nulls_0_next_5k_dict)                  66.61ms    15.01
run(Double_Filter_50_Nulls_0_next_5k_plain)                 87.16ms    11.47
run(Double_Filter_50_Nulls_0_next_10k_dict)                 69.75ms    14.34
run(Double_Filter_50_Nulls_0_next_10k_Plain)                61.24ms    16.33
run(Double_Filter_50_Nulls_0_next_20k_dict)                 91.42ms    10.94
run(Double_Filter_50_Nulls_0_next_20k_plain)                58.48ms    17.10
run(Double_Filter_50_Nulls_0_next_50k_dict)                115.35ms     8.67
run(Double_Filter_50_Nulls_0_next_50k_plain)                35.94ms    27.83
run(Double_Filter_50_Nulls_0_next_100k_dict)               106.14ms     9.42
run(Double_Filter_50_Nulls_0_next_100k_plain)               46.01ms    21.73
----------------------------------------------------------------------------
run(Double_Filter_50_Nulls_20_next_5k_dict)                 92.77ms    10.78
run(Double_Filter_50_Nulls_20_next_5k_plain)                44.99ms    22.23
run(Double_Filter_50_Nulls_20_next_10k_dict)               100.23ms     9.98
run(Double_Filter_50_Nulls_20_next_10k_Plain)               23.82ms    41.99
run(Double_Filter_50_Nulls_20_next_20k_dict)                79.26ms    12.62
run(Double_Filter_50_Nulls_20_next_20k_plain)               42.55ms    23.50
run(Double_Filter_50_Nulls_20_next_50k_dict)                86.56ms    11.55
run(Double_Filter_50_Nulls_20_next_50k_plain)               33.30ms    30.03
run(Double_Filter_50_Nulls_20_next_100k_dict)               82.79ms    12.08
run(Double_Filter_50_Nulls_20_next_100k_plain)              40.66ms    24.60
----------------------------------------------------------------------------
run(Double_Filter_50_Nulls_50_next_5k_dict)                 58.81ms    17.00
run(Double_Filter_50_Nulls_50_next_5k_plain)                23.38ms    42.76
run(Double_Filter_50_Nulls_50_next_10k_dict)                44.38ms    22.53
run(Double_Filter_50_Nulls_50_next_10k_Plain)               25.49ms    39.22
run(Double_Filter_50_Nulls_50_next_20k_dict)                47.56ms    21.03
run(Double_Filter_50_Nulls_50_next_20k_plain)               29.23ms    34.22
run(Double_Filter_50_Nulls_50_next_50k_dict)                47.74ms    20.95
run(Double_Filter_50_Nulls_50_next_50k_plain)               28.10ms    35.59
run(Double_Filter_50_Nulls_50_next_100k_dict)               45.19ms    22.13
run(Double_Filter_50_Nulls_50_next_100k_plain)              30.13ms    33.19
----------------------------------------------------------------------------
run(Double_Filter_50_Nulls_70_next_5k_dict)                 42.39ms    23.59
run(Double_Filter_50_Nulls_70_next_5k_plain)                19.13ms    52.27
run(Double_Filter_50_Nulls_70_next_10k_dict)                39.34ms    25.42
run(Double_Filter_50_Nulls_70_next_10k_Plain)               19.04ms    52.52
run(Double_Filter_50_Nulls_70_next_20k_dict)                31.11ms    32.15
run(Double_Filter_50_Nulls_70_next_20k_plain)               19.06ms    52.47
run(Double_Filter_50_Nulls_70_next_50k_dict)                32.22ms    31.03
run(Double_Filter_50_Nulls_70_next_50k_plain)               17.79ms    56.20
run(Double_Filter_50_Nulls_70_next_100k_dict)               29.08ms    34.39
run(Double_Filter_50_Nulls_70_next_100k_plain)              18.06ms    55.36
----------------------------------------------------------------------------
run(Double_Filter_50_Nulls_100_next_5k_dict)                 1.19ms   838.37
run(Double_Filter_50_Nulls_100_next_5k_plain)                1.11ms   900.88
run(Double_Filter_50_Nulls_100_next_10k_dict)                1.15ms   871.34
run(Double_Filter_50_Nulls_100_next_10k_Plain)               1.09ms   913.35
run(Double_Filter_50_Nulls_100_next_20k_dict)                1.20ms   832.54
run(Double_Filter_50_Nulls_100_next_20k_plain)               1.11ms   901.54
run(Double_Filter_50_Nulls_100_next_50k_dict)                1.19ms   840.64
run(Double_Filter_50_Nulls_100_next_50k_plain)               1.11ms   901.77
run(Double_Filter_50_Nulls_100_next_100k_dict)               1.18ms   844.59
run(Double_Filter_50_Nulls_100_next_100k_plain)              1.12ms   894.72
----------------------------------------------------------------------------
run(Double_Filter_70_Nulls_0_next_5k_dict)                  68.68ms    14.56
run(Double_Filter_70_Nulls_0_next_5k_plain)                 52.46ms    19.06
run(Double_Filter_70_Nulls_0_next_10k_dict)                104.38ms     9.58
run(Double_Filter_70_Nulls_0_next_10k_Plain)                45.86ms    21.81
run(Double_Filter_70_Nulls_0_next_20k_dict)                 92.56ms    10.80
run(Double_Filter_70_Nulls_0_next_20k_plain)                60.98ms    16.40
run(Double_Filter_70_Nulls_0_next_50k_dict)                126.62ms     7.90
run(Double_Filter_70_Nulls_0_next_50k_plain)                36.57ms    27.35
run(Double_Filter_70_Nulls_0_next_100k_dict)               106.28ms     9.41
run(Double_Filter_70_Nulls_0_next_100k_plain)               43.34ms    23.07
----------------------------------------------------------------------------
run(Double_Filter_70_Nulls_20_next_5k_dict)                 78.40ms    12.75
run(Double_Filter_70_Nulls_20_next_5k_plain)                26.06ms    38.37
run(Double_Filter_70_Nulls_20_next_10k_dict)                73.75ms    13.56
run(Double_Filter_70_Nulls_20_next_10k_Plain)               49.60ms    20.16
run(Double_Filter_70_Nulls_20_next_20k_dict)                76.06ms    13.15
run(Double_Filter_70_Nulls_20_next_20k_plain)               50.12ms    19.95
run(Double_Filter_70_Nulls_20_next_50k_dict)                75.28ms    13.28
run(Double_Filter_70_Nulls_20_next_50k_plain)               72.46ms    13.80
run(Double_Filter_70_Nulls_20_next_100k_dict)              111.26ms     8.99
run(Double_Filter_70_Nulls_20_next_100k_plain)             112.18ms     8.91
----------------------------------------------------------------------------
run(Double_Filter_70_Nulls_50_next_5k_dict)                 54.95ms    18.20
run(Double_Filter_70_Nulls_50_next_5k_plain)                32.73ms    30.56
run(Double_Filter_70_Nulls_50_next_10k_dict)                46.12ms    21.68
run(Double_Filter_70_Nulls_50_next_10k_Plain)               32.05ms    31.20
run(Double_Filter_70_Nulls_50_next_20k_dict)                41.23ms    24.26
run(Double_Filter_70_Nulls_50_next_20k_plain)               18.43ms    54.26
run(Double_Filter_70_Nulls_50_next_50k_dict)                43.51ms    22.99
run(Double_Filter_70_Nulls_50_next_50k_plain)               28.03ms    35.68
run(Double_Filter_70_Nulls_50_next_100k_dict)               48.58ms    20.59
run(Double_Filter_70_Nulls_50_next_100k_plain)              27.13ms    36.86
----------------------------------------------------------------------------
run(Double_Filter_70_Nulls_70_next_5k_dict)                 36.94ms    27.07
run(Double_Filter_70_Nulls_70_next_5k_plain)                21.51ms    46.48
run(Double_Filter_70_Nulls_70_next_10k_dict)                30.33ms    32.97
run(Double_Filter_70_Nulls_70_next_10k_Plain)               19.03ms    52.55
run(Double_Filter_70_Nulls_70_next_20k_dict)                29.70ms    33.68
run(Double_Filter_70_Nulls_70_next_20k_plain)               44.39ms    22.53
run(Double_Filter_70_Nulls_70_next_50k_dict)                29.47ms    33.93
run(Double_Filter_70_Nulls_70_next_50k_plain)               19.93ms    50.18
run(Double_Filter_70_Nulls_70_next_100k_dict)               30.55ms    32.73
run(Double_Filter_70_Nulls_70_next_100k_plain)              78.45ms    12.75
----------------------------------------------------------------------------
run(Double_Filter_70_Nulls_100_next_5k_dict)                 1.22ms   821.69
run(Double_Filter_70_Nulls_100_next_5k_plain)                1.08ms   922.01
run(Double_Filter_70_Nulls_100_next_10k_dict)                1.13ms   886.77
run(Double_Filter_70_Nulls_100_next_10k_Plain)               1.10ms   906.80
run(Double_Filter_70_Nulls_100_next_20k_dict)                1.25ms   801.04
run(Double_Filter_70_Nulls_100_next_20k_plain)               1.08ms   922.37
run(Double_Filter_70_Nulls_100_next_50k_dict)                1.13ms   881.13
run(Double_Filter_70_Nulls_100_next_50k_plain)               1.08ms   926.57
run(Double_Filter_70_Nulls_100_next_100k_dict)               1.16ms   859.24
run(Double_Filter_70_Nulls_100_next_100k_plain)              1.10ms   908.55
----------------------------------------------------------------------------
run(Double_Filter_100_Nulls_0_next_5k_dict)                 60.38ms    16.56
run(Double_Filter_100_Nulls_0_next_5k_plain)                77.51ms    12.90
run(Double_Filter_100_Nulls_0_next_10k_dict)                64.41ms    15.53
run(Double_Filter_100_Nulls_0_next_10k_Plain)               48.80ms    20.49
run(Double_Filter_100_Nulls_0_next_20k_dict)                77.02ms    12.98
run(Double_Filter_100_Nulls_0_next_20k_plain)               62.04ms    16.12
run(Double_Filter_100_Nulls_0_next_50k_dict)                70.91ms    14.10
run(Double_Filter_100_Nulls_0_next_50k_plain)               60.01ms    16.66
run(Double_Filter_100_Nulls_0_next_100k_dict)               79.32ms    12.61
run(Double_Filter_100_Nulls_0_next_100k_plain)              57.80ms    17.30
----------------------------------------------------------------------------
run(Double_Filter_100_Nulls_20_next_5k_dict)                69.99ms    14.29
run(Double_Filter_100_Nulls_20_next_5k_plain)               44.72ms    22.36
run(Double_Filter_100_Nulls_20_next_10k_dict)               63.70ms    15.70
run(Double_Filter_100_Nulls_20_next_10k_Plain)              45.46ms    22.00
run(Double_Filter_100_Nulls_20_next_20k_dict)               63.55ms    15.73
run(Double_Filter_100_Nulls_20_next_20k_plain)              44.95ms    22.25
run(Double_Filter_100_Nulls_20_next_50k_dict)               74.41ms    13.44
run(Double_Filter_100_Nulls_20_next_50k_plain)              33.79ms    29.60
run(Double_Filter_100_Nulls_20_next_100k_dict)              73.03ms    13.69
run(Double_Filter_100_Nulls_20_next_100k_plain)             36.55ms    27.36
----------------------------------------------------------------------------
run(Double_Filter_100_Nulls_50_next_5k_dict)                47.57ms    21.02
run(Double_Filter_100_Nulls_50_next_5k_plain)               30.64ms    32.63
run(Double_Filter_100_Nulls_50_next_10k_dict)               46.46ms    21.53
run(Double_Filter_100_Nulls_50_next_10k_Plain)              28.97ms    34.52
run(Double_Filter_100_Nulls_50_next_20k_dict)               36.57ms    27.35
run(Double_Filter_100_Nulls_50_next_20k_plain)              30.93ms    32.33
run(Double_Filter_100_Nulls_50_next_50k_dict)               39.82ms    25.11
run(Double_Filter_100_Nulls_50_next_50k_plain)              26.45ms    37.81
run(Double_Filter_100_Nulls_50_next_100k_dict)              38.63ms    25.89
run(Double_Filter_100_Nulls_50_next_100k_plain)             26.30ms    38.03
----------------------------------------------------------------------------
run(Double_Filter_100_Nulls_70_next_5k_dict)                35.46ms    28.20
run(Double_Filter_100_Nulls_70_next_5k_plain)               16.41ms    60.94
run(Double_Filter_100_Nulls_70_next_10k_dict)               26.78ms    37.34
run(Double_Filter_100_Nulls_70_next_10k_Plain)              23.31ms    42.90
run(Double_Filter_100_Nulls_70_next_20k_dict)               68.08ms    14.69
run(Double_Filter_100_Nulls_70_next_20k_plain)              19.56ms    51.12
run(Double_Filter_100_Nulls_70_next_50k_dict)               26.04ms    38.41
run(Double_Filter_100_Nulls_70_next_50k_plain)              19.09ms    52.37
run(Double_Filter_100_Nulls_70_next_100k_dict)              26.38ms    37.91
run(Double_Filter_100_Nulls_70_next_100k_plain)             19.99ms    50.03
----------------------------------------------------------------------------
run(Double_Filter_100_Nulls_100_next_5k_dict)                1.23ms   812.53
run(Double_Filter_100_Nulls_100_next_5k_plain)               1.16ms   865.38
run(Double_Filter_100_Nulls_100_next_10k_dict)               1.22ms   818.96
run(Double_Filter_100_Nulls_100_next_10k_Plain)              1.12ms   890.70
run(Double_Filter_100_Nulls_100_next_20k_dict)               1.18ms   850.97
run(Double_Filter_100_Nulls_100_next_20k_plain)              1.11ms   903.99
run(Double_Filter_100_Nulls_100_next_50k_dict)               1.17ms   852.25
run(Double_Filter_100_Nulls_100_next_50k_plain)              1.08ms   926.62
run(Double_Filter_100_Nulls_100_next_100k_dict)              1.21ms   823.54
run(Double_Filter_100_Nulls_100_next_100k_plain              1.07ms   931.79
----------------------------------------------------------------------------
----------------------------------------------------------------------------
run(Map_Filter_100_Nulls_0_next_5k_dict)                   512.01ms     1.95
run(Map_Filter_100_Nulls_0_next_5k_plain)                  519.26ms     1.93
run(Map_Filter_100_Nulls_0_next_10k_dict)                  491.47ms     2.03
run(Map_Filter_100_Nulls_0_next_10k_Plain)                 477.31ms     2.10
run(Map_Filter_100_Nulls_0_next_20k_dict)                  483.19ms     2.07
run(Map_Filter_100_Nulls_0_next_20k_plain)                 492.69ms     2.03
run(Map_Filter_100_Nulls_0_next_50k_dict)                  503.63ms     1.99
run(Map_Filter_100_Nulls_0_next_50k_plain)                 458.13ms     2.18
run(Map_Filter_100_Nulls_0_next_100k_dict)                 526.96ms     1.90
run(Map_Filter_100_Nulls_0_next_100k_plain)                484.31ms     2.06
----------------------------------------------------------------------------
run(Map_Filter_100_Nulls_20_next_5k_dict)                  485.14ms     2.06
run(Map_Filter_100_Nulls_20_next_5k_plain)                 523.81ms     1.91
run(Map_Filter_100_Nulls_20_next_10k_dict)                 487.79ms     2.05
run(Map_Filter_100_Nulls_20_next_10k_Plain)                501.78ms     1.99
run(Map_Filter_100_Nulls_20_next_20k_dict)                 480.07ms     2.08
run(Map_Filter_100_Nulls_20_next_20k_plain)                500.53ms     2.00
run(Map_Filter_100_Nulls_20_next_50k_dict)                 496.29ms     2.01
run(Map_Filter_100_Nulls_20_next_50k_plain)                449.78ms     2.22
run(Map_Filter_100_Nulls_20_next_100k_dict)                479.64ms     2.08
run(Map_Filter_100_Nulls_20_next_100k_plain)               451.55ms     2.21
----------------------------------------------------------------------------
run(Map_Filter_100_Nulls_50_next_5k_dict)                  377.07ms     2.65
run(Map_Filter_100_Nulls_50_next_5k_plain)                 344.94ms     2.90
run(Map_Filter_100_Nulls_50_next_10k_dict)                 380.58ms     2.63
run(Map_Filter_100_Nulls_50_next_10k_Plain)                334.15ms     2.99
run(Map_Filter_100_Nulls_50_next_20k_dict)                 397.08ms     2.52
run(Map_Filter_100_Nulls_50_next_20k_plain)                318.45ms     3.14
run(Map_Filter_100_Nulls_50_next_50k_dict)                 420.00ms     2.38
run(Map_Filter_100_Nulls_50_next_50k_plain)                328.35ms     3.05
run(Map_Filter_100_Nulls_50_next_100k_dict)                380.53ms     2.63
run(Map_Filter_100_Nulls_50_next_100k_plain)               335.41ms     2.98
----------------------------------------------------------------------------
run(Map_Filter_100_Nulls_70_next_5k_dict)                  269.50ms     3.71
run(Map_Filter_100_Nulls_70_next_5k_plain)                 251.71ms     3.97
run(Map_Filter_100_Nulls_70_next_10k_dict)                 245.63ms     4.07
run(Map_Filter_100_Nulls_70_next_10k_Plain)                237.48ms     4.21
run(Map_Filter_100_Nulls_70_next_20k_dict)                 242.31ms     4.13
run(Map_Filter_100_Nulls_70_next_20k_plain)                244.02ms     4.10
run(Map_Filter_100_Nulls_70_next_50k_dict)                 262.59ms     3.81
run(Map_Filter_100_Nulls_70_next_50k_plain)                230.39ms     4.34
run(Map_Filter_100_Nulls_70_next_100k_dict)                257.85ms     3.88
run(Map_Filter_100_Nulls_70_next_100k_plain)               231.06ms     4.33
----------------------------------------------------------------------------
run(Map_Filter_100_Nulls_100_next_5k_dict)                  52.11ms    19.19
run(Map_Filter_100_Nulls_100_next_5k_plain)                 50.11ms    19.95
run(Map_Filter_100_Nulls_100_next_10k_dict)                 49.96ms    20.02
run(Map_Filter_100_Nulls_100_next_10k_Plain)                50.56ms    19.78
run(Map_Filter_100_Nulls_100_next_20k_dict)                 50.12ms    19.95
run(Map_Filter_100_Nulls_100_next_20k_plain)                50.14ms    19.95
run(Map_Filter_100_Nulls_100_next_50k_dict)                 50.40ms    19.84
run(Map_Filter_100_Nulls_100_next_50k_plain)                50.28ms    19.89
run(Map_Filter_100_Nulls_100_next_100k_dict)                50.64ms    19.75
run(Map_Filter_100_Nulls_100_next_100k_plain)               50.84ms    19.67
----------------------------------------------------------------------------
----------------------------------------------------------------------------
run(List_Filter_100_Nulls_0_next_5k_dict)                  510.33ms     1.96
run(List_Filter_100_Nulls_0_next_5k_plain)                 470.09ms     2.13
run(List_Filter_100_Nulls_0_next_10k_dict)                 635.49ms     1.57
run(List_Filter_100_Nulls_0_next_10k_Plain)                479.80ms     2.08
run(List_Filter_100_Nulls_0_next_20k_dict)                 583.73ms     1.71
run(List_Filter_100_Nulls_0_next_20k_plain)                494.83ms     2.02
run(List_Filter_100_Nulls_0_next_50k_dict)                 578.41ms     1.73
run(List_Filter_100_Nulls_0_next_50k_plain)                480.41ms     2.08
run(List_Filter_100_Nulls_0_next_100k_dict)                515.73ms     1.94
run(List_Filter_100_Nulls_0_next_100k_plain)               492.10ms     2.03
----------------------------------------------------------------------------
run(List_Filter_100_Nulls_20_next_5k_dict)                 527.48ms     1.90
run(List_Filter_100_Nulls_20_next_5k_plain)                448.50ms     2.23
run(List_Filter_100_Nulls_20_next_10k_dict)                527.30ms     1.90
run(List_Filter_100_Nulls_20_next_10k_Plain)               450.71ms     2.22
run(List_Filter_100_Nulls_20_next_20k_dict)                513.22ms     1.95
run(List_Filter_100_Nulls_20_next_20k_plain)               475.91ms     2.10
run(List_Filter_100_Nulls_20_next_50k_dict)                509.59ms     1.96
run(List_Filter_100_Nulls_20_next_50k_plain)               487.91ms     2.05
run(List_Filter_100_Nulls_20_next_100k_dict)               551.87ms     1.81
run(List_Filter_100_Nulls_20_next_100k_plain)              444.48ms     2.25
----------------------------------------------------------------------------
run(List_Filter_100_Nulls_50_next_5k_dict)                 398.44ms     2.51
run(List_Filter_100_Nulls_50_next_5k_plain)                351.05ms     2.85
run(List_Filter_100_Nulls_50_next_10k_dict)                358.92ms     2.79
run(List_Filter_100_Nulls_50_next_10k_Plain)               361.92ms     2.76
run(List_Filter_100_Nulls_50_next_20k_dict)                365.71ms     2.73
run(List_Filter_100_Nulls_50_next_20k_plain)               379.67ms     2.63
run(List_Filter_100_Nulls_50_next_50k_dict)                386.53ms     2.59
run(List_Filter_100_Nulls_50_next_50k_plain)               333.08ms     3.00
run(List_Filter_100_Nulls_50_next_100k_dict)               391.00ms     2.56
run(List_Filter_100_Nulls_50_next_100k_plain)              314.15ms     3.18
----------------------------------------------------------------------------
run(List_Filter_100_Nulls_70_next_5k_dict)                 269.02ms     3.72
run(List_Filter_100_Nulls_70_next_5k_plain)                237.83ms     4.20
run(List_Filter_100_Nulls_70_next_10k_dict)                240.33ms     4.16
run(List_Filter_100_Nulls_70_next_10k_Plain)               242.11ms     4.13
run(List_Filter_100_Nulls_70_next_20k_dict)                243.19ms     4.11
run(List_Filter_100_Nulls_70_next_20k_plain)               262.23ms     3.81
run(List_Filter_100_Nulls_70_next_50k_dict)                241.58ms     4.14
run(List_Filter_100_Nulls_70_next_50k_plain)               237.81ms     4.21
run(List_Filter_100_Nulls_70_next_100k_dict)               262.14ms     3.81
run(List_Filter_100_Nulls_70_next_100k_plain)              238.90ms     4.19
----------------------------------------------------------------------------
run(List_Filter_100_Nulls_100_next_5k_dict)                 49.10ms    20.37
run(List_Filter_100_Nulls_100_next_5k_plain)                49.41ms    20.24
run(List_Filter_100_Nulls_100_next_10k_dict)                49.93ms    20.03
run(List_Filter_100_Nulls_100_next_10k_Plain)               49.90ms    20.04
run(List_Filter_100_Nulls_100_next_20k_dict)                49.38ms    20.25
run(List_Filter_100_Nulls_100_next_20k_plain)               48.86ms    20.47
run(List_Filter_100_Nulls_100_next_50k_dict)                48.91ms    20.45
run(List_Filter_100_Nulls_100_next_50k_plain)               49.07ms    20.38
run(List_Filter_100_Nulls_100_next_100k_dict)               49.25ms    20.31
run(List_Filter_100_Nulls_100_next_100k_plain)              48.90ms    20.45
----------------------------------------------------------------------------
----------------------------------------------------------------------------
*/
