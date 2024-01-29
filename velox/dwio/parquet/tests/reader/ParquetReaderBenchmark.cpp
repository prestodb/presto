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
  explicit ParquetReaderBenchmark(
      bool disableDictionary,
      const RowTypePtr& rowType)
      : disableDictionary_(disableDictionary) {
    rootPool_ = memory::memoryManager()->addRootPool("ParquetReaderBenchmark");
    leafPool_ = rootPool_->addLeafChild("ParquetReaderBenchmark");
    dataSetBuilder_ = std::make_unique<DataSetBuilder>(*leafPool_, 0);
    auto path = fileFolder_->path + "/" + fileName_;
    auto localWriteFile = std::make_unique<LocalWriteFile>(path, true, false);
    auto sink =
        std::make_unique<WriteFileSink>(std::move(localWriteFile), path);
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
      const std::vector<RowVectorPtr>& batches,
      bool /*forRowGroupSkip*/) {
    for (auto& batch : batches) {
      writer_->write(batch);
    }
    writer_->flush();
    writer_->close();
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
      case TypeKind::HUGEINT:
        return FilterSpec(
            columnName,
            startPct,
            selectPct,
            FilterKind::kHugeintRange,
            isForRowGroupSkip,
            allowNulls);
      case TypeKind::VARCHAR:
        return FilterSpec(
            columnName,
            startPct,
            selectPct,
            FilterKind::kBytesRange,
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
      std::shared_ptr<ScanSpec> scanSpec,
      const RowTypePtr& rowType) {
    dwio::common::ReaderOptions readerOpts{leafPool_.get()};
    auto input = std::make_unique<BufferedInput>(
        std::make_shared<LocalReadFile>(fileFolder_->path + "/" + fileName_),
        readerOpts.getMemoryPool());

    std::unique_ptr<Reader> reader =
        std::make_unique<ParquetReader>(std::move(input), readerOpts);

    dwio::common::RowReaderOptions rowReaderOpts;
    rowReaderOpts.select(
        std::make_shared<facebook::velox::dwio::common::ColumnSelector>(
            rowType, rowType->names()));
    rowReaderOpts.setScanSpec(scanSpec);
    auto rowReader = reader->createRowReader(rowReaderOpts);

    return rowReader;
  }

  // This method is the place where we do the read opeartions.
  // scanSpec contains the setting of filters. e.g.
  // filterRateX100 = 30 means it would filter out 70% of rows and 30% remain.
  // nullsRateX100 = 70 means it would filter out 70% of rows and 30% remain.
  // Return the number of rows after the filter and null-filter.
  int read(
      const RowTypePtr& rowType,
      std::shared_ptr<ScanSpec> scanSpec,
      uint32_t nextSize) {
    auto rowReader = createReader(scanSpec, rowType);
    runtimeStats_ = dwio::common::RuntimeStatistics();

    rowReader->resetFilterCaches();
    auto result = BaseVector::create(rowType, 1, leafPool_.get());
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
      const std::string& columnName,
      const TypePtr& type,
      float startPct,
      float selectPct,
      uint8_t nullsRateX100,
      uint32_t nextSize) {
    folly::BenchmarkSuspender suspender;

    auto rowType = ROW({columnName}, {type});
    // Generating the data (consider the null rate).
    auto batches =
        dataSetBuilder_->makeDataset(rowType, kNumBatches, kNumRowsPerBatch)
            .withRowGroupSpecificData(kNumRowsPerRowGroup)
            .withNullsForField(Subfield(columnName), nullsRateX100)
            .build();
    writeToFile(*batches, true);
    std::vector<FilterSpec> filterSpecs;

    // Filters on List and Map are not supported currently.
    if (type->kind() != TypeKind::ARRAY && type->kind() != TypeKind::MAP) {
      filterSpecs.emplace_back(createFilterSpec(
          columnName, startPct, selectPct, rowType, false, false));
    }

    std::vector<uint64_t> hitRows;
    auto scanSpec = createScanSpec(*batches, rowType, filterSpecs, hitRows);

    suspender.dismiss();

    // Filter range is generated from a small sample data of 4096 rows. So the
    // upperBound and lowerBound are introduced to estimate the result size.
    auto resultSize = read(rowType, scanSpec, nextSize);

    // Calculate the expected number of rows after the filters.
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
  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> leafPool_;
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
  RowTypePtr rowType = ROW({columnName}, {type});
  ParquetReaderBenchmark benchmark(disableDictionary, rowType);
  BIGINT()->toString();
  benchmark.readSingleColumn(
      columnName, type, 0, filterRateX100, nullsRateX100, nextSize);
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

PARQUET_BENCHMARKS(DECIMAL(18, 3), ShortDecimalType);
PARQUET_BENCHMARKS(DECIMAL(38, 3), LongDecimalType);
PARQUET_BENCHMARKS(VARCHAR(), Varchar);

PARQUET_BENCHMARKS(BIGINT(), BigInt);
PARQUET_BENCHMARKS(DOUBLE(), Double);
PARQUET_BENCHMARKS_NO_FILTER(MAP(BIGINT(), BIGINT()), Map);
PARQUET_BENCHMARKS_NO_FILTER(ARRAY(BIGINT()), List);

// TODO: Add all data types

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  memory::MemoryManager::initialize({});
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



CPU model name: AMD EPYC Processor (with IBPB)
Core(s) used: 16
Memory(GB): 32
============================================================================
relative                                                  time/iter  iters/s
============================================================================
run(ShortDecimalType_Filter_0_Nulls_0_next_5k_d            21.72ms     46.04
run(ShortDecimalType_Filter_0_Nulls_0_next_5k_p            15.15ms     66.03
run(ShortDecimalType_Filter_0_Nulls_0_next_10k_            16.90ms     59.19
run(ShortDecimalType_Filter_0_Nulls_0_next_10k_            15.26ms     65.53
run(ShortDecimalType_Filter_0_Nulls_0_next_20k_            16.73ms     59.79
run(ShortDecimalType_Filter_0_Nulls_0_next_20k_            14.76ms     67.75
run(ShortDecimalType_Filter_0_Nulls_0_next_50k_            16.59ms     60.27
run(ShortDecimalType_Filter_0_Nulls_0_next_50k_            15.99ms     62.55
run(ShortDecimalType_Filter_0_Nulls_0_next_100k            16.91ms     59.15
run(ShortDecimalType_Filter_0_Nulls_0_next_100k            14.22ms     70.32
----------------------------------------------------------------------------
run(ShortDecimalType_Filter_0_Nulls_20_next_5k_            16.31ms     61.32
run(ShortDecimalType_Filter_0_Nulls_20_next_5k_            15.28ms     65.43
run(ShortDecimalType_Filter_0_Nulls_20_next_10k            17.38ms     57.55
run(ShortDecimalType_Filter_0_Nulls_20_next_10k            14.76ms     67.77
run(ShortDecimalType_Filter_0_Nulls_20_next_20k            17.48ms     57.19
run(ShortDecimalType_Filter_0_Nulls_20_next_20k            14.15ms     70.66
run(ShortDecimalType_Filter_0_Nulls_20_next_50k            17.91ms     55.83
run(ShortDecimalType_Filter_0_Nulls_20_next_50k            13.08ms     76.44
run(ShortDecimalType_Filter_0_Nulls_20_next_100            18.50ms     54.06
run(ShortDecimalType_Filter_0_Nulls_20_next_100            14.72ms     67.92
----------------------------------------------------------------------------
run(ShortDecimalType_Filter_0_Nulls_50_next_5k_            12.73ms     78.55
run(ShortDecimalType_Filter_0_Nulls_50_next_5k_             8.62ms    116.06
run(ShortDecimalType_Filter_0_Nulls_50_next_10k            12.00ms     83.34
run(ShortDecimalType_Filter_0_Nulls_50_next_10k            10.18ms     98.28
run(ShortDecimalType_Filter_0_Nulls_50_next_20k            14.24ms     70.21
run(ShortDecimalType_Filter_0_Nulls_50_next_20k             8.27ms    120.96
run(ShortDecimalType_Filter_0_Nulls_50_next_50k            12.58ms     79.52
run(ShortDecimalType_Filter_0_Nulls_50_next_50k             8.38ms    119.27
run(ShortDecimalType_Filter_0_Nulls_50_next_100            12.00ms     83.37
run(ShortDecimalType_Filter_0_Nulls_50_next_100             9.80ms    102.01
----------------------------------------------------------------------------
run(ShortDecimalType_Filter_0_Nulls_70_next_5k_            14.35ms     69.70
run(ShortDecimalType_Filter_0_Nulls_70_next_5k_             9.54ms    104.78
run(ShortDecimalType_Filter_0_Nulls_70_next_10k            13.49ms     74.14
run(ShortDecimalType_Filter_0_Nulls_70_next_10k             9.21ms    108.53
run(ShortDecimalType_Filter_0_Nulls_70_next_20k            13.36ms     74.83
run(ShortDecimalType_Filter_0_Nulls_70_next_20k             9.68ms    103.31
run(ShortDecimalType_Filter_0_Nulls_70_next_50k            13.66ms     73.23
run(ShortDecimalType_Filter_0_Nulls_70_next_50k             9.52ms    105.06
run(ShortDecimalType_Filter_0_Nulls_70_next_100            12.67ms     78.91
run(ShortDecimalType_Filter_0_Nulls_70_next_100             9.32ms    107.24
----------------------------------------------------------------------------
run(ShortDecimalType_Filter_0_Nulls_100_next_5k           573.95us     1.74K
run(ShortDecimalType_Filter_0_Nulls_100_next_5k           794.51us     1.26K
run(ShortDecimalType_Filter_0_Nulls_100_next_10           827.35us     1.21K
run(ShortDecimalType_Filter_0_Nulls_100_next_10           987.43us     1.01K
run(ShortDecimalType_Filter_0_Nulls_100_next_20           711.98us     1.40K
run(ShortDecimalType_Filter_0_Nulls_100_next_20           623.45us     1.60K
run(ShortDecimalType_Filter_0_Nulls_100_next_50           797.94us     1.25K
run(ShortDecimalType_Filter_0_Nulls_100_next_50           722.29us     1.38K
run(ShortDecimalType_Filter_0_Nulls_100_next_10           669.57us     1.49K
run(ShortDecimalType_Filter_0_Nulls_100_next_10             1.05ms    955.26
----------------------------------------------------------------------------
run(ShortDecimalType_Filter_20_Nulls_0_next_5k_            39.65ms     25.22
run(ShortDecimalType_Filter_20_Nulls_0_next_5k_            29.28ms     34.15
run(ShortDecimalType_Filter_20_Nulls_0_next_10k            39.80ms     25.12
run(ShortDecimalType_Filter_20_Nulls_0_next_10k            29.28ms     34.15
run(ShortDecimalType_Filter_20_Nulls_0_next_20k            35.91ms     27.85
run(ShortDecimalType_Filter_20_Nulls_0_next_20k            31.80ms     31.45
run(ShortDecimalType_Filter_20_Nulls_0_next_50k            41.78ms     23.93
run(ShortDecimalType_Filter_20_Nulls_0_next_50k            47.05ms     21.25
run(ShortDecimalType_Filter_20_Nulls_0_next_100            37.11ms     26.95
run(ShortDecimalType_Filter_20_Nulls_0_next_100            28.33ms     35.29
----------------------------------------------------------------------------
run(ShortDecimalType_Filter_20_Nulls_20_next_5k            34.53ms     28.96
run(ShortDecimalType_Filter_20_Nulls_20_next_5k            26.46ms     37.79
run(ShortDecimalType_Filter_20_Nulls_20_next_10            33.29ms     30.04
run(ShortDecimalType_Filter_20_Nulls_20_next_10            26.29ms     38.03
run(ShortDecimalType_Filter_20_Nulls_20_next_20            34.22ms     29.22
run(ShortDecimalType_Filter_20_Nulls_20_next_20            26.23ms     38.13
run(ShortDecimalType_Filter_20_Nulls_20_next_50            32.77ms     30.52
run(ShortDecimalType_Filter_20_Nulls_20_next_50            24.74ms     40.41
run(ShortDecimalType_Filter_20_Nulls_20_next_10            34.24ms     29.20
run(ShortDecimalType_Filter_20_Nulls_20_next_10            26.11ms     38.30
----------------------------------------------------------------------------
run(ShortDecimalType_Filter_20_Nulls_50_next_5k            22.69ms     44.08
run(ShortDecimalType_Filter_20_Nulls_50_next_5k            15.64ms     63.95
run(ShortDecimalType_Filter_20_Nulls_50_next_10            22.52ms     44.41
run(ShortDecimalType_Filter_20_Nulls_50_next_10            14.97ms     66.82
run(ShortDecimalType_Filter_20_Nulls_50_next_20            22.16ms     45.12
run(ShortDecimalType_Filter_20_Nulls_50_next_20            15.71ms     63.67
run(ShortDecimalType_Filter_20_Nulls_50_next_50            21.76ms     45.96
run(ShortDecimalType_Filter_20_Nulls_50_next_50            14.78ms     67.65
run(ShortDecimalType_Filter_20_Nulls_50_next_10            21.21ms     47.14
run(ShortDecimalType_Filter_20_Nulls_50_next_10            15.28ms     65.44
----------------------------------------------------------------------------
run(ShortDecimalType_Filter_20_Nulls_70_next_5k            18.94ms     52.80
run(ShortDecimalType_Filter_20_Nulls_70_next_5k            13.72ms     72.89
run(ShortDecimalType_Filter_20_Nulls_70_next_10            23.16ms     43.19
run(ShortDecimalType_Filter_20_Nulls_70_next_10            13.37ms     74.82
run(ShortDecimalType_Filter_20_Nulls_70_next_20            17.90ms     55.87
run(ShortDecimalType_Filter_20_Nulls_70_next_20            13.12ms     76.21
run(ShortDecimalType_Filter_20_Nulls_70_next_50            18.30ms     54.65
run(ShortDecimalType_Filter_20_Nulls_70_next_50            13.14ms     76.11
run(ShortDecimalType_Filter_20_Nulls_70_next_10            18.80ms     53.19
run(ShortDecimalType_Filter_20_Nulls_70_next_10            12.75ms     78.46
----------------------------------------------------------------------------
run(ShortDecimalType_Filter_20_Nulls_100_next_5           603.54us     1.66K
run(ShortDecimalType_Filter_20_Nulls_100_next_5           621.35us     1.61K
run(ShortDecimalType_Filter_20_Nulls_100_next_1           626.45us     1.60K
run(ShortDecimalType_Filter_20_Nulls_100_next_1           583.92us     1.71K
run(ShortDecimalType_Filter_20_Nulls_100_next_2           586.87us     1.70K
run(ShortDecimalType_Filter_20_Nulls_100_next_2           614.55us     1.63K
run(ShortDecimalType_Filter_20_Nulls_100_next_5           611.04us     1.64K
run(ShortDecimalType_Filter_20_Nulls_100_next_5           565.80us     1.77K
run(ShortDecimalType_Filter_20_Nulls_100_next_1           573.20us     1.74K
run(ShortDecimalType_Filter_20_Nulls_100_next_1           605.31us     1.65K
----------------------------------------------------------------------------
run(ShortDecimalType_Filter_50_Nulls_0_next_5k_            39.04ms     25.61
run(ShortDecimalType_Filter_50_Nulls_0_next_5k_            28.54ms     35.04
run(ShortDecimalType_Filter_50_Nulls_0_next_10k            46.40ms     21.55
run(ShortDecimalType_Filter_50_Nulls_0_next_10k            27.06ms     36.96
run(ShortDecimalType_Filter_50_Nulls_0_next_20k            37.07ms     26.97
run(ShortDecimalType_Filter_50_Nulls_0_next_20k            28.90ms     34.60
run(ShortDecimalType_Filter_50_Nulls_0_next_50k            47.22ms     21.18
run(ShortDecimalType_Filter_50_Nulls_0_next_50k            29.26ms     34.18
run(ShortDecimalType_Filter_50_Nulls_0_next_100            40.18ms     24.89
run(ShortDecimalType_Filter_50_Nulls_0_next_100            29.13ms     34.33
----------------------------------------------------------------------------
run(ShortDecimalType_Filter_50_Nulls_20_next_5k            40.29ms     24.82
run(ShortDecimalType_Filter_50_Nulls_20_next_5k            27.31ms     36.61
run(ShortDecimalType_Filter_50_Nulls_20_next_10            35.42ms     28.24
run(ShortDecimalType_Filter_50_Nulls_20_next_10            28.13ms     35.54
run(ShortDecimalType_Filter_50_Nulls_20_next_20            38.90ms     25.71
run(ShortDecimalType_Filter_50_Nulls_20_next_20            28.59ms     34.98
run(ShortDecimalType_Filter_50_Nulls_20_next_50            38.84ms     25.75
run(ShortDecimalType_Filter_50_Nulls_20_next_50            29.50ms     33.89
run(ShortDecimalType_Filter_50_Nulls_20_next_10            37.49ms     26.67
run(ShortDecimalType_Filter_50_Nulls_20_next_10            27.24ms     36.71
----------------------------------------------------------------------------
run(ShortDecimalType_Filter_50_Nulls_50_next_5k            27.91ms     35.83
run(ShortDecimalType_Filter_50_Nulls_50_next_5k            19.78ms     50.56
run(ShortDecimalType_Filter_50_Nulls_50_next_10            28.16ms     35.51
run(ShortDecimalType_Filter_50_Nulls_50_next_10            17.31ms     57.78
run(ShortDecimalType_Filter_50_Nulls_50_next_20            42.08ms     23.77
run(ShortDecimalType_Filter_50_Nulls_50_next_20            24.54ms     40.74
run(ShortDecimalType_Filter_50_Nulls_50_next_50            38.75ms     25.80
run(ShortDecimalType_Filter_50_Nulls_50_next_50            16.97ms     58.92
run(ShortDecimalType_Filter_50_Nulls_50_next_10            25.09ms     39.85
run(ShortDecimalType_Filter_50_Nulls_50_next_10            17.14ms     58.36
----------------------------------------------------------------------------
run(ShortDecimalType_Filter_50_Nulls_70_next_5k            20.86ms     47.94
run(ShortDecimalType_Filter_50_Nulls_70_next_5k            14.46ms     69.17
run(ShortDecimalType_Filter_50_Nulls_70_next_10            21.57ms     46.36
run(ShortDecimalType_Filter_50_Nulls_70_next_10            13.85ms     72.20
run(ShortDecimalType_Filter_50_Nulls_70_next_20            21.30ms     46.94
run(ShortDecimalType_Filter_50_Nulls_70_next_20            12.98ms     77.06
run(ShortDecimalType_Filter_50_Nulls_70_next_50            21.15ms     47.29
run(ShortDecimalType_Filter_50_Nulls_70_next_50            12.86ms     77.74
run(ShortDecimalType_Filter_50_Nulls_70_next_10            21.88ms     45.70
run(ShortDecimalType_Filter_50_Nulls_70_next_10            12.71ms     78.66
----------------------------------------------------------------------------
run(ShortDecimalType_Filter_50_Nulls_100_next_5           614.68us     1.63K
run(ShortDecimalType_Filter_50_Nulls_100_next_5           639.33us     1.56K
run(ShortDecimalType_Filter_50_Nulls_100_next_1           623.23us     1.60K
run(ShortDecimalType_Filter_50_Nulls_100_next_1           651.28us     1.54K
run(ShortDecimalType_Filter_50_Nulls_100_next_2           646.00us     1.55K
run(ShortDecimalType_Filter_50_Nulls_100_next_2           624.55us     1.60K
run(ShortDecimalType_Filter_50_Nulls_100_next_5           625.01us     1.60K
run(ShortDecimalType_Filter_50_Nulls_100_next_5           627.96us     1.59K
run(ShortDecimalType_Filter_50_Nulls_100_next_1           600.03us     1.67K
run(ShortDecimalType_Filter_50_Nulls_100_next_1           596.54us     1.68K
----------------------------------------------------------------------------
run(ShortDecimalType_Filter_70_Nulls_0_next_5k_            40.92ms     24.44
run(ShortDecimalType_Filter_70_Nulls_0_next_5k_            31.12ms     32.14
run(ShortDecimalType_Filter_70_Nulls_0_next_10k            39.10ms     25.58
run(ShortDecimalType_Filter_70_Nulls_0_next_10k            41.03ms     24.37
run(ShortDecimalType_Filter_70_Nulls_0_next_20k            41.96ms     23.83
run(ShortDecimalType_Filter_70_Nulls_0_next_20k            33.50ms     29.85
run(ShortDecimalType_Filter_70_Nulls_0_next_50k            44.77ms     22.34
run(ShortDecimalType_Filter_70_Nulls_0_next_50k            33.55ms     29.81
run(ShortDecimalType_Filter_70_Nulls_0_next_100            45.97ms     21.76
run(ShortDecimalType_Filter_70_Nulls_0_next_100            35.93ms     27.83
----------------------------------------------------------------------------
run(ShortDecimalType_Filter_70_Nulls_20_next_5k            39.39ms     25.39
run(ShortDecimalType_Filter_70_Nulls_20_next_5k            31.30ms     31.94
run(ShortDecimalType_Filter_70_Nulls_20_next_10            41.99ms     23.82
run(ShortDecimalType_Filter_70_Nulls_20_next_10            28.51ms     35.08
run(ShortDecimalType_Filter_70_Nulls_20_next_20            38.52ms     25.96
run(ShortDecimalType_Filter_70_Nulls_20_next_20            29.03ms     34.45
run(ShortDecimalType_Filter_70_Nulls_20_next_50            36.55ms     27.36
run(ShortDecimalType_Filter_70_Nulls_20_next_50            33.36ms     29.98
run(ShortDecimalType_Filter_70_Nulls_20_next_10            37.82ms     26.44
run(ShortDecimalType_Filter_70_Nulls_20_next_10            29.84ms     33.52
----------------------------------------------------------------------------
run(ShortDecimalType_Filter_70_Nulls_50_next_5k            27.03ms     37.00
run(ShortDecimalType_Filter_70_Nulls_50_next_5k            18.59ms     53.78
run(ShortDecimalType_Filter_70_Nulls_50_next_10            26.15ms     38.24
run(ShortDecimalType_Filter_70_Nulls_50_next_10            17.44ms     57.35
run(ShortDecimalType_Filter_70_Nulls_50_next_20            28.15ms     35.52
run(ShortDecimalType_Filter_70_Nulls_50_next_20            18.21ms     54.90
run(ShortDecimalType_Filter_70_Nulls_50_next_50            32.06ms     31.19
run(ShortDecimalType_Filter_70_Nulls_50_next_50            20.95ms     47.74
run(ShortDecimalType_Filter_70_Nulls_50_next_10            29.12ms     34.34
run(ShortDecimalType_Filter_70_Nulls_50_next_10            18.59ms     53.78
----------------------------------------------------------------------------
run(ShortDecimalType_Filter_70_Nulls_70_next_5k            22.59ms     44.27
run(ShortDecimalType_Filter_70_Nulls_70_next_5k            15.04ms     66.50
run(ShortDecimalType_Filter_70_Nulls_70_next_10            21.59ms     46.33
run(ShortDecimalType_Filter_70_Nulls_70_next_10            14.80ms     67.57
run(ShortDecimalType_Filter_70_Nulls_70_next_20            20.41ms     49.00
run(ShortDecimalType_Filter_70_Nulls_70_next_20            12.84ms     77.90
run(ShortDecimalType_Filter_70_Nulls_70_next_50            21.03ms     47.56
run(ShortDecimalType_Filter_70_Nulls_70_next_50            13.87ms     72.08
run(ShortDecimalType_Filter_70_Nulls_70_next_10            21.64ms     46.21
run(ShortDecimalType_Filter_70_Nulls_70_next_10            13.27ms     75.38
----------------------------------------------------------------------------
run(ShortDecimalType_Filter_70_Nulls_100_next_5           650.21us     1.54K
run(ShortDecimalType_Filter_70_Nulls_100_next_5           622.35us     1.61K
run(ShortDecimalType_Filter_70_Nulls_100_next_1           624.70us     1.60K
run(ShortDecimalType_Filter_70_Nulls_100_next_1           638.53us     1.57K
run(ShortDecimalType_Filter_70_Nulls_100_next_2           607.00us     1.65K
run(ShortDecimalType_Filter_70_Nulls_100_next_2           558.10us     1.79K
run(ShortDecimalType_Filter_70_Nulls_100_next_5           623.82us     1.60K
run(ShortDecimalType_Filter_70_Nulls_100_next_5           598.61us     1.67K
run(ShortDecimalType_Filter_70_Nulls_100_next_1           589.09us     1.70K
run(ShortDecimalType_Filter_70_Nulls_100_next_1           671.15us     1.49K
----------------------------------------------------------------------------
run(ShortDecimalType_Filter_100_Nulls_0_next_5k            35.37ms     28.27
run(ShortDecimalType_Filter_100_Nulls_0_next_5k            32.02ms     31.23
run(ShortDecimalType_Filter_100_Nulls_0_next_10            38.95ms     25.68
run(ShortDecimalType_Filter_100_Nulls_0_next_10            28.91ms     34.59
run(ShortDecimalType_Filter_100_Nulls_0_next_20            39.62ms     25.24
run(ShortDecimalType_Filter_100_Nulls_0_next_20            31.46ms     31.79
run(ShortDecimalType_Filter_100_Nulls_0_next_50            39.79ms     25.13
run(ShortDecimalType_Filter_100_Nulls_0_next_50            32.13ms     31.12
run(ShortDecimalType_Filter_100_Nulls_0_next_10            41.02ms     24.38
run(ShortDecimalType_Filter_100_Nulls_0_next_10            32.80ms     30.49
----------------------------------------------------------------------------
run(ShortDecimalType_Filter_100_Nulls_20_next_5            35.12ms     28.48
run(ShortDecimalType_Filter_100_Nulls_20_next_5            29.50ms     33.89
run(ShortDecimalType_Filter_100_Nulls_20_next_1            34.08ms     29.34
run(ShortDecimalType_Filter_100_Nulls_20_next_1            27.51ms     36.35
run(ShortDecimalType_Filter_100_Nulls_20_next_2            48.46ms     20.64
run(ShortDecimalType_Filter_100_Nulls_20_next_2            28.95ms     34.54
run(ShortDecimalType_Filter_100_Nulls_20_next_5            36.25ms     27.58
run(ShortDecimalType_Filter_100_Nulls_20_next_5            29.11ms     34.36
run(ShortDecimalType_Filter_100_Nulls_20_next_1            35.82ms     27.91
run(ShortDecimalType_Filter_100_Nulls_20_next_1            30.05ms     33.28
----------------------------------------------------------------------------
run(ShortDecimalType_Filter_100_Nulls_50_next_5            23.45ms     42.65
run(ShortDecimalType_Filter_100_Nulls_50_next_5            15.89ms     62.93
run(ShortDecimalType_Filter_100_Nulls_50_next_1            23.38ms     42.77
run(ShortDecimalType_Filter_100_Nulls_50_next_1            16.73ms     59.76
run(ShortDecimalType_Filter_100_Nulls_50_next_2            22.33ms     44.79
run(ShortDecimalType_Filter_100_Nulls_50_next_2            16.95ms     59.00
run(ShortDecimalType_Filter_100_Nulls_50_next_5            23.87ms     41.89
run(ShortDecimalType_Filter_100_Nulls_50_next_5            16.75ms     59.72
run(ShortDecimalType_Filter_100_Nulls_50_next_1            23.06ms     43.36
run(ShortDecimalType_Filter_100_Nulls_50_next_1            24.77ms     40.38
----------------------------------------------------------------------------
run(ShortDecimalType_Filter_100_Nulls_70_next_5            22.53ms     44.38
run(ShortDecimalType_Filter_100_Nulls_70_next_5            13.93ms     71.80
run(ShortDecimalType_Filter_100_Nulls_70_next_1            20.18ms     49.54
run(ShortDecimalType_Filter_100_Nulls_70_next_1            13.76ms     72.66
run(ShortDecimalType_Filter_100_Nulls_70_next_2            19.31ms     51.80
run(ShortDecimalType_Filter_100_Nulls_70_next_2            12.76ms     78.37
run(ShortDecimalType_Filter_100_Nulls_70_next_5            18.97ms     52.72
run(ShortDecimalType_Filter_100_Nulls_70_next_5            12.93ms     77.32
run(ShortDecimalType_Filter_100_Nulls_70_next_1            19.90ms     50.25
run(ShortDecimalType_Filter_100_Nulls_70_next_1            14.15ms     70.68
----------------------------------------------------------------------------
run(ShortDecimalType_Filter_100_Nulls_100_next_           612.67us     1.63K
run(ShortDecimalType_Filter_100_Nulls_100_next_           721.03us     1.39K
run(ShortDecimalType_Filter_100_Nulls_100_next_           785.26us     1.27K
run(ShortDecimalType_Filter_100_Nulls_100_next_           722.81us     1.38K
run(ShortDecimalType_Filter_100_Nulls_100_next_           627.85us     1.59K
run(ShortDecimalType_Filter_100_Nulls_100_next_           902.08us     1.11K
run(ShortDecimalType_Filter_100_Nulls_100_next_           611.49us     1.64K
run(ShortDecimalType_Filter_100_Nulls_100_next_           641.36us     1.56K
run(ShortDecimalType_Filter_100_Nulls_100_next_           650.79us     1.54K
run(ShortDecimalType_Filter_100_Nulls_100_next_           637.31us     1.57K
----------------------------------------------------------------------------
----------------------------------------------------------------------------
run(LongDecimalType_Filter_0_Nulls_0_next_5k_di            76.66ms     13.04
run(LongDecimalType_Filter_0_Nulls_0_next_5k_pl            64.22ms     15.57
run(LongDecimalType_Filter_0_Nulls_0_next_10k_d            68.92ms     14.51
run(LongDecimalType_Filter_0_Nulls_0_next_10k_P            66.74ms     14.98
run(LongDecimalType_Filter_0_Nulls_0_next_20k_d            68.02ms     14.70
run(LongDecimalType_Filter_0_Nulls_0_next_20k_p            63.05ms     15.86
run(LongDecimalType_Filter_0_Nulls_0_next_50k_d            68.68ms     14.56
run(LongDecimalType_Filter_0_Nulls_0_next_50k_p            69.72ms     14.34
run(LongDecimalType_Filter_0_Nulls_0_next_100k_            68.21ms     14.66
run(LongDecimalType_Filter_0_Nulls_0_next_100k_            66.13ms     15.12
----------------------------------------------------------------------------
run(LongDecimalType_Filter_0_Nulls_20_next_5k_d            61.61ms     16.23
run(LongDecimalType_Filter_0_Nulls_20_next_5k_p            65.93ms     15.17
run(LongDecimalType_Filter_0_Nulls_20_next_10k_            66.43ms     15.05
run(LongDecimalType_Filter_0_Nulls_20_next_10k_            67.66ms     14.78
run(LongDecimalType_Filter_0_Nulls_20_next_20k_            64.45ms     15.52
run(LongDecimalType_Filter_0_Nulls_20_next_20k_            64.60ms     15.48
run(LongDecimalType_Filter_0_Nulls_20_next_50k_            65.84ms     15.19
run(LongDecimalType_Filter_0_Nulls_20_next_50k_            72.76ms     13.74
run(LongDecimalType_Filter_0_Nulls_20_next_100k           186.94ms      5.35
run(LongDecimalType_Filter_0_Nulls_20_next_100k            68.10ms     14.68
----------------------------------------------------------------------------
run(LongDecimalType_Filter_0_Nulls_50_next_5k_d            45.83ms     21.82
run(LongDecimalType_Filter_0_Nulls_50_next_5k_p            44.24ms     22.60
run(LongDecimalType_Filter_0_Nulls_50_next_10k_            44.87ms     22.29
run(LongDecimalType_Filter_0_Nulls_50_next_10k_            47.78ms     20.93
run(LongDecimalType_Filter_0_Nulls_50_next_20k_            43.88ms     22.79
run(LongDecimalType_Filter_0_Nulls_50_next_20k_            42.90ms     23.31
run(LongDecimalType_Filter_0_Nulls_50_next_50k_            43.90ms     22.78
run(LongDecimalType_Filter_0_Nulls_50_next_50k_            46.58ms     21.47
run(LongDecimalType_Filter_0_Nulls_50_next_100k            61.02ms     16.39
run(LongDecimalType_Filter_0_Nulls_50_next_100k            47.68ms     20.97
----------------------------------------------------------------------------
run(LongDecimalType_Filter_0_Nulls_70_next_5k_d            39.04ms     25.61
run(LongDecimalType_Filter_0_Nulls_70_next_5k_p            38.10ms     26.25
run(LongDecimalType_Filter_0_Nulls_70_next_10k_            43.64ms     22.92
run(LongDecimalType_Filter_0_Nulls_70_next_10k_            39.14ms     25.55
run(LongDecimalType_Filter_0_Nulls_70_next_20k_            29.53ms     33.86
run(LongDecimalType_Filter_0_Nulls_70_next_20k_            27.58ms     36.25
run(LongDecimalType_Filter_0_Nulls_70_next_50k_            29.06ms     34.41
run(LongDecimalType_Filter_0_Nulls_70_next_50k_            28.59ms     34.98
run(LongDecimalType_Filter_0_Nulls_70_next_100k            26.91ms     37.16
run(LongDecimalType_Filter_0_Nulls_70_next_100k            27.63ms     36.19
----------------------------------------------------------------------------
run(LongDecimalType_Filter_0_Nulls_100_next_5k_            24.83ms     40.28
run(LongDecimalType_Filter_0_Nulls_100_next_5k_            24.19ms     41.34
run(LongDecimalType_Filter_0_Nulls_100_next_10k            24.91ms     40.14
run(LongDecimalType_Filter_0_Nulls_100_next_10k            25.10ms     39.85
run(LongDecimalType_Filter_0_Nulls_100_next_20k            24.62ms     40.62
run(LongDecimalType_Filter_0_Nulls_100_next_20k            23.36ms     42.81
run(LongDecimalType_Filter_0_Nulls_100_next_50k            23.87ms     41.89
run(LongDecimalType_Filter_0_Nulls_100_next_50k            24.23ms     41.28
run(LongDecimalType_Filter_0_Nulls_100_next_100            24.71ms     40.47
run(LongDecimalType_Filter_0_Nulls_100_next_100            24.04ms     41.60
----------------------------------------------------------------------------
run(LongDecimalType_Filter_20_Nulls_0_next_5k_d            77.02ms     12.98
run(LongDecimalType_Filter_20_Nulls_0_next_5k_p            79.29ms     12.61
run(LongDecimalType_Filter_20_Nulls_0_next_10k_            77.49ms     12.91
run(LongDecimalType_Filter_20_Nulls_0_next_10k_            76.19ms     13.12
run(LongDecimalType_Filter_20_Nulls_0_next_20k_            84.83ms     11.79
run(LongDecimalType_Filter_20_Nulls_0_next_20k_            75.95ms     13.17
run(LongDecimalType_Filter_20_Nulls_0_next_50k_            76.49ms     13.07
run(LongDecimalType_Filter_20_Nulls_0_next_50k_            83.88ms     11.92
run(LongDecimalType_Filter_20_Nulls_0_next_100k            81.96ms     12.20
run(LongDecimalType_Filter_20_Nulls_0_next_100k            86.59ms     11.55
----------------------------------------------------------------------------
run(LongDecimalType_Filter_20_Nulls_20_next_5k_            78.46ms     12.75
run(LongDecimalType_Filter_20_Nulls_20_next_5k_            71.30ms     14.03
run(LongDecimalType_Filter_20_Nulls_20_next_10k            77.22ms     12.95
run(LongDecimalType_Filter_20_Nulls_20_next_10k            74.54ms     13.42
run(LongDecimalType_Filter_20_Nulls_20_next_20k            73.62ms     13.58
run(LongDecimalType_Filter_20_Nulls_20_next_20k            77.65ms     12.88
run(LongDecimalType_Filter_20_Nulls_20_next_50k            79.87ms     12.52
run(LongDecimalType_Filter_20_Nulls_20_next_50k            83.14ms     12.03
run(LongDecimalType_Filter_20_Nulls_20_next_100            78.04ms     12.81
run(LongDecimalType_Filter_20_Nulls_20_next_100            79.42ms     12.59
----------------------------------------------------------------------------
run(LongDecimalType_Filter_20_Nulls_50_next_5k_            53.61ms     18.65
run(LongDecimalType_Filter_20_Nulls_50_next_5k_            49.40ms     20.24
run(LongDecimalType_Filter_20_Nulls_50_next_10k            49.18ms     20.33
run(LongDecimalType_Filter_20_Nulls_50_next_10k            51.49ms     19.42
run(LongDecimalType_Filter_20_Nulls_50_next_20k            52.53ms     19.04
run(LongDecimalType_Filter_20_Nulls_50_next_20k            51.83ms     19.29
run(LongDecimalType_Filter_20_Nulls_50_next_50k            49.50ms     20.20
run(LongDecimalType_Filter_20_Nulls_50_next_50k            48.93ms     20.44
run(LongDecimalType_Filter_20_Nulls_50_next_100            51.05ms     19.59
run(LongDecimalType_Filter_20_Nulls_50_next_100            48.74ms     20.52
----------------------------------------------------------------------------
run(LongDecimalType_Filter_20_Nulls_70_next_5k_            32.74ms     30.54
run(LongDecimalType_Filter_20_Nulls_70_next_5k_            32.05ms     31.21
run(LongDecimalType_Filter_20_Nulls_70_next_10k            30.18ms     33.14
run(LongDecimalType_Filter_20_Nulls_70_next_10k            30.12ms     33.20
run(LongDecimalType_Filter_20_Nulls_70_next_20k            31.63ms     31.62
run(LongDecimalType_Filter_20_Nulls_70_next_20k            29.40ms     34.02
run(LongDecimalType_Filter_20_Nulls_70_next_50k            30.70ms     32.58
run(LongDecimalType_Filter_20_Nulls_70_next_50k            32.77ms     30.52
run(LongDecimalType_Filter_20_Nulls_70_next_100            32.34ms     30.92
run(LongDecimalType_Filter_20_Nulls_70_next_100            33.69ms     29.68
----------------------------------------------------------------------------
run(LongDecimalType_Filter_20_Nulls_100_next_5k            26.02ms     38.43
run(LongDecimalType_Filter_20_Nulls_100_next_5k            24.45ms     40.91
run(LongDecimalType_Filter_20_Nulls_100_next_10            24.91ms     40.15
run(LongDecimalType_Filter_20_Nulls_100_next_10            24.55ms     40.73
run(LongDecimalType_Filter_20_Nulls_100_next_20            24.98ms     40.03
run(LongDecimalType_Filter_20_Nulls_100_next_20            24.53ms     40.77
run(LongDecimalType_Filter_20_Nulls_100_next_50            25.79ms     38.78
run(LongDecimalType_Filter_20_Nulls_100_next_50            23.92ms     41.81
run(LongDecimalType_Filter_20_Nulls_100_next_10            25.83ms     38.71
run(LongDecimalType_Filter_20_Nulls_100_next_10            29.98ms     33.35
----------------------------------------------------------------------------
run(LongDecimalType_Filter_50_Nulls_0_next_5k_d            90.26ms     11.08
run(LongDecimalType_Filter_50_Nulls_0_next_5k_p           104.37ms      9.58
run(LongDecimalType_Filter_50_Nulls_0_next_10k_            95.15ms     10.51
run(LongDecimalType_Filter_50_Nulls_0_next_10k_            93.95ms     10.64
run(LongDecimalType_Filter_50_Nulls_0_next_20k_            93.04ms     10.75
run(LongDecimalType_Filter_50_Nulls_0_next_20k_            97.55ms     10.25
run(LongDecimalType_Filter_50_Nulls_0_next_50k_            95.10ms     10.51
run(LongDecimalType_Filter_50_Nulls_0_next_50k_            96.42ms     10.37
run(LongDecimalType_Filter_50_Nulls_0_next_100k            96.43ms     10.37
run(LongDecimalType_Filter_50_Nulls_0_next_100k            94.00ms     10.64
----------------------------------------------------------------------------
run(LongDecimalType_Filter_50_Nulls_20_next_5k_            89.87ms     11.13
run(LongDecimalType_Filter_50_Nulls_20_next_5k_            91.85ms     10.89
run(LongDecimalType_Filter_50_Nulls_20_next_10k            90.28ms     11.08
run(LongDecimalType_Filter_50_Nulls_20_next_10k            88.73ms     11.27
run(LongDecimalType_Filter_50_Nulls_20_next_20k            84.99ms     11.77
run(LongDecimalType_Filter_50_Nulls_20_next_20k           102.88ms      9.72
run(LongDecimalType_Filter_50_Nulls_20_next_50k            85.07ms     11.76
run(LongDecimalType_Filter_50_Nulls_20_next_50k            88.89ms     11.25
run(LongDecimalType_Filter_50_Nulls_20_next_100            91.98ms     10.87
run(LongDecimalType_Filter_50_Nulls_20_next_100            93.01ms     10.75
----------------------------------------------------------------------------
run(LongDecimalType_Filter_50_Nulls_50_next_5k_            61.98ms     16.13
run(LongDecimalType_Filter_50_Nulls_50_next_5k_            57.08ms     17.52
run(LongDecimalType_Filter_50_Nulls_50_next_10k            57.35ms     17.44
run(LongDecimalType_Filter_50_Nulls_50_next_10k            55.49ms     18.02
run(LongDecimalType_Filter_50_Nulls_50_next_20k            62.28ms     16.06
run(LongDecimalType_Filter_50_Nulls_50_next_20k            59.12ms     16.91
run(LongDecimalType_Filter_50_Nulls_50_next_50k            58.16ms     17.19
run(LongDecimalType_Filter_50_Nulls_50_next_50k            58.34ms     17.14
run(LongDecimalType_Filter_50_Nulls_50_next_100            59.68ms     16.75
run(LongDecimalType_Filter_50_Nulls_50_next_100            60.51ms     16.53
----------------------------------------------------------------------------
run(LongDecimalType_Filter_50_Nulls_70_next_5k_            37.08ms     26.97
run(LongDecimalType_Filter_50_Nulls_70_next_5k_            35.88ms     27.87
run(LongDecimalType_Filter_50_Nulls_70_next_10k            34.95ms     28.61
run(LongDecimalType_Filter_50_Nulls_70_next_10k            35.41ms     28.24
run(LongDecimalType_Filter_50_Nulls_70_next_20k            36.05ms     27.74
run(LongDecimalType_Filter_50_Nulls_70_next_20k            34.76ms     28.77
run(LongDecimalType_Filter_50_Nulls_70_next_50k            38.94ms     25.68
run(LongDecimalType_Filter_50_Nulls_70_next_50k            38.62ms     25.89
run(LongDecimalType_Filter_50_Nulls_70_next_100            37.89ms     26.39
run(LongDecimalType_Filter_50_Nulls_70_next_100            38.19ms     26.18
----------------------------------------------------------------------------
run(LongDecimalType_Filter_50_Nulls_100_next_5k            25.32ms     39.50
run(LongDecimalType_Filter_50_Nulls_100_next_5k            25.81ms     38.74
run(LongDecimalType_Filter_50_Nulls_100_next_10            25.05ms     39.93
run(LongDecimalType_Filter_50_Nulls_100_next_10            25.40ms     39.37
run(LongDecimalType_Filter_50_Nulls_100_next_20            25.66ms     38.98
run(LongDecimalType_Filter_50_Nulls_100_next_20            25.05ms     39.93
run(LongDecimalType_Filter_50_Nulls_100_next_50            24.28ms     41.19
run(LongDecimalType_Filter_50_Nulls_100_next_50            25.32ms     39.49
run(LongDecimalType_Filter_50_Nulls_100_next_10            24.06ms     41.56
run(LongDecimalType_Filter_50_Nulls_100_next_10            25.53ms     39.17
----------------------------------------------------------------------------
run(LongDecimalType_Filter_70_Nulls_0_next_5k_d            93.38ms     10.71
run(LongDecimalType_Filter_70_Nulls_0_next_5k_p            89.19ms     11.21
run(LongDecimalType_Filter_70_Nulls_0_next_10k_            92.77ms     10.78
run(LongDecimalType_Filter_70_Nulls_0_next_10k_            91.37ms     10.94
run(LongDecimalType_Filter_70_Nulls_0_next_20k_            95.88ms     10.43
run(LongDecimalType_Filter_70_Nulls_0_next_20k_            91.04ms     10.98
run(LongDecimalType_Filter_70_Nulls_0_next_50k_            93.91ms     10.65
run(LongDecimalType_Filter_70_Nulls_0_next_50k_            90.86ms     11.01
run(LongDecimalType_Filter_70_Nulls_0_next_100k            96.36ms     10.38
run(LongDecimalType_Filter_70_Nulls_0_next_100k           105.14ms      9.51
----------------------------------------------------------------------------
run(LongDecimalType_Filter_70_Nulls_20_next_5k_            87.67ms     11.41
run(LongDecimalType_Filter_70_Nulls_20_next_5k_            79.95ms     12.51
run(LongDecimalType_Filter_70_Nulls_20_next_10k           118.13ms      8.47
run(LongDecimalType_Filter_70_Nulls_20_next_10k            83.85ms     11.93
run(LongDecimalType_Filter_70_Nulls_20_next_20k           108.83ms      9.19
run(LongDecimalType_Filter_70_Nulls_20_next_20k            84.20ms     11.88
run(LongDecimalType_Filter_70_Nulls_20_next_50k            92.36ms     10.83
run(LongDecimalType_Filter_70_Nulls_20_next_50k            93.84ms     10.66
run(LongDecimalType_Filter_70_Nulls_20_next_100            96.75ms     10.34
run(LongDecimalType_Filter_70_Nulls_20_next_100            89.03ms     11.23
----------------------------------------------------------------------------
run(LongDecimalType_Filter_70_Nulls_50_next_5k_            59.74ms     16.74
run(LongDecimalType_Filter_70_Nulls_50_next_5k_            56.34ms     17.75
run(LongDecimalType_Filter_70_Nulls_50_next_10k            59.64ms     16.77
run(LongDecimalType_Filter_70_Nulls_50_next_10k            58.97ms     16.96
run(LongDecimalType_Filter_70_Nulls_50_next_20k            70.27ms     14.23
run(LongDecimalType_Filter_70_Nulls_50_next_20k            56.86ms     17.59
run(LongDecimalType_Filter_70_Nulls_50_next_50k            56.50ms     17.70
run(LongDecimalType_Filter_70_Nulls_50_next_50k            56.93ms     17.57
run(LongDecimalType_Filter_70_Nulls_50_next_100            79.82ms     12.53
run(LongDecimalType_Filter_70_Nulls_50_next_100            72.13ms     13.86
----------------------------------------------------------------------------
run(LongDecimalType_Filter_70_Nulls_70_next_5k_            39.27ms     25.47
run(LongDecimalType_Filter_70_Nulls_70_next_5k_            36.73ms     27.22
run(LongDecimalType_Filter_70_Nulls_70_next_10k            37.86ms     26.41
run(LongDecimalType_Filter_70_Nulls_70_next_10k            35.13ms     28.46
run(LongDecimalType_Filter_70_Nulls_70_next_20k            34.74ms     28.78
run(LongDecimalType_Filter_70_Nulls_70_next_20k            33.68ms     29.69
run(LongDecimalType_Filter_70_Nulls_70_next_50k            39.28ms     25.46
run(LongDecimalType_Filter_70_Nulls_70_next_50k            32.20ms     31.06
run(LongDecimalType_Filter_70_Nulls_70_next_100            34.63ms     28.87
run(LongDecimalType_Filter_70_Nulls_70_next_100            32.55ms     30.72
----------------------------------------------------------------------------
run(LongDecimalType_Filter_70_Nulls_100_next_5k            23.11ms     43.28
run(LongDecimalType_Filter_70_Nulls_100_next_5k            24.16ms     41.40
run(LongDecimalType_Filter_70_Nulls_100_next_10            23.84ms     41.94
run(LongDecimalType_Filter_70_Nulls_100_next_10            26.33ms     37.98
run(LongDecimalType_Filter_70_Nulls_100_next_20            26.97ms     37.07
run(LongDecimalType_Filter_70_Nulls_100_next_20            27.13ms     36.86
run(LongDecimalType_Filter_70_Nulls_100_next_50            24.30ms     41.16
run(LongDecimalType_Filter_70_Nulls_100_next_50            24.70ms     40.49
run(LongDecimalType_Filter_70_Nulls_100_next_10            24.78ms     40.35
run(LongDecimalType_Filter_70_Nulls_100_next_10            25.50ms     39.21
----------------------------------------------------------------------------
run(LongDecimalType_Filter_100_Nulls_0_next_5k_            85.33ms     11.72
run(LongDecimalType_Filter_100_Nulls_0_next_5k_            85.23ms     11.73
run(LongDecimalType_Filter_100_Nulls_0_next_10k            80.77ms     12.38
run(LongDecimalType_Filter_100_Nulls_0_next_10k            83.18ms     12.02
run(LongDecimalType_Filter_100_Nulls_0_next_20k            83.83ms     11.93
run(LongDecimalType_Filter_100_Nulls_0_next_20k            82.84ms     12.07
run(LongDecimalType_Filter_100_Nulls_0_next_50k           103.60ms      9.65
run(LongDecimalType_Filter_100_Nulls_0_next_50k            82.66ms     12.10
run(LongDecimalType_Filter_100_Nulls_0_next_100           106.65ms      9.38
run(LongDecimalType_Filter_100_Nulls_0_next_100            91.97ms     10.87
----------------------------------------------------------------------------
run(LongDecimalType_Filter_100_Nulls_20_next_5k            84.15ms     11.88
run(LongDecimalType_Filter_100_Nulls_20_next_5k            74.99ms     13.33
run(LongDecimalType_Filter_100_Nulls_20_next_10            76.52ms     13.07
run(LongDecimalType_Filter_100_Nulls_20_next_10            82.84ms     12.07
run(LongDecimalType_Filter_100_Nulls_20_next_20            75.68ms     13.21
run(LongDecimalType_Filter_100_Nulls_20_next_20            78.93ms     12.67
run(LongDecimalType_Filter_100_Nulls_20_next_50            78.41ms     12.75
run(LongDecimalType_Filter_100_Nulls_20_next_50            82.22ms     12.16
run(LongDecimalType_Filter_100_Nulls_20_next_10            98.61ms     10.14
run(LongDecimalType_Filter_100_Nulls_20_next_10            99.57ms     10.04
----------------------------------------------------------------------------
run(LongDecimalType_Filter_100_Nulls_50_next_5k            67.28ms     14.86
run(LongDecimalType_Filter_100_Nulls_50_next_5k            57.17ms     17.49
run(LongDecimalType_Filter_100_Nulls_50_next_10            56.13ms     17.82
run(LongDecimalType_Filter_100_Nulls_50_next_10            48.82ms     20.48
run(LongDecimalType_Filter_100_Nulls_50_next_20            53.31ms     18.76
run(LongDecimalType_Filter_100_Nulls_50_next_20            52.70ms     18.98
run(LongDecimalType_Filter_100_Nulls_50_next_50            55.78ms     17.93
run(LongDecimalType_Filter_100_Nulls_50_next_50            51.69ms     19.35
run(LongDecimalType_Filter_100_Nulls_50_next_10            58.04ms     17.23
run(LongDecimalType_Filter_100_Nulls_50_next_10            53.51ms     18.69
----------------------------------------------------------------------------
run(LongDecimalType_Filter_100_Nulls_70_next_5k            34.48ms     29.00
run(LongDecimalType_Filter_100_Nulls_70_next_5k            32.31ms     30.95
run(LongDecimalType_Filter_100_Nulls_70_next_10            32.52ms     30.75
run(LongDecimalType_Filter_100_Nulls_70_next_10            32.83ms     30.46
run(LongDecimalType_Filter_100_Nulls_70_next_20            32.68ms     30.60
run(LongDecimalType_Filter_100_Nulls_70_next_20            30.73ms     32.55
run(LongDecimalType_Filter_100_Nulls_70_next_50            33.84ms     29.56
run(LongDecimalType_Filter_100_Nulls_70_next_50            32.30ms     30.96
run(LongDecimalType_Filter_100_Nulls_70_next_10            31.61ms     31.64
run(LongDecimalType_Filter_100_Nulls_70_next_10            34.85ms     28.69
----------------------------------------------------------------------------
run(LongDecimalType_Filter_100_Nulls_100_next_5            25.57ms     39.11
run(LongDecimalType_Filter_100_Nulls_100_next_5            23.66ms     42.27
run(LongDecimalType_Filter_100_Nulls_100_next_1            26.32ms     38.00
run(LongDecimalType_Filter_100_Nulls_100_next_1            23.76ms     42.08
run(LongDecimalType_Filter_100_Nulls_100_next_2            25.42ms     39.34
run(LongDecimalType_Filter_100_Nulls_100_next_2            23.70ms     42.19
run(LongDecimalType_Filter_100_Nulls_100_next_5            24.96ms     40.06
run(LongDecimalType_Filter_100_Nulls_100_next_5            24.25ms     41.23
run(LongDecimalType_Filter_100_Nulls_100_next_1            26.71ms     37.44
run(LongDecimalType_Filter_100_Nulls_100_next_1            25.32ms     39.50
----------------------------------------------------------------------------
----------------------------------------------------------------------------
run(Varchar_Filter_0_Nulls_0_next_5k_dict)                  5.32ms    187.93
run(Varchar_Filter_0_Nulls_0_next_5k_plain)                 5.29ms    188.99
run(Varchar_Filter_0_Nulls_0_next_10k_dict)                 9.26ms    108.03
run(Varchar_Filter_0_Nulls_0_next_10k_Plain)                5.88ms    169.96
run(Varchar_Filter_0_Nulls_0_next_20k_dict)                 5.46ms    183.17
run(Varchar_Filter_0_Nulls_0_next_20k_plain)                5.38ms    185.74
run(Varchar_Filter_0_Nulls_0_next_50k_dict)                40.03ms     24.98
run(Varchar_Filter_0_Nulls_0_next_50k_plain)                6.69ms    149.58
run(Varchar_Filter_0_Nulls_0_next_100k_dict)                5.80ms    172.37
run(Varchar_Filter_0_Nulls_0_next_100k_plain)               5.43ms    184.19
----------------------------------------------------------------------------
run(Varchar_Filter_0_Nulls_20_next_5k_dict)                 9.72ms    102.93
run(Varchar_Filter_0_Nulls_20_next_5k_plain)                4.86ms    205.67
run(Varchar_Filter_0_Nulls_20_next_10k_dict)                5.15ms    194.35
run(Varchar_Filter_0_Nulls_20_next_10k_Plain)               4.79ms    208.90
run(Varchar_Filter_0_Nulls_20_next_20k_dict)                6.92ms    144.53
run(Varchar_Filter_0_Nulls_20_next_20k_plain)               7.02ms    142.54
run(Varchar_Filter_0_Nulls_20_next_50k_dict)                4.54ms    220.29
run(Varchar_Filter_0_Nulls_20_next_50k_plain)               4.55ms    219.86
run(Varchar_Filter_0_Nulls_20_next_100k_dict)               7.61ms    131.44
run(Varchar_Filter_0_Nulls_20_next_100k_plain)              6.61ms    151.21
----------------------------------------------------------------------------
run(Varchar_Filter_0_Nulls_50_next_5k_dict)                 3.77ms    265.25
run(Varchar_Filter_0_Nulls_50_next_5k_plain)                3.12ms    320.20
run(Varchar_Filter_0_Nulls_50_next_10k_dict)                3.70ms    270.15
run(Varchar_Filter_0_Nulls_50_next_10k_Plain)               3.53ms    283.03
run(Varchar_Filter_0_Nulls_50_next_20k_dict)                3.67ms    272.58
run(Varchar_Filter_0_Nulls_50_next_20k_plain)               4.29ms    232.95
run(Varchar_Filter_0_Nulls_50_next_50k_dict)                4.55ms    219.95
run(Varchar_Filter_0_Nulls_50_next_50k_plain)               3.95ms    253.08
run(Varchar_Filter_0_Nulls_50_next_100k_dict)               5.18ms    192.97
run(Varchar_Filter_0_Nulls_50_next_100k_plain)              3.62ms    276.17
----------------------------------------------------------------------------
run(Varchar_Filter_0_Nulls_70_next_5k_dict)                 2.56ms    389.88
run(Varchar_Filter_0_Nulls_70_next_5k_plain)                8.11ms    123.38
run(Varchar_Filter_0_Nulls_70_next_10k_dict)                2.39ms    418.79
run(Varchar_Filter_0_Nulls_70_next_10k_Plain)               8.72ms    114.71
run(Varchar_Filter_0_Nulls_70_next_20k_dict)                2.53ms    394.64
run(Varchar_Filter_0_Nulls_70_next_20k_plain)               7.82ms    127.86
run(Varchar_Filter_0_Nulls_70_next_50k_dict)                2.29ms    436.80
run(Varchar_Filter_0_Nulls_70_next_50k_plain)               7.84ms    127.52
run(Varchar_Filter_0_Nulls_70_next_100k_dict)               2.35ms    424.96
run(Varchar_Filter_0_Nulls_70_next_100k_plain)              9.49ms    105.36
----------------------------------------------------------------------------
run(Varchar_Filter_0_Nulls_100_next_5k_dict)               26.86ms     37.24
run(Varchar_Filter_0_Nulls_100_next_5k_plain)              25.52ms     39.18
run(Varchar_Filter_0_Nulls_100_next_10k_dict)              25.54ms     39.15
run(Varchar_Filter_0_Nulls_100_next_10k_Plain)             26.48ms     37.76
run(Varchar_Filter_0_Nulls_100_next_20k_dict)              26.30ms     38.03
run(Varchar_Filter_0_Nulls_100_next_20k_plain)             25.58ms     39.09
run(Varchar_Filter_0_Nulls_100_next_50k_dict)              25.09ms     39.86
run(Varchar_Filter_0_Nulls_100_next_50k_plain)             27.84ms     35.92
run(Varchar_Filter_0_Nulls_100_next_100k_dict)             26.56ms     37.65
run(Varchar_Filter_0_Nulls_100_next_100k_plain)            26.71ms     37.43
----------------------------------------------------------------------------
run(Varchar_Filter_20_Nulls_0_next_5k_dict)                90.28ms     11.08
run(Varchar_Filter_20_Nulls_0_next_5k_plain)               83.37ms     12.00
run(Varchar_Filter_20_Nulls_0_next_10k_dict)               85.23ms     11.73
run(Varchar_Filter_20_Nulls_0_next_10k_Plain)              79.87ms     12.52
run(Varchar_Filter_20_Nulls_0_next_20k_dict)               85.88ms     11.64
run(Varchar_Filter_20_Nulls_0_next_20k_plain)              84.85ms     11.79
run(Varchar_Filter_20_Nulls_0_next_50k_dict)               87.89ms     11.38
run(Varchar_Filter_20_Nulls_0_next_50k_plain)             125.19ms      7.99
run(Varchar_Filter_20_Nulls_0_next_100k_dict)              89.07ms     11.23
run(Varchar_Filter_20_Nulls_0_next_100k_plain)             83.43ms     11.99
----------------------------------------------------------------------------
run(Varchar_Filter_20_Nulls_20_next_5k_dict)               88.11ms     11.35
run(Varchar_Filter_20_Nulls_20_next_5k_plain)              84.79ms     11.79
run(Varchar_Filter_20_Nulls_20_next_10k_dict)             104.79ms      9.54
run(Varchar_Filter_20_Nulls_20_next_10k_Plain)             82.26ms     12.16
run(Varchar_Filter_20_Nulls_20_next_20k_dict)              88.02ms     11.36
run(Varchar_Filter_20_Nulls_20_next_20k_plain)            107.05ms      9.34
run(Varchar_Filter_20_Nulls_20_next_50k_dict)              94.33ms     10.60
run(Varchar_Filter_20_Nulls_20_next_50k_plain)            102.25ms      9.78
run(Varchar_Filter_20_Nulls_20_next_100k_dict)            103.49ms      9.66
run(Varchar_Filter_20_Nulls_20_next_100k_plain)            85.39ms     11.71
----------------------------------------------------------------------------
run(Varchar_Filter_20_Nulls_50_next_5k_dict)               57.68ms     17.34
run(Varchar_Filter_20_Nulls_50_next_5k_plain)              76.37ms     13.09
run(Varchar_Filter_20_Nulls_50_next_10k_dict)              56.99ms     17.55
run(Varchar_Filter_20_Nulls_50_next_10k_Plain)             55.20ms     18.12
run(Varchar_Filter_20_Nulls_50_next_20k_dict)              56.72ms     17.63
run(Varchar_Filter_20_Nulls_50_next_20k_plain)             64.91ms     15.41
run(Varchar_Filter_20_Nulls_50_next_50k_dict)              61.01ms     16.39
run(Varchar_Filter_20_Nulls_50_next_50k_plain)             67.19ms     14.88
run(Varchar_Filter_20_Nulls_50_next_100k_dict)             56.58ms     17.67
run(Varchar_Filter_20_Nulls_50_next_100k_plain)            54.86ms     18.23
----------------------------------------------------------------------------
run(Varchar_Filter_20_Nulls_70_next_5k_dict)               38.30ms     26.11
run(Varchar_Filter_20_Nulls_70_next_5k_plain)              39.38ms     25.40
run(Varchar_Filter_20_Nulls_70_next_10k_dict)              37.44ms     26.71
run(Varchar_Filter_20_Nulls_70_next_10k_Plain)             36.66ms     27.28
run(Varchar_Filter_20_Nulls_70_next_20k_dict)              43.10ms     23.20
run(Varchar_Filter_20_Nulls_70_next_20k_plain)             39.95ms     25.03
run(Varchar_Filter_20_Nulls_70_next_50k_dict)              74.13ms     13.49
run(Varchar_Filter_20_Nulls_70_next_50k_plain)             36.83ms     27.15
run(Varchar_Filter_20_Nulls_70_next_100k_dict)             37.65ms     26.56
run(Varchar_Filter_20_Nulls_70_next_100k_plain)            39.82ms     25.11
----------------------------------------------------------------------------
run(Varchar_Filter_20_Nulls_100_next_5k_dict)              27.91ms     35.83
run(Varchar_Filter_20_Nulls_100_next_5k_plain)             26.33ms     37.99
run(Varchar_Filter_20_Nulls_100_next_10k_dict)             27.48ms     36.39
run(Varchar_Filter_20_Nulls_100_next_10k_Plain)            26.24ms     38.11
run(Varchar_Filter_20_Nulls_100_next_20k_dict)             26.66ms     37.51
run(Varchar_Filter_20_Nulls_100_next_20k_plain)            25.34ms     39.46
run(Varchar_Filter_20_Nulls_100_next_50k_dict)             27.19ms     36.78
run(Varchar_Filter_20_Nulls_100_next_50k_plain)            26.96ms     37.09
run(Varchar_Filter_20_Nulls_100_next_100k_dict)            28.31ms     35.32
run(Varchar_Filter_20_Nulls_100_next_100k_plain            28.91ms     34.59
----------------------------------------------------------------------------
run(Varchar_Filter_50_Nulls_0_next_5k_dict)               130.23ms      7.68
run(Varchar_Filter_50_Nulls_0_next_5k_plain)              115.45ms      8.66
run(Varchar_Filter_50_Nulls_0_next_10k_dict)              141.33ms      7.08
run(Varchar_Filter_50_Nulls_0_next_10k_Plain)             114.94ms      8.70
run(Varchar_Filter_50_Nulls_0_next_20k_dict)              144.02ms      6.94
run(Varchar_Filter_50_Nulls_0_next_20k_plain)             132.83ms      7.53
run(Varchar_Filter_50_Nulls_0_next_50k_dict)              145.94ms      6.85
run(Varchar_Filter_50_Nulls_0_next_50k_plain)             113.28ms      8.83
run(Varchar_Filter_50_Nulls_0_next_100k_dict)             122.83ms      8.14
run(Varchar_Filter_50_Nulls_0_next_100k_plain)            118.80ms      8.42
----------------------------------------------------------------------------
run(Varchar_Filter_50_Nulls_20_next_5k_dict)              116.37ms      8.59
run(Varchar_Filter_50_Nulls_20_next_5k_plain)             133.08ms      7.51
run(Varchar_Filter_50_Nulls_20_next_10k_dict)             104.11ms      9.61
run(Varchar_Filter_50_Nulls_20_next_10k_Plain)            104.95ms      9.53
run(Varchar_Filter_50_Nulls_20_next_20k_dict)             112.93ms      8.86
run(Varchar_Filter_50_Nulls_20_next_20k_plain)            124.88ms      8.01
run(Varchar_Filter_50_Nulls_20_next_50k_dict)             116.44ms      8.59
run(Varchar_Filter_50_Nulls_20_next_50k_plain)            121.84ms      8.21
run(Varchar_Filter_50_Nulls_20_next_100k_dict)            116.72ms      8.57
run(Varchar_Filter_50_Nulls_20_next_100k_plain)           108.65ms      9.20
----------------------------------------------------------------------------
run(Varchar_Filter_50_Nulls_50_next_5k_dict)               74.30ms     13.46
run(Varchar_Filter_50_Nulls_50_next_5k_plain)              90.97ms     10.99
run(Varchar_Filter_50_Nulls_50_next_10k_dict)              89.80ms     11.14
run(Varchar_Filter_50_Nulls_50_next_10k_Plain)             81.69ms     12.24
run(Varchar_Filter_50_Nulls_50_next_20k_dict)              83.35ms     12.00
run(Varchar_Filter_50_Nulls_50_next_20k_plain)             85.58ms     11.68
run(Varchar_Filter_50_Nulls_50_next_50k_dict)              91.49ms     10.93
run(Varchar_Filter_50_Nulls_50_next_50k_plain)             87.85ms     11.38
run(Varchar_Filter_50_Nulls_50_next_100k_dict)             73.89ms     13.53
run(Varchar_Filter_50_Nulls_50_next_100k_plain)            72.06ms     13.88
----------------------------------------------------------------------------
run(Varchar_Filter_50_Nulls_70_next_5k_dict)               43.12ms     23.19
run(Varchar_Filter_50_Nulls_70_next_5k_plain)              42.75ms     23.39
run(Varchar_Filter_50_Nulls_70_next_10k_dict)              43.55ms     22.96
run(Varchar_Filter_50_Nulls_70_next_10k_Plain)             43.24ms     23.13
run(Varchar_Filter_50_Nulls_70_next_20k_dict)              40.80ms     24.51
run(Varchar_Filter_50_Nulls_70_next_20k_plain)             44.61ms     22.42
run(Varchar_Filter_50_Nulls_70_next_50k_dict)              48.10ms     20.79
run(Varchar_Filter_50_Nulls_70_next_50k_plain)             55.42ms     18.04
run(Varchar_Filter_50_Nulls_70_next_100k_dict)             46.17ms     21.66
run(Varchar_Filter_50_Nulls_70_next_100k_plain)            53.83ms     18.58
----------------------------------------------------------------------------
run(Varchar_Filter_50_Nulls_100_next_5k_dict)              27.64ms     36.19
run(Varchar_Filter_50_Nulls_100_next_5k_plain)             27.66ms     36.16
run(Varchar_Filter_50_Nulls_100_next_10k_dict)             25.95ms     38.54
run(Varchar_Filter_50_Nulls_100_next_10k_Plain)            26.32ms     37.99
run(Varchar_Filter_50_Nulls_100_next_20k_dict)             25.18ms     39.72
run(Varchar_Filter_50_Nulls_100_next_20k_plain)            25.54ms     39.16
run(Varchar_Filter_50_Nulls_100_next_50k_dict)             25.61ms     39.04
run(Varchar_Filter_50_Nulls_100_next_50k_plain)            35.36ms     28.28
run(Varchar_Filter_50_Nulls_100_next_100k_dict)            27.92ms     35.81
run(Varchar_Filter_50_Nulls_100_next_100k_plain            26.12ms     38.28
----------------------------------------------------------------------------
run(Varchar_Filter_70_Nulls_0_next_5k_dict)               124.91ms      8.01
run(Varchar_Filter_70_Nulls_0_next_5k_plain)              132.41ms      7.55
run(Varchar_Filter_70_Nulls_0_next_10k_dict)              129.19ms      7.74
run(Varchar_Filter_70_Nulls_0_next_10k_Plain)             124.86ms      8.01
run(Varchar_Filter_70_Nulls_0_next_20k_dict)              119.14ms      8.39
run(Varchar_Filter_70_Nulls_0_next_20k_plain)             126.10ms      7.93
run(Varchar_Filter_70_Nulls_0_next_50k_dict)              129.98ms      7.69
run(Varchar_Filter_70_Nulls_0_next_50k_plain)             128.86ms      7.76
run(Varchar_Filter_70_Nulls_0_next_100k_dict)             139.25ms      7.18
run(Varchar_Filter_70_Nulls_0_next_100k_plain)            132.04ms      7.57
----------------------------------------------------------------------------
run(Varchar_Filter_70_Nulls_20_next_5k_dict)              109.20ms      9.16
run(Varchar_Filter_70_Nulls_20_next_5k_plain)             122.28ms      8.18
run(Varchar_Filter_70_Nulls_20_next_10k_dict)             111.34ms      8.98
run(Varchar_Filter_70_Nulls_20_next_10k_Plain)            111.96ms      8.93
run(Varchar_Filter_70_Nulls_20_next_20k_dict)             109.83ms      9.11
run(Varchar_Filter_70_Nulls_20_next_20k_plain)            118.30ms      8.45
run(Varchar_Filter_70_Nulls_20_next_50k_dict)             120.12ms      8.33
run(Varchar_Filter_70_Nulls_20_next_50k_plain)            116.68ms      8.57
run(Varchar_Filter_70_Nulls_20_next_100k_dict)            117.64ms      8.50
run(Varchar_Filter_70_Nulls_20_next_100k_plain)           108.25ms      9.24
----------------------------------------------------------------------------
run(Varchar_Filter_70_Nulls_50_next_5k_dict)               70.74ms     14.14
run(Varchar_Filter_70_Nulls_50_next_5k_plain)              79.03ms     12.65
run(Varchar_Filter_70_Nulls_50_next_10k_dict)              70.72ms     14.14
run(Varchar_Filter_70_Nulls_50_next_10k_Plain)             71.20ms     14.04
run(Varchar_Filter_70_Nulls_50_next_20k_dict)              75.30ms     13.28
run(Varchar_Filter_70_Nulls_50_next_20k_plain)             73.00ms     13.70
run(Varchar_Filter_70_Nulls_50_next_50k_dict)              74.73ms     13.38
run(Varchar_Filter_70_Nulls_50_next_50k_plain)             76.80ms     13.02
run(Varchar_Filter_70_Nulls_50_next_100k_dict)             91.29ms     10.95
run(Varchar_Filter_70_Nulls_50_next_100k_plain)            79.11ms     12.64
----------------------------------------------------------------------------
run(Varchar_Filter_70_Nulls_70_next_5k_dict)               41.44ms     24.13
run(Varchar_Filter_70_Nulls_70_next_5k_plain)              44.55ms     22.44
run(Varchar_Filter_70_Nulls_70_next_10k_dict)              42.10ms     23.76
run(Varchar_Filter_70_Nulls_70_next_10k_Plain)             44.21ms     22.62
run(Varchar_Filter_70_Nulls_70_next_20k_dict)              42.54ms     23.51
run(Varchar_Filter_70_Nulls_70_next_20k_plain)             43.78ms     22.84
run(Varchar_Filter_70_Nulls_70_next_50k_dict)              46.25ms     21.62
run(Varchar_Filter_70_Nulls_70_next_50k_plain)             44.13ms     22.66
run(Varchar_Filter_70_Nulls_70_next_100k_dict)             43.66ms     22.91
run(Varchar_Filter_70_Nulls_70_next_100k_plain)            43.26ms     23.12
----------------------------------------------------------------------------
run(Varchar_Filter_70_Nulls_100_next_5k_dict)              26.05ms     38.39
run(Varchar_Filter_70_Nulls_100_next_5k_plain)             30.05ms     33.28
run(Varchar_Filter_70_Nulls_100_next_10k_dict)             26.27ms     38.07
run(Varchar_Filter_70_Nulls_100_next_10k_Plain)            24.99ms     40.02
run(Varchar_Filter_70_Nulls_100_next_20k_dict)             23.60ms     42.38
run(Varchar_Filter_70_Nulls_100_next_20k_plain)            26.28ms     38.05
run(Varchar_Filter_70_Nulls_100_next_50k_dict)             27.20ms     36.76
run(Varchar_Filter_70_Nulls_100_next_50k_plain)            25.04ms     39.93
run(Varchar_Filter_70_Nulls_100_next_100k_dict)            27.26ms     36.68
run(Varchar_Filter_70_Nulls_100_next_100k_plain            26.59ms     37.60
----------------------------------------------------------------------------
run(Varchar_Filter_100_Nulls_0_next_5k_dict)              137.16ms      7.29
run(Varchar_Filter_100_Nulls_0_next_5k_plain)             135.69ms      7.37
run(Varchar_Filter_100_Nulls_0_next_10k_dict)             134.54ms      7.43
run(Varchar_Filter_100_Nulls_0_next_10k_Plain)            131.98ms      7.58
run(Varchar_Filter_100_Nulls_0_next_20k_dict)             136.59ms      7.32
run(Varchar_Filter_100_Nulls_0_next_20k_plain)            190.80ms      5.24
run(Varchar_Filter_100_Nulls_0_next_50k_dict)             140.13ms      7.14
run(Varchar_Filter_100_Nulls_0_next_50k_plain)            129.68ms      7.71
run(Varchar_Filter_100_Nulls_0_next_100k_dict)            137.93ms      7.25
run(Varchar_Filter_100_Nulls_0_next_100k_plain)           171.40ms      5.83
----------------------------------------------------------------------------
run(Varchar_Filter_100_Nulls_20_next_5k_dict)             115.31ms      8.67
run(Varchar_Filter_100_Nulls_20_next_5k_plain)            109.52ms      9.13
run(Varchar_Filter_100_Nulls_20_next_10k_dict)            124.47ms      8.03
run(Varchar_Filter_100_Nulls_20_next_10k_Plain)           111.67ms      8.96
run(Varchar_Filter_100_Nulls_20_next_20k_dict)            108.94ms      9.18
run(Varchar_Filter_100_Nulls_20_next_20k_plain)           114.43ms      8.74
run(Varchar_Filter_100_Nulls_20_next_50k_dict)            118.60ms      8.43
run(Varchar_Filter_100_Nulls_20_next_50k_plain)           115.76ms      8.64
run(Varchar_Filter_100_Nulls_20_next_100k_dict)           127.47ms      7.85
run(Varchar_Filter_100_Nulls_20_next_100k_plain           147.07ms      6.80
----------------------------------------------------------------------------
run(Varchar_Filter_100_Nulls_50_next_5k_dict)              84.54ms     11.83
run(Varchar_Filter_100_Nulls_50_next_5k_plain)             80.97ms     12.35
run(Varchar_Filter_100_Nulls_50_next_10k_dict)             80.03ms     12.49
run(Varchar_Filter_100_Nulls_50_next_10k_Plain)            96.20ms     10.40
run(Varchar_Filter_100_Nulls_50_next_20k_dict)             74.17ms     13.48
run(Varchar_Filter_100_Nulls_50_next_20k_plain)            74.38ms     13.44
run(Varchar_Filter_100_Nulls_50_next_50k_dict)             77.30ms     12.94
run(Varchar_Filter_100_Nulls_50_next_50k_plain)            78.24ms     12.78
run(Varchar_Filter_100_Nulls_50_next_100k_dict)            83.58ms     11.97
run(Varchar_Filter_100_Nulls_50_next_100k_plain            81.96ms     12.20
----------------------------------------------------------------------------
run(Varchar_Filter_100_Nulls_70_next_5k_dict)              42.43ms     23.57
run(Varchar_Filter_100_Nulls_70_next_5k_plain)             46.09ms     21.70
run(Varchar_Filter_100_Nulls_70_next_10k_dict)             43.93ms     22.76
run(Varchar_Filter_100_Nulls_70_next_10k_Plain)            44.54ms     22.45
run(Varchar_Filter_100_Nulls_70_next_20k_dict)             44.16ms     22.64
run(Varchar_Filter_100_Nulls_70_next_20k_plain)            47.50ms     21.05
run(Varchar_Filter_100_Nulls_70_next_50k_dict)             44.29ms     22.58
run(Varchar_Filter_100_Nulls_70_next_50k_plain)            45.82ms     21.82
run(Varchar_Filter_100_Nulls_70_next_100k_dict)            45.90ms     21.79
run(Varchar_Filter_100_Nulls_70_next_100k_plain            83.63ms     11.96
----------------------------------------------------------------------------
run(Varchar_Filter_100_Nulls_100_next_5k_dict)             25.68ms     38.94
run(Varchar_Filter_100_Nulls_100_next_5k_plain)            25.76ms     38.81
run(Varchar_Filter_100_Nulls_100_next_10k_dict)            25.33ms     39.48
run(Varchar_Filter_100_Nulls_100_next_10k_Plain            24.44ms     40.91
run(Varchar_Filter_100_Nulls_100_next_20k_dict)            27.36ms     36.55
run(Varchar_Filter_100_Nulls_100_next_20k_plain            27.52ms     36.34
run(Varchar_Filter_100_Nulls_100_next_50k_dict)            25.19ms     39.70
run(Varchar_Filter_100_Nulls_100_next_50k_plain            26.57ms     37.64
run(Varchar_Filter_100_Nulls_100_next_100k_dict            26.48ms     37.77
run(Varchar_Filter_100_Nulls_100_next_100k_plai            25.44ms     39.30
----------------------------------------------------------------------------
----------------------------------------------------------------------------
run(BigInt_Filter_0_Nulls_0_next_5k_dict)                  19.67ms     50.84
run(BigInt_Filter_0_Nulls_0_next_5k_plain)                 17.62ms     56.75
run(BigInt_Filter_0_Nulls_0_next_10k_dict)                 21.28ms     46.99
run(BigInt_Filter_0_Nulls_0_next_10k_Plain)                17.49ms     57.19
run(BigInt_Filter_0_Nulls_0_next_20k_dict)                 20.34ms     49.17
run(BigInt_Filter_0_Nulls_0_next_20k_plain)                19.35ms     51.69
run(BigInt_Filter_0_Nulls_0_next_50k_dict)                 20.95ms     47.74
run(BigInt_Filter_0_Nulls_0_next_50k_plain)                12.82ms     78.02
run(BigInt_Filter_0_Nulls_0_next_100k_dict)                15.67ms     63.83
run(BigInt_Filter_0_Nulls_0_next_100k_plain)               13.20ms     75.75
----------------------------------------------------------------------------
run(BigInt_Filter_0_Nulls_20_next_5k_dict)                 17.65ms     56.65
run(BigInt_Filter_0_Nulls_20_next_5k_plain)                14.22ms     70.33
run(BigInt_Filter_0_Nulls_20_next_10k_dict)                17.24ms     58.00
run(BigInt_Filter_0_Nulls_20_next_10k_Plain)               13.93ms     71.80
run(BigInt_Filter_0_Nulls_20_next_20k_dict)                15.86ms     63.06
run(BigInt_Filter_0_Nulls_20_next_20k_plain)               12.72ms     78.61
run(BigInt_Filter_0_Nulls_20_next_50k_dict)                15.00ms     66.66
run(BigInt_Filter_0_Nulls_20_next_50k_plain)               12.97ms     77.11
run(BigInt_Filter_0_Nulls_20_next_100k_dict)               16.33ms     61.22
run(BigInt_Filter_0_Nulls_20_next_100k_plain)              13.08ms     76.47
----------------------------------------------------------------------------
run(BigInt_Filter_0_Nulls_50_next_5k_dict)                 12.00ms     83.33
run(BigInt_Filter_0_Nulls_50_next_5k_plain)                 8.93ms    111.96
run(BigInt_Filter_0_Nulls_50_next_10k_dict)                12.58ms     79.48
run(BigInt_Filter_0_Nulls_50_next_10k_Plain)                9.55ms    104.72
run(BigInt_Filter_0_Nulls_50_next_20k_dict)                11.84ms     84.47
run(BigInt_Filter_0_Nulls_50_next_20k_plain)                8.27ms    120.98
run(BigInt_Filter_0_Nulls_50_next_50k_dict)                12.15ms     82.30
run(BigInt_Filter_0_Nulls_50_next_50k_plain)                8.10ms    123.44
run(BigInt_Filter_0_Nulls_50_next_100k_dict)               11.75ms     85.07
run(BigInt_Filter_0_Nulls_50_next_100k_plain)               9.61ms    104.07
----------------------------------------------------------------------------
run(BigInt_Filter_0_Nulls_70_next_5k_dict)                 12.23ms     81.75
run(BigInt_Filter_0_Nulls_70_next_5k_plain)                10.29ms     97.21
run(BigInt_Filter_0_Nulls_70_next_10k_dict)                11.69ms     85.55
run(BigInt_Filter_0_Nulls_70_next_10k_Plain)                9.01ms    110.93
run(BigInt_Filter_0_Nulls_70_next_20k_dict)                12.22ms     81.81
run(BigInt_Filter_0_Nulls_70_next_20k_plain)                8.91ms    112.21
run(BigInt_Filter_0_Nulls_70_next_50k_dict)                14.01ms     71.40
run(BigInt_Filter_0_Nulls_70_next_50k_plain)                8.92ms    112.16
run(BigInt_Filter_0_Nulls_70_next_100k_dict)               14.05ms     71.16
run(BigInt_Filter_0_Nulls_70_next_100k_plain)               8.95ms    111.70
----------------------------------------------------------------------------
run(BigInt_Filter_0_Nulls_100_next_5k_dict)               589.01us     1.70K
run(BigInt_Filter_0_Nulls_100_next_5k_plain)              589.14us     1.70K
run(BigInt_Filter_0_Nulls_100_next_10k_dict)              585.08us     1.71K
run(BigInt_Filter_0_Nulls_100_next_10k_Plain)             568.23us     1.76K
run(BigInt_Filter_0_Nulls_100_next_20k_dict)              603.65us     1.66K
run(BigInt_Filter_0_Nulls_100_next_20k_plain)             588.12us     1.70K
run(BigInt_Filter_0_Nulls_100_next_50k_dict)              627.55us     1.59K
run(BigInt_Filter_0_Nulls_100_next_50k_plain)             549.83us     1.82K
run(BigInt_Filter_0_Nulls_100_next_100k_dict)             582.57us     1.72K
run(BigInt_Filter_0_Nulls_100_next_100k_plain)            579.90us     1.72K
----------------------------------------------------------------------------
run(BigInt_Filter_20_Nulls_0_next_5k_dict)                 33.11ms     30.20
run(BigInt_Filter_20_Nulls_0_next_5k_plain)                30.10ms     33.22
run(BigInt_Filter_20_Nulls_0_next_10k_dict)                38.61ms     25.90
run(BigInt_Filter_20_Nulls_0_next_10k_Plain)               27.26ms     36.68
run(BigInt_Filter_20_Nulls_0_next_20k_dict)                33.59ms     29.77
run(BigInt_Filter_20_Nulls_0_next_20k_plain)               26.07ms     38.36
run(BigInt_Filter_20_Nulls_0_next_50k_dict)                35.97ms     27.80
run(BigInt_Filter_20_Nulls_0_next_50k_plain)               26.86ms     37.24
run(BigInt_Filter_20_Nulls_0_next_100k_dict)               34.78ms     28.75
run(BigInt_Filter_20_Nulls_0_next_100k_plain)              26.03ms     38.42
----------------------------------------------------------------------------
run(BigInt_Filter_20_Nulls_20_next_5k_dict)                32.30ms     30.96
run(BigInt_Filter_20_Nulls_20_next_5k_plain)               25.54ms     39.15
run(BigInt_Filter_20_Nulls_20_next_10k_dict)               31.17ms     32.08
run(BigInt_Filter_20_Nulls_20_next_10k_Plain)              27.79ms     35.99
run(BigInt_Filter_20_Nulls_20_next_20k_dict)               31.33ms     31.92
run(BigInt_Filter_20_Nulls_20_next_20k_plain)              25.95ms     38.54
run(BigInt_Filter_20_Nulls_20_next_50k_dict)               33.84ms     29.55
run(BigInt_Filter_20_Nulls_20_next_50k_plain)              26.10ms     38.31
run(BigInt_Filter_20_Nulls_20_next_100k_dict)              33.24ms     30.08
run(BigInt_Filter_20_Nulls_20_next_100k_plain)             26.67ms     37.50
----------------------------------------------------------------------------
run(BigInt_Filter_20_Nulls_50_next_5k_dict)                22.89ms     43.69
run(BigInt_Filter_20_Nulls_50_next_5k_plain)               15.59ms     64.15
run(BigInt_Filter_20_Nulls_50_next_10k_dict)               22.89ms     43.68
run(BigInt_Filter_20_Nulls_50_next_10k_Plain)              15.13ms     66.07
run(BigInt_Filter_20_Nulls_50_next_20k_dict)               22.60ms     44.25
run(BigInt_Filter_20_Nulls_50_next_20k_plain)              15.21ms     65.74
run(BigInt_Filter_20_Nulls_50_next_50k_dict)               22.60ms     44.26
run(BigInt_Filter_20_Nulls_50_next_50k_plain)              15.70ms     63.70
run(BigInt_Filter_20_Nulls_50_next_100k_dict)              22.60ms     44.24
run(BigInt_Filter_20_Nulls_50_next_100k_plain)             16.28ms     61.43
----------------------------------------------------------------------------
run(BigInt_Filter_20_Nulls_70_next_5k_dict)                19.76ms     50.60
run(BigInt_Filter_20_Nulls_70_next_5k_plain)               13.61ms     73.45
run(BigInt_Filter_20_Nulls_70_next_10k_dict)               18.26ms     54.77
run(BigInt_Filter_20_Nulls_70_next_10k_Plain)              13.27ms     75.34
run(BigInt_Filter_20_Nulls_70_next_20k_dict)               17.91ms     55.84
run(BigInt_Filter_20_Nulls_70_next_20k_plain)              13.04ms     76.71
run(BigInt_Filter_20_Nulls_70_next_50k_dict)               24.10ms     41.50
run(BigInt_Filter_20_Nulls_70_next_50k_plain)              12.82ms     78.03
run(BigInt_Filter_20_Nulls_70_next_100k_dict)              18.59ms     53.78
run(BigInt_Filter_20_Nulls_70_next_100k_plain)             13.43ms     74.48
----------------------------------------------------------------------------
run(BigInt_Filter_20_Nulls_100_next_5k_dict)              583.76us     1.71K
run(BigInt_Filter_20_Nulls_100_next_5k_plain)             581.78us     1.72K
run(BigInt_Filter_20_Nulls_100_next_10k_dict)             587.00us     1.70K
run(BigInt_Filter_20_Nulls_100_next_10k_Plain)            549.73us     1.82K
run(BigInt_Filter_20_Nulls_100_next_20k_dict)             578.79us     1.73K
run(BigInt_Filter_20_Nulls_100_next_20k_plain)            564.92us     1.77K
run(BigInt_Filter_20_Nulls_100_next_50k_dict)             557.14us     1.79K
run(BigInt_Filter_20_Nulls_100_next_50k_plain)            557.82us     1.79K
run(BigInt_Filter_20_Nulls_100_next_100k_dict)            600.05us     1.67K
run(BigInt_Filter_20_Nulls_100_next_100k_plain)           545.52us     1.83K
----------------------------------------------------------------------------
run(BigInt_Filter_50_Nulls_0_next_5k_dict)                 36.14ms     27.67
run(BigInt_Filter_50_Nulls_0_next_5k_plain)                30.09ms     33.23
run(BigInt_Filter_50_Nulls_0_next_10k_dict)                36.09ms     27.71
run(BigInt_Filter_50_Nulls_0_next_10k_Plain)               28.10ms     35.58
run(BigInt_Filter_50_Nulls_0_next_20k_dict)                37.02ms     27.01
run(BigInt_Filter_50_Nulls_0_next_20k_plain)               25.86ms     38.68
run(BigInt_Filter_50_Nulls_0_next_50k_dict)                37.96ms     26.34
run(BigInt_Filter_50_Nulls_0_next_50k_plain)               26.22ms     38.13
run(BigInt_Filter_50_Nulls_0_next_100k_dict)               38.88ms     25.72
run(BigInt_Filter_50_Nulls_0_next_100k_plain)              28.82ms     34.70
----------------------------------------------------------------------------
run(BigInt_Filter_50_Nulls_20_next_5k_dict)                35.84ms     27.90
run(BigInt_Filter_50_Nulls_20_next_5k_plain)               26.28ms     38.06
run(BigInt_Filter_50_Nulls_20_next_10k_dict)               35.65ms     28.05
run(BigInt_Filter_50_Nulls_20_next_10k_Plain)              26.53ms     37.69
run(BigInt_Filter_50_Nulls_20_next_20k_dict)               35.21ms     28.40
run(BigInt_Filter_50_Nulls_20_next_20k_plain)              26.81ms     37.30
run(BigInt_Filter_50_Nulls_20_next_50k_dict)               38.06ms     26.28
run(BigInt_Filter_50_Nulls_20_next_50k_plain)              27.06ms     36.95
run(BigInt_Filter_50_Nulls_20_next_100k_dict)              38.68ms     25.86
run(BigInt_Filter_50_Nulls_20_next_100k_plain)             26.93ms     37.14
----------------------------------------------------------------------------
run(BigInt_Filter_50_Nulls_50_next_5k_dict)                26.05ms     38.39
run(BigInt_Filter_50_Nulls_50_next_5k_plain)               17.94ms     55.74
run(BigInt_Filter_50_Nulls_50_next_10k_dict)               26.62ms     37.56
run(BigInt_Filter_50_Nulls_50_next_10k_Plain)              16.03ms     62.40
run(BigInt_Filter_50_Nulls_50_next_20k_dict)               24.46ms     40.88
run(BigInt_Filter_50_Nulls_50_next_20k_plain)              16.13ms     62.00
run(BigInt_Filter_50_Nulls_50_next_50k_dict)               26.59ms     37.60
run(BigInt_Filter_50_Nulls_50_next_50k_plain)              14.98ms     66.75
run(BigInt_Filter_50_Nulls_50_next_100k_dict)              25.41ms     39.35
run(BigInt_Filter_50_Nulls_50_next_100k_plain)             18.06ms     55.36
----------------------------------------------------------------------------
run(BigInt_Filter_50_Nulls_70_next_5k_dict)                21.19ms     47.19
run(BigInt_Filter_50_Nulls_70_next_5k_plain)               14.22ms     70.32
run(BigInt_Filter_50_Nulls_70_next_10k_dict)               21.14ms     47.30
run(BigInt_Filter_50_Nulls_70_next_10k_Plain)              12.84ms     77.90
run(BigInt_Filter_50_Nulls_70_next_20k_dict)               21.26ms     47.04
run(BigInt_Filter_50_Nulls_70_next_20k_plain)              12.68ms     78.84
run(BigInt_Filter_50_Nulls_70_next_50k_dict)               19.16ms     52.19
run(BigInt_Filter_50_Nulls_70_next_50k_plain)              12.39ms     80.70
run(BigInt_Filter_50_Nulls_70_next_100k_dict)              19.42ms     51.48
run(BigInt_Filter_50_Nulls_70_next_100k_plain)             11.80ms     84.71
----------------------------------------------------------------------------
run(BigInt_Filter_50_Nulls_100_next_5k_dict)              570.17us     1.75K
run(BigInt_Filter_50_Nulls_100_next_5k_plain)             568.22us     1.76K
run(BigInt_Filter_50_Nulls_100_next_10k_dict)             566.96us     1.76K
run(BigInt_Filter_50_Nulls_100_next_10k_Plain)            578.18us     1.73K
run(BigInt_Filter_50_Nulls_100_next_20k_dict)             606.88us     1.65K
run(BigInt_Filter_50_Nulls_100_next_20k_plain)            571.60us     1.75K
run(BigInt_Filter_50_Nulls_100_next_50k_dict)             647.80us     1.54K
run(BigInt_Filter_50_Nulls_100_next_50k_plain)            555.44us     1.80K
run(BigInt_Filter_50_Nulls_100_next_100k_dict)            612.23us     1.63K
run(BigInt_Filter_50_Nulls_100_next_100k_plain)           588.30us     1.70K
----------------------------------------------------------------------------
run(BigInt_Filter_70_Nulls_0_next_5k_dict)                 38.75ms     25.81
run(BigInt_Filter_70_Nulls_0_next_5k_plain)                29.74ms     33.63
run(BigInt_Filter_70_Nulls_0_next_10k_dict)                38.17ms     26.20
run(BigInt_Filter_70_Nulls_0_next_10k_Plain)               30.71ms     32.57
run(BigInt_Filter_70_Nulls_0_next_20k_dict)                37.38ms     26.76
run(BigInt_Filter_70_Nulls_0_next_20k_plain)               29.70ms     33.68
run(BigInt_Filter_70_Nulls_0_next_50k_dict)                39.56ms     25.28
run(BigInt_Filter_70_Nulls_0_next_50k_plain)               29.87ms     33.47
run(BigInt_Filter_70_Nulls_0_next_100k_dict)               45.84ms     21.82
run(BigInt_Filter_70_Nulls_0_next_100k_plain)              30.22ms     33.09
----------------------------------------------------------------------------
run(BigInt_Filter_70_Nulls_20_next_5k_dict)                53.33ms     18.75
run(BigInt_Filter_70_Nulls_20_next_5k_plain)               28.98ms     34.51
run(BigInt_Filter_70_Nulls_20_next_10k_dict)               51.84ms     19.29
run(BigInt_Filter_70_Nulls_20_next_10k_Plain)              40.89ms     24.45
run(BigInt_Filter_70_Nulls_20_next_20k_dict)               64.73ms     15.45
run(BigInt_Filter_70_Nulls_20_next_20k_plain)              42.02ms     23.80
run(BigInt_Filter_70_Nulls_20_next_50k_dict)               59.81ms     16.72
run(BigInt_Filter_70_Nulls_20_next_50k_plain)              29.56ms     33.83
run(BigInt_Filter_70_Nulls_20_next_100k_dict)              39.13ms     25.56
run(BigInt_Filter_70_Nulls_20_next_100k_plain)             28.53ms     35.05
----------------------------------------------------------------------------
run(BigInt_Filter_70_Nulls_50_next_5k_dict)                27.15ms     36.83
run(BigInt_Filter_70_Nulls_50_next_5k_plain)               17.64ms     56.69
run(BigInt_Filter_70_Nulls_50_next_10k_dict)               27.58ms     36.26
run(BigInt_Filter_70_Nulls_50_next_10k_Plain)              17.51ms     57.10
run(BigInt_Filter_70_Nulls_50_next_20k_dict)               26.22ms     38.13
run(BigInt_Filter_70_Nulls_50_next_20k_plain)              16.76ms     59.66
run(BigInt_Filter_70_Nulls_50_next_50k_dict)               25.33ms     39.48
run(BigInt_Filter_70_Nulls_50_next_50k_plain)              19.20ms     52.09
run(BigInt_Filter_70_Nulls_50_next_100k_dict)              25.25ms     39.61
run(BigInt_Filter_70_Nulls_50_next_100k_plain)             18.35ms     54.51
----------------------------------------------------------------------------
run(BigInt_Filter_70_Nulls_70_next_5k_dict)                21.82ms     45.84
run(BigInt_Filter_70_Nulls_70_next_5k_plain)               13.82ms     72.38
run(BigInt_Filter_70_Nulls_70_next_10k_dict)               20.45ms     48.90
run(BigInt_Filter_70_Nulls_70_next_10k_Plain)              13.49ms     74.16
run(BigInt_Filter_70_Nulls_70_next_20k_dict)               21.34ms     46.87
run(BigInt_Filter_70_Nulls_70_next_20k_plain)              13.40ms     74.64
run(BigInt_Filter_70_Nulls_70_next_50k_dict)               20.91ms     47.82
run(BigInt_Filter_70_Nulls_70_next_50k_plain)              13.26ms     75.40
run(BigInt_Filter_70_Nulls_70_next_100k_dict)              21.22ms     47.13
run(BigInt_Filter_70_Nulls_70_next_100k_plain)             14.13ms     70.76
----------------------------------------------------------------------------
run(BigInt_Filter_70_Nulls_100_next_5k_dict)              542.65us     1.84K
run(BigInt_Filter_70_Nulls_100_next_5k_plain)             585.13us     1.71K
run(BigInt_Filter_70_Nulls_100_next_10k_dict)             603.16us     1.66K
run(BigInt_Filter_70_Nulls_100_next_10k_Plain)            501.70us     1.99K
run(BigInt_Filter_70_Nulls_100_next_20k_dict)             547.60us     1.83K
run(BigInt_Filter_70_Nulls_100_next_20k_plain)            546.64us     1.83K
run(BigInt_Filter_70_Nulls_100_next_50k_dict)             545.93us     1.83K
run(BigInt_Filter_70_Nulls_100_next_50k_plain)            580.78us     1.72K
run(BigInt_Filter_70_Nulls_100_next_100k_dict)            614.73us     1.63K
run(BigInt_Filter_70_Nulls_100_next_100k_plain)           560.30us     1.78K
----------------------------------------------------------------------------
run(BigInt_Filter_100_Nulls_0_next_5k_dict)                34.82ms     28.72
run(BigInt_Filter_100_Nulls_0_next_5k_plain)               32.44ms     30.83
run(BigInt_Filter_100_Nulls_0_next_10k_dict)               37.55ms     26.63
run(BigInt_Filter_100_Nulls_0_next_10k_Plain)              30.13ms     33.19
run(BigInt_Filter_100_Nulls_0_next_20k_dict)               36.24ms     27.59
run(BigInt_Filter_100_Nulls_0_next_20k_plain)              31.41ms     31.84
run(BigInt_Filter_100_Nulls_0_next_50k_dict)               38.83ms     25.75
run(BigInt_Filter_100_Nulls_0_next_50k_plain)              29.08ms     34.39
run(BigInt_Filter_100_Nulls_0_next_100k_dict)              37.95ms     26.35
run(BigInt_Filter_100_Nulls_0_next_100k_plain)             34.97ms     28.59
----------------------------------------------------------------------------
run(BigInt_Filter_100_Nulls_20_next_5k_dict)               32.57ms     30.70
run(BigInt_Filter_100_Nulls_20_next_5k_plain)              26.87ms     37.22
run(BigInt_Filter_100_Nulls_20_next_10k_dict)              33.73ms     29.65
run(BigInt_Filter_100_Nulls_20_next_10k_Plain)             25.98ms     38.49
run(BigInt_Filter_100_Nulls_20_next_20k_dict)              34.50ms     28.98
run(BigInt_Filter_100_Nulls_20_next_20k_plain)             26.85ms     37.24
run(BigInt_Filter_100_Nulls_20_next_50k_dict)              32.18ms     31.08
run(BigInt_Filter_100_Nulls_20_next_50k_plain)             25.44ms     39.31
run(BigInt_Filter_100_Nulls_20_next_100k_dict)             36.54ms     27.37
run(BigInt_Filter_100_Nulls_20_next_100k_plain)            29.77ms     33.59
----------------------------------------------------------------------------
run(BigInt_Filter_100_Nulls_50_next_5k_dict)               23.14ms     43.22
run(BigInt_Filter_100_Nulls_50_next_5k_plain)              14.13ms     70.75
run(BigInt_Filter_100_Nulls_50_next_10k_dict)              23.29ms     42.94
run(BigInt_Filter_100_Nulls_50_next_10k_Plain)             15.94ms     62.75
run(BigInt_Filter_100_Nulls_50_next_20k_dict)              21.27ms     47.02
run(BigInt_Filter_100_Nulls_50_next_20k_plain)             16.11ms     62.07
run(BigInt_Filter_100_Nulls_50_next_50k_dict)              24.28ms     41.19
run(BigInt_Filter_100_Nulls_50_next_50k_plain)             16.12ms     62.04
run(BigInt_Filter_100_Nulls_50_next_100k_dict)             23.40ms     42.74
run(BigInt_Filter_100_Nulls_50_next_100k_plain)            17.04ms     58.69
----------------------------------------------------------------------------
run(BigInt_Filter_100_Nulls_70_next_5k_dict)               20.43ms     48.94
run(BigInt_Filter_100_Nulls_70_next_5k_plain)              14.00ms     71.45
run(BigInt_Filter_100_Nulls_70_next_10k_dict)              17.35ms     57.65
run(BigInt_Filter_100_Nulls_70_next_10k_Plain)             12.86ms     77.74
run(BigInt_Filter_100_Nulls_70_next_20k_dict)              19.99ms     50.03
run(BigInt_Filter_100_Nulls_70_next_20k_plain)             12.87ms     77.72
run(BigInt_Filter_100_Nulls_70_next_50k_dict)              18.68ms     53.52
run(BigInt_Filter_100_Nulls_70_next_50k_plain)             12.30ms     81.27
run(BigInt_Filter_100_Nulls_70_next_100k_dict)             18.65ms     53.61
run(BigInt_Filter_100_Nulls_70_next_100k_plain)            15.90ms     62.87
----------------------------------------------------------------------------
run(BigInt_Filter_100_Nulls_100_next_5k_dict)             590.53us     1.69K
run(BigInt_Filter_100_Nulls_100_next_5k_plain)            601.18us     1.66K
run(BigInt_Filter_100_Nulls_100_next_10k_dict)            591.00us     1.69K
run(BigInt_Filter_100_Nulls_100_next_10k_Plain)           529.76us     1.89K
run(BigInt_Filter_100_Nulls_100_next_20k_dict)            560.74us     1.78K
run(BigInt_Filter_100_Nulls_100_next_20k_plain)           609.59us     1.64K
run(BigInt_Filter_100_Nulls_100_next_50k_dict)            559.05us     1.79K
run(BigInt_Filter_100_Nulls_100_next_50k_plain)           561.57us     1.78K
run(BigInt_Filter_100_Nulls_100_next_100k_dict)           576.35us     1.74K
run(BigInt_Filter_100_Nulls_100_next_100k_plain           581.41us     1.72K
----------------------------------------------------------------------------
----------------------------------------------------------------------------
run(Double_Filter_0_Nulls_0_next_5k_dict)                  16.95ms     59.00
run(Double_Filter_0_Nulls_0_next_5k_plain)                 14.06ms     71.10
run(Double_Filter_0_Nulls_0_next_10k_dict)                 16.57ms     60.37
run(Double_Filter_0_Nulls_0_next_10k_Plain)                13.73ms     72.86
run(Double_Filter_0_Nulls_0_next_20k_dict)                 17.56ms     56.93
run(Double_Filter_0_Nulls_0_next_20k_plain)                14.56ms     68.70
run(Double_Filter_0_Nulls_0_next_50k_dict)                 19.33ms     51.74
run(Double_Filter_0_Nulls_0_next_50k_plain)                 8.63ms    115.88
run(Double_Filter_0_Nulls_0_next_100k_dict)                10.51ms     95.17
run(Double_Filter_0_Nulls_0_next_100k_plain)                8.82ms    113.36
----------------------------------------------------------------------------
run(Double_Filter_0_Nulls_20_next_5k_dict)                 26.96ms     37.10
run(Double_Filter_0_Nulls_20_next_5k_plain)                17.49ms     57.18
run(Double_Filter_0_Nulls_20_next_10k_dict)                21.34ms     46.87
run(Double_Filter_0_Nulls_20_next_10k_Plain)               16.88ms     59.24
run(Double_Filter_0_Nulls_20_next_20k_dict)                26.51ms     37.73
run(Double_Filter_0_Nulls_20_next_20k_plain)               17.78ms     56.25
run(Double_Filter_0_Nulls_20_next_50k_dict)                24.67ms     40.54
run(Double_Filter_0_Nulls_20_next_50k_plain)               17.19ms     58.19
run(Double_Filter_0_Nulls_20_next_100k_dict)               21.23ms     47.11
run(Double_Filter_0_Nulls_20_next_100k_plain)              18.61ms     53.72
----------------------------------------------------------------------------
run(Double_Filter_0_Nulls_50_next_5k_dict)                 18.62ms     53.71
run(Double_Filter_0_Nulls_50_next_5k_plain)                10.74ms     93.13
run(Double_Filter_0_Nulls_50_next_10k_dict)                16.03ms     62.37
run(Double_Filter_0_Nulls_50_next_10k_Plain)                9.32ms    107.31
run(Double_Filter_0_Nulls_50_next_20k_dict)                19.31ms     51.80
run(Double_Filter_0_Nulls_50_next_20k_plain)                9.53ms    104.98
run(Double_Filter_0_Nulls_50_next_50k_dict)                16.89ms     59.19
run(Double_Filter_0_Nulls_50_next_50k_plain)               10.68ms     93.63
run(Double_Filter_0_Nulls_50_next_100k_dict)               15.31ms     65.30
run(Double_Filter_0_Nulls_50_next_100k_plain)               9.97ms    100.28
----------------------------------------------------------------------------
run(Double_Filter_0_Nulls_70_next_5k_dict)                 17.47ms     57.23
run(Double_Filter_0_Nulls_70_next_5k_plain)                10.01ms     99.87
run(Double_Filter_0_Nulls_70_next_10k_dict)                16.64ms     60.08
run(Double_Filter_0_Nulls_70_next_10k_Plain)               10.11ms     98.87
run(Double_Filter_0_Nulls_70_next_20k_dict)                15.93ms     62.76
run(Double_Filter_0_Nulls_70_next_20k_plain)                9.98ms    100.24
run(Double_Filter_0_Nulls_70_next_50k_dict)                14.67ms     68.19
run(Double_Filter_0_Nulls_70_next_50k_plain)               11.27ms     88.70
run(Double_Filter_0_Nulls_70_next_100k_dict)               14.93ms     66.97
run(Double_Filter_0_Nulls_70_next_100k_plain)              12.22ms     81.80
----------------------------------------------------------------------------
run(Double_Filter_0_Nulls_100_next_5k_dict)               806.96us     1.24K
run(Double_Filter_0_Nulls_100_next_5k_plain)              564.56us     1.77K
run(Double_Filter_0_Nulls_100_next_10k_dict)              767.22us     1.30K
run(Double_Filter_0_Nulls_100_next_10k_Plain)             745.21us     1.34K
run(Double_Filter_0_Nulls_100_next_20k_dict)              567.85us     1.76K
run(Double_Filter_0_Nulls_100_next_20k_plain)             570.24us     1.75K
run(Double_Filter_0_Nulls_100_next_50k_dict)              557.05us     1.80K
run(Double_Filter_0_Nulls_100_next_50k_plain)             571.49us     1.75K
run(Double_Filter_0_Nulls_100_next_100k_dict)             556.75us     1.80K
run(Double_Filter_0_Nulls_100_next_100k_plain)            573.88us     1.74K
----------------------------------------------------------------------------
run(Double_Filter_20_Nulls_0_next_5k_dict)                 35.40ms     28.25
run(Double_Filter_20_Nulls_0_next_5k_plain)                26.72ms     37.42
run(Double_Filter_20_Nulls_0_next_10k_dict)                34.58ms     28.92
run(Double_Filter_20_Nulls_0_next_10k_Plain)               26.70ms     37.45
run(Double_Filter_20_Nulls_0_next_20k_dict)                37.30ms     26.81
run(Double_Filter_20_Nulls_0_next_20k_plain)               26.44ms     37.83
run(Double_Filter_20_Nulls_0_next_50k_dict)                34.40ms     29.07
run(Double_Filter_20_Nulls_0_next_50k_plain)               25.28ms     39.56
run(Double_Filter_20_Nulls_0_next_100k_dict)               34.47ms     29.01
run(Double_Filter_20_Nulls_0_next_100k_plain)              25.68ms     38.95
----------------------------------------------------------------------------
run(Double_Filter_20_Nulls_20_next_5k_dict)                33.47ms     29.88
run(Double_Filter_20_Nulls_20_next_5k_plain)               23.17ms     43.15
run(Double_Filter_20_Nulls_20_next_10k_dict)               31.23ms     32.02
run(Double_Filter_20_Nulls_20_next_10k_Plain)              24.08ms     41.53
run(Double_Filter_20_Nulls_20_next_20k_dict)               32.51ms     30.76
run(Double_Filter_20_Nulls_20_next_20k_plain)              24.10ms     41.49
run(Double_Filter_20_Nulls_20_next_50k_dict)               30.59ms     32.69
run(Double_Filter_20_Nulls_20_next_50k_plain)              24.37ms     41.03
run(Double_Filter_20_Nulls_20_next_100k_dict)              31.70ms     31.54
run(Double_Filter_20_Nulls_20_next_100k_plain)             25.16ms     39.75
----------------------------------------------------------------------------
run(Double_Filter_20_Nulls_50_next_5k_dict)                21.62ms     46.25
run(Double_Filter_20_Nulls_50_next_5k_plain)               14.71ms     67.96
run(Double_Filter_20_Nulls_50_next_10k_dict)               23.46ms     42.63
run(Double_Filter_20_Nulls_50_next_10k_Plain)              15.57ms     64.21
run(Double_Filter_20_Nulls_50_next_20k_dict)               22.88ms     43.70
run(Double_Filter_20_Nulls_50_next_20k_plain)              14.88ms     67.22
run(Double_Filter_20_Nulls_50_next_50k_dict)               22.83ms     43.80
run(Double_Filter_20_Nulls_50_next_50k_plain)              14.90ms     67.10
run(Double_Filter_20_Nulls_50_next_100k_dict)              23.05ms     43.38
run(Double_Filter_20_Nulls_50_next_100k_plain)             15.48ms     64.62
----------------------------------------------------------------------------
run(Double_Filter_20_Nulls_70_next_5k_dict)                20.02ms     49.96
run(Double_Filter_20_Nulls_70_next_5k_plain)               14.13ms     70.75
run(Double_Filter_20_Nulls_70_next_10k_dict)               19.44ms     51.44
run(Double_Filter_20_Nulls_70_next_10k_Plain)              12.27ms     81.50
run(Double_Filter_20_Nulls_70_next_20k_dict)               19.40ms     51.54
run(Double_Filter_20_Nulls_70_next_20k_plain)              12.49ms     80.08
run(Double_Filter_20_Nulls_70_next_50k_dict)               18.77ms     53.28
run(Double_Filter_20_Nulls_70_next_50k_plain)              13.24ms     75.53
run(Double_Filter_20_Nulls_70_next_100k_dict)              19.31ms     51.78
run(Double_Filter_20_Nulls_70_next_100k_plain)             12.25ms     81.64
----------------------------------------------------------------------------
run(Double_Filter_20_Nulls_100_next_5k_dict)              520.21us     1.92K
run(Double_Filter_20_Nulls_100_next_5k_plain)             515.48us     1.94K
run(Double_Filter_20_Nulls_100_next_10k_dict)             500.81us     2.00K
run(Double_Filter_20_Nulls_100_next_10k_Plain)            489.78us     2.04K
run(Double_Filter_20_Nulls_100_next_20k_dict)             487.37us     2.05K
run(Double_Filter_20_Nulls_100_next_20k_plain)            512.60us     1.95K
run(Double_Filter_20_Nulls_100_next_50k_dict)             491.70us     2.03K
run(Double_Filter_20_Nulls_100_next_50k_plain)            550.40us     1.82K
run(Double_Filter_20_Nulls_100_next_100k_dict)            538.62us     1.86K
run(Double_Filter_20_Nulls_100_next_100k_plain)           546.61us     1.83K
----------------------------------------------------------------------------
run(Double_Filter_50_Nulls_0_next_5k_dict)                 39.49ms     25.33
run(Double_Filter_50_Nulls_0_next_5k_plain)                26.48ms     37.76
run(Double_Filter_50_Nulls_0_next_10k_dict)                40.15ms     24.91
run(Double_Filter_50_Nulls_0_next_10k_Plain)               27.07ms     36.94
run(Double_Filter_50_Nulls_0_next_20k_dict)                37.66ms     26.55
run(Double_Filter_50_Nulls_0_next_20k_plain)               26.60ms     37.60
run(Double_Filter_50_Nulls_0_next_50k_dict)                51.83ms     19.29
run(Double_Filter_50_Nulls_0_next_50k_plain)               27.42ms     36.47
run(Double_Filter_50_Nulls_0_next_100k_dict)               38.65ms     25.87
run(Double_Filter_50_Nulls_0_next_100k_plain)              28.42ms     35.18
----------------------------------------------------------------------------
run(Double_Filter_50_Nulls_20_next_5k_dict)                44.88ms     22.28
run(Double_Filter_50_Nulls_20_next_5k_plain)               26.71ms     37.44
run(Double_Filter_50_Nulls_20_next_10k_dict)               35.76ms     27.96
run(Double_Filter_50_Nulls_20_next_10k_Plain)              23.87ms     41.89
run(Double_Filter_50_Nulls_20_next_20k_dict)               34.60ms     28.90
run(Double_Filter_50_Nulls_20_next_20k_plain)              24.75ms     40.40
run(Double_Filter_50_Nulls_20_next_50k_dict)               36.69ms     27.25
run(Double_Filter_50_Nulls_20_next_50k_plain)              26.61ms     37.58
run(Double_Filter_50_Nulls_20_next_100k_dict)              37.51ms     26.66
run(Double_Filter_50_Nulls_20_next_100k_plain)             26.76ms     37.36
----------------------------------------------------------------------------
run(Double_Filter_50_Nulls_50_next_5k_dict)                27.65ms     36.17
run(Double_Filter_50_Nulls_50_next_5k_plain)               16.24ms     61.56
run(Double_Filter_50_Nulls_50_next_10k_dict)               26.41ms     37.86
run(Double_Filter_50_Nulls_50_next_10k_Plain)              16.34ms     61.18
run(Double_Filter_50_Nulls_50_next_20k_dict)               27.98ms     35.74
run(Double_Filter_50_Nulls_50_next_20k_plain)              14.34ms     69.75
run(Double_Filter_50_Nulls_50_next_50k_dict)               24.83ms     40.27
run(Double_Filter_50_Nulls_50_next_50k_plain)              15.14ms     66.05
run(Double_Filter_50_Nulls_50_next_100k_dict)              25.19ms     39.69
run(Double_Filter_50_Nulls_50_next_100k_plain)             15.75ms     63.49
----------------------------------------------------------------------------
run(Double_Filter_50_Nulls_70_next_5k_dict)                23.58ms     42.41
run(Double_Filter_50_Nulls_70_next_5k_plain)               13.14ms     76.12
run(Double_Filter_50_Nulls_70_next_10k_dict)               22.59ms     44.27
run(Double_Filter_50_Nulls_70_next_10k_Plain)              13.14ms     76.13
run(Double_Filter_50_Nulls_70_next_20k_dict)               22.50ms     44.45
run(Double_Filter_50_Nulls_70_next_20k_plain)              12.23ms     81.78
run(Double_Filter_50_Nulls_70_next_50k_dict)               20.87ms     47.91
run(Double_Filter_50_Nulls_70_next_50k_plain)              12.86ms     77.76
run(Double_Filter_50_Nulls_70_next_100k_dict)              21.61ms     46.27
run(Double_Filter_50_Nulls_70_next_100k_plain)             12.95ms     77.19
----------------------------------------------------------------------------
run(Double_Filter_50_Nulls_100_next_5k_dict)              559.98us     1.79K
run(Double_Filter_50_Nulls_100_next_5k_plain)             518.65us     1.93K
run(Double_Filter_50_Nulls_100_next_10k_dict)             527.41us     1.90K
run(Double_Filter_50_Nulls_100_next_10k_Plain)            516.49us     1.94K
run(Double_Filter_50_Nulls_100_next_20k_dict)             523.37us     1.91K
run(Double_Filter_50_Nulls_100_next_20k_plain)            579.82us     1.72K
run(Double_Filter_50_Nulls_100_next_50k_dict)             525.49us     1.90K
run(Double_Filter_50_Nulls_100_next_50k_plain)            589.64us     1.70K
run(Double_Filter_50_Nulls_100_next_100k_dict)            581.97us     1.72K
run(Double_Filter_50_Nulls_100_next_100k_plain)           584.73us     1.71K
----------------------------------------------------------------------------
run(Double_Filter_70_Nulls_0_next_5k_dict)                 41.53ms     24.08
run(Double_Filter_70_Nulls_0_next_5k_plain)                30.28ms     33.03
run(Double_Filter_70_Nulls_0_next_10k_dict)                39.76ms     25.15
run(Double_Filter_70_Nulls_0_next_10k_Plain)               31.15ms     32.10
run(Double_Filter_70_Nulls_0_next_20k_dict)                48.57ms     20.59
run(Double_Filter_70_Nulls_0_next_20k_plain)               41.49ms     24.10
run(Double_Filter_70_Nulls_0_next_50k_dict)                42.17ms     23.72
run(Double_Filter_70_Nulls_0_next_50k_plain)               34.40ms     29.07
run(Double_Filter_70_Nulls_0_next_100k_dict)               42.75ms     23.39
run(Double_Filter_70_Nulls_0_next_100k_plain)              30.58ms     32.70
----------------------------------------------------------------------------
run(Double_Filter_70_Nulls_20_next_5k_dict)                41.48ms     24.11
run(Double_Filter_70_Nulls_20_next_5k_plain)               37.12ms     26.94
run(Double_Filter_70_Nulls_20_next_10k_dict)               38.96ms     25.66
run(Double_Filter_70_Nulls_20_next_10k_Plain)              39.01ms     25.64
run(Double_Filter_70_Nulls_20_next_20k_dict)               38.53ms     25.96
run(Double_Filter_70_Nulls_20_next_20k_plain)              30.65ms     32.62
run(Double_Filter_70_Nulls_20_next_50k_dict)               36.69ms     27.26
run(Double_Filter_70_Nulls_20_next_50k_plain)              27.46ms     36.41
run(Double_Filter_70_Nulls_20_next_100k_dict)              43.29ms     23.10
run(Double_Filter_70_Nulls_20_next_100k_plain)             28.34ms     35.29
----------------------------------------------------------------------------
run(Double_Filter_70_Nulls_50_next_5k_dict)                27.22ms     36.73
run(Double_Filter_70_Nulls_50_next_5k_plain)               17.74ms     56.36
run(Double_Filter_70_Nulls_50_next_10k_dict)               25.79ms     38.77
run(Double_Filter_70_Nulls_50_next_10k_Plain)              16.51ms     60.56
run(Double_Filter_70_Nulls_50_next_20k_dict)               26.07ms     38.37
run(Double_Filter_70_Nulls_50_next_20k_plain)              16.40ms     60.96
run(Double_Filter_70_Nulls_50_next_50k_dict)               31.15ms     32.11
run(Double_Filter_70_Nulls_50_next_50k_plain)              16.43ms     60.86
run(Double_Filter_70_Nulls_50_next_100k_dict)              26.90ms     37.17
run(Double_Filter_70_Nulls_50_next_100k_plain)             17.56ms     56.95
----------------------------------------------------------------------------
run(Double_Filter_70_Nulls_70_next_5k_dict)                22.04ms     45.38
run(Double_Filter_70_Nulls_70_next_5k_plain)               14.41ms     69.41
run(Double_Filter_70_Nulls_70_next_10k_dict)               22.59ms     44.26
run(Double_Filter_70_Nulls_70_next_10k_Plain)              13.49ms     74.15
run(Double_Filter_70_Nulls_70_next_20k_dict)               21.62ms     46.26
run(Double_Filter_70_Nulls_70_next_20k_plain)              13.83ms     72.31
run(Double_Filter_70_Nulls_70_next_50k_dict)               24.32ms     41.12
run(Double_Filter_70_Nulls_70_next_50k_plain)              12.97ms     77.08
run(Double_Filter_70_Nulls_70_next_100k_dict)              20.48ms     48.82
run(Double_Filter_70_Nulls_70_next_100k_plain)             13.28ms     75.29
----------------------------------------------------------------------------
run(Double_Filter_70_Nulls_100_next_5k_dict)              532.31us     1.88K
run(Double_Filter_70_Nulls_100_next_5k_plain)             525.19us     1.90K
run(Double_Filter_70_Nulls_100_next_10k_dict)             499.46us     2.00K
run(Double_Filter_70_Nulls_100_next_10k_Plain)            524.79us     1.91K
run(Double_Filter_70_Nulls_100_next_20k_dict)             562.89us     1.78K
run(Double_Filter_70_Nulls_100_next_20k_plain)            503.11us     1.99K
run(Double_Filter_70_Nulls_100_next_50k_dict)             545.93us     1.83K
run(Double_Filter_70_Nulls_100_next_50k_plain)            492.14us     2.03K
run(Double_Filter_70_Nulls_100_next_100k_dict)            542.50us     1.84K
run(Double_Filter_70_Nulls_100_next_100k_plain)           508.18us     1.97K
----------------------------------------------------------------------------
run(Double_Filter_100_Nulls_0_next_5k_dict)                42.20ms     23.70
run(Double_Filter_100_Nulls_0_next_5k_plain)               28.71ms     34.83
run(Double_Filter_100_Nulls_0_next_10k_dict)               39.90ms     25.06
run(Double_Filter_100_Nulls_0_next_10k_Plain)              29.15ms     34.30
run(Double_Filter_100_Nulls_0_next_20k_dict)               39.04ms     25.62
run(Double_Filter_100_Nulls_0_next_20k_plain)              30.05ms     33.28
run(Double_Filter_100_Nulls_0_next_50k_dict)               37.10ms     26.95
run(Double_Filter_100_Nulls_0_next_50k_plain)              34.80ms     28.74
run(Double_Filter_100_Nulls_0_next_100k_dict)              41.12ms     24.32
run(Double_Filter_100_Nulls_0_next_100k_plain)             31.07ms     32.18
----------------------------------------------------------------------------
run(Double_Filter_100_Nulls_20_next_5k_dict)               34.79ms     28.75
run(Double_Filter_100_Nulls_20_next_5k_plain)              25.63ms     39.01
run(Double_Filter_100_Nulls_20_next_10k_dict)              34.52ms     28.97
run(Double_Filter_100_Nulls_20_next_10k_Plain)             28.46ms     35.14
run(Double_Filter_100_Nulls_20_next_20k_dict)              33.51ms     29.84
run(Double_Filter_100_Nulls_20_next_20k_plain)             27.19ms     36.78
run(Double_Filter_100_Nulls_20_next_50k_dict)              36.64ms     27.29
run(Double_Filter_100_Nulls_20_next_50k_plain)             29.55ms     33.84
run(Double_Filter_100_Nulls_20_next_100k_dict)             38.46ms     26.00
run(Double_Filter_100_Nulls_20_next_100k_plain)            30.21ms     33.10
----------------------------------------------------------------------------
run(Double_Filter_100_Nulls_50_next_5k_dict)               23.76ms     42.09
run(Double_Filter_100_Nulls_50_next_5k_plain)              16.19ms     61.76
run(Double_Filter_100_Nulls_50_next_10k_dict)              23.38ms     42.76
run(Double_Filter_100_Nulls_50_next_10k_Plain)             16.12ms     62.05
run(Double_Filter_100_Nulls_50_next_20k_dict)              25.53ms     39.18
run(Double_Filter_100_Nulls_50_next_20k_plain)             16.77ms     59.64
run(Double_Filter_100_Nulls_50_next_50k_dict)              24.23ms     41.26
run(Double_Filter_100_Nulls_50_next_50k_plain)             16.35ms     61.18
run(Double_Filter_100_Nulls_50_next_100k_dict)             27.04ms     36.98
run(Double_Filter_100_Nulls_50_next_100k_plain)            17.02ms     58.75
----------------------------------------------------------------------------
run(Double_Filter_100_Nulls_70_next_5k_dict)               19.17ms     52.15
run(Double_Filter_100_Nulls_70_next_5k_plain)              14.58ms     68.57
run(Double_Filter_100_Nulls_70_next_10k_dict)              22.88ms     43.70
run(Double_Filter_100_Nulls_70_next_10k_Plain)             13.92ms     71.85
run(Double_Filter_100_Nulls_70_next_20k_dict)              22.75ms     43.95
run(Double_Filter_100_Nulls_70_next_20k_plain)             14.73ms     67.88
run(Double_Filter_100_Nulls_70_next_50k_dict)              19.73ms     50.69
run(Double_Filter_100_Nulls_70_next_50k_plain)             12.98ms     77.07
run(Double_Filter_100_Nulls_70_next_100k_dict)             19.56ms     51.13
run(Double_Filter_100_Nulls_70_next_100k_plain)            12.50ms     79.99
----------------------------------------------------------------------------
run(Double_Filter_100_Nulls_100_next_5k_dict)             519.92us     1.92K
run(Double_Filter_100_Nulls_100_next_5k_plain)            525.53us     1.90K
run(Double_Filter_100_Nulls_100_next_10k_dict)            535.95us     1.87K
run(Double_Filter_100_Nulls_100_next_10k_Plain)           561.09us     1.78K
run(Double_Filter_100_Nulls_100_next_20k_dict)            527.62us     1.90K
run(Double_Filter_100_Nulls_100_next_20k_plain)           533.94us     1.87K
run(Double_Filter_100_Nulls_100_next_50k_dict)            522.11us     1.92K
run(Double_Filter_100_Nulls_100_next_50k_plain)           529.04us     1.89K
run(Double_Filter_100_Nulls_100_next_100k_dict)           570.26us     1.75K
run(Double_Filter_100_Nulls_100_next_100k_plain           581.40us     1.72K
----------------------------------------------------------------------------
----------------------------------------------------------------------------
run(Map_Filter_100_Nulls_0_next_5k_dict)                  449.08ms      2.23
run(Map_Filter_100_Nulls_0_next_5k_plain)                 424.40ms      2.36
run(Map_Filter_100_Nulls_0_next_10k_dict)                 435.04ms      2.30
run(Map_Filter_100_Nulls_0_next_10k_Plain)                421.84ms      2.37
run(Map_Filter_100_Nulls_0_next_20k_dict)                 490.99ms      2.04
run(Map_Filter_100_Nulls_0_next_20k_plain)                411.19ms      2.43
run(Map_Filter_100_Nulls_0_next_50k_dict)                 423.53ms      2.36
run(Map_Filter_100_Nulls_0_next_50k_plain)                448.39ms      2.23
run(Map_Filter_100_Nulls_0_next_100k_dict)                430.18ms      2.32
run(Map_Filter_100_Nulls_0_next_100k_plain)               466.14ms      2.15
----------------------------------------------------------------------------
run(Map_Filter_100_Nulls_20_next_5k_dict)                 435.39ms      2.30
run(Map_Filter_100_Nulls_20_next_5k_plain)                451.52ms      2.21
run(Map_Filter_100_Nulls_20_next_10k_dict)                433.89ms      2.30
run(Map_Filter_100_Nulls_20_next_10k_Plain)               447.31ms      2.24
run(Map_Filter_100_Nulls_20_next_20k_dict)                428.55ms      2.33
run(Map_Filter_100_Nulls_20_next_20k_plain)               419.92ms      2.38
run(Map_Filter_100_Nulls_20_next_50k_dict)                470.74ms      2.12
run(Map_Filter_100_Nulls_20_next_50k_plain)               446.65ms      2.24
run(Map_Filter_100_Nulls_20_next_100k_dict)               480.55ms      2.08
run(Map_Filter_100_Nulls_20_next_100k_plain)              441.30ms      2.27
----------------------------------------------------------------------------
run(Map_Filter_100_Nulls_50_next_5k_dict)                 343.65ms      2.91
run(Map_Filter_100_Nulls_50_next_5k_plain)                345.07ms      2.90
run(Map_Filter_100_Nulls_50_next_10k_dict)                341.37ms      2.93
run(Map_Filter_100_Nulls_50_next_10k_Plain)               370.81ms      2.70
run(Map_Filter_100_Nulls_50_next_20k_dict)                357.00ms      2.80
run(Map_Filter_100_Nulls_50_next_20k_plain)               341.98ms      2.92
run(Map_Filter_100_Nulls_50_next_50k_dict)                350.33ms      2.85
run(Map_Filter_100_Nulls_50_next_50k_plain)               360.85ms      2.77
run(Map_Filter_100_Nulls_50_next_100k_dict)               374.14ms      2.67
run(Map_Filter_100_Nulls_50_next_100k_plain)              349.25ms      2.86
----------------------------------------------------------------------------
run(Map_Filter_100_Nulls_70_next_5k_dict)                 248.20ms      4.03
run(Map_Filter_100_Nulls_70_next_5k_plain)                241.43ms      4.14
run(Map_Filter_100_Nulls_70_next_10k_dict)                257.56ms      3.88
run(Map_Filter_100_Nulls_70_next_10k_Plain)               241.40ms      4.14
run(Map_Filter_100_Nulls_70_next_20k_dict)                249.02ms      4.02
run(Map_Filter_100_Nulls_70_next_20k_plain)               237.84ms      4.20
run(Map_Filter_100_Nulls_70_next_50k_dict)                246.21ms      4.06
run(Map_Filter_100_Nulls_70_next_50k_plain)               251.70ms      3.97
run(Map_Filter_100_Nulls_70_next_100k_dict)               261.97ms      3.82
run(Map_Filter_100_Nulls_70_next_100k_plain)              262.31ms      3.81
----------------------------------------------------------------------------
run(Map_Filter_100_Nulls_100_next_5k_dict)                 59.85ms     16.71
run(Map_Filter_100_Nulls_100_next_5k_plain)                60.60ms     16.50
run(Map_Filter_100_Nulls_100_next_10k_dict)                59.22ms     16.89
run(Map_Filter_100_Nulls_100_next_10k_Plain)               57.78ms     17.31
run(Map_Filter_100_Nulls_100_next_20k_dict)                57.73ms     17.32
run(Map_Filter_100_Nulls_100_next_20k_plain)               62.77ms     15.93
run(Map_Filter_100_Nulls_100_next_50k_dict)                59.70ms     16.75
run(Map_Filter_100_Nulls_100_next_50k_plain)               59.44ms     16.82
run(Map_Filter_100_Nulls_100_next_100k_dict)               60.29ms     16.59
run(Map_Filter_100_Nulls_100_next_100k_plain)              61.46ms     16.27
----------------------------------------------------------------------------
----------------------------------------------------------------------------
run(List_Filter_100_Nulls_0_next_5k_dict)                 483.27ms      2.07
run(List_Filter_100_Nulls_0_next_5k_plain)                468.06ms      2.14
run(List_Filter_100_Nulls_0_next_10k_dict)                660.44ms      1.51
run(List_Filter_100_Nulls_0_next_10k_Plain)               489.41ms      2.04
run(List_Filter_100_Nulls_0_next_20k_dict)                507.95ms      1.97
run(List_Filter_100_Nulls_0_next_20k_plain)               493.37ms      2.03
run(List_Filter_100_Nulls_0_next_50k_dict)                514.33ms      1.94
run(List_Filter_100_Nulls_0_next_50k_plain)               528.36ms      1.89
run(List_Filter_100_Nulls_0_next_100k_dict)               481.74ms      2.08
run(List_Filter_100_Nulls_0_next_100k_plain)              473.79ms      2.11
----------------------------------------------------------------------------
run(List_Filter_100_Nulls_20_next_5k_dict)                459.09ms      2.18
run(List_Filter_100_Nulls_20_next_5k_plain)               442.37ms      2.26
run(List_Filter_100_Nulls_20_next_10k_dict)               487.41ms      2.05
run(List_Filter_100_Nulls_20_next_10k_Plain)              470.98ms      2.12
run(List_Filter_100_Nulls_20_next_20k_dict)               482.92ms      2.07
run(List_Filter_100_Nulls_20_next_20k_plain)              440.49ms      2.27
run(List_Filter_100_Nulls_20_next_50k_dict)               467.28ms      2.14
run(List_Filter_100_Nulls_20_next_50k_plain)              454.34ms      2.20
run(List_Filter_100_Nulls_20_next_100k_dict)              462.37ms      2.16
run(List_Filter_100_Nulls_20_next_100k_plain)             491.07ms      2.04
----------------------------------------------------------------------------
run(List_Filter_100_Nulls_50_next_5k_dict)                358.78ms      2.79
run(List_Filter_100_Nulls_50_next_5k_plain)               371.98ms      2.69
run(List_Filter_100_Nulls_50_next_10k_dict)               360.20ms      2.78
run(List_Filter_100_Nulls_50_next_10k_Plain)              356.06ms      2.81
run(List_Filter_100_Nulls_50_next_20k_dict)               361.11ms      2.77
run(List_Filter_100_Nulls_50_next_20k_plain)              416.14ms      2.40
run(List_Filter_100_Nulls_50_next_50k_dict)               366.68ms      2.73
run(List_Filter_100_Nulls_50_next_50k_plain)              362.87ms      2.76
run(List_Filter_100_Nulls_50_next_100k_dict)              403.01ms      2.48
run(List_Filter_100_Nulls_50_next_100k_plain)             362.46ms      2.76
----------------------------------------------------------------------------
run(List_Filter_100_Nulls_70_next_5k_dict)                251.62ms      3.97
run(List_Filter_100_Nulls_70_next_5k_plain)               245.23ms      4.08
run(List_Filter_100_Nulls_70_next_10k_dict)               245.22ms      4.08
run(List_Filter_100_Nulls_70_next_10k_Plain)              242.18ms      4.13
run(List_Filter_100_Nulls_70_next_20k_dict)               307.15ms      3.26
run(List_Filter_100_Nulls_70_next_20k_plain)              265.67ms      3.76
run(List_Filter_100_Nulls_70_next_50k_dict)               262.71ms      3.81
run(List_Filter_100_Nulls_70_next_50k_plain)              242.98ms      4.12
run(List_Filter_100_Nulls_70_next_100k_dict)              253.17ms      3.95
run(List_Filter_100_Nulls_70_next_100k_plain)             264.51ms      3.78
----------------------------------------------------------------------------
run(List_Filter_100_Nulls_100_next_5k_dict)                60.55ms     16.52
run(List_Filter_100_Nulls_100_next_5k_plain)               68.59ms     14.58
run(List_Filter_100_Nulls_100_next_10k_dict)               60.19ms     16.61
run(List_Filter_100_Nulls_100_next_10k_Plain)              59.63ms     16.77
run(List_Filter_100_Nulls_100_next_20k_dict)               58.64ms     17.05
run(List_Filter_100_Nulls_100_next_20k_plain)              57.89ms     17.28
run(List_Filter_100_Nulls_100_next_50k_dict)               58.19ms     17.19
run(List_Filter_100_Nulls_100_next_50k_plain)              57.99ms     17.25
run(List_Filter_100_Nulls_100_next_100k_dict)              58.45ms     17.11
run(List_Filter_100_Nulls_100_next_100k_plain)             79.83ms     12.53
----------------------------------------------------------------------------
----------------------------------------------------------------------------
*/
