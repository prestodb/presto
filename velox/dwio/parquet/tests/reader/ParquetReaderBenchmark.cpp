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

#include "velox/dwio/parquet/tests/reader/ParquetReaderBenchmark.h"

using namespace facebook::velox;
using namespace facebook::velox::dwio;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::parquet;
using namespace facebook::velox::test;

namespace facebook::velox::parquet::test {
void ParquetReaderBenchmark::writeToFile(
    const std::vector<RowVectorPtr>& batches,
    bool /*forRowGroupSkip*/) {
  for (auto& batch : batches) {
    writer_->write(batch);
  }
  writer_->flush();
  writer_->close();
}

FilterSpec ParquetReaderBenchmark::createFilterSpec(
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

std::shared_ptr<ScanSpec> ParquetReaderBenchmark::createScanSpec(
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

std::unique_ptr<RowReader> ParquetReaderBenchmark::createReader(
    std::shared_ptr<ScanSpec> scanSpec,
    const RowTypePtr& rowType) {
  dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  auto input = std::make_unique<BufferedInput>(
      std::make_shared<LocalReadFile>(fileFolder_->getPath() + "/" + fileName_),
      readerOpts.memoryPool());

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
int ParquetReaderBenchmark::read(
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

void ParquetReaderBenchmark::readSingleColumn(
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

void run(
    uint32_t,
    const std::string& columnName,
    const TypePtr& type,
    float filterRateX100,
    uint8_t nullsRateX100,
    uint32_t nextSize,
    bool disableDictionary) {
  RowTypePtr rowType = ROW({columnName}, {type});
  facebook::velox::parquet::test::ParquetReaderBenchmark benchmark(
      disableDictionary, rowType);
  BIGINT()->toString();
  benchmark.readSingleColumn(
      columnName, type, 0, filterRateX100, nullsRateX100, nextSize);
}

} // namespace facebook::velox::parquet::test
