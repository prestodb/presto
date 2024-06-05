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

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "velox/dwio/common/tests/utils/DataFiles.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/exec/RowContainer.h"
#include "velox/external/timsort/TimSort.hpp"
#include "velox/type/StringView.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/SimpleVector.h"
#include "velox/vector/tests/VectorTestUtils.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace facebook::velox::test {
namespace {
std::string getExampleFilePath(const std::string& fileName) {
  return test::getDataFilePath(
      "velox/dwio/parquet/tests/reader", "../examples/" + fileName);
}

dwio::common::RowReaderOptions getReaderOpts(
    const RowTypePtr& rowType,
    bool fileColumnNamesReadAsLowerCase = false) {
  dwio::common::RowReaderOptions rowReaderOpts;
  rowReaderOpts.select(
      std::make_shared<facebook::velox::dwio::common::ColumnSelector>(
          rowType, rowType->names(), nullptr, fileColumnNamesReadAsLowerCase));
  return rowReaderOpts;
}

std::shared_ptr<velox::common::ScanSpec> makeScanSpec(
    const RowTypePtr& rowType) {
  auto scanSpec = std::make_shared<velox::common::ScanSpec>("");
  scanSpec->addAllChildFields(*rowType);
  return scanSpec;
}

facebook::velox::parquet::ParquetReader createReader(
    const std::string& path,
    const facebook::velox::dwio::common::ReaderOptions& opts) {
  return facebook::velox::parquet::ParquetReader(
      std::make_unique<facebook::velox::dwio::common::BufferedInput>(
          std::make_shared<LocalReadFile>(path), opts.memoryPool()),
      opts);
}

std::vector<std::optional<StringView>> getDataFromFile() {
  const std::string sample(getExampleFilePath("str_sort.parquet"));
  auto rowType = ROW({"query_sig", "result_sig"}, {VARCHAR(), VARCHAR()});
  auto pool = memory::memoryManager()->addLeafPool();
  facebook::velox::dwio::common::ReaderOptions readerOptions{pool.get()};
  facebook::velox::parquet::ParquetReader reader =
      createReader(sample, readerOptions);
  auto rowReaderOpts = getReaderOpts(rowType);
  auto scanSpec = makeScanSpec(rowType);
  rowReaderOpts.setScanSpec(scanSpec);
  auto rowReader = reader.createRowReader(rowReaderOpts);
  auto data = BaseVector::create(rowType, 50000, pool.get());
  rowReader->next(50000, data);
  auto querySigCol =
      data->as<RowVector>()->childAt(0)->asFlatVector<StringView>();
  auto resSigCol =
      data->as<RowVector>()->childAt(1)->asFlatVector<StringView>();
  std::vector<std::optional<StringView>> stdVector(querySigCol->size());
  for (int i = 0; i < querySigCol->size(); i++) {
    auto merge =
        querySigCol->valueAt(i).getString() + resSigCol->valueAt(i).getString();
    stdVector[i] = StringView(merge);
  }
  return stdVector;
}

std::vector<char*> store(
    velox::exec::RowContainer& rowContainer,
    DecodedVector& decodedVector,
    vector_size_t size) {
  std::vector<char*> rows(size);
  for (size_t row = 0; row < size; ++row) {
    rows[row] = rowContainer.newRow();
    rowContainer.store(decodedVector, row, rows[row], 0);
  }
  return rows;
}

template <typename T>
void rowContainerStdSortBenchmark(uint32_t iterations, size_t cardinality) {
  folly::BenchmarkSuspender suspender;
  auto pool = memory::memoryManager()->addLeafPool();
  VectorMaker vectorMaker(pool.get());

  for (size_t k = 0; k < iterations; ++k) {
    // Use std::nullopt for seed to generate unpredictable pseudo-random for
    // benchmark.
    auto data =
        genTestData<T>(cardinality, CppToType<T>::create(), true, false, false);
    auto vector =
        vectorMaker.encodedVector<T>(VectorEncoding::Simple::FLAT, data.data());
    DecodedVector decoded(*vector);
    // Create row container.
    std::vector<TypePtr> types{vector->type()};
    // Store the vector in the rowContainer.
    auto rowContainer =
        std::make_unique<velox::exec::RowContainer>(types, pool.get());
    int size = data.data().size();
    auto rows = store(*rowContainer, decoded, size);
    suspender.dismiss();
    std::sort(
        rows.begin(), rows.end(), [&](const char* left, const char* right) {
          return rowContainer->compareRows(left, right) < 0;
        });
    suspender.rehire();
  }
}

template <typename T>
void rowContainerTimSortBenchmark(uint32_t iterations, size_t cardinality) {
  folly::BenchmarkSuspender suspender;
  auto pool = memory::memoryManager()->addLeafPool();
  VectorMaker vectorMaker(pool.get());

  for (size_t k = 0; k < iterations; ++k) {
    auto data =
        genTestData<T>(cardinality, CppToType<T>::create(), true, false, false);
    auto vector =
        vectorMaker.encodedVector<T>(VectorEncoding::Simple::FLAT, data.data());
    DecodedVector decoded(*vector);
    // Create row container.
    std::vector<TypePtr> types{vector->type()};
    // Store the vector in the rowContainer.
    auto rowContainer =
        std::make_unique<velox::exec::RowContainer>(types, pool.get());
    int size = vector->size();
    auto rows = store(*rowContainer, decoded, size);
    suspender.dismiss();
    gfx::timsort(
        rows.begin(), rows.end(), [&](const char* left, const char* right) {
          return rowContainer->compareRows(left, right) < 0;
        });
    suspender.rehire();
  }
}

void BM_Int64_stdSort(uint32_t iterations, size_t cardinality) {
  rowContainerStdSortBenchmark<int64_t>(iterations, cardinality);
}

void BM_Int64_timSort(uint32_t iterations, size_t cardinality) {
  rowContainerTimSortBenchmark<int64_t>(iterations, cardinality);
}

void BM_STR_stdSort(uint32_t iterations) {
  folly::BenchmarkSuspender suspender;
  auto pool = memory::memoryManager()->addLeafPool();
  VectorMaker vectorMaker(pool.get());
  auto data = getDataFromFile();
  auto vector =
      vectorMaker.encodedVector<StringView>(VectorEncoding::Simple::FLAT, data);
  DecodedVector decoded(*vector);
  // Create row container.
  std::vector<TypePtr> types{vector->type()};
  // Store the vector in the rowContainer.
  auto rowContainer =
      std::make_unique<velox::exec::RowContainer>(types, pool.get());
  int size = data.size();
  auto rows = store(*rowContainer, decoded, size);
  for (size_t k = 0; k < iterations; ++k) {
    suspender.dismiss();
    std::sort(
        rows.begin(), rows.end(), [&](const char* left, const char* right) {
          return rowContainer->compareRows(left, right) < 0;
        });
    suspender.rehire();
  }
}

void BM_STR_timSort(uint32_t iterations) {
  folly::BenchmarkSuspender suspender;
  auto pool = memory::memoryManager()->addLeafPool();
  VectorMaker vectorMaker(pool.get());
  auto data = getDataFromFile();
  auto vector =
      vectorMaker.encodedVector<StringView>(VectorEncoding::Simple::FLAT, data);
  DecodedVector decoded(*vector);
  // Create row container.
  std::vector<TypePtr> types{vector->type()};
  // Store the vector in the rowContainer.
  auto rowContainer =
      std::make_unique<velox::exec::RowContainer>(types, pool.get());
  int size = vector->size();
  auto rows = store(*rowContainer, decoded, size);
  for (size_t k = 0; k < iterations; ++k) {
    suspender.dismiss();
    gfx::timsort(
        rows.begin(), rows.end(), [&](const char* left, const char* right) {
          return rowContainer->compareRows(left, right) < 0;
        });
    suspender.rehire();
  }
}
} // namespace

BENCHMARK_NAMED_PARAM(BM_Int64_stdSort, 100k_uni_noseq, 100000);
BENCHMARK_RELATIVE_NAMED_PARAM(BM_Int64_timSort, 100k_uni_noseq, 100000);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(BM_Int64_stdSort, 10k_uni_noseq, 10000);
BENCHMARK_RELATIVE_NAMED_PARAM(BM_Int64_timSort, 10k_uni_noseq, 10000);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(BM_Int64_stdSort, 1k_uni_noseq, 1000);
BENCHMARK_RELATIVE_NAMED_PARAM(BM_Int64_timSort, 1k_uni_noseq, 1000);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(BM_STR_stdSort, RealWorldData_stdSort);
BENCHMARK_RELATIVE_NAMED_PARAM(BM_STR_timSort, RealWorldData_timSort);
BENCHMARK_DRAW_LINE();
} // namespace facebook::velox::test

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  facebook::velox::memory::MemoryManager::initialize({});
  folly::runBenchmarks();
  return 0;
}
