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

#include "glog/logging.h"
#include "velox/exec/PrefixSort.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace {

class TestCase {
 public:
  TestCase(
      memory::MemoryPool* pool,
      const std::string& testName,
      size_t numRows,
      const RowTypePtr& rowType,
      int numKeys)
      : testName_(testName), numRows_(numRows), pool_(pool), rowType_(rowType) {
    // Initialize a RowContainer that holds fuzzed rows to be sorted.
    std::vector<TypePtr> keyTypes;
    std::vector<TypePtr> dependentTypes;
    for (auto i = 0; i < rowType->size(); ++i) {
      if (i < numKeys) {
        keyTypes.push_back(rowType->childAt(i));
      } else {
        dependentTypes.push_back(rowType->childAt(i));
      }
    }
    data_ = std::make_unique<RowContainer>(keyTypes, dependentTypes, pool);
    RowVectorPtr sortedRows = fuzzRows(numRows, numKeys);
    storeRows(numRows, sortedRows);

    // Initialize CompareFlags, it could be same for each key in benchmark.
    for (int i = 0; i < numKeys; ++i) {
      compareFlags_.push_back(
          {true, true, false, CompareFlags::NullHandlingMode::kNullAsValue});
    }
  };

  const std::string& testName() const {
    return testName_;
  }

  size_t numRows() const {
    return numRows_;
  }

  const std::vector<char*>& rows() const {
    return rows_;
  }

  RowContainer* rowContainer() const {
    return data_.get();
  }

  const std::vector<CompareFlags>& compareFlags() const {
    return compareFlags_;
  }

 private:
  // Store data into the RowContainer to mock the behavior of SortBuffer.
  void storeRows(int numRows, const RowVectorPtr& data) {
    rows_.resize(numRows);
    for (auto row = 0; row < numRows; ++row) {
      rows_[row] = rowContainer()->newRow();
    }
    for (auto column = 0; column < data->childrenSize(); ++column) {
      DecodedVector decoded(*data->childAt(column));
      for (int i = 0; i < numRows; ++i) {
        char* row = rows_[i];
        rowContainer()->store(decoded, i, row, column);
      }
    }
  }

  RowVectorPtr fuzzRows(size_t numRows, int numKeys) {
    VectorFuzzer fuzzer({.vectorSize = numRows}, pool_);
    VectorFuzzer fuzzerWithNulls(
        {.vectorSize = numRows, .nullRatio = 0.7}, pool_);
    std::vector<VectorPtr> children;

    // Fuzz keys: for front keys (column 0 to numKeys -2) use high
    // nullRatio to enforce all columns to be compared.
    {
      for (auto i = 0; i < numKeys - 1; ++i) {
        children.push_back(fuzzerWithNulls.fuzz(rowType_->childAt(i)));
      }
      children.push_back(fuzzer.fuzz(rowType_->childAt(numKeys - 1)));
    }
    // Fuzz payload
    {
      for (auto i = numKeys; i < rowType_->size(); ++i) {
        children.push_back(fuzzer.fuzz(rowType_->childAt(i)));
      }
    }
    return std::make_shared<RowVector>(
        pool_, rowType_, nullptr, numRows, std::move(children));
  }

  const std::string testName_;
  const size_t numRows_;
  // Rows address stored in RowContainer
  std::vector<char*> rows_;
  std::unique_ptr<RowContainer> data_;
  memory::MemoryPool* const pool_;
  const RowTypePtr rowType_;
  std::vector<CompareFlags> compareFlags_;
};

// You could config threshold, e.i. 0, to test prefix-sort for small
// dateset.
static const common::PrefixSortConfig kDefaultSortConfig(1024, 100);

// For small dataset, in some test environments, if std-sort is defined in the
// benchmark file, the test results may be strangely regressed. When the
// threshold is particularly large, PrefixSort is actually std-sort, hence, we
// can use this as std-sort benchmark base.
static const common::PrefixSortConfig kStdSortConfig(
    1024,
    std::numeric_limits<int>::max());

class PrefixSortBenchmark {
 public:
  PrefixSortBenchmark(memory::MemoryPool* pool) : pool_(pool) {}

  void runPrefixSort(
      const std::vector<char*>& rows,
      RowContainer* rowContainer,
      const std::vector<CompareFlags>& compareFlags) {
    // Copy rows to avoid sort rows already sorted.
    std::vector<char*> sortedRows = rows;
    PrefixSort::sort(
        sortedRows, pool_, rowContainer, compareFlags, kDefaultSortConfig);
  }

  void runStdSort(
      const std::vector<char*>& rows,
      RowContainer* rowContainer,
      const std::vector<CompareFlags>& compareFlags) {
    std::vector<char*> sortedRows = rows;
    PrefixSort::sort(
        sortedRows, pool_, rowContainer, compareFlags, kStdSortConfig);
  }

  // Add benchmark manually to avoid writing a lot of BENCHMARK.
  void addBenchmark(
      const std::string& testName,
      size_t numRows,
      const RowTypePtr& rowType,
      int iterations,
      int numKeys,
      bool testStdSort) {
    auto testCase =
        std::make_unique<TestCase>(pool_, testName, numRows, rowType, numKeys);
    // Add benchmarks for std-sort and prefix-sort.
    {
      if (testStdSort) {
        folly::addBenchmark(
            __FILE__,
            "StdSort_" + testCase->testName(),
            [rows = testCase->rows(),
             container = testCase->rowContainer(),
             sortFlags = testCase->compareFlags(),
             iterations = iterations,
             this]() {
              for (auto i = 0; i < iterations; ++i) {
                runStdSort(rows, container, sortFlags);
              }
              return rows.size() * iterations;
            });
      }
      folly::addBenchmark(
          __FILE__,
          testStdSort ? "%PrefixSort" : "PrefixSort_" + testCase->testName(),
          [rows = testCase->rows(),
           container = testCase->rowContainer(),
           sortFlags = testCase->compareFlags(),
           iterations = iterations,
           this]() {
            for (auto i = 0; i < iterations; ++i) {
              runPrefixSort(rows, container, sortFlags);
            }
            return rows.size() * iterations;
          });
    }
    testCases_.push_back(std::move(testCase));
  }

  void benchmark(
      const std::string& prefix,
      const std::string& keyName,
      const std::vector<vector_size_t>& batchSizes,
      const std::vector<RowTypePtr>& rowTypes,
      const std::vector<int>& numKeys,
      int32_t iterations,
      bool testStdSort = true) {
    for (auto batchSize : batchSizes) {
      for (auto i = 0; i < rowTypes.size(); ++i) {
        const auto name = fmt::format(
            "{}_{}_{}_{}k", prefix, numKeys[i], keyName, batchSize / 1000.0);
        addBenchmark(
            name, batchSize, rowTypes[i], iterations, numKeys[i], testStdSort);
      }
    }
  }

  std::vector<RowTypePtr> bigintRowTypes(bool noPayload) {
    if (noPayload) {
      return {
          ROW({BIGINT()}),
          ROW({BIGINT(), BIGINT()}),
          ROW({BIGINT(), BIGINT(), BIGINT()}),
          ROW({BIGINT(), BIGINT(), BIGINT(), BIGINT()}),
      };
    } else {
      return {
          ROW({BIGINT(), VARCHAR(), VARCHAR()}),
          ROW({BIGINT(), BIGINT(), VARCHAR(), VARCHAR()}),
          ROW({BIGINT(), BIGINT(), BIGINT(), VARCHAR(), VARCHAR()}),
          ROW({BIGINT(), BIGINT(), BIGINT(), BIGINT(), VARCHAR(), VARCHAR()}),
      };
    }
  }

  void bigint(
      bool noPayload,
      int numIterations,
      const std::vector<vector_size_t>& batchSizes) {
    std::vector<RowTypePtr> rowTypes = bigintRowTypes(noPayload);
    std::vector<int> numKeys = {1, 2, 3, 4};
    benchmark(
        noPayload ? "no-payload" : "payload",
        "bigint",
        batchSizes,
        rowTypes,
        numKeys,
        numIterations);
  }

  void smallBigint() {
    // For small dateset, iterations need to be large enough to ensure that the
    // benchmark runs for enough time.
    const auto iterations = 100'000;
    const std::vector<vector_size_t> batchSizes = {10, 50, 100, 500};
    bigint(true, iterations, batchSizes);
  }

  void smallBigintWithPayload() {
    const auto iterations = 100'000;
    const std::vector<vector_size_t> batchSizes = {10, 50, 100, 500};
    bigint(false, iterations, batchSizes);
  }

  void largeBigint() {
    const auto iterations = 10;
    const std::vector<vector_size_t> batchSizes = {
        1'000, 10'000, 100'000, 1'000'000};
    bigint(true, iterations, batchSizes);
  }

  void largeBigintWithPayloads() {
    const auto iterations = 10;
    const std::vector<vector_size_t> batchSizes = {
        1'000, 10'000, 100'000, 1'000'000};
    bigint(false, iterations, batchSizes);
  }

  void largeVarchar() {
    const auto iterations = 10;
    const std::vector<vector_size_t> batchSizes = {
        1'000, 10'000, 100'000, 1'000'000};
    std::vector<RowTypePtr> rowTypes = {
        ROW({VARCHAR()}),
        ROW({VARCHAR(), VARCHAR()}),
        ROW({VARCHAR(), VARCHAR(), VARCHAR()}),
        ROW({VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR()}),
    };
    std::vector<int> numKeys = {1, 2, 3, 4};
    benchmark(
        "no-payloads", "varchar", batchSizes, rowTypes, numKeys, iterations);
  }

 private:
  std::vector<std::unique_ptr<TestCase>> testCases_;
  memory::MemoryPool* pool_;
};
} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);

  memory::MemoryManager::initialize({});
  auto rootPool = memory::memoryManager()->addRootPool();
  auto leafPool = rootPool->addLeafChild("leaf");

  PrefixSortBenchmark bm(leafPool.get());

  bm.smallBigint();
  bm.largeBigint();
  bm.largeBigintWithPayloads();
  bm.smallBigintWithPayload();
  bm.largeVarchar();
  folly::runBenchmarks();

  return 0;
}
