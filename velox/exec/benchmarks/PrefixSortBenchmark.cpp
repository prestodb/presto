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
#include "velox/exec/benchmarks/OrderByBenchmarkUtil.h"

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
    RowVectorPtr sortedRows =
        OrderByBenchmarkUtil::fuzzRows(rowType, numRows, pool_);
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
      rowContainer()->store(
          decoded, folly::Range(rows_.data(), numRows), column);
    }
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
static const common::PrefixSortConfig kDefaultSortConfig(1024, 100, 50);

// For small dataset, in some test environments, if std-sort is defined in the
// benchmark file, the test results may be strangely regressed. When the
// threshold is particularly large, PrefixSort is actually std-sort, hence, we
// can use this as std-sort benchmark base.
static const common::PrefixSortConfig
    kStdSortConfig(1024, std::numeric_limits<int>::max(), 50);

class PrefixSortBenchmark {
 public:
  PrefixSortBenchmark(memory::MemoryPool* pool) : pool_(pool) {}

  void runPrefixSort(
      const std::vector<char*>& rows,
      RowContainer* rowContainer,
      const std::vector<CompareFlags>& compareFlags) {
    // Copy rows to avoid sort rows already sorted.
    auto sortedRows = std::vector<char*, memory::StlAllocator<char*>>(
        rows.begin(), rows.end(), *pool_);
    PrefixSort::sort(
        rowContainer, compareFlags, kDefaultSortConfig, pool_, sortedRows);
  }

  void runStdSort(
      const std::vector<char*>& rows,
      RowContainer* rowContainer,
      const std::vector<CompareFlags>& compareFlags) {
    auto sortedRows = std::vector<char*, memory::StlAllocator<char*>>(
        rows.begin(), rows.end(), *pool_);
    PrefixSort::sort(
        rowContainer, compareFlags, kStdSortConfig, pool_, sortedRows);
  }

  // Add benchmark manually to avoid writing a lot of BENCHMARK.
  void addBenchmark(
      const std::string& testName,
      size_t numRows,
      const RowTypePtr& rowType,
      int iterations,
      int numKeys) {
    auto testCase =
        std::make_unique<TestCase>(pool_, testName, numRows, rowType, numKeys);
    // Add benchmarks for std-sort and prefix-sort.
    {
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
      folly::addBenchmark(
          __FILE__,
          "%PrefixSort",
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

  OrderByBenchmarkUtil::addBenchmarks([&](const std::string& testName,
                                          size_t numRows,
                                          const RowTypePtr& rowType,
                                          int iterations,
                                          int numKeys) {
    bm.addBenchmark(testName, numRows, rowType, iterations, numKeys);
  });

  folly::runBenchmarks();
  return 0;
}
