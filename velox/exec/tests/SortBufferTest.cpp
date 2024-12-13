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

#include "velox/exec/SortBuffer.h"
#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/type/Type.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox;
using namespace facebook::velox::memory;

namespace facebook::velox::functions::test {
namespace {
// Class to write runtime stats in the tests to the stats container.
class TestRuntimeStatWriter : public BaseRuntimeStatWriter {
 public:
  explicit TestRuntimeStatWriter(
      std::unordered_map<std::string, RuntimeMetric>& stats)
      : stats_{stats} {}

  void addRuntimeStat(const std::string& name, const RuntimeCounter& value)
      override {
    addOperatorRuntimeStats(name, value, stats_);
  }

 private:
  std::unordered_map<std::string, RuntimeMetric>& stats_;
};
} // namespace

class SortBufferTest : public OperatorTestBase,
                       public testing::WithParamInterface<bool> {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
    filesystems::registerLocalFileSystem();
    rng_.seed(123);
    statWriter_ = std::make_unique<TestRuntimeStatWriter>(stats_);
    setThreadLocalRunTimeStatWriter(statWriter_.get());
  }

  void TearDown() override {
    pool_.reset();
    rootPool_.reset();
    OperatorTestBase::TearDown();
  }

  common::SpillConfig getSpillConfig(
      const std::string& spillDir,
      bool enableSpillPrefixSort = true) const {
    std::optional<common::PrefixSortConfig> spillPrefixSortConfig =
        enableSpillPrefixSort
        ? std::optional<common::PrefixSortConfig>(prefixSortConfig_)
        : std::nullopt;
    return common::SpillConfig(
        [spillDir]() -> const std::string& { return spillDir; },
        [&](uint64_t) {},
        "0.0.0",
        0,
        0,
        1 << 20,
        executor_.get(),
        5,
        10,
        0,
        0,
        0,
        0,
        0,
        "none",
        spillPrefixSortConfig);
  }

  const bool enableSpillPrefixSort_{GetParam()};
  const velox::common::PrefixSortConfig prefixSortConfig_ =
      velox::common::PrefixSortConfig{
          std::numeric_limits<uint32_t>::max(),
          GetParam() ? 8 : std::numeric_limits<uint32_t>::max(),
          12};
  const std::optional<common::PrefixSortConfig> spillPrefixSortConfig_ =
      enableSpillPrefixSort_
      ? std::optional<common::PrefixSortConfig>(prefixSortConfig_)
      : std::nullopt;

  const RowTypePtr inputType_ = ROW(
      {{"c0", BIGINT()},
       {"c1", INTEGER()},
       {"c2", SMALLINT()},
       {"c3", REAL()},
       {"c4", DOUBLE()},
       {"c5", VARCHAR()}});
  // Specifies the sort columns ["c4", "c1"].
  std::vector<column_index_t> sortColumnIndices_{4, 1};
  std::vector<CompareFlags> sortCompareFlags_{
      {true, true, false, CompareFlags::NullHandlingMode::kNullAsValue},
      {true, true, false, CompareFlags::NullHandlingMode::kNullAsValue}};

  const std::shared_ptr<folly::Executor> executor_{
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency())};

  tsan_atomic<bool> nonReclaimableSection_{false};
  folly::Random::DefaultGenerator rng_;
  std::unordered_map<std::string, RuntimeMetric> stats_;
  std::unique_ptr<TestRuntimeStatWriter> statWriter_;
};

TEST_P(SortBufferTest, singleKey) {
  const RowVectorPtr data = makeRowVector(
      {makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6, 8, 10, 12, 15}),
       makeFlatVector<int32_t>(
           {17, 16, 15, 14, 13, 10, 8, 7, 4, 3}), // sorted column
       makeFlatVector<int16_t>({1, 2, 3, 4, 5, 6, 8, 10, 12, 15}),
       makeFlatVector<float>(
           {1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 10.1}),
       makeFlatVector<double>(
           {1.1, 2.2, 2.2, 5.5, 5.5, 6.6, 7.7, 8.8, 9.9, 10.1}),
       makeFlatVector<std::string>(
           {"hello",
            "world",
            "today",
            "is",
            "great",
            "hello",
            "world",
            "is",
            "great",
            "today"})});

  struct {
    std::vector<CompareFlags> sortCompareFlags;
    std::vector<int32_t> expectedResult;

    std::string debugString() const {
      const std::string expectedResultStr = folly::join(",", expectedResult);
      std::stringstream sortCompareFlagsStr;
      for (const auto sortCompareFlag : sortCompareFlags) {
        sortCompareFlagsStr << sortCompareFlag.toString();
      }
      return fmt::format(
          "sortCompareFlags:{}, expectedResult:{}",
          sortCompareFlagsStr.str(),
          expectedResultStr);
    }
  } testSettings[] = {
      {{{true,
         true,
         false,
         CompareFlags::NullHandlingMode::kNullAsValue}}, // Ascending
       {3, 4, 7, 8, 10, 13, 14, 15, 16, 17}},
      {{{true,
         false,
         false,
         CompareFlags::NullHandlingMode::kNullAsValue}}, // Descending
       {17, 16, 15, 14, 13, 10, 8, 7, 4, 3}}};

  // Specifies the sort columns ["c1"].
  sortColumnIndices_ = {1};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto sortBuffer = std::make_unique<SortBuffer>(
        inputType_,
        sortColumnIndices_,
        testData.sortCompareFlags,
        pool_.get(),
        &nonReclaimableSection_,
        prefixSortConfig_);

    sortBuffer->addInput(data);
    sortBuffer->noMoreInput();
    auto output = sortBuffer->getOutput(10000);
    ASSERT_EQ(output->size(), 10);
    int resultIndex = 0;
    for (int expectedValue : testData.expectedResult) {
      ASSERT_EQ(
          output->childAt(1)->asFlatVector<int32_t>()->valueAt(resultIndex++),
          expectedValue);
    }
    if (GetParam()) {
      ASSERT_EQ(
          stats_.at(PrefixSort::kNumPrefixSortKeys).sum,
          sortColumnIndices_.size());
      ASSERT_EQ(
          stats_.at(PrefixSort::kNumPrefixSortKeys).max,
          sortColumnIndices_.size());
      ASSERT_EQ(
          stats_.at(PrefixSort::kNumPrefixSortKeys).min,
          sortColumnIndices_.size());
    } else {
      ASSERT_EQ(stats_.count(PrefixSort::kNumPrefixSortKeys), 0);
    }
    stats_.clear();
  }
}

TEST_P(SortBufferTest, multipleKeys) {
  auto sortBuffer = std::make_unique<SortBuffer>(
      inputType_,
      sortColumnIndices_,
      sortCompareFlags_,
      pool_.get(),
      &nonReclaimableSection_,
      prefixSortConfig_);

  RowVectorPtr data = makeRowVector(
      {makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6, 8, 10, 12, 15}),
       makeFlatVector<int32_t>(
           {15, 12, 9, 8, 7, 6, 5, 4, 3, 1}), // sorted-2 column
       makeFlatVector<int16_t>({1, 2, 3, 4, 5, 6, 8, 10, 12, 15}),
       makeFlatVector<float>(
           {1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 10.1}),
       makeFlatVector<double>(
           {1.1, 2.2, 2.2, 5.5, 5.5, 7.7, 8.1, 8, 8.1, 8.1, 10.0}), // sorted-1
                                                                    // column
       makeFlatVector<std::string>(
           {"hello",
            "world",
            "today",
            "is",
            "great",
            "hello",
            "world",
            "is",
            "sort",
            "sorted"})});

  sortBuffer->addInput(data);
  sortBuffer->noMoreInput();
  auto output = sortBuffer->getOutput(10000);
  ASSERT_EQ(output->size(), 10);
  ASSERT_EQ(output->childAt(1)->asFlatVector<int32_t>()->valueAt(0), 15);
  ASSERT_EQ(output->childAt(1)->asFlatVector<int32_t>()->valueAt(1), 9);
  ASSERT_EQ(output->childAt(1)->asFlatVector<int32_t>()->valueAt(2), 12);
  ASSERT_EQ(output->childAt(1)->asFlatVector<int32_t>()->valueAt(3), 7);
  ASSERT_EQ(output->childAt(1)->asFlatVector<int32_t>()->valueAt(4), 8);
  ASSERT_EQ(output->childAt(1)->asFlatVector<int32_t>()->valueAt(5), 6);
  ASSERT_EQ(output->childAt(1)->asFlatVector<int32_t>()->valueAt(6), 4);
  ASSERT_EQ(output->childAt(1)->asFlatVector<int32_t>()->valueAt(7), 1);
  ASSERT_EQ(output->childAt(1)->asFlatVector<int32_t>()->valueAt(8), 3);
  ASSERT_EQ(output->childAt(1)->asFlatVector<int32_t>()->valueAt(9), 5);
  if (GetParam()) {
    ASSERT_EQ(
        stats_.at(PrefixSort::kNumPrefixSortKeys).sum,
        sortColumnIndices_.size());
    ASSERT_EQ(
        stats_.at(PrefixSort::kNumPrefixSortKeys).max,
        sortColumnIndices_.size());
    ASSERT_EQ(
        stats_.at(PrefixSort::kNumPrefixSortKeys).min,
        sortColumnIndices_.size());
  } else {
    ASSERT_EQ(stats_.count(PrefixSort::kNumPrefixSortKeys), 0);
  }
}

// TODO: enable it later with test utility to compare the sorted result.
TEST_P(SortBufferTest, DISABLED_randomData) {
  struct {
    RowTypePtr inputType;
    std::vector<column_index_t> sortColumnIndices;
    std::vector<CompareFlags> sortCompareFlags;

    std::string debugString() const {
      const std::string sortColumnIndicesStr =
          folly::join(",", sortColumnIndices);
      std::stringstream sortCompareFlagsStr;
      for (auto sortCompareFlag : sortCompareFlags) {
        sortCompareFlagsStr << sortCompareFlag.toString() << ";";
      }
      return fmt::format(
          "inputType:{}, sortColumnIndices:{}, sortCompareFlags:{}",
          inputType,
          sortColumnIndicesStr,
          sortCompareFlagsStr.str());
    }
  } testSettings[] = {
      {ROW(
           {{"c0", BIGINT()},
            {"c1", INTEGER()},
            {"c2", SMALLINT()},
            {"c3", REAL()},
            {"c4", DOUBLE()},
            {"c5", VARCHAR()}}),
       {2},
       {{true, true, false, CompareFlags::NullHandlingMode::kNullAsValue}}},
      {ROW(
           {{"c0", BIGINT()},
            {"c1", INTEGER()},
            {"c2", SMALLINT()},
            {"c3", REAL()},
            {"c4", DOUBLE()},
            {"c5", VARCHAR()}}),
       {4, 1},
       {{true, true, false, CompareFlags::NullHandlingMode::kNullAsValue},
        {true, true, false, CompareFlags::NullHandlingMode::kNullAsValue}}},
      {ROW(
           {{"c0", BIGINT()},
            {"c1", INTEGER()},
            {"c2", SMALLINT()},
            {"c3", REAL()},
            {"c4", DOUBLE()},
            {"c5", VARCHAR()}}),
       {4, 1},
       {{true, true, false, CompareFlags::NullHandlingMode::kNullAsValue},
        {false, false, false, CompareFlags::NullHandlingMode::kNullAsValue}}}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto sortBuffer = std::make_unique<SortBuffer>(
        testData.inputType,
        testData.sortColumnIndices,
        testData.sortCompareFlags,
        pool_.get(),
        &nonReclaimableSection_,
        prefixSortConfig_);

    const std::shared_ptr<memory::MemoryPool> fuzzerPool =
        memory::memoryManager()->addLeafPool("VectorFuzzer");

    std::vector<RowVectorPtr> inputVectors;
    inputVectors.reserve(3);
    for (size_t inputRows : {1000, 1000, 1000}) {
      VectorFuzzer fuzzer({.vectorSize = inputRows}, fuzzerPool.get());
      RowVectorPtr input = fuzzer.fuzzRow(inputType_);
      sortBuffer->addInput(input);
      inputVectors.push_back(input);
    }
    sortBuffer->noMoreInput();
    stats_.clear();
    // todo: have a utility function buildExpectedSortResult and verify the
    // sorting result for random data.
  }
}

TEST_P(SortBufferTest, batchOutput) {
  struct {
    bool triggerSpill;
    std::vector<size_t> numInputRows;
    size_t maxOutputRows;
    std::vector<size_t> expectedOutputRowCount;

    std::string debugString() const {
      const std::string numInputRowsStr = folly::join(",", numInputRows);
      const std::string expectedOutputRowCountStr =
          folly::join(",", expectedOutputRowCount);
      return fmt::format(
          "triggerSpill:{}, numInputRows:{}, maxOutputRows:{}, expectedOutputRowCount:{}",
          triggerSpill,
          numInputRowsStr,
          maxOutputRows,
          expectedOutputRowCountStr);
    }
  } testSettings[] = {
      {false, {2, 3, 3}, 1, {1, 1, 1, 1, 1, 1, 1, 1}},
      {true, {2, 3, 3}, 1, {1, 1, 1, 1, 1, 1, 1, 1}},
      {false, {2000, 2000}, 10000, {4000}},
      {true, {2000, 2000}, 10000, {4000}},
      {false, {2000, 2000}, 2000, {2000, 2000}},
      {true, {2000, 2000}, 2000, {2000, 2000}},
      {false, {1024, 1024, 1024}, 1000, {1000, 1000, 1000, 72}},
      {true, {1024, 1024, 1024}, 1000, {1000, 1000, 1000, 72}}};

  TestScopedSpillInjection scopedSpillInjection(100);
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto spillDirectory = exec::test::TempDirectoryPath::create();
    auto spillConfig = common::SpillConfig(
        [&]() -> const std::string& { return spillDirectory->getPath(); },
        [&](uint64_t) {},
        "0.0.0",
        1000,
        0,
        1 << 20,
        executor_.get(),
        5,
        10,
        0,
        0,
        0,
        0,
        0,
        "none",
        prefixSortConfig_);
    folly::Synchronized<common::SpillStats> spillStats;
    auto sortBuffer = std::make_unique<SortBuffer>(
        inputType_,
        sortColumnIndices_,
        sortCompareFlags_,
        pool_.get(),
        &nonReclaimableSection_,
        prefixSortConfig_,
        testData.triggerSpill ? &spillConfig : nullptr,
        &spillStats);
    ASSERT_EQ(sortBuffer->canSpill(), testData.triggerSpill);

    const std::shared_ptr<memory::MemoryPool> fuzzerPool =
        memory::memoryManager()->addLeafPool("VectorFuzzer");

    std::vector<RowVectorPtr> inputVectors;
    inputVectors.reserve(testData.numInputRows.size());
    uint64_t totalNumInput = 0;
    for (size_t inputRows : testData.numInputRows) {
      VectorFuzzer fuzzer({.vectorSize = inputRows}, fuzzerPool.get());
      RowVectorPtr input = fuzzer.fuzzRow(inputType_);
      sortBuffer->addInput(input);
      inputVectors.push_back(input);
      totalNumInput += inputRows;
    }
    sortBuffer->noMoreInput();
    int expectedOutputBufferIndex = 0;
    RowVectorPtr output = sortBuffer->getOutput(testData.maxOutputRows);
    while (output != nullptr) {
      ASSERT_EQ(
          output->size(),
          testData.expectedOutputRowCount[expectedOutputBufferIndex++]);
      output = sortBuffer->getOutput(testData.maxOutputRows);
    }

    if (!testData.triggerSpill) {
      ASSERT_TRUE(spillStats.rlock()->empty());
    } else {
      ASSERT_FALSE(spillStats.rlock()->empty());
      ASSERT_GT(spillStats.rlock()->spilledRows, 0);
      ASSERT_LE(spillStats.rlock()->spilledRows, totalNumInput);
      ASSERT_GT(spillStats.rlock()->spilledBytes, 0);
      ASSERT_EQ(spillStats.rlock()->spilledPartitions, 1);
      ASSERT_GT(spillStats.rlock()->spilledFiles, 0);
    }
  }
}

TEST_P(SortBufferTest, spill) {
  struct {
    bool spillEnabled;
    bool memoryReservationFailure;
    bool triggerSpill;
    bool spillTriggered;

    std::string debugString() const {
      return fmt::format(
          "spillEnabled:{}, memoryReservationFailure:{}, triggerSpill:{}, spillTriggered:{}",
          spillEnabled,
          memoryReservationFailure,
          triggerSpill,
          spillTriggered);
    }
  } testSettings[] = {
      {false, true, 0, false}, // spilling is not enabled.
      {true,
       true,
       0,
       false}, // memory reservation failure won't trigger spilling.
      {true, false, true, true},
      {true, false, false, false}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto spillDirectory = exec::test::TempDirectoryPath::create();
    // memory pool limit is 20M
    // Set 'kSpillableReservationGrowthPct' to an extreme large value to trigger
    // memory reservation failure and thus trigger disk spilling.
    auto spillableReservationGrowthPct =
        testData.memoryReservationFailure ? 100000 : 100;
    auto spillConfig = common::SpillConfig(
        [&]() -> const std::string& { return spillDirectory->getPath(); },
        [&](uint64_t) {},
        "0.0.0",
        1000,
        0,
        1 << 20,
        executor_.get(),
        100,
        spillableReservationGrowthPct,
        0,
        0,
        0,
        0,
        0,
        "none",
        prefixSortConfig_);
    folly::Synchronized<common::SpillStats> spillStats;
    auto sortBuffer = std::make_unique<SortBuffer>(
        inputType_,
        sortColumnIndices_,
        sortCompareFlags_,
        pool_.get(),
        &nonReclaimableSection_,
        prefixSortConfig_,
        testData.spillEnabled ? &spillConfig : nullptr,
        &spillStats);

    const std::shared_ptr<memory::MemoryPool> fuzzerPool =
        memory::memoryManager()->addLeafPool("spillSource");
    VectorFuzzer fuzzer({.vectorSize = 1024}, fuzzerPool.get());
    uint64_t totalNumInput = 0;

    ASSERT_EQ(memory::spillMemoryPool()->stats().usedBytes, 0);
    const auto peakSpillMemoryUsage =
        memory::spillMemoryPool()->stats().peakBytes;

    TestScopedSpillInjection scopedSpillInjection(
        testData.triggerSpill ? 100 : 0);
    for (int i = 0; i < 3; ++i) {
      sortBuffer->addInput(fuzzer.fuzzRow(inputType_));
      totalNumInput += 1024;
    }
    sortBuffer->noMoreInput();

    if (!testData.spillTriggered) {
      ASSERT_TRUE(spillStats.rlock()->empty());
      if (!testData.spillEnabled) {
        VELOX_ASSERT_THROW(sortBuffer->spill(), "spill config is null");
      }
    } else {
      ASSERT_FALSE(spillStats.rlock()->empty());
      ASSERT_GT(spillStats.rlock()->spilledRows, 0);
      ASSERT_LE(spillStats.rlock()->spilledRows, totalNumInput);
      ASSERT_GT(spillStats.rlock()->spilledBytes, 0);
      ASSERT_EQ(spillStats.rlock()->spilledPartitions, 1);
      // SortBuffer shall not respect maxFileSize. Total files should be num
      // addInput() calls minus one which is the first one that has nothing to
      // spill.
      ASSERT_EQ(spillStats.rlock()->spilledFiles, 3);
      sortBuffer.reset();
      ASSERT_EQ(memory::spillMemoryPool()->stats().usedBytes, 0);
      if (memory::spillMemoryPool()->trackUsage()) {
        ASSERT_GT(memory::spillMemoryPool()->stats().peakBytes, 0);
        ASSERT_GE(
            memory::spillMemoryPool()->stats().peakBytes, peakSpillMemoryUsage);
      }
    }
    if (GetParam()) {
      ASSERT_GE(
          stats_.at(PrefixSort::kNumPrefixSortKeys).sum,
          sortColumnIndices_.size());
      ASSERT_EQ(
          stats_.at(PrefixSort::kNumPrefixSortKeys).max,
          sortColumnIndices_.size());
      ASSERT_EQ(
          stats_.at(PrefixSort::kNumPrefixSortKeys).min,
          sortColumnIndices_.size());
    } else {
      ASSERT_EQ(stats_.count(PrefixSort::kNumPrefixSortKeys), 0);
    }
    stats_.clear();
  }
}

DEBUG_ONLY_TEST_P(SortBufferTest, spillDuringInput) {
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  const auto spillConfig = getSpillConfig(spillDirectory->getPath());
  folly::Synchronized<common::SpillStats> spillStats;
  auto sortBuffer = std::make_unique<SortBuffer>(
      inputType_,
      sortColumnIndices_,
      sortCompareFlags_,
      pool_.get(),
      &nonReclaimableSection_,
      prefixSortConfig_,
      &spillConfig,
      &spillStats);

  const int numInputs = 10;
  const int numSpilledInputs = 10 / 2;
  std::atomic_int processedInputs{0};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::SortBuffer::addInput",
      std::function<void(SortBuffer*)>(([&](SortBuffer* sortBuffer) {
        if (processedInputs++ != numSpilledInputs) {
          return;
        }
        ASSERT_GT(sortBuffer->pool()->usedBytes(), 0);
        sortBuffer->spill();
        ASSERT_EQ(sortBuffer->pool()->usedBytes(), 0);
      })));

  const std::shared_ptr<memory::MemoryPool> fuzzerPool =
      memory::memoryManager()->addLeafPool("spillDuringInput");
  VectorFuzzer fuzzer({.vectorSize = 1024}, fuzzerPool.get());

  ASSERT_EQ(memory::spillMemoryPool()->stats().usedBytes, 0);
  const auto peakSpillMemoryUsage =
      memory::spillMemoryPool()->stats().peakBytes;

  for (int i = 0; i < numInputs; ++i) {
    sortBuffer->addInput(fuzzer.fuzzRow(inputType_));
  }
  sortBuffer->noMoreInput();

  ASSERT_FALSE(spillStats.rlock()->empty());
  ASSERT_GT(spillStats.rlock()->spilledRows, 0);
  ASSERT_EQ(spillStats.rlock()->spilledRows, numInputs * 1024);
  ASSERT_GT(spillStats.rlock()->spilledBytes, 0);
  ASSERT_EQ(spillStats.rlock()->spilledPartitions, 1);
  ASSERT_EQ(spillStats.rlock()->spilledFiles, 2);

  ASSERT_EQ(memory::spillMemoryPool()->stats().usedBytes, 0);
  if (memory::spillMemoryPool()->trackUsage()) {
    ASSERT_GT(memory::spillMemoryPool()->stats().peakBytes, 0);
    ASSERT_GE(
        memory::spillMemoryPool()->stats().peakBytes, peakSpillMemoryUsage);
  }
}

DEBUG_ONLY_TEST_P(SortBufferTest, spillDuringOutput) {
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  const auto spillConfig = getSpillConfig(spillDirectory->getPath());
  folly::Synchronized<common::SpillStats> spillStats;
  auto sortBuffer = std::make_unique<SortBuffer>(
      inputType_,
      sortColumnIndices_,
      sortCompareFlags_,
      pool_.get(),
      &nonReclaimableSection_,
      prefixSortConfig_,
      &spillConfig,
      &spillStats);

  const int numInputs = 10;
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::SortBuffer::noMoreInput",
      std::function<void(SortBuffer*)>(([&](SortBuffer* sortBuffer) {
        ASSERT_GT(sortBuffer->pool()->usedBytes(), 0);
        sortBuffer->spill();
        ASSERT_EQ(sortBuffer->pool()->usedBytes(), 0);
      })));

  const std::shared_ptr<memory::MemoryPool> fuzzerPool =
      memory::memoryManager()->addLeafPool("spillDuringOutput");
  VectorFuzzer fuzzer({.vectorSize = 1024}, fuzzerPool.get());

  ASSERT_EQ(memory::spillMemoryPool()->stats().usedBytes, 0);
  const auto peakSpillMemoryUsage =
      memory::spillMemoryPool()->stats().peakBytes;

  for (int i = 0; i < numInputs; ++i) {
    sortBuffer->addInput(fuzzer.fuzzRow(inputType_));
  }
  sortBuffer->noMoreInput();

  ASSERT_FALSE(spillStats.rlock()->empty());
  ASSERT_GT(spillStats.rlock()->spilledRows, 0);
  ASSERT_EQ(spillStats.rlock()->spilledRows, numInputs * 1024);
  ASSERT_GT(spillStats.rlock()->spilledBytes, 0);
  ASSERT_EQ(spillStats.rlock()->spilledPartitions, 1);
  ASSERT_EQ(spillStats.rlock()->spilledFiles, 1);

  ASSERT_EQ(memory::spillMemoryPool()->stats().usedBytes, 0);
  if (memory::spillMemoryPool()->trackUsage()) {
    ASSERT_GT(memory::spillMemoryPool()->stats().peakBytes, 0);
    ASSERT_GE(
        memory::spillMemoryPool()->stats().peakBytes, peakSpillMemoryUsage);
  }
}

DEBUG_ONLY_TEST_P(SortBufferTest, reserveMemorySortGetOutput) {
  for (bool spillEnabled : {false, true}) {
    SCOPED_TRACE(fmt::format("spillEnabled {}", spillEnabled));

    auto spillDirectory = exec::test::TempDirectoryPath::create();
    const auto spillConfig = getSpillConfig(spillDirectory->getPath());
    folly::Synchronized<common::SpillStats> spillStats;
    auto sortBuffer = std::make_unique<SortBuffer>(
        inputType_,
        sortColumnIndices_,
        sortCompareFlags_,
        pool_.get(),
        &nonReclaimableSection_,
        prefixSortConfig_,
        spillEnabled ? &spillConfig : nullptr,
        &spillStats);

    const std::shared_ptr<memory::MemoryPool> fuzzerPool =
        memory::memoryManager()->addLeafPool("reserveMemoryGetOutput");
    VectorFuzzer fuzzer({.vectorSize = 1024}, fuzzerPool.get());

    const int numInputs{10};
    for (int i = 0; i < numInputs; ++i) {
      sortBuffer->addInput(fuzzer.fuzzRow(inputType_));
    }

    std::atomic_bool noMoreInput{false};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::SortBuffer::noMoreInput",
        std::function<void(SortBuffer*)>(
            ([&](SortBuffer* sortBuffer) { noMoreInput.store(true); })));

    std::atomic_int numReserves{0};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::common::memory::MemoryPoolImpl::maybeReserve",
        std::function<void(memory::MemoryPoolImpl*)>(
            ([&](memory::MemoryPoolImpl* pool) {
              if (noMoreInput) {
                ++numReserves;
              }
            })));

    sortBuffer->noMoreInput();
    // Sets an extreme large value to get output once to avoid test flakiness.
    sortBuffer->getOutput(1'000'000);
    if (spillEnabled) {
      // Reserve memory for sort and getOutput.
      ASSERT_EQ(numReserves, 2);
    } else {
      ASSERT_EQ(numReserves, 0);
    }
  }
}

DEBUG_ONLY_TEST_P(SortBufferTest, reserveMemorySort) {
  struct {
    bool usePrefixSort;
    bool spillEnabled;
  } testSettings[] = {{false, true}, {true, false}, {true, true}};

  for (const auto [usePrefixSort, spillEnabled] : testSettings) {
    SCOPED_TRACE(fmt::format(
        "usePrefixSort: {}, spillEnabled: {}, ", usePrefixSort, spillEnabled));
    auto spillDirectory = exec::test::TempDirectoryPath::create();
    auto spillConfig = getSpillConfig(spillDirectory->getPath(), usePrefixSort);
    folly::Synchronized<common::SpillStats> spillStats;
    auto sortBuffer = std::make_unique<SortBuffer>(
        inputType_,
        sortColumnIndices_,
        sortCompareFlags_,
        pool_.get(),
        &nonReclaimableSection_,
        prefixSortConfig_,
        spillEnabled ? &spillConfig : nullptr,
        &spillStats);

    const std::shared_ptr<memory::MemoryPool> spillSource =
        memory::memoryManager()->addLeafPool("spillSource");
    VectorFuzzer fuzzer({.vectorSize = 100}, spillSource.get());

    TestScopedSpillInjection scopedSpillInjection(0);
    sortBuffer->addInput(fuzzer.fuzzRow(inputType_));

    std::atomic_bool hasReserveMemory = false;
    // Reserve memory for sort.
    SCOPED_TESTVALUE_SET(
        "facebook::velox::common::memory::MemoryPoolImpl::maybeReserve",
        std::function<void(memory::MemoryPoolImpl*)>(
            ([&](memory::MemoryPoolImpl* pool) {
              hasReserveMemory.store(true);
            })));

    sortBuffer->noMoreInput();
    if (spillEnabled) {
      // Reserve memory for sort.
      ASSERT_TRUE(hasReserveMemory);
    } else {
      ASSERT_FALSE(hasReserveMemory);
    }
  }
}

TEST_P(SortBufferTest, emptySpill) {
  const std::shared_ptr<memory::MemoryPool> fuzzerPool =
      memory::memoryManager()->addLeafPool("emptySpillSource");

  for (bool hasPostSpillData : {false, true}) {
    SCOPED_TRACE(fmt::format("hasPostSpillData {}", hasPostSpillData));
    auto spillDirectory = exec::test::TempDirectoryPath::create();
    auto spillConfig = getSpillConfig(spillDirectory->getPath());
    folly::Synchronized<common::SpillStats> spillStats;
    auto sortBuffer = std::make_unique<SortBuffer>(
        inputType_,
        sortColumnIndices_,
        sortCompareFlags_,
        pool_.get(),
        &nonReclaimableSection_,
        prefixSortConfig_,
        &spillConfig,
        &spillStats);

    sortBuffer->spill();
    if (hasPostSpillData) {
      VectorFuzzer fuzzer({.vectorSize = 1024}, fuzzerPool.get());
      sortBuffer->addInput(fuzzer.fuzzRow(inputType_));
    }
    sortBuffer->noMoreInput();
    ASSERT_TRUE(spillStats.rlock()->empty());
  }
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    SortBufferTest,
    SortBufferTest,
    testing::ValuesIn({false, true}));
} // namespace facebook::velox::functions::test
