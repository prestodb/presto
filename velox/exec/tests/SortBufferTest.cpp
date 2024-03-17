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

class SortBufferTest : public OperatorTestBase {
 protected:
  void SetUp() override {
    filesystems::registerLocalFileSystem();
    if (!isRegisteredVectorSerde()) {
      this->registerVectorSerde();
    }
    rng_.seed(123);
  }

  common::SpillConfig getSpillConfig(const std::string& spillDir) const {
    return common::SpillConfig(
        [&]() -> const std::string& { return spillDir; },
        [&](uint64_t) {},
        "0.0.0",
        0,
        0,
        0,
        executor_.get(),
        5,
        10,
        0,
        0,
        0,
        0,
        0,
        "none");
  }

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

  const int64_t maxBytes_ = 20LL << 20; // 20 MB
  const std::shared_ptr<memory::MemoryPool> rootPool_{
      memory::memoryManager()->addRootPool("SortBufferTest", maxBytes_)};
  const std::shared_ptr<memory::MemoryPool> pool_{
      rootPool_->addLeafChild("SortBufferTest", maxBytes_)};
  const std::shared_ptr<folly::Executor> executor_{
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency())};

  tsan_atomic<bool> nonReclaimableSection_{false};
  folly::Random::DefaultGenerator rng_;
};

TEST_F(SortBufferTest, singleKey) {
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
       {1, 2, 3, 4, 5}},
      {{{true,
         false,
         false,
         CompareFlags::NullHandlingMode::kNullAsValue}}, // Descending
       {5, 4, 3, 2, 1}}};

  // Specifies the sort columns ["c1"].
  sortColumnIndices_ = {1};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto sortBuffer = std::make_unique<SortBuffer>(
        inputType_,
        sortColumnIndices_,
        testData.sortCompareFlags,
        pool_.get(),
        &nonReclaimableSection_);

    RowVectorPtr data = makeRowVector(
        {makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
         makeFlatVector<int32_t>({5, 4, 3, 2, 1}), // sorted column
         makeFlatVector<int16_t>({1, 2, 3, 4, 5}),
         makeFlatVector<float>({1.1, 2.2, 3.3, 4.4, 5.5}),
         makeFlatVector<double>({1.1, 2.2, 2.2, 5.5, 5.5}),
         makeFlatVector<std::string>(
             {"hello", "world", "today", "is", "great"})});

    sortBuffer->addInput(data);
    sortBuffer->noMoreInput();
    auto output = sortBuffer->getOutput(10000);
    ASSERT_EQ(output->size(), 5);
    int resultIndex = 0;
    for (int expectedValue : testData.expectedResult) {
      ASSERT_EQ(
          output->childAt(1)->asFlatVector<int32_t>()->valueAt(resultIndex++),
          expectedValue);
    }
  }
}

TEST_F(SortBufferTest, multipleKeys) {
  auto sortBuffer = std::make_unique<SortBuffer>(
      inputType_,
      sortColumnIndices_,
      sortCompareFlags_,
      pool_.get(),
      &nonReclaimableSection_);

  RowVectorPtr data = makeRowVector(
      {makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
       makeFlatVector<int32_t>({5, 4, 3, 2, 1}), // sorted-2 column
       makeFlatVector<int16_t>({1, 2, 3, 4, 5}),
       makeFlatVector<float>({1.1, 2.2, 3.3, 4.4, 5.5}),
       makeFlatVector<double>({1.1, 2.2, 2.2, 5.5, 5.5}), // sorted-1 column
       makeFlatVector<std::string>(
           {"hello", "world", "today", "is", "great"})});

  sortBuffer->addInput(data);
  sortBuffer->noMoreInput();
  auto output = sortBuffer->getOutput(10000);
  ASSERT_EQ(output->size(), 5);
  ASSERT_EQ(output->childAt(1)->asFlatVector<int32_t>()->valueAt(0), 5);
  ASSERT_EQ(output->childAt(1)->asFlatVector<int32_t>()->valueAt(1), 3);
  ASSERT_EQ(output->childAt(1)->asFlatVector<int32_t>()->valueAt(2), 4);
  ASSERT_EQ(output->childAt(1)->asFlatVector<int32_t>()->valueAt(3), 1);
  ASSERT_EQ(output->childAt(1)->asFlatVector<int32_t>()->valueAt(4), 2);
}

// TODO: enable it later with test utility to compare the sorted result.
TEST_F(SortBufferTest, DISABLED_randomData) {
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
        &nonReclaimableSection_);

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
    // todo: have a utility function buildExpectedSortResult and verify the
    // sorting result for random data.
  }
}

TEST_F(SortBufferTest, batchOutput) {
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
        [&]() -> const std::string& { return spillDirectory->path; },
        [&](uint64_t) {},
        "0.0.0",
        1000,
        0,
        1000,
        executor_.get(),
        5,
        10,
        0,
        0,
        0,
        0,
        0,
        "none");
    auto sortBuffer = std::make_unique<SortBuffer>(
        inputType_,
        sortColumnIndices_,
        sortCompareFlags_,
        pool_.get(),
        &nonReclaimableSection_,
        testData.triggerSpill ? &spillConfig : nullptr);
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
    auto spillStats = sortBuffer->spilledStats();

    int expectedOutputBufferIndex = 0;
    RowVectorPtr output = sortBuffer->getOutput(testData.maxOutputRows);
    while (output != nullptr) {
      ASSERT_EQ(
          output->size(),
          testData.expectedOutputRowCount[expectedOutputBufferIndex++]);
      output = sortBuffer->getOutput(testData.maxOutputRows);
    }

    if (!testData.triggerSpill) {
      ASSERT_FALSE(spillStats.has_value());
    } else {
      ASSERT_TRUE(spillStats.has_value());
      ASSERT_GT(spillStats->spilledRows, 0);
      ASSERT_LE(spillStats->spilledRows, totalNumInput);
      ASSERT_GT(spillStats->spilledBytes, 0);
      ASSERT_EQ(spillStats->spilledPartitions, 1);
      ASSERT_GT(spillStats->spilledFiles, 0);
    }
  }
}

TEST_F(SortBufferTest, spill) {
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
        [&]() -> const std::string& { return spillDirectory->path; },
        [&](uint64_t) {},
        "0.0.0",
        1000,
        0,
        1000,
        executor_.get(),
        100,
        spillableReservationGrowthPct,
        0,
        0,
        0,
        0,
        0,
        "none");
    auto sortBuffer = std::make_unique<SortBuffer>(
        inputType_,
        sortColumnIndices_,
        sortCompareFlags_,
        pool_.get(),
        &nonReclaimableSection_,
        testData.spillEnabled ? &spillConfig : nullptr);

    const std::shared_ptr<memory::MemoryPool> fuzzerPool =
        memory::memoryManager()->addLeafPool("spillSource");
    VectorFuzzer fuzzer({.vectorSize = 1024}, fuzzerPool.get());
    uint64_t totalNumInput = 0;

    ASSERT_EQ(memory::spillMemoryPool()->stats().currentBytes, 0);
    const auto peakSpillMemoryUsage =
        memory::spillMemoryPool()->stats().peakBytes;

    TestScopedSpillInjection scopedSpillInjection(
        testData.triggerSpill ? 100 : 0);
    for (int i = 0; i < 3; ++i) {
      sortBuffer->addInput(fuzzer.fuzzRow(inputType_));
      totalNumInput += 1024;
    }
    sortBuffer->noMoreInput();
    const auto spillStats = sortBuffer->spilledStats();

    if (!testData.spillTriggered) {
      ASSERT_FALSE(spillStats.has_value());
      if (!testData.spillEnabled) {
        VELOX_ASSERT_THROW(sortBuffer->spill(), "spill config is null");
      }
    } else {
      ASSERT_TRUE(spillStats.has_value());
      ASSERT_GT(spillStats->spilledRows, 0);
      ASSERT_LE(spillStats->spilledRows, totalNumInput);
      ASSERT_GT(spillStats->spilledBytes, 0);
      ASSERT_EQ(spillStats->spilledPartitions, 1);
      // SortBuffer shall not respect maxFileSize. Total files should be num
      // addInput() calls minus one which is the first one that has nothing to
      // spill.
      ASSERT_EQ(spillStats->spilledFiles, 3);
      sortBuffer.reset();
      ASSERT_EQ(memory::spillMemoryPool()->stats().currentBytes, 0);
      if (memory::spillMemoryPool()->trackUsage()) {
        ASSERT_GT(memory::spillMemoryPool()->stats().peakBytes, 0);
        ASSERT_GE(
            memory::spillMemoryPool()->stats().peakBytes, peakSpillMemoryUsage);
      }
    }
  }
}

TEST_F(SortBufferTest, emptySpill) {
  const std::shared_ptr<memory::MemoryPool> fuzzerPool =
      memory::memoryManager()->addLeafPool("emptySpillSource");

  for (bool hasPostSpillData : {false, true}) {
    SCOPED_TRACE(fmt::format("hasPostSpillData {}", hasPostSpillData));
    auto spillDirectory = exec::test::TempDirectoryPath::create();
    auto spillConfig = getSpillConfig(spillDirectory->path);
    auto sortBuffer = std::make_unique<SortBuffer>(
        inputType_,
        sortColumnIndices_,
        sortCompareFlags_,
        pool_.get(),
        &nonReclaimableSection_,
        &spillConfig);

    sortBuffer->spill();
    if (hasPostSpillData) {
      VectorFuzzer fuzzer({.vectorSize = 1024}, fuzzerPool.get());
      sortBuffer->addInput(fuzzer.fuzzRow(inputType_));
    }
    sortBuffer->noMoreInput();
    ASSERT_FALSE(sortBuffer->spilledStats());
  }
}
} // namespace facebook::velox::functions::test
