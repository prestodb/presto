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
#include <cuda_runtime.h> // @manual
#include "velox/exec/ExchangeSource.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/experimental/wave/exec/ToWave.h"
#include "velox/experimental/wave/exec/WaveHiveDataSource.h"
#include "velox/experimental/wave/exec/tests/utils/FileFormat.h"
#include "velox/experimental/wave/exec/tests/utils/WaveTestSplitReader.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

class TableScanTest : public virtual HiveConnectorTestBase {
 protected:
  void SetUp() override {
    if (int device; cudaGetDevice(&device) != cudaSuccess) {
      GTEST_SKIP() << "No CUDA detected, skipping all tests";
    }
    HiveConnectorTestBase::SetUp();
    wave::registerWave();
    wave::WaveHiveDataSource::registerConnector();
    wave::test::WaveTestSplitReader::registerTestSplitReader();
    exec::ExchangeSource::factories().clear();
    exec::ExchangeSource::registerFactory(createLocalExchangeSource);
    fuzzer_ = std::make_unique<VectorFuzzer>(options_, pool_.get());
  }

  static void SetUpTestCase() {
    OperatorTestBase::SetUpTestCase();
  }

  void TearDown() override {
    wave::test::Table::dropAll();
    HiveConnectorTestBase::TearDown();
  }

  std::vector<RowVectorPtr> makeVectors(
      const RowTypePtr& rowType,
      int32_t numVectors,
      int32_t rowsPerVector,
      float nullRatio = 0) {
    std::vector<RowVectorPtr> vectors;
    options_.vectorSize = rowsPerVector;
    options_.nullRatio = nullRatio;
    fuzzer_->setOptions(options_);

    for (int32_t i = 0; i < numVectors; ++i) {
      auto vector = fuzzer_->fuzzInputFlatRow(rowType);
      vectors.push_back(vector);
    }
    return vectors;
  }

  void makeRange(
      RowVectorPtr row,
      int64_t mod = std::numeric_limits<int64_t>::max(),
      bool notNull = true) {
    for (auto i = 0; i < row->type()->size(); ++i) {
      auto child = row->childAt(i);
      if (auto ints = child->as<FlatVector<int64_t>>()) {
        for (auto i = 0; i < child->size(); ++i) {
          if (!notNull && ints->isNullAt(i)) {
            continue;
          }
          ints->set(i, ints->valueAt(i) % mod);
        }
      }
      if (notNull) {
        child->clearNulls(0, row->size());
      }
    }
  }

  wave::test::SplitVector makeTable(
      const std::string& name,
      std::vector<RowVectorPtr>& rows) {
    wave::test::Table::dropTable(name);
    return wave::test::Table::defineTable(name, rows)->splits();
  }

  std::shared_ptr<Task> assertQuery(
      const PlanNodePtr& plan,
      const wave::test::SplitVector& splits,
      const std::string& duckDbSql) {
    return OperatorTestBase::assertQuery(plan, splits, duckDbSql);
  }

  std::shared_ptr<Task> assertQuery(
      const PlanNodePtr& plan,
      const wave::test::SplitVector& splits,
      const std::string& duckDbSql,
      const int32_t numPrefetchSplit) {
    return AssertQueryBuilder(plan, duckDbQueryRunner_)
        .config(
            core::QueryConfig::kMaxSplitPreloadPerDriver,
            std::to_string(numPrefetchSplit))
        .splits(splits)
        .assertResults(duckDbSql);
  }

  core::PlanNodePtr tableScanNode(const RowTypePtr& outputType) {
    return PlanBuilder(pool_.get()).tableScan(outputType).planNode();
  }

  static PlanNodeStats getTableScanStats(const std::shared_ptr<Task>& task) {
    auto planStats = toPlanStats(task->taskStats());
    return std::move(planStats.at("0"));
  }

  static std::unordered_map<std::string, RuntimeMetric>
  getTableScanRuntimeStats(const std::shared_ptr<Task>& task) {
    return task->taskStats().pipelineStats[0].operatorStats[0].runtimeStats;
  }

  static int64_t getSkippedStridesStat(const std::shared_ptr<Task>& task) {
    return getTableScanRuntimeStats(task)["skippedStrides"].sum;
  }

  static int64_t getSkippedSplitsStat(const std::shared_ptr<Task>& task) {
    return getTableScanRuntimeStats(task)["skippedSplits"].sum;
  }

  static void waitForFinishedDrivers(
      const std::shared_ptr<Task>& task,
      uint32_t n) {
    // Limit wait to 10 seconds.
    size_t iteration{0};
    while (task->numFinishedDrivers() < n and iteration < 100) {
      /* sleep override */
      usleep(100'000); // 0.1 second.
      ++iteration;
    }
    ASSERT_EQ(n, task->numFinishedDrivers());
  }

  VectorFuzzer::Options options_;
  std::unique_ptr<VectorFuzzer> fuzzer_;
};

TEST_F(TableScanTest, basic) {
  auto type = ROW({"c0"}, {BIGINT()});
  auto vectors = makeVectors(type, 10, 1'000);
  auto splits = makeTable("test", vectors);
  createDuckDbTable(vectors);

  auto plan = tableScanNode(type);
  auto task = assertQuery(plan, splits, "SELECT * FROM tmp");

  auto planStats = toPlanStats(task->taskStats());
  auto scanNodeId = plan->id();
  auto it = planStats.find(scanNodeId);
  ASSERT_TRUE(it != planStats.end());
}

TEST_F(TableScanTest, filter) {
  auto type =
      ROW({"c0", "c1", "c2", "c3"}, {BIGINT(), BIGINT(), BIGINT(), BIGINT()});
  auto vectors = makeVectors(type, 1, 1'500);
  int32_t cnt = 0;
  for (auto& vector : vectors) {
    makeRange(vector, 1000000000);
    auto rn = vector->childAt(3)->as<FlatVector<int64_t>>();
    for (auto i = 0; i < rn->size(); ++i) {
      rn->set(i, cnt++);
    }
    std::cout << vector->toString(0, vector->size(), "\n", true) << std::endl;
  }
  auto splits = makeTable("test", vectors);
  createDuckDbTable(vectors);

  auto plan = PlanBuilder(pool_.get())
                  .tableScan(type)
                  .filter("c0 < 500000000")
                  .project({"c0", "c1 + 100000000 as c1", "c2", "c3"})
                  .filter("c1 < 500000000")
                  .project({"c0", "c1", "c2 + 1", "c3", "c3 + 2"})
                  .planNode();
  auto task = assertQuery(
      plan,
      splits,
      "SELECT c0, c1 + 100000000, c2 + 1, c3, c3 + 2 FROM tmp where c0 < 500000000 and c1 + 100000000 < 500000000");
}

TEST_F(TableScanTest, filterInScan) {
  auto type =
      ROW({"c0", "c1", "c2", "c3"}, {BIGINT(), BIGINT(), BIGINT(), BIGINT()});
  auto vectors = makeVectors(type, 10, 2'000);
  for (auto& vector : vectors) {
    makeRange(vector, 1000000000);
  }
  auto splits = makeTable("test", vectors);
  createDuckDbTable(vectors);

  auto plan = PlanBuilder(pool_.get())
                  .tableScan(type, {"c0 < 500000000", "c1 < 400000000"})
                  .project({"c0", "c1 + 100000000 as c1", "c2", "c3"})
                  .project({"c0", "c1", "c2 + 1", "c3", "c3 + 2"})
                  .planNode();
  auto task = assertQuery(
      plan,
      splits,
      "SELECT c0, c1 + 100000000, c2 + 1, c3, c3 + 2 FROM tmp where c0 < 500000000 and c1 + 100000000 < 500000000");
}

TEST_F(TableScanTest, filterInScanNull) {
  auto type =
      ROW({"c0", "c1", "c2", "c3", "rn"},
          {BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT()});
  auto vectors = makeVectors(type, 1, 20'000, 0.1);
  int32_t cnt = 0;
  for (auto& vector : vectors) {
    makeRange(vector, 1000000000, false);
    auto rn = vector->childAt(4)->as<FlatVector<int64_t>>();
    for (auto i = 0; i < rn->size(); ++i) {
      rn->set(i, cnt++);
    }
  }
  auto splits = makeTable("test", vectors);
  createDuckDbTable(vectors);

  auto plan =
      PlanBuilder(pool_.get())
          .tableScan(type, {"c0 < 500000000", "c1 < 400000000"})
          .project(
              {"c0",
               "c1",
               "c1 + 100000000 as c1f",
               "c2 as c2p",
               "c3 as c3p",
               "rn"})
          .project({"c0", "c1", "c1f", "c2p + 1", "c3p", "c3p + 2", "rn"})
          .planNode();
  auto task = assertQuery(
      plan,
      splits,
      "SELECT c0, c1, c1 + 100000000, c2 + 1, c3, c3 + 2, rn FROM tmp where c0 < 500000000 and c1 + 100000000 < 500000000");
}
