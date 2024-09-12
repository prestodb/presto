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
#include "velox/common/base/tests/GTestUtils.h"

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

DECLARE_int32(wave_max_reader_batch_rows);
DECLARE_int32(max_streams_per_driver);
DECLARE_int32(wave_reader_rows_per_tb);

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

struct WaveScanTestParam {
  int32_t numStreams{1};
  int32_t batchSize{20000};
  int32_t rowsPerTB{1024};
};

std::vector<WaveScanTestParam> waveScanTestParams() {
  return {
      WaveScanTestParam{},
      WaveScanTestParam{.numStreams = 4, .rowsPerTB = 4096},
      WaveScanTestParam{.numStreams = 4, .batchSize = 1111},
      WaveScanTestParam{.numStreams = 9, .batchSize = 16500},
      WaveScanTestParam{
          .numStreams = 2, .batchSize = 20000, .rowsPerTB = 20480}};
}

class TableScanTest : public virtual HiveConnectorTestBase,
                      public testing::WithParamInterface<WaveScanTestParam> {
 protected:
  void SetUp() override {
    HiveConnectorTestBase::SetUp();
    if (int device; cudaGetDevice(&device) != cudaSuccess) {
      GTEST_SKIP() << "No CUDA detected, skipping all tests";
    }
    wave::registerWave();
    wave::WaveHiveDataSource::registerConnector();
    wave::test::WaveTestSplitReader::registerTestSplitReader();
    exec::ExchangeSource::factories().clear();
    exec::ExchangeSource::registerFactory(createLocalExchangeSource);
    fuzzer_ = std::make_unique<VectorFuzzer>(options_, pool_.get());
    auto param = GetParam();
    FLAGS_max_streams_per_driver = param.numStreams;
    FLAGS_wave_max_reader_batch_rows = param.batchSize;
    FLAGS_wave_reader_rows_per_tb = param.rowsPerTB;
  }

  static void SetUpTestCase() {
    OperatorTestBase::SetUpTestCase();
  }

  void TearDown() override {
    vectors_.clear();
    wave::test::Table::dropAll();
    HiveConnectorTestBase::TearDown();
  }

  auto makeData(
      const RowTypePtr& type,
      int32_t numVectors,
      int32_t vectorSize,
      bool notNull = true,
      std::function<void(RowVectorPtr)> custom = nullptr) {
    vectors_ = makeVectors(type, numVectors, vectorSize, notNull ? 0 : 0.1);
    int32_t cnt = 0;
    for (auto& vector : vectors_) {
      if (custom) {
        custom(vector);
      } else {
        makeRange(vector, 1000000000, notNull);
      }
      auto rn = vector->childAt(type->size() - 1)->as<FlatVector<int64_t>>();
      for (auto i = 0; i < rn->size(); ++i) {
        rn->set(i, cnt++);
      }
    }
    auto splits = makeTable("test", vectors_);
    createDuckDbTable(vectors_);
    if (dumpData_) {
      toFile();
    }
    return splits;
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
      bool notNull = true,
      int32_t begin = -1,
      int32_t end = -1) {
    if (begin == -1) {
      begin = 0;
    }
    if (end == -1) {
      end = row->type()->size();
    }
    for (auto i = begin; i < end; ++i) {
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

  FOLLY_NOINLINE void toFile() {
    std::ofstream out("/tmp/file.txt");
    int32_t row = 0;
    for (auto i = 0; i < vectors_.size(); ++i) {
      out << "\n\n*** " << row;
      out << vectors_[i]->toString(0, vectors_[i]->size(), "\n", true);
    }
    out.close();
  }

  VectorFuzzer::Options options_;
  std::unique_ptr<VectorFuzzer> fuzzer_;
  int32_t numBatches_ = 3;
  int32_t batchSize_ = 20'000;
  std::vector<RowVectorPtr> vectors_;
  bool dumpData_{false};
};

TEST_P(TableScanTest, basic) {
  auto type = ROW({"c0"}, {BIGINT()});
  auto splits = makeData(type, numBatches_, batchSize_);

  auto plan = tableScanNode(type);
  auto task = assertQuery(plan, splits, "SELECT * FROM tmp");

  auto planStats = toPlanStats(task->taskStats());
  auto scanNodeId = plan->id();
  auto it = planStats.find(scanNodeId);
  ASSERT_TRUE(it != planStats.end());
}

TEST_P(TableScanTest, filter) {
  auto type =
      ROW({"c0", "c1", "c2", "c3"}, {BIGINT(), BIGINT(), BIGINT(), BIGINT()});
  auto splits = makeData(type, numBatches_, batchSize_);

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

TEST_P(TableScanTest, filterNull) {
  auto type =
      ROW({"c0", "c1", "c2", "c3"}, {BIGINT(), BIGINT(), BIGINT(), BIGINT()});
  auto splits = makeData(type, numBatches_, batchSize_, false);

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

TEST_P(TableScanTest, filterInScan) {
  auto type =
      ROW({"c0", "c1", "c2", "c3"}, {BIGINT(), BIGINT(), BIGINT(), BIGINT()});
  auto splits = makeData(type, numBatches_, batchSize_);

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

TEST_P(TableScanTest, filterInScanNull) {
  auto type =
      ROW({"c0", "c1", "c2", "c3", "rn"},
          {BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT()});
  auto splits = makeData(type, numBatches_, batchSize_, false);

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

TEST_P(TableScanTest, scanAgg) {
  auto type =
      ROW({"c0", "c1", "c2", "c3", "rn"},
          {BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT()});
  auto splits = makeData(type, numBatches_, batchSize_);

  auto plan =
      PlanBuilder(pool_.get())
          .tableScan(type, {"c0 < 950000000"})
          .project(
              {"c0",
               "c1 + 1 as c1",
               "c2 + 2 as c2",
               "c3 + c2 as c3",
               "rn + 1 as rn"})
          .singleAggregation(
              {}, {"sum(c0)", "sum(c1)", "sum(c2)", "sum(c3)", "sum(rn)"})
          .planNode();
  auto task = assertQuery(
      plan,
      splits,
      "SELECT sum(c0), sum(c1 + 1), sum(c2 + 2), sum(c3 + c2), sum(rn + 1) FROM tmp where c0 < 950000000");
}

TEST_P(TableScanTest, scanGroupBy) {
  GTEST_SKIP();
  auto type =
      ROW({"c0", "c1", "c2", "c3", "rn"},
          {BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT()});
  auto splits =
      makeData(type, numBatches_, batchSize_, true, [&](RowVectorPtr row) {
        makeRange(row, 1000000000, true, 1, -1);
      });

  auto plan = PlanBuilder(pool_.get())
                  .tableScan(type, {"c1 < 950000000"})
                  .project(
                      {"c0",
                       "c1 + 1 as c1",
                       "c2 + 2 as c2",
                       "c3 + c2 as c3",
                       "rn + 1 as rn"})
                  .singleAggregation(
                      {"c0"}, {"sum(c1)", "sum(c2)", "sum(c3)", "sum(rn)"})
                  .planNode();
  auto task = assertQuery(
      plan,
      splits,
      "SELECT c0, sum(c1 + 1), sum(c2 + 2), sum(c3 + c2), sum(rn + 1) FROM tmp where c0 < 950000000 group by c0");
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    TableScanTests,
    TableScanTest,
    testing::ValuesIn(waveScanTestParams()));
