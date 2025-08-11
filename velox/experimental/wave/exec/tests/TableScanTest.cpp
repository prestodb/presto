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

DEFINE_int32(agg_mod1, 50000, "Num distinct in 1st group by");

DEFINE_int32(agg_mod2, 9000, "Num distinct in 2nd group by");

DEFINE_int32(max_drivers, 2, "Number of drivers in multidriver tests");

DEFINE_int32(wt_num_batches, 3, "Number of batches of test data");

DEFINE_int32(wt_batch_size, 20'000, "Batch size  in test data");

DEFINE_bool(extended, false, "Run extra permutations of drivers/streams");

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

struct WaveScanTestParam {
  int32_t numStreams{1};
  int32_t batchSize{20000};
  int32_t rowsPerTB{1024};
  bool makeDict{false};
  int32_t numDrivers{1};
};

std::vector<WaveScanTestParam> waveScanTestParams() {
  if (FLAGS_extended) {
    return {
        WaveScanTestParam{},
        WaveScanTestParam{.numStreams = 4, .rowsPerTB = 4096, .makeDict = true},
        WaveScanTestParam{
            .numStreams = 4,
            .batchSize = 1111,
            .numDrivers = FLAGS_max_drivers},
        WaveScanTestParam{
            .numStreams = 9,
            .batchSize = 16500,
            .makeDict = true,
            .numDrivers = FLAGS_max_drivers},
        WaveScanTestParam{
            .numStreams = 2, .batchSize = 20000, .rowsPerTB = 20480}};
  } else {
    return {
        WaveScanTestParam{},
        WaveScanTestParam{.numStreams = 4, .rowsPerTB = 4096, .makeDict = true},
        WaveScanTestParam{
            .numStreams = 4,
            .batchSize = 1111,
            .numDrivers = FLAGS_max_drivers}};
  }
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
    numDrivers_ = param.numDrivers;
    if (param.makeDict) {
      roundTo_ = 500000;
    }
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
        makeRange(vector, 1000000000, notNull, -1, -1, roundTo_);
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
      int32_t end = -1,
      int64_t roundTo = 1) {
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
          ints->set(i, bits::roundUp(ints->valueAt(i) % mod, roundTo));
        }
      }
      if (notNull) {
        child->clearNulls(0, row->size());
      }
    }
  }

  void makeNumbers(VectorPtr vector, int32_t& counter) {
    auto numbers = vector->as<FlatVector<int64_t>>();
    for (auto i = 0; i < numbers->size(); ++i) {
      numbers->set(i, counter++);
    }
  }

  void makeSeq(
      RowVectorPtr row,
      int64_t mod = std::numeric_limits<int64_t>::max(),
      bool notNull = true,
      int32_t begin = -1,
      int32_t end = -1,
      int64_t roundTo = 1) {
    if (begin == -1) {
      begin = 0;
    }
    if (end == -1) {
      end = row->type()->size();
    }
    for (auto i = begin; i < end; ++i) {
      auto child = row->childAt(i);
      int32_t counter = 0;
      if (child->as<FlatVector<int64_t>>()) {
        makeNumbers(child, mod, counter);
      }
      if (notNull) {
        child->clearNulls(0, row->size());
      }
    }
  }

  void makeNumbers(VectorPtr vector, int32_t mod, int32_t& counter) {
    auto numbers = vector->as<FlatVector<int64_t>>();
    for (auto i = 0; i < numbers->size(); ++i) {
      numbers->set(i, counter++ % mod);
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
      const std::string& duckDbSql,
      const int32_t numPrefetchSplit = 1) {
    return AssertQueryBuilder(plan, duckDbQueryRunner_)
        .config(
            core::QueryConfig::kMaxSplitPreloadPerDriver,
            std::to_string(numPrefetchSplit))
        .splits(splits)
        .maxDrivers(numDrivers_)
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
  int32_t numBatches_ = FLAGS_wt_num_batches;
  int32_t batchSize_ = FLAGS_wt_batch_size;
  int32_t numDrivers_{1};
  std::vector<RowVectorPtr> vectors_;
  bool dumpData_{false};
  int64_t roundTo_{1};
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

TEST_P(TableScanTest, scanDictAgg) {
  auto type =
      ROW({"c0", "c1", "c2", "c3", "rn"},
          {BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT()});
  auto splits =
      makeData(type, numBatches_, batchSize_, false, [&](RowVectorPtr row) {
        makeRange(row, 10000000000, true);
        int32_t card = 1200;
        for (auto i = 0; i < row->childrenSize(); ++i) {
          auto child = row->childAt(i);
          for (auto i = 0; i < card; ++i) {
          }
          for (auto j = card; j < child->size(); ++j) {
            child->copy(child.get(), j, (j * 121) % card, 1);
          }
          card *= 1.3;
        }
      });

  auto plan =
      PlanBuilder(pool_.get())
          .tableScan(type, {"c0 < 9500000000", "c1 < 9000000000"})
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
      "SELECT sum(c0), sum(c1 + 1), sum(c2 + 2), sum(c3 + c2), sum(rn + 1) FROM tmp where c0 < 9500000000 and c1 < 9000000000");
}

TEST_P(TableScanTest, scanDict) {
  auto type =
      ROW({"c0", "c1", "c2", "c3", "rn"},
          {BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT()});
  int32_t counter = 0;
  auto splits =
      makeData(type, numBatches_, batchSize_, false, [&](RowVectorPtr row) {
        makeRange(row, 10000000000, false);
        int32_t card = 1200;
        makeNumbers(row->childAt(row->childrenSize() - 1), 1000000000, counter);
        for (auto i = 0; i < row->childrenSize() - 1; ++i) {
          auto child = row->childAt(i);
          for (auto i = 0; i < card; ++i) {
          }
          for (auto j = card; j < child->size(); ++j) {
            child->copy(child.get(), j, (j * 121) % card, 1);
          }
          card *= 1.3;
        }
      });

  auto plan = PlanBuilder(pool_.get())
                  .tableScan(type, {"c0 < 9500000000", "c1 < 9000000000"})
                  .planNode();
  auto task = assertQuery(
      plan,
      splits,
      "SELECT c0, c1, (c2), (c3), (rn) FROM tmp where c0 < 9500000000 and c1 < 9000000000");
}

TEST_P(TableScanTest, scanGroupBy) {
  auto type =
      ROW({"c0", "c1", "c2", "c3", "rn"},
          {BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT()});
  auto splits =
      makeData(type, numBatches_, batchSize_, true, [&](RowVectorPtr row) {
        makeRange(row, 1000000000, true);
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
      "SELECT c0, sum(c1 + 1), sum(c2 + 2), sum(c3 + c2), sum(rn + 1) FROM tmp where c1 < 950000000 group by c0");
}

TEST_P(TableScanTest, scan2GroupBy) {
  auto type =
      ROW({"c0", "c1", "c2", "c3", "rn"},
          {BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT()});
  auto splits =
      makeData(type, numBatches_ * 4, batchSize_, true, [&](RowVectorPtr row) {
        makeSeq(row, FLAGS_agg_mod1, true);
      });

  auto plan = PlanBuilder(pool_.get())
                  .tableScan(type)
                  .singleAggregation(
                      {"c0"}, {"sum(1)", "sum(c1)", "sum(c2)", "sum(rn)"})
                  .singleAggregation({}, {"sum(1)", "sum(a0)"})
                  .planNode();
  auto task = assertQuery(
      plan,
      splits,
      "SELECT sum(1), sum(a0) from (SELECT c0, sum(1) as a0, sum(c1), sum(c2), sum(c3), sum(rn) FROM tmp  group by c0) d");
}

TEST_P(TableScanTest, scan3GroupBy) {
  auto type =
      ROW({"c0", "c1", "c2", "c3", "rn"},
          {BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT()});
  auto splits =
      makeData(type, numBatches_ * 4, batchSize_, true, [&](RowVectorPtr row) {
        makeSeq(row, FLAGS_agg_mod1, true);
      });

  auto plan = PlanBuilder(pool_.get())
                  .tableScan(type)
                  .singleAggregation({"c0"}, {"sum(1)", "sum(c1)"})
                  .project({fmt::format("c0 % {} as c0", FLAGS_agg_mod2), "a0"})
                  .singleAggregation({"c0"}, {"sum(1)", "sum(a0)"})
                  .planNode();
  auto task = assertQuery(
      plan,
      splits,
      fmt::format(
          "SELECT c0 % {}, sum(1), sum(a0) from (SELECT c0, sum(1) as a0, sum(c1), sum(c2), sum(c3), sum(rn) FROM tmp  group by c0) d group by c0 % {}",
          FLAGS_agg_mod2,
          FLAGS_agg_mod2));
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    TableScanTests,
    TableScanTest,
    testing::ValuesIn(waveScanTestParams()));
