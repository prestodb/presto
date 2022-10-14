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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/Cursor.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/expression/ExprToSubfieldFilter.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

using facebook::velox::test::BatchMaker;

namespace {
// Returns aggregated spilled stats by 'task'.
Spiller::Stats taskSpilledStats(const exec::Task& task) {
  Spiller::Stats spilledStats;
  auto stats = task.taskStats();
  for (auto& pipeline : stats.pipelineStats) {
    for (auto op : pipeline.operatorStats) {
      spilledStats.spilledBytes += op.spilledBytes;
      spilledStats.spilledRows += op.spilledRows;
      spilledStats.spilledPartitions += op.spilledPartitions;
    }
  }
  return spilledStats;
}
} // namespace

struct TestParam {
  int numDrivers;

  explicit TestParam(int _numDrivers) : numDrivers(_numDrivers) {}
};

class HashJoinTest : public HiveConnectorTestBase {
 protected:
  HashJoinTest() : HashJoinTest(TestParam(1)) {}

  explicit HashJoinTest(const TestParam& param)
      : numDrivers_(param.numDrivers), fuzzer_({}, pool_.get()) {}

  void SetUp() override {
    HiveConnectorTestBase::SetUp();

    // NOTE: use the same row types to ensure generating similar data for both
    // build and probe sides if we use the same random seed to build vectors.
    leftType_ = ROW(
        {{"t_k1", INTEGER()},
         {"t_k2", VARCHAR()},
         {"t_v1", VARCHAR()},
         {"t_v2", INTEGER()}});
    rightType_ = ROW(
        {{"u_k1", INTEGER()},
         {"u_k2", VARCHAR()},
         {"u_v1", VARCHAR()},
         {"u_v2", INTEGER()}});

    // Small batches create more edge cases.
    fuzzerOpts_.vectorSize = 10;
    fuzzerOpts_.nullRatio = 0.1;
    fuzzerOpts_.stringVariableLength = true;
    fuzzerOpts_.containerVariableLength = true;
    fuzzer_.setOptions(fuzzerOpts_);
  }

  static std::vector<std::string> concat(
      const std::vector<std::string>& a,
      const std::vector<std::string>& b) {
    std::vector<std::string> result;
    result.insert(result.end(), a.begin(), a.end());
    result.insert(result.end(), b.begin(), b.end());
    return result;
  }

  void runTest(
      const std::vector<std::string>& leftKeys,
      std::vector<RowVectorPtr>& leftBatches,
      bool isLeftParallelizable,
      const std::vector<std::string>& rightKeys,
      std::vector<RowVectorPtr>& rightBatches,
      bool isRightParallelizable,
      const std::string& referenceQuery,
      const std::string& filter,
      const std::vector<std::string>& outputLayout,
      core::JoinType joinType,
      bool injectSpill) {
    CursorParameters params;
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    params.planNode = PlanBuilder(planNodeIdGenerator)
                          .values(leftBatches, isLeftParallelizable)
                          .hashJoin(
                              leftKeys,
                              rightKeys,
                              PlanBuilder(planNodeIdGenerator)
                                  .values(rightBatches, isRightParallelizable)
                                  .planNode(),
                              filter,
                              outputLayout,
                              joinType)
                          .planNode();
    params.maxDrivers = numDrivers_;

    std::shared_ptr<TempDirectoryPath> spillDirectory;
    if (injectSpill) {
      spillDirectory = exec::test::TempDirectoryPath::create();
      params.queryCtx = core::QueryCtx::createForTest();
      params.queryCtx->setConfigOverridesUnsafe({
          {core::QueryConfig::kTestingSpillPct, "100"},
          {core::QueryConfig::kSpillEnabled, "true"},
          {core::QueryConfig::kJoinSpillEnabled, "true"},
          {core::QueryConfig::kSpillPath, spillDirectory->path},
      });
    }

    auto task = ::assertQuery(
        params, [](auto*) {}, referenceQuery, duckDbQueryRunner_);
    // A quick sanity check for memory usage reporting. Check that peak total
    // memory usage for the hash join node is > 0.
    auto planStats = toPlanStats(task->taskStats());
    auto joinNodeId = params.planNode->id();
    ASSERT_TRUE(planStats.at(joinNodeId).peakMemoryBytes > 0);
    auto spillStats = taskSpilledStats(*task);
    if (injectSpill) {
      EXPECT_GT(spillStats.spilledRows, 0);
      EXPECT_GT(spillStats.spilledBytes, 0);
      EXPECT_GT(spillStats.spilledPartitions, 0);
    } else {
      EXPECT_EQ(spillStats.spilledRows, 0);
      EXPECT_EQ(spillStats.spilledBytes, 0);
      EXPECT_EQ(spillStats.spilledPartitions, 0);
    }
  }

  void testJoin(
      const std::vector<TypePtr>& keyTypes,
      int leftBatchSize,
      int numLeftBatches,
      int rightBatchSize,
      int numRightBatches,
      const std::string& referenceQuery,
      const std::string& filter = "",
      const std::vector<std::string>& outputLayout = {},
      core::JoinType joinType = core::JoinType::kInner,
      bool injectSpill = false) {
    auto leftType = makeRowType(keyTypes, "t_");
    auto rightType = makeRowType(keyTypes, "u_");

    std::vector<RowVectorPtr> leftBatches;
    std::vector<RowVectorPtr> rightBatches;
    auto opts = fuzzerOpts_;
    if (leftBatchSize != 0) {
      opts.vectorSize = leftBatchSize;
      fuzzer_.setOptions(opts);
      for (int i = 0; i < numLeftBatches; ++i) {
        leftBatches.push_back(fuzzer_.fuzzRow(leftType));
      }
    } else {
      for (int i = 0; i < numLeftBatches; ++i) {
        leftBatches.push_back(RowVector::createEmpty(leftType, pool_.get()));
      }
    }

    if (rightBatchSize != 0) {
      opts.vectorSize = rightBatchSize;
      fuzzer_.setOptions(opts);
      for (int i = 0; i < numRightBatches; ++i) {
        rightBatches.push_back(fuzzer_.fuzzRow(rightType));
      }
    } else {
      for (int i = 0; i < numRightBatches; ++i) {
        rightBatches.push_back(RowVector::createEmpty(rightType, pool_.get()));
      }
    }

    testJoin(
        leftType,
        makeKeyNames(keyTypes.size(), "t_"),
        leftBatches,
        rightType,
        makeKeyNames(keyTypes.size(), "u_"),
        rightBatches,
        referenceQuery,
        filter,
        outputLayout,
        joinType,
        injectSpill);
  }

  void testJoin(
      RowTypePtr leftType,
      const std::vector<std::string>& leftKeys,
      std::vector<RowVectorPtr>& leftBatches,
      RowTypePtr rightType,
      const std::vector<std::string>& rightKeys,
      std::vector<RowVectorPtr>& rightBatches,
      const std::string& referenceQuery,
      const std::string& filter = "",
      const std::vector<std::string>& outputLayout = {},
      core::JoinType joinType = core::JoinType::kInner,
      bool injectSpill = false) {
    // NOTE: there is one value node copy per driver thread and if the value
    // node is not parallelizable, then the associated driver pipeline will be
    // single threaded. We will try to test different multithreading
    // combinations of build and probe sides, so populate duckdb with all the
    // data: 'allLeftBatches' and 'allRightBatches'.
    std::vector<RowVectorPtr> allLeftBatches;
    allLeftBatches.reserve(leftBatches.size() * numDrivers_);
    std::vector<RowVectorPtr> allRightBatches;
    allRightBatches.reserve(rightBatches.size() * numDrivers_);
    for (int i = 0; i < numDrivers_; ++i) {
      std::copy(
          leftBatches.begin(),
          leftBatches.end(),
          std::back_inserter(allLeftBatches));
      std::copy(
          rightBatches.begin(),
          rightBatches.end(),
          std::back_inserter(allRightBatches));
    }

    createDuckDbTable("t", allLeftBatches);
    createDuckDbTable("u", allRightBatches);

    struct TestSettings {
      int leftParallelize;
      int rightParallelize;

      std::string debugString() const {
        return fmt::format(
            "leftParallelize: {}, rightParallelize: {}",
            leftParallelize,
            rightParallelize);
      }
    };

    std::vector<TestSettings> testSettings;
    testSettings.push_back({true, true});
    if (numDrivers_ != 1) {
      testSettings.push_back({true, false});
      testSettings.push_back({false, true});
    }

    for (const auto& testData : testSettings) {
      SCOPED_TRACE(fmt::format("{}/{}", testData.debugString(), numDrivers_));

      runTest(
          leftKeys,
          testData.leftParallelize ? leftBatches : allLeftBatches,
          testData.leftParallelize,
          rightKeys,
          testData.rightParallelize ? rightBatches : allRightBatches,
          testData.rightParallelize,
          referenceQuery,
          filter,
          outputLayout.empty() ? concat(leftType->names(), rightType->names())
                               : outputLayout,
          joinType,
          injectSpill);
    }
  }

  static std::vector<std::string> makeKeyNames(
      int cnt,
      const std::string& prefix) {
    std::vector<std::string> names;
    for (int i = 0; i < cnt; ++i) {
      names.push_back(fmt::format("{}k{}", prefix, i));
    }
    return names;
  }

  static RowTypePtr makeRowType(
      const std::vector<TypePtr>& keyTypes,
      const std::string& namePrefix) {
    std::vector<std::string> names = makeKeyNames(keyTypes.size(), namePrefix);
    names.push_back(fmt::format("{}data", namePrefix));

    std::vector<TypePtr> types = keyTypes;
    types.push_back(VARCHAR());

    return ROW(std::move(names), std::move(types));
  }

  static RuntimeMetric getFiltersProduced(
      const std::shared_ptr<Task>& task,
      int operatorIndex) {
    auto stats = task->taskStats().pipelineStats.front().operatorStats;
    return stats[operatorIndex].runtimeStats["dynamicFiltersProduced"];
  }

  static RuntimeMetric getFiltersAccepted(
      const std::shared_ptr<Task>& task,
      int operatorIndex) {
    auto stats = task->taskStats().pipelineStats.front().operatorStats;
    return stats[operatorIndex].runtimeStats["dynamicFiltersAccepted"];
  }

  static RuntimeMetric getReplacedWithFilterRows(
      const std::shared_ptr<Task>& task,
      int operatorIndex) {
    auto stats = task->taskStats().pipelineStats.front().operatorStats;
    return stats[operatorIndex].runtimeStats["replacedWithDynamicFilterRows"];
  }

  static uint64_t getInputPositions(
      const std::shared_ptr<Task>& task,
      int operatorIndex) {
    auto stats = task->taskStats().pipelineStats.front().operatorStats;
    return stats[operatorIndex].inputPositions;
  }

  static uint64_t getOutputPositions(
      const std::shared_ptr<Task>& task,
      const core::PlanNodeId planId,
      const std::string& operatorType) {
    uint64_t count = 0;
    for (const auto& pipelineStat : task->taskStats().pipelineStats) {
      for (const auto& operatorStat : pipelineStat.operatorStats) {
        if (operatorStat.planNodeId == planId &&
            operatorStat.operatorType == operatorType) {
          count += operatorStat.outputPositions;
        }
      }
    }
    return count;
  }

  const int numDrivers_;

  VectorFuzzer::Options fuzzerOpts_;
  VectorFuzzer fuzzer_;

  // The default left and right table types used for test.
  RowTypePtr leftType_;
  RowTypePtr rightType_;
};

class MultiThreadedHashJoinTest
    : public HashJoinTest,
      public testing::WithParamInterface<TestParam> {
 public:
  MultiThreadedHashJoinTest() : HashJoinTest(GetParam()) {}
};

TEST_P(MultiThreadedHashJoinTest, bigintArray) {
  testJoin(
      {BIGINT()},
      1600,
      5,
      1500,
      5,
      "SELECT t_k0, t_data, u_k0, u_data FROM "
      "  t, u "
      "  WHERE t_k0 = u_k0");
}

TEST_P(MultiThreadedHashJoinTest, outOfJoinKeyColumnOrder) {
  const int numLeftBatches = 10;
  std::vector<RowVectorPtr> leftBatches;
  leftBatches.reserve(numLeftBatches);
  for (int i = 0; i < numLeftBatches; ++i) {
    leftBatches.push_back(fuzzer_.fuzzRow(leftType_));
  }
  const int numRightBatches = 15;
  std::vector<RowVectorPtr> rightBatches;
  rightBatches.reserve(numLeftBatches);
  for (int i = 0; i < numLeftBatches; ++i) {
    rightBatches.push_back(fuzzer_.fuzzRow(rightType_));
  }

  testJoin(
      leftType_,
      {"t_k2"},
      leftBatches,
      rightType_,
      {"u_k2"},
      rightBatches,
      "SELECT t_k1, t_k2, u_k1, u_k2, u_v1 FROM "
      "  t, u "
      "  WHERE t_k2 = u_k2",
      "",
      {"t_k1", "t_k2", "u_k1", "u_k2", "u_v1"});
}

TEST_P(MultiThreadedHashJoinTest, emptyBuild) {
  testJoin(
      {BIGINT()},
      1600,
      5,
      0,
      5,
      "SELECT t_k0, t_data, u_k0, u_data FROM "
      "  t, u "
      "  WHERE t_k0 = u_k0");
}

TEST_P(MultiThreadedHashJoinTest, emptyProbe) {
  testJoin(
      {BIGINT()},
      0,
      5,
      1500,
      5,
      "SELECT t_k0, t_data, u_k0, u_data FROM "
      "  t, u "
      "  WHERE t_k0 = u_k0",
      "",
      {},
      core::JoinType::kInner,
      true);
}

TEST_P(MultiThreadedHashJoinTest, normalizedKey) {
  testJoin(
      {BIGINT(), VARCHAR()},
      1600,
      5,
      1500,
      5,
      "SELECT t_k0, t_k1, t_data, u_k0, u_k1, u_data FROM "
      "  t, u "
      "  WHERE t_k0 = u_k0 AND t_k1 = u_k1");
}

TEST_P(MultiThreadedHashJoinTest, normalizedKeyOverflow) {
  testJoin(
      {BIGINT(), VARCHAR(), BIGINT(), BIGINT(), BIGINT(), BIGINT()},
      1600,
      5,
      1500,
      5,
      "SELECT t_k0, t_k1, t_k2, t_k3, t_k4, t_k5, t_data, u_k0, u_k1, u_k2, u_k3, u_k4, u_k5, u_data FROM "
      "  t, u "
      "  WHERE t_k0 = u_k0 AND t_k1 = u_k1 AND t_k2 = u_k2 AND t_k3 = u_k3 AND t_k4 = u_k4 AND t_k5 = u_k5  ");
}

TEST_P(MultiThreadedHashJoinTest, allTypes) {
  testJoin(
      {BIGINT(), VARCHAR(), REAL(), DOUBLE(), INTEGER(), SMALLINT(), TINYINT()},
      1600,
      5,
      1500,
      5,
      "SELECT t_k0, t_k1, t_k2, t_k3, t_k4, t_k5, t_k6, t_data, u_k0, u_k1, u_k2, u_k3, u_k4, u_k5, u_k6, u_data FROM "
      "  t, u "
      "  WHERE t_k0 = u_k0 AND t_k1 = u_k1 AND t_k2 = u_k2 AND t_k3 = u_k3 AND t_k4 = u_k4 AND t_k5 = u_k5 AND t_k6 = u_k6 ");
}

TEST_P(MultiThreadedHashJoinTest, filter) {
  testJoin(
      {BIGINT()},
      1600,
      5,
      1500,
      5,
      "SELECT t_k0, t_data, u_k0, u_data FROM "
      "  t, u "
      "  WHERE t_k0 = u_k0 AND ((t_k0 % 100) + (u_k0 % 100)) % 40 < 20",
      "((t_k0 % 100) + (u_k0 % 100)) % 40 < 20");
}

TEST_P(MultiThreadedHashJoinTest, antiJoinWithNull) {
  struct {
    double leftNullRate;
    double rightNullRate;

    std::string debugString() const {
      return fmt::format(
          "leftNullRate: {}, rightNullRate: {}", leftNullRate, rightNullRate);
    }
  } testSettings[] = {
      {0.0, 1.0}, {0.0, 0.1}, {0.1, 1.0}, {0.1, 0.1}, {1.0, 1.0}, {1.0, 0.1}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    auto fuzzerOpts = fuzzerOpts_;
    fuzzerOpts.nullRatio = testData.leftNullRate;
    fuzzer_.setOptions(fuzzerOpts);
    const int numLeftBatches = 10;
    std::vector<RowVectorPtr> leftBatches;
    leftBatches.reserve(numLeftBatches);
    for (int i = 0; i < numLeftBatches; ++i) {
      leftBatches.push_back(fuzzer_.fuzzRow(leftType_));
    }
    // The first half number of build (right) side batches having no nulls to
    // trigger it later during the processing.
    const int numRightBatches = 50;
    fuzzerOpts.vectorSize = 5;
    fuzzerOpts.nullRatio = 0.0;
    fuzzer_.setOptions(fuzzerOpts);
    std::vector<RowVectorPtr> rightBatches;
    rightBatches.reserve(numRightBatches);
    int i = 0;
    for (; i < numRightBatches / 2; ++i) {
      rightBatches.push_back(fuzzer_.fuzzRow(rightType_));
    }
    fuzzerOpts.nullRatio = testData.rightNullRate;
    fuzzer_.setOptions(fuzzerOpts);
    for (; i < numRightBatches; ++i) {
      rightBatches.push_back(fuzzer_.fuzzRow(rightType_));
    }

    testJoin(
        leftType_,
        {"t_k2"},
        leftBatches,
        rightType_,
        {"u_k2"},
        rightBatches,
        "SELECT t_k1, t_k2 FROM t WHERE t.t_k2 NOT IN (SELECT u_k2 FROM u)",
        "",
        {"t_k1", "t_k2"},
        core::JoinType::kNullAwareAnti);
  }
}

TEST_P(MultiThreadedHashJoinTest, rightSemiJoinWithLargeOutput) {
  const int batchSize = 128;
  // Build the identical left and right vectors to generate large join outputs.
  std::vector<RowVectorPtr> leftVectors;
  for (int i = 0; i < 10; ++i) {
    leftVectors.push_back(makeRowVector(
        {"t0", "t1"},
        {makeFlatVector<int32_t>(batchSize, [](auto row) { return row; }),
         makeFlatVector<int32_t>(batchSize, [](auto row) { return row; })}));
  }
  std::vector<RowVectorPtr> rightVectors;
  for (int i = 0; i < 10; ++i) {
    rightVectors.push_back(makeRowVector(
        {"u0", "u1"},
        {makeFlatVector<int32_t>(batchSize, [](auto row) { return row; }),
         makeFlatVector<int32_t>(batchSize, [](auto row) { return row; })}));
  }

  testJoin(
      ROW({"t0", "t1"}, {INTEGER(), INTEGER()}),
      {"t0"},
      leftVectors,
      ROW({"u0", "u1"}, {INTEGER(), INTEGER()}),
      {"u0"},
      rightVectors,
      "SELECT u.u1 FROM u WHERE u.u0 IN (SELECT t0 FROM t)",
      "",
      {"u1"},
      core::JoinType::kRightSemi);
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    HashJoinTest,
    MultiThreadedHashJoinTest,
    testing::ValuesIn({TestParam{1}, TestParam{4}}));

// TODO: try to parallelize the following test cases if possible.

TEST_F(HashJoinTest, joinSidesDifferentSchema) {
  // In this join, the tables have different schema. LHS table t has schema
  // {INTEGER, VARCHAR, INTEGER}. RHS table u has schema {INTEGER, REAL,
  // INTEGER}. The filter predicate uses
  // a column from the right table  before the left and the corresponding
  // columns at the same channel number(1) have different types. This has been
  // a source of crashes in the join logic.

  size_t batchSize = 100;

  std::vector<std::string> stringVector = {"aaa", "bbb", "ccc", "ddd", "eee"};
  auto leftVectors = makeRowVector({
      makeFlatVector<int32_t>(batchSize, [](auto row) { return row; }),
      makeFlatVector<StringView>(
          batchSize,
          [&](auto row) {
            return StringView(stringVector[row % stringVector.size()]);
          }),
      makeFlatVector<int32_t>(batchSize, [](auto row) { return row; }),
  });
  auto rightVectors = makeRowVector({
      makeFlatVector<int32_t>(batchSize, [](auto row) { return row; }),
      makeFlatVector<double>(batchSize, [](auto row) { return row * 5.0; }),
      makeFlatVector<int32_t>(batchSize, [](auto row) { return row; }),
  });
  createDuckDbTable("t", {leftVectors});
  createDuckDbTable("u", {rightVectors});

  std::string referenceQuery =
      "SELECT t.c0 * t.c2/2 FROM "
      "  t, u "
      "  WHERE t.c0 = u.c0 AND "
      "  u.c2 > 10 AND ltrim(t.c1) = 'a%'";
  // In this hash join the 2 tables have a common key which is the
  // first channel in both tables.
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto planNode =
      PlanBuilder(planNodeIdGenerator)
          .values({leftVectors})
          .project({"c0 AS t_c0", "c1 AS t_c1", "c2 AS t_c2"})
          .hashJoin(
              {"t_c0"},
              {"u_c0"},
              PlanBuilder(planNodeIdGenerator)
                  .values({rightVectors})
                  .project({"c0 AS u_c0", "c1 AS u_c1", "c2 AS u_c2"})
                  .planNode(),
              "u_c2 > 10 AND ltrim(t_c1) = 'a%'",
              {"t_c0", "t_c2"})
          .project({"t_c0 * t_c2/2"})
          .planNode();

  ::assertQuery(planNode, referenceQuery, duckDbQueryRunner_);
}

TEST_F(HashJoinTest, memory) {
  // Measures memory allocation in a 1:n hash join followed by
  // projection and aggregation. We expect vectors to be mostly
  // reused, except for t_k0 + 1, which is a dictionary after the
  // join.
  std::vector<TypePtr> keyTypes = {BIGINT()};
  auto leftType = makeRowType(keyTypes, "t_");
  auto rightType = makeRowType(keyTypes, "u_");

  std::vector<RowVectorPtr> leftBatches;
  std::vector<RowVectorPtr> rightBatches;
  for (auto i = 0; i < 100; ++i) {
    leftBatches.push_back(std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(leftType, 1000, *pool_)));
  }
  for (auto i = 0; i < 10; ++i) {
    rightBatches.push_back(std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rightType, 800, *pool_)));
  }

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  CursorParameters params;
  params.planNode = PlanBuilder(planNodeIdGenerator)
                        .values(leftBatches, true)
                        .hashJoin(
                            makeKeyNames(keyTypes.size(), "t_"),
                            makeKeyNames(keyTypes.size(), "u_"),
                            PlanBuilder(planNodeIdGenerator)
                                .values(rightBatches, true)
                                .planNode(),
                            "",
                            concat(leftType->names(), rightType->names()))
                        .project({"t_k0 % 1000 AS k1", "u_k0 % 1000 AS k2"})
                        .singleAggregation({}, {"sum(k1)", "sum(k2)"})
                        .planNode();
  params.queryCtx = core::QueryCtx::createForTest();
  auto tracker = memory::MemoryUsageTracker::create();
  params.queryCtx->pool()->setMemoryUsageTracker(tracker);
  auto [taskCursor, rows] = readCursor(params, [](Task*) {});
  EXPECT_GT(3'500, tracker->getNumAllocs());
  EXPECT_GT(7'500'000, tracker->getCumulativeBytes());
}

TEST_F(HashJoinTest, lazyVectors) {
  // a dataset of multiple row groups with multiple columns. We create
  // different dictionary wrappings for different columns and load the
  // rows in scope at different times.
  auto leftVectors = makeRowVector(
      {makeFlatVector<int32_t>(30'000, [](auto row) { return row; }),
       makeFlatVector<int64_t>(30'000, [](auto row) { return row % 23; }),
       makeFlatVector<int32_t>(30'000, [](auto row) { return row % 31; }),
       makeFlatVector<StringView>(30'000, [](auto row) {
         return StringView(fmt::format("{}   string", row % 43));
       })});

  auto rightVectors = makeRowVector(
      {makeFlatVector<int32_t>(10'000, [](auto row) { return row * 3; }),
       makeFlatVector<int64_t>(10'000, [](auto row) { return row % 31; })});

  auto leftFile = TempFilePath::create();
  writeToFile(leftFile->path, leftVectors);
  createDuckDbTable("t", {leftVectors});

  auto rightFile = TempFilePath::create();
  writeToFile(rightFile->path, rightVectors);
  createDuckDbTable("u", {rightVectors});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId leftScanId;
  core::PlanNodeId rightScanId;
  auto op = PlanBuilder(planNodeIdGenerator)
                .tableScan(ROW({"c0", "c1"}, {INTEGER(), BIGINT()}))
                .capturePlanNodeId(leftScanId)
                .hashJoin(
                    {"c0"},
                    {"c0"},
                    PlanBuilder(planNodeIdGenerator)
                        .tableScan(ROW({"c0"}, {INTEGER()}))
                        .capturePlanNodeId(rightScanId)
                        .planNode(),
                    "",
                    {"c1"})
                .project({"c1 + 1"})
                .planNode();

  AssertQueryBuilder(op, duckDbQueryRunner_)
      .split(rightScanId, makeHiveConnectorSplit(rightFile->path))
      .split(leftScanId, makeHiveConnectorSplit(leftFile->path))
      .assertResults("SELECT t.c1 + 1 FROM t, u WHERE t.c0 = u.c0");

  planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  op = PlanBuilder(planNodeIdGenerator)
           .tableScan(
               ROW({"c0", "c1", "c2", "c3"},
                   {INTEGER(), BIGINT(), INTEGER(), VARCHAR()}))
           .capturePlanNodeId(leftScanId)
           .filter("c2 < 29")
           .hashJoin(
               {"c0"},
               {"bc0"},
               PlanBuilder(planNodeIdGenerator)
                   .tableScan(ROW({"c0", "c1"}, {INTEGER(), BIGINT()}))
                   .capturePlanNodeId(rightScanId)
                   .project({"c0 as bc0", "c1 as bc1"})
                   .planNode(),
               "(c1 + bc1) % 33 < 27",
               {"c1", "bc1", "c3"})
           .project({"c1 + 1", "bc1", "length(c3)"})
           .planNode();

  AssertQueryBuilder(op, duckDbQueryRunner_)
      .split(rightScanId, makeHiveConnectorSplit(rightFile->path))
      .split(leftScanId, makeHiveConnectorSplit(leftFile->path))
      .assertResults(
          "SELECT t.c1 + 1, U.c1, length(t.c3) FROM t, u "
          "WHERE t.c0 = u.c0 and t.c2 < 29 and (t.c1 + u.c1) % 33 < 27");
}

/// Test hash join where build-side keys come from a small range and allow for
/// array-based lookup instead of a hash table.
TEST_F(HashJoinTest, arrayBasedLookup) {
  auto oddIndices = makeIndices(500, [](auto i) { return 2 * i + 1; });

  auto leftVectors = {
      // Join key vector is flat.
      makeRowVector({
          makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
          makeFlatVector<int64_t>(1'000, [](auto row) { return row; }),
      }),
      // Join key vector is constant. There is a match in the build side.
      makeRowVector({
          BaseVector::createConstant(4, 2'000, pool_.get()),
          makeFlatVector<int64_t>(2'000, [](auto row) { return row; }),
      }),
      // Join key vector is constant. There is no match.
      makeRowVector({
          BaseVector::createConstant(5, 2'000, pool_.get()),
          makeFlatVector<int64_t>(2'000, [](auto row) { return row; }),
      }),
      // Join key vector is a dictionary.
      makeRowVector({
          wrapInDictionary(
              oddIndices,
              500,
              makeFlatVector<int32_t>(1'000, [](auto row) { return row * 4; })),
          makeFlatVector<int64_t>(1'000, [](auto row) { return row; }),
      })};

  // 100 key values in [0, 198] range.
  auto rightVectors = {makeRowVector(
      {makeFlatVector<int32_t>(100, [](auto row) { return row * 2; })})};

  createDuckDbTable("t", {leftVectors});
  createDuckDbTable("u", {rightVectors});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto op =
      PlanBuilder(planNodeIdGenerator)
          .values(leftVectors)
          .hashJoin(
              {"c0"},
              {"c0"},
              PlanBuilder(planNodeIdGenerator).values(rightVectors).planNode(),
              "",
              {"c1"})
          .project({"c1 + 1"})
          .planNode();

  auto task = assertQuery(op, "SELECT t.c1 + 1 FROM t, u WHERE t.c0 = u.c0");
  auto joinStats =
      task->taskStats().pipelineStats.back().operatorStats.back().runtimeStats;
  EXPECT_EQ(101, joinStats["distinctKey0"].sum);
  EXPECT_EQ(200, joinStats["rangeKey0"].sum);
}

TEST_F(HashJoinTest, innerJoinWithEmptyBuild) {
  auto leftVectors = makeRowVector({
      makeFlatVector<int32_t>(
          1'234, [](auto row) { return row % 11; }, nullEvery(13)),
      makeFlatVector<int32_t>(1'234, [](auto row) { return row; }),
  });

  auto rightVectors = makeRowVector({makeFlatVector<int32_t>(
      123, [](auto row) { return row % 5; }, nullEvery(7))});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto op = PlanBuilder(planNodeIdGenerator)
                .values({leftVectors})
                .hashJoin(
                    {"c0"},
                    {"c0"},
                    PlanBuilder(planNodeIdGenerator)
                        .values({rightVectors})
                        .filter("c0 < 0")
                        .planNode(),
                    "",
                    {"c1"},
                    core::JoinType::kInner)
                .planNode();

  assertQueryReturnsEmptyResult(op);
}

TEST_F(HashJoinTest, leftSemiJoin) {
  auto leftVectors = makeRowVector({
      makeFlatVector<int32_t>(
          1'234, [](auto row) { return row % 11; }, nullEvery(13)),
      makeFlatVector<int32_t>(1'234, [](auto row) { return row; }),
  });

  auto rightVectors = makeRowVector({
      makeFlatVector<int32_t>(
          123, [](auto row) { return row % 5; }, nullEvery(7)),
  });

  createDuckDbTable("t", {leftVectors});
  createDuckDbTable("u", {rightVectors});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto op = PlanBuilder(planNodeIdGenerator)
                .values({leftVectors})
                .hashJoin(
                    {"c0"},
                    {"c0"},
                    PlanBuilder(planNodeIdGenerator)
                        .values({rightVectors})
                        .planNode(),
                    "",
                    {"c1"},
                    core::JoinType::kLeftSemi)
                .planNode();

  assertQuery(op, "SELECT t.c1 FROM t WHERE t.c0 IN (SELECT c0 FROM u)");

  // Empty build side.
  planNodeIdGenerator->reset();
  op = PlanBuilder(planNodeIdGenerator)
           .values({leftVectors})
           .hashJoin(
               {"c0"},
               {"c0"},
               PlanBuilder(planNodeIdGenerator)
                   .values({rightVectors})
                   .filter("c0 < 0")
                   .planNode(),
               "",
               {"c1"},
               core::JoinType::kLeftSemi)
           .planNode();

  assertQuery(
      op, "SELECT t.c1 FROM t WHERE t.c0 IN (SELECT c0 FROM u WHERE c0 < 0)");
}

TEST_F(HashJoinTest, leftSemiJoinWithFilter) {
  auto leftVectors = makeRowVector(
      {"t0", "t1"},
      {
          makeFlatVector<int32_t>(1'000, [](auto row) { return row % 11; }),
          makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
      });

  auto rightVectors = makeRowVector(
      {"u0", "u1"},
      {
          makeFlatVector<int32_t>(1'234, [](auto row) { return row % 5; }),
          makeFlatVector<int32_t>(1'234, [](auto row) { return row; }),
      });

  createDuckDbTable("t", {leftVectors});
  createDuckDbTable("u", {rightVectors});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto op = PlanBuilder(planNodeIdGenerator)
                .values({leftVectors})
                .hashJoin(
                    {"t0"},
                    {"u0"},
                    PlanBuilder(planNodeIdGenerator)
                        .values({rightVectors})
                        .planNode(),
                    "",
                    {"t0", "t1"},
                    core::JoinType::kLeftSemi)
                .planNode();

  assertQuery(
      op, "SELECT t.* FROM t WHERE EXISTS (SELECT u0 FROM u WHERE t0 = u0)");

  op = PlanBuilder(planNodeIdGenerator)
           .values({leftVectors})
           .hashJoin(
               {"t0"},
               {"u0"},
               PlanBuilder(planNodeIdGenerator)
                   .values({rightVectors})
                   .planNode(),
               "t1 != u1",
               {"t0", "t1"},
               core::JoinType::kLeftSemi)
           .planNode();

  assertQuery(
      op,
      "SELECT t.* FROM t WHERE EXISTS (SELECT u0, u1 FROM u WHERE t0 = u0 AND t1 <> u1)");
}

TEST_F(HashJoinTest, rightSemiJoin) {
  // leftVector size is greater than rightVector size.
  auto leftVectors = makeRowVector(
      {"u0", "u1"},
      {
          makeFlatVector<int32_t>(
              1'234, [](auto row) { return row % 11; }, nullEvery(13)),
          makeFlatVector<int32_t>(1'234, [](auto row) { return row; }),
      });

  auto rightVectors = makeRowVector(
      {"t0", "t1"},
      {
          makeFlatVector<int32_t>(
              123, [](auto row) { return row % 5; }, nullEvery(7)),
          makeFlatVector<int32_t>(123, [](auto row) { return row; }),
      });

  createDuckDbTable("u", {leftVectors});
  createDuckDbTable("t", {rightVectors});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto op = PlanBuilder(planNodeIdGenerator)
                .values({leftVectors})
                .hashJoin(
                    {"u0"},
                    {"t0"},
                    PlanBuilder(planNodeIdGenerator)
                        .values({rightVectors})
                        .planNode(),
                    "",
                    {"t1"},
                    core::JoinType::kRightSemi)
                .planNode();

  assertQuery(op, "SELECT t.t1 FROM t WHERE t.t0 IN (SELECT u0 FROM u)");

  // Make build side larger to test all rows are returned.
  op =
      PlanBuilder(planNodeIdGenerator)
          .values({rightVectors})
          .hashJoin(
              {"t0"},
              {"u0"},
              PlanBuilder(planNodeIdGenerator).values({leftVectors}).planNode(),
              "",
              {"u1"},
              core::JoinType::kRightSemi)
          .planNode();

  assertQuery(op, "SELECT u.u1 FROM u WHERE u.u0 IN (SELECT t0 FROM t)");

  // Empty build side.
  planNodeIdGenerator->reset();
  op = PlanBuilder(planNodeIdGenerator)
           .values({leftVectors})
           .hashJoin(
               {"u0"},
               {"t0"},
               PlanBuilder(planNodeIdGenerator)
                   .values({rightVectors})
                   .filter("t0 < 0")
                   .planNode(),
               "",
               {"t1"},
               core::JoinType::kRightSemi)
           .planNode();

  auto task = assertQuery(
      op, "SELECT t.t1 FROM t WHERE t.t0 IN (SELECT u0 FROM u) AND t.t0 < 0");
  EXPECT_EQ(getInputPositions(task, 1), 0);
}

TEST_F(HashJoinTest, rightSemiJoinWithFilter) {
  auto leftVectors = makeRowVector(
      {"u0", "u1"},
      {
          makeFlatVector<int32_t>(1'234, [](auto row) { return row; }),
          makeFlatVector<int32_t>(1'234, [](auto row) { return row; }),
      });

  auto rightVectors = makeRowVector(
      {"t0", "t1"},
      {
          makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
          makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
      });

  createDuckDbTable("u", {leftVectors});
  createDuckDbTable("t", {rightVectors});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId hashJoinId;
  auto plan = [&](const std::string& filter) -> core::PlanNodePtr {
    return PlanBuilder(planNodeIdGenerator)
        .values({leftVectors})
        .hashJoin(
            {"u0"},
            {"t0"},
            PlanBuilder(planNodeIdGenerator).values({rightVectors}).planNode(),
            filter,
            {"t0", "t1"},
            core::JoinType::kRightSemi)
        .capturePlanNodeId(hashJoinId)
        .planNode();
  };
  {
    // Always true filter.
    auto task = assertQuery(
        plan("u1 > -1"),
        "SELECT t.* FROM t WHERE EXISTS (SELECT u0 FROM u WHERE t0 = u0 AND u1 > -1)");
    EXPECT_EQ(getOutputPositions(task, hashJoinId, "HashProbe"), 1'000);
  }
  {
    // Always true filter.
    auto task = assertQuery(
        plan("t1 > -1"),
        "SELECT t.* FROM t WHERE EXISTS (SELECT u0 FROM u WHERE t0 = u0 AND t1 > -1)");
    EXPECT_EQ(getOutputPositions(task, hashJoinId, "HashProbe"), 1'000);
  }
  {
    // Always false filter.
    auto task = assertQuery(
        plan("u1 > 100000"),
        "SELECT t.* FROM t WHERE EXISTS (SELECT u0 FROM u WHERE t0 = u0 AND u1 > 100000)");
    EXPECT_EQ(getOutputPositions(task, hashJoinId, "HashProbe"), 0);
  }
  {
    // Always false filter.
    auto task = assertQuery(
        plan("t1 > 100000"),
        "SELECT t.* FROM t WHERE EXISTS (SELECT u0 FROM u WHERE t0 = u0 AND t1 > 100000)");
    EXPECT_EQ(getOutputPositions(task, hashJoinId, "HashProbe"), 0);
  }
  {
    // Selective filter.
    auto task = assertQuery(
        plan("u1 % 5 = 0"),
        "SELECT t.* FROM t WHERE EXISTS (SELECT u0, u1 FROM u WHERE t0 = u0 AND u1 % 5 = 0)");
    EXPECT_EQ(getOutputPositions(task, hashJoinId, "HashProbe"), 200);
  }
}

TEST_F(HashJoinTest, antiJoin) {
  auto leftVectors = makeRowVector({
      makeFlatVector<int32_t>(
          1'000, [](auto row) { return row % 11; }, nullEvery(13)),
      makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
  });

  auto rightVectors = makeRowVector({
      makeFlatVector<int32_t>(
          1'234, [](auto row) { return row % 5; }, nullEvery(7)),
  });

  createDuckDbTable("t", {leftVectors});
  createDuckDbTable("u", {rightVectors});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto op = PlanBuilder(planNodeIdGenerator)
                .values({leftVectors})
                .hashJoin(
                    {"c0"},
                    {"c0"},
                    PlanBuilder(planNodeIdGenerator)
                        .values({rightVectors})
                        .filter("c0 IS NOT NULL")
                        .planNode(),
                    "",
                    {"c1"},
                    core::JoinType::kNullAwareAnti)
                .planNode();

  assertQuery(
      op,
      "SELECT t.c1 FROM t WHERE t.c0 NOT IN (SELECT c0 FROM u WHERE c0 IS NOT NULL)");

  // Empty build side.
  planNodeIdGenerator->reset();
  op = PlanBuilder(planNodeIdGenerator)
           .values({leftVectors})
           .hashJoin(
               {"c0"},
               {"c0"},
               PlanBuilder(planNodeIdGenerator)
                   .values({rightVectors})
                   .filter("c0 < 0")
                   .planNode(),
               "",
               {"c1"},
               core::JoinType::kNullAwareAnti)
           .planNode();

  assertQuery(
      op,
      "SELECT t.c1 FROM t WHERE t.c0 NOT IN (SELECT c0 FROM u WHERE c0 < 0)");

  // Build side with nulls. Anti join always returns nothing.
  planNodeIdGenerator->reset();
  op = PlanBuilder(planNodeIdGenerator)
           .values({leftVectors})
           .hashJoin(
               {"c0"},
               {"c0"},
               PlanBuilder(planNodeIdGenerator)
                   .values({rightVectors})
                   .planNode(),
               "",
               {"c1"},
               core::JoinType::kNullAwareAnti)
           .planNode();

  assertQuery(op, "SELECT t.c1 FROM t WHERE t.c0 NOT IN (SELECT c0 FROM u)");
}

TEST_F(HashJoinTest, nullAwareAntiJoinWithFilter) {
  auto leftVectors = makeRowVector(
      {"t0", "t1"},
      {
          makeFlatVector<int32_t>(1'000, [](auto row) { return row % 11; }),
          makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
      });

  auto rightVectors = makeRowVector(
      {"u0", "u1"},
      {
          makeFlatVector<int32_t>(1'234, [](auto row) { return row % 5; }),
          makeFlatVector<int32_t>(1'234, [](auto row) { return row; }),
      });

  createDuckDbTable("t", {leftVectors});
  createDuckDbTable("u", {rightVectors});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto op = PlanBuilder(planNodeIdGenerator)
                .values({leftVectors})
                .hashJoin(
                    {"t0"},
                    {"u0"},
                    PlanBuilder(planNodeIdGenerator)
                        .values({rightVectors})
                        .planNode(),
                    "",
                    {"t0", "t1"},
                    core::JoinType::kNullAwareAnti)
                .planNode();

  assertQuery(op, "SELECT t.* FROM t WHERE t0 NOT IN (SELECT u0 FROM u)");

  op = PlanBuilder(planNodeIdGenerator)
           .values({leftVectors})
           .hashJoin(
               {"t0"},
               {"u0"},
               PlanBuilder(planNodeIdGenerator)
                   .values({rightVectors})
                   .planNode(),
               "t1 != u1",
               {"t0", "t1"},
               core::JoinType::kNullAwareAnti)
           .planNode();

  assertQuery(
      op,
      "SELECT t.* FROM t WHERE t0 NOT IN (SELECT u0 FROM u WHERE t1 <> u1)");
}

TEST_F(HashJoinTest, nullAwareAntiJoinWithFilterAndNullKey) {
  auto leftVectors = makeRowVector(
      {"t0", "t1"},
      {
          makeNullableFlatVector<int32_t>({std::nullopt, 1, 2}),
          makeFlatVector<int32_t>({0, 1, 2}),
      });
  auto rightVectors = makeRowVector(
      {"u0", "u1"},
      {
          makeNullableFlatVector<int32_t>({std::nullopt, 2, 3}),
          makeFlatVector<int32_t>({0, 2, 3}),
      });
  createDuckDbTable("t", {leftVectors});
  createDuckDbTable("u", {rightVectors});
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  std::vector<std::string> filters = {"u1 > t1", "u1 * t1 > 0"};
  for (const std::string& filter : filters) {
    auto sql = fmt::format(
        "SELECT t.* FROM t WHERE t0 NOT IN (SELECT u0 FROM u WHERE {})",
        filter);
    auto op = PlanBuilder(planNodeIdGenerator)
                  .values({leftVectors})
                  .hashJoin(
                      {"t0"},
                      {"u0"},
                      PlanBuilder(planNodeIdGenerator)
                          .values({rightVectors})
                          .planNode(),
                      filter,
                      {"t0", "t1"},
                      core::JoinType::kNullAwareAnti)
                  .planNode();
    assertQuery(op, sql);
  }
}

TEST_F(HashJoinTest, nullAwareAntiJoinWithFilterOnNullableColumn) {
  auto runTest = [this](
                     const std::string& desc,
                     const std::vector<VectorPtr>& leftColumns,
                     const std::vector<VectorPtr>& rightColumns) {
    SCOPED_TRACE(desc);
    auto left = makeRowVector({"t0", "t1"}, leftColumns);
    auto right = makeRowVector({"u0", "u1"}, rightColumns);
    createDuckDbTable("t", {left});
    createDuckDbTable("u", {right});
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto op =
        PlanBuilder(planNodeIdGenerator)
            .values({left})
            .hashJoin(
                {"t0"},
                {"u0"},
                PlanBuilder(planNodeIdGenerator).values({right}).planNode(),
                "t1 <> u1",
                {"t0", "t1"},
                core::JoinType::kNullAwareAnti)
            .planNode();
    assertQuery(
        op,
        "SELECT t.* FROM t WHERE t0 NOT IN (SELECT u0 FROM u WHERE t1 <> u1)");
  };
  runTest(
      "null filter column",
      {
          makeFlatVector<int32_t>(1'000, [](auto row) { return row % 11; }),
          makeFlatVector<int32_t>(1'000, folly::identity, nullEvery(97)),
      },
      {
          makeFlatVector<int32_t>(1'234, [](auto row) { return row % 5; }),
          makeFlatVector<int32_t>(1'234, folly::identity, nullEvery(91)),
      });
  runTest(
      "null filter and key column",
      {
          makeFlatVector<int32_t>(
              1'000, [](auto row) { return row % 11; }, nullEvery(23)),
          makeFlatVector<int32_t>(1'000, folly::identity, nullEvery(29)),
      },
      {
          makeFlatVector<int32_t>(
              1'234, [](auto row) { return row % 5; }, nullEvery(31)),
          makeFlatVector<int32_t>(1'234, folly::identity, nullEvery(37)),
      });
}

TEST_F(HashJoinTest, dynamicFilters) {
  const int32_t numSplits = 20;
  const int32_t numRowsProbe = 1024;
  const int32_t numRowsBuild = 100;

  std::vector<RowVectorPtr> leftVectors;
  leftVectors.reserve(numSplits);

  auto leftFiles = makeFilePaths(numSplits);

  for (int i = 0; i < numSplits; i++) {
    auto rowVector = makeRowVector({
        makeFlatVector<int32_t>(
            numRowsProbe, [&](auto row) { return row - i * 10; }),
        makeFlatVector<int64_t>(numRowsProbe, [](auto row) { return row; }),
    });
    leftVectors.push_back(rowVector);
    writeToFile(leftFiles[i]->path, rowVector);
  }

  // 100 key values in [35, 233] range.
  auto rightKey = makeFlatVector<int32_t>(
      numRowsBuild, [](auto row) { return 35 + row * 2; });
  auto rightVectors = {makeRowVector({
      rightKey,
      makeFlatVector<int64_t>(numRowsBuild, [](auto row) { return row; }),
  })};

  createDuckDbTable("t", {leftVectors});
  createDuckDbTable("u", {rightVectors});

  auto probeType = ROW({"c0", "c1"}, {INTEGER(), BIGINT()});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  auto buildSide = PlanBuilder(planNodeIdGenerator)
                       .values(rightVectors)
                       .project({"c0 AS u_c0", "c1 AS u_c1"})
                       .planNode();
  auto keyOnlyBuildSide = PlanBuilder(planNodeIdGenerator)
                              .values({makeRowVector({rightKey})})
                              .project({"c0 AS u_c0"})
                              .planNode();

  // Basic push-down.
  {
    // Inner join.
    core::PlanNodeId leftScanId;
    auto op = PlanBuilder(planNodeIdGenerator)
                  .tableScan(probeType)
                  .capturePlanNodeId(leftScanId)
                  .hashJoin(
                      {"c0"},
                      {"u_c0"},
                      buildSide,
                      "",
                      {"c0", "c1", "u_c1"},
                      core::JoinType::kInner)
                  .project({"c0", "c1 + 1", "c1 + u_c1"})
                  .planNode();

    auto task =
        AssertQueryBuilder(op, duckDbQueryRunner_)
            .splits(leftScanId, makeHiveConnectorSplits(leftFiles))
            .assertResults(
                "SELECT t.c0, t.c1 + 1, t.c1 + u.c1 FROM t, u WHERE t.c0 = u.c0");
    EXPECT_EQ(1, getFiltersProduced(task, 1).sum);
    EXPECT_EQ(1, getFiltersAccepted(task, 0).sum);
    EXPECT_EQ(0, getReplacedWithFilterRows(task, 1).sum);
    EXPECT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);

    // Left semi join.
    op = PlanBuilder(planNodeIdGenerator)
             .tableScan(probeType)
             .capturePlanNodeId(leftScanId)
             .hashJoin(
                 {"c0"},
                 {"u_c0"},
                 buildSide,
                 "",
                 {"c0", "c1"},
                 core::JoinType::kLeftSemi)
             .project({"c0", "c1 + 1"})
             .planNode();

    task =
        AssertQueryBuilder(op, duckDbQueryRunner_)
            .splits(leftScanId, makeHiveConnectorSplits(leftFiles))
            .assertResults(
                "SELECT t.c0, t.c1 + 1 FROM t WHERE t.c0 IN (SELECT c0 FROM u)");
    EXPECT_EQ(1, getFiltersProduced(task, 1).sum);
    EXPECT_EQ(1, getFiltersAccepted(task, 0).sum);
    EXPECT_GT(getReplacedWithFilterRows(task, 1).sum, 0);
    EXPECT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);

    // Right semi join.
    op = PlanBuilder(planNodeIdGenerator)
             .tableScan(probeType)
             .capturePlanNodeId(leftScanId)
             .hashJoin(
                 {"c0"},
                 {"u_c0"},
                 buildSide,
                 "",
                 {"c0", "c1"},
                 core::JoinType::kRightSemi)
             .project({"c0", "c1 + 1"})
             .planNode();

    task =
        AssertQueryBuilder(op, duckDbQueryRunner_)
            .splits(leftScanId, makeHiveConnectorSplits(leftFiles))
            .assertResults(
                "SELECT t.c0, t.c1 + 1 FROM t WHERE t.c0 IN (SELECT c0 FROM u)");
    EXPECT_EQ(1, getFiltersProduced(task, 1).sum);
    EXPECT_EQ(1, getFiltersAccepted(task, 0).sum);
    EXPECT_GT(getReplacedWithFilterRows(task, 1).sum, 0);
    EXPECT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
  }
  // Basic push-down with column names projected out of the table scan having
  // different names than column names in the files.
  {
    auto scanOutputType = ROW({"a", "b"}, {INTEGER(), BIGINT()});
    ColumnHandleMap assignments;
    assignments["a"] = regularColumn("c0", INTEGER());
    assignments["b"] = regularColumn("c1", BIGINT());

    core::PlanNodeId leftScanId;
    auto op =
        PlanBuilder(planNodeIdGenerator)
            .tableScan(
                scanOutputType,
                makeTableHandle(common::test::SubfieldFiltersBuilder().build()),
                assignments)
            .capturePlanNodeId(leftScanId)
            .hashJoin({"a"}, {"u_c0"}, buildSide, "", {"a", "b", "u_c1"})
            .project({"a", "b + 1", "b + u_c1"})
            .planNode();

    auto task =
        AssertQueryBuilder(op, duckDbQueryRunner_)
            .splits(leftScanId, makeHiveConnectorSplits(leftFiles))
            .assertResults(
                "SELECT t.c0, t.c1 + 1, t.c1 + u.c1 FROM t, u WHERE t.c0 = u.c0");
    EXPECT_EQ(1, getFiltersProduced(task, 1).sum);
    EXPECT_EQ(1, getFiltersAccepted(task, 0).sum);
    EXPECT_EQ(0, getReplacedWithFilterRows(task, 1).sum);
    EXPECT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
  }

  // Push-down that requires merging filters.
  {
    core::PlanNodeId leftScanId;
    auto op = PlanBuilder(planNodeIdGenerator)
                  .tableScan(probeType, {"c0 < 500::INTEGER"})
                  .capturePlanNodeId(leftScanId)
                  .hashJoin({"c0"}, {"u_c0"}, buildSide, "", {"c1", "u_c1"})
                  .project({"c1 + u_c1"})
                  .planNode();

    auto task =
        AssertQueryBuilder(op, duckDbQueryRunner_)
            .splits(leftScanId, makeHiveConnectorSplits(leftFiles))
            .assertResults(
                "SELECT t.c1 + u.c1 FROM t, u WHERE t.c0 = u.c0 AND t.c0 < 500");
    EXPECT_EQ(1, getFiltersProduced(task, 1).sum);
    EXPECT_EQ(1, getFiltersAccepted(task, 0).sum);
    EXPECT_EQ(0, getReplacedWithFilterRows(task, 1).sum);
    EXPECT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
  }

  // Push-down that turns join into a no-op.
  {
    core::PlanNodeId leftScanId;
    auto op =
        PlanBuilder(planNodeIdGenerator)
            .tableScan(probeType)
            .capturePlanNodeId(leftScanId)
            .hashJoin({"c0"}, {"u_c0"}, keyOnlyBuildSide, "", {"c0", "c1"})
            .project({"c0", "c1 + 1"})
            .planNode();

    auto task =
        AssertQueryBuilder(op, duckDbQueryRunner_)
            .splits(leftScanId, makeHiveConnectorSplits(leftFiles))
            .assertResults("SELECT t.c0, t.c1 + 1 FROM t, u WHERE t.c0 = u.c0");
    EXPECT_EQ(1, getFiltersProduced(task, 1).sum);
    EXPECT_EQ(1, getFiltersAccepted(task, 0).sum);
    EXPECT_EQ(getReplacedWithFilterRows(task, 1).sum, numRowsBuild * numSplits);
    EXPECT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
  }

  // Push-down that turns join into a no-op with output having a different
  // number of columns than the input.
  {
    core::PlanNodeId probeScanId;
    auto op = PlanBuilder(planNodeIdGenerator)
                  .tableScan(probeType)
                  .capturePlanNodeId(probeScanId)
                  .hashJoin({"c0"}, {"u_c0"}, keyOnlyBuildSide, "", {"c0"})
                  .planNode();

    auto task =
        AssertQueryBuilder(op, duckDbQueryRunner_)
            .splits(probeScanId, makeHiveConnectorSplits(leftFiles))
            .assertResults("SELECT t.c0 FROM t JOIN u ON (t.c0 = u.c0)");
    EXPECT_EQ(1, getFiltersProduced(task, 1).sum);
    EXPECT_EQ(1, getFiltersAccepted(task, 0).sum);
    EXPECT_EQ(getReplacedWithFilterRows(task, 1).sum, numRowsBuild * numSplits);
    EXPECT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
  }

  // Push-down that requires merging filters and turns join into a no-op.
  {
    core::PlanNodeId leftScanId;
    auto op = PlanBuilder(planNodeIdGenerator)
                  .tableScan(probeType, {"c0 < 500::INTEGER"})
                  .capturePlanNodeId(leftScanId)
                  .hashJoin({"c0"}, {"u_c0"}, keyOnlyBuildSide, "", {"c1"})
                  .project({"c1 + 1"})
                  .planNode();

    auto task =
        AssertQueryBuilder(op, duckDbQueryRunner_)
            .splits(leftScanId, makeHiveConnectorSplits(leftFiles))
            .assertResults(
                "SELECT t.c1 + 1 FROM t, u WHERE t.c0 = u.c0 AND t.c0 < 500");
    EXPECT_EQ(1, getFiltersProduced(task, 1).sum);
    EXPECT_EQ(1, getFiltersAccepted(task, 0).sum);
    EXPECT_GT(getReplacedWithFilterRows(task, 1).sum, 0);
    EXPECT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
  }

  // Push-down with highly selective filter in the scan.
  {
    // Inner join.
    core::PlanNodeId leftScanId;
    auto op =
        PlanBuilder(planNodeIdGenerator)
            .tableScan(probeType, {"c0 < 200::INTEGER"})
            .capturePlanNodeId(leftScanId)
            .hashJoin(
                {"c0"}, {"u_c0"}, buildSide, "", {"c1"}, core::JoinType::kInner)
            .project({"c1 + 1"})
            .planNode();

    auto task =
        AssertQueryBuilder(op, duckDbQueryRunner_)
            .splits(leftScanId, makeHiveConnectorSplits(leftFiles))
            .assertResults(
                "SELECT t.c1 + 1 FROM t, u WHERE t.c0 = u.c0 AND t.c0 < 200");
    EXPECT_EQ(1, getFiltersProduced(task, 1).sum);
    EXPECT_EQ(1, getFiltersAccepted(task, 0).sum);
    EXPECT_GT(getReplacedWithFilterRows(task, 1).sum, 0);
    EXPECT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);

    // Left semi join.
    op = PlanBuilder(planNodeIdGenerator)
             .tableScan(probeType, {"c0 < 200::INTEGER"})
             .capturePlanNodeId(leftScanId)
             .hashJoin(
                 {"c0"},
                 {"u_c0"},
                 buildSide,
                 "",
                 {"c1"},
                 core::JoinType::kLeftSemi)
             .project({"c1 + 1"})
             .planNode();

    task =
        AssertQueryBuilder(op, duckDbQueryRunner_)
            .splits(leftScanId, makeHiveConnectorSplits(leftFiles))
            .assertResults(
                "SELECT t.c1 + 1 FROM t WHERE t.c0 IN (SELECT c0 FROM u) AND t.c0 < 200");
    EXPECT_EQ(1, getFiltersProduced(task, 1).sum);
    EXPECT_EQ(1, getFiltersAccepted(task, 0).sum);
    EXPECT_GT(getReplacedWithFilterRows(task, 1).sum, 0);
    EXPECT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);

    // Right semi join.
    op = PlanBuilder(planNodeIdGenerator)
             .tableScan(probeType, {"c0 < 200::INTEGER"})
             .capturePlanNodeId(leftScanId)
             .hashJoin(
                 {"c0"},
                 {"u_c0"},
                 buildSide,
                 "",
                 {"c1"},
                 core::JoinType::kRightSemi)
             .project({"c1 + 1"})
             .planNode();

    task =
        AssertQueryBuilder(op, duckDbQueryRunner_)
            .splits(leftScanId, makeHiveConnectorSplits(leftFiles))
            .assertResults(
                "SELECT t.c1 + 1 FROM t WHERE t.c0 IN (SELECT c0 FROM u) AND t.c0 < 200");
    EXPECT_EQ(1, getFiltersProduced(task, 1).sum);
    EXPECT_EQ(1, getFiltersAccepted(task, 0).sum);
    EXPECT_GT(getReplacedWithFilterRows(task, 1).sum, 0);
    EXPECT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
  }

  // Disable filter push-down by using values in place of scan.
  {
    auto op = PlanBuilder(planNodeIdGenerator)
                  .values(leftVectors)
                  .hashJoin({"c0"}, {"u_c0"}, buildSide, "", {"c1"})
                  .project({"c1 + 1"})
                  .planNode();

    auto task = assertQuery(op, "SELECT t.c1 + 1 FROM t, u WHERE t.c0 = u.c0");
    EXPECT_EQ(0, getFiltersProduced(task, 1).sum);
    EXPECT_EQ(0, getFiltersAccepted(task, 0).sum);
    EXPECT_EQ(numRowsProbe * numSplits, getInputPositions(task, 1));
  }

  // Disable filter push-down by using an expression as the join key on the
  // probe side.
  {
    core::PlanNodeId leftScanId;
    auto op = PlanBuilder(planNodeIdGenerator)
                  .tableScan(probeType)
                  .capturePlanNodeId(leftScanId)
                  .project({"cast(c0 + 1 as integer) AS t_key", "c1"})
                  .hashJoin({"t_key"}, {"u_c0"}, buildSide, "", {"c1"})
                  .project({"c1 + 1"})
                  .planNode();

    auto task =
        AssertQueryBuilder(op, duckDbQueryRunner_)
            .splits(leftScanId, makeHiveConnectorSplits(leftFiles))
            .assertResults("SELECT t.c1 + 1 FROM t, u WHERE (t.c0 + 1) = u.c0");
    EXPECT_EQ(0, getFiltersProduced(task, 1).sum);
    EXPECT_EQ(0, getFiltersAccepted(task, 0).sum);
    EXPECT_EQ(numRowsProbe * numSplits, getInputPositions(task, 1));
  }
}

TEST_F(HashJoinTest, leftJoin) {
  // Left side keys are [0, 1, 2,..10].
  // Use 3-rd column as row number to allow for asserting the order of results.
  auto leftVectors = {
      makeRowVector(
          {"c0", "c1", "row_number"},
          {
              makeFlatVector<int32_t>(
                  1'234, [](auto row) { return row % 11; }, nullEvery(13)),
              makeFlatVector<int32_t>(1'234, [](auto row) { return row; }),
              makeFlatVector<int32_t>(1'234, [](auto row) { return row; }),
          }),
      makeRowVector(
          {"c0", "c1", "row_number"},
          {
              makeFlatVector<int32_t>(
                  2'222,
                  [](auto row) { return (row + 3) % 11; },
                  nullEvery(13)),
              makeFlatVector<int32_t>(2'222, [](auto row) { return row; }),
              makeFlatVector<int32_t>(
                  2'222, [](auto row) { return 1'234 + row; }),
          }),
  };

  // Right side keys are [0, 1, 2, 3, 4].
  auto rightVectors = makeRowVector({
      makeFlatVector<int32_t>(
          123, [](auto row) { return row % 5; }, nullEvery(7)),
      makeFlatVector<int32_t>(
          123, [](auto row) { return -111 + row * 2; }, nullEvery(7)),
  });

  createDuckDbTable("t", leftVectors);
  createDuckDbTable("u", {rightVectors});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto buildSide = PlanBuilder(planNodeIdGenerator)
                       .values({rightVectors})
                       .project({"c0 AS u_c0", "c1 AS u_c1"})
                       .planNode();

  auto op = PlanBuilder(planNodeIdGenerator)
                .values(leftVectors)
                .hashJoin(
                    {"c0"},
                    {"u_c0"},
                    buildSide,
                    "",
                    {"row_number", "c0", "c1", "u_c1"},
                    core::JoinType::kLeft)
                .planNode();

  assertQueryOrdered(
      op,
      "SELECT t.row_number, t.c0, t.c1, u.c1 FROM t LEFT JOIN u ON t.c0 = u.c0 ORDER BY 1",
      {0});

  // Empty build side.
  auto emptyBuildSide = PlanBuilder(planNodeIdGenerator)
                            .values({rightVectors})
                            .filter("c0 < 0")
                            .project({"c0 AS u_c0", "c1 AS u_c1"})
                            .planNode();

  op = PlanBuilder(planNodeIdGenerator)
           .values(leftVectors)
           .hashJoin(
               {"c0"},
               {"u_c0"},
               emptyBuildSide,
               "",
               {"row_number", "c1"},
               core::JoinType::kLeft)
           .planNode();

  assertQueryOrdered(
      op,
      "SELECT t.row_number, t.c1 FROM t LEFT JOIN (SELECT c0 FROM u WHERE c0 < 0) u ON t.c0 = u.c0 ORDER BY 1",
      {0});

  // No match on the join key
  auto noMatchBuildSide =
      PlanBuilder(planNodeIdGenerator)
          .values({rightVectors})
          .project({"c0 - 123::INTEGER AS u_c0", "c1 AS u_c1"})
          .planNode();
  op = PlanBuilder(planNodeIdGenerator)
           .values(leftVectors)
           .hashJoin(
               {"c0"},
               {"u_c0"},
               noMatchBuildSide,
               "",
               {"row_number", "c0", "u_c1"},
               core::JoinType::kLeft)
           .planNode();

  assertQueryOrdered(
      op,
      "SELECT t.row_number, t.c0, u.c1 FROM t LEFT JOIN (SELECT c0 - 123::INTEGER AS u_c0, c1 FROM u) u ON t.c0 = u.u_c0 ORDER BY 1",
      {0});

  // All left-side rows have a match on the build side.
  op = PlanBuilder(planNodeIdGenerator)
           .values(leftVectors)
           .filter("c0 < 5")
           .hashJoin(
               {"c0"},
               {"u_c0"},
               buildSide,
               "",
               {"row_number", "c0", "c1", "u_c1"},
               core::JoinType::kLeft)
           .planNode();

  assertQueryOrdered(
      op,
      "SELECT t.row_number, t.c0, t.c1, u.c1 FROM (SELECT * FROM t WHERE c0 < 5) t"
      " LEFT JOIN u ON t.c0 = u.c0 ORDER BY t.row_number",
      {0});

  // Additional filter.
  op = PlanBuilder(planNodeIdGenerator)
           .values(leftVectors)
           .hashJoin(
               {"c0"},
               {"u_c0"},
               buildSide,
               "(c1 + u_c1) % 2 = 1",
               {"row_number", "c0", "c1", "u_c1"},
               core::JoinType::kLeft)
           .planNode();

  assertQueryOrdered(
      op,
      "SELECT t.row_number, t.c0, t.c1, u.c1 FROM t LEFT JOIN u ON t.c0 = u.c0 AND (t.c1 + u.c1) % 2 = 1 ORDER BY t.row_number",
      {0});

  // No rows pass the additional filter.
  op = PlanBuilder(planNodeIdGenerator)
           .values(leftVectors)
           .hashJoin(
               {"c0"},
               {"u_c0"},
               buildSide,
               "(c1 + u_c1) % 2  = 3",
               {"row_number", "c0", "c1", "u_c1"},
               core::JoinType::kLeft)
           .planNode();

  assertQueryOrdered(
      op,
      "SELECT t.row_number, t.c0, t.c1, u.c1 FROM t LEFT JOIN u ON t.c0 = u.c0 AND (t.c1 + u.c1) % 2 = 3 ORDER BY t.row_number",
      {0});
}

/// Tests left join with a filter that may evaluate to true, false or null.
/// Makes sure that null filter results are handled correctly, e.g. as if the
/// filter returned false.
TEST_F(HashJoinTest, leftJoinWithNullableFilter) {
  auto leftVectors = {
      makeRowVector({
          makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
          makeNullableFlatVector<int32_t>(
              {10, std::nullopt, 30, std::nullopt, 50}),
      }),
      makeRowVector({
          makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
          makeNullableFlatVector<int32_t>(
              {std::nullopt, 20, 30, std::nullopt, 50}),
      })};
  auto rightVectors = {
      makeRowVector({makeFlatVector<int32_t>({1, 2, 10, 30, 40})}),
  };

  createDuckDbTable("t", leftVectors);
  createDuckDbTable("u", rightVectors);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  auto buildSide = PlanBuilder(planNodeIdGenerator)
                       .values(rightVectors)
                       .project({"c0 AS u_c0"})
                       .planNode();

  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(leftVectors)
                  .hashJoin(
                      {"c0"},
                      {"u_c0"},
                      buildSide,
                      "c1 + u_c0 > 0",
                      {"c0", "c1", "u_c0"},
                      core::JoinType::kLeft)
                  .planNode();

  assertQuery(
      plan, "SELECT * FROM t LEFT JOIN u ON (t.c0 = u.c0 AND t.c1 + u.c0 > 0)");
}

TEST_F(HashJoinTest, rightJoin) {
  // Left side keys are [0, 1, 2,..10].
  auto leftVectors = {
      makeRowVector({
          makeFlatVector<int32_t>(
              1'234, [](auto row) { return row % 11; }, nullEvery(13)),
          makeFlatVector<int32_t>(1'234, [](auto row) { return row; }),
      }),
      makeRowVector({
          makeFlatVector<int32_t>(
              2'222, [](auto row) { return (row + 3) % 11; }, nullEvery(13)),
          makeFlatVector<int32_t>(2'222, [](auto row) { return row; }),
      }),
  };

  // Right side keys are [-3, -2, -1, 0, 1, 2, 3].
  auto rightVectors = makeRowVector({
      makeFlatVector<int32_t>(
          123, [](auto row) { return -3 + row % 7; }, nullEvery(11)),
      makeFlatVector<int32_t>(
          123, [](auto row) { return -111 + row * 2; }, nullEvery(13)),
  });

  createDuckDbTable("t", leftVectors);
  createDuckDbTable("u", {rightVectors});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  auto buildSide = PlanBuilder(planNodeIdGenerator)
                       .values({rightVectors})
                       .project({"c0 AS u_c0", "c1 AS u_c1"})
                       .planNode();

  auto op = PlanBuilder(planNodeIdGenerator)
                .values(leftVectors)
                .hashJoin(
                    {"c0"},
                    {"u_c0"},
                    buildSide,
                    "",
                    {"c0", "c1", "u_c1"},
                    core::JoinType::kRight)
                .planNode();

  assertQuery(op, "SELECT t.c0, t.c1, u.c1 FROM t RIGHT JOIN u ON t.c0 = u.c0");

  // Empty build side.
  auto emptyBuildSide = PlanBuilder(planNodeIdGenerator)
                            .values({rightVectors})
                            .filter("c0 > 100")
                            .project({"c0 AS u_c0", "c1 AS u_c1"})
                            .planNode();
  op = PlanBuilder(planNodeIdGenerator)
           .values(leftVectors)
           .hashJoin(
               {"c0"},
               {"u_c0"},
               emptyBuildSide,
               "",
               {"c1"},
               core::JoinType::kRight)
           .planNode();

  assertQueryReturnsEmptyResult(op);

  // All right-side rows have a match on the left side.
  planNodeIdGenerator->reset();
  op = PlanBuilder(planNodeIdGenerator)
           .values(leftVectors)
           .hashJoin(
               {"c0"},
               {"u_c0"},
               PlanBuilder(planNodeIdGenerator)
                   .values({rightVectors})
                   .filter("c0 >= 0")
                   .project({"c0 AS u_c0", "c1 AS u_c1"})
                   .planNode(),
               "",
               {"c0", "c1", "u_c1"},
               core::JoinType::kRight)
           .planNode();

  assertQuery(
      op,
      "SELECT t.c0, t.c1, u.c1 FROM t"
      " RIGHT JOIN (SELECT * FROM u WHERE c0 >= 0) u ON t.c0 = u.c0");

  // Additional filter.
  op = PlanBuilder(planNodeIdGenerator)
           .values(leftVectors)
           .hashJoin(
               {"c0"},
               {"u_c0"},
               buildSide,
               "(c1 + u_c1) % 2 = 1",
               {"c0", "c1", "u_c1"},
               core::JoinType::kRight)
           .planNode();

  assertQuery(
      op,
      "SELECT t.c0, t.c1, u.c1 FROM t RIGHT JOIN u ON t.c0 = u.c0 AND (t.c1 + u.c1) % 2 = 1");

  // No rows pass the additional filter.
  op = PlanBuilder(planNodeIdGenerator)
           .values(leftVectors)
           .hashJoin(
               {"c0"},
               {"u_c0"},
               buildSide,
               "(c1 + u_c1) % 2 = 3",
               {"c0", "c1", "u_c1"},
               core::JoinType::kRight)
           .planNode();

  assertQuery(
      op,
      "SELECT t.c0, t.c1, u.c1 FROM t RIGHT JOIN u ON t.c0 = u.c0 AND (t.c1 + u.c1) % 2 = 3");
}

TEST_F(HashJoinTest, fullJoin) {
  // Left side keys are [0, 1, 2,..10].
  auto leftVectors = {
      makeRowVector({
          makeFlatVector<int32_t>(
              1'234, [](auto row) { return row % 11; }, nullEvery(13)),
          makeFlatVector<int32_t>(1'234, [](auto row) { return row; }),
      }),
      makeRowVector({
          makeFlatVector<int32_t>(
              2'222, [](auto row) { return (row + 3) % 11; }, nullEvery(13)),
          makeFlatVector<int32_t>(2'222, [](auto row) { return row; }),
      }),
  };

  // Right side keys are [-3, -2, -1, 0, 1, 2, 3].
  auto rightVectors = makeRowVector({
      makeFlatVector<int32_t>(
          123, [](auto row) { return -3 + row % 7; }, nullEvery(11)),
      makeFlatVector<int32_t>(
          123, [](auto row) { return -111 + row * 2; }, nullEvery(13)),
  });

  createDuckDbTable("t", leftVectors);
  createDuckDbTable("u", {rightVectors});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  auto buildSide = PlanBuilder(planNodeIdGenerator)
                       .values({rightVectors})
                       .project({"c0 AS u_c0", "c1 AS u_c1"})
                       .planNode();

  auto op = PlanBuilder(planNodeIdGenerator)
                .values(leftVectors)
                .hashJoin(
                    {"c0"},
                    {"u_c0"},
                    buildSide,
                    "",
                    {"c0", "c1", "u_c1"},
                    core::JoinType::kFull)
                .planNode();

  assertQuery(
      op, "SELECT t.c0, t.c1, u.c1 FROM t FULL OUTER JOIN u ON t.c0 = u.c0");

  // Empty build side.
  planNodeIdGenerator->reset();
  auto emptyBuildSide = PlanBuilder(planNodeIdGenerator)
                            .values({rightVectors})
                            .filter("c0 > 100")
                            .project({"c0 AS u_c0", "c1 AS u_c1"})
                            .planNode();
  op = PlanBuilder(planNodeIdGenerator)
           .values(leftVectors)
           .hashJoin(
               {"c0"},
               {"u_c0"},
               emptyBuildSide,
               "",
               {"c1"},
               core::JoinType::kFull)
           .planNode();

  assertQuery(
      op,
      "SELECT t.c1 FROM t FULL OUTER JOIN (SELECT * FROM u WHERE c0 > 100) u ON t.c0 = u.c0");

  // No match on join key
  auto noMatchBuildSide = PlanBuilder(planNodeIdGenerator)
                              .values({rightVectors})
                              .filter("c0 < 0")
                              .project({"c0 AS u_c0", "c1 AS u_c1"})
                              .planNode();
  op = PlanBuilder(planNodeIdGenerator)
           .values(leftVectors)
           .hashJoin(
               {"c0"},
               {"u_c0"},
               noMatchBuildSide,
               "",
               {"c1"},
               core::JoinType::kFull)
           .planNode();

  assertQuery(
      op,
      "SELECT t.c1 FROM t FULL OUTER JOIN (SELECT * FROM u WHERE c0 < 0) u ON t.c0 = u.c0");

  // Additional filter.
  op = PlanBuilder(planNodeIdGenerator)
           .values(leftVectors)
           .hashJoin(
               {"c0"},
               {"u_c0"},
               buildSide,
               "(c1 + u_c1) % 2 = 1",
               {"c0", "c1", "u_c1"},
               core::JoinType::kFull)
           .planNode();

  assertQuery(
      op,
      "SELECT t.c0, t.c1, u.c1 FROM t FULL OUTER JOIN u ON t.c0 = u.c0 AND (t.c1 + u.c1) % 2 = 1");

  // No rows pass the additional filter.
  op = PlanBuilder(planNodeIdGenerator)
           .values(leftVectors)
           .hashJoin(
               {"c0"},
               {"u_c0"},
               buildSide,
               "(c1 + u_c1) % 2 = 3",
               {"c0", "c1", "u_c1"},
               core::JoinType::kFull)
           .planNode();

  assertQuery(
      op,
      "SELECT t.c0, t.c1, u.c1 FROM t FULL OUTER JOIN u ON t.c0 = u.c0 AND (t.c1 + u.c1) % 2 = 3");
}

// Verify the size of the join output vectors when projecting build-side
// variable-width column.
TEST_F(HashJoinTest, memoryUsage) {
  std::vector<RowVectorPtr> probeData;
  probeData.reserve(10);
  for (auto i = 0; i < 10; i++) {
    probeData.push_back(makeRowVector(
        {makeFlatVector<int32_t>(1'000, [](auto row) { return row % 5; })}));
  }
  auto buildData = makeRowVector(
      {"u_c0", "u_c1"},
      {makeFlatVector<int32_t>({0, 1, 2}),
       makeFlatVector<std::string>({
           std::string(40, 'a'),
           std::string(50, 'b'),
           std::string(30, 'c'),
       })});

  core::PlanNodeId joinNodeId;

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .values(probeData)
          .hashJoin(
              {"c0"},
              {"u_c0"},
              PlanBuilder(planNodeIdGenerator).values({buildData}).planNode(),
              "",
              {"c0", "u_c1"})
          .capturePlanNodeId(joinNodeId)
          .singleAggregation({}, {"count(1)"})
          .planNode();

  auto task = assertQuery(plan, "SELECT 6000");

  auto planStats = toPlanStats(task->taskStats());
  auto outputBytes = planStats.at(joinNodeId).outputBytes;
  ASSERT_LT(outputBytes, 700 * 1024);

  // Verify number of memory allocations. Should not be too high if hash join is
  // able to re-use output vectors that contain build-side data.
  ASSERT_GT(30, task->pool()->getMemoryUsageTracker()->getNumAllocs());
}

/// Test an edge case in producing small output batches where the logic to
/// calculate the set of probe-side rows to load lazy vectors for was triggering
/// a crash.
TEST_F(HashJoinTest, smallOutputBatchSize) {
  // Setup probe data with 50 non-null matching keys followed by 50 null keys:
  // 1, 2, 1, 2,...null, null.
  auto probeData = makeRowVector({
      makeFlatVector<int32_t>(
          100,
          [](auto row) { return 1 + row % 2; },
          [](auto row) { return row > 50; }),
      makeFlatVector<int32_t>(100, [](auto row) { return row * 10; }),
  });

  // Setup build side to match non-null probe side keys.
  auto buildData = makeRowVector(
      {"u_c0", "u_c1"},
      {
          makeFlatVector<int32_t>({1, 2}),
          makeFlatVector<int32_t>({100, 200}),
      });

  createDuckDbTable("t", {probeData});
  createDuckDbTable("u", {buildData});

  // Plan hash inner join with a filter.
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .values({probeData})
          .hashJoin(
              {"c0"},
              {"u_c0"},
              PlanBuilder(planNodeIdGenerator).values({buildData}).planNode(),
              "c1 < u_c1",
              {"c0", "u_c1"})
          .planNode();

  // Use small output batch size to trigger logic for calculating set of
  // probe-side rows to load lazy vectors for.
  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .config(core::QueryConfig::kPreferredOutputBatchSize, std::to_string(10))
      .assertResults("SELECT c0, u_c1 FROM t, u WHERE c0 = u_c0 AND c1 < u_c1");
}
