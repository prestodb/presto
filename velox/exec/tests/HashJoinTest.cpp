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
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

using facebook::velox::test::BatchMaker;

namespace {
struct TestParam {
  int numDrivers;

  explicit TestParam(int _numDrivers) : numDrivers(_numDrivers) {}
};

using SplitInput =
    std::unordered_map<core::PlanNodeId, std::vector<exec::Split>>;

std::function<void(Task* task)> makeAddSplit(
    bool& noMoreSplits,
    SplitInput splits) {
  return [&](Task* task) {
    if (noMoreSplits) {
      return;
    }
    for (auto& [nodeId, nodeSplits] : splits) {
      for (auto& split : nodeSplits) {
        task->addSplit(nodeId, std::move(split));
      }
      task->noMoreSplits(nodeId);
    }
    noMoreSplits = true;
  };
}

// Shuffle batches.
void shuffleBatches(std::vector<RowVectorPtr>& batches) {
  std::default_random_engine rng(1234);
  std::shuffle(std::begin(batches), std::end(batches), rng);
}

// Make batches with random data.
//
// NOTE: if 'batchSize' is 0, then 'numBatches' is ignored and the function
// returns a single empty batch.
std::vector<RowVectorPtr> makeBatches(
    int32_t batchSize,
    int32_t numBatches,
    const RowTypePtr& rowType,
    memory::MemoryPool* pool,
    double nullRatio = 0.1,
    bool shuffle = true) {
  VELOX_CHECK_GE(batchSize, 0);
  VELOX_CHECK_GT(numBatches, 0);

  std::vector<RowVectorPtr> batches;
  batches.reserve(numBatches);
  if (batchSize != 0) {
    VectorFuzzer::Options options;
    options.vectorSize = batchSize;
    options.nullRatio = nullRatio;
    VectorFuzzer fuzzer(options, pool);
    for (int32_t i = 0; i < numBatches; ++i) {
      batches.push_back(fuzzer.fuzzInputRow(rowType));
    }
  } else {
    batches.push_back(RowVector::createEmpty(rowType, pool));
  }
  // NOTE: we generate a number of vectors with a fresh new fuzzer init with
  // the same fix seed. The purpose is to ensure we have sufficient match if
  // we use the row type for both build and probe inputs. Here we shuffle the
  // built vectors to introduce some randomness during the join execution.
  if (shuffle) {
    shuffleBatches(batches);
  }
  return batches;
}

std::vector<RowVectorPtr> makeBatches(
    vector_size_t numBatches,
    std::function<RowVectorPtr(int32_t)> makeVector,
    bool shuffle = true) {
  std::vector<RowVectorPtr> batches;
  batches.reserve(numBatches);
  for (int32_t i = 0; i < numBatches; ++i) {
    batches.push_back(makeVector(i));
  }
  if (shuffle) {
    shuffleBatches(batches);
  }
  return batches;
}

std::vector<RowVectorPtr> mergeBatches(
    std::vector<RowVectorPtr>&& lhs,
    std::vector<RowVectorPtr>&& rhs,
    bool shuffle = false) {
  std::vector<RowVectorPtr> mergedBatches;
  mergedBatches.reserve(lhs.size() + rhs.size());
  std::move(lhs.begin(), lhs.end(), std::back_inserter(mergedBatches));
  std::move(rhs.begin(), rhs.end(), std::back_inserter(mergedBatches));
  if (shuffle) {
    shuffleBatches(mergedBatches);
  }
  return mergedBatches;
}

std::vector<std::string> concat(
    const std::vector<std::string>& a,
    const std::vector<std::string>& b) {
  std::vector<std::string> result;
  result.insert(result.end(), a.begin(), a.end());
  result.insert(result.end(), b.begin(), b.end());
  return result;
}

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

using JoinResultsVerifier = std::function<void(const std::shared_ptr<Task>&)>;

class HashJoinBuilder {
 public:
  HashJoinBuilder(
      memory::MemoryPool& pool,
      DuckDbQueryRunner& duckDbQueryRunner)
      : pool_(pool), duckDbQueryRunner_(duckDbQueryRunner) {
    // Small batches create more edge cases.
    fuzzerOpts_.vectorSize = 10;
    fuzzerOpts_.nullRatio = 0.1;
    fuzzerOpts_.stringVariableLength = true;
    fuzzerOpts_.containerVariableLength = true;
  }

  HashJoinBuilder& numDrivers(int32_t numDrivers) {
    numDrivers_ = numDrivers;
    return *this;
  }

  HashJoinBuilder& planNode(core::PlanNodePtr&& planNode) {
    VELOX_CHECK_NULL(planNode_);
    planNode_ = planNode;
    return *this;
  }

  HashJoinBuilder& keyTypes(const std::vector<TypePtr>& keyTypes) {
    VELOX_CHECK_NULL(probeType_);
    VELOX_CHECK_NULL(buildType_);
    probeType_ = makeProbeType(keyTypes);
    probeKeys_ = makeKeyNames(keyTypes.size(), "t_");
    buildType_ = makeBuildType(keyTypes);
    buildKeys_ = makeKeyNames(keyTypes.size(), "u_");
    return *this;
  }

  HashJoinBuilder& referenceQuery(const std::string& referenceQuery) {
    referenceQuery_ = referenceQuery;
    return *this;
  }

  HashJoinBuilder& probeType(const RowTypePtr& probeType) {
    VELOX_CHECK_NULL(probeType_);
    probeType_ = probeType;
    return *this;
  }

  HashJoinBuilder& probeKeys(const std::vector<std::string>& probeKeys) {
    probeKeys_ = probeKeys;
    return *this;
  }

  HashJoinBuilder& probeFilter(const std::string& probeFilter) {
    probeFilter_ = probeFilter;
    return *this;
  }

  HashJoinBuilder& probeProjections(
      std::vector<std::string>&& probeProjections) {
    probeProjections_ = std::move(probeProjections);
    return *this;
  }

  HashJoinBuilder& probeVectors(int32_t vectorSize, int32_t numVectors) {
    VELOX_CHECK_NOT_NULL(probeType_);
    VELOX_CHECK(probeVectors_.empty());
    auto vectors = makeVectors(vectorSize, numVectors, probeType_);
    return probeVectors(std::move(vectors));
  }

  HashJoinBuilder& probeVectors(std::vector<RowVectorPtr>&& probeVectors) {
    VELOX_CHECK(!probeVectors.empty());
    if (probeType_ == nullptr) {
      probeType_ = asRowType(probeVectors[0]->type());
    }
    probeVectors_ = std::move(probeVectors);
    // NOTE: there is one value node copy per driver thread and if the value
    // node is not parallelizable, then the associated driver pipeline will be
    // single threaded. 'allProbeVectors_' contains the value vectors fed to
    // all the hash probe drivers, which will be used to populate the duckdb as
    // well.
    allProbeVectors_.reserve(probeVectors_.size() * numDrivers_);
    for (int i = 0; i < numDrivers_; ++i) {
      std::copy(
          probeVectors_.begin(),
          probeVectors_.end(),
          std::back_inserter(allProbeVectors_));
    }
    return *this;
  }

  HashJoinBuilder& buildType(const RowTypePtr& buildType) {
    VELOX_CHECK_NULL(buildType_);
    buildType_ = buildType;
    return *this;
  }

  HashJoinBuilder& buildKeys(const std::vector<std::string>& buildKeys) {
    buildKeys_ = buildKeys;
    return *this;
  }

  HashJoinBuilder& buildFilter(const std::string& buildFilter) {
    buildFilter_ = buildFilter;
    return *this;
  }

  HashJoinBuilder& buildProjections(
      std::vector<std::string>&& buildProjections) {
    buildProjections_ = std::move(buildProjections);
    return *this;
  }

  HashJoinBuilder& buildVectors(int32_t vectorSize, int32_t numVectors) {
    VELOX_CHECK_NOT_NULL(buildType_);
    VELOX_CHECK(buildVectors_.empty());
    auto vectors = makeVectors(vectorSize, numVectors, buildType_);
    return buildVectors(std::move(vectors));
  }

  HashJoinBuilder& buildVectors(std::vector<RowVectorPtr>&& buildVectors) {
    VELOX_CHECK(!buildVectors.empty());
    if (buildType_ == nullptr) {
      buildType_ = asRowType(buildVectors[0]->type());
    }
    buildVectors_ = std::move(buildVectors);
    // NOTE: there is one value node copy per driver thread and if the value
    // node is not parallelizable, then the associated driver pipeline will be
    // single threaded. 'allBuildVectors_' contains the value vectors fed to
    // all the hash build drivers, which will be used to populate the duckdb as
    // well.
    allBuildVectors_.reserve(buildVectors_.size() * numDrivers_);
    for (int i = 0; i < numDrivers_; ++i) {
      std::copy(
          buildVectors_.begin(),
          buildVectors_.end(),
          std::back_inserter(allBuildVectors_));
    }
    return *this;
  }

  HashJoinBuilder& joinType(core::JoinType joinType) {
    joinType_ = joinType;
    return *this;
  }

  HashJoinBuilder& joinFilter(const std::string& joinFilter) {
    joinFilter_ = joinFilter;
    return *this;
  }

  HashJoinBuilder& joinOutputLayout(
      std::vector<std::string>&& joinOutputLayout) {
    joinOutputLayout_ = std::move(joinOutputLayout);
    return *this;
  }

  HashJoinBuilder& outputProjections(
      std::vector<std::string>&& outputProjections) {
    outputProjections_ = std::move(outputProjections);
    return *this;
  }

  HashJoinBuilder& inputSplits(const SplitInput& inputSplits) {
    inputSplits_ = inputSplits;
    return *this;
  }

  HashJoinBuilder& config(const std::string& key, const std::string& value) {
    configs_[key] = value;
    return *this;
  }

  HashJoinBuilder& tracker(
      const std::shared_ptr<memory::MemoryUsageTracker>& tracker) {
    tracker_ = tracker;
    return *this;
  }

  HashJoinBuilder& injectSpill(bool injectSpill) {
    injectSpill_ = injectSpill;
    return *this;
  }

  HashJoinBuilder& verifier(JoinResultsVerifier testVerifier) {
    testVerifier_ = std::move(testVerifier);
    return *this;
  }

  void run() {
    if (planNode_ != nullptr) {
      ASSERT_EQ(numDrivers_, 1);
      runTest(planNode_);
      return;
    }

    ASSERT_FALSE(referenceQuery_.empty());
    ASSERT_TRUE(probeType_ != nullptr);
    ASSERT_FALSE(probeKeys_.empty());
    ASSERT_TRUE(buildType_ != nullptr);
    ASSERT_FALSE(buildKeys_.empty());
    ASSERT_EQ(probeKeys_.size(), buildKeys_.size());

    if (joinOutputLayout_.empty()) {
      joinOutputLayout_ = concat(probeType_->names(), buildType_->names());
    }

    createDuckDbTable("t", allProbeVectors_);
    createDuckDbTable("u", allBuildVectors_);

    struct TestSettings {
      int probeParallelize;
      int buildParallelize;

      std::string debugString() const {
        return fmt::format(
            "probeParallelize: {}, buildParallelize: {}",
            probeParallelize,
            buildParallelize);
      }
    };

    std::vector<TestSettings> testSettings;
    testSettings.push_back({
        true,
        true,
    });
    if (numDrivers_ != 1) {
      testSettings.push_back({true, false});
      testSettings.push_back({false, true});
    }

    for (const auto& testData : testSettings) {
      SCOPED_TRACE(fmt::format(
          "{} numDrivers: {}", testData.debugString(), numDrivers_));
      auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
      core::PlanNodePtr planNode =
          PlanBuilder(planNodeIdGenerator)
              .values(
                  testData.probeParallelize ? probeVectors_ : allProbeVectors_,
                  testData.probeParallelize)
              .optionalFilter(probeFilter_)
              .optionalProject(probeProjections_)
              .hashJoin(
                  probeKeys_,
                  buildKeys_,
                  PlanBuilder(planNodeIdGenerator)
                      .values(
                          testData.buildParallelize ? buildVectors_
                                                    : allBuildVectors_,
                          testData.buildParallelize)
                      .optionalFilter(buildFilter_)
                      .optionalProject(buildProjections_)
                      .planNode(),
                  joinFilter_,
                  joinOutputLayout_,
                  joinType_)
              .optionalProject(outputProjections_)
              .planNode();
      runTest(planNode);
    }
  }

 private:
  // NOTE: if 'vectorSize' is 0, then 'numVectors' is ignored and the function
  // returns a single empty batch.
  std::vector<RowVectorPtr> makeVectors(
      vector_size_t vectorSize,
      vector_size_t numVectors,
      RowTypePtr rowType,
      double nullRatio = 0.1,
      bool shuffle = true) {
    VELOX_CHECK_GE(vectorSize, 0);
    VELOX_CHECK_GT(numVectors, 0);

    std::vector<RowVectorPtr> vectors;
    vectors.reserve(numVectors);
    if (vectorSize != 0) {
      fuzzerOpts_.vectorSize = vectorSize;
      fuzzerOpts_.nullRatio = nullRatio;
      VectorFuzzer fuzzer(fuzzerOpts_, &pool_);
      for (int32_t i = 0; i < numVectors; ++i) {
        vectors.push_back(fuzzer.fuzzInputRow(rowType));
      }
    } else {
      vectors.push_back(RowVector::createEmpty(rowType, &pool_));
    }
    // NOTE: we generate a number of vectors with a fresh new fuzzer init with
    // the same fix seed. The purpose is to ensure we have sufficient match if
    // we use the row type for both build and probe inputs. Here we shuffle the
    // built vectors to introduce some randomness during the join execution.
    if (shuffle) {
      shuffleBatches(vectors);
    }
    return vectors;
  }

  static RowTypePtr makeProbeType(const std::vector<TypePtr>& keyTypes) {
    return makeRowType(keyTypes, "t_");
  }

  static RowTypePtr makeBuildType(const std::vector<TypePtr>& keyTypes) {
    return makeRowType(keyTypes, "u_");
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

  static std::vector<std::string> makeKeyNames(
      int32_t cnt,
      const std::string& prefix) {
    std::vector<std::string> names;
    for (int i = 0; i < cnt; ++i) {
      names.push_back(fmt::format("{}k{}", prefix, i));
    }
    return names;
  }

  void createDuckDbTable(
      const std::string& tableName,
      const std::vector<RowVectorPtr>& data) {
    duckDbQueryRunner_.createTable(tableName, data);
  }

  void runTest(const core::PlanNodePtr& planNode) {
    runTest(planNode, false);
    if (injectSpill_) {
      runTest(planNode, true);
    }
  }

  void runTest(const core::PlanNodePtr& planNode, bool injectSpill) {
    AssertQueryBuilder builder(planNode, duckDbQueryRunner_);
    builder.maxDrivers(numDrivers_);
    for (const auto& splitEntry : inputSplits_) {
      builder.splits(splitEntry.first, splitEntry.second);
    }
    auto queryCtx = core::QueryCtx::createForTest();
    std::shared_ptr<TempDirectoryPath> spillDirectory;
    if (injectSpill) {
      spillDirectory = exec::test::TempDirectoryPath::create();
      config(core::QueryConfig::kSpillEnabled, "true");
      config(core::QueryConfig::kJoinSpillEnabled, "true");
      config(core::QueryConfig::kTestingSpillPct, "100");
      config(core::QueryConfig::kSpillPath, spillDirectory->path);
    }
    if (!configs_.empty()) {
      queryCtx->setConfigOverridesUnsafe(std::move(configs_));
    }
    if (tracker_ != nullptr) {
      queryCtx->pool()->setMemoryUsageTracker(tracker_);
    }
    builder.queryCtx(queryCtx);

    SCOPED_TRACE(injectSpill ? "With Spill" : "Without Spill");
    auto task = builder.assertResults(referenceQuery_);
    auto spillStats = taskSpilledStats(*task);
    if (injectSpill) {
      ASSERT_GT(spillStats.spilledRows, 0);
      ASSERT_GT(spillStats.spilledBytes, 0);
      ASSERT_GT(spillStats.spilledPartitions, 0);
    } else {
      ASSERT_EQ(spillStats.spilledRows, 0);
      ASSERT_EQ(spillStats.spilledBytes, 0);
      ASSERT_EQ(spillStats.spilledPartitions, 0);
    }
    // Customized test verification.
    if (testVerifier_ != nullptr) {
      testVerifier_(task);
    }
  }

  VectorFuzzer::Options fuzzerOpts_;
  memory::MemoryPool& pool_;
  DuckDbQueryRunner& duckDbQueryRunner_;

  int32_t numDrivers_{1};
  core::JoinType joinType_{core::JoinType::kInner};
  std::string referenceQuery_;

  RowTypePtr probeType_;
  std::vector<std::string> probeKeys_;
  RowTypePtr buildType_;
  std::vector<std::string> buildKeys_;

  std::string probeFilter_;
  std::vector<std::string> probeProjections_;
  std::vector<RowVectorPtr> probeVectors_;
  std::vector<RowVectorPtr> allProbeVectors_;
  std::string buildFilter_;
  std::vector<std::string> buildProjections_;
  std::vector<RowVectorPtr> buildVectors_;
  std::vector<RowVectorPtr> allBuildVectors_;
  std::string joinFilter_;
  std::vector<std::string> joinOutputLayout_;
  std::vector<std::string> outputProjections_;

  SplitInput inputSplits_;
  core::PlanNodePtr planNode_;
  std::shared_ptr<memory::MemoryUsageTracker> tracker_;
  std::unordered_map<std::string, std::string> configs_;
  bool injectSpill_{false};

  JoinResultsVerifier testVerifier_{};
};

class HashJoinTest : public HiveConnectorTestBase {
 protected:
  friend class HashJoinBuilder;

  HashJoinTest() : HashJoinTest(TestParam(1)) {}

  explicit HashJoinTest(const TestParam& param)
      : numDrivers_(param.numDrivers) {}

  void SetUp() override {
    HiveConnectorTestBase::SetUp();

    probeType_ =
        ROW({{"t_k1", INTEGER()}, {"t_k2", VARCHAR()}, {"t_v1", VARCHAR()}});
    buildType_ =
        ROW({{"u_k1", INTEGER()}, {"u_k2", VARCHAR()}, {"u_v1", INTEGER()}});
  }

  // Make splits with each plan node having a number of source files.
  SplitInput makeSpiltInput(
      const std::vector<core::PlanNodeId>& nodeIds,
      const std::vector<std::vector<std::shared_ptr<TempFilePath>>>& files) {
    VELOX_CHECK_EQ(nodeIds.size(), files.size());
    SplitInput splitInput;
    for (int i = 0; i < nodeIds.size(); ++i) {
      std::vector<exec::Split> splits;
      splits.reserve(files[i].size());
      for (const auto& file : files[i]) {
        splits.push_back(exec::Split(makeHiveConnectorSplit(file->path)));
      }
      splitInput.emplace(nodeIds[i], std::move(splits));
    }
    return splitInput;
  }

  static uint64_t getInputPositions(
      const std::shared_ptr<Task>& task,
      int operatorIndex) {
    auto stats = task->taskStats().pipelineStats.front().operatorStats;
    return stats[operatorIndex].inputPositions;
  }

  static uint64_t getOutputPositions(
      const std::shared_ptr<Task>& task,
      const std::string& operatorType) {
    uint64_t count = 0;
    for (const auto& pipelineStat : task->taskStats().pipelineStats) {
      for (const auto& operatorStat : pipelineStat.operatorStats) {
        if (operatorStat.operatorType == operatorType) {
          count += operatorStat.outputPositions;
        }
      }
    }
    return count;
  }

  static RuntimeMetric getFiltersProduced(
      const std::shared_ptr<Task>& task,
      int operatorIndex) {
    return getOperatorRuntimeStats(
        task, operatorIndex, "dynamicFiltersProduced");
  }

  static RuntimeMetric getFiltersAccepted(
      const std::shared_ptr<Task>& task,
      int operatorIndex) {
    return getOperatorRuntimeStats(
        task, operatorIndex, "dynamicFiltersAccepted");
  }

  static RuntimeMetric getReplacedWithFilterRows(
      const std::shared_ptr<Task>& task,
      int operatorIndex) {
    return getOperatorRuntimeStats(
        task, operatorIndex, "replacedWithDynamicFilterRows");
  }

  static RuntimeMetric getOperatorRuntimeStats(
      const std::shared_ptr<Task>& task,
      int32_t operatorIndex,
      const std::string& statsName) {
    auto stats = task->taskStats().pipelineStats.front().operatorStats;
    return stats[operatorIndex].runtimeStats[statsName];
  }

  const int32_t numDrivers_;

  // The default left and right table types used for test.
  RowTypePtr probeType_;
  RowTypePtr buildType_;
};

class MultiThreadedHashJoinTest
    : public HashJoinTest,
      public testing::WithParamInterface<TestParam> {
 public:
  MultiThreadedHashJoinTest() : HashJoinTest(GetParam()) {}

  static std::vector<TestParam> getTestParams() {
    return std::vector<TestParam>({TestParam{1}, TestParam{4}});
  }
};

TEST_P(MultiThreadedHashJoinTest, bigintArray) {
  HashJoinBuilder(*pool_, duckDbQueryRunner_)
      .numDrivers(numDrivers_)
      .keyTypes({BIGINT()})
      .probeVectors(1600, 5)
      .buildVectors(1500, 5)
      .referenceQuery(
          "SELECT t_k0, t_data, u_k0, u_data FROM t, u WHERE t.t_k0 = u.u_k0")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, outOfJoinKeyColumnOrder) {
  HashJoinBuilder(*pool_, duckDbQueryRunner_)
      .numDrivers(numDrivers_)
      .probeType(probeType_)
      .probeKeys({"t_k2"})
      .probeVectors(5, 10)
      .buildType(buildType_)
      .buildKeys({"u_k2"})
      .buildVectors(5, 15)
      .joinOutputLayout({"t_k1", "t_k2", "u_k1", "u_k2", "u_v1"})
      .referenceQuery(
          "SELECT t_k1, t_k2, u_k1, u_k2, u_v1 FROM t, u WHERE t_k2 = u_k2")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, emptyBuild) {
  HashJoinBuilder(*pool_, duckDbQueryRunner_)
      .numDrivers(numDrivers_)
      .keyTypes({BIGINT()})
      .probeVectors(1600, 5)
      .buildVectors(0, 5)
      .referenceQuery(
          "SELECT t_k0, t_data, u_k0, u_data FROM t, u WHERE t_k0 = u_k0")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, emptyProbe) {
  HashJoinBuilder(*pool_, duckDbQueryRunner_)
      .numDrivers(numDrivers_)
      .keyTypes({BIGINT()})
      .probeVectors(0, 5)
      .buildVectors(1500, 5)
      .referenceQuery(
          "SELECT t_k0, t_data, u_k0, u_data FROM t, u WHERE t_k0 = u_k0")
      .injectSpill(true)
      .run();
}

TEST_P(MultiThreadedHashJoinTest, normalizedKey) {
  HashJoinBuilder(*pool_, duckDbQueryRunner_)
      .numDrivers(numDrivers_)
      .keyTypes({BIGINT(), VARCHAR()})
      .probeVectors(1600, 5)
      .buildVectors(1500, 5)
      .referenceQuery(
          "SELECT t_k0, t_k1, t_data, u_k0, u_k1, u_data FROM t, u WHERE t_k0 = u_k0 AND t_k1 = u_k1")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, normalizedKeyOverflow) {
  HashJoinBuilder(*pool_, duckDbQueryRunner_)
      .keyTypes({BIGINT(), VARCHAR(), BIGINT(), BIGINT(), BIGINT(), BIGINT()})
      .probeVectors(1600, 5)
      .buildVectors(1500, 5)
      .referenceQuery(
          "SELECT t_k0, t_k1, t_k2, t_k3, t_k4, t_k5, t_data, u_k0, u_k1, u_k2, u_k3, u_k4, u_k5, u_data FROM t, u WHERE t_k0 = u_k0 AND t_k1 = u_k1 AND t_k2 = u_k2 AND t_k3 = u_k3 AND t_k4 = u_k4 AND t_k5 = u_k5")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, allTypes) {
  HashJoinBuilder(*pool_, duckDbQueryRunner_)
      .keyTypes(
          {BIGINT(),
           VARCHAR(),
           REAL(),
           DOUBLE(),
           INTEGER(),
           SMALLINT(),
           TINYINT()})
      .probeVectors(1600, 5)
      .buildVectors(1500, 5)
      .referenceQuery(
          "SELECT t_k0, t_k1, t_k2, t_k3, t_k4, t_k5, t_k6, t_data, u_k0, u_k1, u_k2, u_k3, u_k4, u_k5, u_k6, u_data FROM t, u WHERE t_k0 = u_k0 AND t_k1 = u_k1 AND t_k2 = u_k2 AND t_k3 = u_k3 AND t_k4 = u_k4 AND t_k5 = u_k5 AND t_k6 = u_k6")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, filter) {
  HashJoinBuilder(*pool_, duckDbQueryRunner_)
      .numDrivers(numDrivers_)
      .keyTypes({BIGINT()})
      .probeVectors(1600, 5)
      .buildVectors(1500, 5)
      .joinFilter("((t_k0 % 100) + (u_k0 % 100)) % 40 < 20")
      .referenceQuery(
          "SELECT t_k0, t_data, u_k0, u_data FROM t, u WHERE t_k0 = u_k0 AND ((t_k0 % 100) + (u_k0 % 100)) % 40 < 20")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, antiJoinWithNull) {
  struct {
    double probeNullRatio;
    double buildNullRatio;

    std::string debugString() const {
      return fmt::format(
          "probeNullRatio: {}, buildNullRatio: {}",
          probeNullRatio,
          buildNullRatio);
    }
  } testSettings[] = {
      {0.0, 1.0}, {0.0, 0.1}, {0.1, 1.0}, {0.1, 0.1}, {1.0, 1.0}, {1.0, 0.1}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    std::vector<RowVectorPtr> probeVectors =
        makeBatches(5, 3, probeType_, pool_.get(), testData.probeNullRatio);

    // The first half number of build batches having no nulls to trigger it
    // later during the processing.
    std::vector<RowVectorPtr> buildVectors = mergeBatches(
        makeBatches(5, 6, buildType_, pool_.get(), 0.0),
        makeBatches(5, 6, buildType_, pool_.get(), testData.buildNullRatio));

    HashJoinBuilder(*pool_, duckDbQueryRunner_)
        .numDrivers(numDrivers_)
        .probeType(probeType_)
        .probeKeys({"t_k2"})
        .probeVectors(std::move(probeVectors))
        .buildType(buildType_)
        .buildKeys({"u_k2"})
        .buildVectors(std::move(buildVectors))
        .joinType(core::JoinType::kNullAwareAnti)
        .joinOutputLayout({"t_k1", "t_k2"})
        .referenceQuery(
            "SELECT t_k1, t_k2 FROM t WHERE t.t_k2 NOT IN (SELECT u_k2 FROM u)")
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, rightSemiJoinWithLargeOutput) {
  // Build the identical left and right vectors to generate large join outputs.
  std::vector<RowVectorPtr> probeVectors =
      makeBatches(10, [&](uint32_t /*unused*/) {
        return makeRowVector(
            {"t0", "t1"},
            {makeFlatVector<int32_t>(2048, [](auto row) { return row; }),
             makeFlatVector<int32_t>(2048, [](auto row) { return row; })});
      });

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(10, [&](uint32_t /*unused*/) {
        return makeRowVector(
            {"u0", "u1"},
            {makeFlatVector<int32_t>(2048, [](auto row) { return row; }),
             makeFlatVector<int32_t>(2048, [](auto row) { return row; })});
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_)
      .numDrivers(numDrivers_)
      .probeKeys({"t0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u0"})
      .buildVectors(std::move(buildVectors))
      .joinType(core::JoinType::kRightSemi)
      .joinOutputLayout({"u1"})
      .referenceQuery("SELECT u.u1 FROM u WHERE u.u0 IN (SELECT t0 FROM t)")
      .run();
}

/// Test hash join where build-side keys come from a small range and allow for
/// array-based lookup instead of a hash table.
TEST_P(MultiThreadedHashJoinTest, arrayBasedLookup) {
  auto oddIndices = makeIndices(500, [](auto i) { return 2 * i + 1; });

  std::vector<RowVectorPtr> probeVectors = {
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
  std::vector<RowVectorPtr> buildVectors = {
      makeRowVector(
          {makeFlatVector<int32_t>(100, [](auto row) { return row / 2; })}),
      makeRowVector(
          {makeFlatVector<int32_t>(100, [](auto row) { return row * 2; })}),
      makeRowVector(
          {makeFlatVector<int32_t>(100, [](auto row) { return row; })})};

  HashJoinBuilder(*pool_, duckDbQueryRunner_)
      .numDrivers(numDrivers_)
      .probeKeys({"c0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"c0"})
      .buildVectors(std::move(buildVectors))
      .joinOutputLayout({"c1"})
      .outputProjections({"c1 + 1"})
      .referenceQuery("SELECT t.c1 + 1 FROM t, u WHERE t.c0 = u.c0")
      .verifier([&](const std::shared_ptr<Task>& task) {
        auto joinStats = task->taskStats()
                             .pipelineStats.back()
                             .operatorStats.back()
                             .runtimeStats;
        ASSERT_EQ(151, joinStats["distinctKey0"].sum);
        ASSERT_EQ(200, joinStats["rangeKey0"].sum);
      })
      .run();
}

TEST_P(MultiThreadedHashJoinTest, joinSidesDifferentSchema) {
  // In this join, the tables have different schema. LHS table t has schema
  // {INTEGER, VARCHAR, INTEGER}. RHS table u has schema {INTEGER, REAL,
  // INTEGER}. The filter predicate uses
  // a column from the right table  before the left and the corresponding
  // columns at the same channel number(1) have different types. This has been
  // a source of crashes in the join logic.
  size_t batchSize = 100;

  std::vector<std::string> stringVector = {"aaa", "bbb", "ccc", "ddd", "eee"};
  std::vector<RowVectorPtr> probeVectors =
      makeBatches(5, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(batchSize, [](auto row) { return row; }),
            makeFlatVector<StringView>(
                batchSize,
                [&](auto row) {
                  return StringView(stringVector[row % stringVector.size()]);
                }),
            makeFlatVector<int32_t>(batchSize, [](auto row) { return row; }),
        });
      });
  std::vector<RowVectorPtr> buildVectors =
      makeBatches(5, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(batchSize, [](auto row) { return row; }),
            makeFlatVector<double>(
                batchSize, [](auto row) { return row * 5.0; }),
            makeFlatVector<int32_t>(batchSize, [](auto row) { return row; }),
        });
      });

  // In this hash join the 2 tables have a common key which is the
  // first channel in both tables.
  const std::string referenceQuery =
      "SELECT t.c0 * t.c2/2 FROM "
      "  t, u "
      "  WHERE t.c0 = u.c0 AND "
      // TODO: enable ltrim test after the race condition in expression
      // execution gets fixed.
      //"  u.c2 > 10 AND ltrim(t.c1) = 'aaa'";
      "  u.c2 > 10";

  HashJoinBuilder(*pool_, duckDbQueryRunner_)
      .numDrivers(numDrivers_)
      .probeKeys({"t_c0"})
      .probeVectors(std::move(probeVectors))
      .probeProjections({"c0 AS t_c0", "c1 AS t_c1", "c2 AS t_c2"})
      .buildKeys({"u_c0"})
      .buildVectors(std::move(buildVectors))
      .buildProjections({"c0 AS u_c0", "c1 AS u_c1", "c2 AS u_c2"})
      //.joinFilter("u_c2 > 10 AND ltrim(t_c1) == 'aaa'")
      .joinFilter("u_c2 > 10")
      .joinOutputLayout({"t_c0", "t_c2"})
      .outputProjections({"t_c0 * t_c2/2"})
      .referenceQuery(referenceQuery)
      .run();
}

TEST_P(MultiThreadedHashJoinTest, innerJoinWithEmptyBuild) {
  std::vector<RowVectorPtr> probeVectors = makeBatches(5, [&](int32_t batch) {
    return makeRowVector({
        makeFlatVector<int32_t>(
            123,
            [batch](auto row) { return row * 11 / std::max(batch, 1); },
            nullEvery(13)),
        makeFlatVector<int32_t>(1'234, [](auto row) { return row; }),
    });
  });
  std::vector<RowVectorPtr> buildVectors = makeBatches(10, [&](int32_t batch) {
    return makeRowVector({makeFlatVector<int32_t>(
        123,
        [batch](auto row) { return row % std::max(batch, 1); },
        nullEvery(7))});
  });

  HashJoinBuilder(*pool_, duckDbQueryRunner_)
      .numDrivers(numDrivers_)
      .probeKeys({"c0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"c0"})
      .buildVectors(std::move(buildVectors))
      .joinFilter("c0 < 0")
      .joinOutputLayout({"c1"})
      .referenceQuery("SELECT null LIMIT 0")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, leftSemiJoin) {
  std::vector<RowVectorPtr> probeVectors =
      makeBatches(10, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                1'234, [](auto row) { return row % 11; }, nullEvery(13)),
            makeFlatVector<int32_t>(1'234, [](auto row) { return row; }),
        });
      });
  std::vector<RowVectorPtr> buildVectors =
      makeBatches(10, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                123, [](auto row) { return row % 5; }, nullEvery(7)),
        });
      });

  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_)
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"c0"})
        .buildVectors(std::move(testBuildVectors))
        .joinType(core::JoinType::kLeftSemi)
        .joinOutputLayout({"c1"})
        .referenceQuery("SELECT t.c1 FROM t WHERE t.c0 IN (SELECT c0 FROM u)")
        .run();
  }

  // Empty build side.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder joinBuilder(*pool_, duckDbQueryRunner_);
    joinBuilder.numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"c0"})
        .buildVectors(std::move(testBuildVectors))
        .joinType(core::JoinType::kLeftSemi)
        .joinFilter("c0 < 0")
        .joinOutputLayout({"c1"})
        .referenceQuery(
            "SELECT t.c1 FROM t WHERE t.c0 IN (SELECT c0 FROM u WHERE c0 < 0)")
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, leftSemiJoinWithFilter) {
  std::vector<RowVectorPtr> probeVectors = makeBatches(5, [&](int32_t batch) {
    return makeRowVector(
        {"t0", "t1"},
        {
            makeFlatVector<int32_t>(
                250, [batch](auto row) { return row % (11 + batch); }),
            makeFlatVector<int32_t>(
                250, [batch](auto row) { return row * batch; }),
        });
  });

  std::vector<RowVectorPtr> buildVectors = makeBatches(5, [&](int32_t batch) {
    return makeRowVector(
        {"u0", "u1"},
        {
            makeFlatVector<int32_t>(
                123, [batch](auto row) { return row % (5 + batch); }),
            makeFlatVector<int32_t>(
                123, [batch](auto row) { return row * batch; }),
        });
  });

  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_)
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::move(testBuildVectors))
        .joinType(core::JoinType::kLeftSemi)
        .joinOutputLayout({"t0", "t1"})
        .referenceQuery(
            "SELECT t.* FROM t WHERE EXISTS (SELECT u0 FROM u WHERE t0 = u0)")
        .run();
  }

  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_)
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::move(testBuildVectors))
        .joinType(core::JoinType::kLeftSemi)
        .joinFilter("t1 != u1")
        .joinOutputLayout({"t0", "t1"})
        .referenceQuery(
            "SELECT t.* FROM t WHERE EXISTS (SELECT u0, u1 FROM u WHERE t0 = u0 AND t1 <> u1)")
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, rightSemiJoin) {
  // probeVectors size is greater than buildVector size.
  std::vector<RowVectorPtr> probeVectors =
      makeBatches(3, [&](uint32_t /*unused*/) {
        return makeRowVector(
            {"t0", "t1"},
            {makeFlatVector<int32_t>(
                 431, [](auto row) { return row % 11; }, nullEvery(13)),
             makeFlatVector<int32_t>(431, [](auto row) { return row; })});
      });

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](uint32_t /*unused*/) {
        return makeRowVector(
            {"u0", "u1"},
            {
                makeFlatVector<int32_t>(
                    434, [](auto row) { return row % 5; }, nullEvery(7)),
                makeFlatVector<int32_t>(434, [](auto row) { return row; }),
            });
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_)
      .numDrivers(numDrivers_)
      .probeKeys({"t0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u0"})
      .buildVectors(std::move(buildVectors))
      .joinType(core::JoinType::kRightSemi)
      .joinOutputLayout({"u1"})
      .referenceQuery("SELECT u.u1 FROM u WHERE u.u0 IN (SELECT t0 FROM t)")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, rightSemiJoinWithEmptyBuild) {
  // probeVectors size is greater than buildVector size.
  std::vector<RowVectorPtr> probeVectors =
      makeBatches(5, [&](uint32_t /*unused*/) {
        return makeRowVector(
            {"t0", "t1"},
            {makeFlatVector<int32_t>(
                 431, [](auto row) { return row % 11; }, nullEvery(13)),
             makeFlatVector<int32_t>(431, [](auto row) { return row; })});
      });

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(5, [&](uint32_t /*unused*/) {
        return makeRowVector(
            {"u0", "u1"},
            {
                makeFlatVector<int32_t>(
                    434, [](auto row) { return row % 5; }, nullEvery(7)),
                makeFlatVector<int32_t>(434, [](auto row) { return row; }),
            });
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_)
      .numDrivers(numDrivers_)
      .probeKeys({"t0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u0"})
      .buildVectors(std::move(buildVectors))
      .buildFilter("u0 < 0")
      .joinType(core::JoinType::kRightSemi)
      .joinOutputLayout({"u1"})
      .referenceQuery(
          "SELECT u.u1 FROM u WHERE u.u0 IN (SELECT t0 FROM t) AND u.u0 < 0")
      .verifier([&](const std::shared_ptr<Task>& task) {
        ASSERT_EQ(getInputPositions(task, 1), 0);
      })
      .run();
}

TEST_P(MultiThreadedHashJoinTest, rightSemiJoinWithAllMatches) {
  // Make build side larger to test all rows are returned.
  std::vector<RowVectorPtr> probeVectors =
      makeBatches(3, [&](uint32_t /*unused*/) {
        return makeRowVector(
            {"t0", "t1"},
            {
                makeFlatVector<int32_t>(
                    123, [](auto row) { return row % 5; }, nullEvery(7)),
                makeFlatVector<int32_t>(123, [](auto row) { return row; }),
            });
      });

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(5, [&](uint32_t /*unused*/) {
        return makeRowVector(
            {"u0", "u1"},
            {makeFlatVector<int32_t>(
                 314, [](auto row) { return row % 11; }, nullEvery(13)),
             makeFlatVector<int32_t>(314, [](auto row) { return row; })});
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_)
      .numDrivers(numDrivers_)
      .probeKeys({"t0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u0"})
      .buildVectors(std::move(buildVectors))
      .joinType(core::JoinType::kRightSemi)
      .joinOutputLayout({"u1"})
      .referenceQuery("SELECT u.u1 FROM u WHERE u.u0 IN (SELECT t0 FROM t)")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, rightSemiJoinWithFilter) {
  auto probeVectors = makeBatches(4, [&](int32_t /*unused*/) {
    return makeRowVector(
        {"t0", "t1"},
        {
            makeFlatVector<int32_t>(345, [](auto row) { return row; }),
            makeFlatVector<int32_t>(345, [](auto row) { return row; }),
        });
  });

  auto buildVectors = makeBatches(4, [&](int32_t /*unused*/) {
    return makeRowVector(
        {"u0", "u1"},
        {
            makeFlatVector<int32_t>(250, [](auto row) { return row; }),
            makeFlatVector<int32_t>(250, [](auto row) { return row; }),
        });
  });

  // Always true filter.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_)
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::move(testBuildVectors))
        .joinType(core::JoinType::kRightSemi)
        .joinFilter("t1 > -1")
        .joinOutputLayout({"u0", "u1"})
        .referenceQuery(
            "SELECT u.* FROM u WHERE EXISTS (SELECT t0 FROM t WHERE u0 = t0 AND t1 > -1)")
        .verifier([&](const std::shared_ptr<Task>& task) {
          ASSERT_EQ(
              getOutputPositions(task, "HashProbe"), 200 * 5 * numDrivers_);
        })
        .run();
  }

  // Always false filter.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_)
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::move(testBuildVectors))
        .joinType(core::JoinType::kRightSemi)
        .joinFilter("t1 > 100000")
        .joinOutputLayout({"u0", "u1"})
        .referenceQuery(
            "SELECT u.* FROM u WHERE EXISTS (SELECT t0 FROM t WHERE u0 = t0 AND t1 > 100000)")
        .verifier([&](const std::shared_ptr<Task>& task) {
          ASSERT_EQ(getOutputPositions(task, "HashProbe"), 0);
        })
        .run();
  }

  // Selective filter.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_)
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::move(testBuildVectors))
        .joinType(core::JoinType::kRightSemi)
        .joinFilter("t1 % 5 = 0")
        .joinOutputLayout({"u0", "u1"})
        .referenceQuery(
            "SELECT u.* FROM u WHERE EXISTS (SELECT t0 FROM t WHERE u0 = t0 AND t1 % 5 = 0)")
        .verifier([&](const std::shared_ptr<Task>& task) {
          ASSERT_EQ(
              getOutputPositions(task, "HashProbe"), 200 / 5 * 5 * numDrivers_);
        })
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, nullAwareAntiJoin) {
  std::vector<RowVectorPtr> probeVectors =
      makeBatches(5, [&](uint32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                1'000, [](auto row) { return row % 11; }, nullEvery(13)),
            makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
        });
      });

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(5, [&](uint32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                1'234, [](auto row) { return row % 5; }, nullEvery(7)),
        });
      });

  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_)
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"c0"})
        .buildVectors(std::move(testBuildVectors))
        .buildFilter("c0 IS NOT NULL")
        .joinType(core::JoinType::kNullAwareAnti)
        .joinOutputLayout({"c1"})
        .referenceQuery(
            "SELECT t.c1 FROM t WHERE t.c0 NOT IN (SELECT c0 FROM u WHERE c0 IS NOT NULL)")
        .run();
  }

  // Empty build side.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_)
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"c0"})
        .buildVectors(std::move(testBuildVectors))
        .buildFilter("c0 < 0")
        .joinType(core::JoinType::kNullAwareAnti)
        .joinOutputLayout({"c1"})
        .referenceQuery(
            "SELECT t.c1 FROM t WHERE t.c0 NOT IN (SELECT c0 FROM u WHERE c0 < 0)")
        .run();
  }

  // Build side with nulls. Null-aware Anti join always returns nothing.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_)
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"c0"})
        .buildVectors(std::move(testBuildVectors))
        .joinType(core::JoinType::kNullAwareAnti)
        .joinOutputLayout({"c1"})
        .referenceQuery(
            "SELECT t.c1 FROM t WHERE t.c0 NOT IN (SELECT c0 FROM u)")
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, nullAwareAntiJoinWithFilter) {
  std::vector<RowVectorPtr> probeVectors =
      makeBatches(5, [&](int32_t /*unused*/) {
        return makeRowVector(
            {"t0", "t1"},
            {
                makeFlatVector<int32_t>(
                    1'000, [](auto row) { return row % 11; }),
                makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
            });
      });

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(5, [&](int32_t /*unused*/) {
        return makeRowVector(
            {"u0", "u1"},
            {
                makeFlatVector<int32_t>(
                    1'234, [](auto row) { return row % 5; }),
                makeFlatVector<int32_t>(1'234, [](auto row) { return row; }),
            });
      });

  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_)
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::move(testBuildVectors))
        .joinType(core::JoinType::kNullAwareAnti)
        .joinOutputLayout({"t0", "t1"})
        .referenceQuery(
            "SELECT t.* FROM t WHERE NOT EXISTS (SELECT * FROM u WHERE t0 = u0)")
        .run();
  }

  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_)
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::move(testBuildVectors))
        .joinType(core::JoinType::kNullAwareAnti)
        .joinFilter("t1 != u1")
        .joinOutputLayout({"t0", "t1"})
        .referenceQuery(
            "SELECT t.* FROM t WHERE NOT EXISTS (SELECT * FROM u WHERE t0 = u0 AND t1 <> u1)")
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, nullAwareAntiJoinWithFilterAndNullKey) {
  auto probeVectors = makeBatches(4, [&](int32_t /*unused*/) {
    return makeRowVector(
        {"t0", "t1"},
        {
            makeNullableFlatVector<int32_t>({std::nullopt, 1, 2}),
            makeFlatVector<int32_t>({0, 1, 2}),
        });
  });
  auto buildVectors = makeBatches(4, [&](int32_t /*unused*/) {
    return makeRowVector(
        {"u0", "u1"},
        {
            makeNullableFlatVector<int32_t>({std::nullopt, 2, 3}),
            makeFlatVector<int32_t>({0, 2, 3}),
        });
  });

  std::vector<std::string> filters({"u1 > t1", "u1 * t1 > 0"});
  for (const std::string& filter : filters) {
    const auto referenceSql = fmt::format(
        "SELECT t.* FROM t WHERE t0 NOT IN (SELECT u0 FROM u WHERE {})",
        filter);

    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_)
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::move(testBuildVectors))
        .joinType(core::JoinType::kNullAwareAnti)
        .joinFilter(filter)
        .joinOutputLayout({"t0", "t1"})
        .referenceQuery(referenceSql)
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, nullAwareAntiJoinWithFilterOnNullableColumn) {
  // auto probeVectors = makeBatches(4, {"t0", "t1"}, leftColumns);
  const std::string referenceSql =
      "SELECT t.* FROM t WHERE t0 NOT IN (SELECT u0 FROM u WHERE t1 <> u1)";
  const std::string joinFilter = "t1 <> u1";
  {
    SCOPED_TRACE("null filter column");
    auto probeVectors = makeBatches(3, [&](int32_t /*unused*/) {
      return makeRowVector(
          {"t0", "t1"},
          {
              makeFlatVector<int32_t>(200, [](auto row) { return row % 11; }),
              makeFlatVector<int32_t>(200, folly::identity, nullEvery(97)),
          });
    });
    auto buildVectors = makeBatches(3, [&](int32_t /*unused*/) {
      return makeRowVector(
          {"u0", "u1"},
          {
              makeFlatVector<int32_t>(234, [](auto row) { return row % 5; }),
              makeFlatVector<int32_t>(234, folly::identity, nullEvery(91)),
          });
    });
    HashJoinBuilder(*pool_, duckDbQueryRunner_)
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::move(probeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::move(buildVectors))
        .joinType(core::JoinType::kNullAwareAnti)
        .joinFilter(joinFilter)
        .joinOutputLayout({"t0", "t1"})
        .referenceQuery(referenceSql)
        .run();
  }

  {
    SCOPED_TRACE("null filter and key column");
    auto probeVectors = makeBatches(3, [&](int32_t /*unused*/) {
      return makeRowVector(
          {"t0", "t1"},
          {
              makeFlatVector<int32_t>(
                  200, [](auto row) { return row % 11; }, nullEvery(23)),
              makeFlatVector<int32_t>(200, folly::identity, nullEvery(29)),
          });
    });
    auto buildVectors = makeBatches(3, [&](int32_t /*unused*/) {
      return makeRowVector(
          {"u0", "u1"},
          {
              makeFlatVector<int32_t>(
                  234, [](auto row) { return row % 5; }, nullEvery(31)),
              makeFlatVector<int32_t>(234, folly::identity, nullEvery(37)),
          });
    });
    HashJoinBuilder(*pool_, duckDbQueryRunner_)
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::move(probeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::move(buildVectors))
        .joinType(core::JoinType::kNullAwareAnti)
        .joinFilter(joinFilter)
        .joinOutputLayout({"t0", "t1"})
        .referenceQuery(referenceSql)
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, antiJoin) {
  auto probeVectors = makeBatches(4, [&](int32_t /*unused*/) {
    return makeRowVector(
        {"t0", "t1"},
        {
            makeNullableFlatVector<int32_t>({std::nullopt, 1, 2}),
            makeFlatVector<int32_t>({0, 1, 2}),
        });
  });
  auto buildVectors = makeBatches(4, [&](int32_t /*unused*/) {
    return makeRowVector(
        {"u0", "u1"},
        {
            makeNullableFlatVector<int32_t>({std::nullopt, 2, 3}),
            makeFlatVector<int32_t>({0, 2, 3}),
        });
  });
  HashJoinBuilder(*pool_, duckDbQueryRunner_)
      .numDrivers(numDrivers_)
      .probeKeys({"t0"})
      .probeVectors(std::vector<RowVectorPtr>(probeVectors))
      .buildKeys({"u0"})
      .buildVectors(std::vector<RowVectorPtr>(buildVectors))
      .joinType(core::JoinType::kAnti)
      .joinOutputLayout({"t0", "t1"})
      .referenceQuery(
          "SELECT t.* FROM t WHERE NOT EXISTS (SELECT * FROM u WHERE u.u0 = t.t0)")
      .run();
  std::vector<std::string> filters({"u1 > t1", "u1 * t1 > 0"});
  for (const std::string& filter : filters) {
    HashJoinBuilder(*pool_, duckDbQueryRunner_)
        .numDrivers(numDrivers_)
        .probeKeys({"t0"})
        .probeVectors(std::vector<RowVectorPtr>(probeVectors))
        .buildKeys({"u0"})
        .buildVectors(std::vector<RowVectorPtr>(buildVectors))
        .joinType(core::JoinType::kAnti)
        .joinFilter(filter)
        .joinOutputLayout({"t0", "t1"})
        .referenceQuery(fmt::format(
            "SELECT t.* FROM t WHERE NOT EXISTS (SELECT * FROM u WHERE u.u0 = t.t0 AND {})",
            filter))
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, leftJoin) {
  // Left side keys are [0, 1, 2,..10].
  // Use 3-rd column as row number to allow for asserting the order of results.
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector(
                {"c0", "c1", "row_number"},
                {
                    makeFlatVector<int32_t>(
                        77, [](auto row) { return row % 11; }, nullEvery(13)),
                    makeFlatVector<int32_t>(77, [](auto row) { return row; }),
                    makeFlatVector<int32_t>(77, [](auto row) { return row; }),
                });
          }),
      makeBatches(
          2,
          [&](int32_t /*unused*/) {
            return makeRowVector(
                {"c0", "c1", "row_number"},
                {
                    makeFlatVector<int32_t>(
                        97,
                        [](auto row) { return (row + 3) % 11; },
                        nullEvery(13)),
                    makeFlatVector<int32_t>(97, [](auto row) { return row; }),
                    makeFlatVector<int32_t>(
                        97, [](auto row) { return 97 + row; }),
                });
          }),
      true);

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                73, [](auto row) { return row % 5; }, nullEvery(7)),
            makeFlatVector<int32_t>(
                73, [](auto row) { return -111 + row * 2; }, nullEvery(7)),
        });
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_)
      .numDrivers(numDrivers_)
      .probeKeys({"c0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u_c0"})
      .buildVectors(std::move(buildVectors))
      .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
      .joinType(core::JoinType::kLeft)
      .joinOutputLayout({"row_number", "c0", "c1", "u_c1"})
      .referenceQuery(
          "SELECT t.row_number, t.c0, t.c1, u.c1 FROM t LEFT JOIN u ON t.c0 = u.c0")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, leftJoinWithEmptyBuild) {
  // Left side keys are [0, 1, 2,..10].
  // Use 3-rd column as row number to allow for asserting the order of results.
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector(
                {"c0", "c1", "row_number"},
                {
                    makeFlatVector<int32_t>(
                        77, [](auto row) { return row % 11; }, nullEvery(13)),
                    makeFlatVector<int32_t>(77, [](auto row) { return row; }),
                    makeFlatVector<int32_t>(77, [](auto row) { return row; }),
                });
          }),
      makeBatches(
          2,
          [&](int32_t /*unused*/) {
            return makeRowVector(
                {"c0", "c1", "row_number"},
                {
                    makeFlatVector<int32_t>(
                        97,
                        [](auto row) { return (row + 3) % 11; },
                        nullEvery(13)),
                    makeFlatVector<int32_t>(97, [](auto row) { return row; }),
                    makeFlatVector<int32_t>(
                        97, [](auto row) { return 97 + row; }),
                });
          }),
      true);

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                73, [](auto row) { return row % 5; }, nullEvery(7)),
            makeFlatVector<int32_t>(
                73, [](auto row) { return -111 + row * 2; }, nullEvery(7)),
        });
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_)
      .numDrivers(numDrivers_)
      .probeKeys({"c0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u_c0"})
      .buildVectors(std::move(buildVectors))
      .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
      .buildFilter("c0 < 0")
      .joinType(core::JoinType::kLeft)
      .joinOutputLayout({"row_number", "c1"})
      .referenceQuery(
          "SELECT t.row_number, t.c1 FROM t LEFT JOIN (SELECT c0 FROM u WHERE c0 < 0) u ON t.c0 = u.c0")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, leftJoinWithNoJoin) {
  // Left side keys are [0, 1, 2,..10].
  // Use 3-rd column as row number to allow for asserting the order of results.
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector(
                {"c0", "c1", "row_number"},
                {
                    makeFlatVector<int32_t>(
                        77, [](auto row) { return row % 11; }, nullEvery(13)),
                    makeFlatVector<int32_t>(77, [](auto row) { return row; }),
                    makeFlatVector<int32_t>(77, [](auto row) { return row; }),
                });
          }),
      makeBatches(
          2,
          [&](int32_t /*unused*/) {
            return makeRowVector(
                {"c0", "c1", "row_number"},
                {
                    makeFlatVector<int32_t>(
                        97,
                        [](auto row) { return (row + 3) % 11; },
                        nullEvery(13)),
                    makeFlatVector<int32_t>(97, [](auto row) { return row; }),
                    makeFlatVector<int32_t>(
                        97, [](auto row) { return 97 + row; }),
                });
          }),
      true);

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                73, [](auto row) { return row % 5; }, nullEvery(7)),
            makeFlatVector<int32_t>(
                73, [](auto row) { return -111 + row * 2; }, nullEvery(7)),
        });
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_)
      .numDrivers(numDrivers_)
      .probeKeys({"c0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u_c0"})
      .buildVectors(std::move(buildVectors))
      .buildProjections({"c0 - 123::INTEGER AS u_c0", "c1 AS u_c1"})
      .joinType(core::JoinType::kLeft)
      .joinOutputLayout({"row_number", "c0", "u_c1"})
      .referenceQuery(
          "SELECT t.row_number, t.c0, u.c1 FROM t LEFT JOIN (SELECT c0 - 123::INTEGER AS u_c0, c1 FROM u) u ON t.c0 = u.u_c0")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, leftJoinWithAllMatch) {
  // Left side keys are [0, 1, 2,..10].
  // Use 3-rd column as row number to allow for asserting the order of results.
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector(
                {"c0", "c1", "row_number"},
                {
                    makeFlatVector<int32_t>(
                        77, [](auto row) { return row % 11; }, nullEvery(13)),
                    makeFlatVector<int32_t>(77, [](auto row) { return row; }),
                    makeFlatVector<int32_t>(77, [](auto row) { return row; }),
                });
          }),
      makeBatches(
          2,
          [&](int32_t /*unused*/) {
            return makeRowVector(
                {"c0", "c1", "row_number"},
                {
                    makeFlatVector<int32_t>(
                        97,
                        [](auto row) { return (row + 3) % 11; },
                        nullEvery(13)),
                    makeFlatVector<int32_t>(97, [](auto row) { return row; }),
                    makeFlatVector<int32_t>(
                        97, [](auto row) { return 97 + row; }),
                });
          }),
      true);

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                73, [](auto row) { return row % 5; }, nullEvery(7)),
            makeFlatVector<int32_t>(
                73, [](auto row) { return -111 + row * 2; }, nullEvery(7)),
        });
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_)
      .numDrivers(numDrivers_)
      .probeKeys({"c0"})
      .probeVectors(std::move(probeVectors))
      .probeFilter("c0 < 5")
      .buildKeys({"u_c0"})
      .buildVectors(std::move(buildVectors))
      .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
      .joinType(core::JoinType::kLeft)
      .joinOutputLayout({"row_number", "c0", "c1", "u_c1"})
      .referenceQuery(
          "SELECT t.row_number, t.c0, t.c1, u.c1 FROM (SELECT * FROM t WHERE c0 < 5) t LEFT JOIN u ON t.c0 = u.c0")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, leftJoinWithFilter) {
  // Left side keys are [0, 1, 2,..10].
  // Use 3-rd column as row number to allow for asserting the order of results.
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector(
                {"c0", "c1", "row_number"},
                {
                    makeFlatVector<int32_t>(
                        77, [](auto row) { return row % 11; }, nullEvery(13)),
                    makeFlatVector<int32_t>(77, [](auto row) { return row; }),
                    makeFlatVector<int32_t>(77, [](auto row) { return row; }),
                });
          }),
      makeBatches(
          2,
          [&](int32_t /*unused*/) {
            return makeRowVector(
                {"c0", "c1", "row_number"},
                {
                    makeFlatVector<int32_t>(
                        97,
                        [](auto row) { return (row + 3) % 11; },
                        nullEvery(13)),
                    makeFlatVector<int32_t>(97, [](auto row) { return row; }),
                    makeFlatVector<int32_t>(
                        97, [](auto row) { return 97 + row; }),
                });
          }),
      true);

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                73, [](auto row) { return row % 5; }, nullEvery(7)),
            makeFlatVector<int32_t>(
                73, [](auto row) { return -111 + row * 2; }, nullEvery(7)),
        });
      });

  // Additional filter.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_)
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u_c0"})
        .buildVectors(std::move(testBuildVectors))
        .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
        .joinType(core::JoinType::kLeft)
        .joinFilter("(c1 + u_c1) % 2 = 1")
        .joinOutputLayout({"row_number", "c0", "c1", "u_c1"})
        .referenceQuery(
            "SELECT t.row_number, t.c0, t.c1, u.c1 FROM t LEFT JOIN u ON t.c0 = u.c0 AND (t.c1 + u.c1) % 2 = 1")
        .run();
  }

  // No rows pass the additional filter.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_)
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u_c0"})
        .buildVectors(std::move(testBuildVectors))
        .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
        .joinType(core::JoinType::kLeft)
        .joinFilter("(c1 + u_c1) % 2  = 3")
        .joinOutputLayout({"row_number", "c0", "c1", "u_c1"})
        .referenceQuery(
            "SELECT t.row_number, t.c0, t.c1, u.c1 FROM t LEFT JOIN u ON t.c0 = u.c0 AND (t.c1 + u.c1) % 2 = 3")
        .run();
  }
}

/// Tests left join with a filter that may evaluate to true, false or null.
/// Makes sure that null filter results are handled correctly, e.g. as if the
/// filter returned false.
TEST_P(MultiThreadedHashJoinTest, leftJoinWithNullableFilter) {
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          5,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
                makeNullableFlatVector<int32_t>(
                    {10, std::nullopt, 30, std::nullopt, 50}),
            });
          }),
      makeBatches(
          5,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
                makeNullableFlatVector<int32_t>(
                    {std::nullopt, 20, 30, std::nullopt, 50}),
            });
          }),
      true);

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(5, [&](int32_t /*unused*/) {
        return makeRowVector({makeFlatVector<int32_t>({1, 2, 10, 30, 40})});
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_)
      .numDrivers(numDrivers_)
      .probeKeys({"c0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u_c0"})
      .buildVectors(std::move(buildVectors))
      .buildProjections({"c0 AS u_c0"})
      .joinType(core::JoinType::kLeft)
      .joinFilter("c1 + u_c0 > 0")
      .joinOutputLayout({"c0", "c1", "u_c0"})
      .referenceQuery(
          "SELECT * FROM t LEFT JOIN u ON (t.c0 = u.c0 AND t.c1 + u.c0 > 0)")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, rightJoin) {
  // Left side keys are [0, 1, 2,..10].
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    137, [](auto row) { return row % 11; }, nullEvery(13)),
                makeFlatVector<int32_t>(137, [](auto row) { return row; }),
            });
          }),
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    234,
                    [](auto row) { return (row + 3) % 11; },
                    nullEvery(13)),
                makeFlatVector<int32_t>(234, [](auto row) { return row; }),
            });
          }),
      true);

  // Right side keys are [-3, -2, -1, 0, 1, 2, 3].
  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                123, [](auto row) { return -3 + row % 7; }, nullEvery(11)),
            makeFlatVector<int32_t>(
                123, [](auto row) { return -111 + row * 2; }, nullEvery(13)),
        });
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_)
      .numDrivers(numDrivers_)
      .probeKeys({"c0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u_c0"})
      .buildVectors(std::move(buildVectors))
      .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
      .joinType(core::JoinType::kRight)
      .joinOutputLayout({"c0", "c1", "u_c1"})
      .referenceQuery(
          "SELECT t.c0, t.c1, u.c1 FROM t RIGHT JOIN u ON t.c0 = u.c0")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, rightJoinWithEmptyBuild) {
  // Left side keys are [0, 1, 2,..10].
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    137, [](auto row) { return row % 11; }, nullEvery(13)),
                makeFlatVector<int32_t>(137, [](auto row) { return row; }),
            });
          }),
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    234,
                    [](auto row) { return (row + 3) % 11; },
                    nullEvery(13)),
                makeFlatVector<int32_t>(234, [](auto row) { return row; }),
            });
          }),
      true);

  // Right side keys are [-3, -2, -1, 0, 1, 2, 3].
  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                123, [](auto row) { return -3 + row % 7; }, nullEvery(11)),
            makeFlatVector<int32_t>(
                123, [](auto row) { return -111 + row * 2; }, nullEvery(13)),
        });
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_)
      .numDrivers(numDrivers_)
      .probeKeys({"c0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u_c0"})
      .buildVectors(std::move(buildVectors))
      .buildFilter("c0 > 100")
      .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
      .joinType(core::JoinType::kRight)
      .joinOutputLayout({"c1"})
      .referenceQuery("SELECT null LIMIT 0")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, rightJoinWithAllMatch) {
  // Left side keys are [0, 1, 2,..10].
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    137, [](auto row) { return row % 11; }, nullEvery(13)),
                makeFlatVector<int32_t>(137, [](auto row) { return row; }),
            });
          }),
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    234,
                    [](auto row) { return (row + 3) % 11; },
                    nullEvery(13)),
                makeFlatVector<int32_t>(234, [](auto row) { return row; }),
            });
          }),
      true);

  // Right side keys are [-3, -2, -1, 0, 1, 2, 3].
  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                123, [](auto row) { return -3 + row % 7; }, nullEvery(11)),
            makeFlatVector<int32_t>(
                123, [](auto row) { return -111 + row * 2; }, nullEvery(13)),
        });
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_)
      .numDrivers(numDrivers_)
      .probeKeys({"c0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u_c0"})
      .buildVectors(std::move(buildVectors))
      .buildFilter("c0 >= 0")
      .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
      .joinType(core::JoinType::kRight)
      .joinOutputLayout({"c0", "c1", "u_c1"})
      .referenceQuery(
          "SELECT t.c0, t.c1, u.c1 FROM t RIGHT JOIN (SELECT * FROM u WHERE c0 >= 0) u ON t.c0 = u.c0")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, rightJoinTemp) {
  // Left side keys are [0, 1, 2,..10].
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    137, [](auto row) { return row % 11; }, nullEvery(13)),
                makeFlatVector<int32_t>(137, [](auto row) { return row; }),
            });
          }),
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    234,
                    [](auto row) { return (row + 3) % 11; },
                    nullEvery(13)),
                makeFlatVector<int32_t>(234, [](auto row) { return row; }),
            });
          }),
      true);

  // Right side keys are [-3, -2, -1, 0, 1, 2, 3].
  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                123, [](auto row) { return -3 + row % 7; }, nullEvery(11)),
            makeFlatVector<int32_t>(
                123, [](auto row) { return -111 + row * 2; }, nullEvery(13)),
        });
      });

  // Filter with passed rows.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_)
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u_c0"})
        .buildVectors(std::move(testBuildVectors))
        .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
        .joinType(core::JoinType::kRight)
        .joinFilter("(c1 + u_c1) % 2 = 1")
        .joinOutputLayout({"c0", "c1", "u_c1"})
        .referenceQuery(
            "SELECT t.c0, t.c1, u.c1 FROM t RIGHT JOIN u ON t.c0 = u.c0 AND (t.c1 + u.c1) % 2 = 1")
        .run();
  }

  // Filter without passed rows.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_)
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u_c0"})
        .buildVectors(std::move(testBuildVectors))
        .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
        .joinType(core::JoinType::kRight)
        .joinFilter("(c1 + u_c1) % 2 = 3")
        .joinOutputLayout({"c0", "c1", "u_c1"})
        .referenceQuery(
            "SELECT t.c0, t.c1, u.c1 FROM t RIGHT JOIN u ON t.c0 = u.c0 AND (t.c1 + u.c1) % 2 = 3")
        .run();
  }
}

TEST_P(MultiThreadedHashJoinTest, fullJoin) {
  // Left side keys are [0, 1, 2,..10].
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    213, [](auto row) { return row % 11; }, nullEvery(13)),
                makeFlatVector<int32_t>(213, [](auto row) { return row; }),
            });
          }),
      makeBatches(
          2,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    137,
                    [](auto row) { return (row + 3) % 11; },
                    nullEvery(13)),
                makeFlatVector<int32_t>(137, [](auto row) { return row; }),
            });
          }),
      true);

  // Right side keys are [-3, -2, -1,
  // 0, 1, 2, 3].
  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                123, [](auto row) { return -3 + row % 7; }, nullEvery(11)),
            makeFlatVector<int32_t>(
                123, [](auto row) { return -111 + row * 2; }, nullEvery(13)),
        });
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_)
      .numDrivers(numDrivers_)
      .probeKeys({"c0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u_c0"})
      .buildVectors(std::move(buildVectors))
      .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
      .joinType(core::JoinType::kFull)
      .joinOutputLayout({"c0", "c1", "u_c1"})
      .referenceQuery(
          "SELECT t.c0, t.c1, u.c1 FROM t FULL OUTER JOIN u ON t.c0 = u.c0")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, fullJoinWithEmptyBuild) {
  // Left side keys are [0, 1, 2,..10].
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    213, [](auto row) { return row % 11; }, nullEvery(13)),
                makeFlatVector<int32_t>(213, [](auto row) { return row; }),
            });
          }),
      makeBatches(
          2,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    137,
                    [](auto row) { return (row + 3) % 11; },
                    nullEvery(13)),
                makeFlatVector<int32_t>(137, [](auto row) { return row; }),
            });
          }),
      true);

  // Right side keys are [-3, -2, -1, 0, 1, 2, 3].
  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                123, [](auto row) { return -3 + row % 7; }, nullEvery(11)),
            makeFlatVector<int32_t>(
                123, [](auto row) { return -111 + row * 2; }, nullEvery(13)),
        });
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_)
      .numDrivers(numDrivers_)
      .probeKeys({"c0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u_c0"})
      .buildVectors(std::move(buildVectors))
      .buildFilter("c0 > 100")
      .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
      .joinType(core::JoinType::kFull)
      .joinOutputLayout({"c1"})
      .referenceQuery(
          "SELECT t.c1 FROM t FULL OUTER JOIN (SELECT * FROM u WHERE c0 > 100) u ON t.c0 = u.c0")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, fullJoinWithNoMatch) {
  // Left side keys are [0, 1, 2,..10].
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    213, [](auto row) { return row % 11; }, nullEvery(13)),
                makeFlatVector<int32_t>(213, [](auto row) { return row; }),
            });
          }),
      makeBatches(
          2,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    137,
                    [](auto row) { return (row + 3) % 11; },
                    nullEvery(13)),
                makeFlatVector<int32_t>(137, [](auto row) { return row; }),
            });
          }),
      true);

  // Right side keys are [-3, -2, -1, 0, 1, 2, 3].
  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                123, [](auto row) { return -3 + row % 7; }, nullEvery(11)),
            makeFlatVector<int32_t>(
                123, [](auto row) { return -111 + row * 2; }, nullEvery(13)),
        });
      });

  HashJoinBuilder(*pool_, duckDbQueryRunner_)
      .numDrivers(numDrivers_)
      .probeKeys({"c0"})
      .probeVectors(std::move(probeVectors))
      .buildKeys({"u_c0"})
      .buildVectors(std::move(buildVectors))
      .buildFilter("c0 < 0")
      .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
      .joinType(core::JoinType::kFull)
      .joinOutputLayout({"c1"})
      .referenceQuery(
          "SELECT t.c1 FROM t FULL OUTER JOIN (SELECT * FROM u WHERE c0 < 0) u ON t.c0 = u.c0")
      .run();
}

TEST_P(MultiThreadedHashJoinTest, fullJoinWithFilters) {
  // Left side keys are [0, 1, 2,..10].
  std::vector<RowVectorPtr> probeVectors = mergeBatches(
      makeBatches(
          3,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    213, [](auto row) { return row % 11; }, nullEvery(13)),
                makeFlatVector<int32_t>(213, [](auto row) { return row; }),
            });
          }),
      makeBatches(
          2,
          [&](int32_t /*unused*/) {
            return makeRowVector({
                makeFlatVector<int32_t>(
                    137,
                    [](auto row) { return (row + 3) % 11; },
                    nullEvery(13)),
                makeFlatVector<int32_t>(137, [](auto row) { return row; }),
            });
          }),
      true);

  // Right side keys are [-3, -2, -1, 0, 1, 2, 3].
  std::vector<RowVectorPtr> buildVectors =
      makeBatches(3, [&](int32_t /*unused*/) {
        return makeRowVector({
            makeFlatVector<int32_t>(
                123, [](auto row) { return -3 + row % 7; }, nullEvery(11)),
            makeFlatVector<int32_t>(
                123, [](auto row) { return -111 + row * 2; }, nullEvery(13)),
        });
      });

  // Filter with passed rows.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_)
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u_c0"})
        .buildVectors(std::move(testBuildVectors))
        .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
        .joinType(core::JoinType::kFull)
        .joinFilter("(c1 + u_c1) % 2 = 1")
        .joinOutputLayout({"c0", "c1", "u_c1"})
        .referenceQuery(
            "SELECT t.c0, t.c1, u.c1 FROM t FULL OUTER JOIN u ON t.c0 = u.c0 AND (t.c1 + u.c1) % 2 = 1")
        .run();
  }

  // Filter without passed rows.
  {
    auto testProbeVectors = probeVectors;
    auto testBuildVectors = buildVectors;
    HashJoinBuilder(*pool_, duckDbQueryRunner_)
        .numDrivers(numDrivers_)
        .probeKeys({"c0"})
        .probeVectors(std::move(testProbeVectors))
        .buildKeys({"u_c0"})
        .buildVectors(std::move(testBuildVectors))
        .buildProjections({"c0 AS u_c0", "c1 AS u_c1"})
        .joinType(core::JoinType::kFull)
        .joinFilter("(c1 + u_c1) % 2 = 3")
        .joinOutputLayout({"c0", "c1", "u_c1"})
        .referenceQuery(
            "SELECT t.c0, t.c1, u.c1 FROM t FULL OUTER JOIN u ON t.c0 = u.c0 AND (t.c1 + u.c1) % 2 = 3")
        .run();
  }
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    HashJoinTest,
    MultiThreadedHashJoinTest,
    testing::ValuesIn(MultiThreadedHashJoinTest::getTestParams()));

// TODO: try to parallelize the following test cases if possible.
TEST_F(HashJoinTest, memory) {
  // Measures memory allocation in a 1:n hash join followed by
  // projection and aggregation. We expect vectors to be mostly
  // reused, except for t_k0 + 1, which is a dictionary after the
  // join.
  // std::vector<TypePtr> keyTypes = {BIGINT()};

  // auto probeType = makeRowType(keyTypes, "t_");
  std::vector<RowVectorPtr> probeVectors =
      makeBatches(10, [&](int32_t /*unused*/) {
        return std::dynamic_pointer_cast<RowVector>(
            BatchMaker::createBatch(probeType_, 1000, *pool_));
      });

  // auto buildType = makeRowType(keyTypes, "u_");
  std::vector<RowVectorPtr> buildVectors =
      makeBatches(10, [&](int32_t /*unused*/) {
        return std::dynamic_pointer_cast<RowVector>(
            BatchMaker::createBatch(buildType_, 1000, *pool_));
      });

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  CursorParameters params;
  params.planNode = PlanBuilder(planNodeIdGenerator)
                        .values(probeVectors, true)
                        .hashJoin(
                            {"t_k1"},
                            {"u_k1"},
                            PlanBuilder(planNodeIdGenerator)
                                .values(buildVectors, true)
                                .planNode(),
                            "",
                            concat(probeType_->names(), buildType_->names()))
                        .project({"t_k1 % 1000 AS k1", "u_k1 % 1000 AS k2"})
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
  auto probeVectors = makeBatches(10, [&](int32_t /*unused*/) {
    return makeRowVector(
        {makeFlatVector<int32_t>(3'000, [](auto row) { return row; }),
         makeFlatVector<int64_t>(30'000, [](auto row) { return row % 23; }),
         makeFlatVector<int32_t>(30'000, [](auto row) { return row % 31; }),
         makeFlatVector<StringView>(30'000, [](auto row) {
           return StringView(fmt::format("{}   string", row % 43));
         })});
  });

  std::vector<RowVectorPtr> buildVectors =
      makeBatches(10, [&](int32_t /*unused*/) {
        return makeRowVector(
            {makeFlatVector<int32_t>(1'000, [](auto row) { return row * 3; }),
             makeFlatVector<int64_t>(
                 10'000, [](auto row) { return row % 31; })});
      });

  auto probeFile = TempFilePath::create();
  writeToFile(probeFile->path, probeVectors);
  createDuckDbTable("t", probeVectors);

  auto buildFile = TempFilePath::create();
  writeToFile(buildFile->path, buildVectors);
  createDuckDbTable("u", buildVectors);

  {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    core::PlanNodeId probeScanId;
    core::PlanNodeId buildScanId;
    auto op = PlanBuilder(planNodeIdGenerator)
                  .tableScan(ROW({"c0", "c1"}, {INTEGER(), BIGINT()}))
                  .capturePlanNodeId(probeScanId)
                  .hashJoin(
                      {"c0"},
                      {"c0"},
                      PlanBuilder(planNodeIdGenerator)
                          .tableScan(ROW({"c0"}, {INTEGER()}))
                          .capturePlanNodeId(buildScanId)
                          .planNode(),
                      "",
                      {"c1"})
                  .project({"c1 + 1"})
                  .planNode();

    SplitInput splits;
    splits.emplace(
        probeScanId,
        std::vector<exec::Split>(
            {exec::Split(makeHiveConnectorSplit(probeFile->path))}));

    splits.emplace(
        buildScanId,
        std::vector<exec::Split>(
            {exec::Split(makeHiveConnectorSplit(buildFile->path))}));
    HashJoinBuilder(*pool_, duckDbQueryRunner_)
        .planNode(std::move(op))
        .inputSplits(splits)
        .referenceQuery("SELECT t.c1 + 1 FROM t, u WHERE t.c0 = u.c0")
        .run();
  }

  {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    core::PlanNodeId probeScanId;
    core::PlanNodeId buildScanId;
    auto op = PlanBuilder(planNodeIdGenerator)
                  .tableScan(
                      ROW({"c0", "c1", "c2", "c3"},
                          {INTEGER(), BIGINT(), INTEGER(), VARCHAR()}))
                  .capturePlanNodeId(probeScanId)
                  .filter("c2 < 29")
                  .hashJoin(
                      {"c0"},
                      {"bc0"},
                      PlanBuilder(planNodeIdGenerator)
                          .tableScan(ROW({"c0", "c1"}, {INTEGER(), BIGINT()}))
                          .capturePlanNodeId(buildScanId)
                          .project({"c0 as bc0", "c1 as bc1"})
                          .planNode(),
                      "(c1 + bc1) % 33 < 27",
                      {"c1", "bc1", "c3"})
                  .project({"c1 + 1", "bc1", "length(c3)"})
                  .planNode();

    SplitInput splits;
    splits.emplace(
        probeScanId,
        std::vector<exec::Split>(
            {exec::Split(makeHiveConnectorSplit(probeFile->path))}));
    splits.emplace(
        buildScanId,
        std::vector<exec::Split>(
            {exec::Split(makeHiveConnectorSplit(buildFile->path))}));

    HashJoinBuilder(*pool_, duckDbQueryRunner_)
        .planNode(std::move(op))
        .inputSplits(splits)
        .referenceQuery(
            "SELECT t.c1 + 1, U.c1, length(t.c3) FROM t, u WHERE t.c0 = u.c0 and t.c2 < 29 and (t.c1 + u.c1) % 33 < 27")
        .run();
  }
}

TEST_F(HashJoinTest, dynamicFilters) {
  const int32_t numSplits = 20;
  const int32_t numRowsProbe = 1024;
  const int32_t numRowsBuild = 100;

  std::vector<RowVectorPtr> probeVectors;
  probeVectors.reserve(numSplits);

  auto probeFiles = makeFilePaths(numSplits);

  for (int i = 0; i < numSplits; i++) {
    auto rowVector = makeRowVector({
        makeFlatVector<int32_t>(
            numRowsProbe, [&](auto row) { return row - i * 10; }),
        makeFlatVector<int64_t>(numRowsProbe, [](auto row) { return row; }),
    });
    probeVectors.push_back(rowVector);
    writeToFile(probeFiles[i]->path, rowVector);
  }

  // 100 key values in [35, 233] range.
  auto buildKey = makeFlatVector<int32_t>(
      numRowsBuild, [](auto row) { return 35 + row * 2; });
  std::vector<RowVectorPtr> buildVectors;
  for (int i = 0; i < 1; ++i) {
    buildVectors.push_back(makeRowVector({
        buildKey,
        makeFlatVector<int64_t>(numRowsBuild, [](auto row) { return row; }),
    }));
  }

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  auto probeType = ROW({"c0", "c1"}, {INTEGER(), BIGINT()});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  auto buildSide = PlanBuilder(planNodeIdGenerator)
                       .values(buildVectors)
                       .project({"c0 AS u_c0", "c1 AS u_c1"})
                       .planNode();
  auto keyOnlyBuildSide = PlanBuilder(planNodeIdGenerator)
                              .values({makeRowVector({buildKey})})
                              .project({"c0 AS u_c0"})
                              .planNode();

  // Basic push-down.
  {
    // Inner join.
    core::PlanNodeId probeScanId;
    auto op = PlanBuilder(planNodeIdGenerator)
                  .tableScan(probeType)
                  .capturePlanNodeId(probeScanId)
                  .hashJoin(
                      {"c0"},
                      {"u_c0"},
                      buildSide,
                      "",
                      {"c0", "c1", "u_c1"},
                      core::JoinType::kInner)
                  .project({"c0", "c1 + 1", "c1 + u_c1"})
                  .planNode();
    {
      HashJoinBuilder(*pool_, duckDbQueryRunner_)
          .planNode(std::move(op))
          .inputSplits(makeSpiltInput({probeScanId}, {probeFiles}))
          .referenceQuery(
              "SELECT t.c0, t.c1 + 1, t.c1 + u.c1 FROM t, u WHERE t.c0 = u.c0")
          .verifier([&](const std::shared_ptr<Task>& task) {
            ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
            ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
            ASSERT_EQ(0, getReplacedWithFilterRows(task, 1).sum);
            ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
          })
          .run();
    }

    // Left semi join.
    op = PlanBuilder(planNodeIdGenerator)
             .tableScan(probeType)
             .capturePlanNodeId(probeScanId)
             .hashJoin(
                 {"c0"},
                 {"u_c0"},
                 buildSide,
                 "",
                 {"c0", "c1"},
                 core::JoinType::kLeftSemi)
             .project({"c0", "c1 + 1"})
             .planNode();

    {
      HashJoinBuilder(*pool_, duckDbQueryRunner_)
          .planNode(std::move(op))
          .inputSplits(makeSpiltInput({probeScanId}, {probeFiles}))
          .referenceQuery(
              "SELECT t.c0, t.c1 + 1 FROM t WHERE t.c0 IN (SELECT c0 FROM u)")
          .verifier([&](const std::shared_ptr<Task>& task) {
            ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
            ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
            ASSERT_GT(getReplacedWithFilterRows(task, 1).sum, 0);
            ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
          })
          .run();
    }

    // Right semi join.
    op = PlanBuilder(planNodeIdGenerator)
             .tableScan(probeType)
             .capturePlanNodeId(probeScanId)
             .hashJoin(
                 {"c0"},
                 {"u_c0"},
                 buildSide,
                 "",
                 {"c0", "c1"},
                 core::JoinType::kRightSemi)
             .project({"c0", "c1 + 1"})
             .planNode();

    {
      HashJoinBuilder(*pool_, duckDbQueryRunner_)
          .planNode(std::move(op))
          .inputSplits(makeSpiltInput({probeScanId}, {probeFiles}))
          .referenceQuery(
              "SELECT t.c0, t.c1 + 1 FROM t WHERE t.c0 IN (SELECT c0 FROM u)")
          .verifier([&](const std::shared_ptr<Task>& task) {
            ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
            ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
            ASSERT_GT(getReplacedWithFilterRows(task, 1).sum, 0);
            ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
          })
          .run();
    }
  }

  // Basic push-down with column names projected out of the table scan
  // having different names than column names in the files.
  {
    auto scanOutputType = ROW({"a", "b"}, {INTEGER(), BIGINT()});
    ColumnHandleMap assignments;
    assignments["a"] = regularColumn("c0", INTEGER());
    assignments["b"] = regularColumn("c1", BIGINT());

    core::PlanNodeId probeScanId;
    auto op =
        PlanBuilder(planNodeIdGenerator)
            .tableScan(
                scanOutputType,
                makeTableHandle(common::test::SubfieldFiltersBuilder().build()),
                assignments)
            .capturePlanNodeId(probeScanId)
            .hashJoin({"a"}, {"u_c0"}, buildSide, "", {"a", "b", "u_c1"})
            .project({"a", "b + 1", "b + u_c1"})
            .planNode();

    HashJoinBuilder(*pool_, duckDbQueryRunner_)
        .planNode(std::move(op))
        .inputSplits(makeSpiltInput({probeScanId}, {probeFiles}))
        .referenceQuery(
            "SELECT t.c0, t.c1 + 1, t.c1 + u.c1 FROM t, u WHERE t.c0 = u.c0")
        .verifier([&](const std::shared_ptr<Task>& task) {
          ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
          ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
          ASSERT_EQ(0, getReplacedWithFilterRows(task, 1).sum);
          ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
        })
        .run();
  }

  // Push-down that requires merging filters.
  {
    core::PlanNodeId probeScanId;
    auto op = PlanBuilder(planNodeIdGenerator)
                  .tableScan(probeType, {"c0 < 500::INTEGER"})
                  .capturePlanNodeId(probeScanId)
                  .hashJoin({"c0"}, {"u_c0"}, buildSide, "", {"c1", "u_c1"})
                  .project({"c1 + u_c1"})
                  .planNode();

    HashJoinBuilder(*pool_, duckDbQueryRunner_)
        .planNode(std::move(op))
        .inputSplits(makeSpiltInput({probeScanId}, {probeFiles}))
        .referenceQuery(
            "SELECT t.c1 + u.c1 FROM t, u WHERE t.c0 = u.c0 AND t.c0 < 500")
        .verifier([&](const std::shared_ptr<Task>& task) {
          ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
          ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
          ASSERT_EQ(0, getReplacedWithFilterRows(task, 1).sum);
          ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
        })
        .run();
  }

  // Push-down that turns join into a no-op.
  {
    core::PlanNodeId probeScanId;
    auto op =
        PlanBuilder(planNodeIdGenerator)
            .tableScan(probeType)
            .capturePlanNodeId(probeScanId)
            .hashJoin({"c0"}, {"u_c0"}, keyOnlyBuildSide, "", {"c0", "c1"})
            .project({"c0", "c1 + 1"})
            .planNode();

    HashJoinBuilder(*pool_, duckDbQueryRunner_)
        .planNode(std::move(op))
        .inputSplits(makeSpiltInput({probeScanId}, {probeFiles}))
        .referenceQuery("SELECT t.c0, t.c1 + 1 FROM t, u WHERE t.c0 = u.c0")
        .verifier([&](const std::shared_ptr<Task>& task) {
          ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
          ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
          ASSERT_EQ(
              getReplacedWithFilterRows(task, 1).sum, numRowsBuild * numSplits);
          ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
        })
        .run();
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

    HashJoinBuilder(*pool_, duckDbQueryRunner_)
        .planNode(std::move(op))
        .inputSplits(makeSpiltInput({probeScanId}, {probeFiles}))
        .referenceQuery("SELECT t.c0 FROM t JOIN u ON (t.c0 = u.c0)")
        .verifier([&](const std::shared_ptr<Task>& task) {
          ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
          ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
          ASSERT_EQ(
              getReplacedWithFilterRows(task, 1).sum, numRowsBuild * numSplits);
          ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
        })
        .run();
  }

  // Push-down that requires merging filters and turns join into a no-op.
  {
    core::PlanNodeId probeScanId;
    auto op = PlanBuilder(planNodeIdGenerator)
                  .tableScan(probeType, {"c0 < 500::INTEGER"})
                  .capturePlanNodeId(probeScanId)
                  .hashJoin({"c0"}, {"u_c0"}, keyOnlyBuildSide, "", {"c1"})
                  .project({"c1 + 1"})
                  .planNode();

    HashJoinBuilder(*pool_, duckDbQueryRunner_)
        .planNode(std::move(op))
        .inputSplits(makeSpiltInput({probeScanId}, {probeFiles}))
        .referenceQuery(
            "SELECT t.c1 + 1 FROM t, u WHERE t.c0 = u.c0 AND t.c0 < 500")
        .verifier([&](const std::shared_ptr<Task>& task) {
          ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
          ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
          ASSERT_GT(getReplacedWithFilterRows(task, 1).sum, 0);
          ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
        })
        .run();
  }

  // Push-down with highly selective filter in the scan.
  {
    // Inner join.
    core::PlanNodeId probeScanId;
    auto op =
        PlanBuilder(planNodeIdGenerator)
            .tableScan(probeType, {"c0 < 200::INTEGER"})
            .capturePlanNodeId(probeScanId)
            .hashJoin(
                {"c0"}, {"u_c0"}, buildSide, "", {"c1"}, core::JoinType::kInner)
            .project({"c1 + 1"})
            .planNode();

    {
      HashJoinBuilder(*pool_, duckDbQueryRunner_)
          .planNode(std::move(op))
          .inputSplits(makeSpiltInput({probeScanId}, {probeFiles}))
          .referenceQuery(
              "SELECT t.c1 + 1 FROM t, u WHERE t.c0 = u.c0 AND t.c0 < 200")
          .verifier([&](const std::shared_ptr<Task>& task) {
            ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
            ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
            ASSERT_GT(getReplacedWithFilterRows(task, 1).sum, 0);
            ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
          })
          .run();
    }

    // Left semi join.
    op = PlanBuilder(planNodeIdGenerator)
             .tableScan(probeType, {"c0 < 200::INTEGER"})
             .capturePlanNodeId(probeScanId)
             .hashJoin(
                 {"c0"},
                 {"u_c0"},
                 buildSide,
                 "",
                 {"c1"},
                 core::JoinType::kLeftSemi)
             .project({"c1 + 1"})
             .planNode();

    {
      HashJoinBuilder(*pool_, duckDbQueryRunner_)
          .planNode(std::move(op))
          .inputSplits(makeSpiltInput({probeScanId}, {probeFiles}))
          .referenceQuery(
              "SELECT t.c1 + 1 FROM t WHERE t.c0 IN (SELECT c0 FROM u) AND t.c0 < 200")
          .verifier([&](const std::shared_ptr<Task>& task) {
            ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
            ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
            ASSERT_GT(getReplacedWithFilterRows(task, 1).sum, 0);
            ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
          })
          .run();
    }

    // Right semi join.
    op = PlanBuilder(planNodeIdGenerator)
             .tableScan(probeType, {"c0 < 200::INTEGER"})
             .capturePlanNodeId(probeScanId)
             .hashJoin(
                 {"c0"},
                 {"u_c0"},
                 buildSide,
                 "",
                 {"c1"},
                 core::JoinType::kRightSemi)
             .project({"c1 + 1"})
             .planNode();

    {
      HashJoinBuilder(*pool_, duckDbQueryRunner_)
          .planNode(std::move(op))
          .inputSplits(makeSpiltInput({probeScanId}, {probeFiles}))
          .referenceQuery(
              "SELECT t.c1 + 1 FROM t WHERE t.c0 IN (SELECT c0 FROM u) AND t.c0 < 200")
          .verifier([&](const std::shared_ptr<Task>& task) {
            ASSERT_EQ(1, getFiltersProduced(task, 1).sum);
            ASSERT_EQ(1, getFiltersAccepted(task, 0).sum);
            ASSERT_GT(getReplacedWithFilterRows(task, 1).sum, 0);
            ASSERT_LT(getInputPositions(task, 1), numRowsProbe * numSplits);
          })
          .run();
    }
  }

  // Disable filter push-down by using values in place of scan.
  {
    auto op = PlanBuilder(planNodeIdGenerator)
                  .values(probeVectors)
                  .hashJoin({"c0"}, {"u_c0"}, buildSide, "", {"c1"})
                  .project({"c1 + 1"})
                  .planNode();

    HashJoinBuilder(*pool_, duckDbQueryRunner_)
        .planNode(std::move(op))
        .referenceQuery("SELECT t.c1 + 1 FROM t, u WHERE t.c0 = u.c0")
        .verifier([&](const std::shared_ptr<Task>& task) {
          ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
          ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
          ASSERT_EQ(numRowsProbe * numSplits, getInputPositions(task, 1));
        })
        .run();
  }

  // Disable filter push-down by using an expression as the join key on the
  // probe side.
  {
    core::PlanNodeId probeScanId;
    auto op = PlanBuilder(planNodeIdGenerator)
                  .tableScan(probeType)
                  .capturePlanNodeId(probeScanId)
                  .project({"cast(c0 + 1 as integer) AS t_key", "c1"})
                  .hashJoin({"t_key"}, {"u_c0"}, buildSide, "", {"c1"})
                  .project({"c1 + 1"})
                  .planNode();

    HashJoinBuilder(*pool_, duckDbQueryRunner_)
        .planNode(std::move(op))
        .inputSplits(makeSpiltInput({probeScanId}, {probeFiles}))
        .referenceQuery("SELECT t.c1 + 1 FROM t, u WHERE (t.c0 + 1) = u.c0")
        .verifier([&](const std::shared_ptr<Task>& task) {
          ASSERT_EQ(0, getFiltersProduced(task, 1).sum);
          ASSERT_EQ(0, getFiltersAccepted(task, 0).sum);
          ASSERT_EQ(numRowsProbe * numSplits, getInputPositions(task, 1));
        })
        .run();
  }
}

// Verify the size of the join output vectors when projecting build-side
// variable-width column.
TEST_F(HashJoinTest, memoryUsage) {
  std::vector<RowVectorPtr> probeVectors =
      makeBatches(10, [&](int32_t /*unused*/) {
        return makeRowVector(
            {makeFlatVector<int32_t>(1'000, [](auto row) { return row % 5; })});
      });
  std::vector<RowVectorPtr> buildVectors =
      makeBatches(5, [&](int32_t /*unused*/) {
        return makeRowVector(
            {"u_c0", "u_c1"},
            {makeFlatVector<int32_t>({0, 1, 2}),
             makeFlatVector<std::string>({
                 std::string(40, 'a'),
                 std::string(50, 'b'),
                 std::string(30, 'c'),
             })});
      });
  core::PlanNodeId joinNodeId;

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(probeVectors)
                  .hashJoin(
                      {"c0"},
                      {"u_c0"},
                      PlanBuilder(planNodeIdGenerator)
                          .values({buildVectors})
                          .planNode(),
                      "",
                      {"c0", "u_c1"})
                  .capturePlanNodeId(joinNodeId)
                  .singleAggregation({}, {"count(1)"})
                  .planNode();

  HashJoinBuilder(*pool_, duckDbQueryRunner_)
      .planNode(std::move(plan))
      .referenceQuery("SELECT 30000")
      .verifier([&](const std::shared_ptr<Task>& task) {
        auto planStats = toPlanStats(task->taskStats());
        auto outputBytes = planStats.at(joinNodeId).outputBytes;
        ASSERT_LT(outputBytes, ((40 + 50 + 30) / 3 + 8) * 1000 * 10 * 5);
        // Verify number of memory allocations. Should not be too high if
        // hash join is able to re-use output vectors that contain
        // build-side data.
        ASSERT_GT(40, task->pool()->getMemoryUsageTracker()->getNumAllocs());
      })
      .run();
}

/// Test an edge case in producing small output batches where the logic to
/// calculate the set of probe-side rows to load lazy vectors for was triggering
/// a crash.
TEST_F(HashJoinTest, smallOutputBatchSize) {
  // Setup probe data with 50 non-null matching keys followed by 50 null
  // keys: 1, 2, 1, 2,...null, null.
  auto probeVectors = makeRowVector({
      makeFlatVector<int32_t>(
          100,
          [](auto row) { return 1 + row % 2; },
          [](auto row) { return row > 50; }),
      makeFlatVector<int32_t>(100, [](auto row) { return row * 10; }),
  });

  // Setup build side to match non-null probe side keys.
  auto buildVectors = makeRowVector(
      {"u_c0", "u_c1"},
      {
          makeFlatVector<int32_t>({1, 2}),
          makeFlatVector<int32_t>({100, 200}),
      });

  createDuckDbTable("t", {probeVectors});
  createDuckDbTable("u", {buildVectors});

  // Plan hash inner join with a filter.
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values({probeVectors})
                  .hashJoin(
                      {"c0"},
                      {"u_c0"},
                      PlanBuilder(planNodeIdGenerator)
                          .values({buildVectors})
                          .planNode(),
                      "c1 < u_c1",
                      {"c0", "u_c1"})
                  .planNode();

  // Use small output batch size to trigger logic for calculating set of
  // probe-side rows to load lazy vectors for.
  HashJoinBuilder(*pool_, duckDbQueryRunner_)
      .planNode(std::move(plan))
      .config(core::QueryConfig::kPreferredOutputBatchSize, std::to_string(10))
      .referenceQuery("SELECT c0, u_c1 FROM t, u WHERE c0 = u_c0 AND c1 < u_c1")
      .run();
}
} // namespace
