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

#include <re2/re2.h>

#include <fmt/format.h>
#include "folly/experimental/EventCount.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/Cursor.h"
#include "velox/exec/HashBuild.h"
#include "velox/exec/HashJoinBridge.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/ArbitratorTestUtil.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/exec/tests/utils/VectorTestUtil.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

namespace facebook::velox::exec::test {

using namespace facebook::velox::common::testutil;

using facebook::velox::test::BatchMaker;

struct TestParam {
  int64_t numDrivers{1};

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

// Returns aggregated spilled stats by build and probe operators from 'task'.
std::pair<common::SpillStats, common::SpillStats> taskSpilledStats(
    const exec::Task& task) {
  common::SpillStats buildStats;
  common::SpillStats probeStats;
  auto stats = task.taskStats();
  for (auto& pipeline : stats.pipelineStats) {
    for (auto op : pipeline.operatorStats) {
      if (op.operatorType == "HashBuild") {
        buildStats.spilledInputBytes += op.spilledInputBytes;
        buildStats.spilledBytes += op.spilledBytes;
        buildStats.spilledRows += op.spilledRows;
        buildStats.spilledPartitions += op.spilledPartitions;
        buildStats.spilledFiles += op.spilledFiles;
      } else if (op.operatorType == "HashProbe") {
        probeStats.spilledInputBytes += op.spilledInputBytes;
        probeStats.spilledBytes += op.spilledBytes;
        probeStats.spilledRows += op.spilledRows;
        probeStats.spilledPartitions += op.spilledPartitions;
        probeStats.spilledFiles += op.spilledFiles;
      }
    }
  }
  return {buildStats, probeStats};
}

// Returns aggregated spilled runtime stats by build and probe operators from
// 'task'.
void verifyTaskSpilledRuntimeStats(const exec::Task& task, bool expectedSpill) {
  auto stats = task.taskStats();
  for (auto& pipeline : stats.pipelineStats) {
    for (auto op : pipeline.operatorStats) {
      if ((op.operatorType == "HashBuild") ||
          (op.operatorType == "HashProbe")) {
        if (!expectedSpill) {
          ASSERT_EQ(op.runtimeStats[Operator::kSpillRuns].count, 0);
          ASSERT_EQ(op.runtimeStats[Operator::kSpillFillTime].count, 0);
          ASSERT_EQ(op.runtimeStats[Operator::kSpillSortTime].count, 0);
          ASSERT_EQ(
              op.runtimeStats[Operator::kSpillExtractVectorTime].count, 0);
          ASSERT_EQ(
              op.runtimeStats[Operator::kSpillSerializationTime].count, 0);
          ASSERT_EQ(op.runtimeStats[Operator::kSpillFlushTime].count, 0);
          ASSERT_EQ(op.runtimeStats[Operator::kSpillWrites].count, 0);
          ASSERT_EQ(op.runtimeStats[Operator::kSpillWriteTime].count, 0);
          ASSERT_EQ(op.runtimeStats[Operator::kSpillReadBytes].count, 0);
          ASSERT_EQ(op.runtimeStats[Operator::kSpillReads].count, 0);
          ASSERT_EQ(op.runtimeStats[Operator::kSpillReadTime].count, 0);
          ASSERT_EQ(
              op.runtimeStats[Operator::kSpillDeserializationTime].count, 0);
        } else {
          if (op.operatorType == "HashBuild") {
            ASSERT_GT(op.runtimeStats[Operator::kSpillRuns].count, 0);
            ASSERT_GT(op.runtimeStats[Operator::kSpillFillTime].sum, 0);
            ASSERT_GT(
                op.runtimeStats[Operator::kSpillExtractVectorTime].sum, 0);
          } else {
            // The table spilling might also be triggered from hash probe side.
            ASSERT_GE(op.runtimeStats[Operator::kSpillRuns].count, 0);
            ASSERT_GE(op.runtimeStats[Operator::kSpillFillTime].sum, 0);
            ASSERT_GE(
                op.runtimeStats[Operator::kSpillExtractVectorTime].sum, 0);
          }
          ASSERT_EQ(op.runtimeStats[Operator::kSpillSortTime].sum, 0);
          ASSERT_GT(op.runtimeStats[Operator::kSpillSerializationTime].sum, 0);
          ASSERT_GE(op.runtimeStats[Operator::kSpillFlushTime].sum, 0);
          // NOTE: spill flush might take less than one microsecond.
          ASSERT_GE(
              op.runtimeStats[Operator::kSpillSerializationTime].count,
              op.runtimeStats[Operator::kSpillFlushTime].count);
          ASSERT_GT(op.runtimeStats[Operator::kSpillWrites].sum, 0);
          ASSERT_GE(op.runtimeStats[Operator::kSpillWriteTime].sum, 0);
          // NOTE: spill flush might take less than one microsecond.
          ASSERT_GE(
              op.runtimeStats[Operator::kSpillWrites].count,
              op.runtimeStats[Operator::kSpillWriteTime].count);
          ASSERT_GT(op.runtimeStats[Operator::kSpillReadBytes].sum, 0);
          ASSERT_GT(op.runtimeStats[Operator::kSpillReads].sum, 0);
          ASSERT_GT(op.runtimeStats[Operator::kSpillReadTime].sum, 0);
          ASSERT_GT(
              op.runtimeStats[Operator::kSpillDeserializationTime].sum, 0);
        }
      }
    }
  }
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

// Returns the max hash build spill level by 'task'.
int32_t maxHashBuildSpillLevel(const exec::Task& task) {
  int32_t maxSpillLevel = -1;
  for (auto& pipelineStat : task.taskStats().pipelineStats) {
    for (auto& operatorStat : pipelineStat.operatorStats) {
      if (operatorStat.operatorType == "HashBuild") {
        if (operatorStat.runtimeStats.count("maxSpillLevel") == 0) {
          continue;
        }
        maxSpillLevel = std::max<int32_t>(
            maxSpillLevel, operatorStat.runtimeStats["maxSpillLevel"].max);
      }
    }
  }
  return maxSpillLevel;
}

std::pair<int32_t, int32_t> numTaskSpillFiles(const exec::Task& task) {
  int32_t numBuildFiles = 0;
  int32_t numProbeFiles = 0;
  for (auto& pipelineStat : task.taskStats().pipelineStats) {
    for (auto& operatorStat : pipelineStat.operatorStats) {
      if (operatorStat.runtimeStats.count("spillFileSize") == 0) {
        continue;
      }
      if (operatorStat.operatorType == "HashBuild") {
        numBuildFiles += operatorStat.runtimeStats["spillFileSize"].count;
        continue;
      }
      if (operatorStat.operatorType == "HashProbe") {
        numProbeFiles += operatorStat.runtimeStats["spillFileSize"].count;
      }
    }
  }
  return {numBuildFiles, numProbeFiles};
}

void abortPool(memory::MemoryPool* pool) {
  try {
    VELOX_FAIL("Manual MemoryPool Abortion");
  } catch (const VeloxException&) {
    pool->abort(std::current_exception());
  }
}

using JoinResultsVerifier =
    std::function<void(const std::shared_ptr<Task>&, bool)>;

class HashJoinBuilder {
 public:
  HashJoinBuilder(
      memory::MemoryPool& pool,
      DuckDbQueryRunner& duckDbQueryRunner,
      folly::Executor* executor)
      : pool_(pool),
        duckDbQueryRunner_(duckDbQueryRunner),
        executor_(executor) {
    // Small batches create more edge cases.
    fuzzerOpts_.vectorSize = 10;
    fuzzerOpts_.nullRatio = 0.1;
    fuzzerOpts_.stringVariableLength = true;
    fuzzerOpts_.containerVariableLength = true;
  }

  HashJoinBuilder& numDrivers(
      int32_t numDrivers,
      std::optional<bool> runParallelProbe = std::nullopt,
      std::optional<bool> runParallelBuild = std::nullopt) {
    VELOX_CHECK_EQ(runParallelProbe.has_value(), runParallelBuild.has_value());
    numDrivers_ = numDrivers;
    runParallelProbe_ = runParallelProbe;
    runParallelBuild_ = runParallelBuild;
    return *this;
  }

  HashJoinBuilder& planNode(core::PlanNodePtr planNode) {
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
    // all the hash probe drivers, which will be used to populate the duckdb
    // as well.
    allProbeVectors_ = makeCopies(probeVectors_, numDrivers_);
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
    // all the hash build drivers, which will be used to populate the duckdb
    // as well.
    allBuildVectors_ = makeCopies(buildVectors_, numDrivers_);
    return *this;
  }

  HashJoinBuilder& joinType(core::JoinType joinType) {
    joinType_ = joinType;
    return *this;
  }

  HashJoinBuilder& nullAware(bool nullAware) {
    nullAware_ = nullAware;
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
    makeInputSplits_ = [inputSplits] { return inputSplits; };
    return *this;
  }

  HashJoinBuilder& makeInputSplits(
      std::function<SplitInput()>&& makeInputSplits) {
    makeInputSplits_ = makeInputSplits;
    return *this;
  }

  HashJoinBuilder& config(const std::string& key, const std::string& value) {
    configs_[key] = value;
    return *this;
  }

  HashJoinBuilder& injectSpill(bool injectSpill) {
    injectSpill_ = injectSpill;
    return *this;
  }

  HashJoinBuilder& injectTaskCancellation(bool injectTaskCancellation) {
    injectTaskCancellation_ = injectTaskCancellation;
    return *this;
  }

  HashJoinBuilder& maxSpillLevel(int32_t maxSpillLevel) {
    maxSpillLevel_ = maxSpillLevel;
    return *this;
  }

  HashJoinBuilder& checkSpillStats(bool checkSpillStats) {
    checkSpillStats_ = checkSpillStats;
    return *this;
  }

  HashJoinBuilder& queryPool(std::shared_ptr<memory::MemoryPool>&& queryPool) {
    queryPool_ = queryPool;
    return *this;
  }

  HashJoinBuilder& hashProbeFinishEarlyOnEmptyBuild(bool value) {
    hashProbeFinishEarlyOnEmptyBuild_ = value;
    return *this;
  }

  HashJoinBuilder& spillDirectory(const std::string& spillDirectory) {
    spillDirectory_ = spillDirectory;
    return *this;
  }

  HashJoinBuilder& verifier(JoinResultsVerifier testVerifier) {
    testVerifier_ = std::move(testVerifier);
    return *this;
  }

  void run() {
    if (planNode_ != nullptr) {
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
    if (!runParallelBuild_.has_value()) {
      ASSERT_FALSE(runParallelProbe_.has_value());
      testSettings.push_back({
          true,
          true,
      });
      if (numDrivers_ != 1) {
        testSettings.push_back({true, false});
        testSettings.push_back({false, true});
      }
    } else {
      ASSERT_TRUE(runParallelProbe_.has_value());
      testSettings.push_back(
          {runParallelProbe_.value(), runParallelBuild_.value()});
    }

    for (const auto& testData : testSettings) {
      SCOPED_TRACE(fmt::format(
          "{} numDrivers: {}", testData.debugString(), numDrivers_));
      auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
      std::shared_ptr<const core::HashJoinNode> joinNode;
      auto planNode =
          PlanBuilder(planNodeIdGenerator, &pool_)
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
                  joinType_,
                  nullAware_)
              .capturePlanNode<core::HashJoinNode>(joinNode)
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
      VectorFuzzer fuzzer(fuzzerOpts_, &pool_, 42);
      for (int32_t i = 0; i < numVectors; ++i) {
        vectors.push_back(fuzzer.fuzzInputRow(rowType));
      }
    } else {
      vectors.push_back(RowVector::createEmpty(rowType, &pool_));
    }
    // NOTE: we generate a number of vectors with a fresh new fuzzer init with
    // the same fix seed. The purpose is to ensure we have sufficient match if
    // we use the row type for both build and probe inputs. Here we shuffle
    // the built vectors to introduce some randomness during the join
    // execution.
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
    runTest(planNode, false, maxSpillLevel_.value_or(-1));
    if (injectSpill_) {
      if (maxSpillLevel_.has_value()) {
        runTest(planNode, true, maxSpillLevel_.value(), 100);
      } else {
        runTest(planNode, true, 0, 100);
        runTest(planNode, true, 2, 100);
      }
    }
  }

  void runTest(
      const core::PlanNodePtr& planNode,
      bool injectSpill,
      int32_t maxSpillLevel = -1,
      uint32_t maxDriverYieldTimeMs = 0) {
    AssertQueryBuilder builder(planNode, duckDbQueryRunner_);
    builder.maxDrivers(numDrivers_);
    if (makeInputSplits_) {
      for (const auto& splitEntry : makeInputSplits_()) {
        builder.splits(splitEntry.first, splitEntry.second);
      }
    }
    auto queryCtx = core::QueryCtx::create(
        executor_,
        core::QueryConfig{{}},
        std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>{},
        cache::AsyncDataCache::getInstance(),
        memory::MemoryManager::getInstance()->addRootPool(
            "query_pool",
            memory::kMaxMemory,
            memory::MemoryReclaimer::create()));
    std::shared_ptr<TempDirectoryPath> spillDirectory;
    int32_t spillPct{0};
    if (injectSpill) {
      spillDirectory = exec::test::TempDirectoryPath::create();
      builder.spillDirectory(spillDirectory->getPath());
      config(core::QueryConfig::kSpillEnabled, "true");
      config(core::QueryConfig::kMaxSpillLevel, std::to_string(maxSpillLevel));
      config(core::QueryConfig::kJoinSpillEnabled, "true");
      // Disable write buffering to ease test verification. For example, we
      // want many spilled vectors in a spilled file to trigger recursive
      // spilling.
      config(core::QueryConfig::kSpillWriteBufferSize, std::to_string(0));
      spillPct = 100;
    } else if (!spillDirectory_.empty()) {
      builder.spillDirectory(spillDirectory_);
      config(core::QueryConfig::kSpillEnabled, "true");
      config(core::QueryConfig::kJoinSpillEnabled, "true");
    } else {
      config(core::QueryConfig::kSpillEnabled, "false");
    }
    config(
        core::QueryConfig::kHashProbeFinishEarlyOnEmptyBuild,
        hashProbeFinishEarlyOnEmptyBuild_ ? "true" : "false");
    if (maxDriverYieldTimeMs != 0) {
      config(
          core::QueryConfig::kDriverCpuTimeSliceLimitMs,
          std::to_string(maxDriverYieldTimeMs));
    }

    if (!configs_.empty()) {
      auto configCopy = configs_;
      queryCtx->testingOverrideConfigUnsafe(std::move(configCopy));
    }
    if (queryPool_ != nullptr) {
      queryCtx->testingOverrideMemoryPool(queryPool_);
    }
    builder.queryCtx(queryCtx);

    SCOPED_TRACE(
        injectSpill ? fmt::format("With Max Spill Level: {}", maxSpillLevel)
                    : "Without Spill");
    ASSERT_EQ(memory::spillMemoryPool()->stats().usedBytes, 0);
    const uint64_t peakSpillMemoryUsage =
        memory::spillMemoryPool()->stats().peakBytes;
    TestScopedSpillInjection scopedSpillInjection(spillPct);
    auto task = builder.assertResults(referenceQuery_);

    if (injectTaskCancellation_) {
      task->requestCancel();
    }

    // Wait up to 5 seconds for all the task background activities to complete.
    // Then we can collect the stats from all the operators.
    //
    // TODO: replace this with task utility to ensure all the background
    // activities to finish and all the drivers stats have been reported.
    uint64_t totalTaskWaitTimeUs{0};
    while (task.use_count() != 1) {
      constexpr uint64_t kWaitInternalUs = 1'000;
      std::this_thread::sleep_for(std::chrono::microseconds(kWaitInternalUs));
      totalTaskWaitTimeUs += kWaitInternalUs;
      if (totalTaskWaitTimeUs >= 5'000'000) {
        VELOX_FAIL(
            "Failed to wait for all the background activities of task {} to finish, pending reference count: {}",
            task->taskId(),
            task.use_count());
      }
    }
    const auto statsPair = taskSpilledStats(*task);
    if (injectSpill) {
      if (checkSpillStats_) {
        ASSERT_GT(statsPair.first.spilledRows, 0);
        ASSERT_GT(statsPair.second.spilledRows, 0);
        ASSERT_GT(statsPair.first.spilledBytes, 0);
        ASSERT_GT(statsPair.second.spilledBytes, 0);
        ASSERT_GT(statsPair.first.spilledInputBytes, 0);
        ASSERT_GT(statsPair.second.spilledInputBytes, 0);
        ASSERT_GT(statsPair.first.spilledPartitions, 0);
        ASSERT_GT(statsPair.second.spilledPartitions, 0);
        ASSERT_GT(statsPair.first.spilledFiles, 0);
        ASSERT_GT(statsPair.second.spilledFiles, 0);
        if (maxSpillLevel != -1) {
          ASSERT_EQ(maxHashBuildSpillLevel(*task), maxSpillLevel);
        }
        verifyTaskSpilledRuntimeStats(*task, true);
      }
      if (statsPair.first.spilledBytes > 0 &&
          memory::spillMemoryPool()->trackUsage()) {
        ASSERT_GT(memory::spillMemoryPool()->stats().peakBytes, 0);
        ASSERT_GE(
            memory::spillMemoryPool()->stats().peakBytes, peakSpillMemoryUsage);
      }
      // NOTE: if 'spillDirectory_' is not empty, the test might trigger
      // spilling by its own.
    } else if (spillDirectory_.empty()) {
      ASSERT_EQ(statsPair.first.spilledRows, 0);
      ASSERT_EQ(statsPair.first.spilledInputBytes, 0);
      ASSERT_EQ(statsPair.first.spilledBytes, 0);
      ASSERT_EQ(statsPair.first.spilledPartitions, 0);
      ASSERT_EQ(statsPair.first.spilledFiles, 0);
      ASSERT_EQ(statsPair.second.spilledRows, 0);
      ASSERT_EQ(statsPair.second.spilledBytes, 0);
      ASSERT_EQ(statsPair.second.spilledBytes, 0);
      ASSERT_EQ(statsPair.second.spilledPartitions, 0);
      ASSERT_EQ(statsPair.second.spilledFiles, 0);
      verifyTaskSpilledRuntimeStats(*task, false);
    }
    // Customized test verification.
    if (testVerifier_ != nullptr) {
      testVerifier_(task, injectSpill);
    }
    OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
    ASSERT_EQ(memory::spillMemoryPool()->stats().usedBytes, 0);
  }

  VectorFuzzer::Options fuzzerOpts_;
  memory::MemoryPool& pool_;
  DuckDbQueryRunner& duckDbQueryRunner_;
  folly::Executor* executor_;

  int32_t numDrivers_{1};
  core::JoinType joinType_{core::JoinType::kInner};
  bool nullAware_{false};
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
  std::optional<bool> runParallelProbe_;
  std::optional<bool> runParallelBuild_;

  bool injectTaskCancellation_{false};
  bool injectSpill_{true};
  // If not set, then the test will run the test with different settings:
  // 0, 2.
  std::optional<int32_t> maxSpillLevel_;
  bool checkSpillStats_{true};

  std::shared_ptr<memory::MemoryPool> queryPool_;
  std::string spillDirectory_;
  bool hashProbeFinishEarlyOnEmptyBuild_{true};

  SplitInput inputSplits_;
  std::function<SplitInput()> makeInputSplits_;
  core::PlanNodePtr planNode_;
  std::unordered_map<std::string, std::string> configs_;

  JoinResultsVerifier testVerifier_{};
};

class HashJoinTestBase : public HiveConnectorTestBase {
 protected:
  HashJoinTestBase() : HashJoinTestBase(TestParam(1)) {}

  explicit HashJoinTestBase(const TestParam& param)
      : numDrivers_(param.numDrivers) {}

  void SetUp() override {
    HiveConnectorTestBase::SetUp();

    probeType_ =
        ROW({{"t_k1", INTEGER()}, {"t_k2", VARCHAR()}, {"t_v1", VARCHAR()}});
    buildType_ =
        ROW({{"u_k1", INTEGER()}, {"u_k2", VARCHAR()}, {"u_v1", INTEGER()}});
    fuzzerOpts_ = {
        .vectorSize = 1024,
        .nullRatio = 0.1,
        .stringLength = 1024,
        .stringVariableLength = false,
        .allowLazyVector = false};
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
        splits.push_back(exec::Split(makeHiveConnectorSplit(file->getPath())));
      }
      splitInput.emplace(nodeIds[i], std::move(splits));
    }
    return splitInput;
  }

  void testLazyVectorsWithFilter(
      const core::JoinType joinType,
      const std::string& filter,
      const std::vector<std::string>& outputLayout,
      const std::string& referenceQuery) {
    const vector_size_t vectorSize = 1'000;
    auto probeVectors = makeBatches(1, [&](int32_t /*unused*/) {
      return makeRowVector(
          {makeFlatVector<int32_t>(vectorSize, folly::identity),
           makeFlatVector<int64_t>(
               vectorSize, [](auto row) { return row % 23; }),
           makeFlatVector<int32_t>(
               vectorSize, [](auto row) { return row % 31; })});
    });

    std::vector<RowVectorPtr> buildVectors =
        makeBatches(1, [&](int32_t /*unused*/) {
          return makeRowVector({makeFlatVector<int32_t>(
              vectorSize, [](auto row) { return row * 3; })});
        });

    std::shared_ptr<TempFilePath> probeFile = TempFilePath::create();
    writeToFile(probeFile->getPath(), probeVectors);

    std::shared_ptr<TempFilePath> buildFile = TempFilePath::create();
    writeToFile(buildFile->getPath(), buildVectors);

    createDuckDbTable("t", probeVectors);
    createDuckDbTable("u", buildVectors);

    // Lazy vector is part of the filter but never gets loaded.
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    core::PlanNodeId probeScanId;
    core::PlanNodeId buildScanId;
    auto op = PlanBuilder(planNodeIdGenerator)
                  .tableScan(asRowType(probeVectors[0]->type()))
                  .capturePlanNodeId(probeScanId)
                  .hashJoin(
                      {"c0"},
                      {"c0"},
                      PlanBuilder(planNodeIdGenerator)
                          .tableScan(asRowType(buildVectors[0]->type()))
                          .capturePlanNodeId(buildScanId)
                          .planNode(),
                      filter,
                      outputLayout,
                      joinType)
                  .planNode();
    SplitInput splitInput = {
        {probeScanId,
         {exec::Split(makeHiveConnectorSplit(probeFile->getPath()))}},
        {buildScanId,
         {exec::Split(makeHiveConnectorSplit(buildFile->getPath()))}},
    };
    HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
        .planNode(std::move(op))
        .inputSplits(splitInput)
        .checkSpillStats(false)
        .referenceQuery(referenceQuery)
        .run();
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

  // Get the operator index from the plan node id. Only used in the probe-side
  // pipeline. The plan node id starts from "1" and the operator index starts
  // from 0. Plan node IDs map to operators 1:1.
  static int32_t getOperatorIndex(const core::PlanNodeId& planNodeId) {
    return folly::to<int32_t>(planNodeId) - 1;
  }

  static core::JoinType flipJoinType(core::JoinType joinType) {
    switch (joinType) {
      case core::JoinType::kInner:
        return joinType;
      case core::JoinType::kLeft:
        return core::JoinType::kRight;
      case core::JoinType::kRight:
        return core::JoinType::kLeft;
      case core::JoinType::kFull:
        return joinType;
      case core::JoinType::kLeftSemiFilter:
        return core::JoinType::kRightSemiFilter;
      case core::JoinType::kLeftSemiProject:
        return core::JoinType::kRightSemiProject;
      case core::JoinType::kRightSemiFilter:
        return core::JoinType::kLeftSemiFilter;
      case core::JoinType::kRightSemiProject:
        return core::JoinType::kLeftSemiProject;
      default:
        VELOX_FAIL("Cannot flip join type: {}", core::joinTypeName(joinType));
    }
  }

  static core::PlanNodePtr flipJoinSides(const core::PlanNodePtr& plan) {
    auto joinNode = std::dynamic_pointer_cast<const core::HashJoinNode>(plan);
    VELOX_CHECK_NOT_NULL(joinNode);
    return std::make_shared<core::HashJoinNode>(
        joinNode->id(),
        flipJoinType(joinNode->joinType()),
        joinNode->isNullAware(),
        joinNode->rightKeys(),
        joinNode->leftKeys(),
        joinNode->filter(),
        joinNode->sources()[1],
        joinNode->sources()[0],
        joinNode->outputType());
  }

  const int32_t numDrivers_;

  // The default left and right table types used for test.
  RowTypePtr probeType_;
  RowTypePtr buildType_;
  VectorFuzzer::Options fuzzerOpts_;

  memory::MemoryReclaimer::Stats reclaimerStats_;
  friend class HashJoinBuilder;
};

} // namespace facebook::velox::exec::test
