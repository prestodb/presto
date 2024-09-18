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
#include "velox/exec/fuzzer/JoinFuzzer.h"
#include <boost/random/uniform_int_distribution.hpp>
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/hive/PartitionIdGenerator.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/fuzzer/FuzzerUtil.h"
#include "velox/exec/fuzzer/ReferenceQueryRunner.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

DEFINE_int32(steps, 10, "Number of plans to generate and test.");

DEFINE_int32(
    duration_sec,
    0,
    "For how long it should run (in seconds). If zero, "
    "it executes exactly --steps iterations and exits.");

DEFINE_int32(
    batch_size,
    100,
    "The number of elements on each generated vector.");

DEFINE_int32(num_batches, 10, "The number of generated vectors.");

DEFINE_double(
    null_ratio,
    0.1,
    "Chance of adding a null value in a vector "
    "(expressed as double from 0 to 1).");

DEFINE_bool(enable_spill, true, "Whether to test plans with spilling enabled");

DEFINE_bool(
    enable_oom_injection,
    false,
    "When enabled OOMs will randomly be triggered while executing query "
    "plans. The goal of this mode is to ensure unexpected exceptions "
    "aren't thrown and the process isn't killed in the process of cleaning "
    "up after failures. Therefore, results are not compared when this is "
    "enabled. Note that this option only works in debug builds.");

namespace facebook::velox::exec::test {

namespace {

std::string makePercentageString(size_t value, size_t total) {
  return fmt::format("{} ({:.2f}%)", value, (double)value / total * 100);
}

class JoinFuzzer {
 public:
  JoinFuzzer(
      size_t initialSeed,
      std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner);

  void go();

  struct PlanWithSplits {
    core::PlanNodePtr plan;
    core::PlanNodeId probeScanId;
    core::PlanNodeId buildScanId;
    std::unordered_map<core::PlanNodeId, std::vector<velox::exec::Split>>
        splits;
    core::ExecutionStrategy executionStrategy{
        core::ExecutionStrategy::kUngrouped};
    int32_t numGroups;

    explicit PlanWithSplits(
        const core::PlanNodePtr& _plan,
        const core::PlanNodeId& _probeScanId = "",
        const core::PlanNodeId& _buildScanId = "",
        const std::unordered_map<
            core::PlanNodeId,
            std::vector<velox::exec::Split>>& _splits = {},
        core::ExecutionStrategy _executionStrategy =
            core::ExecutionStrategy::kUngrouped,
        int32_t _numGroups = 0)
        : plan(_plan),
          probeScanId(_probeScanId),
          buildScanId(_buildScanId),
          splits(_splits),
          executionStrategy(_executionStrategy),
          numGroups(_numGroups) {}
  };

 private:
  static VectorFuzzer::Options getFuzzerOptions() {
    VectorFuzzer::Options opts;
    opts.vectorSize = FLAGS_batch_size;
    opts.stringVariableLength = true;
    opts.stringLength = 100;
    opts.nullRatio = FLAGS_null_ratio;
    opts.timestampPrecision =
        VectorFuzzer::Options::TimestampPrecision::kMilliSeconds;
    return opts;
  }

  void seed(size_t seed) {
    currentSeed_ = seed;
    vectorFuzzer_.reSeed(seed);
    rng_.seed(currentSeed_);
  }

  void reSeed() {
    seed(rng_());
  }

  // Randomly pick a join type to test.
  core::JoinType pickJoinType();

  // Makes the query plan with default settings in JoinFuzzer and value inputs
  // for both probe and build sides.
  //
  // NOTE: 'probeInput' and 'buildInput' could either input rows with lazy
  // vectors or flatten ones.
  JoinFuzzer::PlanWithSplits makeDefaultPlan(
      core::JoinType joinType,
      bool nullAware,
      const std::vector<std::string>& probeKeys,
      const std::vector<std::string>& buildKeys,
      const std::vector<RowVectorPtr>& probeInput,
      const std::vector<RowVectorPtr>& buildInput,
      const std::vector<std::string>& outputColumns);

  JoinFuzzer::PlanWithSplits makeMergeJoinPlan(
      core::JoinType joinType,
      const std::vector<std::string>& probeKeys,
      const std::vector<std::string>& buildKeys,
      const std::vector<RowVectorPtr>& probeInput,
      const std::vector<RowVectorPtr>& buildInput,
      const std::vector<std::string>& outputColumns);

  // Returns a PlanWithSplits for NestedLoopJoin with inputs from Values nodes.
  // If withFilter is true, uses the equality filter between probeKeys and
  // buildKeys as the join filter. Uses empty join filter otherwise.
  JoinFuzzer::PlanWithSplits makeNestedLoopJoinPlan(
      core::JoinType joinType,
      const std::vector<std::string>& probeKeys,
      const std::vector<std::string>& buildKeys,
      const std::vector<RowVectorPtr>& probeInput,
      const std::vector<RowVectorPtr>& buildInput,
      const std::vector<std::string>& outputColumns,
      bool withFilter = true);

  // Makes the default query plan with table scan as inputs for both probe and
  // build sides.
  JoinFuzzer::PlanWithSplits makeDefaultPlanWithTableScan(
      core::JoinType joinType,
      bool nullAware,
      const RowTypePtr& probeType,
      const RowTypePtr& buildType,
      const std::vector<std::string>& probeKeys,
      const std::vector<std::string>& buildKeys,
      const std::vector<Split>& probeSplits,
      const std::vector<Split>& buildSplits,
      const std::vector<std::string>& outputColumns);

  JoinFuzzer::PlanWithSplits makeMergeJoinPlanWithTableScan(
      core::JoinType joinType,
      const RowTypePtr& probeType,
      const RowTypePtr& buildType,
      const std::vector<std::string>& probeKeys,
      const std::vector<std::string>& buildKeys,
      const std::vector<Split>& probeSplits,
      const std::vector<Split>& buildSplits,
      const std::vector<std::string>& outputColumns);

  // Returns a PlanWithSplits for NestedLoopJoin with inputs from TableScan
  // nodes. If withFilter is true, uses the equiality filter between probeKeys
  // and buildKeys as the join filter. Uses empty join filter otherwise.
  JoinFuzzer::PlanWithSplits makeNestedLoopJoinPlanWithTableScan(
      core::JoinType joinType,
      const RowTypePtr& probeType,
      const RowTypePtr& buildType,
      const std::vector<std::string>& probeKeys,
      const std::vector<std::string>& buildKeys,
      const std::vector<Split>& probeSplits,
      const std::vector<Split>& buildSplits,
      const std::vector<std::string>& outputColumns,
      bool withFilter = true);

  void makeAlternativePlans(
      const core::PlanNodePtr& plan,
      const std::vector<RowVectorPtr>& probeInput,
      const std::vector<RowVectorPtr>& buildInput,
      std::vector<JoinFuzzer::PlanWithSplits>& plans);

  // Makes the query plan from 'planWithTableScan' with grouped execution mode.
  // Correspondingly, it replaces the table scan input splits with grouped ones.
  JoinFuzzer::PlanWithSplits makeGroupedExecutionPlanWithTableScan(
      const JoinFuzzer::PlanWithSplits& planWithTableScan,
      int32_t numGroups,
      const std::vector<exec::Split>& groupedProbeScanSplits,
      const std::vector<exec::Split>& groupedBuildScanSplits);

  // Runs one test iteration from query plans generations, executions and result
  // verifications.
  void verify(core::JoinType joinType);

  // Returns a list of randomly generated join key types.
  std::vector<TypePtr> generateJoinKeyTypes(int32_t numKeys);

  // Returns randomly generated probe input with upto 3 additional payload
  // columns.
  std::vector<RowVectorPtr> generateProbeInput(
      const std::vector<std::string>& keyNames,
      const std::vector<TypePtr>& keyTypes);

  // Same as generateProbeInput() but copies over 10% of the input in the probe
  // columns to ensure some matches during joining. Also generates an empty
  // input with a 10% chance.
  std::vector<RowVectorPtr> generateBuildInput(
      const std::vector<RowVectorPtr>& probeInput,
      const std::vector<std::string>& probeKeys,
      const std::vector<std::string>& buildKeys);

  void shuffleJoinKeys(
      std::vector<std::string>& probeKeys,
      std::vector<std::string>& buildKeys);

  void addPlansWithTableScan(
      const std::string& tableDir,
      core::JoinType joinType,
      bool nullAware,
      const std::vector<std::string>& probeKeys,
      const std::vector<std::string>& buildKeys,
      const std::vector<RowVectorPtr>& probeInput,
      const std::vector<RowVectorPtr>& buildInput,
      const std::vector<std::string>& outputColumns,
      std::vector<PlanWithSplits>& altPlans);

  // Splits the input into groups by partitioning on the join keys.
  std::vector<std::vector<RowVectorPtr>> splitInputByGroup(
      int32_t numGroups,
      size_t numKeys,
      const std::vector<RowVectorPtr>& inputs);

  // Generates the grouped splits.
  std::vector<exec::Split> generateSplitsWithGroup(
      const std::string& tableDir,
      int32_t numGroups,
      bool isProbe,
      size_t numKeys,
      const std::vector<RowVectorPtr>& input);

  RowVectorPtr execute(const PlanWithSplits& plan, bool injectSpill);

  std::optional<MaterializedRowMultiset> computeReferenceResults(
      const core::PlanNodePtr& plan,
      const std::vector<RowVectorPtr>& probeInput,
      const std::vector<RowVectorPtr>& buildInput);

  // Generates and executes plans using NestedLoopJoin without filters. The
  // result is compared to DuckDB. Returns the result vector of the cross
  // product.
  RowVectorPtr testCrossProduct(
      const std::string& tableDir,
      core::JoinType joinType,
      const std::vector<std::string>& probeKeys,
      const std::vector<std::string>& buildKeys,
      const std::vector<RowVectorPtr>& probeInput,
      const std::vector<RowVectorPtr>& buildInput);

  int32_t randInt(int32_t min, int32_t max) {
    return boost::random::uniform_int_distribution<int32_t>(min, max)(rng_);
  }

  FuzzerGenerator rng_;
  size_t currentSeed_{0};

  std::shared_ptr<memory::MemoryPool> rootPool_{
      memory::memoryManager()->addRootPool(
          "joinFuzzer",
          memory::kMaxMemory,
          memory::MemoryReclaimer::create())};
  std::shared_ptr<memory::MemoryPool> pool_{rootPool_->addLeafChild(
      "joinFuzzerLeaf",
      true,
      exec::MemoryReclaimer::create())};
  std::shared_ptr<memory::MemoryPool> writerPool_{rootPool_->addAggregateChild(
      "joinFuzzerWriter",
      exec::MemoryReclaimer::create())};

  VectorFuzzer vectorFuzzer_;
  std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner_;

  struct Stats {
    // The total number of iterations tested.
    size_t numIterations{0};

    // The number of iterations verified against reference DB.
    size_t numVerified{0};

    // The number of iterations that test cross product.
    size_t numCrossProduct{0};

    std::string toString() const {
      std::stringstream out;
      out << "\nTotal iterations tested: " << numIterations << std::endl;
      out << "Total iterations verified against reference DB: "
          << makePercentageString(numVerified, numIterations) << std::endl;
      out << "Total iterations testing cross product: "
          << makePercentageString(numCrossProduct, numIterations) << std::endl;

      return out.str();
    }
  };

  Stats stats_;
};

JoinFuzzer::JoinFuzzer(
    size_t initialSeed,
    std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner)
    : vectorFuzzer_{getFuzzerOptions(), pool_.get()},
      referenceQueryRunner_{std::move(referenceQueryRunner)} {
  filesystems::registerLocalFileSystem();

  // Make sure not to run out of open file descriptors.
  std::unordered_map<std::string, std::string> hiveConfig = {
      {connector::hive::HiveConfig::kNumCacheFileHandles, "1000"}};
  auto hiveConnector =
      connector::getConnectorFactory(
          connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(
              kHiveConnectorId,
              std::make_shared<config::ConfigBase>(std::move(hiveConfig)));
  connector::registerConnector(hiveConnector);

  seed(initialSeed);
}

template <typename T>
bool isDone(size_t i, T startTime) {
  if (FLAGS_duration_sec > 0) {
    std::chrono::duration<double> elapsed =
        std::chrono::system_clock::now() - startTime;
    return elapsed.count() >= FLAGS_duration_sec;
  }
  return i >= FLAGS_steps;
}

core::JoinType JoinFuzzer::pickJoinType() {
  // Right joins are tested by flipping sides of the left joins.
  static std::vector<core::JoinType> kJoinTypes = {
      core::JoinType::kInner,
      core::JoinType::kLeft,
      core::JoinType::kFull,
      core::JoinType::kLeftSemiFilter,
      core::JoinType::kLeftSemiProject,
      core::JoinType::kAnti};

  const size_t idx = randInt(0, kJoinTypes.size() - 1);
  return kJoinTypes[idx];
}

std::vector<TypePtr> JoinFuzzer::generateJoinKeyTypes(int32_t numKeys) {
  std::vector<TypePtr> types;
  types.reserve(numKeys);
  for (auto i = 0; i < numKeys; ++i) {
    // Pick random scalar type.
    types.push_back(vectorFuzzer_.randType(
        referenceQueryRunner_->supportedScalarTypes(), /*maxDepth=*/0));
  }
  return types;
}

std::vector<RowVectorPtr> JoinFuzzer::generateProbeInput(
    const std::vector<std::string>& keyNames,
    const std::vector<TypePtr>& keyTypes) {
  std::vector<std::string> names = keyNames;
  std::vector<TypePtr> types = keyTypes;

  bool keyTypesAllBool = true;
  for (const auto& type : keyTypes) {
    if (!type->isBoolean()) {
      keyTypesAllBool = false;
      break;
    }
  }

  // Add up to 3 payload columns.
  const auto numPayload = randInt(0, 3);
  for (auto i = 0; i < numPayload; ++i) {
    names.push_back(fmt::format("tp{}", i + keyNames.size()));
    types.push_back(vectorFuzzer_.randType(
        referenceQueryRunner_->supportedScalarTypes(), /*maxDepth=*/2));
  }

  const auto inputType = ROW(std::move(names), std::move(types));
  std::vector<RowVectorPtr> input;
  for (auto i = 0; i < FLAGS_num_batches; ++i) {
    if (keyTypesAllBool) {
      // Joining on just boolean keys creates so many hits it explodes the
      // output size, reduce the batch size to 10% to control the output size
      // while still covering this case.
      input.push_back(
          vectorFuzzer_.fuzzRow(inputType, FLAGS_batch_size / 10, false));
    } else {
      input.push_back(vectorFuzzer_.fuzzInputRow(inputType));
    }
  }
  return input;
}

std::vector<RowVectorPtr> JoinFuzzer::generateBuildInput(
    const std::vector<RowVectorPtr>& probeInput,
    const std::vector<std::string>& probeKeys,
    const std::vector<std::string>& buildKeys) {
  std::vector<std::string> names = buildKeys;
  std::vector<TypePtr> types;
  for (const auto& key : probeKeys) {
    types.push_back(asRowType(probeInput[0]->type())->findChild(key));
  }

  // Add up to 3 payload columns.
  const auto numPayload = randInt(0, 3);
  for (auto i = 0; i < numPayload; ++i) {
    names.push_back(fmt::format("bp{}", i + buildKeys.size()));
    types.push_back(vectorFuzzer_.randType(
        referenceQueryRunner_->supportedScalarTypes(), /*maxDepth=*/2));
  }

  const auto rowType = ROW(std::move(names), std::move(types));

  // 1 in 10 times use empty build.
  // TODO Use non-empty build with no matches sometimes.
  if (vectorFuzzer_.coinToss(0.1)) {
    return {BaseVector::create<RowVector>(rowType, 0, pool_.get())};
  }

  // TODO Remove the assumption that probeKeys are the first columns in
  // probeInput.

  // To ensure there are some matches, sample with replacement 10% of probe join
  // keys and use these as 80% of build keys. The rest build keys are randomly
  // generated. This allows the build side to have unmatched rows that should
  // appear in right join and full join.
  std::vector<RowVectorPtr> input;
  for (const auto& probe : probeInput) {
    auto numRows = 1 + probe->size() / 8;
    auto build = vectorFuzzer_.fuzzRow(rowType, numRows, false);

    // Pick probe side rows to copy.
    std::vector<vector_size_t> rowNumbers(numRows);
    SelectivityVector rows(numRows, false);
    for (auto i = 0; i < numRows; ++i) {
      if (vectorFuzzer_.coinToss(0.8) && probe->size() > 0) {
        rowNumbers[i] = randInt(0, probe->size() - 1);
        rows.setValid(i, true);
      }
    }

    for (auto i = 0; i < probeKeys.size(); ++i) {
      build->childAt(i)->resize(numRows);
      build->childAt(i)->copy(probe->childAt(i).get(), rows, rowNumbers.data());
    }

    for (auto i = 0; i < numPayload; ++i) {
      auto column = i + probeKeys.size();
      build->childAt(column) =
          vectorFuzzer_.fuzz(rowType->childAt(column), numRows);
    }

    input.push_back(build);
  }

  return input;
}

std::vector<RowVectorPtr> flatten(const std::vector<RowVectorPtr>& vectors) {
  std::vector<RowVectorPtr> flatVectors;
  for (const auto& vector : vectors) {
    auto flat = BaseVector::create<RowVector>(
        vector->type(), vector->size(), vector->pool());
    flat->copy(vector.get(), 0, 0, vector->size());
    flatVectors.push_back(flat);
  }

  return flatVectors;
}

RowVectorPtr JoinFuzzer::execute(const PlanWithSplits& plan, bool injectSpill) {
  LOG(INFO) << "Executing query plan with "
            << executionStrategyToString(plan.executionStrategy) << " strategy["
            << (plan.executionStrategy == core::ExecutionStrategy::kGrouped
                    ? plan.numGroups
                    : 0)
            << " groups]" << (injectSpill ? " and spilling injection" : "")
            << ": " << std::endl
            << plan.plan->toString(true, true);

  AssertQueryBuilder builder(plan.plan);
  for (const auto& [planNodeId, nodeSplits] : plan.splits) {
    builder.splits(planNodeId, nodeSplits);
  }

  if (plan.executionStrategy == core::ExecutionStrategy::kGrouped) {
    builder.executionStrategy(core::ExecutionStrategy::kGrouped);
    builder.groupedExecutionLeafNodeIds({plan.probeScanId, plan.buildScanId});
    builder.numSplitGroups(plan.numGroups);
    builder.numConcurrentSplitGroups(randInt(1, plan.numGroups));
  }

  std::shared_ptr<TempDirectoryPath> spillDirectory;
  int32_t spillPct{0};
  if (injectSpill) {
    spillDirectory = exec::test::TempDirectoryPath::create();
    builder.config(core::QueryConfig::kSpillEnabled, true)
        .config(core::QueryConfig::kJoinSpillEnabled, true)
        .spillDirectory(spillDirectory->getPath());
    spillPct = 10;
  }

  ScopedOOMInjector oomInjector(
      []() -> bool { return folly::Random::oneIn(10); },
      10); // Check the condition every 10 ms.
  if (FLAGS_enable_oom_injection) {
    oomInjector.enable();
  }

  TestScopedSpillInjection scopedSpillInjection(spillPct);
  RowVectorPtr result;
  try {
    result = builder.maxDrivers(2).copyResults(pool_.get());
  } catch (VeloxRuntimeError& e) {
    if (FLAGS_enable_oom_injection &&
        e.errorCode() == facebook::velox::error_code::kMemCapExceeded &&
        e.message() == ScopedOOMInjector::kErrorMessage) {
      // If we enabled OOM injection we expect the exception thrown by the
      // ScopedOOMInjector.
      return nullptr;
    }

    throw e;
  }
  LOG(INFO) << "Results: " << result->toString();
  if (VLOG_IS_ON(1)) {
    VLOG(1) << std::endl << result->toString(0, result->size());
  }
  // Wait for the task to be destroyed before start next query execution to
  // avoid the potential interference of the background activities across query
  // executions.
  waitForAllTasksToBeDeleted();
  return result;
}

std::optional<core::JoinType> tryFlipJoinType(core::JoinType joinType) {
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
      return std::nullopt;
  }
}

// Returns a plan with flipped join sides of the input hash join node. If the
// join type doesn't allow flipping, returns a nullptr.
core::PlanNodePtr tryFlipJoinSides(const core::HashJoinNode& joinNode) {
  auto flippedJoinType = tryFlipJoinType(joinNode.joinType());
  if (!flippedJoinType.has_value()) {
    return nullptr;
  }

  return std::make_shared<core::HashJoinNode>(
      joinNode.id(),
      flippedJoinType.value(),
      joinNode.isNullAware(),
      joinNode.rightKeys(),
      joinNode.leftKeys(),
      joinNode.filter(),
      joinNode.sources()[1],
      joinNode.sources()[0],
      joinNode.outputType());
}

// Returns a plan with flipped join sides of the input merge join node. If the
// join type doesn't allow flipping, returns a nullptr.
core::PlanNodePtr tryFlipJoinSides(const core::MergeJoinNode& joinNode) {
  // Merge join only supports inner and left join, so only inner join can be
  // flipped.
  if (joinNode.joinType() != core::JoinType::kInner) {
    return nullptr;
  }
  auto flippedJoinType = core::JoinType::kInner;

  return std::make_shared<core::MergeJoinNode>(
      joinNode.id(),
      flippedJoinType,
      joinNode.rightKeys(),
      joinNode.leftKeys(),
      joinNode.filter(),
      joinNode.sources()[1],
      joinNode.sources()[0],
      joinNode.outputType());
}

// Returns a plan with flipped join sides of the input nested loop join node. If
// the join type doesn't allow flipping, returns a nullptr.
core::PlanNodePtr tryFlipJoinSides(const core::NestedLoopJoinNode& joinNode) {
  auto flippedJoinType = tryFlipJoinType(joinNode.joinType());
  if (!flippedJoinType.has_value()) {
    return nullptr;
  }

  return std::make_shared<core::NestedLoopJoinNode>(
      joinNode.id(),
      flippedJoinType.value(),
      joinNode.joinCondition(),
      joinNode.sources()[1],
      joinNode.sources()[0],
      joinNode.outputType());
}

std::optional<MaterializedRowMultiset> JoinFuzzer::computeReferenceResults(
    const core::PlanNodePtr& plan,
    const std::vector<RowVectorPtr>& probeInput,
    const std::vector<RowVectorPtr>& buildInput) {
  if (referenceQueryRunner_->runnerType() ==
      ReferenceQueryRunner::RunnerType::kDuckQueryRunner) {
    VELOX_CHECK(!containsUnsupportedTypes(probeInput[0]->type()));
    VELOX_CHECK(!containsUnsupportedTypes(buildInput[0]->type()));
  }

  if (auto sql = referenceQueryRunner_->toSql(plan)) {
    return referenceQueryRunner_->execute(
        sql.value(), probeInput, buildInput, plan->outputType());
  }

  LOG(INFO) << "Query not supported by the reference DB";
  return std::nullopt;
}

std::vector<std::string> fieldNames(
    const std::vector<core::FieldAccessTypedExprPtr>& fields) {
  std::vector<std::string> names;
  names.reserve(fields.size());
  for (const auto& field : fields) {
    names.push_back(field->name());
  }
  return names;
}

JoinFuzzer::PlanWithSplits JoinFuzzer::makeDefaultPlan(
    core::JoinType joinType,
    bool nullAware,
    const std::vector<std::string>& probeKeys,
    const std::vector<std::string>& buildKeys,
    const std::vector<RowVectorPtr>& probeInput,
    const std::vector<RowVectorPtr>& buildInput,
    const std::vector<std::string>& outputColumns) {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .values(probeInput)
          .hashJoin(
              probeKeys,
              buildKeys,
              PlanBuilder(planNodeIdGenerator).values(buildInput).planNode(),
              /*filter=*/"",
              outputColumns,
              joinType,
              nullAware)
          .planNode();
  return PlanWithSplits{plan};
}

JoinFuzzer::PlanWithSplits JoinFuzzer::makeDefaultPlanWithTableScan(
    core::JoinType joinType,
    bool nullAware,
    const RowTypePtr& probeType,
    const RowTypePtr& buildType,
    const std::vector<std::string>& probeKeys,
    const std::vector<std::string>& buildKeys,
    const std::vector<Split>& probeSplits,
    const std::vector<Split>& buildSplits,
    const std::vector<std::string>& outputColumns) {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId probeScanId;
  core::PlanNodeId buildScanId;
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .tableScan(probeType)
                  .capturePlanNodeId(probeScanId)
                  .hashJoin(
                      probeKeys,
                      buildKeys,
                      PlanBuilder(planNodeIdGenerator)
                          .tableScan(buildType)
                          .capturePlanNodeId(buildScanId)
                          .planNode(),
                      /*filter=*/"",
                      outputColumns,
                      joinType,
                      nullAware)
                  .planNode();
  return PlanWithSplits{
      plan,
      probeScanId,
      buildScanId,
      {{probeScanId, probeSplits}, {buildScanId, buildSplits}}};
}

JoinFuzzer::PlanWithSplits JoinFuzzer::makeGroupedExecutionPlanWithTableScan(
    const JoinFuzzer::PlanWithSplits& planWithTableScan,
    int32_t numGroups,
    const std::vector<exec::Split>& groupedProbeScanSplits,
    const std::vector<exec::Split>& groupedBuildScanSplits) {
  return PlanWithSplits{
      planWithTableScan.plan,
      planWithTableScan.probeScanId,
      planWithTableScan.buildScanId,
      {{planWithTableScan.probeScanId, groupedProbeScanSplits},
       {planWithTableScan.buildScanId, groupedBuildScanSplits}},
      core::ExecutionStrategy::kGrouped,
      numGroups};
}

std::vector<core::PlanNodePtr> makeSources(
    const std::vector<RowVectorPtr>& input,
    std::shared_ptr<core::PlanNodeIdGenerator>& planNodeIdGenerator) {
  auto numSources = std::min<size_t>(4, input.size());
  std::vector<std::vector<RowVectorPtr>> sourceInputs(numSources);
  for (auto i = 0; i < input.size(); ++i) {
    sourceInputs[i % numSources].push_back(input[i]);
  }

  std::vector<core::PlanNodePtr> sourceNodes;
  for (const auto& sourceInput : sourceInputs) {
    sourceNodes.push_back(
        PlanBuilder(planNodeIdGenerator).values(sourceInput).planNode());
  }

  return sourceNodes;
}

// Returns an equality join filter between probeKeys and buildKeys.
std::string makeJoinFilter(
    const std::vector<std::string>& probeKeys,
    const std::vector<std::string>& buildKeys) {
  const auto numKeys = probeKeys.size();
  std::string filter;
  VELOX_CHECK_EQ(numKeys, buildKeys.size());
  for (auto i = 0; i < numKeys; ++i) {
    if (i > 0) {
      filter += " AND ";
    }
    filter += fmt::format("{} = {}", probeKeys[i], buildKeys[i]);
  }
  return filter;
}

template <typename TNode>
void addFlippedJoinPlan(
    const core::PlanNodePtr& plan,
    std::vector<JoinFuzzer::PlanWithSplits>& plans,
    const core::PlanNodeId& probeScanId = "",
    const core::PlanNodeId& buildScanId = "",
    const std::unordered_map<core::PlanNodeId, std::vector<velox::exec::Split>>&
        splits = {},
    core::ExecutionStrategy executionStrategy =
        core::ExecutionStrategy::kUngrouped,
    int32_t numGroups = 0) {
  auto joinNode = std::dynamic_pointer_cast<const TNode>(plan);
  VELOX_CHECK_NOT_NULL(joinNode);
  if (auto flippedPlan = tryFlipJoinSides(*joinNode)) {
    plans.push_back(JoinFuzzer::PlanWithSplits{
        flippedPlan,
        probeScanId,
        buildScanId,
        splits,
        executionStrategy,
        numGroups});
  }
}

JoinFuzzer::PlanWithSplits JoinFuzzer::makeMergeJoinPlan(
    core::JoinType joinType,
    const std::vector<std::string>& probeKeys,
    const std::vector<std::string>& buildKeys,
    const std::vector<RowVectorPtr>& probeInput,
    const std::vector<RowVectorPtr>& buildInput,
    const std::vector<std::string>& outputColumns) {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  return JoinFuzzer::PlanWithSplits{PlanBuilder(planNodeIdGenerator)
                                        .values(probeInput)
                                        .orderBy(probeKeys, false)
                                        .mergeJoin(
                                            probeKeys,
                                            buildKeys,
                                            PlanBuilder(planNodeIdGenerator)
                                                .values(buildInput)
                                                .orderBy(buildKeys, false)
                                                .planNode(),
                                            /*filter=*/"",
                                            outputColumns,
                                            joinType)
                                        .planNode()};
}

JoinFuzzer::PlanWithSplits JoinFuzzer::makeNestedLoopJoinPlan(
    core::JoinType joinType,
    const std::vector<std::string>& probeKeys,
    const std::vector<std::string>& buildKeys,
    const std::vector<RowVectorPtr>& probeInput,
    const std::vector<RowVectorPtr>& buildInput,
    const std::vector<std::string>& outputColumns,
    bool withFilter) {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  const std::string filter =
      withFilter ? makeJoinFilter(probeKeys, buildKeys) : "";
  return JoinFuzzer::PlanWithSplits{
      PlanBuilder(planNodeIdGenerator)
          .values(probeInput)
          .nestedLoopJoin(
              PlanBuilder(planNodeIdGenerator).values(buildInput).planNode(),
              filter,
              outputColumns,
              joinType)
          .planNode()};
}

void JoinFuzzer::makeAlternativePlans(
    const core::PlanNodePtr& plan,
    const std::vector<RowVectorPtr>& probeInput,
    const std::vector<RowVectorPtr>& buildInput,
    std::vector<JoinFuzzer::PlanWithSplits>& plans) {
  auto joinNode = std::dynamic_pointer_cast<const core::HashJoinNode>(plan);
  VELOX_CHECK_NOT_NULL(joinNode);

  // Flip join sides.
  addFlippedJoinPlan<core::HashJoinNode>(plan, plans);

  // Parallelize probe and build sides.
  const auto probeKeys = fieldNames(joinNode->leftKeys());
  const auto buildKeys = fieldNames(joinNode->rightKeys());
  const auto outputColumns = joinNode->outputType()->names();
  const auto joinType = joinNode->joinType();

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  plans.push_back(JoinFuzzer::PlanWithSplits{
      PlanBuilder(planNodeIdGenerator)
          .localPartitionRoundRobin(
              makeSources(probeInput, planNodeIdGenerator))
          .hashJoin(
              probeKeys,
              buildKeys,
              PlanBuilder(planNodeIdGenerator)
                  .localPartitionRoundRobin(
                      makeSources(buildInput, planNodeIdGenerator))
                  .planNode(),
              /*filter=*/"",
              outputColumns,
              joinType,
              joinNode->isNullAware())
          .planNode()});

  // Use OrderBy + MergeJoin
  if (core::MergeJoinNode::isSupported(joinNode->joinType())) {
    auto planWithSplits = makeMergeJoinPlan(
        joinType, probeKeys, buildKeys, probeInput, buildInput, outputColumns);
    plans.push_back(planWithSplits);

    addFlippedJoinPlan<core::MergeJoinNode>(planWithSplits.plan, plans);
  }

  // Use NestedLoopJoin.
  if (core::NestedLoopJoinNode::isSupported(joinNode->joinType())) {
    auto planWithSplits = makeNestedLoopJoinPlan(
        joinType, probeKeys, buildKeys, probeInput, buildInput, outputColumns);
    plans.push_back(planWithSplits);

    addFlippedJoinPlan<core::NestedLoopJoinNode>(planWithSplits.plan, plans);
  }
}

void JoinFuzzer::shuffleJoinKeys(
    std::vector<std::string>& probeKeys,
    std::vector<std::string>& buildKeys) {
  auto numKeys = probeKeys.size();
  if (numKeys == 1) {
    return;
  }

  std::vector<column_index_t> columnIndices(numKeys);
  std::iota(columnIndices.begin(), columnIndices.end(), 0);
  std::shuffle(columnIndices.begin(), columnIndices.end(), rng_);

  auto copyProbeKeys = probeKeys;
  auto copyBuildKeys = buildKeys;

  for (auto i = 0; i < numKeys; ++i) {
    probeKeys[i] = copyProbeKeys[columnIndices[i]];
    buildKeys[i] = copyBuildKeys[columnIndices[i]];
  }
}

RowVectorPtr JoinFuzzer::testCrossProduct(
    const std::string& tableDir,
    core::JoinType joinType,
    const std::vector<std::string>& probeKeys,
    const std::vector<std::string>& buildKeys,
    const std::vector<RowVectorPtr>& probeInput,
    const std::vector<RowVectorPtr>& buildInput) {
  VELOX_CHECK_GT(probeInput.size(), 0);
  VELOX_CHECK_GT(buildInput.size(), 0);

  const auto probeType = asRowType(probeInput[0]->type());
  const auto buildType = asRowType(buildInput[0]->type());
  auto outputColumns =
      concat(asRowType(probeInput[0]->type()), asRowType(buildInput[0]->type()))
          ->names();

  auto plan = makeNestedLoopJoinPlan(
      joinType,
      probeKeys,
      buildKeys,
      probeInput,
      buildInput,
      outputColumns,
      /*withFilter*/ false);
  const auto expected = execute(plan, /*injectSpill=*/false);

  // If OOM injection is not enabled verify the results against Reference query
  // runner.
  if (!FLAGS_enable_oom_injection) {
    if (auto referenceResult =
            computeReferenceResults(plan.plan, probeInput, buildInput)) {
      VELOX_CHECK(
          assertEqualResults(
              referenceResult.value(), plan.plan->outputType(), {expected}),
          "Velox and DuckDB results don't match");

      LOG(INFO) << "Result matches with referenc DB.";
      stats_.numVerified++;
    }
  }

  std::vector<PlanWithSplits> altPlans;
  if (isTableScanSupported(probeInput[0]->type()) &&
      isTableScanSupported(buildInput[0]->type())) {
    std::vector<Split> probeScanSplits =
        makeSplits(probeInput, fmt::format("{}/probe", tableDir), writerPool_);
    std::vector<Split> buildScanSplits =
        makeSplits(buildInput, fmt::format("{}/build", tableDir), writerPool_);

    altPlans.push_back(makeNestedLoopJoinPlanWithTableScan(
        joinType,
        probeType,
        buildType,
        probeKeys,
        buildKeys,
        probeScanSplits,
        buildScanSplits,
        outputColumns,
        /*withFilter*/ false));
  }
  addFlippedJoinPlan<core::NestedLoopJoinNode>(plan.plan, altPlans);

  for (const auto& altPlan : altPlans) {
    auto actual = execute(altPlan, /*injectSpill=*/false);
    if (actual != nullptr && expected != nullptr) {
      VELOX_CHECK(
          assertEqualResults({expected}, {actual}),
          "Logically equivalent plans produced different results");
    }
  }
  return expected;
}

void JoinFuzzer::verify(core::JoinType joinType) {
  const bool nullAware =
      isNullAwareSupported(joinType) && vectorFuzzer_.coinToss(0.5);

  const auto numKeys = nullAware ? 1 : randInt(1, 5);

  // Pick number and types of join keys.
  const std::vector<TypePtr> keyTypes = generateJoinKeyTypes(numKeys);
  std::vector<std::string> probeKeys = makeNames("t", keyTypes.size());
  std::vector<std::string> buildKeys = makeNames("u", keyTypes.size());

  auto probeInput = generateProbeInput(probeKeys, keyTypes);
  auto buildInput = generateBuildInput(probeInput, probeKeys, buildKeys);

  // Flatten inputs.
  auto flatProbeInput = flatten(probeInput);
  auto flatBuildInput = flatten(buildInput);

  if (VLOG_IS_ON(1)) {
    VLOG(1) << "Probe input: " << probeInput[0]->toString();
    for (const auto& v : flatProbeInput) {
      VLOG(1) << std::endl << v->toString(0, v->size());
    }

    VLOG(1) << "Build input: " << buildInput[0]->toString();
    for (const auto& v : flatBuildInput) {
      VLOG(1) << std::endl << v->toString(0, v->size());
    }
  }

  const auto tableScanDir = exec::test::TempDirectoryPath::create();

  // Test cross product without filter with 10% chance. Avoid testing cross
  // product if input size is too large.
  if ((core::isInnerJoin(joinType) || core::isLeftJoin(joinType) ||
       core::isFullJoin(joinType)) &&
      FLAGS_batch_size * FLAGS_num_batches <= 500) {
    if (vectorFuzzer_.coinToss(0.1)) {
      stats_.numCrossProduct++;

      auto result = testCrossProduct(
          tableScanDir->getPath(),
          joinType,
          probeKeys,
          buildKeys,
          probeInput,
          buildInput);
      auto flatResult = testCrossProduct(
          tableScanDir->getPath(),
          joinType,
          probeKeys,
          buildKeys,
          flatProbeInput,
          flatBuildInput);
      assertEqualResults({result}, {flatResult});
    }
  }

  auto outputColumns =
      (core::isLeftSemiProjectJoin(joinType) ||
       core::isLeftSemiFilterJoin(joinType) || core::isAntiJoin(joinType))
      ? asRowType(probeInput[0]->type())->names()
      : concat(
            asRowType(probeInput[0]->type()), asRowType(buildInput[0]->type()))
            ->names();

  // Shuffle output columns.
  std::shuffle(outputColumns.begin(), outputColumns.end(), rng_);

  // Remove some output columns.
  const auto numOutput = randInt(1, outputColumns.size());
  outputColumns.resize(numOutput);

  if (core::isLeftSemiProjectJoin(joinType) ||
      core::isRightSemiProjectJoin(joinType)) {
    outputColumns.push_back("match");
  }

  shuffleJoinKeys(probeKeys, buildKeys);

  const auto defaultPlan = makeDefaultPlan(
      joinType,
      nullAware,
      probeKeys,
      buildKeys,
      probeInput,
      buildInput,
      outputColumns);

  const auto expected = execute(defaultPlan, /*injectSpill=*/false);

  // If OOM injection is not enabled verify the results against Reference query
  // runner.
  if (!FLAGS_enable_oom_injection) {
    if (auto referenceResult =
            computeReferenceResults(defaultPlan.plan, probeInput, buildInput)) {
      VELOX_CHECK(
          assertEqualResults(
              referenceResult.value(),
              defaultPlan.plan->outputType(),
              {expected}),
          "Velox and Reference results don't match");

      LOG(INFO) << "Result matches with referenc DB.";
      stats_.numVerified++;
    }
  }

  std::vector<PlanWithSplits> altPlans;
  altPlans.push_back(makeDefaultPlan(
      joinType,
      nullAware,
      probeKeys,
      buildKeys,
      flatProbeInput,
      flatBuildInput,
      outputColumns));

  makeAlternativePlans(defaultPlan.plan, probeInput, buildInput, altPlans);
  makeAlternativePlans(
      defaultPlan.plan, flatProbeInput, flatBuildInput, altPlans);

  addPlansWithTableScan(
      tableScanDir->getPath(),
      joinType,
      nullAware,
      probeKeys,
      buildKeys,
      flatProbeInput,
      flatBuildInput,
      outputColumns,
      altPlans);

  for (auto i = 0; i < altPlans.size(); ++i) {
    LOG(INFO) << "Testing plan #" << i;
    auto actual = execute(altPlans[i], /*injectSpill=*/false);
    if (actual != nullptr && expected != nullptr) {
      VELOX_CHECK(
          assertEqualResults({expected}, {actual}),
          "Logically equivalent plans produced different results");
    } else {
      VELOX_CHECK(
          FLAGS_enable_oom_injection, "Got unexpected nullptr for results");
    }

    if (FLAGS_enable_spill) {
      // Spilling for right semi project doesn't work yet.
      if (auto hashJoin = std::dynamic_pointer_cast<const core::HashJoinNode>(
              altPlans[i].plan)) {
        if (hashJoin->isRightSemiProjectJoin()) {
          continue;
        }
      }

      LOG(INFO) << "Testing plan #" << i << " with spilling";
      actual = execute(altPlans[i], /*=injectSpill=*/true);
      if (actual != nullptr && expected != nullptr) {
        try {
          VELOX_CHECK(
              assertEqualResults({expected}, {actual}),
              "Logically equivalent plans produced different results");
        } catch (const VeloxException&) {
          LOG(ERROR) << "Expected\n"
                     << expected->toString(0, expected->size()) << "\nActual\n"
                     << actual->toString(0, actual->size());
          throw;
        }
      } else {
        VELOX_CHECK(
            FLAGS_enable_oom_injection, "Got unexpected nullptr for results");
      }
    }
  }
}

JoinFuzzer::PlanWithSplits JoinFuzzer::makeMergeJoinPlanWithTableScan(
    core::JoinType joinType,
    const RowTypePtr& probeType,
    const RowTypePtr& buildType,
    const std::vector<std::string>& probeKeys,
    const std::vector<std::string>& buildKeys,
    const std::vector<Split>& probeSplits,
    const std::vector<Split>& buildSplits,
    const std::vector<std::string>& outputColumns) {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId probeScanId;
  core::PlanNodeId buildScanId;

  return JoinFuzzer::PlanWithSplits{
      PlanBuilder(planNodeIdGenerator)
          .tableScan(probeType)
          .capturePlanNodeId(probeScanId)
          .orderBy(probeKeys, false)
          .mergeJoin(
              probeKeys,
              buildKeys,
              PlanBuilder(planNodeIdGenerator)
                  .tableScan(buildType)
                  .capturePlanNodeId(buildScanId)
                  .orderBy(buildKeys, false)
                  .planNode(),
              /*filter=*/"",
              outputColumns,
              joinType)
          .planNode(),
      probeScanId,
      buildScanId,
      {{probeScanId, probeSplits}, {buildScanId, buildSplits}}};
}

JoinFuzzer::PlanWithSplits JoinFuzzer::makeNestedLoopJoinPlanWithTableScan(
    core::JoinType joinType,
    const RowTypePtr& probeType,
    const RowTypePtr& buildType,
    const std::vector<std::string>& probeKeys,
    const std::vector<std::string>& buildKeys,
    const std::vector<Split>& probeSplits,
    const std::vector<Split>& buildSplits,
    const std::vector<std::string>& outputColumns,
    bool withFilter) {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId probeScanId;
  core::PlanNodeId buildScanId;

  const std::string filter =
      withFilter ? makeJoinFilter(probeKeys, buildKeys) : "";
  return JoinFuzzer::PlanWithSplits{
      PlanBuilder(planNodeIdGenerator)
          .tableScan(probeType)
          .capturePlanNodeId(probeScanId)
          .nestedLoopJoin(
              PlanBuilder(planNodeIdGenerator)
                  .tableScan(buildType)
                  .capturePlanNodeId(buildScanId)
                  .planNode(),
              filter,
              outputColumns,
              joinType)
          .planNode(),
      probeScanId,
      buildScanId,
      {{probeScanId, probeSplits}, {buildScanId, buildSplits}}};
}

void JoinFuzzer::addPlansWithTableScan(
    const std::string& tableDir,
    core::JoinType joinType,
    bool nullAware,
    const std::vector<std::string>& probeKeys,
    const std::vector<std::string>& buildKeys,
    const std::vector<RowVectorPtr>& probeInput,
    const std::vector<RowVectorPtr>& buildInput,
    const std::vector<std::string>& outputColumns,
    std::vector<PlanWithSplits>& altPlans) {
  VELOX_CHECK(!tableDir.empty());

  if (!isTableScanSupported(probeInput[0]->type()) ||
      !isTableScanSupported(buildInput[0]->type())) {
    return;
  }

  std::vector<Split> probeScanSplits =
      makeSplits(probeInput, fmt::format("{}/probe", tableDir), writerPool_);
  std::vector<Split> buildScanSplits =
      makeSplits(buildInput, fmt::format("{}/build", tableDir), writerPool_);

  const auto probeType = asRowType(probeInput[0]->type());
  const auto buildType = asRowType(buildInput[0]->type());

  std::vector<PlanWithSplits> plansWithTableScan;
  auto defaultPlan = makeDefaultPlanWithTableScan(
      joinType,
      nullAware,
      probeType,
      buildType,
      probeKeys,
      buildKeys,
      probeScanSplits,
      buildScanSplits,
      outputColumns);
  plansWithTableScan.push_back(defaultPlan);

  auto joinNode =
      std::dynamic_pointer_cast<const core::HashJoinNode>(defaultPlan.plan);
  VELOX_CHECK_NOT_NULL(joinNode);

  // Flip join sides.
  addFlippedJoinPlan<core::HashJoinNode>(
      defaultPlan.plan,
      plansWithTableScan,
      defaultPlan.probeScanId,
      defaultPlan.buildScanId,
      defaultPlan.splits);

  const int32_t numGroups = randInt(1, probeScanSplits.size());
  const std::vector<exec::Split> groupedProbeScanSplits =
      generateSplitsWithGroup(
          tableDir,
          numGroups,
          /*isProbe=*/true,
          probeKeys.size(),
          probeInput);
  const std::vector<exec::Split> groupedBuildScanSplits =
      generateSplitsWithGroup(
          tableDir,
          numGroups,
          /*isProbe=*/false,
          buildKeys.size(),
          buildInput);

  for (const auto& planWithTableScan : plansWithTableScan) {
    altPlans.push_back(planWithTableScan);
    altPlans.push_back(makeGroupedExecutionPlanWithTableScan(
        planWithTableScan,
        numGroups,
        groupedProbeScanSplits,
        groupedBuildScanSplits));
  }

  // Add ungrouped MergeJoin with TableScan.
  if (core::MergeJoinNode::isSupported(joinNode->joinType())) {
    auto planWithSplits = makeMergeJoinPlanWithTableScan(
        joinType,
        probeType,
        buildType,
        probeKeys,
        buildKeys,
        probeScanSplits,
        buildScanSplits,
        outputColumns);
    altPlans.push_back(planWithSplits);

    addFlippedJoinPlan<core::MergeJoinNode>(
        planWithSplits.plan,
        altPlans,
        planWithSplits.probeScanId,
        planWithSplits.buildScanId,
        {{planWithSplits.probeScanId, probeScanSplits},
         {planWithSplits.buildScanId, buildScanSplits}});
  }

  // Add ungrouped NestedLoopJoin with TableScan.
  if (core::NestedLoopJoinNode::isSupported(joinNode->joinType())) {
    auto planWithSplits = makeNestedLoopJoinPlanWithTableScan(
        joinType,
        probeType,
        buildType,
        probeKeys,
        buildKeys,
        probeScanSplits,
        buildScanSplits,
        outputColumns);
    altPlans.push_back(planWithSplits);

    addFlippedJoinPlan<core::NestedLoopJoinNode>(
        planWithSplits.plan,
        altPlans,
        planWithSplits.probeScanId,
        planWithSplits.buildScanId,
        {{planWithSplits.probeScanId, probeScanSplits},
         {planWithSplits.buildScanId, buildScanSplits}});
  }
}

std::vector<exec::Split> JoinFuzzer::generateSplitsWithGroup(
    const std::string& tableDir,
    int32_t numGroups,
    bool isProbe,
    size_t numKeys,
    const std::vector<RowVectorPtr>& input) {
  const std::vector<std::vector<RowVectorPtr>> inputVectorsByGroup =
      splitInputByGroup(numGroups, numKeys, input);

  std::vector<exec::Split> splitsWithGroup;
  for (int32_t groupId = 0; groupId < numGroups; ++groupId) {
    for (auto i = 0; i < inputVectorsByGroup[groupId].size(); ++i) {
      const std::string filePath = fmt::format(
          "{}/grouped[{}].{}.{}",
          tableDir,
          groupId,
          isProbe ? "probe" : "build",
          i);
      writeToFile(filePath, inputVectorsByGroup[groupId][i], writerPool_.get());
      splitsWithGroup.emplace_back(makeConnectorSplit(filePath), groupId);
    }
    splitsWithGroup.emplace_back(nullptr, groupId);
  }
  return splitsWithGroup;
}

std::vector<std::vector<RowVectorPtr>> JoinFuzzer::splitInputByGroup(
    int32_t numGroups,
    size_t numKeys,
    const std::vector<RowVectorPtr>& inputs) {
  if (numGroups == 1) {
    return {inputs};
  }

  // Partition 'input' based on the join keys for group execution with one
  // partition per each group.
  const RowTypePtr& inputType = asRowType(inputs[0]->type());
  std::vector<column_index_t> partitionChannels(numKeys);
  std::iota(partitionChannels.begin(), partitionChannels.end(), 0);
  std::vector<std::unique_ptr<exec::VectorHasher>> hashers;
  hashers.reserve(numKeys);
  for (auto channel : partitionChannels) {
    hashers.emplace_back(
        exec::VectorHasher::create(inputType->childAt(channel), channel));
  }

  std::vector<std::vector<RowVectorPtr>> inputsByGroup{
      static_cast<size_t>(numGroups)};
  raw_vector<uint64_t> groupHashes;
  std::vector<BufferPtr> groupRows(numGroups);
  std::vector<vector_size_t*> rawGroupRows(numGroups);
  std::vector<vector_size_t> groupSizes(numGroups, 0);
  SelectivityVector inputRows;

  for (const auto& input : inputs) {
    const int numRows = input->size();
    inputRows.resize(numRows);
    inputRows.setAll();
    groupHashes.resize(numRows);
    std::fill(groupSizes.begin(), groupSizes.end(), 0);
    std::fill(groupHashes.begin(), groupHashes.end(), 0);

    for (auto i = 0; i < hashers.size(); ++i) {
      auto& hasher = hashers[i];
      auto* keyVector = input->childAt(hashers[i]->channel())->loadedVector();
      hashers[i]->decode(*keyVector, inputRows);
      if (hasher->channel() != kConstantChannel) {
        hashers[i]->hash(inputRows, i > 0, groupHashes);
      } else {
        hashers[i]->hashPrecomputed(inputRows, i > 0, groupHashes);
      }
    }

    for (int row = 0; row < numRows; ++row) {
      const int32_t groupId = groupHashes[row] % numGroups;
      if (groupRows[groupId] == nullptr ||
          (groupRows[groupId]->capacity() < numRows * sizeof(vector_size_t))) {
        groupRows[groupId] = allocateIndices(numRows, pool_.get());
        rawGroupRows[groupId] = groupRows[groupId]->asMutable<vector_size_t>();
      }
      rawGroupRows[groupId][groupSizes[groupId]++] = row;
    }

    for (int32_t groupId = 0; groupId < numGroups; ++groupId) {
      const size_t groupSize = groupSizes[groupId];
      if (groupSize != 0) {
        VELOX_CHECK_NOT_NULL(groupRows[groupId]);
        groupRows[groupId]->setSize(
            groupSizes[groupId] * sizeof(vector_size_t));
        inputsByGroup[groupId].push_back(
            (groupSize == numRows)
                ? input
                : exec::wrap(groupSize, std::move(groupRows[groupId]), input));
      }
    }
  }
  return inputsByGroup;
}

void JoinFuzzer::go() {
  VELOX_USER_CHECK(
      FLAGS_steps > 0 || FLAGS_duration_sec > 0,
      "Either --steps or --duration_sec needs to be greater than zero.");
  VELOX_USER_CHECK_GE(FLAGS_batch_size, 10, "Batch size must be at least 10.");

  const auto startTime = std::chrono::system_clock::now();

  while (!isDone(stats_.numIterations, startTime)) {
    LOG(WARNING) << "==============================> Started iteration "
                 << stats_.numIterations << " (seed: " << currentSeed_ << ")";

    // Pick join type.
    const auto joinType = pickJoinType();

    verify(joinType);

    LOG(WARNING) << "==============================> Done with iteration "
                 << stats_.numIterations;

    reSeed();
    ++stats_.numIterations;
  }
  LOG(INFO) << stats_.toString();
}

} // namespace

void joinFuzzer(
    size_t seed,
    std::unique_ptr<test::ReferenceQueryRunner> referenceQueryRunner) {
  JoinFuzzer(seed, std::move(referenceQueryRunner)).go();
}
} // namespace facebook::velox::exec::test
