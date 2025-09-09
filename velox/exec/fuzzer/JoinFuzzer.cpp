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
#include "velox/dwio/dwrf/RegisterDwrfReader.h"
#include "velox/dwio/dwrf/RegisterDwrfWriter.h"
#include "velox/exec/HashJoinBridge.h"
#include "velox/exec/Spill.h"
#include "velox/exec/fuzzer/FuzzerUtil.h"
#include "velox/exec/fuzzer/JoinMaker.h"
#include "velox/exec/fuzzer/ReferenceQueryRunner.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
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

DEFINE_double(
    filter_ratio,
    0,
    "The chance of testing plans with filters enabled.");

namespace facebook::velox::exec {

namespace {

std::string makePercentageString(size_t value, size_t total) {
  return fmt::format("{} ({:.2f}%)", value, (double)value / total * 100);
}

class JoinFuzzer {
 public:
  JoinFuzzer(
      size_t initialSeed,
      std::unique_ptr<test::ReferenceQueryRunner> referenceQueryRunner);

  void go();

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

  RowVectorPtr execute(const JoinMaker::PlanWithSplits& plan, bool injectSpill);

  std::optional<test::MaterializedRowMultiset> computeReferenceResults(
      const core::PlanNodePtr& plan,
      const std::vector<RowVectorPtr>& probeInput,
      const std::vector<RowVectorPtr>& buildInput);

  // Generates and executes plans using NestedLoopJoin without filters. The
  // result is compared to DuckDB. Returns the result vector of the cross
  // product.
  RowVectorPtr testCrossProduct(
      const JoinMaker& joinMaker,
      const JoinMaker::InputType inputType,
      const std::shared_ptr<JoinSource>& probeSource,
      const std::shared_ptr<JoinSource>& buildSource);

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
  std::unique_ptr<test::ReferenceQueryRunner> referenceQueryRunner_;

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
    std::unique_ptr<test::ReferenceQueryRunner> referenceQueryRunner)
    : vectorFuzzer_{getFuzzerOptions(), pool_.get()},
      referenceQueryRunner_{std::move(referenceQueryRunner)} {
  filesystems::registerLocalFileSystem();

  // Make sure not to run out of open file descriptors.
  std::unordered_map<std::string, std::string> hiveConfig = {
      {connector::hive::HiveConfig::kNumCacheFileHandles, "1000"}};

  connector::hive::HiveConnectorFactory factory;
  auto hiveConnector = factory.newConnector(
      test::kHiveConnectorId,
      std::make_shared<config::ConfigBase>(std::move(hiveConfig)));
  connector::registerConnector(hiveConnector);
  dwrf::registerDwrfReaderFactory();
  dwrf::registerDwrfWriterFactory();

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

RowVectorPtr JoinFuzzer::execute(
    const JoinMaker::PlanWithSplits& plan,
    bool injectSpill) {
  LOG(INFO) << "Executing query plan with "
            << executionStrategyToString(plan.executionStrategy)
            << (plan.executionStrategy == core::ExecutionStrategy::kGrouped
                    ? fmt::format(
                          "({})",
                          plan.mixedGroupedExecution ? "mixed" : "unmixed")
                    : "")
            << " strategy["
            << (plan.executionStrategy == core::ExecutionStrategy::kGrouped
                    ? plan.numGroups
                    : 0)
            << " groups]" << (injectSpill ? " and spilling injection" : "")
            << ": " << std::endl
            << plan.plan->toString(true, true);

  test::AssertQueryBuilder builder(plan.plan);
  for (const auto& [planNodeId, nodeSplits] : plan.splits) {
    builder.splits(planNodeId, nodeSplits);
  }

  if (plan.executionStrategy == core::ExecutionStrategy::kGrouped) {
    builder.executionStrategy(core::ExecutionStrategy::kGrouped);
    if (plan.mixedGroupedExecution) {
      builder.groupedExecutionLeafNodeIds({plan.probeScanId});
    } else {
      builder.groupedExecutionLeafNodeIds({plan.probeScanId, plan.buildScanId});
    }
    builder.numSplitGroups(plan.numGroups);
    builder.numConcurrentSplitGroups(randInt(1, plan.numGroups));
  }

  std::shared_ptr<test::TempDirectoryPath> spillDirectory;
  int32_t spillPct{0};
  if (injectSpill) {
    spillDirectory = exec::test::TempDirectoryPath::create();
    builder.config(core::QueryConfig::kSpillEnabled, true)
        .config(core::QueryConfig::kJoinSpillEnabled, true)
        .config(core::QueryConfig::kMixedGroupedModeHashJoinSpillEnabled, true)
        .spillDirectory(spillDirectory->getPath());
    spillPct = 10;
  }

  test::ScopedOOMInjector oomInjector(
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
        e.message() == test::ScopedOOMInjector::kErrorMessage) {
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
  test::waitForAllTasksToBeDeleted();
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

std::optional<test::MaterializedRowMultiset>
JoinFuzzer::computeReferenceResults(
    const core::PlanNodePtr& plan,
    const std::vector<RowVectorPtr>& probeInput,
    const std::vector<RowVectorPtr>& buildInput) {
  if (referenceQueryRunner_->runnerType() ==
      test::ReferenceQueryRunner::RunnerType::kDuckQueryRunner) {
    VELOX_CHECK(!test::containsUnsupportedTypes(probeInput[0]->type()));
    VELOX_CHECK(!test::containsUnsupportedTypes(buildInput[0]->type()));
  }

  auto result = referenceQueryRunner_->execute(plan);
  VELOX_CHECK_NE(
      result.second, test::ReferenceQueryErrorCode::kReferenceQueryFail);
  return result.first;
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
    const JoinMaker& joinMaker,
    const JoinMaker::InputType inputType,
    const std::shared_ptr<JoinSource>& probeSource,
    const std::shared_ptr<JoinSource>& buildSource) {
  auto plan =
      joinMaker.makeNestedLoopJoin(inputType, JoinMaker::JoinOrder::NATURAL);
  const auto expected = execute(plan, /*injectSpill=*/false);

  // If OOM injection is not enabled verify the results against Reference
  // query runner.
  if (!FLAGS_enable_oom_injection) {
    if (auto referenceResult = computeReferenceResults(
            plan.plan, probeSource->batches(), buildSource->batches())) {
      VELOX_CHECK(
          test::assertEqualResults(
              referenceResult.value(), plan.plan->outputType(), {expected}),
          "Velox and DuckDB results don't match");

      LOG(INFO) << "Result matches with referenc DB.";
      stats_.numVerified++;
    }
  }

  std::vector<JoinMaker::PlanWithSplits> altPlans;
  if (joinMaker.supportsTableScan()) {
    altPlans.push_back(joinMaker.makeNestedLoopJoinWithTableScan(
        JoinMaker::JoinOrder::NATURAL));
  }

  if (joinMaker.supportsFlippingNestedLoopJoin()) {
    altPlans.push_back(
        joinMaker.makeNestedLoopJoin(inputType, JoinMaker::JoinOrder::FLIPPED));
  }

  for (const auto& altPlan : altPlans) {
    auto actual = execute(altPlan, /*injectSpill=*/false);
    if (actual != nullptr && expected != nullptr) {
      VELOX_CHECK(
          test::assertEqualResults({expected}, {actual}),
          "Logically equivalent plans produced different results");
    }
  }
  return expected;
}

void addPlansForInputType(
    const JoinMaker::InputType inputType,
    const JoinMaker& joinMaker,
    std::vector<JoinMaker::PlanWithSplits>& plans) {
  plans.push_back(joinMaker.makeHashJoin(
      inputType,
      JoinMaker::PartitionStrategy::NONE,
      JoinMaker::JoinOrder::NATURAL));
  plans.push_back(joinMaker.makeHashJoin(
      inputType,
      JoinMaker::PartitionStrategy::ROUND_ROBIN,
      JoinMaker::JoinOrder::NATURAL));
  plans.push_back(joinMaker.makeHashJoin(
      inputType,
      JoinMaker::PartitionStrategy::HASH,
      JoinMaker::JoinOrder::NATURAL));
  if (joinMaker.supportsFlippingHashJoin()) {
    plans.push_back(joinMaker.makeHashJoin(
        inputType,
        JoinMaker::PartitionStrategy::NONE,
        JoinMaker::JoinOrder::FLIPPED));
  }

  if (joinMaker.supportsMergeJoin()) {
    plans.push_back(
        joinMaker.makeMergeJoin(inputType, JoinMaker::JoinOrder::NATURAL));
    if (joinMaker.supportsFlippingMergeJoin()) {
      plans.push_back(
          joinMaker.makeMergeJoin(inputType, JoinMaker::JoinOrder::FLIPPED));
    }
  }

  if (joinMaker.supportsNestedLoopJoin()) {
    plans.push_back(
        joinMaker.makeNestedLoopJoin(inputType, JoinMaker::JoinOrder::NATURAL));
    if (joinMaker.supportsFlippingNestedLoopJoin()) {
      plans.push_back(joinMaker.makeNestedLoopJoin(
          inputType, JoinMaker::JoinOrder::FLIPPED));
    }
  }
}

void JoinFuzzer::verify(core::JoinType joinType) {
  const bool nullAware =
      isNullAwareSupported(joinType) && vectorFuzzer_.coinToss(0.5);

  // Add boolean/integer join filter.
  const bool withFilter = vectorFuzzer_.coinToss(FLAGS_filter_ratio);
  // Null-aware joins allow only one join key.
  const int numKeys = nullAware ? (withFilter ? 0 : 1) : randInt(1, 5);
  std::vector<TypePtr> keyTypes = generateJoinKeyTypes(numKeys);
  std::string filter;

  if (withFilter) {
    if (vectorFuzzer_.coinToss(0.5)) {
      keyTypes.push_back(BOOLEAN());
      filter = vectorFuzzer_.coinToss(0.5)
          ? fmt::format("t{} = true", keyTypes.size() - 1)
          : fmt::format("u{} = true", keyTypes.size() - 1);
    } else {
      keyTypes.push_back(INTEGER());
      filter = vectorFuzzer_.coinToss(0.5)
          ? fmt::format("t{} % {} = 0", keyTypes.size() - 1, randInt(1, 9))
          : fmt::format("u{} % {} = 0", keyTypes.size() - 1, randInt(1, 9));
    }
  }

  const auto tableScanDir = exec::test::TempDirectoryPath::create();
  auto localFs = filesystems::getFileSystem(tableScanDir->getPath(), nullptr);
  std::string probePath =
      fmt::format("{}/{}", tableScanDir->getPath(), "probe");
  localFs->mkdir(probePath);
  std::string buildPath =
      fmt::format("{}/{}", tableScanDir->getPath(), "build");
  localFs->mkdir(buildPath);

  std::vector<std::string> probeKeys = test::makeNames("t", keyTypes.size());
  std::vector<std::string> buildKeys = test::makeNames("u", keyTypes.size());

  auto probeInput = generateProbeInput(probeKeys, keyTypes);
  auto buildInput = generateBuildInput(probeInput, probeKeys, buildKeys);

  auto [convertedProbeInput, probeProjections] =
      referenceQueryRunner_->inputProjections(probeInput);
  auto [convertedBuildInput, buildProjections] =
      referenceQueryRunner_->inputProjections(buildInput);

  VELOX_CHECK(!convertedProbeInput.empty());
  VELOX_CHECK(!convertedBuildInput.empty());

  auto probeSource = std::make_shared<JoinSource>(
      asRowType(convertedProbeInput[0]->type()),
      probeKeys,
      convertedProbeInput,
      probeProjections,
      probePath,
      writerPool_);

  auto buildSource = std::make_shared<JoinSource>(
      asRowType(convertedBuildInput[0]->type()),
      buildKeys,
      convertedBuildInput,
      buildProjections,
      buildPath,
      writerPool_);

  if (VLOG_IS_ON(1)) {
    VLOG(1) << "Probe input: " << convertedProbeInput[0]->toString();
    for (const auto& v : probeSource->flatBatches()) {
      VLOG(1) << std::endl << v->toString(0, v->size());
    }

    VLOG(1) << "Build input: " << convertedBuildInput[0]->toString();
    for (const auto& v : buildSource->flatBatches()) {
      VLOG(1) << std::endl << v->toString(0, v->size());
    }
  }

  // Test cross product without filter with 10% chance. Avoid testing cross
  // product if input size is too large.
  if ((core::isInnerJoin(joinType) || core::isLeftJoin(joinType) ||
       core::isFullJoin(joinType)) &&
      FLAGS_batch_size * FLAGS_num_batches <= 500) {
    if (vectorFuzzer_.coinToss(0.1)) {
      stats_.numCrossProduct++;

      JoinMaker crossJoinMaker(
          joinType,
          nullAware,
          {probeSource, buildSource},
          test::concat(probeSource->outputType(), buildSource->outputType())
              ->names(),
          "" // It's a cross join, so no filter.
      );

      auto result = testCrossProduct(
          crossJoinMaker,
          JoinMaker::InputType::ENCODED,
          probeSource,
          buildSource);
      auto flatResult = testCrossProduct(
          crossJoinMaker, JoinMaker::InputType::FLAT, probeSource, buildSource);
      test::assertEqualResults({result}, {flatResult});
    }
  }

  auto outputColumns =
      (core::isLeftSemiProjectJoin(joinType) ||
       core::isLeftSemiFilterJoin(joinType) || core::isAntiJoin(joinType))
      ? asRowType(probeInput[0]->type())->names()
      : test::concat(
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

  JoinMaker joinMaker(
      joinType, nullAware, {probeSource, buildSource}, outputColumns, filter);

  const auto defaultPlan = joinMaker.makeHashJoin(
      JoinMaker::InputType::ENCODED,
      JoinMaker::PartitionStrategy::NONE,
      JoinMaker::JoinOrder::NATURAL);

  const auto expected = execute(defaultPlan, /*injectSpill=*/false);

  // If OOM injection is not enabled verify the results against Reference
  // query runner.
  if (!FLAGS_enable_oom_injection) {
    if (auto referenceResult = computeReferenceResults(
            defaultPlan.plan, convertedProbeInput, convertedBuildInput)) {
      VELOX_CHECK(
          test::assertEqualResults(
              referenceResult.value(),
              defaultPlan.plan->outputType(),
              {expected}),
          "Velox and Reference results don't match");

      LOG(INFO) << "Result matches with reference DB.";
      stats_.numVerified++;
    }
  }

  std::vector<JoinMaker::PlanWithSplits> altPlans;

  addPlansForInputType(JoinMaker::InputType::ENCODED, joinMaker, altPlans);
  addPlansForInputType(JoinMaker::InputType::FLAT, joinMaker, altPlans);

  if (joinMaker.supportsTableScan()) {
    const int32_t numGroups = randInt(1, probeSource->batches().size());

    // Use ungrouped execution.
    altPlans.push_back(joinMaker.makeHashJoinWithTableScan(
        std::nullopt, false, JoinMaker::JoinOrder::NATURAL));

    // Use unmixed mode grouped execution.
    altPlans.push_back(joinMaker.makeHashJoinWithTableScan(
        numGroups, false, JoinMaker::JoinOrder::NATURAL));

    // Use mixed mode grouped execution.
    if (!needRightSideJoin(joinType)) {
      // Mixed grouped mode join does not support types that needs right side
      // join.
      altPlans.push_back(joinMaker.makeHashJoinWithTableScan(
          numGroups, true, JoinMaker::JoinOrder::NATURAL));
    }

    if (joinMaker.supportsFlippingHashJoin()) {
      // Use ungrouped execution.
      altPlans.push_back(joinMaker.makeHashJoinWithTableScan(
          std::nullopt, false, JoinMaker::JoinOrder::FLIPPED));

      // Use unmixed mode grouped execution.
      altPlans.push_back(joinMaker.makeHashJoinWithTableScan(
          numGroups, false, JoinMaker::JoinOrder::FLIPPED));

      // Use mixed mode grouped execution.
      if (!needRightSideJoin(flipJoinType(joinType))) {
        // Mixed grouped mode join does not support types that needs right side
        // join.
        altPlans.push_back(joinMaker.makeHashJoinWithTableScan(
            numGroups, true, JoinMaker::JoinOrder::FLIPPED));
      }
    }

    if (joinMaker.supportsMergeJoin()) {
      altPlans.push_back(
          joinMaker.makeMergeJoinWithTableScan(JoinMaker::JoinOrder::NATURAL));
      if (joinMaker.supportsFlippingMergeJoin()) {
        altPlans.push_back(joinMaker.makeMergeJoinWithTableScan(
            JoinMaker::JoinOrder::FLIPPED));
      }
    }

    if (joinMaker.supportsNestedLoopJoin()) {
      altPlans.push_back(joinMaker.makeNestedLoopJoinWithTableScan(
          JoinMaker::JoinOrder::NATURAL));
      if (joinMaker.supportsFlippingNestedLoopJoin()) {
        altPlans.push_back(joinMaker.makeNestedLoopJoinWithTableScan(
            JoinMaker::JoinOrder::FLIPPED));
      }
    }
  }

  for (auto i = 0; i < altPlans.size(); ++i) {
    LOG(INFO) << "Testing plan #" << i;
    auto actual = execute(altPlans[i], /*injectSpill=*/false);
    if (actual != nullptr && expected != nullptr) {
      VELOX_CHECK(
          test::assertEqualResults({expected}, {actual}),
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
              test::assertEqualResults({expected}, {actual}),
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
} // namespace facebook::velox::exec
