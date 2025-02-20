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

#include "velox/exec/fuzzer/MemoryArbitrationFuzzer.h"
#include <boost/random/uniform_int_distribution.hpp>

#include <folly/concurrency/ConcurrentHashMap.h>
#include "velox/common/file/FileSystems.h"
#include "velox/common/file/tests/FaultyFileSystem.h"
#include "velox/common/fuzzer/Utils.h"
#include "velox/common/memory/SharedArbitrator.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/dwio/dwrf/RegisterDwrfReader.h" // @manual
#include "velox/dwio/dwrf/RegisterDwrfWriter.h" // @manual
#include "velox/exec/MemoryReclaimer.h"
#include "velox/exec/fuzzer/FuzzerUtil.h"
#include "velox/exec/tests/utils/ArbitratorTestUtil.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/functions/sparksql/aggregates/Register.h"
#include "velox/serializers/CompactRowSerializer.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/serializers/UnsafeRowSerializer.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

DECLARE_int64(arbitrator_capacity);

DEFINE_int32(steps, 10, "Number of test iterations.");

DEFINE_int32(
    duration_sec,
    0,
    "For how long it should run (in seconds). If zero, "
    "it executes exactly --steps iterations and exits.");

DEFINE_int32(
    iteration_duration_sec,
    10,
    "For how long it should run (in seconds) per iteration.");

DEFINE_int32(
    batch_size,
    100,
    "The number of elements on each generated vector.");

DEFINE_int32(num_batches, 32, "The number of generated vectors.");

DEFINE_double(
    null_ratio,
    0.2,
    "Chance of adding a null value in a vector "
    "(expressed as double from 0 to 1).");

DEFINE_int32(
    num_threads,
    72,
    "The number of threads to run queries concurrently for each iteration.");

DEFINE_int64(arbitrator_capacity, 256L << 20, "Arbitrator capacity in bytes.");

DEFINE_int32(
    global_arbitration_pct,
    5,
    "Each second, the percentage chance of triggering global arbitration by "
    "calling shrinking pools globally.");

DEFINE_double(
    spill_faulty_fs_ratio,
    0.1,
    "Chance of spill filesystem being faulty(expressed as double from 0 to 1)");

DEFINE_int32(
    spill_fs_fault_injection_ratio,
    0.01,
    "The chance of actually injecting fault in file operations for spill "
    "filesystem. This is only applicable when 'spill_faulty_fs_ratio' is "
    "larger than 0");

DEFINE_int32(
    task_abort_interval_ms,
    1000,
    "After each specified number of milliseconds, abort a random task."
    "If given 0, no task will be aborted.");

using namespace facebook::velox::tests::utils;

namespace facebook::velox::exec::test {
namespace {

using fuzzer::coinToss;

class MemoryArbitrationFuzzer {
 public:
  explicit MemoryArbitrationFuzzer(size_t initialSeed);

  struct PlanWithSplits {
    core::PlanNodePtr plan;
    std::unordered_map<core::PlanNodeId, std::vector<Split>> splits;

    PlanWithSplits(
        core::PlanNodePtr _plan,
        const std::unordered_map<core::PlanNodeId, std::vector<Split>>& _splits)
        : plan(std::move(_plan)), splits(_splits) {}
  };

  struct Stats {
    size_t successCount{0};
    size_t oomCount{0};
    size_t abortCount{0};

    void print() const {
      std::stringstream ss;
      ss << "success count = " << successCount << ", oom count  = " << oomCount
         << ", abort count = " << abortCount;
      LOG(INFO) << ss.str();
    }
  };

  void go();

 private:
  void seed(size_t seed) {
    currentSeed_ = seed;
    vectorFuzzer_.reSeed(seed);
    rng_.seed(currentSeed_);
  }

  void reSeed() {
    seed(rng_());
  }

  int32_t randInt(int32_t min, int32_t max) {
    return boost::random::uniform_int_distribution<int32_t>(min, max)(rng_);
  }

  std::shared_ptr<TempDirectoryPath> maybeGenerateFaultySpillDirectory();

  // Returns a list of randomly generated key types for join and aggregation.
  std::vector<TypePtr> generateKeyTypes(int32_t numKeys);

  std::pair<std::vector<std::string>, std::vector<TypePtr>>
  generatePartitionKeys();

  // Returns randomly generated input with up to 3 additional payload columns.
  std::vector<RowVectorPtr> generateInput(
      const std::vector<std::string>& keyNames,
      const std::vector<TypePtr>& keyTypes);

  // Reuses the 'generateInput' method to return randomly generated
  // probe input.
  std::vector<RowVectorPtr> generateProbeInput(
      const std::vector<std::string>& keyNames,
      const std::vector<TypePtr>& keyTypes);

  // Reuses the 'generateInput' method to return randomly generated
  // aggregation input.
  std::vector<RowVectorPtr> generateAggregateInput(
      const std::vector<std::string>& keyNames,
      const std::vector<TypePtr>& keyTypes);

  // Reuses the 'generateInput' method to return randomly generated
  // row number input.
  std::vector<RowVectorPtr> generateRowNumberInput(
      const std::vector<std::string>& keyNames,
      const std::vector<TypePtr>& keyTypes);

  // Reuses the 'generateInput' method to return randomly generated
  // order by input.
  std::vector<RowVectorPtr> generateOrderByInput(
      const std::vector<std::string>& keyNames,
      const std::vector<TypePtr>& keyTypes);

  // Same as generateProbeInput() but copies over 10% of the input in the probe
  // columns to ensure some matches during joining. Also generates an empty
  // input with a 10% chance.
  std::vector<RowVectorPtr> generateBuildInput(
      const std::vector<RowVectorPtr>& probeInput,
      const std::vector<std::string>& probeKeys,
      const std::vector<std::string>& buildKeys);

  static std::vector<PlanWithSplits> hashJoinPlans(
      const core::JoinType& joinType,
      const std::vector<std::string>& probeKeys,
      const std::vector<std::string>& buildKeys,
      const std::vector<RowVectorPtr>& probeInput,
      const std::vector<RowVectorPtr>& buildInput,
      const std::vector<Split>& probeSplits,
      const std::vector<Split>& buildSplits);

  std::vector<PlanWithSplits> hashJoinPlans(const std::string& tableDir);

  std::vector<PlanWithSplits> aggregatePlans(const std::string& tableDir);

  std::vector<PlanWithSplits> rowNumberPlans(const std::string& tableDir);

  std::vector<PlanWithSplits> orderByPlans(const std::string& tableDir);

  // Helper method that combines all above plan methods into one.
  std::vector<PlanWithSplits> allPlans(const std::string& tableDir);

  void verify();

  static VectorFuzzer::Options getFuzzerOptions() {
    VectorFuzzer::Options opts;
    opts.vectorSize = FLAGS_batch_size;
    opts.stringVariableLength = true;
    opts.stringLength = 100;
    opts.nullRatio = FLAGS_null_ratio;
    return opts;
  }

  FuzzerGenerator rng_;
  size_t currentSeed_{0};
  std::unordered_map<std::string, std::string> queryConfigsWithSpill_{
      {core::QueryConfig::kSpillEnabled, "true"},
      {core::QueryConfig::kJoinSpillEnabled, "true"},
      {core::QueryConfig::kSpillStartPartitionBit, "29"},
      {core::QueryConfig::kAggregationSpillEnabled, "true"},
      {core::QueryConfig::kRowNumberSpillEnabled, "true"},
      {core::QueryConfig::kOrderBySpillEnabled, "true"},
  };

  std::shared_ptr<memory::MemoryPool> rootPool_{
      memory::memoryManager()->addRootPool(
          "memoryArbitrationFuzzer",
          memory::kMaxMemory,
          memory::MemoryReclaimer::create())};
  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->testingDefaultRoot().addLeafChild(
          "memoryArbitrationFuzzerLeaf",
          true)};
  std::shared_ptr<memory::MemoryPool> writerPool_{rootPool_->addAggregateChild(
      "joinFuzzerWriter",
      exec::MemoryReclaimer::create())};

  VectorFuzzer vectorFuzzer_;
  std::shared_ptr<folly::Executor> executor_{
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency())};
  folly::Synchronized<Stats> stats_;
};

MemoryArbitrationFuzzer::MemoryArbitrationFuzzer(size_t initialSeed)
    : vectorFuzzer_{getFuzzerOptions(), pool_.get()} {
  if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kPresto)) {
    serializer::presto::PrestoVectorSerde::registerNamedVectorSerde();
  }
  if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kCompactRow)) {
    serializer::CompactRowVectorSerde::registerNamedVectorSerde();
  }
  if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kUnsafeRow)) {
    serializer::spark::UnsafeRowVectorSerde::registerNamedVectorSerde();
  }
  // Make sure not to run out of open file descriptors.
  std::unordered_map<std::string, std::string> hiveConfig = {
      {connector::hive::HiveConfig::kNumCacheFileHandles, "1000"}};
  connector::registerConnectorFactory(
      std::make_shared<connector::hive::HiveConnectorFactory>());
  const auto hiveConnector =
      connector::getConnectorFactory(
          connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(
              kHiveConnectorId,
              std::make_shared<config::ConfigBase>(std::move(hiveConfig)));
  connector::registerConnector(hiveConnector);
  dwrf::registerDwrfReaderFactory();
  dwrf::registerDwrfWriterFactory();

  seed(initialSeed);
}

template <typename T>
bool isDone(size_t i, T startTime) {
  if (FLAGS_duration_sec > 0) {
    const std::chrono::duration<double> elapsed =
        std::chrono::system_clock::now() - startTime;
    return elapsed.count() >= FLAGS_duration_sec;
  }
  return i >= FLAGS_steps;
}

std::vector<TypePtr> MemoryArbitrationFuzzer::generateKeyTypes(
    int32_t numKeys) {
  std::vector<TypePtr> types;
  types.reserve(numKeys);
  for (auto i = 0; i < numKeys; ++i) {
    // Pick random scalar type.
    types.push_back(vectorFuzzer_.randType(0 /*maxDepth*/));
  }
  return types;
}

std::pair<std::vector<std::string>, std::vector<TypePtr>>
MemoryArbitrationFuzzer::generatePartitionKeys() {
  const auto numKeys = randInt(1, 3);
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  for (auto i = 0; i < numKeys; ++i) {
    names.push_back(fmt::format("c{}", i));
    types.push_back(vectorFuzzer_.randType(/*maxDepth=*/1));
  }
  return std::make_pair(names, types);
}

std::vector<RowVectorPtr> MemoryArbitrationFuzzer::generateInput(
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
    types.push_back(vectorFuzzer_.randType(2 /*maxDepth*/));
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

std::vector<RowVectorPtr> MemoryArbitrationFuzzer::generateProbeInput(
    const std::vector<std::string>& keyNames,
    const std::vector<TypePtr>& keyTypes) {
  return generateInput(keyNames, keyTypes);
}

std::vector<RowVectorPtr> MemoryArbitrationFuzzer::generateBuildInput(
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
    types.push_back(vectorFuzzer_.randType(2 /*maxDepth*/));
  }

  const auto rowType = ROW(std::move(names), std::move(types));

  // 1 in 10 times use empty build.
  if (vectorFuzzer_.coinToss(0.1)) {
    return {BaseVector::create<RowVector>(rowType, 0, pool_.get())};
  }

  // To ensure there are some matches, sample with replacement 10% of probe join
  // keys and use these as build keys.
  std::vector<RowVectorPtr> input;
  for (const auto& probe : probeInput) {
    const auto numRows = 1 + probe->size() / 10;
    auto build = BaseVector::create<RowVector>(rowType, numRows, probe->pool());

    // Pick probe side rows to copy.
    std::vector<vector_size_t> rowNumbers(numRows);
    for (auto i = 0; i < numRows; ++i) {
      rowNumbers[i] = randInt(0, probe->size() - 1);
    }

    SelectivityVector rows(numRows);
    for (auto i = 0; i < probeKeys.size(); ++i) {
      build->childAt(i)->resize(numRows);
      build->childAt(i)->copy(probe->childAt(i).get(), rows, rowNumbers.data());
    }

    for (auto i = 0; i < numPayload; ++i) {
      const auto column = i + probeKeys.size();
      build->childAt(column) =
          vectorFuzzer_.fuzz(rowType->childAt(column), numRows);
    }

    input.push_back(build);
  }

  return input;
}

std::vector<RowVectorPtr> MemoryArbitrationFuzzer::generateAggregateInput(
    const std::vector<std::string>& keyNames,
    const std::vector<TypePtr>& keyTypes) {
  return generateInput(keyNames, keyTypes);
}

std::vector<RowVectorPtr> MemoryArbitrationFuzzer::generateRowNumberInput(
    const std::vector<std::string>& keyNames,
    const std::vector<TypePtr>& keyTypes) {
  return generateInput(keyNames, keyTypes);
}

std::vector<RowVectorPtr> MemoryArbitrationFuzzer::generateOrderByInput(
    const std::vector<std::string>& keyNames,
    const std::vector<TypePtr>& keyTypes) {
  return generateInput(keyNames, keyTypes);
}

std::vector<MemoryArbitrationFuzzer::PlanWithSplits>
MemoryArbitrationFuzzer::hashJoinPlans(
    const core::JoinType& joinType,
    const std::vector<std::string>& probeKeys,
    const std::vector<std::string>& buildKeys,
    const std::vector<RowVectorPtr>& probeInput,
    const std::vector<RowVectorPtr>& buildInput,
    const std::vector<Split>& probeSplits,
    const std::vector<Split>& buildSplits) {
  auto outputColumns =
      (core::isLeftSemiProjectJoin(joinType) ||
       core::isLeftSemiFilterJoin(joinType) || core::isAntiJoin(joinType))
      ? asRowType(probeInput[0]->type())->names()
      : concat(
            asRowType(probeInput[0]->type()), asRowType(buildInput[0]->type()))
            ->names();

  if (core::isLeftSemiProjectJoin(joinType) ||
      core::isRightSemiProjectJoin(joinType)) {
    outputColumns.emplace_back("match");
  }

  std::vector<PlanWithSplits> plans;
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
              false)
          .planNode();
  plans.push_back(PlanWithSplits{std::move(plan), {}});

  if (!isTableScanSupported(probeInput[0]->type()) ||
      !isTableScanSupported(buildInput[0]->type())) {
    return plans;
  }

  planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  const auto probeType = asRowType(probeInput[0]->type());
  const auto buildType = asRowType(buildInput[0]->type());
  core::PlanNodeId probeScanId;
  core::PlanNodeId buildScanId;
  plan = PlanBuilder(planNodeIdGenerator)
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
                 false)
             .planNode();
  plans.push_back(PlanWithSplits{
      std::move(plan),
      {{probeScanId, probeSplits}, {buildScanId, buildSplits}}});
  return plans;
}

std::vector<MemoryArbitrationFuzzer::PlanWithSplits>
MemoryArbitrationFuzzer::hashJoinPlans(const std::string& tableDir) {
  static const std::vector<core::JoinType> kJoinTypes = {
      core::JoinType::kInner,
      core::JoinType::kLeft,
      core::JoinType::kFull,
      core::JoinType::kLeftSemiFilter,
      core::JoinType::kLeftSemiProject,
      core::JoinType::kAnti};

  const auto numKeys = randInt(1, 5);
  const std::vector<TypePtr> keyTypes = generateKeyTypes(numKeys);
  std::vector<std::string> probeKeys = makeNames("t", keyTypes.size());
  std::vector<std::string> buildKeys = makeNames("u", keyTypes.size());
  const auto probeInput = generateProbeInput(probeKeys, keyTypes);
  const auto buildInput = generateBuildInput(probeInput, probeKeys, buildKeys);
  const std::vector<Split> probeScanSplits =
      makeSplits(probeInput, fmt::format("{}/probe", tableDir), writerPool_);
  const std::vector<Split> buildScanSplits =
      makeSplits(buildInput, fmt::format("{}/build", tableDir), writerPool_);

  std::vector<PlanWithSplits> totalPlans;
  for (const auto& joinType : kJoinTypes) {
    auto plans = hashJoinPlans(
        joinType,
        probeKeys,
        buildKeys,
        probeInput,
        buildInput,
        probeScanSplits,
        buildScanSplits);
    totalPlans.insert(
        totalPlans.end(),
        std::make_move_iterator(plans.begin()),
        std::make_move_iterator(plans.end()));
  }
  return totalPlans;
}

std::vector<MemoryArbitrationFuzzer::PlanWithSplits>
MemoryArbitrationFuzzer::aggregatePlans(const std::string& tableDir) {
  const auto numKeys = randInt(1, 5);
  // Reuse the hash join utilities to generate aggregation keys and inputs.
  const std::vector<TypePtr> keyTypes = generateKeyTypes(numKeys);
  const std::vector<std::string> groupingKeys = makeNames("g", keyTypes.size());
  const auto aggregateInput = generateAggregateInput(groupingKeys, keyTypes);
  const std::vector<std::string> aggregates{"count(1)"};
  const std::vector<Split> splits = makeSplits(
      aggregateInput, fmt::format("{}/aggregate", tableDir), writerPool_);

  std::vector<PlanWithSplits> plans;
  const auto inputRowType = asRowType(aggregateInput[0]->type());
  {
    // Single final aggregation plan.
    const auto planNodeIdGenerator =
        std::make_shared<core::PlanNodeIdGenerator>();
    core::PlanNodeId scanId;
    auto plan = PlanWithSplits{
        PlanBuilder(planNodeIdGenerator)
            .tableScan(inputRowType)
            .capturePlanNodeId(scanId)
            .singleAggregation(groupingKeys, aggregates, {})
            .planNode(),
        {{scanId, splits}}};
    plans.push_back(std::move(plan));

    plan = PlanWithSplits{
        PlanBuilder()
            .values(aggregateInput)
            .singleAggregation(groupingKeys, aggregates, {})
            .planNode(),
        {}};
    plans.push_back(std::move(plan));
  }

  {
    // Partial -> final aggregation plan.
    const auto planNodeIdGenerator =
        std::make_shared<core::PlanNodeIdGenerator>();
    core::PlanNodeId scanId;
    auto plan = PlanWithSplits{
        PlanBuilder(planNodeIdGenerator)
            .tableScan(inputRowType)
            .capturePlanNodeId(scanId)
            .partialAggregation(groupingKeys, aggregates, {})
            .finalAggregation()
            .planNode(),
        {{scanId, splits}}};
    plans.push_back(std::move(plan));

    plan = PlanWithSplits{
        PlanBuilder()
            .values(aggregateInput)
            .partialAggregation(groupingKeys, aggregates, {})
            .finalAggregation()
            .planNode(),
        {}};
    plans.push_back(std::move(plan));
  }

  {
    // Partial -> intermediate -> final aggregation plan.
    const auto planNodeIdGenerator =
        std::make_shared<core::PlanNodeIdGenerator>();
    core::PlanNodeId scanId;
    auto plan = PlanWithSplits{
        PlanBuilder(planNodeIdGenerator)
            .tableScan(inputRowType)
            .capturePlanNodeId(scanId)
            .partialAggregation(groupingKeys, aggregates, {})
            .intermediateAggregation()
            .finalAggregation()
            .planNode(),
        {{scanId, splits}}};
    plans.push_back(std::move(plan));

    plan = PlanWithSplits{
        PlanBuilder()
            .values(aggregateInput)
            .partialAggregation(groupingKeys, aggregates, {})
            .intermediateAggregation()
            .finalAggregation()
            .planNode(),
        {}};
    plans.push_back(std::move(plan));
  }

  return plans;
}

std::vector<MemoryArbitrationFuzzer::PlanWithSplits>
MemoryArbitrationFuzzer::rowNumberPlans(const std::string& tableDir) {
  const auto [keyNames, keyTypes] = generatePartitionKeys();
  const auto input = generateRowNumberInput(keyNames, keyTypes);

  std::vector<PlanWithSplits> plans;

  std::vector<std::string> projectFields = keyNames;
  projectFields.emplace_back("row_number");
  auto plan = PlanWithSplits{
      PlanBuilder()
          .values(input)
          .rowNumber(keyNames)
          .project(projectFields)
          .planNode(),
      {}};
  plans.push_back(std::move(plan));

  if (!isTableScanSupported(input[0]->type())) {
    return plans;
  }

  const std::vector<Split> splits =
      makeSplits(input, fmt::format("{}/row_number", tableDir), writerPool_);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId scanId;
  plan = PlanWithSplits{
      PlanBuilder(planNodeIdGenerator)
          .tableScan(asRowType(input[0]->type()))
          .capturePlanNodeId(scanId)
          .rowNumber(keyNames)
          .project(projectFields)
          .planNode(),
      {{scanId, splits}}};
  plans.push_back(std::move(plan));

  return plans;
}

std::vector<MemoryArbitrationFuzzer::PlanWithSplits>
MemoryArbitrationFuzzer::orderByPlans(const std::string& tableDir) {
  const auto [keyNames, keyTypes] = generatePartitionKeys();
  const auto input = generateOrderByInput(keyNames, keyTypes);

  std::vector<PlanWithSplits> plans;

  auto plan = PlanWithSplits{
      PlanBuilder().values(input).orderBy(keyNames, false).planNode(), {}};
  plans.push_back(std::move(plan));

  if (!isTableScanSupported(input[0]->type())) {
    return plans;
  }

  const std::vector<Split> splits =
      makeSplits(input, fmt::format("{}/order_by", tableDir), writerPool_);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId scanId;
  plan = PlanWithSplits{
      PlanBuilder(std::move(planNodeIdGenerator))
          .tableScan(asRowType(input[0]->type()))
          .capturePlanNodeId(scanId)
          .orderBy(keyNames, false)
          .planNode(),
      {{scanId, splits}}};
  plans.push_back(std::move(plan));

  return plans;
}

std::vector<MemoryArbitrationFuzzer::PlanWithSplits>
MemoryArbitrationFuzzer::allPlans(const std::string& tableDir) {
  std::vector<PlanWithSplits> plans;
  for (const auto& plan : hashJoinPlans(tableDir)) {
    plans.push_back(plan);
  }
  for (const auto& plan : aggregatePlans(tableDir)) {
    plans.push_back(plan);
  }
  for (const auto& plan : rowNumberPlans(tableDir)) {
    plans.push_back(plan);
  }
  for (const auto& plan : orderByPlans(tableDir)) {
    plans.push_back(plan);
  }
  return plans;
}

struct ThreadLocalStats {
  uint64_t spillFsFaultCount{0};
};

// Stats that keeps track of per thread execution status in verify()
thread_local ThreadLocalStats threadLocalStats;

std::shared_ptr<TempDirectoryPath>
MemoryArbitrationFuzzer::maybeGenerateFaultySpillDirectory() {
  FuzzerGenerator fsRng(rng_());
  const auto injectFsFault =
      coinToss(fsRng, FLAGS_spill_fs_fault_injection_ratio);
  if (!injectFsFault) {
    return exec::test::TempDirectoryPath::create(false);
  }
  using OpType = FaultFileOperation::Type;
  static const std::vector<std::unordered_set<OpType>> opTypes{
      {OpType::kRead},
      {OpType::kReadv},
      {OpType::kWrite},
      {OpType::kRead, OpType::kReadv},
      {OpType::kRead, OpType::kWrite},
      {OpType::kReadv, OpType::kWrite}};

  const auto directory = exec::test::TempDirectoryPath::create(true);
  auto faultyFileSystem = std::dynamic_pointer_cast<FaultyFileSystem>(
      filesystems::getFileSystem(directory->getPath(), nullptr));
  faultyFileSystem->setFileInjectionHook(
      [this, injectTypes = opTypes[getRandomIndex(fsRng, opTypes.size() - 1)]](
          FaultFileOperation* op) {
        if (injectTypes.count(op->type) == 0) {
          return;
        }
        FuzzerGenerator fsRng(rng_());
        if (coinToss(fsRng, FLAGS_spill_fs_fault_injection_ratio)) {
          ++threadLocalStats.spillFsFaultCount;
          VELOX_FAIL(
              "Fault file injection on {}",
              FaultFileOperation::typeString(op->type));
        }
      });
  return directory;
}

void MemoryArbitrationFuzzer::verify() {
  auto spillDirectory = maybeGenerateFaultySpillDirectory();
  const auto tableScanDir = exec::test::TempDirectoryPath::create(false);

  auto plans = allPlans(tableScanDir->getPath());

  SCOPE_EXIT {
    waitForAllTasksToBeDeleted();
    if (auto faultyFileSystem = std::dynamic_pointer_cast<FaultyFileSystem>(
            filesystems::getFileSystem(spillDirectory->getPath(), nullptr))) {
      faultyFileSystem->clearFileFaultInjections();
    }
  };

  const auto numThreads = FLAGS_num_threads;
  std::atomic_bool stop{false};
  std::vector<std::thread> queryThreads;
  queryThreads.reserve(numThreads);
  // A map to keep track of the query task abort request. The key is the query
  // id and the value indicates if an abort request is injected.
  folly::ConcurrentHashMap<std::string, bool> queryTaskAbortRequestMap;
  std::atomic_int32_t queryCount{0};
  for (int i = 0; i < numThreads; ++i) {
    auto seed = rng_();
    queryThreads.emplace_back([&, spillDirectory, i, seed]() {
      FuzzerGenerator rng(seed);
      while (!stop) {
        const auto prevSpillFsFaultCount = threadLocalStats.spillFsFaultCount;
        const auto queryId = fmt::format("query_id_{}", queryCount++);
        queryTaskAbortRequestMap.insert(queryId, false);
        try {
          const auto queryCtx = newQueryCtx(
              memory::memoryManager(),
              executor_.get(),
              FLAGS_arbitrator_capacity,
              queryId);

          const auto plan = plans.at(getRandomIndex(rng, plans.size() - 1));
          AssertQueryBuilder builder(plan.plan);
          builder.queryCtx(queryCtx);
          for (const auto& [planNodeId, nodeSplits] : plan.splits) {
            builder.splits(planNodeId, nodeSplits);
          }

          if (coinToss(rng, 0.3)) {
            builder.queryCtx(queryCtx).copyResults(pool_.get());
          } else {
            auto res =
                builder.configs(queryConfigsWithSpill_)
                    .spillDirectory(
                        spillDirectory->getPath() + fmt::format("/{}/", i))
                    .queryCtx(queryCtx)
                    .copyResults(pool_.get());
          }
          ++stats_.wlock()->successCount;
          VELOX_CHECK_EQ(
              threadLocalStats.spillFsFaultCount, prevSpillFsFaultCount);
        } catch (const VeloxException& e) {
          auto lockedStats = stats_.wlock();
          if (e.errorCode() == error_code::kMemCapExceeded.c_str()) {
            ++lockedStats->oomCount;
          } else if (e.errorCode() == error_code::kMemAborted.c_str()) {
            ++lockedStats->abortCount;
          } else if (e.errorCode() == error_code::kInvalidState.c_str()) {
            const auto injectedSpillFsFault =
                threadLocalStats.spillFsFaultCount > prevSpillFsFaultCount;
            const auto injectedTaskAbortRequest =
                queryTaskAbortRequestMap.find(queryId)->second;
            VELOX_CHECK(
                injectedSpillFsFault || injectedTaskAbortRequest,
                "injectedSpillFsFault: {}, injectedTaskAbortRequest: {}, error message: {}",
                injectedSpillFsFault,
                injectedTaskAbortRequest,
                e.message());

            if (injectedTaskAbortRequest && !injectedSpillFsFault) {
              VELOX_CHECK(
                  e.message().find("Aborted for external error") !=
                      std::string::npos,
                  e.message());
            } else if (!injectedTaskAbortRequest && injectedSpillFsFault) {
              VELOX_CHECK(
                  e.message().find("Fault file injection on") !=
                      std::string::npos,
                  e.message());
            } else {
              VELOX_CHECK(
                  e.message().find("Fault file injection on") !=
                          std::string::npos ||
                      e.message().find("Aborted for external error") !=
                          std::string::npos,
                  e.message());
            }
          } else {
            LOG(ERROR) << "Unexpected exception:\n" << e.what();
            std::rethrow_exception(std::current_exception());
          }
        }
        queryTaskAbortRequestMap.erase(queryId);
      }
    });
  }

  // Inject global arbitration from a background thread.
  auto shrinkRng = FuzzerGenerator(rng_());
  std::thread globalShrinkThread([&]() {
    while (!stop) {
      if (getRandomIndex(shrinkRng, 99) < FLAGS_global_arbitration_pct) {
        memory::memoryManager()->shrinkPools();
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });

  // Inject random task abortion from a background thread.
  auto abortRng = FuzzerGenerator(rng_());
  std::thread abortControlThread([&]() {
    if (FLAGS_task_abort_interval_ms == 0) {
      return;
    }
    while (!stop) {
      try {
        std::this_thread::sleep_for(
            std::chrono::milliseconds(FLAGS_task_abort_interval_ms));
        auto tasksList = Task::getRunningTasks();

        // queryThreads start a new Task each time a Task finishes, but we still
        // may get unlucky and hit a point where there are no tasks running.
        if (!tasksList.empty()) {
          vector_size_t index = getRandomIndex(abortRng, tasksList.size() - 1);
          auto& task = tasksList[index];
          const auto queryId = task->queryCtx()->queryId();
          queryTaskAbortRequestMap.assign(queryId, true);
          task->requestAbort();
        }
      } catch (const VeloxException& e) {
        LOG(ERROR) << "Unexpected exception in abortControlScheduler:\n"
                   << e.what();
        std::rethrow_exception(std::current_exception());
      }
    }
  });

  std::this_thread::sleep_for(
      std::chrono::seconds(FLAGS_iteration_duration_sec));
  stop = true;

  for (auto& queryThread : queryThreads) {
    queryThread.join();
  }
  globalShrinkThread.join();
  abortControlThread.join();
}

void MemoryArbitrationFuzzer::go() {
  VELOX_USER_CHECK(
      FLAGS_steps > 0 || FLAGS_duration_sec > 0,
      "Either --steps or --duration_sec needs to be greater than zero.");
  VELOX_USER_CHECK_GE(FLAGS_batch_size, 10, "Batch size must be at least 10.");

  const auto startTime = std::chrono::system_clock::now();
  size_t iteration = 0;

  while (!isDone(iteration, startTime)) {
    LOG(WARNING) << "==============================> Started iteration "
                 << iteration << " (seed: " << currentSeed_ << ")";
    verify();

    LOG(INFO) << "==============================> Done with iteration "
              << iteration;
    stats_.rlock()->print();

    reSeed();
    ++iteration;
  }
}

} // namespace

void memoryArbitrationFuzzer(size_t seed) {
  MemoryArbitrationFuzzer(seed).go();
}
} // namespace facebook::velox::exec::test
