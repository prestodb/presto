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

#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/SharedArbitrator.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/exec/MemoryReclaimer.h"
#include "velox/exec/TableWriter.h"
#include "velox/exec/fuzzer/FuzzerUtil.h"
#include "velox/exec/tests/utils/ArbitratorTestUtil.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/functions/lib/aggregates/AverageAggregateBase.h"
#include "velox/functions/sparksql/aggregates/Register.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

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

namespace facebook::velox::exec::test {
namespace {

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
    size_t failureCount{0};
    size_t oomCount{0};
    size_t abortCount{0};

    void print() const {
      std::stringstream ss;
      ss << "Success count = " << successCount
         << ", failure count = " << failureCount
         << ". OOM count  = " << oomCount << " Abort count = " << abortCount;
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
  };

  std::shared_ptr<memory::MemoryPool> rootPool_{
      memory::memoryManager()->addRootPool(
          "memoryArbitrationFuzzer",
          memory::kMaxMemory,
          memory::MemoryReclaimer::create())};
  std::shared_ptr<memory::MemoryPool> pool_{rootPool_->addLeafChild(
      "memoryArbitrationFuzzerLeaf",
      true,
      exec::MemoryReclaimer::create())};
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
  // Make sure not to run out of open file descriptors.
  const std::unordered_map<std::string, std::string> hiveConfig = {
      {connector::hive::HiveConfig::kNumCacheFileHandles, "1000"}};
  const auto hiveConnector =
      connector::getConnectorFactory(
          connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(
              kHiveConnectorId, std::make_shared<core::MemConfig>(hiveConfig));
  connector::registerConnector(hiveConnector);
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

void MemoryArbitrationFuzzer::verify() {
  const auto outputDirectory = TempDirectoryPath::create();
  const auto spillDirectory = exec::test::TempDirectoryPath::create();
  const auto tableScanDir = exec::test::TempDirectoryPath::create();

  std::vector<PlanWithSplits> plans;
  for (const auto& plan : hashJoinPlans(tableScanDir->getPath())) {
    plans.push_back(plan);
  }
  for (const auto& plan : aggregatePlans(tableScanDir->getPath())) {
    plans.push_back(plan);
  }
  for (const auto& plan : rowNumberPlans(tableScanDir->getPath())) {
    plans.push_back(plan);
  }

  SCOPE_EXIT {
    waitForAllTasksToBeDeleted();
  };

  const auto numThreads = FLAGS_num_threads;
  std::atomic_bool stop{false};
  std::vector<std::thread> queryThreads;
  queryThreads.reserve(numThreads);
  for (int i = 0; i < numThreads; ++i) {
    queryThreads.emplace_back([&, i]() {
      while (!stop) {
        try {
          const auto queryCtx = newQueryCtx(
              memory::memoryManager(),
              executor_.get(),
              FLAGS_arbitrator_capacity);
          const auto plan = plans.at(randInt(0, plans.size() - 1));
          AssertQueryBuilder builder(plan.plan);
          builder.queryCtx(queryCtx);
          for (const auto& [planNodeId, nodeSplits] : plan.splits) {
            builder.splits(planNodeId, nodeSplits);
          }

          if (vectorFuzzer_.coinToss(0.3)) {
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
        } catch (const VeloxException& e) {
          auto lockedStats = stats_.wlock();
          if (e.errorCode() == error_code::kMemCapExceeded.c_str()) {
            ++lockedStats->oomCount;
          } else if (e.errorCode() == error_code::kMemAborted.c_str()) {
            ++lockedStats->abortCount;
          } else {
            ++lockedStats->failureCount;
            std::rethrow_exception(std::current_exception());
          }
        }
      }
    });
  }

  std::this_thread::sleep_for(
      std::chrono::seconds(FLAGS_iteration_duration_sec));
  stop = true;

  for (auto& queryThread : queryThreads) {
    queryThread.join();
  }
}

void MemoryArbitrationFuzzer::go() {
  VELOX_USER_CHECK(
      FLAGS_steps > 0 || FLAGS_duration_sec > 0,
      "Either --steps or --duration_sec needs to be greater than zero.")
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
