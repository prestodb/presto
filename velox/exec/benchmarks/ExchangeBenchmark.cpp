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
#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "velox/core/QueryConfig.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

DEFINE_int32(width, 16, "Number of parties in shuffle");
DEFINE_int32(task_width, 4, "Number of threads in each task in shuffle");

DEFINE_int32(num_local_tasks, 8, "Number of concurrent local shuffles");
DEFINE_int32(num_local_repeat, 8, "Number of repeats of local exchange query");
DEFINE_int32(flat_batch_mb, 1, "MB in a 10k row flat batch.");
DEFINE_int64(
    local_exchange_buffer_mb,
    32,
    "task-wide buffer in local exchange");
DEFINE_int64(exchange_buffer_mb, 32, "task-wide buffer in remote exchange");
DEFINE_int32(dict_pct, 0, "Percentage of columns wrapped in dictionary");
// Add the following definitions to allow Clion runs
DEFINE_bool(gtest_color, false, "");
DEFINE_string(gtest_filter, "*", "");

/// Benchmarks repartition/exchange with different batch sizes,
/// numbers of destinations and data type mixes.  Generates a plan
/// that 1. shuffles a constant input in each of n workers, sending
/// each partition to n consumers in the next stage. The consumers
/// count the rows and send the count to a final single task stage
/// that returns the sum of the counts. The sum is expected to be n *
/// number of rows in constant input.

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;

namespace {

struct LocalPartitionWaitStats {
  int64_t totalProducerWaitMs = 0;
  int64_t totalConsumerWaitMs = 0;
  std::vector<RuntimeMetric> consumerWaitMs;
  std::vector<RuntimeMetric> producerWaitMs;
  std::vector<int64_t> wallMs;
};

void sortByMax(std::vector<RuntimeMetric>& metrics) {
  std::sort(
      metrics.begin(),
      metrics.end(),
      [](const RuntimeMetric& left, const RuntimeMetric& right) {
        return left.max > right.max;
      });
}

void sortByAndPrintMax(
    const char* title,
    int64_t total,
    std::vector<RuntimeMetric>& metrics) {
  sortByMax(metrics);
  VELOX_CHECK(!metrics.empty());
  std::cout << title << "\n Total " << succinctNanos(total)
            << "\n Max: " << metrics.front().toString()
            << "\n Median: " << metrics[metrics.size() / 2].toString()
            << "\n Min: " << metrics.back().toString() << std::endl;
}

class ExchangeBenchmark : public VectorTestBase {
 public:
  std::vector<RowVectorPtr> makeRows(
      RowTypePtr type,
      int32_t numVectors,
      int32_t rowsPerVector,
      int32_t dictPct = 0) {
    std::vector<RowVectorPtr> vectors;
    BufferPtr indices;
    for (int32_t i = 0; i < numVectors; ++i) {
      auto vector = std::dynamic_pointer_cast<RowVector>(
          BatchMaker::createBatch(type, rowsPerVector, *pool_));

      auto width = vector->childrenSize();
      for (auto child = 0; child < width; ++child) {
        if (100 * child / width > dictPct) {
          if (!indices) {
            indices = makeIndices(vector->size(), [&](auto i) { return i; });
          }
          vector->childAt(child) = BaseVector::wrapInDictionary(
              nullptr, indices, vector->size(), vector->childAt(child));
        }
      }
      vectors.push_back(vector);
    }
    return vectors;
  }

  void run(
      std::vector<RowVectorPtr>& vectors,
      int32_t width,
      int32_t taskWidth,
      int64_t& wallUs,
      PlanNodeStats& partitionedOutputStats,
      PlanNodeStats& exchangeStats) {
    core::PlanNodePtr plan;
    core::PlanNodeId exchangeId;
    core::PlanNodeId leafPartitionedOutputId;
    core::PlanNodeId finalAggPartitionedOutputId;

    std::vector<std::shared_ptr<Task>> leafTasks;
    std::vector<std::shared_ptr<Task>> finalAggTasks;
    std::vector<exec::Split> finalAggSplits;

    RowVectorPtr expected;

    const auto startUs = getCurrentTimeMicro();
    BENCHMARK_SUSPEND {
      assert(!vectors.empty());
      configSettings_[core::QueryConfig::kMaxPartitionedOutputBufferSize] =
          fmt::format("{}", FLAGS_exchange_buffer_mb << 20);
      const auto iteration = ++iteration_;

      // leafPlan: PartitionedOutput/kPartitioned(1) <-- Values(0)
      std::vector<std::string> leafTaskIds;
      auto leafPlan = exec::test::PlanBuilder()
                          .values(vectors, true)
                          .partitionedOutput({"c0"}, width)
                          .capturePlanNodeId(leafPartitionedOutputId)
                          .planNode();

      for (int32_t counter = 0; counter < width; ++counter) {
        auto leafTaskId = makeTaskId(iteration, "leaf", counter);
        leafTaskIds.push_back(leafTaskId);
        auto leafTask = makeTask(leafTaskId, leafPlan, counter);
        leafTasks.push_back(leafTask);
        leafTask->start(taskWidth);
      }

      // finalAggPlan: PartitionedOutput/kPartitioned(2) <-- Agg/kSingle(1) <--
      // Exchange(0)
      std::vector<std::string> finalAggTaskIds;
      core::PlanNodePtr finalAggPlan =
          exec::test::PlanBuilder()
              .exchange(leafPlan->outputType(), VectorSerde::Kind::kPresto)
              .capturePlanNodeId(exchangeId)
              .singleAggregation({}, {"count(1)"})
              .partitionedOutput({}, 1)
              .capturePlanNodeId(finalAggPartitionedOutputId)
              .planNode();

      for (int i = 0; i < width; i++) {
        auto taskId = makeTaskId(iteration, "final-agg", i);
        finalAggSplits.push_back(
            exec::Split(std::make_shared<exec::RemoteConnectorSplit>(taskId)));
        auto finalAggTask = makeTask(taskId, finalAggPlan, i);
        finalAggTasks.push_back(finalAggTask);
        finalAggTask->start(taskWidth);
        addRemoteSplits(finalAggTask, leafTaskIds);
      }

      expected = makeRowVector({makeFlatVector<int64_t>(1, [&](auto /*row*/) {
        return vectors.size() * vectors[0]->size() * width * taskWidth;
      })});

      // plan: Agg/kSingle(1) <-- Exchange (0)
      plan =
          exec::test::PlanBuilder()
              .exchange(finalAggPlan->outputType(), VectorSerde::Kind::kPresto)
              .singleAggregation({}, {"sum(a0)"})
              .planNode();
    };

    exec::test::AssertQueryBuilder(plan)
        .splits(finalAggSplits)
        .assertResults(expected);

    BENCHMARK_SUSPEND {
      wallUs = getCurrentTimeMicro() - startUs;
      std::vector<int64_t> taskWallMs;

      for (const auto& task : leafTasks) {
        const auto& taskStats = task->taskStats();
        taskWallMs.push_back(
            taskStats.executionEndTimeMs - taskStats.executionStartTimeMs);
        const auto& planStats = toPlanStats(taskStats);
        auto& taskPartitionedOutputStats =
            planStats.at(leafPartitionedOutputId);
        partitionedOutputStats += taskPartitionedOutputStats;
      }

      for (const auto& task : finalAggTasks) {
        const auto& taskStats = task->taskStats();
        taskWallMs.push_back(
            taskStats.executionEndTimeMs - taskStats.executionStartTimeMs);
        const auto& planStats = toPlanStats(taskStats);

        auto& taskPartitionedOutputStats =
            planStats.at(finalAggPartitionedOutputId);
        partitionedOutputStats += taskPartitionedOutputStats;

        auto& taskExchangeStats = planStats.at(exchangeId);
        exchangeStats += taskExchangeStats;
      }
    };
  }

  void runLocal(
      std::vector<RowVectorPtr>& vectors,
      int32_t taskWidth,
      int32_t numTasks,
      int64_t& localPartitionWallUs,
      PlanNodeStats& partitionedOutputStats,
      LocalPartitionWaitStats& localPartitionWaitStats) {
    assert(!vectors.empty());

    core::PlanNodePtr plan;
    core::PlanNodeId localPartitionId1;
    core::PlanNodeId localPartitionId2;
    std::vector<std::shared_ptr<Task>> tasks;
    std::vector<std::thread> threads;

    RowVectorPtr expected;

    BENCHMARK_SUSPEND {
      std::vector<std::string> aggregates = {"count(1)"};
      auto& rowType = vectors[0]->type()->as<TypeKind::ROW>();
      for (auto i = 1; i < rowType.size(); ++i) {
        aggregates.push_back(fmt::format("checksum({})", rowType.nameOf(i)));
      }

      // plan: Agg/kSingle(4) <-- LocalPartition/Gather(3) <-- Agg/kGather(2)
      // <-- LocalPartition/kRepartition(1) <-- Values(0)
      plan = exec::test::PlanBuilder()
                 .values(vectors, true)
                 .localPartition({"c0"})
                 .capturePlanNodeId(localPartitionId1)
                 .singleAggregation({}, aggregates)
                 .localPartition(std::vector<std::string>{})
                 .capturePlanNodeId(localPartitionId2)
                 .singleAggregation({}, {"sum(a0)"})
                 .planNode();

      threads.reserve(numTasks);
      expected = makeRowVector({makeFlatVector<int64_t>(1, [&](auto /*row*/) {
        return vectors.size() * vectors[0]->size() * taskWidth;
      })});
    };

    auto startMicros = getCurrentTimeMicro();
    std::mutex mutex;
    for (int32_t i = 0; i < numTasks; ++i) {
      threads.push_back(std::thread([&]() {
        for (auto repeat = 0; repeat < FLAGS_num_local_repeat; ++repeat) {
          auto task =
              exec::test::AssertQueryBuilder(plan)
                  .config(
                      core::QueryConfig::kMaxLocalExchangeBufferSize,
                      fmt::format("{}", FLAGS_local_exchange_buffer_mb << 20))
                  .maxDrivers(taskWidth)
                  .assertResults(expected);
          {
            std::lock_guard<std::mutex> l(mutex);
            tasks.push_back(task);
          }
        }
      }));
    }
    for (auto& thread : threads) {
      thread.join();
    }

    BENCHMARK_SUSPEND {
      localPartitionWallUs = getCurrentTimeMicro() - startMicros;

      std::vector<core::PlanNodeId> localPartitionNodeIds{
          localPartitionId1, localPartitionId2};

      localPartitionWaitStats.totalProducerWaitMs = 0;
      localPartitionWaitStats.totalConsumerWaitMs = 0;
      for (const auto& task : tasks) {
        auto taskStats = task->taskStats();
        localPartitionWaitStats.wallMs.push_back(
            taskStats.executionEndTimeMs - taskStats.executionStartTimeMs);
        auto planStats = toPlanStats(taskStats);

        for (const auto& nodeId : localPartitionNodeIds) {
          auto& taskLocalPartition1Stats = planStats.at(nodeId);
          partitionedOutputStats += taskLocalPartition1Stats;

          auto& taskLocalPartition1RuntimeStats =
              taskLocalPartition1Stats.customStats;
          localPartitionWaitStats.producerWaitMs.push_back(
              taskLocalPartition1RuntimeStats
                  ["blockedWaitForProducerWallNanos"]);
          localPartitionWaitStats.consumerWaitMs.push_back(
              taskLocalPartition1RuntimeStats
                  ["blockedWaitForConsumerWallNanos"]);
          localPartitionWaitStats.totalProducerWaitMs +=
              localPartitionWaitStats.producerWaitMs.back().sum;
          localPartitionWaitStats.totalConsumerWaitMs +=
              localPartitionWaitStats.consumerWaitMs.back().sum;
        }
      }
    };
  }

 private:
  static constexpr int64_t kMaxMemory = 6UL << 30; // 6GB

  static std::string
  makeTaskId(int32_t iteration, const std::string& prefix, int num) {
    return fmt::format("local://{}-{}-{}", iteration, prefix, num);
  }

  std::shared_ptr<Task> makeTask(
      const std::string& taskId,
      std::shared_ptr<const core::PlanNode> planNode,
      int destination,
      Consumer consumer = nullptr,
      int64_t maxMemory = kMaxMemory) {
    auto configCopy = configSettings_;
    auto queryCtx = core::QueryCtx::create(
        executor_.get(), core::QueryConfig(std::move(configCopy)));
    queryCtx->testingOverrideMemoryPool(
        memory::memoryManager()->addRootPool(queryCtx->queryId(), maxMemory));
    core::PlanFragment planFragment{planNode};
    return Task::create(
        taskId,
        std::move(planFragment),
        destination,
        std::move(queryCtx),
        Task::ExecutionMode::kParallel,
        std::move(consumer));
  }

  void addRemoteSplits(
      std::shared_ptr<Task> task,
      const std::vector<std::string>& remoteTaskIds) {
    for (const auto& taskId : remoteTaskIds) {
      auto split =
          exec::Split(std::make_shared<RemoteConnectorSplit>(taskId), -1);
      task->addSplit("0", std::move(split));
    }
    task->noMoreSplits("0");
  }

  std::unordered_map<std::string, std::string> configSettings_;
  // Serial number to differentiate consecutive benchmark repeats.
  static int32_t iteration_;
};

int32_t ExchangeBenchmark::iteration_;

std::unique_ptr<ExchangeBenchmark> bm;

void runBenchmarks() {
  std::vector<std::string> flatNames = {"c0"};
  std::vector<TypePtr> flatTypes = {BIGINT()};
  std::vector<TypePtr> typeSelection = {
      BOOLEAN(),
      TINYINT(),
      DECIMAL(20, 3),
      INTEGER(),
      BIGINT(),
      REAL(),
      DECIMAL(10, 2),
      DOUBLE(),
      VARCHAR()};

  int64_t flatSize = 0;
  // Add enough columns of different types to make a 10K row batch be
  // flat_batch_mb in flat size.
  while (flatSize * 10000 < static_cast<int64_t>(FLAGS_flat_batch_mb) << 20) {
    flatNames.push_back(fmt::format("c{}", flatNames.size()));
    assert(!flatNames.empty());
    flatTypes.push_back(typeSelection[flatTypes.size() % typeSelection.size()]);
    if (flatTypes.back()->isFixedWidth()) {
      flatSize += flatTypes.back()->cppSizeInBytes();
    } else {
      flatSize += 20;
    }
  }
  auto flatType = ROW(std::move(flatNames), std::move(flatTypes));

  auto structType = ROW(
      {{"c0", BIGINT()},
       {"r1",
        ROW(
            {{"k2", BIGINT()},
             {"r2",
              ROW(
                  {{"i1", BIGINT()},
                   {"i2", BIGINT()},
                   {"r3}, ROW({{s3", VARCHAR()},
                   {"i5", INTEGER()},
                   {"d5", DOUBLE()},
                   {"b5", BOOLEAN()},
                   {"a5", ARRAY(TINYINT())}})}})}});

  auto deepType = ROW(
      {{"c0", BIGINT()},
       {"long_array_val", ARRAY(ARRAY(BIGINT()))},
       {"array_val", ARRAY(VARCHAR())},
       {"struct_val", ROW({{"s_int", INTEGER()}, {"s_array", ARRAY(REAL())}})},
       {"map_val",
        MAP(VARCHAR(),
            MAP(BIGINT(),
                ROW({{"s2_int", INTEGER()}, {"s2_string", VARCHAR()}})))}});

  std::vector<RowVectorPtr> flat10k(
      bm->makeRows(flatType, 10, 10000, FLAGS_dict_pct));
  std::vector<RowVectorPtr> deep10k(
      bm->makeRows(deepType, 10, 10000, FLAGS_dict_pct));
  std::vector<RowVectorPtr> flat50(
      bm->makeRows(flatType, 2000, 50, FLAGS_dict_pct));
  std::vector<RowVectorPtr> deep50(
      bm->makeRows(deepType, 2000, 50, FLAGS_dict_pct));
  std::vector<RowVectorPtr> struct1k(
      bm->makeRows(structType, 100, 1000, FLAGS_dict_pct));

  int64_t flat10KWallUs;
  PlanNodeStats partitionedOutputStatsFlat10K;
  PlanNodeStats exchangeStatsFlat10K;
  folly::addBenchmark(__FILE__, "exchangeFlat10k", [&]() {
    bm->run(
        flat10k,
        FLAGS_width,
        FLAGS_task_width,
        flat10KWallUs,
        partitionedOutputStatsFlat10K,
        exchangeStatsFlat10K);
    return 1;
  });

  int64_t flat50KWallUs;
  PlanNodeStats partitionedOutputStatsFlat50;
  PlanNodeStats exchangeStatsFlat50;
  folly::addBenchmark(__FILE__, "exchangeFlat50", [&]() {
    bm->run(
        flat50,
        FLAGS_width,
        FLAGS_task_width,
        flat50KWallUs,
        partitionedOutputStatsFlat50,
        exchangeStatsFlat50);
    return 1;
  });

  int64_t deep10KWallUs;
  PlanNodeStats partitionedOutputStatsDeep10K;
  PlanNodeStats exchangeStatsDeep10K;
  folly::addBenchmark(__FILE__, "exchangeDeep10k", [&]() {
    bm->run(
        deep10k,
        FLAGS_width,
        FLAGS_task_width,
        deep10KWallUs,
        partitionedOutputStatsDeep10K,
        exchangeStatsDeep10K);
    return 1;
  });

  int64_t deep50KWallUs;
  PlanNodeStats partitionedOutputStatsDeep50;
  PlanNodeStats exchangeStatsDeep50;
  folly::addBenchmark(__FILE__, "exchangeDeep50", [&]() {
    bm->run(
        deep50,
        FLAGS_width,
        FLAGS_task_width,
        deep50KWallUs,
        partitionedOutputStatsDeep50,
        exchangeStatsDeep50);
    return 1;
  });

  int64_t stuct1KWallUs;
  PlanNodeStats partitionedOutputStatsStruct1K;
  PlanNodeStats exchangeStatsStruct1K;
  folly::addBenchmark(__FILE__, "exchangeStruct1K", [&]() {
    bm->run(
        struct1k,
        FLAGS_width,
        FLAGS_task_width,
        stuct1KWallUs,
        partitionedOutputStatsStruct1K,
        exchangeStatsStruct1K);
    return 1;
  });

  int64_t localPartitionWallUs;
  PlanNodeStats localPartitionStatsFlat10K;
  LocalPartitionWaitStats localPartitionWaitStats;
  folly::addBenchmark(__FILE__, "localFlat10k", [&]() {
    bm->runLocal(
        flat10k,
        FLAGS_width,
        FLAGS_num_local_tasks,
        localPartitionWallUs,
        localPartitionStatsFlat10K,
        localPartitionWaitStats);
    return 1;
  });

  folly::runBenchmarks();

  std::cout
      << "----------------------------------Flat10K----------------------------------"
      << std::endl;
  std::cout << "Wall Time (ms): " << succinctMicros(flat10KWallUs) << std::endl;
  std::cout << "PartitionOutput: " << partitionedOutputStatsFlat10K.toString()
            << std::endl;
  std::cout << "Exchange: " << exchangeStatsFlat10K.toString() << std::endl;

  std::cout
      << "----------------------------------Flat50K----------------------------------"
      << std::endl;
  std::cout << "Wall Time (ms): " << succinctMicros(flat50KWallUs) << std::endl;
  std::cout << "PartitionOutput: " << partitionedOutputStatsFlat50.toString()
            << std::endl;
  std::cout << "Exchange: " << exchangeStatsFlat10K.toString() << std::endl;

  std::cout
      << "----------------------------------Deep10K----------------------------------"
      << std::endl;
  std::cout << "Wall Time (ms): " << succinctMicros(deep10KWallUs) << std::endl;
  std::cout << "PartitionOutput: " << partitionedOutputStatsDeep10K.toString()
            << std::endl;
  std::cout << "Exchange: " << exchangeStatsDeep10K.toString() << std::endl;

  std::cout
      << "----------------------------------Deep50K----------------------------------"
      << std::endl;
  std::cout << "Wall Time (ms): " << succinctMicros(deep50KWallUs) << std::endl;
  std::cout << "PartitionOutput: " << partitionedOutputStatsDeep50.toString()
            << std::endl;
  std::cout << "Exchange: " << exchangeStatsDeep50.toString() << std::endl;

  std::cout
      << "----------------------------------Struct1K---------------------------------"
      << std::endl;
  std::cout << "Wall Time (ms): " << succinctMicros(stuct1KWallUs) << std::endl;
  std::cout << "PartitionOutput: " << partitionedOutputStatsStruct1K.toString()
            << std::endl;
  std::cout << "Exchange: " << exchangeStatsStruct1K.toString() << std::endl;

  std::cout
      << "--------------------------------LocalFlat10K-------------------------------"
      << std::endl;
  std::cout << "Wall Time (ms): " << "\n Total: "
            << succinctMicros(localPartitionWallUs)
            << "\n Max: " << localPartitionWaitStats.wallMs.back()
            << "\n Median: "
            << localPartitionWaitStats
                   .wallMs[localPartitionWaitStats.wallMs.size() / 2]
            << "\n Min: " << localPartitionWaitStats.wallMs.front()
            << std::endl;
  std::cout << "LocalPartition: " << localPartitionStatsFlat10K.toString()
            << std::endl;
  sortByAndPrintMax(
      "Producer Wait Time (ms)",
      localPartitionWaitStats.totalProducerWaitMs,
      localPartitionWaitStats.producerWaitMs);
  sortByAndPrintMax(
      "Consumer Wait Time (ms)",
      localPartitionWaitStats.totalConsumerWaitMs,
      localPartitionWaitStats.consumerWaitMs);
  std::sort(
      localPartitionWaitStats.wallMs.begin(),
      localPartitionWaitStats.wallMs.end());
  assert(!localPartitionWaitStats.wallMs.empty());
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  memory::MemoryManager::initialize({});
  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();
  parse::registerTypeResolver();
  if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kPresto)) {
    serializer::presto::PrestoVectorSerde::registerNamedVectorSerde();
  }
  exec::ExchangeSource::registerFactory(exec::test::createLocalExchangeSource);

  bm = std::make_unique<ExchangeBenchmark>();
  runBenchmarks();
  bm.reset();

  return 0;
}
