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

#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

DEFINE_int32(width, 16, "Number of parties in shuffle");

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

struct Counters {
  int64_t bytes{0};
  int64_t rows{0};
  int64_t usec{0};

  std::string toString() {
    return fmt::format("{} MB/s", (bytes / (1024 * 1024.0)) / (usec / 1.0e6));
  }
};

class ExchangeBenchmark : public VectorTestBase {
 public:
  std::vector<RowVectorPtr>
  makeRows(RowTypePtr type, int32_t numVectors, int32_t rowsPerVector) {
    std::vector<RowVectorPtr> vectors;
    for (int32_t i = 0; i < numVectors; ++i) {
      auto vector = std::dynamic_pointer_cast<RowVector>(
          BatchMaker::createBatch(type, rowsPerVector, *pool_));
      vectors.push_back(vector);
    }
    return vectors;
  }

  void
  run(std::vector<RowVectorPtr>& vectors, int32_t width, Counters& counters) {
    assert(!vectors.empty());
    std::vector<std::shared_ptr<Task>> tasks;
    std::vector<std::string> leafTaskIds;
    auto leafPlan = exec::test::PlanBuilder()
                        .values(vectors)
                        .partitionedOutput({"c0"}, width)
                        .planNode();

    auto startMicros = getCurrentTimeMicro();
    for (int32_t counter = 0; counter < width; ++counter) {
      auto leafTaskId = makeTaskId("leaf", counter);
      leafTaskIds.push_back(leafTaskId);
      auto leafTask = makeTask(leafTaskId, leafPlan, counter);
      tasks.push_back(leafTask);
      Task::start(leafTask, 1);
    }

    core::PlanNodePtr finalAggPlan;
    std::vector<std::string> finalAggTaskIds;
    finalAggPlan = exec::test::PlanBuilder()
                       .exchange(leafPlan->outputType())
                       .singleAggregation({}, {"count(1)"})
                       .partitionedOutput({}, 1)
                       .planNode();

    std::vector<exec::Split> finalAggSplits;
    for (int i = 0; i < width; i++) {
      auto taskId = makeTaskId("final-agg", i);
      finalAggSplits.push_back(
          exec::Split(std::make_shared<exec::RemoteConnectorSplit>(taskId)));
      auto task = makeTask(taskId, finalAggPlan, i);
      tasks.push_back(task);
      Task::start(task, 1);
      addRemoteSplits(task, leafTaskIds);
    }

    auto plan = exec::test::PlanBuilder()
                    .exchange(finalAggPlan->outputType())
                    .singleAggregation({}, {"sum(a0)"})
                    .planNode();

    auto expected =
        makeRowVector({makeFlatVector<int64_t>(1, [&](auto /*row*/) {
          return vectors.size() * vectors[0]->size() * width;
        })});

    exec::test::AssertQueryBuilder(plan)
        .splits(finalAggSplits)
        .assertResults(expected);
    auto elapsed = getCurrentTimeMicro() - startMicros;
    int64_t bytes = 0;
    for (auto& task : tasks) {
      auto stats = task->taskStats();
      for (auto& pipeline : stats.pipelineStats) {
        for (auto& op : pipeline.operatorStats) {
          if (op.operatorType == "Exchange") {
            bytes += op.rawInputBytes;
          }
        }
      }
    }

    counters.bytes += bytes;
    counters.rows += width * vectors.size() * vectors[0]->size();
    counters.usec += elapsed;
  }

 private:
  static constexpr int64_t kMaxMemory = 6UL << 30; // 6GB

  static std::string makeTaskId(const std::string& prefix, int num) {
    return fmt::format("local://{}-{}", prefix, num);
  }

  std::shared_ptr<Task> makeTask(
      const std::string& taskId,
      std::shared_ptr<const core::PlanNode> planNode,
      int destination,
      Consumer consumer = nullptr,
      int64_t maxMemory = kMaxMemory) {
    auto queryCtx = std::make_shared<core::QueryCtx>(
        executor_.get(), std::make_shared<core::MemConfig>(configSettings_));
    queryCtx->pool()->setMemoryUsageTracker(
        memory::MemoryUsageTracker::create(maxMemory));
    core::PlanFragment planFragment{planNode};
    return std::make_shared<Task>(
        taskId,
        std::move(planFragment),
        destination,
        std::move(queryCtx),
        std::move(consumer));
  }

  void addRemoteSplits(
      std::shared_ptr<Task> task,
      const std::vector<std::string>& remoteTaskIds) {
    for (auto& taskId : remoteTaskIds) {
      auto split =
          exec::Split(std::make_shared<RemoteConnectorSplit>(taskId), -1);
      task->addSplit("0", std::move(split));
    }
    task->noMoreSplits("0");
  }

  std::unordered_map<std::string, std::string> configSettings_;
};

ExchangeBenchmark bm;

std::vector<RowVectorPtr> flat10k;
std::vector<RowVectorPtr> deep10k;
std::vector<RowVectorPtr> flat50;
std::vector<RowVectorPtr> deep50;

Counters flat10kCounters;
Counters deep10kCounters;
Counters flat50Counters;
Counters deep50Counters;

BENCHMARK(exchanegeFlat10k) {
  bm.run(flat10k, FLAGS_width, flat10kCounters);
}

BENCHMARK_RELATIVE(exchanegeFlat50) {
  bm.run(flat50, FLAGS_width, flat50Counters);
}

BENCHMARK(exchanegeDeep10k) {
  bm.run(deep10k, FLAGS_width, deep10kCounters);
}

BENCHMARK_RELATIVE(exchanegeDeep50) {
  bm.run(deep50, FLAGS_width, deep50Counters);
}
} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();
  parse::registerTypeResolver();
  serializer::presto::PrestoVectorSerde::registerVectorSerde();
  exec::ExchangeSource::registerFactory();

  auto flatType = ROW({
      {"c0", BIGINT()},
      {"bool_val", BOOLEAN()},
      {"tiny_val", TINYINT()},
      {"long_decimal_val", LONG_DECIMAL(20, 3)},
      {"small_val", SMALLINT()},
      {"int_val", INTEGER()},
      {"long_val", BIGINT()},
      {"float_val", REAL()},
      {"short_decimal_val", SHORT_DECIMAL(10, 2)},
      {"double_val", DOUBLE()},
      {"string_val", VARCHAR()},
  });
  auto deepType = ROW(
      {{"c0", BIGINT()},
       {"long_array_val", ARRAY(ARRAY(BIGINT()))},
       {"array_val", ARRAY(VARCHAR())},
       {"struct_val", ROW({{"s_int", INTEGER()}, {"s_array", ARRAY(REAL())}})},
       {"map_val",
        MAP(VARCHAR(),
            MAP(BIGINT(),
                ROW({{"s2_int", INTEGER()}, {"s2_string", VARCHAR()}})))}});

  flat10k = bm.makeRows(flatType, 10, 10000);
  deep10k = bm.makeRows(deepType, 10, 10000);
  flat50 = bm.makeRows(flatType, 2000, 50);
  deep50 = bm.makeRows(deepType, 2000, 50);

  folly::runBenchmarks();
  std::cout << "flat10k: " << flat10kCounters.toString() << std::endl
            << "flat50: " << flat50Counters.toString() << std::endl
            << "deep10k: " << deep10kCounters.toString() << std::endl
            << "deep50: " << deep50Counters.toString() << std::endl;
  return 0;
  return 0;
}
