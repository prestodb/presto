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
#include "folly/Benchmark.h"
#include "folly/init/Init.h"

#include "velox/common/memory/Memory.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;

namespace facebook::velox::exec {
namespace {

struct TestCase {
  std::vector<RowVectorPtr> data;
  std::vector<std::string> aggregates;
  core::PlanNodePtr plan;
};

struct BenchmarkParams {
  // Each time test one aggregate function.
  std::string aggregate;
  // Type of payload columns, assume all payloads have the same type.
  TypePtr payloadType;
  // Number of payload columns.
  int32_t numPayloads;
  // Number of clustered groups.
  int32_t numGroups;
  // Number of rows per group, i.e. how many times each key is repeated.
  int32_t numRowsPerGroup;
};

// Benchmark for streaming aggregation with different aggregates, can be used
// to evaluate the performance of various aggregates under different workloads.
//
// The benchmark uses synthetic data with 1 integer grouping key and 1000
// payloads of different data types. For each combination of aggregate and
// payload type, run 4 benchmarks by repeating the grouping key 1 (baseline,
// should be the slowest), 5, 10, and 15 times.
class StreamingAggregationBenchmark : public VectorTestBase {
 public:
  // Make a single benchmark.
  void makeBenchmark(const BenchmarkParams& params) {
    auto test = std::make_unique<TestCase>();
    test->data = makeData(
        params.payloadType,
        params.numPayloads,
        params.numGroups,
        params.numRowsPerGroup);
    test->aggregates = makeAggregates(params.aggregate, params.numPayloads);
    test->plan = exec::test::PlanBuilder()
                     .values(test->data)
                     .streamingAggregation(
                         {"k0"},
                         test->aggregates,
                         {},
                         core::AggregationNode::Step::kSingle,
                         false)
                     .planNode();

    auto name = fmt::format(
        "[{}]_{}_{}_groups",
        params.aggregate,
        params.payloadType->toString(),
        std::to_string(params.numGroups));

    folly::addBenchmark(__FILE__, name, [plan = &test->plan]() {
      std::shared_ptr<Task> task;
      exec::test::AssertQueryBuilder(*plan)
          .serialExecution(true)
          .runWithoutResults(task);
      return 1;
    });

    cases_.push_back(std::move(test));
  }

  // Make a set of benchmarks for a given aggregate function and payload type.
  // Assume 1000 payload columns and a total of 150 rows.
  void makeBenchmarks(
      const std::string& aggregate,
      const TypePtr& payloadType) {
    for (auto numRepeats : {1, 5, 10, 15}) {
      makeBenchmark(
          {.aggregate = aggregate,
           .payloadType = payloadType,
           .numPayloads = 1000,
           .numGroups = 150 / numRepeats,
           .numRowsPerGroup = numRepeats});
    }
  }

 private:
  // Make the test data with one key column and 'numPayloads' payloads with
  // the same data type.
  std::vector<RowVectorPtr> makeData(
      const TypePtr& payloadType,
      int32_t numPayloads,
      int32_t numGroups,
      int32_t numRowsPerGroup) {
    std::vector<std::string> names;
    // One grouping key.
    const auto totalCols = 1 + numPayloads;
    names.reserve(totalCols);
    names.emplace_back("k0");
    for (auto i = 0; i < numPayloads; ++i) {
      names.push_back(fmt::format("c{}", i));
    }

    std::vector<RowVectorPtr> data;
    data.reserve(numGroups);

    VectorFuzzer::Options opts;
    opts.vectorSize = numRowsPerGroup;
    opts.complexElementsMaxSize = 5;
    VectorFuzzer fuzzer(opts, pool());

    for (auto i = 0; i < numGroups; ++i) {
      std::vector<VectorPtr> children;
      children.reserve(totalCols);
      auto keys = makeConstant(i, numRowsPerGroup);
      children.push_back(keys);

      for (auto j = 0; j < numPayloads; ++j) {
        auto payload = fuzzer.fuzz(payloadType);
        children.push_back(payload);
      }
      data.push_back(makeRowVector(names, children));
    }

    return data;
  }

  std::vector<std::string> makeAggregates(
      const std::string& aggregate,
      int32_t numPayloads) {
    std::vector<std::string> aggregates;
    aggregates.reserve(numPayloads);
    for (auto i = 0; i < numPayloads; ++i) {
      aggregates.push_back(fmt::format("{}(c{})", aggregate, i));
    }
    return aggregates;
  }

  std::vector<std::unique_ptr<TestCase>> cases_;
};

} // namespace
} // namespace facebook::velox::exec

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  memory::initializeMemoryManager(memory::MemoryManager::Options{});
  aggregate::prestosql::registerAllAggregateFunctions();

  StreamingAggregationBenchmark bm;
  bm.makeBenchmarks("array_agg", REAL());
  BENCHMARK_DRAW_LINE();
  bm.makeBenchmarks("array_agg", ARRAY(REAL()));
  BENCHMARK_DRAW_LINE();
  bm.makeBenchmarks("arbitrary", REAL());
  BENCHMARK_DRAW_LINE();
  bm.makeBenchmarks("arbitrary", ARRAY(BIGINT()));

  folly::runBenchmarks();

  return 0;
}
