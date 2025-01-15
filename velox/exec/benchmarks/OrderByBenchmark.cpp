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
#include <iostream>

#include "glog/logging.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/benchmarks/OrderByBenchmarkUtil.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace {

struct TestCase {
  vector_size_t numRows;
  RowTypePtr rowType;
  int numKeys;
};

class OrderByBenchmark {
 public:
  void addBenchmark(
      const std::string& benchmarkName,
      vector_size_t numRows,
      const RowTypePtr& rowType,
      int32_t iterations,
      int numKeys) {
    TestCase testCase = {numRows, rowType, numKeys};
    {
      folly::addBenchmark(
          __FILE__,
          "OrderBy_" + benchmarkName,
          [test = testCase, iterations = std::max(1, iterations / 10), this]() {
            core::PlanNodeId orderByNodeId;
            const auto plan = makeOrderByPlan(test, orderByNodeId);
            uint64_t inputNs = 0;
            uint64_t outputNs = 0;
            uint64_t finishNs = 0;
            const auto start = getCurrentTimeMicro();
            for (auto i = 0; i < iterations; ++i) {
              std::shared_ptr<Task> task;
              test::AssertQueryBuilder(plan).runWithoutResults(task);
              auto taskStats = exec::toPlanStats(task->taskStats());
              auto& stats = taskStats.at(orderByNodeId);
              inputNs += stats.addInputTiming.wallNanos;
              finishNs += stats.finishTiming.wallNanos;
              outputNs += stats.getOutputTiming.wallNanos;
            }
            const uint64_t total = getCurrentTimeMicro() - start;
            std::cout << "Total " << succinctMicros(total) << " Input "
                      << succinctNanos(inputNs) << " Output "
                      << succinctNanos(outputNs) << " Finish "
                      << succinctNanos(finishNs) << std::endl;
            return 1;
          });
    }
  }

 private:
  core::PlanNodePtr makeOrderByPlan(
      const TestCase& test,
      core::PlanNodeId& orderByNodeId) {
    folly::BenchmarkSuspender suspender;
    std::vector<RowVectorPtr> vectors;
    vectors.emplace_back(OrderByBenchmarkUtil::fuzzRows(
        test.rowType, test.numRows, pool_.get()));

    std::vector<std::string> keys;
    keys.reserve(test.numKeys);
    for (auto i = 0; i < test.numKeys; i++) {
      keys.emplace_back(fmt::format("c{} ASC NULLS LAST", i));
    }

    return test::PlanBuilder()
        .values(vectors)
        .orderBy(keys, false)
        .capturePlanNodeId(orderByNodeId)
        .planNode();
  }

  std::shared_ptr<memory::MemoryPool> rootPool_{
      memory::memoryManager()->addRootPool()};
  std::shared_ptr<memory::MemoryPool> pool_{
      rootPool_->addLeafChild("OrderByBenchmark")};
};
} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);

  memory::MemoryManager::initialize({});
  OrderByBenchmark bm;
  OrderByBenchmarkUtil::addBenchmarks([&](const std::string& benchmarkName,
                                          vector_size_t numRows,
                                          const RowTypePtr& rowType,
                                          int iterations,
                                          int numKeys) {
    bm.addBenchmark(benchmarkName, numRows, rowType, iterations, numKeys);
  });

  folly::runBenchmarks();
  return 0;
}
