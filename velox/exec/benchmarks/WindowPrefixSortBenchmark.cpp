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

#include <fmt/format.h>
#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <string>

#include "velox/exec/tests/utils/Cursor.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

DEFINE_int64(fuzzer_seed, 99887766, "Seed for random input dataset generator");

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

static constexpr int32_t kNumVectors = 50;
static constexpr int32_t kRowsPerVector = 1'0000;

namespace {

class WindowPrefixSortBenchmark : public HiveConnectorTestBase {
 public:
  explicit WindowPrefixSortBenchmark() {
    HiveConnectorTestBase::SetUp();
    aggregate::prestosql::registerAllAggregateFunctions();
    window::prestosql::registerAllWindowFunctions();

    inputType_ = ROW({
        {"k_array", INTEGER()},
        {"k_norm", INTEGER()},
        {"k_hash", INTEGER()},
        {"k_sort", INTEGER()},
        {"i32", INTEGER()},
        {"i64", BIGINT()},
        {"f32", REAL()},
        {"f64", DOUBLE()},
        {"i32_halfnull", INTEGER()},
        {"i64_halfnull", BIGINT()},
        {"f32_halfnull", REAL()},
        {"f64_halfnull", DOUBLE()},
    });

    VectorFuzzer::Options opts;
    opts.vectorSize = kRowsPerVector;
    opts.nullRatio = 0;
    VectorFuzzer fuzzer(opts, pool_.get(), FLAGS_fuzzer_seed);
    std::vector<RowVectorPtr> inputVectors;
    for (auto i = 0; i < kNumVectors; ++i) {
      std::vector<VectorPtr> children;

      // Generate key with a small number of unique values from a small range
      // (0-16).
      children.emplace_back(makeFlatVector<int32_t>(
          kRowsPerVector, [](auto row) { return row % 17; }));

      // Generate key with a small number of unique values from a large range
      // (300 total values).
      children.emplace_back(
          makeFlatVector<int32_t>(kRowsPerVector, [](auto row) {
            if (row % 3 == 0) {
              return std::numeric_limits<int32_t>::max() - row % 100;
            } else if (row % 3 == 1) {
              return row % 100;
            } else {
              return std::numeric_limits<int32_t>::min() + row % 100;
            }
          }));

      // Generate key with many unique values from a large range (500K total
      // values).
      children.emplace_back(fuzzer.fuzzFlat(INTEGER()));

      // Generate a column with increasing values to get a deterministic sort
      // order.
      children.emplace_back(makeFlatVector<int32_t>(
          kRowsPerVector, [](auto row) { return row; }));

      // Generate random values without nulls.
      children.emplace_back(fuzzer.fuzzFlat(INTEGER()));
      children.emplace_back(fuzzer.fuzzFlat(BIGINT()));
      children.emplace_back(fuzzer.fuzzFlat(REAL()));
      children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));

      // Generate random values with nulls.
      opts.nullRatio = 0.05; // 5%
      fuzzer.setOptions(opts);

      children.emplace_back(fuzzer.fuzzFlat(INTEGER()));
      children.emplace_back(fuzzer.fuzzFlat(BIGINT()));
      children.emplace_back(fuzzer.fuzzFlat(REAL()));
      children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));

      inputVectors.emplace_back(makeRowVector(inputType_->names(), children));
    }

    sourceFilePath_ = TempFilePath::create();
    writeToFile(sourceFilePath_->getPath(), inputVectors);
  }

  ~WindowPrefixSortBenchmark() override {
    HiveConnectorTestBase::TearDown();
  }

  CpuWallTiming windowNanos() {
    return windowNanos_;
  }

  void TestBody() override {}

  void run(
      const std::string& key,
      const std::string& aggregate,
      bool prefixSort = true) {
    folly::BenchmarkSuspender suspender1;

    if ((prefixSort && !lastRunPrefixSort_) ||
        (!prefixSort && lastRunPrefixSort_)) {
      std::cout << "WindowNanos: " << windowNanos_.toString() << "\n";
      windowNanos_.clear();
    }

    lastRunPrefixSort_ = prefixSort;
    std::string functionSql = fmt::format(
        "{} over (partition by {} order by k_sort)", aggregate, key);

    core::PlanNodeId tableScanPlanId;
    core::PlanFragment plan = PlanBuilder()
                                  .tableScan(inputType_)
                                  .capturePlanNodeId(tableScanPlanId)
                                  .window({functionSql})
                                  .planFragment();

    vector_size_t numResultRows = 0;
    auto task = makeTask(plan, prefixSort);
    task->addSplit(
        tableScanPlanId,
        exec::Split(makeHiveConnectorSplit(sourceFilePath_->getPath())));
    task->noMoreSplits(tableScanPlanId);
    suspender1.dismiss();

    while (auto result = task->next()) {
      numResultRows += result->size();
    }

    folly::BenchmarkSuspender suspender2;
    auto stats = task->taskStats();
    for (auto& pipeline : stats.pipelineStats) {
      for (auto& op : pipeline.operatorStats) {
        if (op.operatorType == "Window") {
          windowNanos_.add(op.addInputTiming);
          windowNanos_.add(op.getOutputTiming);
        }
        if (op.operatorType == "Values") {
          // This is the timing for Window::noMoreInput() where the window
          // sorting happens. So including in the cpu timing.
          windowNanos_.add(op.finishTiming);
        }
      }
    }
    suspender2.dismiss();
    folly::doNotOptimizeAway(numResultRows);
  }

  std::shared_ptr<exec::Task> makeTask(
      core::PlanFragment plan,
      bool prefixSort) {
    if (prefixSort) {
      return exec::Task::create(
          "t",
          std::move(plan),
          0,
          core::QueryCtx::create(executor_.get()),
          Task::ExecutionMode::kSerial);
    } else {
      const std::unordered_map<std::string, std::string> queryConfigMap(
          {{core::QueryConfig::kPrefixSortNormalizedKeyMaxBytes, "0"}});
      return exec::Task::create(
          "t",
          std::move(plan),
          0,
          core::QueryCtx::create(
              executor_.get(), core::QueryConfig(queryConfigMap)),
          Task::ExecutionMode::kSerial);
    }
  }

 private:
  RowTypePtr inputType_;
  std::shared_ptr<TempFilePath> sourceFilePath_;

  CpuWallTiming windowNanos_;
  bool lastRunPrefixSort_;
};

std::unique_ptr<WindowPrefixSortBenchmark> benchmark;

void doSortRun(uint32_t, const std::string& key, const std::string& aggregate) {
  benchmark->run(key, aggregate, false);
}

void doPrefixSortRun(
    uint32_t,
    const std::string& key,
    const std::string& aggregate) {
  benchmark->run(key, aggregate, true);
}

#define AGG_BENCHMARKS(_name_, _key_)              \
  BENCHMARK_NAMED_PARAM(                           \
      doSortRun,                                   \
      _name_##_INTEGER_##_key_,                    \
      #_key_,                                      \
      fmt::format("{}(i32)", (#_name_)));          \
  BENCHMARK_NAMED_PARAM(                           \
      doPrefixSortRun,                             \
      _name_##_INTEGER_##_key_,                    \
      #_key_,                                      \
      fmt::format("{}(i32)", (#_name_)));          \
  BENCHMARK_NAMED_PARAM(                           \
      doSortRun,                                   \
      _name_##_REAL_##_key_,                       \
      #_key_,                                      \
      fmt::format("{}(f32)", (#_name_)));          \
  BENCHMARK_NAMED_PARAM(                           \
      doPrefixSortRun,                             \
      _name_##_REAL_##_key_,                       \
      #_key_,                                      \
      fmt::format("{}(f32)", (#_name_)));          \
  BENCHMARK_NAMED_PARAM(                           \
      doSortRun,                                   \
      _name_##_INTEGER_NULLS_##_key_,              \
      #_key_,                                      \
      fmt::format("{}(i32_halfnull)", (#_name_))); \
  BENCHMARK_NAMED_PARAM(                           \
      doPrefixSortRun,                             \
      _name_##_INTEGER_NULLS_##_key_,              \
      #_key_,                                      \
      fmt::format("{}(i32_halfnull)", (#_name_))); \
  BENCHMARK_NAMED_PARAM(                           \
      doSortRun,                                   \
      _name_##_REAL_NULLS_##_key_,                 \
      #_key_,                                      \
      fmt::format("{}(f32_halfnull)", (#_name_))); \
  BENCHMARK_NAMED_PARAM(                           \
      doPrefixSortRun,                             \
      _name_##_REAL_NULLS_##_key_,                 \
      #_key_,                                      \
      fmt::format("{}(f32_halfnull)", (#_name_))); \
  BENCHMARK_DRAW_LINE();                           \
  BENCHMARK_DRAW_LINE();

#define MULTI_KEY_AGG_BENCHMARKS(_name_, _key1_, _key2_) \
  BENCHMARK_NAMED_PARAM(                                 \
      doSortRun,                                         \
      _name_##_BIGINT_##_key1_##_key2_,                  \
      fmt::format("{},{}", (#_key1_), (#_key2_)),        \
      fmt::format("{}(i64)", (#_name_)));                \
  BENCHMARK_NAMED_PARAM(                                 \
      doPrefixSortRun,                                   \
      _name_##_BIGINT_##_key1_##_key2_,                  \
      fmt::format("{},{}", (#_key1_), (#_key2_)),        \
      fmt::format("{}(i64)", (#_name_)));                \
  BENCHMARK_NAMED_PARAM(                                 \
      doSortRun,                                         \
      _name_##_BIGINT_NULLS_##_key1_##_key2_,            \
      fmt::format("{},{}", (#_key1_), (#_key2_)),        \
      fmt::format("{}(i64_halfnull)", (#_name_)));       \
  BENCHMARK_NAMED_PARAM(                                 \
      doPrefixSortRun,                                   \
      _name_##_BIGINT_NULLS_##_key1_##_key2_,            \
      fmt::format("{},{}", (#_key1_), (#_key2_)),        \
      fmt::format("{}(i64_halfnull)", (#_name_)));       \
  BENCHMARK_NAMED_PARAM(                                 \
      doSortRun,                                         \
      _name_##_DOUBLE_##_key1_##_key2_,                  \
      fmt::format("{},{}", (#_key1_), (#_key2_)),        \
      fmt::format("{}(f64)", (#_name_)));                \
  BENCHMARK_NAMED_PARAM(                                 \
      doPrefixSortRun,                                   \
      _name_##_DOUBLE_##_key1_##_key2_,                  \
      fmt::format("{},{}", (#_key1_), (#_key2_)),        \
      fmt::format("{}(f64)", (#_name_)));                \
  BENCHMARK_NAMED_PARAM(                                 \
      doSortRun,                                         \
      _name_##_DOUBLE_NULLS_##_key1_##_key2_,            \
      fmt::format("{},{}", (#_key1_), (#_key2_)),        \
      fmt::format("{}(f64_halfnull)", (#_name_)));       \
  BENCHMARK_NAMED_PARAM(                                 \
      doPrefixSortRun,                                   \
      _name_##_DOUBLE_NULLS_##_key1_##_key2_,            \
      fmt::format("{},{}", (#_key1_), (#_key2_)),        \
      fmt::format("{}(f64_halfnull)", (#_name_)));       \
  BENCHMARK_DRAW_LINE();                                 \
  BENCHMARK_DRAW_LINE();

// Count(1) aggregate.
BENCHMARK_NAMED_PARAM(doSortRun, count_k_array, "k_array", "count(1)");
BENCHMARK_NAMED_PARAM(doPrefixSortRun, count_k_array, "k_array", "count(1)");
BENCHMARK_NAMED_PARAM(doSortRun, count_k_norm, "k_norm", "count(1)");
BENCHMARK_NAMED_PARAM(doPrefixSortRun, count_k_norm, "k_norm", "count(1)");
BENCHMARK_NAMED_PARAM(doSortRun, count_k_hash, "k_hash", "count(1)");
BENCHMARK_NAMED_PARAM(doPrefixSortRun, count_k_hash, "k_hash", "count(1)");
BENCHMARK_NAMED_PARAM(
    doSortRun,
    count_k_array_k_hash,
    "k_array,i32",
    "count(1)");
BENCHMARK_NAMED_PARAM(
    doPrefixSortRun,
    count_k_array_k_hash,
    "k_array,i64",
    "count(1)");
BENCHMARK_DRAW_LINE();

// Count aggregate.
AGG_BENCHMARKS(count, k_array)
AGG_BENCHMARKS(count, k_norm)
AGG_BENCHMARKS(count, k_hash)
MULTI_KEY_AGG_BENCHMARKS(count, k_array, i32)
MULTI_KEY_AGG_BENCHMARKS(count, k_array, i64)
MULTI_KEY_AGG_BENCHMARKS(count, k_hash, f32)
MULTI_KEY_AGG_BENCHMARKS(count, k_hash, f64)
BENCHMARK_DRAW_LINE();

// Avg aggregate.
AGG_BENCHMARKS(avg, k_array)
AGG_BENCHMARKS(avg, k_norm)
AGG_BENCHMARKS(avg, k_hash)
MULTI_KEY_AGG_BENCHMARKS(avg, k_array, i32)
MULTI_KEY_AGG_BENCHMARKS(avg, k_array, i64)
MULTI_KEY_AGG_BENCHMARKS(avg, k_hash, f32)
MULTI_KEY_AGG_BENCHMARKS(avg, k_hash, f64)
BENCHMARK_DRAW_LINE();

// Min aggregate.
AGG_BENCHMARKS(min, k_array)
AGG_BENCHMARKS(min, k_norm)
AGG_BENCHMARKS(min, k_hash)
MULTI_KEY_AGG_BENCHMARKS(min, k_array, i32)
MULTI_KEY_AGG_BENCHMARKS(min, k_array, i64)
MULTI_KEY_AGG_BENCHMARKS(min, k_hash, f32)
MULTI_KEY_AGG_BENCHMARKS(min, k_hash, f64)
BENCHMARK_DRAW_LINE();

// Max aggregate.
AGG_BENCHMARKS(max, k_array)
AGG_BENCHMARKS(max, k_norm)
AGG_BENCHMARKS(max, k_hash)
MULTI_KEY_AGG_BENCHMARKS(max, k_array, i32)
MULTI_KEY_AGG_BENCHMARKS(max, k_array, i64)
MULTI_KEY_AGG_BENCHMARKS(max, k_hash, f32)
MULTI_KEY_AGG_BENCHMARKS(max, k_hash, f64)
BENCHMARK_DRAW_LINE();

} // namespace

int main(int argc, char** argv) {
  folly::Init(&argc, &argv);
  facebook::velox::memory::MemoryManager::initialize({});

  benchmark = std::make_unique<WindowPrefixSortBenchmark>();
  folly::runBenchmarks();
  std::cout << "WindowNanos: " << benchmark->windowNanos().toString() << "\n";
  benchmark.reset();
  return 0;
}
