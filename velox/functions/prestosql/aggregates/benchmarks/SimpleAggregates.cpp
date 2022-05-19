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
#include "velox/vector/fuzzer/VectorFuzzer.h"

DEFINE_int64(fuzzer_seed, 99887766, "Seed for random input dataset generator");

using namespace facebook::velox;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::exec::test;

static constexpr int32_t kNumVectors = 100;
static constexpr int32_t kRowsPerVector = 10'000;

namespace {

class SimpleAggregatesBenchmark : public HiveConnectorTestBase {
 public:
  explicit SimpleAggregatesBenchmark() {
    HiveConnectorTestBase::SetUp();

    inputType_ = ROW({
        {"k_array", INTEGER()},
        {"k_norm", INTEGER()},
        {"k_hash", INTEGER()},
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
    opts.nullChance = 0;
    VectorFuzzer fuzzer(opts, pool(), FLAGS_fuzzer_seed);

    std::vector<RowVectorPtr> vectors;
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

      // Generate random values without nulls.
      children.emplace_back(fuzzer.fuzzFlat(INTEGER()));
      children.emplace_back(fuzzer.fuzzFlat(BIGINT()));
      children.emplace_back(fuzzer.fuzzFlat(REAL()));
      children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));

      // Generate random values with nulls.

      opts.nullChance = 2; // 50%
      fuzzer.setOptions(opts);

      children.emplace_back(fuzzer.fuzzFlat(INTEGER()));
      children.emplace_back(fuzzer.fuzzFlat(BIGINT()));
      children.emplace_back(fuzzer.fuzzFlat(REAL()));
      children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));

      vectors.emplace_back(makeRowVector(inputType_->names(), children));
    }

    filePath_ = TempFilePath::create();
    writeToFile(filePath_->path, vectors);
  }

  ~SimpleAggregatesBenchmark() override {
    HiveConnectorTestBase::TearDown();
  }

  void TestBody() override {}

  void run(const std::string& key, const std::string& aggregate) {
    folly::BenchmarkSuspender suspender;

    auto plan = PlanBuilder()
                    .tableScan(inputType_)
                    .partialAggregation({key}, {aggregate})
                    .finalAggregation()
                    .planFragment();

    vector_size_t numResultRows = 0;
    auto task = makeTask(plan, numResultRows);

    task->addSplit("0", exec::Split(makeHiveConnectorSplit(filePath_->path)));
    task->noMoreSplits("0");

    suspender.dismiss();

    exec::Task::start(task, 1);
    auto& executor = folly::QueuedImmediateExecutor::instance();
    auto future = task->stateChangeFuture(60'000'000).via(&executor);
    future.wait();

    folly::doNotOptimizeAway(numResultRows);
  }

  std::shared_ptr<exec::Task> makeTask(
      core::PlanFragment plan,
      vector_size_t& numResultRows) {
    return std::make_shared<exec::Task>(
        "t",
        std::move(plan),
        0,
        core::QueryCtx::createForTest(),
        [&](auto vector, auto* /*future*/) {
          if (vector) {
            numResultRows += vector->size();
          }
          return exec::BlockingReason::kNotBlocked;
        });
  }

 private:
  RowTypePtr inputType_;
  std::shared_ptr<TempFilePath> filePath_;
};

std::unique_ptr<SimpleAggregatesBenchmark> benchmark;

void doRun(uint32_t, const std::string& key, const std::string& aggregate) {
  benchmark->run(key, aggregate);
}

#define AGG_BENCHMARKS(_name_, _key_)              \
  BENCHMARK_NAMED_PARAM(                           \
      doRun,                                       \
      _name_##_INTEGER_##_key_,                    \
      #_key_,                                      \
      fmt::format("{}(i32)", (#_name_)));          \
  BENCHMARK_NAMED_PARAM(                           \
      doRun,                                       \
      _name_##_BIGINT_##_key_,                     \
      #_key_,                                      \
      fmt::format("{}(i64)", (#_name_)));          \
  BENCHMARK_NAMED_PARAM(                           \
      doRun,                                       \
      _name_##_REAL_##_key_,                       \
      #_key_,                                      \
      fmt::format("{}(f32)", (#_name_)));          \
  BENCHMARK_NAMED_PARAM(                           \
      doRun,                                       \
      _name_##_DOUBLE_##_key_,                     \
      #_key_,                                      \
      fmt::format("{}(f64)", (#_name_)));          \
  BENCHMARK_DRAW_LINE();                           \
  BENCHMARK_NAMED_PARAM(                           \
      doRun,                                       \
      _name_##_INTEGER_NULLS_##_key_,              \
      #_key_,                                      \
      fmt::format("{}(i32_halfnull)", (#_name_))); \
  BENCHMARK_NAMED_PARAM(                           \
      doRun,                                       \
      _name_##_BIGINT_NULLS_##_key_,               \
      #_key_,                                      \
      fmt::format("{}(i64_halfnull)", (#_name_))); \
  BENCHMARK_NAMED_PARAM(                           \
      doRun,                                       \
      _name_##_REAL_NULLS_##_key_,                 \
      #_key_,                                      \
      fmt::format("{}(f32_halfnull)", (#_name_))); \
  BENCHMARK_NAMED_PARAM(                           \
      doRun,                                       \
      _name_##_DOUBLE_NULLS_##_key_,               \
      #_key_,                                      \
      fmt::format("{}(f64_halfnull)", (#_name_))); \
  BENCHMARK_DRAW_LINE();                           \
  BENCHMARK_DRAW_LINE();

// Count(1) aggregate.
BENCHMARK_NAMED_PARAM(doRun, count_k_array, "k_array", "count(1)");
BENCHMARK_NAMED_PARAM(doRun, count_k_norm, "k_norm", "count(1)");
BENCHMARK_NAMED_PARAM(doRun, count_k_hash, "k_hash", "count(1)");
BENCHMARK_DRAW_LINE();

// Count aggregate.
AGG_BENCHMARKS(count, k_array)
AGG_BENCHMARKS(count, k_norm)
AGG_BENCHMARKS(count, k_hash)
BENCHMARK_DRAW_LINE();

// Sum aggregate.
AGG_BENCHMARKS(sum, k_array)
AGG_BENCHMARKS(sum, k_norm)
AGG_BENCHMARKS(sum, k_hash)
BENCHMARK_DRAW_LINE();

// Avg aggregate.
AGG_BENCHMARKS(avg, k_array)
AGG_BENCHMARKS(avg, k_norm)
AGG_BENCHMARKS(avg, k_hash)
BENCHMARK_DRAW_LINE();

// Min aggregate.
AGG_BENCHMARKS(min, k_array)
AGG_BENCHMARKS(min, k_norm)
AGG_BENCHMARKS(min, k_hash)
BENCHMARK_DRAW_LINE();

// Max aggregate.
AGG_BENCHMARKS(max, k_array)
AGG_BENCHMARKS(max, k_norm)
AGG_BENCHMARKS(max, k_hash)
BENCHMARK_DRAW_LINE();

// Stddev aggregate.
AGG_BENCHMARKS(stddev, k_array)
AGG_BENCHMARKS(stddev, k_norm)
AGG_BENCHMARKS(stddev, k_hash)
BENCHMARK_DRAW_LINE();

} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv);

  benchmark = std::make_unique<SimpleAggregatesBenchmark>();
  folly::runBenchmarks();
  benchmark.reset();
  return 0;
}
