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

#include <gflags/gflags.h>

#include "velox/functions/Registerer.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/ArithmeticImpl.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

DEFINE_int64(fuzzer_seed, 99887766, "Seed for random input dataset generator");

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;

namespace {
class SimpleCastBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  explicit SimpleCastBenchmark() : FunctionBenchmarkBase() {}

  RowVectorPtr makeRowVector(vector_size_t size) {
    VectorFuzzer::Options opts;
    opts.vectorSize = size;
    opts.nullRatio = 0;
    VectorFuzzer fuzzer(opts, pool(), FLAGS_fuzzer_seed);
    std::vector<VectorPtr> children;
    children.emplace_back(fuzzer.fuzzFlat(TIMESTAMP())); // Col a
    return std::make_shared<RowVector>(
        pool(), inputType_, nullptr, size, std::move(children));
  }

  static constexpr auto kNumSmallRuns = 10'000;
  static constexpr auto kNumMediumRuns = 1000;
  static constexpr auto kNumLargeRuns = 100;

  void runSmall(const std::string& expression) {
    run(expression, kNumSmallRuns, smallRowVector_);
  }

  void runMedium(const std::string& expression) {
    run(expression, kNumMediumRuns, mediumRowVector_);
  }

  void runLarge(const std::string& expression) {
    run(expression, kNumLargeRuns, largeRowVector_);
  }

  // Compiles and runs the `expression` `iterations` number of times.
  size_t run(
      const std::string& expression,
      size_t iterations,
      const RowVectorPtr& input) {
    folly::BenchmarkSuspender suspender;
    auto exprSet = compileExpression(expression, inputType_);
    suspender.dismiss();

    size_t count = 0;
    for (auto i = 0; i < iterations; i++) {
      count += evaluate(exprSet, input)->size();
    }
    return count;
  }

 private:
  const TypePtr inputType_ = ROW({
      {"a", TIMESTAMP()},
  });
  const RowVectorPtr smallRowVector_ = makeRowVector(100);
  const RowVectorPtr mediumRowVector_ = makeRowVector(1'000);
  const RowVectorPtr largeRowVector_ = makeRowVector(10'000);
};

std::unique_ptr<SimpleCastBenchmark> benchmark;

BENCHMARK(castTimestampDate) {
  benchmark->setAdjustTimestampToTimezone("false");
  benchmark->runSmall("cast (a as date)");
  benchmark->runMedium("cast (a as date)");
  benchmark->runLarge("cast (a as date)");
}

BENCHMARK(castTimestampDateAdjustTimeZone) {
  benchmark->setTimezone("America/Los_Angeles");
  benchmark->setAdjustTimestampToTimezone("true");
  benchmark->runSmall("cast (a as date)");
  benchmark->runMedium("cast (a as date)");
  benchmark->runLarge("cast (a as date)");
}
} // namespace

int main(int argc, char* argv[]) {
  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  benchmark = std::make_unique<SimpleCastBenchmark>();
  folly::runBenchmarks();
  benchmark.reset();
  return 0;
}
