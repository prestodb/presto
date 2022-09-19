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
#include "velox/functions/Registerer.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace {

class ConcatBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  explicit ConcatBenchmark(uint32_t seed)
      : FunctionBenchmarkBase(), seed_{seed} {
    functions::prestosql::registerStringFunctions();
  }

  RowVectorPtr generateData() {
    VectorFuzzer::Options options;
    options.vectorSize = 10'024;
    options.stringVariableLength = true;
    options.stringLength = 64;

    VectorFuzzer fuzzer(options, pool(), seed_);

    return vectorMaker_.rowVector(
        {fuzzer.fuzzFlat(VARCHAR()), fuzzer.fuzzFlat(VARCHAR())});
  }

  size_t runBasic(size_t times) {
    folly::BenchmarkSuspender suspender;
    auto data = generateData();
    auto exprSet = compileExpression(
        "concat(c0, concat(', ', concat(c1, concat(',', concat('567', concat(',', concat('129', concat(',', '987654321'))))))))",
        asRowType(data->type()));
    suspender.dismiss();

    return doRun(exprSet, data, times);
  }

  size_t runFlattened(size_t times) {
    folly::BenchmarkSuspender suspender;
    auto data = generateData();
    auto exprSet = compileExpression(
        "concat(c0, ', ', c1, ',', '567', ',', '129', ',', '987654321')",
        asRowType(data->type()));
    suspender.dismiss();

    return doRun(exprSet, data, times);
  }

  size_t runFlattenedAndConstantFolded(size_t times) {
    folly::BenchmarkSuspender suspender;
    auto data = generateData();
    auto exprSet = compileExpression(
        "concat(c0, ', ', c1, ',567,129,987654321')", asRowType(data->type()));
    suspender.dismiss();

    return doRun(exprSet, data, times);
  }

  size_t doRun(ExprSet& exprSet, const RowVectorPtr& rowVector, size_t times) {
    int cnt = 0;
    for (auto i = 0; i < times * 1'000; i++) {
      cnt += evaluate(exprSet, rowVector)->size();
    }
    return cnt;
  }

  const uint32_t seed_;
};

const uint32_t seed = folly::Random::rand32();

BENCHMARK_MULTI(basic, n) {
  ConcatBenchmark benchmark(seed);
  return benchmark.runBasic(n);
}

BENCHMARK_MULTI(flatten, n) {
  ConcatBenchmark benchmark(seed);
  return benchmark.runFlattened(n);
}

BENCHMARK_MULTI(flattenAndConstantFold, n) {
  ConcatBenchmark benchmark(seed);
  return benchmark.runFlattenedAndConstantFolded(n);
}

} // namespace

int main(int /*argc*/, char** /*argv*/) {
  LOG(ERROR) << "Seed: " << seed;
  folly::runBenchmarks();
  return 0;
}
