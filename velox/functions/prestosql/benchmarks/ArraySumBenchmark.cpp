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

#include "velox/functions/lib/LambdaFunctionUtil.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/ArrayFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::functions;

namespace {

class ArraySumBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  ArraySumBenchmark() : FunctionBenchmarkBase() {
    functions::prestosql::registerArrayFunctions();
    registerFunction<ArraySumFunction, int64_t, Array<int32_t>>(
        {"array_sum_alt"});
  }

  void runInteger(const std::string& functionName) {
    folly::BenchmarkSuspender suspender;
    vector_size_t size = 10'000;
    auto arrayVector = vectorMaker_.arrayVector<int32_t>(
        size,
        [](auto row) { return row % 5; },
        [](auto row) { return row % 23; });

    auto rowVector = vectorMaker_.rowVector({arrayVector});
    auto exprSet = compileExpression(
        fmt::format("{}(c0)", functionName), rowVector->type());
    suspender.dismiss();

    doRun(exprSet, rowVector);
  }

  void runIntegerNulls(const std::string& functionName) {
    folly::BenchmarkSuspender suspender;
    vector_size_t size = 10'000;
    auto arrayVector = vectorMaker_.arrayVector<int32_t>(
        size,
        [](auto row) { return row % 5; },
        [](auto row) { return row % 23; },
        [](auto row) { return (row % 513) == 0; },
        [](auto row) { return (row % 13) == 0; });

    auto rowVector = vectorMaker_.rowVector({arrayVector});
    auto exprSet = compileExpression(
        fmt::format("{}(c0)", functionName), rowVector->type());
    suspender.dismiss();

    doRun(exprSet, rowVector);
  }

  void doRun(ExprSet& exprSet, const RowVectorPtr& rowVector) {
    int cnt = 0;
    for (auto i = 0; i < 100; i++) {
      cnt += evaluate(exprSet, rowVector)->size();
    }
    folly::doNotOptimizeAway(cnt);
  }
};

BENCHMARK(SimpleFunction) {
  ArraySumBenchmark benchmark;
  benchmark.runInteger("array_sum_alt");
}

BENCHMARK_RELATIVE(VectorFunction) {
  ArraySumBenchmark benchmark;
  benchmark.runInteger("array_sum");
}

BENCHMARK(SimpleFunctionNulls) {
  ArraySumBenchmark benchmark;
  benchmark.runIntegerNulls("array_sum_alt");
}

BENCHMARK_RELATIVE(VectorFunctionNulls) {
  ArraySumBenchmark benchmark;
  benchmark.runIntegerNulls("array_sum");
}

} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv);

  folly::runBenchmarks();
  return 0;
}
