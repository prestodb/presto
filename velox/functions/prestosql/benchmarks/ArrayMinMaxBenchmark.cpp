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
#include "velox/functions/Macros.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/VectorFunctions.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace {

template <typename T>
VELOX_UDF_BEGIN(array_min_simple)
FOLLY_ALWAYS_INLINE bool call(T& out, const arg_type<Array<T>>& x) {
  if (x.begin() == x.end()) {
    return false;
  }
  out = std::min_element(x.begin(), x.end())->value();
  return true;
}
VELOX_UDF_END();

template <typename T>
VELOX_UDF_BEGIN(array_max_simple)
FOLLY_ALWAYS_INLINE bool call(T& out, const arg_type<Array<T>>& x) {
  if (x.begin() == x.end()) {
    return false;
  }
  out = std::max_element(x.begin(), x.end())->value();
  return true;
}
VELOX_UDF_END();

class ArrayMinMaxBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  ArrayMinMaxBenchmark() : FunctionBenchmarkBase() {
    functions::registerVectorFunctions();
    registerFunction<udf_array_min_simple<int32_t>, int32_t, Array<int32_t>>();
    registerFunction<udf_array_max_simple<int32_t>, int32_t, Array<int32_t>>();
  }

  void runInteger(const std::string& functionName) {
    folly::BenchmarkSuspender suspender;
    vector_size_t size = 1'000;
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

  void doRun(ExprSet& exprSet, const RowVectorPtr& rowVector) {
    int cnt = 0;
    for (auto i = 0; i < 100; i++) {
      cnt += evaluate(exprSet, rowVector)->size();
    }
    folly::doNotOptimizeAway(cnt);
  }
};

BENCHMARK(simpleMinInteger) {
  ArrayMinMaxBenchmark benchmark;
  benchmark.runInteger("array_min_simple");
}

BENCHMARK_RELATIVE(vectorMinInteger) {
  ArrayMinMaxBenchmark benchmark;
  benchmark.runInteger("array_min");
}

BENCHMARK(simpleMaxInteger) {
  ArrayMinMaxBenchmark benchmark;
  benchmark.runInteger("array_max_simple");
}

BENCHMARK_RELATIVE(vectorMaxInteger) {
  ArrayMinMaxBenchmark benchmark;
  benchmark.runInteger("array_max");
}
} // namespace

int main(int /*argc*/, char** /*argv*/) {
  folly::runBenchmarks();
  return 0;
}
