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

#include <velox/common/base/VeloxException.h>
#include "velox/functions/Registerer.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/benchmarks/ArrayMinMaxBenchmark.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

namespace facebook::velox::functions {

void registerVectorFunctionBasic() {
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_min_basic, "vector_basic");
}

void registerSimpleFunctions() {
  registerFunction<ArrayMinSimpleFunction, int32_t, Array<int32_t>>(
      {"array_min_simple"});

  registerFunction<ArrayMinSimpleFunctionIterator, int32_t, Array<int32_t>>(
      {"array_min_simple_iterator"});

  registerFunction<
      ArrayMinSimpleFunctionSkipNullIterator,
      int32_t,
      Array<int32_t>>({"array_min_simple_skip_null_iterator"});
}

namespace {

class ArrayMinMaxBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  ArrayMinMaxBenchmark() : FunctionBenchmarkBase() {
    functions::prestosql::registerArrayFunctions();
    registerVectorFunctionBasic();
    registerSimpleFunctions();
  }

  RowVectorPtr makeData() {
    const vector_size_t size = 1'000;
    auto arrayVector = vectorMaker_.arrayVector<int32_t>(
        size,
        [](auto row) { return row % 5; },
        [](auto row) { return row % 23; });

    return vectorMaker_.rowVector({arrayVector});
  }

  size_t runInteger(const std::string& functionName) {
    folly::BenchmarkSuspender suspender;
    auto arrayVector = makeData()->childAt(0);
    auto rowVector = vectorMaker_.rowVector({arrayVector});
    auto exprSet = compileExpression(
        fmt::format("{}(c0)", functionName), rowVector->type());
    suspender.dismiss();

    return doRun(exprSet, rowVector);
  }

  size_t doRun(exec::ExprSet& exprSet, const RowVectorPtr& rowVector) {
    int cnt = 0;
    for (auto i = 0; i < 100; i++) {
      cnt += evaluate(exprSet, rowVector)->size();
    }
    return cnt;
  }

  template <typename F>
  size_t runFastInteger(F function) {
    folly::BenchmarkSuspender suspender;
    auto arrayVector = makeData()->childAt(0);
    suspender.dismiss();

    int cnt = 0;
    for (auto i = 0; i < 100; i++) {
      cnt += function(arrayVector)->size();
    }
    folly::doNotOptimizeAway(cnt);
    return cnt;
  }
};

BENCHMARK_MULTI(fastMinInteger) {
  ArrayMinMaxBenchmark benchmark;
  return benchmark.runFastInteger(fastMin<int32_t>);
}

BENCHMARK_MULTI(vectorNoFastPath) {
  ArrayMinMaxBenchmark benchmark;
  return benchmark.runInteger("vector_basic");
}

BENCHMARK_MULTI(simpleMinIntegerSkipNullIterator) {
  ArrayMinMaxBenchmark benchmark;
  return benchmark.runInteger("array_min_simple_skip_null_iterator");
}

BENCHMARK_MULTI(simpleMinIntegerIterator) {
  ArrayMinMaxBenchmark benchmark;
  return benchmark.runInteger("array_min_simple_iterator");
}

BENCHMARK_MULTI(simpleMinInteger) {
  ArrayMinMaxBenchmark benchmark;
  return benchmark.runInteger("array_min_simple");
}

// The function registred in prestoSQL.
BENCHMARK_MULTI(prestoSQLArrayMin) {
  ArrayMinMaxBenchmark benchmark;
  return benchmark.runInteger("array_min");
}

} // namespace
} // namespace facebook::velox::functions

int main(int /*argc*/, char** /*argv*/) {
  folly::runBenchmarks();
  return 0;
}
