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
VELOX_UDF_BEGIN(contains)
// UDF to tell whether a element is part of an array
FOLLY_ALWAYS_INLINE bool call(
    bool& out,
    const arg_type<Array<T>>& x,
    const arg_type<T>& element) {
  out = std::find(x.begin(), x.end(), element) != x.end();
  return true;
}
VELOX_UDF_END();

class ArrayContainsBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  ArrayContainsBenchmark() : FunctionBenchmarkBase() {
    functions::registerVectorFunctions();

    registerFunction<
        udf_contains<int32_t>,
        bool,
        facebook::velox::Array<int32_t>,
        int32_t>({"contains_alt"});

    registerFunction<
        udf_contains<Varchar>,
        bool,
        facebook::velox::Array<Varchar>,
        Varchar>({"contains_alt"});
  }

  void runInteger(const std::string& functionName) {
    folly::BenchmarkSuspender suspender;
    vector_size_t size = 1'000;
    auto arrayVector = vectorMaker_.arrayVector<int32_t>(
        size,
        [](auto row) { return row % 5; },
        [](auto row) { return row % 23; });

    auto elementVector = BaseVector::createConstant(7, size, execCtx_.pool());

    auto rowVector = vectorMaker_.rowVector({arrayVector, elementVector});
    auto exprSet = compileExpression(
        fmt::format("{}(c0, c1)", functionName), rowVector->type());
    suspender.dismiss();

    doRun(exprSet, rowVector);
  }

  void runVarchar(const std::string& functionName) {
    folly::BenchmarkSuspender suspender;
    vector_size_t size = 1'000;

    std::vector<std::string> colors = {
        "red",
        "blue",
        "green",
        "yellow",
        "orange",
        "purple",
        "crimson red",
        "cerulean blue"};

    auto arrayVector = vectorMaker_.arrayVector<StringView>(
        size,
        [](auto row) { return row % 5; },
        [&](auto row) { return StringView(colors[row % colors.size()]); });

    auto elementVector =
        BaseVector::createConstant("crimson red", size, execCtx_.pool());

    auto rowVector = vectorMaker_.rowVector({arrayVector, elementVector});
    auto exprSet = compileExpression(
        fmt::format("{}(c0, c1)", functionName), rowVector->type());
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

BENCHMARK(vectorAdapterInteger) {
  ArrayContainsBenchmark benchmark;
  benchmark.runInteger("contains_alt");
}

BENCHMARK_RELATIVE(vectorFunctionInteger) {
  ArrayContainsBenchmark benchmark;
  benchmark.runInteger("contains");
}

BENCHMARK(vectorAdapterVarchar) {
  ArrayContainsBenchmark benchmark;
  benchmark.runVarchar("contains_alt");
}

BENCHMARK_RELATIVE(vectorFunctionVarchar) {
  ArrayContainsBenchmark benchmark;
  benchmark.runVarchar("contains");
}

} // namespace

int main(int /*argc*/, char** /*argv*/) {
  folly::runBenchmarks();
  return 0;
}
