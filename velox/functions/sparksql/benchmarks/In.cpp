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
#include <string>

#include <fmt/format.h>
#include <folly/Benchmark.h>
#include <folly/Conv.h>
#include <folly/Random.h>
#include <folly/init/Init.h>

#include "velox/expression/tests/VectorFuzzer.h"
#include "velox/functions/lib/Re2Functions.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/sparksql/In.h"

namespace facebook::velox::functions {
void registerPrestoIn() {
  VELOX_REGISTER_VECTOR_FUNCTION(udf_in, "presto");
}
} // namespace facebook::velox::functions

namespace facebook::velox::functions::sparksql {
namespace {

int in_int(int iters, int inListSize, const std::string& functionName) {
  folly::BenchmarkSuspender kSuspender;
  test::FunctionBenchmarkBase benchmarkBase;

  VectorFuzzer::Options opts;
  opts.vectorSize = 1024;
  auto vector = VectorFuzzer(opts, benchmarkBase.pool()).fuzzFlat(BIGINT());
  const auto data = benchmarkBase.maker().rowVector({vector});

  std::string exprStr = functionName + "(c0";
  for (int i = 0; i < inListSize; i++) {
    fmt::format_to(
        std::back_inserter(exprStr),
        ", {}",
        vector->as<SimpleVector<int64_t>>()->valueAt(
            folly::Random::rand32() % opts.vectorSize));
  }
  exprStr += ")";
  exec::ExprSet expr = benchmarkBase.compileExpression(exprStr, data->type());
  kSuspender.dismiss();
  for (int i = 0; i != iters; ++i) {
    benchmarkBase.evaluate(expr, data);
  }
  return iters * opts.vectorSize;
}

BENCHMARK_NAMED_PARAM_MULTI(in_int, presto_rhs1, 1, "presto");
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(in_int, spark_rhs1, 1, "spark");
BENCHMARK_NAMED_PARAM_MULTI(in_int, presto_rhs3, 3, "presto");
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(in_int, spark_rhs3, 3, "spark");
BENCHMARK_NAMED_PARAM_MULTI(in_int, presto_rhs10, 10, "presto");
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(in_int, spark_rhs10, 10, "spark");
BENCHMARK_NAMED_PARAM_MULTI(in_int, presto_rhs100, 100, "presto");
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(in_int, spark_rhs100, 100, "spark");

int in_str(int iters, int inListSize, const std::string& functionName) {
  folly::BenchmarkSuspender kSuspender;
  test::FunctionBenchmarkBase benchmarkBase;

  VectorFuzzer::Options opts;
  opts.vectorSize = 1024;
  auto vector = VectorFuzzer(opts, benchmarkBase.pool()).fuzzFlat(VARCHAR());
  const auto data = benchmarkBase.maker().rowVector({vector});

  std::string exprStr = functionName + "(c0";
  for (int i = 0; i < inListSize; i++) {
    fmt::format_to(
        std::back_inserter(exprStr),
        ", '{}'",
        vector->as<SimpleVector<StringView>>()->valueAt(
            folly::Random::rand32() % opts.vectorSize));
  }
  exprStr += ")";
  exec::ExprSet expr = benchmarkBase.compileExpression(exprStr, data->type());
  kSuspender.dismiss();
  for (int i = 0; i != iters; ++i) {
    benchmarkBase.evaluate(expr, data);
  }
  return iters * opts.vectorSize;
}

BENCHMARK_NAMED_PARAM_MULTI(in_str, presto_rhs1, 1, "presto");
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(in_str, spark_rhs1, 1, "spark");
BENCHMARK_NAMED_PARAM_MULTI(in_str, presto_rhs3, 3, "presto");
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(in_str, spark_rhs3, 3, "spark");
BENCHMARK_NAMED_PARAM_MULTI(in_str, presto_rhs10, 10, "presto");
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(in_str, spark_rhs10, 10, "spark");
BENCHMARK_NAMED_PARAM_MULTI(in_str, presto_rhs100, 100, "presto");
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(in_str, spark_rhs100, 100, "spark");

} // namespace

void registerInFunctions() {
  exec::registerStatefulVectorFunction("spark", inSignatures(), makeIn);
}

} // namespace facebook::velox::functions::sparksql

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  facebook::velox::functions::registerPrestoIn();
  facebook::velox::functions::sparksql::registerInFunctions();
  folly::runBenchmarks();
  return 0;
}
