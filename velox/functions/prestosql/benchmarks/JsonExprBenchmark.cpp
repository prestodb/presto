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
/**
 * This file tests the performance of each JsonXXXFunction.call() with
 * expression framework and Velox vectors.
 */
#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include "velox/functions/Registerer.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/JsonFunctions.h"
#include "velox/functions/prestosql/SIMDJsonFunctions.h"
#include "velox/functions/prestosql/benchmarks/JsonBenchmarkUtil.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

namespace facebook::velox::functions::prestosql {
namespace {

const std::string smallJson = R"({"k1":"v1"})";

class JsonBenchmark : public velox::functions::test::FunctionBenchmarkBase {
 public:
  JsonBenchmark() : FunctionBenchmarkBase() {
    velox::functions::prestosql::registerJsonFunctions();
    registerFunction<IsJsonScalarFunction, bool, Json>(
        {"folly_is_json_scalar"});
    registerFunction<SIMDIsJsonScalarFunction, bool, Json>(
        {"simd_is_json_scalar"});
  }

  std::string prepareData(int jsonSize) {
    std::string jsonData = R"({"key": [)" + smallJson;
    for (int i = 0; i < jsonSize / 10; i++) {
      jsonData += "," + smallJson;
    }
    jsonData += "]}";
    return jsonData;
  }

  velox::VectorPtr makeJsonData(const std::string& json, int vectorSize) {
    auto jsonVector =
        vectorMaker_.flatVector<velox::StringView>(vectorSize, JSON());
    for (auto i = 0; i < vectorSize; i++) {
      jsonVector->set(i, velox::StringView(json));
    }
    return jsonVector;
  }

  void runWithJson(
      int iter,
      int vectorSize,
      const std::string& fnName,
      const std::string& json) {
    folly::BenchmarkSuspender suspender;

    auto jsonVector = makeJsonData(json, vectorSize);

    auto rowVector = vectorMaker_.rowVector({jsonVector});
    auto exprSet =
        compileExpression(fmt::format("{}(c0)", fnName), rowVector->type());
    suspender.dismiss();
    doRun(iter, exprSet, rowVector);
  }

  void doRun(
      const int iter,
      velox::exec::ExprSet& exprSet,
      const velox::RowVectorPtr& rowVector) {
    uint32_t cnt = 0;
    for (auto i = 0; i < iter; i++) {
      cnt += evaluate(exprSet, rowVector)->size();
    }
    folly::doNotOptimizeAway(cnt);
  }
};

void FollyIsJsonScalar(int iter, int vectorSize, int jsonSize) {
  folly::BenchmarkSuspender suspender;
  JsonBenchmark benchmark;
  auto json = benchmark.prepareData(jsonSize);
  suspender.dismiss();
  benchmark.runWithJson(iter, vectorSize, "folly_is_json_scalar", json);
}

void SIMDIsJsonScalar(int iter, int vectorSize, int jsonSize) {
  folly::BenchmarkSuspender suspender;
  JsonBenchmark benchmark;
  auto json = benchmark.prepareData(jsonSize);
  suspender.dismiss();
  benchmark.runWithJson(iter, vectorSize, "simd_is_json_scalar", json);
}

BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(FollyIsJsonScalar, 100_iters_1k_size, 100, 10);
BENCHMARK_RELATIVE_NAMED_PARAM(SIMDIsJsonScalar, 100_iters_1k_size, 100, 10);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(FollyIsJsonScalar, 100_iters_10k_size, 100, 100);
BENCHMARK_RELATIVE_NAMED_PARAM(SIMDIsJsonScalar, 100_iters_10k_size, 100, 100);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(FollyIsJsonScalar, 100_iters_100k_size, 100, 1000);
BENCHMARK_RELATIVE_NAMED_PARAM(
    SIMDIsJsonScalar,
    100_iters_100k_size,
    100,
    1000);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(FollyIsJsonScalar, 100_iters_1000k_size, 100, 10000);
BENCHMARK_RELATIVE_NAMED_PARAM(
    SIMDIsJsonScalar,
    100_iters_1000k_size,
    100,
    10000);
BENCHMARK_DRAW_LINE();

} // namespace
} // namespace facebook::velox::functions::prestosql

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
