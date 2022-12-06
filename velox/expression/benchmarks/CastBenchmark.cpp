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

#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox;

namespace {
class CastBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  CastBenchmark() : FunctionBenchmarkBase() {}

  size_t doRun(const TypePtr& inputType, const TypePtr& outputType) {
    folly::BenchmarkSuspender suspender;
    std::string colName = "c0";
    std::vector<facebook::velox::core::TypedExprPtr> inputs{
        std::make_shared<facebook::velox::core::FieldAccessTypedExpr>(
            inputType, colName)};
    std::vector<facebook::velox::core::TypedExprPtr> expr{
        std::make_shared<facebook::velox::core::CastTypedExpr>(
            outputType, inputs, false)};
    exec::ExprSet exprSet(expr, &execCtx_);

    facebook::velox::VectorFuzzer fuzzer({}, pool());
    // With encodings, evalMemo can get invoked which does a copy and adds a lot
    // of overhead.
    auto input = fuzzer.fuzzFlatNotNull(inputType);
    auto rowVector = vectorMaker_.rowVector({colName}, {input});
    suspender.dismiss();

    size_t cnt = 0;
    for (auto i = 0; i < 100; i++) {
      auto result = evaluate(exprSet, rowVector);
      cnt += result->size();

      // For complex types with lots of children, clearing the result can
      // actually consume a lot of resources skewing the result.
      suspender.rehire();
      result.reset();
      suspender.dismiss();
    }

    return cnt;
  }
};

BENCHMARK_MULTI(scalar) {
  folly::BenchmarkSuspender suspender;
  CastBenchmark benchmark;
  suspender.dismiss();

  return benchmark.doRun(INTEGER(), BIGINT());
}

BENCHMARK_MULTI(renameSmallStruct) {
  folly::BenchmarkSuspender suspender;
  CastBenchmark benchmark;
  std::vector<std::string> oldNames;
  std::vector<std::string> newNames;
  std::vector<TypePtr> oldTypes;
  std::vector<TypePtr> newTypes;
  for (int i = 0; i < 3; i++) {
    oldNames.push_back("");
    newNames.push_back(fmt::format("col{}", i));
    oldTypes.push_back(INTEGER());
  }
  newTypes = oldTypes;
  suspender.dismiss();

  return benchmark.doRun(
      ROW(std::move(oldNames), std::move(oldTypes)),
      ROW(std::move(newNames), std::move(newTypes)));
}

BENCHMARK_MULTI(renameLargeStruct) {
  folly::BenchmarkSuspender suspender;
  CastBenchmark benchmark;
  std::vector<std::string> oldNames;
  std::vector<std::string> newNames;
  std::vector<TypePtr> oldTypes;
  std::vector<TypePtr> newTypes;
  for (int i = 0; i < 1000; i++) {
    oldNames.push_back("");
    newNames.push_back(fmt::format("col{}", i));
    oldTypes.push_back(INTEGER());
  }
  newTypes = oldTypes;
  suspender.dismiss();

  return benchmark.doRun(
      ROW(std::move(oldNames), std::move(oldTypes)),
      ROW(std::move(newNames), std::move(newTypes)));
}

BENCHMARK_MULTI(smallStructNested) {
  folly::BenchmarkSuspender suspender;
  CastBenchmark benchmark;
  std::vector<std::string> oldNames;
  std::vector<std::string> newNames;
  std::vector<TypePtr> oldTypes;
  std::vector<TypePtr> newTypes;
  for (int i = 0; i < 3; i++) {
    oldNames.push_back("");
    newNames.push_back(fmt::format("col{}", i));
    oldTypes.push_back(INTEGER());
    newTypes.push_back(BIGINT());
  }
  suspender.dismiss();

  return benchmark.doRun(
      ROW(std::move(oldNames), std::move(oldTypes)),
      ROW(std::move(newNames), std::move(newTypes)));
}

BENCHMARK_MULTI(largeStructNested) {
  folly::BenchmarkSuspender suspender;
  CastBenchmark benchmark;
  std::vector<std::string> oldNames;
  std::vector<std::string> newNames;
  std::vector<TypePtr> oldTypes;
  std::vector<TypePtr> newTypes;
  for (int i = 0; i < 1000; i++) {
    oldNames.push_back("");
    newNames.push_back(fmt::format("col{}", i));
    oldTypes.push_back(INTEGER());
    newTypes.push_back(BIGINT());
  }
  suspender.dismiss();

  return benchmark.doRun(
      ROW(std::move(oldNames), std::move(oldTypes)),
      ROW(std::move(newNames), std::move(newTypes)));
}
} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv);

  folly::runBenchmarks();
  return 0;
}
