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
#include <folly/Random.h>
#include <folly/init/Init.h>
#include "velox/experimental/codegen/CodegenCompiledExpressionTransform.h"
#include "velox/experimental/codegen/benchmark/CodegenBenchmark.h"

using namespace facebook::velox;
using namespace facebook::velox::codegen;

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};

  CodegenBenchmark benchmark;

  auto inputRowType = ROW({"a", "b"}, std::vector<TypePtr>{DOUBLE(), DOUBLE()});
  benchmark.benchmarkExpressions<DoubleType, DoubleType>(
      "SimpleAddSub", {" a + b", "a - b"}, inputRowType, 100, 1000);

  BENCHMARK_DRAW_LINE();

  auto inputRowType2 =
      ROW({"a", "b"}, std::vector<TypePtr>{DOUBLE(), DOUBLE()});
  benchmark.benchmarkExpressions<DoubleType, DoubleType, DoubleType>(
      "SimpleAddSubMul", {" a + b", "a - b", "a * b"}, inputRowType, 100, 1000);

  folly::runBenchmarks();
  benchmark.compareResults();
  return 0;
}
