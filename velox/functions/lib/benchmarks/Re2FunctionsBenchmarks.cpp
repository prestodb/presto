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
#include <fmt/format.h>
#include <folly/Benchmark.h>
#include <folly/Conv.h>
#include <folly/init/Init.h>
#include <string>

#include "velox/functions/lib/Re2Functions.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

namespace facebook::velox::functions::test {
namespace {

int regexMatch(int n, int blockSize, const char* functionName) {
  folly::BenchmarkSuspender kSuspender;
  FunctionBenchmarkBase benchmarkBase;

  VectorFuzzer::Options opts;
  opts.vectorSize = blockSize;
  auto vector = VectorFuzzer(opts, benchmarkBase.pool()).fuzzFlat(VARCHAR());
  const auto data = benchmarkBase.maker().rowVector({vector});

  exec::ExprSet expr = benchmarkBase.compileExpression(
      folly::to<std::string>(functionName, "(c0, '[^9]{3,5}')"), data->type());
  kSuspender.dismiss();
  for (int i = 0; i != n; ++i) {
    benchmarkBase.evaluate(expr, data);
  }
  return n * blockSize;
}

BENCHMARK_NAMED_PARAM_MULTI(regexMatch, bs1k, 1 << 10, "re2_match");
BENCHMARK_NAMED_PARAM_MULTI(regexMatch, bs10k, 10 << 10, "re2_match");
BENCHMARK_NAMED_PARAM_MULTI(regexMatch, bs100k, 100 << 10, "re2_match");

int regexSearch(int n, int blockSize, const char* functionName) {
  return regexMatch(n, blockSize, functionName);
}

BENCHMARK_NAMED_PARAM_MULTI(regexSearch, bs1k, 1 << 10, "re2_search");
BENCHMARK_NAMED_PARAM_MULTI(regexSearch, bs10k, 10 << 10, "re2_search");
BENCHMARK_NAMED_PARAM_MULTI(regexSearch, bs100k, 100 << 10, "re2_search");

int regexExtract(int n, int blockSize) {
  folly::BenchmarkSuspender kSuspender;
  FunctionBenchmarkBase benchmarkBase;

  VectorFuzzer::Options opts;
  opts.vectorSize = blockSize;
  auto vector = VectorFuzzer(opts, benchmarkBase.pool()).fuzzFlat(VARCHAR());
  const auto data = benchmarkBase.maker().rowVector({
      vector,
      benchmarkBase.maker().constantVector<int32_t>({0}),
  });

  exec::ExprSet expr = benchmarkBase.compileExpression(
      "re2_extract(c0, '99[^9]', c1)", data->type());
  kSuspender.dismiss();
  for (int i = 0; i != n; ++i) {
    benchmarkBase.evaluate(expr, data);
  }
  return n * blockSize;
}

BENCHMARK_NAMED_PARAM_MULTI(regexExtract, bs100, 100);
BENCHMARK_NAMED_PARAM_MULTI(regexExtract, bs1k, 1 << 10);
BENCHMARK_NAMED_PARAM_MULTI(regexExtract, bs10k, 10 << 10);
BENCHMARK_NAMED_PARAM_MULTI(regexExtract, bs100k, 100 << 10);

} // namespace

std::shared_ptr<exec::VectorFunction> makeRegexExtract(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  return makeRe2Extract(name, inputArgs, config, /*emptyNoMatch=*/false);
}

void registerRe2Functions() {
  exec::registerStatefulVectorFunction(
      "re2_match", re2MatchSignatures(), makeRe2Match);
  exec::registerStatefulVectorFunction(
      "re2_search", re2SearchSignatures(), makeRe2Search);
  exec::registerStatefulVectorFunction(
      "re2_extract", re2ExtractSignatures(), makeRegexExtract);
}

} // namespace facebook::velox::functions::test

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  facebook::velox::functions::test::registerRe2Functions();
  facebook::velox::memory::MemoryManager::initialize({});
  folly::runBenchmarks();
  return 0;
}
