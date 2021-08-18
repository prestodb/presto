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
#include <random>
#include <string>

#include "velox/functions/lib/Re2Functions.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"

namespace facebook::velox::functions::test {
namespace {

using facebook::velox::test::VectorMaker;

class BenchmarkHelper : public FunctionBenchmarkBase {
 public:
  explicit BenchmarkHelper(std::size_t rows) : numRows_(rows) {}

  FlatVectorPtr<StringView> randomShortStrings() {
    std::uniform_int_distribution<int32_t> dist;
    std::vector<std::string> c0(numRows_);
    for (std::string& str : c0) {
      folly::toAppend(dist(rng_), &str);
    }
    return vectorMaker_.flatVector(c0);
  }

  VectorMaker& maker() {
    return vectorMaker_;
  }

 private:
  const std::size_t numRows_;
  std::minstd_rand rng_;
};

int regexMatch(int n, int blocksize, const char* functionName) {
  folly::BenchmarkSuspender kSuspender;
  BenchmarkHelper helper(blocksize);
  const auto data = helper.maker().rowVector({helper.randomShortStrings()});
  exec::ExprSet expr = helper.compileExpression(
      folly::to<std::string>(functionName, "(c0, '[^9]{3,5}')"), data->type());
  kSuspender.dismiss();
  for (int i = 0; i != n; ++i) {
    helper.evaluate(expr, data);
  }
  return n * blocksize;
}

BENCHMARK_NAMED_PARAM_MULTI(regexMatch, bs1k, 1 << 10, "re2_match");
BENCHMARK_NAMED_PARAM_MULTI(regexMatch, bs10k, 10 << 10, "re2_match");
BENCHMARK_NAMED_PARAM_MULTI(regexMatch, bs100k, 100 << 10, "re2_match");

int regexSearch(int n, int blocksize, const char* functionName) {
  return regexMatch(n, blocksize, functionName);
}

BENCHMARK_NAMED_PARAM_MULTI(regexSearch, bs1k, 1 << 10, "re2_search");
BENCHMARK_NAMED_PARAM_MULTI(regexSearch, bs10k, 10 << 10, "re2_search");
BENCHMARK_NAMED_PARAM_MULTI(regexSearch, bs100k, 100 << 10, "re2_search");

int regexExtract(int n, int blocksize) {
  folly::BenchmarkSuspender kSuspender;
  BenchmarkHelper helper(blocksize);
  const auto data = helper.maker().rowVector(
      {helper.randomShortStrings(),
       helper.maker().constantVector<int32_t>({0})});
  exec::ExprSet expr =
      helper.compileExpression("re2_extract(c0, '99[^9]', c1)", data->type());
  kSuspender.dismiss();
  for (int i = 0; i != n; ++i) {
    helper.evaluate(expr, data);
  }
  return n * blocksize;
}

BENCHMARK_NAMED_PARAM_MULTI(regexExtract, bs100, 100);
BENCHMARK_NAMED_PARAM_MULTI(regexExtract, bs1k, 1 << 10);
BENCHMARK_NAMED_PARAM_MULTI(regexExtract, bs10k, 10 << 10);
BENCHMARK_NAMED_PARAM_MULTI(regexExtract, bs100k, 100 << 10);

} // namespace

void registerRe2Functions() {
  exec::registerStatefulVectorFunction(
      "re2_match", re2MatchSignatures(), makeRe2Match);
  exec::registerStatefulVectorFunction(
      "re2_search", re2SearchSignatures(), makeRe2Search);
  exec::registerStatefulVectorFunction(
      "re2_extract", re2ExtractSignatures(), makeRe2Extract);
}

} // namespace facebook::velox::functions::test

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  facebook::velox::functions::test::registerRe2Functions();
  folly::runBenchmarks();
  return 0;
}
