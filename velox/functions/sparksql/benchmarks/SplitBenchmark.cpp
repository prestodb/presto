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
#include "velox/functions/sparksql/registration/Register.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace {

class SplitBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  SplitBenchmark(uint32_t seed) : FunctionBenchmarkBase(), seed_{seed} {
    parse::registerTypeResolver();
    functions::sparksql::registerFunctions("");
    generateData();
  }

  void generateData() {
    VectorFuzzer::Options options;
    options.vectorSize = 10000;
    options.stringVariableLength = true;
    options.stringLength = 10;

    VectorFuzzer fuzzer(options, pool(), seed_);

    auto vector = vectorMaker_.rowVector(
        {fuzzer.fuzzFlat(VARCHAR()),
         fuzzer.fuzzFlat(VARCHAR()),
         fuzzer.fuzzFlat(VARCHAR()),
         fuzzer.fuzzFlat(VARCHAR()),
         fuzzer.fuzzFlat(VARCHAR())});
    data_ = vectorMaker_.rowVector({evaluateOnce(kConcatExpression, vector)});
  }

  VectorPtr evaluateOnce(
      const std::string& expression,
      const RowVectorPtr& data) {
    auto exprSet = compileExpression(expression, asRowType(data->type()));
    return evaluate(exprSet, data);
  }

  size_t run(size_t times, const std::string& expression) {
    folly::BenchmarkSuspender suspender;
    auto exprSet = compileExpression(expression, asRowType(data_->type()));
    suspender.dismiss();

    return doRun(exprSet, data_, times);
  }

  size_t doRun(ExprSet& exprSet, const RowVectorPtr& rowVector, size_t times) {
    int cnt = 0;
    for (auto i = 0; i < times * 1'000; i++) {
      cnt += evaluate(exprSet, rowVector)->size();
    }
    return cnt;
  }

  static const std::string kSplitExpression;

 private:
  static const std::string kConcatExpression;
  RowVectorPtr data_ = nullptr;

  const uint32_t seed_;
};

const std::string SplitBenchmark::kSplitExpression = "split(c0, '\\044')";
const std::string SplitBenchmark::kConcatExpression =
    "concat(c0, '$', c1, '$', c2, '$', c3, '$', c4)";

const uint32_t seed = folly::Random::rand32();

std::unique_ptr<SplitBenchmark> benchmark;

BENCHMARK_RELATIVE(split, n) {
  benchmark->run(n, SplitBenchmark::kSplitExpression);
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  LOG(INFO) << "Seed: " << seed;
  benchmark = std::make_unique<SplitBenchmark>(seed);
  folly::runBenchmarks();
  benchmark.reset();
  return 0;
}
