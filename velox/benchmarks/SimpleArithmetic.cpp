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

#include "velox/functions/Registerer.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/Arithmetic.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

DEFINE_int64(fuzzer_seed, 99887766, "Seed for random input dataset generator");

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;

namespace {

class SimpleArithmeticBenchmark
    : public functions::test::FunctionBenchmarkBase {
 public:
  SimpleArithmeticBenchmark() : FunctionBenchmarkBase() {
    registerFunction<functions::MultiplyFunction, double, double, double>(
        {"multiply"});
  }

  void setInput(const TypePtr& inputType, const RowVectorPtr& rowVector) {
    inputType_ = inputType;
    rowVector_ = rowVector;
  }

  // Runs `expression` `times` times.
  void run(const std::string& expression, size_t times) {
    folly::BenchmarkSuspender suspender;
    auto exprSet = compileExpression(expression, inputType_);
    suspender.dismiss();

    size_t count = 0;
    for (auto i = 0; i < times; i++) {
      count += evaluate(exprSet, rowVector_)->size();
    }
    folly::doNotOptimizeAway(count);
  }

 private:
  TypePtr inputType_;
  RowVectorPtr rowVector_;
};

SimpleArithmeticBenchmark benchmark;

BENCHMARK(multiply, n) {
  benchmark.run("multiply(a, a)", n);
}

BENCHMARK(multiplyHalfNull, n) {
  benchmark.run("multiply(a, half_null)", n);
}

BENCHMARK(multiplyConstant, n) {
  benchmark.run("multiply(a, constant)", n);
}

BENCHMARK(multiplyNested, n) {
  benchmark.run("multiply(multiply(a, b), b)", n);
}

BENCHMARK(multiplyNestedDeep, n) {
  benchmark.run(
      "multiply(multiply(multiply(a, b), a), "
      "multiply(a, multiply(a, b)))",
      n);
}

} // namespace

int main(int argc, char* argv[]) {
  folly::init(&argc, &argv);

  // Set input schema.
  auto inputType = ROW({
      {"a", DOUBLE()},
      {"b", DOUBLE()},
      {"constant", DOUBLE()},
      {"half_null", DOUBLE()},
  });
  const size_t size = 100'000;

  // Generate input data.
  auto* pool = benchmark.pool();
  VectorFuzzer::Options opts;
  opts.vectorSize = size;
  opts.nullChance = 0;

  std::vector<VectorPtr> children;
  children.emplace_back(
      VectorFuzzer(opts, pool, FLAGS_fuzzer_seed).fuzzFlat(DOUBLE())); // A
  children.emplace_back(
      VectorFuzzer(opts, pool, FLAGS_fuzzer_seed).fuzzFlat(DOUBLE())); // B
  children.emplace_back(
      BaseVector::createConstant(123.45, size, pool)); // Constant
  opts.nullChance = 2; // 50%
  children.emplace_back(VectorFuzzer(opts, pool, FLAGS_fuzzer_seed)
                            .fuzzFlat(DOUBLE())); // HalfNull

  auto rowVector = std::make_shared<RowVector>(
      pool, inputType, nullptr, size, std::move(children));

  // Set them into the benchmark object and start the tests.
  benchmark.setInput(inputType, rowVector);
  folly::runBenchmarks();
  return 0;
}
