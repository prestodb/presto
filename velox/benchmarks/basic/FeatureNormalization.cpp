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

#include <gflags/gflags.h>

#include "velox/functions/Registerer.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

DEFINE_int64(fuzzer_seed, 99887766, "Seed for random input dataset generator");

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::functions;
using namespace facebook::velox::test;

namespace {

class FeatureNormailzationBenchmark
    : public functions::test::FunctionBenchmarkBase {
 public:
  explicit FeatureNormailzationBenchmark() : FunctionBenchmarkBase() {
    prestosql::registerComparisonFunctions();
    prestosql::registerArithmeticFunctions();

    // Set input schema.
    inputType_ = ROW({
        {"a", REAL()},
    });

    // Generate input data.
    rowVector_ = makeRowVector(100);
  }

  RowVectorPtr makeRowVector(vector_size_t size) {
    VectorFuzzer::Options opts;
    opts.vectorSize = size;
    opts.nullRatio = 0;
    VectorFuzzer fuzzer(opts, pool(), FLAGS_fuzzer_seed);

    std::vector<VectorPtr> children{fuzzer.fuzzFlat(REAL())};
    float* rawValues =
        children.at(0)->as<FlatVector<float>>()->mutableRawValues();
    for (int i = 0; i < size; i++) {
      // fuzzFlat returns values in the range [0, 1), transform it into the
      // range [0, 2).
      rawValues[i] = rawValues[i] * 2;
    }
    // Hard code a couple values to ensure the range of data is never entirely
    // in the ranges [0, 1) or [1, 2).
    rawValues[0] = 0;
    rawValues[size - 1] = 1;

    return std::make_shared<RowVector>(
        pool(), inputType_, nullptr, size, std::move(children));
  }

  // Runs `expression` `times` thousand times.
  size_t run(const std::string& expression, size_t times) {
    folly::BenchmarkSuspender suspender;
    auto exprSet = compileExpression(expression, inputType_);
    suspender.dismiss();

    size_t count = 0;
    for (auto i = 0; i < times * 1'000; i++) {
      auto result = evaluate(exprSet, rowVector_);
      count += result->size();
    }
    return count;
  }

 private:
  TypePtr inputType_;
  RowVectorPtr rowVector_;
};

std::unique_ptr<FeatureNormailzationBenchmark> benchmark;

// Benchmark a common calculation used in machine learning to normalize floating
// point features.
BENCHMARK_MULTI(normalize, n) {
  // floor(a) = 1 will return both true and false on some of the rows.
  return benchmark->run(
      "clamp(0.05::REAL * (20.5::REAL + if(floor(a) = 1::REAL, 1.0::REAL, 0.0::REAL)), (-10.0)::REAL, 10.0::REAL)",
      n);
}

BENCHMARK_MULTI(normalizeConstant, n) {
  // floor(a) = 2 will always return false.
  return benchmark->run(
      "clamp(0.05::REAL * (20.5::REAL + if(floor(a) = 2::REAL, 1.0::REAL, 0.0::REAL)), (-10.0)::REAL, 10.0::REAL)",
      n);
}

} // namespace

int main(int argc, char* argv[]) {
  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  benchmark = std::make_unique<FeatureNormailzationBenchmark>();
  folly::runBenchmarks();
  benchmark.reset();
  return 0;
}
