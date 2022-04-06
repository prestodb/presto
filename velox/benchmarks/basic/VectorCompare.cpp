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
#include <gflags/gflags.h>

#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

DEFINE_int64(fuzzer_seed, 99887766, "Seed for random input dataset generator");

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;

namespace {

class VectorCompareBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  explicit VectorCompareBenchmark(size_t vectorSize)
      : FunctionBenchmarkBase(), vectorSize_(vectorSize), rows_(vectorSize) {
    VectorFuzzer::Options opts;
    opts.vectorSize = vectorSize_;
    opts.nullChance = 0;
    opts.containerVariableLength = 1000;
    VectorFuzzer fuzzer(opts, pool(), FLAGS_fuzzer_seed);

    flatVector_ = fuzzer.fuzzFlat(BIGINT());

    arrayVector_ =
        fuzzer.fuzzComplex(std::make_shared<ArrayType>(ArrayType(BIGINT())));

    mapVector_ = fuzzer.fuzzComplex(
        std::make_shared<MapType>(MapType(BIGINT(), BIGINT())));

    rowVector_ = fuzzer.fuzzComplex(
        vectorMaker_.rowType({BIGINT(), BIGINT(), BIGINT()}));
  }

  size_t run(const VectorPtr& vector) {
    size_t sum = 0;

    for (auto i = 0; i < vectorSize_; i++) {
      sum += vector->compare(vector.get(), i, i, flags);
    }
    folly::doNotOptimizeAway(sum);
    return vectorSize_;
  }

  VectorPtr flatVector_;
  VectorPtr arrayVector_;
  VectorPtr mapVector_;
  VectorPtr rowVector_;

 private:
  static constexpr CompareFlags flags{true, true, false};

  const size_t vectorSize_;
  SelectivityVector rows_;
};

std::unique_ptr<VectorCompareBenchmark> benchmark;

BENCHMARK_MULTI(compareSimilarFlat) {
  return benchmark->run(benchmark->flatVector_);
}

BENCHMARK_MULTI(compareSimilarArray) {
  return benchmark->run(benchmark->arrayVector_);
}

BENCHMARK_MULTI(CompareSimilarMap) {
  return benchmark->run(benchmark->mapVector_);
}

BENCHMARK_MULTI(CompareSimilarRow) {
  return benchmark->run(benchmark->rowVector_);
}

BENCHMARK_DRAW_LINE();

} // namespace

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  benchmark = std::make_unique<VectorCompareBenchmark>(1000);
  folly::runBenchmarks();
  benchmark.reset();
  return 0;
}
