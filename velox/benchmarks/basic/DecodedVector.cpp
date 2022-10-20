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

#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

DEFINE_int64(fuzzer_seed, 99887766, "Seed for random input dataset generator");

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;

namespace {

class DecodedVectorBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  explicit DecodedVectorBenchmark(size_t vectorSize)
      : FunctionBenchmarkBase(), vectorSize_(vectorSize), rows_(vectorSize) {
    VectorFuzzer::Options opts;
    opts.vectorSize = vectorSize_;
    opts.nullRatio = 0;
    VectorFuzzer fuzzer(opts, pool(), FLAGS_fuzzer_seed);

    flatVector_ = fuzzer.fuzzFlat(BIGINT());
    constantVector_ = fuzzer.fuzzConstant(BIGINT());
    dictionaryVector_ = fuzzer.fuzzDictionary(fuzzer.fuzzFlat(BIGINT()));

    // Generate nested dictionary vector.
    dictionaryNestedVector_ = fuzzer.fuzzFlat(BIGINT());
    for (size_t i = 0; i < 5; ++i) {
      dictionaryNestedVector_ = fuzzer.fuzzDictionary(dictionaryNestedVector_);
    }
  }

  // Runs a fast path over a flat vector (no decoding).
  size_t runFlat() {
    const int64_t* flatBuffer = flatVector_->values()->as<int64_t>();
    size_t sum = 0;

    for (auto i = 0; i < vectorSize_; i++) {
      sum += flatBuffer[i];
    }
    folly::doNotOptimizeAway(sum);
    return vectorSize_;
  }

  // Runs over a decoded flat vector.
  size_t decodedRunFlat() {
    folly::BenchmarkSuspender suspender;
    DecodedVector decodedVector(*flatVector_, rows_);
    suspender.dismiss();
    decodedRun(decodedVector);
    return vectorSize_;
  }

  // Runs over a decoded constant vector.
  size_t decodedRunConstant() {
    folly::BenchmarkSuspender suspender;
    DecodedVector decodedVector(*constantVector_, rows_);
    suspender.dismiss();
    decodedRun(decodedVector);
    return vectorSize_;
  }

  // Runs over a decoded dictionary vector.
  size_t decodedRunDict() {
    folly::BenchmarkSuspender suspender;
    DecodedVector decodedVector(*dictionaryVector_, rows_);
    suspender.dismiss();
    decodedRun(decodedVector);
    return vectorSize_;
  }

  // Runs over a decoded nested dictionary vector, with 5 layers of indirection.
  size_t decodedRunDict5Nested() {
    folly::BenchmarkSuspender suspender;
    DecodedVector decodedVector(*dictionaryNestedVector_, rows_);
    suspender.dismiss();
    decodedRun(decodedVector);
    return vectorSize_;
  }

  // Measure time to decode a flat vector.
  void decodeFlat() {
    DecodedVector decodedVector(*flatVector_, rows_);
  }

  // Measure time to decode a constant vector.
  void decodeConstant() {
    DecodedVector decodedVector(*constantVector_, rows_);
  }

  // Measure time to decode a dictionary vector.
  void decodeDictionary() {
    DecodedVector decodedVector(*dictionaryVector_, rows_);
  }

  // Measure time to decode a 5-way nested dictionary vector.
  void decodeDictionary5Nested() {
    DecodedVector decodedVector(*dictionaryNestedVector_, rows_);
  }

 private:
  void decodedRun(const DecodedVector& decodedVector) {
    size_t sum = 0;
    for (auto i = 0; i < vectorSize_; i++) {
      sum += decodedVector.valueAt<int64_t>(i);
    }
    folly::doNotOptimizeAway(sum);
  }

  const size_t vectorSize_;

  VectorPtr flatVector_;
  VectorPtr constantVector_;
  VectorPtr dictionaryVector_;
  VectorPtr dictionaryNestedVector_;

  SelectivityVector rows_;
};

std::unique_ptr<DecodedVectorBenchmark> benchmark;

BENCHMARK_MULTI(scanFlat) {
  return benchmark->runFlat();
}

BENCHMARK_MULTI(scanDecodedFlat) {
  return benchmark->decodedRunFlat();
}

BENCHMARK_MULTI(scanDecodedConstant) {
  return benchmark->decodedRunConstant();
}

BENCHMARK_MULTI(scanDecodedDict) {
  return benchmark->decodedRunDict();
}

BENCHMARK_MULTI(scanDecodedDict5Nested) {
  return benchmark->decodedRunDict5Nested();
}

BENCHMARK_DRAW_LINE();

BENCHMARK(decodeFlat) {
  benchmark->decodeFlat();
}

BENCHMARK(decodeConstant) {
  benchmark->decodeConstant();
}

BENCHMARK(decodeDictionary) {
  benchmark->decodeDictionary();
}

BENCHMARK(decodeDictionary5Nested) {
  benchmark->decodeDictionary5Nested();
}

} // namespace

int main(int argc, char* argv[]) {
  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  benchmark = std::make_unique<DecodedVectorBenchmark>(10'000'000);
  folly::runBenchmarks();
  benchmark.reset();
  return 0;
}
