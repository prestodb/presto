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

class SelectivityVectorBenchmark
    : public functions::test::FunctionBenchmarkBase {
 public:
  explicit SelectivityVectorBenchmark(size_t vectorSize)
      : FunctionBenchmarkBase(),
        vectorSize_(vectorSize),
        rowsAll_(vectorSize),
        rows99PerCent_(vectorSize),
        rows50PerCent_(vectorSize),
        rows10PerCent_(vectorSize),
        rows1PerCent_(vectorSize) {
    VectorFuzzer::Options opts;
    opts.vectorSize = vectorSize_;
    opts.nullRatio = 0;
    VectorFuzzer fuzzer(opts, pool(), FLAGS_fuzzer_seed);
    flatVector_ = fuzzer.fuzzFlat(BIGINT());

    for (size_t i = 0; i < vectorSize_; ++i) {
      // Set half to invalid.
      if (fuzzer.coinToss(0.5)) {
        rows50PerCent_.setValid(i, false);
      }

      // Set 90% to invalid.
      if (fuzzer.coinToss(0.9)) {
        rows10PerCent_.setValid(i, false);
      }

      // Set 99% to invalid.
      if (fuzzer.coinToss(0.99)) {
        rows1PerCent_.setValid(i, false);
      }

      // Set 1% to invalid.
      if (fuzzer.coinToss(0.01)) {
        rows99PerCent_.setValid(i, false);
      }
    }

    rowsAll_.updateBounds();
    rows99PerCent_.updateBounds();
    rows50PerCent_.updateBounds();
    rows10PerCent_.updateBounds();
    rows1PerCent_.updateBounds();
  }

  size_t runBaseline() {
    const int64_t* flatBuffer = flatVector_->values()->as<int64_t>();
    size_t sum = 0;

    for (auto i = 0; i < vectorSize_; i++) {
      sum += flatBuffer[i];
    }
    folly::doNotOptimizeAway(sum);
    return vectorSize_;
  }

  size_t runSelectivityAll() {
    return run(rowsAll_);
  }

  size_t runSelectivity50PerCent() {
    return run(rows50PerCent_);
  }

  size_t runSelectivity10PerCent() {
    return run(rows10PerCent_);
  }

  size_t runSelectivity1PerCent() {
    return run(rows1PerCent_);
  }

  size_t runSelectivity99PerCent() {
    return run(rows99PerCent_);
  }

 private:
  size_t run(const SelectivityVector& rows) {
    const int64_t* flatBuffer = flatVector_->values()->as<int64_t>();
    size_t sum = 0;
    rows.applyToSelected(
        [&flatBuffer, &sum](auto row) { sum += flatBuffer[row]; });
    folly::doNotOptimizeAway(sum);
    return vectorSize_;
  }

  const size_t vectorSize_;
  VectorPtr flatVector_;

  SelectivityVector rowsAll_;
  SelectivityVector rows99PerCent_;
  SelectivityVector rows50PerCent_;
  SelectivityVector rows10PerCent_;
  SelectivityVector rows1PerCent_;
};

std::unique_ptr<SelectivityVectorBenchmark> benchmark;

BENCHMARK_MULTI(sumBaselineAll) {
  return benchmark->runBaseline();
}

BENCHMARK_MULTI(sumSelectivityAll) {
  return benchmark->runSelectivityAll();
}

BENCHMARK_MULTI(sumSelectivity99PerCent) {
  return benchmark->runSelectivity99PerCent();
}

BENCHMARK_MULTI(sumSelectivity50PerCent) {
  return benchmark->runSelectivity50PerCent();
}

BENCHMARK_MULTI(sumSelectivity10PerCent) {
  return benchmark->runSelectivity10PerCent();
}

BENCHMARK_MULTI(sumSelectivity1PerCent) {
  return benchmark->runSelectivity1PerCent();
}

} // namespace

int main(int argc, char* argv[]) {
  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  benchmark = std::make_unique<SelectivityVectorBenchmark>(10'000'000);
  folly::runBenchmarks();
  benchmark.reset();
  return 0;
}
