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
#include "velox/vector/fuzzer/VectorFuzzer.h"

DEFINE_int64(fuzzer_seed, 99887766, "Seed for random input dataset generator");
DEFINE_int64(vector_size, 10000, "Size of vectors to benchmark");
DEFINE_int64(row_children, 1000, "Number of children in row vector");
DEFINE_int64(dict_nesting, 5, "Number of dictionary nesting levels");

using namespace facebook::velox;
using namespace facebook::velox::test;

namespace {

class EstimateFlatSizeBenchmark
    : public functions::test::FunctionBenchmarkBase {
 public:
  EstimateFlatSizeBenchmark(size_t vectorSize, size_t rowChildren)
      : FunctionBenchmarkBase(),
        vectorSize_(vectorSize),
        numRowChildren_(rowChildren) {
    VectorFuzzer::Options opts;
    opts.vectorSize = vectorSize_;
    opts.nullRatio = 0;
    opts.stringLength = 10;
    VectorFuzzer fuzzer(opts, pool(), FLAGS_fuzzer_seed);

    // Create flat vectors of different types
    flatBigintVector_ = fuzzer.fuzzFlat(BIGINT());
    flatVarcharVector_ = fuzzer.fuzzFlat(VARCHAR());

    // Create constant vectors
    constantBigintVector_ = fuzzer.fuzzConstant(BIGINT());
    constantVarcharVector_ = fuzzer.fuzzConstant(VARCHAR());

    // Create dictionary vectors
    dictionaryBigintVector_ = fuzzer.fuzzDictionary(fuzzer.fuzzFlat(BIGINT()));
    dictionaryVarcharVector_ =
        fuzzer.fuzzDictionary(fuzzer.fuzzFlat(VARCHAR()));

    // Create nested dictionary vector
    nestedDictionaryVector_ = fuzzer.fuzzFlat(BIGINT());
    for (size_t i = 0; i < 5; ++i) {
      nestedDictionaryVector_ = fuzzer.fuzzDictionary(nestedDictionaryVector_);
    }

    // Create a nested row vector with complex children
    std::vector<std::string> names;
    std::vector<TypePtr> types;
    std::vector<VectorPtr> children;

    // Create a mix of different types of children
    for (size_t i = 0; i < numRowChildren_; ++i) {
      names.push_back(fmt::format("field{}", i));

      // Create different types of children based on the index
      switch (i % 5) {
        case 0: {
          // Flat vector
          types.push_back(BIGINT());
          children.push_back(fuzzer.fuzzFlat(BIGINT()));
          break;
        }
        case 1: {
          // Dictionary vector
          types.push_back(VARCHAR());
          children.push_back(fuzzer.fuzzDictionary(fuzzer.fuzzFlat(VARCHAR())));
          break;
        }
        case 2: {
          // Nested row vector
          std::vector<std::string> nestedNames = {
              "nested1", "nested2", "nested3"};
          std::vector<TypePtr> nestedTypes = {BIGINT(), VARCHAR(), DOUBLE()};
          auto nestedRowType =
              ROW(std::move(nestedNames), std::move(nestedTypes));
          types.push_back(nestedRowType);
          children.push_back(fuzzer.fuzzRow(nestedRowType, vectorSize_));
          break;
        }
        case 3: {
          // Array vector
          auto arrayType = ARRAY(BIGINT());
          types.push_back(arrayType);
          children.push_back(fuzzer.fuzzArray(BIGINT(), vectorSize_));
          break;
        }
        case 4: {
          // Map vector
          auto mapType = MAP(VARCHAR(), BIGINT());
          types.push_back(mapType);
          children.push_back(fuzzer.fuzzMap(VARCHAR(), BIGINT(), vectorSize_));
          break;
        }
      }
    }

    auto rowType = ROW(std::move(names), std::move(types));
    rowVector_ = std::make_shared<RowVector>(
        pool(), rowType, nullptr, vectorSize_, std::move(children));
  }

  // Benchmark methods for estimateFlatSize
  void estimateFlatSizeFlatBigint() {
    auto size = flatBigintVector_->estimateFlatSize();
    folly::doNotOptimizeAway(size);
  }

  void estimateFlatSizeFlatVarchar() {
    auto size = flatVarcharVector_->estimateFlatSize();
    folly::doNotOptimizeAway(size);
  }

  void estimateFlatSizeConstantBigint() {
    auto size = constantBigintVector_->estimateFlatSize();
    folly::doNotOptimizeAway(size);
  }

  void estimateFlatSizeConstantVarchar() {
    auto size = constantVarcharVector_->estimateFlatSize();
    folly::doNotOptimizeAway(size);
  }

  void estimateFlatSizeDictionaryBigint() {
    auto size = dictionaryBigintVector_->estimateFlatSize();
    folly::doNotOptimizeAway(size);
  }

  void estimateFlatSizeDictionaryVarchar() {
    auto size = dictionaryVarcharVector_->estimateFlatSize();
    folly::doNotOptimizeAway(size);
  }

  void estimateFlatSizeNestedDictionary() {
    auto size = nestedDictionaryVector_->estimateFlatSize();
    folly::doNotOptimizeAway(size);
  }

  void estimateFlatSizeRowVector() {
    auto size = rowVector_->estimateFlatSize();
    folly::doNotOptimizeAway(size);
  }

 private:
  const size_t vectorSize_;
  const size_t numRowChildren_;

  VectorPtr flatBigintVector_;
  VectorPtr flatVarcharVector_;
  VectorPtr constantBigintVector_;
  VectorPtr constantVarcharVector_;
  VectorPtr dictionaryBigintVector_;
  VectorPtr dictionaryVarcharVector_;
  VectorPtr nestedDictionaryVector_;
  RowVectorPtr rowVector_;
};

std::unique_ptr<EstimateFlatSizeBenchmark> benchmark;

template <typename Func>
void run(Func&& func, size_t iterations = 100) {
  for (auto i = 0; i < iterations; i++) {
    func();
  }
}

BENCHMARK(estimateFlatSizeFlatBigint) {
  run([&] { benchmark->estimateFlatSizeFlatBigint(); });
}

BENCHMARK(estimateFlatSizeFlatVarchar) {
  run([&] { benchmark->estimateFlatSizeFlatVarchar(); });
}

BENCHMARK(estimateFlatSizeConstantBigint) {
  run([&] { benchmark->estimateFlatSizeConstantBigint(); });
}

BENCHMARK(estimateFlatSizeConstantVarchar) {
  run([&] { benchmark->estimateFlatSizeConstantVarchar(); });
}

BENCHMARK(estimateFlatSizeDictionaryBigint) {
  run([&] { benchmark->estimateFlatSizeDictionaryBigint(); });
}

BENCHMARK(estimateFlatSizeDictionaryVarchar) {
  run([&] { benchmark->estimateFlatSizeDictionaryVarchar(); });
}

BENCHMARK(estimateFlatSizeNestedDictionary) {
  run([&] { benchmark->estimateFlatSizeNestedDictionary(); });
}

BENCHMARK(estimateFlatSizeRowVector) {
  run([&] { benchmark->estimateFlatSizeRowVector(); });
}

} // namespace

int main(int argc, char* argv[]) {
  folly::Init init{&argc, &argv};
  ::gflags::ParseCommandLineFlags(&argc, &argv, true);
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});

  benchmark = std::make_unique<EstimateFlatSizeBenchmark>(
      FLAGS_vector_size, FLAGS_row_children);
  folly::runBenchmarks();
  benchmark.reset();
  return 0;
}
