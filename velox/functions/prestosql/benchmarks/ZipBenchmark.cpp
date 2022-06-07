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
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

namespace facebook::velox::functions::test {
namespace {
class ZipBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  ZipBenchmark() : FunctionBenchmarkBase() {
    prestosql::registerArrayFunctions();
  }

  VectorPtr makeData(vector_size_t arraySize, bool dictionaryEncoding) {
    constexpr vector_size_t size = 100;

    std::vector<std::optional<int64_t>> values;
    values.reserve(size * arraySize);
    for (int i = 0; i < size * arraySize; i++) {
      values.push_back(i);
    }

    VectorPtr valuesVector = vectorMaker_.dictionaryVector(values);

    std::vector<vector_size_t> offsets;
    offsets.reserve(size);
    for (int i = 0; i < size; i++) {
      offsets.push_back(i * arraySize);
    }

    VectorPtr array = vectorMaker_.arrayVector(offsets, valuesVector);

    if (dictionaryEncoding) {
      BufferPtr indices = AlignedBuffer::allocate<vector_size_t>(size, pool());
      auto rawIndices = indices->asMutable<vector_size_t>();
      for (int i = 0; i < size; i++) {
        rawIndices[i] = i;
      }

      array = BaseVector::wrapInDictionary(nullptr, indices, size, array);
    }

    return array;
  }

  size_t run(const VectorPtr& c0, const VectorPtr& c1) {
    folly::BenchmarkSuspender suspender;
    auto inputs = vectorMaker_.rowVector({c0, c1});

    auto exprSet = compileExpression("zip(c0, c1)", inputs->type());
    suspender.dismiss();

    return doRun(exprSet, inputs);
  }

  size_t doRun(exec::ExprSet& exprSet, const RowVectorPtr& rowVector) {
    int cnt = 0;
    for (auto i = 0; i < 100; i++) {
      cnt += evaluate(exprSet, rowVector)->size();
    }
    folly::doNotOptimizeAway(cnt);
    return cnt;
  }
};

BENCHMARK_MULTI(EqSizeFlatFlat) {
  ZipBenchmark benchmark;
  folly::BenchmarkSuspender suspender;
  auto c0 = benchmark.makeData(5, false);
  auto c1 = benchmark.makeData(5, false);
  suspender.dismiss();

  return benchmark.run(c0, c1);
}

BENCHMARK_MULTI(EqSizeFlatDictionary) {
  ZipBenchmark benchmark;
  folly::BenchmarkSuspender suspender;
  auto c0 = benchmark.makeData(5, false);
  auto c1 = benchmark.makeData(5, true);
  suspender.dismiss();

  return benchmark.run(c0, c1);
}

BENCHMARK_MULTI(NeqSizeFlatFlat) {
  ZipBenchmark benchmark;
  folly::BenchmarkSuspender suspender;
  auto c0 = benchmark.makeData(5, false);
  auto c1 = benchmark.makeData(3, false);
  suspender.dismiss();

  return benchmark.run(c0, c1);
}
} // namespace
} // namespace facebook::velox::functions::test

int main(int /*argc*/, char** /*argv*/) {
  folly::runBenchmarks();
  return 0;
}
