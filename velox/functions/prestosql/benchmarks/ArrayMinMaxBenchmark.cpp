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
#include "velox/functions/Macros.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/VectorFunctions.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace {

template <typename T, typename Func>
VectorPtr fastMinMax(const VectorPtr& in, const Func& func) {
  const auto numRows = in->size();
  auto result = std::static_pointer_cast<FlatVector<T>>(
      BaseVector::create(in->type()->childAt(0), numRows, in->pool()));
  auto rawResults = result->mutableRawValues();

  auto arrayVector = in->as<ArrayVector>();
  auto rawOffsets = arrayVector->rawOffsets();
  auto rawSizes = arrayVector->rawSizes();
  auto rawElements = arrayVector->elements()->as<FlatVector<T>>()->rawValues();
  for (auto row = 0; row < numRows; ++row) {
    const auto start = rawOffsets[row];
    const auto end = start + rawSizes[row];
    if (start == end) {
      result->setNull(row, true); // NULL
    } else {
      rawResults[row] = *func(rawElements + start, rawElements + end);
    }
  }

  return result;
}

template <typename T>
VectorPtr fastMin(const VectorPtr& in) {
  return fastMinMax<T, decltype(std::min_element<const T*>)>(
      in, std::min_element<const T*>);
}

template <typename T>
VectorPtr fastMax(const VectorPtr& in) {
  return fastMinMax<T, decltype(std::max_element<const T*>)>(
      in, std::max_element<const T*>);
}

template <typename T>
VELOX_UDF_BEGIN(array_min_simple)
FOLLY_ALWAYS_INLINE bool call(T& out, const arg_type<Array<T>>& x) {
  if (x.begin() == x.end()) {
    return false;
  }
  out = std::min_element(x.begin(), x.end())->value();
  return true;
}
VELOX_UDF_END();

template <typename T>
VELOX_UDF_BEGIN(array_max_simple)
FOLLY_ALWAYS_INLINE bool call(T& out, const arg_type<Array<T>>& x) {
  if (x.begin() == x.end()) {
    return false;
  }
  out = std::max_element(x.begin(), x.end())->value();
  return true;
}
VELOX_UDF_END();

class ArrayMinMaxBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  ArrayMinMaxBenchmark() : FunctionBenchmarkBase() {
    functions::registerVectorFunctions();
    registerFunction<udf_array_min_simple<int32_t>, int32_t, Array<int32_t>>();
    registerFunction<udf_array_max_simple<int32_t>, int32_t, Array<int32_t>>();
  }

  void runInteger(const std::string& functionName) {
    folly::BenchmarkSuspender suspender;
    vector_size_t size = 1'000;
    auto arrayVector = vectorMaker_.arrayVector<int32_t>(
        size,
        [](auto row) { return row % 5; },
        [](auto row) { return row % 23; });

    auto rowVector = vectorMaker_.rowVector({arrayVector});
    auto exprSet = compileExpression(
        fmt::format("{}(c0)", functionName), rowVector->type());
    suspender.dismiss();

    doRun(exprSet, rowVector);
  }

  void doRun(ExprSet& exprSet, const RowVectorPtr& rowVector) {
    int cnt = 0;
    for (auto i = 0; i < 100; i++) {
      cnt += evaluate(exprSet, rowVector)->size();
    }
    folly::doNotOptimizeAway(cnt);
  }

  template <typename F>
  void runFastInteger(F function) {
    folly::BenchmarkSuspender suspender;
    vector_size_t size = 1'000;
    auto arrayVector = vectorMaker_.arrayVector<int32_t>(
        size,
        [](auto row) { return row % 5; },
        [](auto row) { return row % 23; });

    auto rowVector = vectorMaker_.rowVector({arrayVector});
    suspender.dismiss();

    int cnt = 0;
    for (auto i = 0; i < 100; i++) {
      cnt += function(arrayVector)->size();
    }
    folly::doNotOptimizeAway(cnt);
  }
};

BENCHMARK(fastMinInteger) {
  ArrayMinMaxBenchmark benchmark;
  benchmark.runFastInteger(fastMin<int32_t>);
}

BENCHMARK_RELATIVE(vectorMinInteger) {
  ArrayMinMaxBenchmark benchmark;
  benchmark.runInteger("array_min");
}

BENCHMARK_RELATIVE(simpleMinInteger) {
  ArrayMinMaxBenchmark benchmark;
  benchmark.runInteger("array_min_simple");
}

BENCHMARK(fastMaxInteger) {
  ArrayMinMaxBenchmark benchmark;
  benchmark.runFastInteger(fastMax<int32_t>);
}

BENCHMARK_RELATIVE(vectorMaxInteger) {
  ArrayMinMaxBenchmark benchmark;
  benchmark.runInteger("array_max");
}

BENCHMARK_RELATIVE(simpleMaxInteger) {
  ArrayMinMaxBenchmark benchmark;
  benchmark.runInteger("array_max_simple");
}
} // namespace

int main(int /*argc*/, char** /*argv*/) {
  folly::runBenchmarks();
  return 0;
}
