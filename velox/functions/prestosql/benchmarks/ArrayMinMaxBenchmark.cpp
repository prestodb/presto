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
#include "velox/functions/Registerer.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/benchmarks/ArrayMinMaxBasic.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

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
struct ArrayMinSimpleFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool call(
      TInput& out,
      const arg_type<Array<TInput>>& array) {
    if (array.size() == 0) {
      return false; // NULL
    }

    auto min = INT32_MAX;
    if (array.mayHaveNulls()) {
      for (auto i = 0; i < array.size(); i++) {
        if (!array[i].has_value()) {
          return false; // NULL
        }
        if (array[i].value() < min) {
          min = array[i].value();
        }
      }
    } else {
      for (auto i = 0; i < array.size(); i++) {
        if (array[i].value() < min) {
          min = array[i].value();
        }
      }
    }
    out = min;
    return true;
  }
};

template <typename T>
struct ArrayMinSimpleFunctionIterator {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool call(
      TInput& out,
      const arg_type<Array<TInput>>& array) {
    const auto size = array.size();
    if (size == 0) {
      return false; // NULL
    }

    auto min = INT32_MAX;
    if (array.mayHaveNulls()) {
      for (const auto& item : array) {
        if (!item.has_value()) {
          return false; // NULL
        }
        if (item.value() < min) {
          min = item.value();
        }
      }
    } else {
      for (const auto& item : array) {
        if (item.value() < min) {
          min = item.value();
        }
      }
    }

    out = min;
    return true;
  }
};

// Returns the minimum value in an array ignoring nulls.
// The point of this is to exercise SkipNullsIterator.
template <typename T>
struct ArrayMinSimpleFunctionSkipNullIterator {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool call(
      TInput& out,
      const arg_type<Array<TInput>>& array) {
    const auto size = array.size();
    if (size == 0) {
      return false; // NULL
    }

    bool hasValue = false;
    auto min = INT32_MAX;
    for (const auto& item : array.skipNulls()) {
      hasValue = true;
      if (item < min) {
        min = item;
      }
    }

    if (!hasValue) {
      return false;
    }

    out = min;
    return true;
  }
};

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_array_min_basic,
    functions::signatures(),
    std::make_unique<functions::ArrayMinMaxFunctionBasic<std::less>>());

class ArrayMinMaxBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  ArrayMinMaxBenchmark() : FunctionBenchmarkBase() {
    functions::prestosql::registerArrayFunctions();
    VELOX_REGISTER_VECTOR_FUNCTION(udf_array_min_basic, "array_min_basic");

    registerFunction<ArrayMinSimpleFunction, int32_t, Array<int32_t>>(
        {"array_min_simple"});
    registerFunction<ArrayMinSimpleFunctionIterator, int32_t, Array<int32_t>>(
        {"array_min_simple_iterator"});
    registerFunction<
        ArrayMinSimpleFunctionSkipNullIterator,
        int32_t,
        Array<int32_t>>({"array_min_simple_skip_null_iterator"});
  }

  RowVectorPtr makeData() {
    const vector_size_t size = 1'000;
    auto arrayVector = vectorMaker_.arrayVector<int32_t>(
        size,
        [](auto row) { return row % 5; },
        [](auto row) { return row % 23; });

    return vectorMaker_.rowVector({arrayVector});
  }

  void runInteger(const std::string& functionName) {
    folly::BenchmarkSuspender suspender;
    auto arrayVector = makeData()->childAt(0);
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
    auto arrayVector = makeData()->childAt(0);
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

BENCHMARK_RELATIVE(vectorMinIntegerNoFastPath) {
  ArrayMinMaxBenchmark benchmark;
  benchmark.runInteger("array_min_basic");
}

BENCHMARK_RELATIVE(simpleMinIntegerIterator) {
  ArrayMinMaxBenchmark benchmark;
  benchmark.runInteger("array_min_simple_iterator");
}

BENCHMARK_RELATIVE(simpleMinIntegerSkipNullIterator) {
  ArrayMinMaxBenchmark benchmark;
  benchmark.runInteger("array_min_simple_skip_null_iterator");
}

BENCHMARK_RELATIVE(simpleMinInteger) {
  ArrayMinMaxBenchmark benchmark;
  benchmark.runInteger("array_min_simple");
}
} // namespace

int main(int /*argc*/, char** /*argv*/) {
  folly::runBenchmarks();
  return 0;
}
