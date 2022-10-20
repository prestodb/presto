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
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

// These macros should be set, defined in command line.
// #define WITH_NULL_ARRAYS false
// #define WITH_NULL_VALUES false

// Benchmark a function that returns the minimum value in an array.
// The function returns NULL if the array or any of its elements are NULL.

// Results:
// We expect that simpleMinIntegerDefaultContainsNullsBehavior will perform
// better than simpleMinInteger when there are no nulls present because we can
// do a single check for nulls at the batch level, instead of checking on every
// row.  In cases where null arrays or null elements are present we expect
// simpleMinIntegerDefaultHasNullsBehavior to do worse than simpleMinInteger
// due to the distance between where we decode the index to check for nulls and
// where we decode the index to access the value, which prevents the compiler
// from optimizing this into a single lookup.
// We expect simpleMinIntegerNullFreeFastPath to perform better than
// simpleMinInteger when there are no nulls present for the same reason.  We
// expect simpleMinIntegerNullFreeFastPath to do about as well as
// simpleMinInteger when null arrays or null elements are present because they
// use the same code path after a quick additional check once per batch.

namespace facebook::velox::functions {

template <typename T>
VectorPtr fastMin(const VectorPtr& in) {
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
      rawResults[row] =
          *std::min_element(rawElements + start, rawElements + end);
    }
  }

  return result;
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
        auto v = array[i].value();
        if (v < min) {
          min = v;
        }
      }
    } else {
      for (auto i = 0; i < array.size(); i++) {
        auto v = array[i].value();
        if (v < min) {
          min = v;
        }
      }
    }
    out = min;
    return true;
  }
};

template <typename T>
struct ArrayMinDefaultContainsNullsBehaviorFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool callNullFree(
      TInput& out,
      const null_free_arg_type<Array<TInput>>& array) {
    if (array.size() == 0) {
      return false; // NULL
    }

    auto min = INT32_MAX;
    for (auto i = 0; i < array.size(); i++) {
      auto v = array[i];
      if (v < min) {
        min = v;
      }
    }
    out = min;
    return true;
  }
};

template <typename T>
struct ArrayMinNullFreeFastPathFunction {
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
        auto v = array[i].value();
        if (v < min) {
          min = v;
        }
      }
    } else {
      for (auto i = 0; i < array.size(); i++) {
        auto v = array[i].value();
        if (v < min) {
          min = v;
        }
      }
    }
    out = min;
    return true;
  }

  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool callNullFree(
      TInput& out,
      const null_free_arg_type<Array<TInput>>& array) {
    if (array.size() == 0) {
      return false; // NULL
    }

    auto min = INT32_MAX;
    for (auto i = 0; i < array.size(); i++) {
      auto v = array[i];
      if (v < min) {
        min = v;
      }
    }
    out = min;
    return true;
  }
};

void registerSimpleFunctions() {
  registerFunction<ArrayMinSimpleFunction, int32_t, Array<int32_t>>(
      {"array_min_simple"});

  registerFunction<
      ArrayMinDefaultContainsNullsBehaviorFunction,
      int32_t,
      Array<int32_t>>({"array_min_default_contains_nulls_behavior"});

  registerFunction<ArrayMinNullFreeFastPathFunction, int32_t, Array<int32_t>>(
      {"array_min_null_free_fast_path"});
}

namespace {

class CallNullFreeBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  CallNullFreeBenchmark() : FunctionBenchmarkBase() {
    registerSimpleFunctions();
  }

  RowVectorPtr makeData() {
    std::function<bool(vector_size_t /*row */)> noNulls = nullptr;

    const vector_size_t size = 1'000;
    auto arrayVector = vectorMaker_.arrayVector<int32_t>(
        size,
        [](auto row) { return row % 5; },
        [](auto idx) { return idx % 23; },
        WITH_NULL_ARRAYS ? [](auto row) { return row % 13 == 0; } : noNulls,
        WITH_NULL_ELEMENTS ? [](auto idx) { return idx % 19 == 0; } : noNulls);

    return vectorMaker_.rowVector({arrayVector});
  }

  size_t runFast() {
    folly::BenchmarkSuspender suspender;
    auto arrayVector = makeData()->childAt(0);
    suspender.dismiss();

    int cnt = 0;
    for (auto i = 0; i < 100; i++) {
      cnt += fastMin<int32_t>(arrayVector)->size();
    }
    return cnt;
  }

  size_t runInteger(const std::string& functionName) {
    folly::BenchmarkSuspender suspender;
    auto arrayVector = makeData()->childAt(0);
    auto rowVector = vectorMaker_.rowVector({arrayVector});
    auto exprSet = compileExpression(
        fmt::format("{}(c0)", functionName), rowVector->type());
    suspender.dismiss();

    return doRun(exprSet, rowVector);
  }

  size_t doRun(exec::ExprSet& exprSet, const RowVectorPtr& rowVector) {
    int cnt = 0;
    for (auto i = 0; i < 100; i++) {
      cnt += evaluate(exprSet, rowVector)->size();
    }
    return cnt;
  }

  bool hasSameResults(
      const VectorPtr& expected,
      exec::ExprSet& expr,
      const RowVectorPtr& input) {
    auto result = evaluate(expr, input);
    if (expected->size() != result->size()) {
      return false;
    }

    for (auto i = 0; i < result->size(); i++) {
      if (!result->equalValueAt(result.get(), i, i)) {
        return false;
      }
    }
    return true;
  }

  void test() {
    auto input = makeData();
    auto fastResult = fastMin<int32_t>(input->childAt(0));

    std::vector<std::string> functions = {
        "array_min_simple",
        "array_min_default_contains_nulls_behavior",
        "array_min_null_free_fast_path",
    };

    for (const auto& name : functions) {
      auto other =
          compileExpression(fmt::format("{}(c0)", name), input->type());
      if (!hasSameResults(fastResult, other, input)) {
        VELOX_UNREACHABLE(fmt::format("testing failed at function {}", name));
      }
    }
  }
};

BENCHMARK_MULTI(vectorFastPath) {
  CallNullFreeBenchmark benchmark;
  return benchmark.runFast();
}

BENCHMARK_MULTI(simpleMinInteger) {
  CallNullFreeBenchmark benchmark;
  return benchmark.runInteger("array_min_simple");
}

BENCHMARK_MULTI(simpleMinIntegerDefaultContainsNullsBehavior) {
  CallNullFreeBenchmark benchmark;
  return benchmark.runInteger("array_min_default_contains_nulls_behavior");
}

BENCHMARK_MULTI(simpleMinIntegerNullFreeFastPath) {
  CallNullFreeBenchmark benchmark;
  return benchmark.runInteger("array_min_null_free_fast_path");
}
} // namespace
} // namespace facebook::velox::functions

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  facebook::velox::functions::CallNullFreeBenchmark benchmark;
  benchmark.test();
  folly::runBenchmarks();
  return 0;
}
