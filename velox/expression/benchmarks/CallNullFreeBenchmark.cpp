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

void registerVectorFunctionFastPath() {
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_min, "vector_fast_path");
}

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
    registerVectorFunctionFastPath();
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
      exec::ExprSet& expr1,
      exec::ExprSet& expr2,
      const RowVectorPtr& input) {
    auto result1 = evaluate(expr1, input);
    auto result2 = evaluate(expr2, input);
    if (result1->size() != result2->size()) {
      return false;
    }

    for (auto i = 0; i < result1->size(); i++) {
      if (!result1->equalValueAt(result2.get(), i, i)) {
        return false;
      }
    }
    return true;
  }

  void test() {
    auto input = makeData();
    auto exprSetRef = compileExpression("vector_fast_path(c0)", input->type());
    std::vector<std::string> functions = {
        "array_min_simple",
        "array_min_default_contains_nulls_behavior",
        "array_min_null_free_fast_path",
    };

    for (const auto& name : functions) {
      auto other =
          compileExpression(fmt::format("{}(c0)", name), input->type());
      if (!hasSameResults(exprSetRef, other, input)) {
        VELOX_UNREACHABLE(fmt::format("testing failed at function {}", name));
      }
    }
  }
};

BENCHMARK_MULTI(vectorFastPath) {
  CallNullFreeBenchmark benchmark;
  return benchmark.runInteger("vector_fast_path");
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

int main(int /*argc*/, char** /*argv*/) {
  facebook::velox::functions::CallNullFreeBenchmark benchmark;
  benchmark.test();
  folly::runBenchmarks();
  return 0;
}
