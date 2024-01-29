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
#include "velox/benchmarks/ExpressionBenchmarkBuilder.h"
#include "velox/functions/Macros.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace {

template <typename T>
VELOX_UDF_BEGIN(contains)
FOLLY_ALWAYS_INLINE bool call(
    bool& out,
    const arg_type<Array<T>>& array,
    const arg_type<T>& key) {
  if (array.mayHaveNulls()) {
    auto nullFound = false;
    for (const auto& item : array) {
      if (item.has_value()) {
        if (*item == key) {
          out = true;
          return true;
        }
        continue;
      }
      nullFound = true;
    }

    if (!nullFound) {
      out = false;
      return true;
    }
    return false;
  }

  // Not nulls path
  for (const auto& item : array) {
    if (*item == key) {
      out = true;
      return true;
    }
  }
  out = false;
  return true;
}
VELOX_UDF_END();

} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  functions::prestosql::registerArrayFunctions();

  registerFunction<
      udf_contains<int32_t>,
      bool,
      facebook::velox::Array<int32_t>,
      int32_t>({"contains_alt"});

  ExpressionBenchmarkBuilder benchmarkBuilder;
  auto inputType = ROW({"c0", "c1"}, {ARRAY(INTEGER()), INTEGER()});

  benchmarkBuilder.addBenchmarkSet("contains_benchmark", inputType)
      .addExpression("vector", "contains(c0,  c1)")
      .addExpression("simple", "contains_alt(c0, c1)");

  benchmarkBuilder.addBenchmarkSet("contains_benchmark_with_nulls", inputType)
      .withFuzzerOptions({.vectorSize = 1000, .nullRatio = 0})
      .addExpression("vector", "contains(c0,  c1)")
      .addExpression("simple", "contains_alt(c0, c1)");

  benchmarkBuilder.registerBenchmarks();
  // Make sure all expressions within benchmarkSets have the same results.
  benchmarkBuilder.testBenchmarks();
  folly::runBenchmarks();
  return 0;
}
