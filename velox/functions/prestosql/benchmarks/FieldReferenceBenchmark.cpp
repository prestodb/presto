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
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::functions;

std::vector<std::string> getColumnNames(int children) {
  std::vector<std::string> result;
  for (int i = 0; i < children; ++i) {
    result.push_back(fmt::format("{}{}", 'c', i));
  }
  return result;
}

RowTypePtr getRowColumnType(FuzzerGenerator& rng, int children, int level) {
  VELOX_CHECK_GE(level, 1);
  VELOX_CHECK_GE(children, 3);
  std::vector<TypePtr> result;
  result.push_back(ARRAY(INTEGER()));
  result.push_back(INTEGER());
  if (level > 1) {
    result.push_back(getRowColumnType(rng, children, level - 1));
  } else {
    result.push_back(randType(rng, 2));
  }
  for (int i = 0; i < children - 3; ++i) {
    result.push_back(randType(rng, 2));
  }
  return ROW(getColumnNames(children), std::move(result));
}

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};

  ExpressionBenchmarkBuilder benchmarkBuilder;
  FuzzerGenerator rng;

  auto createSet = [&](bool withNulls, RowTypePtr& inputType) {
    benchmarkBuilder
        .addBenchmarkSet(
            fmt::format("dereference_{}", withNulls ? "nulls" : "nullfree"),
            inputType)
        .withFuzzerOptions(
            {.vectorSize = 1000, .nullRatio = withNulls ? 0.2 : 0})
        .addExpression("1LevelThenFlat", "(c0).c1")
        .addExpression("1LevelThenComplex", "(c0).c0")
        .addExpression("2LevelThenFlat", "(c0).c2.c1")
        .addExpression("2LevelThenComplex", "(c0).c2.c0")
        .addExpression("3LevelThenFlat", "(c0).c2.c2.c1")
        .addExpression("3LevelThenComplex", "(c0).c2.c2.c0")
        .addExpression("4LevelThenFlat", "(c0).c2.c2.c2.c1")
        .addExpression("4LevelThenComplex", "(c0).c2.c2.c2.c0");
  };

  // Create a nested row column of depth 4. Each level has 50 columns. Each ROW
  // at depth n will have the first three columns as ARRAY(INTEGER()), INTEGER()
  // and ROW {of depth 4-n} respectively. The third column for the deepest ROW
  // however can be anything.
  auto inputType = ROW({"c0"}, {getRowColumnType(rng, 50, 4)});

  createSet(true, inputType);
  createSet(false, inputType);

  benchmarkBuilder.registerBenchmarks();

  folly::runBenchmarks();
  return 0;
}
