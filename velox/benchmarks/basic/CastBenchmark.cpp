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

using namespace facebook;

using namespace facebook::velox;

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);

  ExpressionBenchmarkBuilder benchmarkBuilder;

  auto vectorMaker = benchmarkBuilder.vectorMaker();
  auto invalidInput = vectorMaker.flatVector<facebook::velox::StringView>({""});
  auto validInput = vectorMaker.flatVector<facebook::velox::StringView>({""});
  auto nanInput = vectorMaker.flatVector<facebook::velox::StringView>({""});

  invalidInput->resize(1000);
  validInput->resize(1000);
  nanInput->resize(1000);

  for (int i = 0; i < 1000; i++) {
    nanInput->set(i, "$"_sv);
    invalidInput->set(i, StringView::makeInline(std::string("")));
    validInput->set(i, StringView::makeInline(std::to_string(i)));
  }

  benchmarkBuilder
      .addBenchmarkSet(
          "cast_int",
          vectorMaker.rowVector(
              {"valid", "empty", "nan"}, {validInput, invalidInput, nanInput}))
      .addExpression("try_cast_invalid_empty_input", "try_cast (empty as int) ")
      .addExpression(
          "tryexpr_cast_invalid_empty_input", "try (cast (empty as int))")
      .addExpression("try_cast_invalid_nan", "try_cast (nan as int)")
      .addExpression("tryexpr_cast_invalid_nan", "try (cast (nan as int))")
      .addExpression("try_cast_valid", "try_cast (valid as int)")
      .addExpression("tryexpr_cast_valid", "try (cast (valid as int))")
      .addExpression("cast_valid", "cast(valid as int)")
      .withIterations(100)
      .disableTesting();

  benchmarkBuilder.registerBenchmarks();
  folly::runBenchmarks();
  return 0;
}
