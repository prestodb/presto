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
  invalidInput->resize(1000);
  validInput->resize(1000);

  for (int i = 0; i < 1000; i++) {
    invalidInput->set(i, ""_sv);
    validInput->set(i, StringView::makeInline(std::to_string(i)));
  }

  benchmarkBuilder
      .addBenchmarkSet(
          "cast_int",
          vectorMaker.rowVector(
              {"valid", "invalid"}, {validInput, invalidInput}))
      .addExpression("try_invalid", "try_cast (invalid as int)")
      .addExpression("try_valid", "try_cast (valid as int)")
      .addExpression("valid", "cast(valid as int)")
      .withIterations(100)
      .disableTesting();

  benchmarkBuilder.registerBenchmarks();
  folly::runBenchmarks();
  return 0;
}
