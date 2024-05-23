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
#include "velox/functions/sparksql/Register.h"

using namespace facebook;

using namespace facebook::velox;

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  memory::MemoryManager::initialize({});
  functions::sparksql::registerFunctions("");

  ExpressionBenchmarkBuilder benchmarkBuilder;
  benchmarkBuilder
      .addBenchmarkSet(
          "compare", ROW({"c0", "c1"}, {DECIMAL(18, 6), DECIMAL(38, 16)}))
      .withFuzzerOptions({.vectorSize = 1000, .nullRatio = 0.1})
      .addExpression("gt", "decimal_greaterthan(c0, c1)")
      .addExpression(
          "gt_with_cast", "greaterthan(cast (c0 as decimal(38, 16)), c1)")
      .addExpression("gte", "decimal_greaterthanorequal(c0, c1)")
      .addExpression(
          "gte_with_cast",
          "greaterthanorequal(cast (c0 as decimal(38, 16)), c1)")
      .addExpression("lt", "decimal_lessthan(c0, c1)")
      .addExpression(
          "lt_with_cast", "lessthan(cast (c0 as decimal(38, 16)), c1)")
      .addExpression("lte", "decimal_lessthanorequal(c0, c1)")
      .addExpression(
          "lte_with_cast", "lessthanorequal(cast (c0 as decimal(38, 16)), c1)")
      .addExpression("eq", "decimal_equalto(c0, c1)")
      .addExpression(
          "eq_with_cast", "equalto(cast (c0 as decimal(38, 16)), c1)")
      .addExpression("neq", "decimal_notequalto(c0, c1)")
      .addExpression(
          "neq_with_cast", "not(equalto(cast (c0 as decimal(38, 16)), c1))")
      .withIterations(100);

  benchmarkBuilder.registerBenchmarks();
  folly::runBenchmarks();
  return 0;
}
